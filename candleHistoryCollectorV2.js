const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { connectToDatabase, createTables, CANDLE_INTERVALS } = require('./dbUtils');
const { aggregateHistoricalCandles } = require('./candleHistoryAggregator');

const LOG_DIR = 'logs';
const LOG_FILE = path.join(LOG_DIR, `candle_history_collector_${new Date().toISOString().split('T')[0]}.log`);

// 로그 디렉토리 생성
if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR);
}

// 로그 파일 스트림 생성
const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });

// 로그 함수
function log(...args) {
    const message = args.join(' ');
    console.log(message);
    logStream.write(message + '\n');
}

const BASE_URL = 'https://api.exchange.coinbase.com';
const GRANULARITIES = [60, 300, 900, 3600, 86400]; // 1분, 5분, 15분, 1시간, 1일
const MAX_CANDLES = {
    60: 6000,    // 1분
    300: 4000,   // 5분
    900: 4000,   // 15분
    3600: 8000,  // 60분
    86400: 14000  // 1일 (기존과 동일)
};
const CANDLES_PER_REQUEST = 300;
const REQUESTS_PER_SECOND = 10;
const EMPTY_RESPONSE_RETRY_COUNT = 5;
const EMPTY_SAME_START_END_RESPONSE_RETRY_COUNT = 5;
const TEMP_DB_NAME = 'temp_candles.db';
const FINAL_DB_NAME = 'candles.db';
const PROGRESS_FILE = 'candle_collection_progress.json';

let totalProducts = 0;
let completedProducts = 0;
let completedGranularities = 0;
let totalCandlesCollected = 0;
let totalCandlesToCollect = 0;
let startTime;
let pausedTime = 0;
let storedCandlesCount = 0;

const GRANULARITY_TO_INTERVAL = {
    60: 1,
    300: 5,
    900: 15,
    3600: 60,
    86400: 1440
};

const GRANULARITY_TO_MAX_CANDLES = {
    60: 6000,
    300: 4000,
    900: 4000,
    3600: 8000,
    86400: 14000
};

let tempDb;
let finalDb;

async function getUSDProducts() {
    try {
        const response = await axios.get(`${BASE_URL}/products`);
        return response.data.filter(product =>
            product.quote_currency === 'USD' &&
            (product.status === 'online' || product.status === 'offline')
        );
    } catch (error) {
        log('상품 목록 조회 중 오류 발생:', error.message);
        throw error;
    }
}

async function fetchCandles(productId, granularity, end = null) {
    const url = `${BASE_URL}/products/${productId}/candles`;
    const params = { granularity };

    if (end) {
        const start = end - (granularity * CANDLES_PER_REQUEST);
        params.start = start.toString();
        params.end = end.toString();
    }

    const response = await axios.get(url, { params });
    return response.data;
}

async function saveCandles(productId, candles, granularity) {
    return new Promise((resolve, reject) => {
        const interval = GRANULARITY_TO_INTERVAL[granularity];
        if (!interval) {
            reject(new Error(`Invalid granularity: ${granularity}`));
            return;
        }

        const sql = `INSERT OR REPLACE INTO candles_${interval} 
                     (code, tms, lp, hp, op, cp, tv) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)`;

        tempDb.serialize(() => {
            tempDb.run('BEGIN TRANSACTION');

            const stmt = tempDb.prepare(sql);
            for (const candle of candles) {
                const [tms, low, high, open, close, volume] = candle;
                stmt.run(productId, tms, low, high, open, close, volume);
            }
            stmt.finalize();

            tempDb.run('COMMIT', (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    });
}

async function getStoredCandleCount(productId, granularity) {
    return new Promise((resolve, reject) => {
        const interval = GRANULARITY_TO_INTERVAL[granularity];
        const sql = `SELECT COUNT(*) as count FROM candles_${interval} WHERE code = ?`;
        tempDb.get(sql, [productId], (err, row) => {
            if (err) {
                reject(err);
            } else {
                resolve(row.count);
            }
        });
    });
}

function saveProgress(productId, granularity, lastTimestamp) {
    let progress = {};
    if (fs.existsSync(PROGRESS_FILE)) {
        progress = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    }
    if (!progress[productId]) {
        progress[productId] = {};
    }
    progress[productId][granularity] = lastTimestamp;
    progress.pausedTime = pausedTime + (Date.now() - startTime);
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

async function loadProgress(productId, granularity) {
    if (fs.existsSync(PROGRESS_FILE)) {
        const data = fs.readFileSync(PROGRESS_FILE, 'utf8');
        const progress = JSON.parse(data);
        const storedCount = await getStoredCandleCount(productId, granularity);
        pausedTime = progress.pausedTime || 0;
        return {
            lastTimestamp: progress[productId]?.[granularity],
            storedCount
        };
    }
    return { storedCount: 0 };
}

function formatElapsedTime(milliseconds) {
    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    return `${days}일 ${hours % 24}시간 ${minutes % 60}분 ${seconds % 60}초`;
}

async function collectHistoricalCandles(product, granularity) {
    let collectedCandles = 0;
    let end = null;

    let emptyResponseCount = 0;
    let emptyResponseRetryMax = granularity === 86400 ? 2 : EMPTY_RESPONSE_RETRY_COUNT;

    let sameStartEndCount = 0;

    const progress = await loadProgress(product.id, granularity);
    const storedCount = progress.storedCount;
    const maxCandles = GRANULARITY_TO_MAX_CANDLES[granularity];
    const remainingCandles = maxCandles - storedCount;

    if (progress.lastTimestamp) {
        end = progress.lastTimestamp;
        log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: 타임스탬프', ${new Date(end * 1000)}, '부터 수집 재개`);
    }

    log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: 현재 저장된 캔들 수: ${storedCount}, 수집 가능한 캔들 수: ${remainingCandles}`);

    if (remainingCandles <= 0) {
        log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: 이미 충분한 캔들이 저장되어 있습니다. 수집을 종료합니다.`);
        totalCandlesCollected += maxCandles;
        updateProgress();
        return;
    }

    while (collectedCandles < remainingCandles) {
        const candles = await fetchCandles(product.id, granularity, end);

        log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 조회: ${candles.length}개`);

        if (candles.length === 0) {
            emptyResponseCount++;
            const startTime = new Date((end - granularity * CANDLES_PER_REQUEST) * 1000);
            const endTime = new Date(end * 1000);
            log(
                `${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: 빈 응답 (${emptyResponseCount}번째)`,
                '시간 범위:', startTime, '~', endTime
            );

            if (emptyResponseCount > emptyResponseRetryMax) {
                log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 수집 완료: 총 ${collectedCandles}개`);
                break;
            }
            end = end - (granularity * CANDLES_PER_REQUEST);
            continue;
        }

        emptyResponseCount = 0;
        const start = candles[0][0];
        end = candles[candles.length - 1][0];

        if (start === end) {
            sameStartEndCount++;
            log(
                `${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: start와 end가 같음 (${sameStartEndCount}번째)`,
                new Date(start * 1000), '~', new Date(end * 1000)
            );

            if (sameStartEndCount > EMPTY_SAME_START_END_RESPONSE_RETRY_COUNT) {
                log(
                    `${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 수집 종료; 총 ${collectedCandles}개`,
                    new Date(start * 1000), '~', new Date(end * 1000)
                );
                break;
            }
            end = end - (granularity * CANDLES_PER_REQUEST);
            continue;
        }

        sameStartEndCount = 0;

        const candlesToSave = candles.slice(0, remainingCandles - collectedCandles);
        await saveCandles(product.id, candlesToSave, granularity);
        collectedCandles += candlesToSave.length;
        totalCandlesCollected += candlesToSave.length;

        log(
            `${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 저장 완료:`,
            `${candlesToSave.length}개`, new Date(start * 1000), '~', new Date(end * 1000),
        );

        saveProgress(product.id, granularity, end);
        updateProgress();

        // API 요청 제한 준수
        await new Promise(resolve => setTimeout(resolve, 1000 / REQUESTS_PER_SECOND));

        if (collectedCandles >= remainingCandles) {
            break;
        }
    }

    log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 수집 최종 완료: 총 ${collectedCandles}개 (전체 저장된 캔들: ${storedCount + collectedCandles}개)`);
    log();
}

async function transferRecentCandles() {
    log('최근 2000개의 캔들을 candles.db로 전송 중...');

    // 모든 상품 코드 조회
    const productCodes = await new Promise((resolve, reject) => {
        tempDb.all("SELECT DISTINCT code FROM candles_1", [], (err, rows) => {
            if (err) reject(err);
            else resolve(rows.map(row => row.code));
        });
    });

    for (const interval of CANDLE_INTERVALS) {
        log(`${interval}분 캔들 전송 시작...`);

        for (const productCode of productCodes) {
            // 임시 데이터베이스에서 최근 2000개의 캔들 조회
            const selectSql = `
                SELECT code, tms, op, hp, lp, cp, tv
                FROM candles_${interval}
                WHERE code = ?
                ORDER BY tms DESC
                LIMIT 2000
            `;

            const candles = await new Promise((resolve, reject) => {
                tempDb.all(selectSql, [productCode], (err, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                });
            });

            // 최종 데이터베이스에 삽입
            const insertSql = `
                INSERT OR REPLACE INTO candles_${interval} (code, tms, op, hp, lp, cp, tv)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            `;

            await new Promise((resolve, reject) => {
                finalDb.serialize(() => {
                    const stmt = finalDb.prepare(insertSql);
                    finalDb.run('BEGIN TRANSACTION');

                    for (const candle of candles.reverse()) { // 시간 순으로 정렬
                        stmt.run(candle.code, candle.tms, candle.op, candle.hp, candle.lp, candle.cp, candle.tv);
                    }

                    stmt.finalize();
                    finalDb.run('COMMIT', (err) => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
            });

            log(`${interval}분 캔들 - ${productCode} 전송 완료 (${candles.length}개)`);
        }

        log(`${interval}분 캔들 전송 완료`);
    }
    log('모든 캔들 전송이 완료되었습니다.');
}

async function calculateStoredCandlesCount() {
    let count = 0;
    for (const interval of CANDLE_INTERVALS) {
        const result = await new Promise((resolve, reject) => {
            tempDb.get(`SELECT COUNT(*) as count FROM candles_${interval}`, (err, row) => {
                if (err) reject(err);
                else resolve(row.count);
            });
        });
        count += result;
    }
    return count;
}

async function main() {
    try {
        startTime = Date.now();
        tempDb = await connectToDatabase(TEMP_DB_NAME);
        await createTables(tempDb);

        const products = await getUSDProducts();
        totalProducts = products.length;
        log(`총 ${totalProducts}개의 USD 상품을 찾았습니다.`);

        // 이미 저장된 캔들 수 계산
        storedCandlesCount = await calculateStoredCandlesCount();

        // 전체 수집해야 할 캔들 수 계산
        totalCandlesToCollect = (totalProducts * GRANULARITIES.reduce((sum, granularity) => sum + GRANULARITY_TO_MAX_CANDLES[granularity], 0)) - storedCandlesCount;

        // 캔들 수집
        for (const product of products) {
            for (const granularity of GRANULARITIES) {
                await collectHistoricalCandles(product, granularity);
                completedGranularities++;
                updateProgress();
            }
            completedProducts++;
            updateProgress();
        }

        log('모든 과거 캔들 데이터 수집이 완료되었습니다. (100%)');

        log('캔들 집계 시작...');
        await aggregateHistoricalCandles(tempDb);
        log('캔들 집계 완료');

        finalDb = await connectToDatabase(FINAL_DB_NAME);
        await createTables(finalDb);
        await transferRecentCandles();

        // 캔들 데이터 성공적으로 수집시 candle_collection_progress.json 파일 삭제
        try {
            fs.unlinkSync(PROGRESS_FILE);
            log('candle_collection_progress.json 파일이 삭제되었습니다.');
        } catch (err) {
            console.error('candle_collection_progress.json 파일 삭제 중 오류 발생:', err);
        }

        const totalElapsedTime = Date.now() - startTime + pausedTime;
        log(`전체 실행 시간: ${formatElapsedTime(totalElapsedTime)}`);

        // temp_candles.db 파일 삭제
        try {
            fs.unlinkSync(TEMP_DB_NAME);
            log(`${TEMP_DB_NAME} 파일이 성공적으로 삭제되었습니다.`);
        } catch (err) {
            console.error(`${TEMP_DB_NAME} 파일 삭제 중 오류 발생:`, err);
        }
    } catch (error) {
        console.error('오류 발생:', error);
    } finally {
        if (tempDb) {
            tempDb.close((err) => {
                if (err) {
                    log('임시 데이터베이스 연결 종료 중 오류 발생:', err.message);
                } else {
                    log('임시 데이터베이스 연결이 종료되었습니다.');
                }
            });
        }
        if (finalDb) {
            finalDb.close((err) => {
                if (err) {
                    log('최종 데이터베이스 연결 종료 중 오류 발생:', err.message);
                } else {
                    log('최종 데이터베이스 연결이 종료되었습니다.');
                }
            });
        }
        // 로그 스트림 닫기
        logStream.end();
    }
}

function updateProgress() {
    const totalTasks = totalProducts * GRANULARITIES.length;
    const completedTasks = (completedProducts * GRANULARITIES.length) + completedGranularities;
    const progressPercentage = (completedTasks / totalTasks) * 100;
    const totalCollectedCandles = totalCandlesCollected + storedCandlesCount;
    const totalExpectedCandles = totalCandlesToCollect + storedCandlesCount;
    const candleProgressPercentage = (totalCollectedCandles / totalExpectedCandles) * 100;
    const currentElapsedTime = Date.now() - startTime + pausedTime;

    log()
    log(`진행 상황: ${progressPercentage.toFixed(2)}% (${completedProducts}/${totalProducts} 상품, ${completedGranularities}/${GRANULARITIES.length} 캔들)`);
    log(`캔들 수집 진행 상황: ${candleProgressPercentage.toFixed(2)}% (${totalCollectedCandles}/${totalExpectedCandles} 캔들)`);
    log(`실행 시간: ${formatElapsedTime(currentElapsedTime)}`);
}

main();
