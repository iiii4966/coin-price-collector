const axios = require('axios');
const fs = require('fs');
const { connectToDatabase, createTables, CANDLE_INTERVALS } = require('./dbUtils');

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
const PROGRESS_FILE = 'candle_collection_progress.json';

const GRANULARITY_TO_INTERVAL = {
    60: 1,
    300: 5,
    900: 15,
    3600: 60,
    86400: 1440
};

let db;

async function getUSDProducts() {
    try {
        const response = await axios.get(`${BASE_URL}/products`);
        return response.data.filter(product =>
            product.quote_currency === 'USD' &&
            (product.status === 'online' || product.status === 'offline')
        );
    } catch (error) {
        console.error('상품 목록 조회 중 오류 발생:', error.message);
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

        db.serialize(() => {
            db.run('BEGIN TRANSACTION');

            const stmt = db.prepare(sql);
            for (const candle of candles) {
                const [tms, low, high, open, close, volume] = candle;
                stmt.run(productId, tms, low, high, open, close, volume);
            }
            stmt.finalize();

            db.run('COMMIT', (err) => {
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
        db.get(sql, [productId], (err, row) => {
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
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

async function loadProgress(productId, granularity) {
    if (fs.existsSync(PROGRESS_FILE)) {
        const data = fs.readFileSync(PROGRESS_FILE, 'utf8');
        const progress = JSON.parse(data);
        const storedCount = await getStoredCandleCount(productId, granularity);
        return {
            lastTimestamp: progress[productId]?.[granularity],
            storedCount
        };
    }
    return { storedCount: 0 };
}

async function collectHistoricalCandles(product, granularity) {
    let collectedCandles = 0;
    let end = null;

    let emptyResponseCount = 0;
    let emptyResponseRetryMax = granularity === 86400 ? 2 : EMPTY_RESPONSE_RETRY_COUNT;

    let sameStartEndCount = 0;

    const progress = await loadProgress(product.id, granularity);
    const storedCount = progress.storedCount;
    const maxCandles = MAX_CANDLES[granularity] || 2000; // Default to 2000 if not specified
    const remainingCandles = maxCandles - storedCount;

    if (progress.lastTimestamp) {
        end = progress.lastTimestamp;
        console.log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: 타임스탬프 ${new Date(end * 1000)}부터 수집 재개`);
    }

    console.log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: 현재 저장된 캔들 수: ${storedCount}, 수집 가능한 캔들 수: ${remainingCandles}`);

    if (remainingCandles <= 0) {
        console.log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: 이미 충분한 캔들이 저장되어 있습니다. 수집을 종료합니다.`);
        return;
    }

    while (collectedCandles < remainingCandles) {
        const candles = await fetchCandles(product.id, granularity, end);

        console.log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 조회: ${candles.length}개`);

        if (candles.length === 0) {
            emptyResponseCount++;
            const startTime = new Date((end - granularity * CANDLES_PER_REQUEST) * 1000);
            const endTime = new Date(end * 1000);
            console.log(
                `${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: 빈 응답 (${emptyResponseCount}번째)`,
                '시간 범위:', startTime, '~', endTime
            );

            if (emptyResponseCount > emptyResponseRetryMax) {
                console.log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 수집 완료: 총 ${collectedCandles}개`);
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
            console.log(
                `${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들: start와 end가 같음 (${sameStartEndCount}번째)`,
                new Date(start * 1000), '~', new Date(end * 1000)
            );

            if (sameStartEndCount > EMPTY_SAME_START_END_RESPONSE_RETRY_COUNT) {
                console.log(
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

        console.log(
            `${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 저장 완료:`,
            `${candlesToSave.length}개`, new Date(start * 1000), '~', new Date(end * 1000),
        );

        saveProgress(product.id, granularity, end);

        // API 요청 제한 준수
        await new Promise(resolve => setTimeout(resolve, 1000 / REQUESTS_PER_SECOND));

        if (collectedCandles >= remainingCandles) {
            break;
        }
    }

    console.log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 수집 최종 완료: 총 ${collectedCandles}개 (전체 저장된 캔들: ${storedCount + collectedCandles}개)`);
    console.log();
}

async function main() {
    try {
        db = await connectToDatabase(TEMP_DB_NAME);
        await createTables(db);

        const products = await getUSDProducts();
        console.log(`총 ${products.length}개의 USD 상품을 찾았습니다.`);

        for (const product of [{id: 'BTC-USD'}]) {
            for (const granularity of GRANULARITIES) {
                await collectHistoricalCandles(product, granularity);
            }
        }

        console.log('모든 과거 캔들 데이터 수집이 완료되었습니다.');
    } catch (error) {
        console.error('오류 발생:', error);
    } finally {
        if (db) {
            db.close((err) => {
                if (err) {
                    console.error('데이터베이스 연결 종료 중 오류 발생:', err.message);
                } else {
                    console.log('데이터베이스 연결이 종료되었습니다.');
                }
            });
        }
    }
}

main();
