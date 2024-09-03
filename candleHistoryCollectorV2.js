const axios = require('axios');
const fs = require('fs');
const { connectToDatabase, createTables, CANDLE_INTERVALS } = require('./dbUtils');

const BASE_URL = 'https://api.exchange.coinbase.com';
const GRANULARITY = 60; // 1분 캔들
const MAX_CANDLES = 6000;
const CANDLES_PER_REQUEST = 300;
const REQUESTS_PER_SECOND = 10;
const PRODUCT_ID = 'BTC-USD';
const TEMP_DB_NAME = 'temp_candles.db';
const PROGRESS_FILE = 'candle_collection_progress.json';

let db;

async function fetchCandles(end = null) {
    const url = `${BASE_URL}/products/${PRODUCT_ID}/candles`;
    const params = { granularity: GRANULARITY };

    if (end) {
        const start = end - (GRANULARITY * CANDLES_PER_REQUEST);
        params.start = start.toString();
        params.end = end.toString();
    }

    const response = await axios.get(url, { params });
    return response.data;
}

async function saveCandles(candles) {
    return new Promise((resolve, reject) => {
        const sql = `INSERT OR REPLACE INTO candles_1 
                     (code, tms, lp, hp, op, cp, tv) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)`;

        db.serialize(() => {
            db.run('BEGIN TRANSACTION');

            const stmt = db.prepare(sql);
            for (const candle of candles) {
                const [tms, low, high, open, close, volume] = candle;
                stmt.run(PRODUCT_ID, tms, low, high, open, close, volume);
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

function saveProgress(lastTimestamp) {
    const progress = {
        productId: PRODUCT_ID,
        lastTimestamp: lastTimestamp
    };
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

function loadProgress() {
    if (fs.existsSync(PROGRESS_FILE)) {
        const data = fs.readFileSync(PROGRESS_FILE, 'utf8');
        return JSON.parse(data);
    }
    return null;
}

async function collectHistoricalCandles() {
    let collectedCandles = 0;
    let end = null;

    const progress = loadProgress();
    if (progress && progress.productId === PRODUCT_ID) {
        end = progress.lastTimestamp;
        console.log(`Resuming collection from timestamp: ${new Date(end * 1000)}`);
    }

    while (collectedCandles < MAX_CANDLES) {
        const candles = await fetchCandles(end);

        console.log(`${PRODUCT_ID} - 1분 캔들 조회: ${candles.length}개`);

        if (candles.length === 0) {
            console.log(`${PRODUCT_ID} - 1분 캔들 수집 완료: 총 ${collectedCandles}개`);
            break;
        }

        const start = candles[0][0];
        end = candles[candles.length - 1][0];

        await saveCandles(candles);
        collectedCandles += candles.length;

        console.log(
            `${PRODUCT_ID} - 1분 캔들 저장 완료:`,
            `${candles.length}개`, new Date(start * 1000), '~', new Date(end * 1000),
        );

        saveProgress(end);

        // API 요청 제한 준수
        await new Promise(resolve => setTimeout(resolve, 1000 / REQUESTS_PER_SECOND));
    }

    console.log(`${PRODUCT_ID} - 1분 캔들 수집 최종 완료: 총 ${collectedCandles}개`);
}

async function main() {
    try {
        db = await connectToDatabase(TEMP_DB_NAME);
        await createTables(db);

        await collectHistoricalCandles();

        console.log('BTC-USD 1분 캔들 데이터 수집이 완료되었습니다.');
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
