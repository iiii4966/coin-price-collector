const axios = require('axios');
const { connectToDatabase, createTables, CANDLE_INTERVALS } = require('./dbUtils');

const BASE_URL = 'https://api.exchange.coinbase.com';
const GRANULARITIES = [60, 300, 900, 3600, 86400]; // 1분, 5분, 15분, 1시간, 1일
const MAX_CANDLES = 2000;
const CANDLES_PER_REQUEST = 300;
const REQUESTS_PER_SECOND = 10;

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

async function collectHistoricalCandles(product, granularity) {
    let collectedCandles = 0;
    let end = null;

    while (collectedCandles < MAX_CANDLES) {
        const candles = await fetchCandles(product.id, granularity, end);

        if (candles.length === 0) {
            console.log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 수집 완료: ${collectedCandles}개`);
            break;
        }

        const start = candles[0][0];
        end = candles[candles.length - 1][0];

        if (start === end) {
            console.log(`${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들 수집 완료: ${collectedCandles}개`);
            break;
        }

        console.log(
            `${product.id} - ${GRANULARITY_TO_INTERVAL[granularity]}분 캔들:`,
            `${candles.length}개`, new Date(start * 1000), '~', new Date(end * 1000),
        );

        await saveCandles(product.id, candles, granularity);
        collectedCandles += candles.length;

        // API 요청 제한 준수
        await new Promise(resolve => setTimeout(resolve, 1000 / REQUESTS_PER_SECOND));
    }
}

async function main() {
    try {
        db = await connectToDatabase();
        await createTables(db);

        const products = await getUSDProducts();
        console.log(`총 ${products.length}개의 USD 상품을 찾았습니다.`);

        // for (const product of products) {
        //     for (const granularity of GRANULARITIES) {
        //         await collectHistoricalCandles(product, granularity);
        //     }
        // }
        for (const granularity of GRANULARITIES.slice(1, 2)) {
            await collectHistoricalCandles({id: 'PAX-USD'}, granularity);
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
