const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();

const BASE_URL = 'https://api.exchange.coinbase.com';
const GRANULARITIES = [60, 900, 3600, 86400]; // 1분, 15분, 1시간, 1일 (초 단위)
const MAX_CANDLES = 2000;
const CANDLES_PER_REQUEST = 300;
const REQUESTS_PER_SECOND = 10;

let db;

function connectToDatabase() {
    return new Promise((resolve, reject) => {
        db = new sqlite3.Database('candles.db', (err) => {
            if (err) {
                console.error('데이터베이스 연결 오류:', err.message);
                reject(err);
            } else {
                console.log('데이터베이스에 연결되었습니다.');
                resolve(db);
            }
        });
    });
}

async function createTable() {
    return new Promise((resolve, reject) => {
        const sql = `CREATE TABLE IF NOT EXISTS candles (
            code TEXT,
            tms INTEGER,
            op REAL,
            hp REAL,
            lp REAL,
            cp REAL,
            tv REAL,
            PRIMARY KEY (code, tms)
        )`;
        db.run(sql, (err) => {
            if (err) {
                console.error('테이블 생성 오류:', err.message);
                reject(err);
            } else {
                console.log('Candles 테이블이 생성되었습니다.');
                resolve();
            }
        });
    });
}

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

async function fetchCandles(productId, granularity, end = new Date().toISOString()) {
    const url = `${BASE_URL}/products/${productId}/candles`;
    const params = { granularity, end };

    try {
        const response = await axios.get(url, { params });
        return response.data;
    } catch (error) {
        console.error(`캔들 데이터 조회 중 오류 발생 (${productId}, ${granularity}):`, error.message);
        return [];
    }
}

async function saveCandles(productId, candles) {
    return new Promise((resolve, reject) => {
        const sql = `INSERT OR REPLACE INTO candles 
                     (code, tms, op, hp, lp, cp, tv) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)`;

        db.serialize(() => {
            db.run('BEGIN TRANSACTION');

            const stmt = db.prepare(sql);
            for (const candle of candles) {
                stmt.run(productId, ...candle);
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
    let end = new Date().toISOString();

    while (collectedCandles < MAX_CANDLES) {
        const candles = await fetchCandles(product.id, granularity, end);
        
        if (candles.length === 0 || candles.length < CANDLES_PER_REQUEST) {
            await saveCandles(product.id, candles);
            console.log(`${product.id} - ${granularity}초 캔들 수집 완료: ${collectedCandles + candles.length}개`);
            break;
        }

        await saveCandles(product.id, candles);
        collectedCandles += candles.length;
        end = new Date(candles[candles.length - 1][0] * 1000).toISOString();

        // API 요청 제한 준수
        await new Promise(resolve => setTimeout(resolve, 1000 / REQUESTS_PER_SECOND));
    }
}

async function main() {
    try {
        await connectToDatabase();
        await createTable();

        const products = await getUSDProducts();
        console.log(`총 ${products.length}개의 USD 상품을 찾았습니다.`);

        for (const product of products) {
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
