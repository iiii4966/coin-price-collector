const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();
const { connectToDatabase, CANDLE_INTERVALS } = require('./dbUtils');
const fs = require('fs');

function isApproximatelyEqual(a, b, tolerance = 0.001) {
    return Math.abs(a - b) < tolerance;
}

const BASE_URL = 'https://api.exchange.coinbase.com';
const PRODUCT_ID = 'BTC-USD';
const CANDLES_TO_CHECK = 200;

const INTERVAL_TO_GRANULARITY = {
    // 3: 180,
    5: 300,
    15: 900,
    60: 3600,
    // 240: 14400,
    1440: 86400,
    // 10080: 604800
};

let db;

async function fetchCoinbaseCandles(interval, startTime, endTime) {
    const granularity = INTERVAL_TO_GRANULARITY[interval];
    if (!granularity) {
        return [];
    }
    const url = `${BASE_URL}/products/${PRODUCT_ID}/candles`;
    const params = {
        granularity,
        start: startTime.toString(),
        end: endTime.toString()
    };

    try {
        const response = await axios.get(url, { params });
        return response.data;
    } catch (error) {
        console.error(`Coinbase API 조회 중 오류 발생 (${interval}분 캔들):`, error.message);
        return [];
    }
}

function getLocalCandles(interval) {
    return new Promise((resolve, reject) => {
        const sql = `SELECT * FROM candles_${interval} 
                     WHERE code = ? 
                     ORDER BY tms DESC 
                     LIMIT ?`;

        db.all(sql, [PRODUCT_ID, CANDLES_TO_CHECK], (err, rows) => {
            if (err) {
                reject(err);
            } else {
                resolve(rows.reverse()); // 시간 순서대로 정렬
            }
        });
    });
}

function writeDifferenceToFile(localCandle, coinbaseCandle, interval) {
    const content = `Interval: ${interval}분\n` +
                    `localCandle: { tms: ${localCandle.tms}, op: ${localCandle.op}, cp: ${localCandle.cp}, hp: ${localCandle.hp}, lp: ${localCandle.lp}, tv: ${localCandle.tv} }\n` +
                    `coinbaseCandle: { tms: ${coinbaseCandle[0]}, op: ${coinbaseCandle[3]}, cp: ${coinbaseCandle[4]}, hp: ${coinbaseCandle[2]}, lp: ${coinbaseCandle[1]}, tv: ${coinbaseCandle[5]} }\n\n`;

    fs.appendFileSync('candle_differences.txt', content);
}

function compareCandles(localCandles, coinbaseCandles, interval) {
    let differences = 0;
    const coinbaseMap = new Map(coinbaseCandles.map(candle => [candle[0], candle]));

    for (const localCandle of localCandles) {
        const coinbaseCandle = coinbaseMap.get(localCandle.tms);
        if (coinbaseCandle) {
            if (
                !isApproximatelyEqual(localCandle.op, coinbaseCandle[3]) ||
                !isApproximatelyEqual(localCandle.hp, coinbaseCandle[2]) ||
                !isApproximatelyEqual(localCandle.lp, coinbaseCandle[1]) ||
                !isApproximatelyEqual(localCandle.cp, coinbaseCandle[4]) ||
                !isApproximatelyEqual(localCandle.tv, coinbaseCandle[5])
            ) {
                differences++;
                writeDifferenceToFile(localCandle, coinbaseCandle, interval);
            }
        } else {
            differences++;
            writeDifferenceToFile(localCandle, [localCandle.tms, null, null, null, null, null], interval);
        }
    }

    console.log(`${interval}분 캔들: ${differences}개의 차이가 발견되었습니다.`);
}

async function checkCandleIntegrity(interval) {
    console.log(`${interval}분 캔들 데이터 무결성 검사 시작...`);

    const localCandles = await getLocalCandles(interval);

    if (localCandles.length === 0) {
        console.log(`로컬 ${interval}분 캔들: 존재하지 않습니다.`);
        return;
    }

    const startTime = localCandles[0].tms;
    const endTime = localCandles[localCandles.length - 1].tms;

    const coinbaseCandles = await fetchCoinbaseCandles(interval, startTime, endTime);

    if (coinbaseCandles.length === 0) {
        console.log(`코인베이스 ${interval}분 캔들: 존재하지 않습니다.`);
        return;
    }

    compareCandles(localCandles, coinbaseCandles, interval);
}

async function main() {
    try {
        db = await connectToDatabase();

        for (const interval of CANDLE_INTERVALS.slice(1)) { // 3분부터 시작
            await checkCandleIntegrity(interval);
            console.log()
        }

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
