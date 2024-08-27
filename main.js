const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');

// SQLite 데이터베이스 연결
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

// 각 duration에 대한 테이블 생성
async function initializeDatabase() {
    try {
        await connectToDatabase();
        const createTablePromises = candleDurations.map(duration => {
            return new Promise((resolve, reject) => {
                const tableName = `candles_${duration}`;
                db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (
                    code TEXT,
                    timestamp INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    CONSTRAINT ${tableName}_PK PRIMARY KEY (code, timestamp)
                )`, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        });

        await Promise.all(createTablePromises);
        console.log('데이터베이스 초기화 완료');
    } catch (error) {
        console.error('데이터베이스 초기화 오류:', error);
        process.exit(1);
    }
}

// 데이터베이스 초기화 실행 후 WebSocket 초기화
initializeDatabase().then(() => {
    initializeWebSocket();
});

let ws;
let request;
const candles = {};

const candleDurations = [1, 3, 5, 10, 15, 30, 60, 240, 1440, 10080]; // 캔들 기간 (분)
candleDurations.forEach(duration => candles[duration] = {});

let marketCodes = [];

async function getMarketCodes() {
    try {
        const response = await axios.get('https://api.upbit.com/v1/market/all?isDetails=false');
        marketCodes = response.data
            .filter(market => market.market.startsWith('KRW-'))
            .map(market => market.market);
        console.log(`총 ${marketCodes.length}개의 KRW 마켓 코드를 가져왔습니다.`);
    } catch (error) {
        console.error('마켓 코드 조회 중 오류 발생:', error);
        process.exit(1);
    }
}

function getCurrentTimestamp() {
    return Math.floor(Date.now() / 1000);
}

async function initializeWebSocket() {
    await getMarketCodes();

    request = [
        {ticket: 'test'},
        {type: 'trade', codes: marketCodes},
    ];

    connect();
}

function connect() {
    ws = new WebSocket('wss://api.upbit.com/websocket/v1');

    ws.addEventListener('open', () => {
        console.log('업비트 WebSocket에 연결되었습니다.');
        ws.send(JSON.stringify(request));
    });

    ws.addEventListener('close', () => {
        console.log('업비트 WebSocket 연결이 닫혔습니다. 재연결 시도 중...');
        setTimeout(connect, 5000);
    });

    ws.addEventListener('error', (error) => {
        console.error('WebSocket 오류:', error);
    });

    ws.addEventListener('message', (event) => {
        const data = JSON.parse(event.data);

        if (data.type === 'trade') {
            const {code, trade_timestamp, trade_price} = data;
            
            candleDurations.forEach(duration => {
                updateCandle(code, trade_timestamp, trade_price, duration);
            });
        }
    });

function updateCandle(code, trade_timestamp, trade_price, duration) {
    const currentCandleStartTime = Math.floor(trade_timestamp / (duration * 60000)) * (duration * 60000);
    const candleKey = `${code}-${currentCandleStartTime}`;

    if (!candles[duration][candleKey]) {
        candles[duration][candleKey] = {
            code,
            duration,
            open: trade_price,
            high: trade_price,
            low: trade_price,
            close: trade_price,
            timestamp: currentCandleStartTime,
            lastUpdated: getCurrentTimestamp(),
        };
    } else {
        const candle = candles[duration][candleKey];
        candle.close = trade_price;
        candle.high = Math.max(candle.high, trade_price);
        candle.low = Math.min(candle.low, trade_price);
        candle.lastUpdated = getCurrentTimestamp();
    }
}
}

// Bulk insert 함수
function bulkInsertCandles(duration, candlesToInsert) {
    return new Promise((resolve, reject) => {
        const tableName = `candles_${duration}`;
        const placeholders = candlesToInsert.map(() => '(?, ?, ?, ?, ?, ?)').join(', ');
        const sql = `INSERT OR REPLACE INTO ${tableName} (code, timestamp, open, high, low, close) VALUES ${placeholders}`;
        const values = candlesToInsert.flatMap(candle => [candle.code, candle.timestamp, candle.open, candle.high, candle.low, candle.close]);

        db.run(sql, values, function(err) {
            if (err) {
                console.error(`Bulk insert 오류 (${tableName}):`, err);
                reject(err);
            } else {
                resolve(this.changes);
            }
        });
    });
}

// 5초마다 모든 OHLC 데이터를 SQLite에 저장
setInterval(async () => {
    const currentTime = getCurrentTimestamp();
    const insertPromises = [];

    db.run('BEGIN TRANSACTION');

    for (const duration of candleDurations) {
        const candlesToInsert = Object.values(candles[duration])
            .filter(candle => candle.lastUpdated < currentTime - 5);

        if (candlesToInsert.length > 0) {
            insertPromises.push(bulkInsertCandles(duration, candlesToInsert));
        }
    }

    try {
        const results = await Promise.all(insertPromises);
        const totalInserted = results.reduce((sum, count) => sum + count, 0);
        db.run('COMMIT');
        console.log(`${totalInserted}개의 캔들 데이터가 저장되었습니다.`);
    } catch (error) {
        db.run('ROLLBACK');
        console.error('데이터 저장 중 오류 발생:', error);
    }

    // 오래된 캔들 데이터 정리
    cleanOldCandles();
}, 5000);

function cleanOldCandles() {
    const currentTime = getCurrentTimestamp();
    candleDurations.forEach(duration => {
        for (const candleKey in candles[duration]) {
            const candle = candles[duration][candleKey];
            if (candle.lastUpdated < currentTime - duration * 60) {
                delete candles[duration][candleKey];
            }
        }
    });
}

// 프로그램 종료 시 데이터베이스 연결 종료
process.on('SIGINT', () => {
    console.log('프로그램을 종료합니다...');
    db.close((err) => {
        if (err) {
            console.error('데이터베이스 연결 종료 중 오류 발생:', err);
        } else {
            console.log('데이터베이스 연결이 안전하게 종료되었습니다.');
        }
        process.exit();
    });
});
