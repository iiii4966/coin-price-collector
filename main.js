const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();

const UPDATE_INTERVAL = 5000; // 5초마다 업데이트
const MARKET_CODE = 'KRW-BTC';
const CANDLE_INTERVALS = [
    { interval: 1, name: '1m' },
    { interval: 3, name: '3m' },
    { interval: 5, name: '5m' },
    { interval: 10, name: '10m' },
    { interval: 15, name: '15m' },
    { interval: 30, name: '30m' },
    { interval: 60, name: '1h' },
    { interval: 240, name: '4h' },
    { interval: 1440, name: '1d' },
    { interval: 10080, name: '1w' }
];

let ws;
let currentCandles = {};
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

async function createTables() {
    const promises = CANDLE_INTERVALS.map(({ name }) => {
        return new Promise((resolve, reject) => {
            const sql = `CREATE TABLE IF NOT EXISTS candles_${name} (
                code TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                PRIMARY KEY (code, timestamp)
            )`;
            db.run(sql, (err) => {
                if (err) {
                    console.error(`테이블 생성 오류 (${name}):`, err.message);
                    reject(err);
                } else {
                    console.log(`candles_${name} 테이블이 생성되었습니다.`);
                    resolve();
                }
            });
        });
    });
    return Promise.all(promises);
}

function getCurrentTimestamp() {
    return Math.floor(Date.now() / 1000);
}

function getCandleStartTime(currentTime, interval) {
    return Math.floor(currentTime / (interval * 60)) * (interval * 60);
}

function initializeWebSocket() {
    ws = new WebSocket('wss://api.upbit.com/websocket/v1');

    ws.on('open', () => {
        console.log('업비트 WebSocket에 연결되었습니다.');
        const request = [
            { ticket: 'test' },
            { type: 'trade', codes: [MARKET_CODE] }
        ];
        ws.send(JSON.stringify(request));
    });

    ws.on('close', () => {
        console.log('업비트 WebSocket 연결이 닫혔습니다. 재연결 시도 중...');
        setTimeout(initializeWebSocket, 5000);
    });

    ws.on('error', (error) => {
        console.error('WebSocket 오류:', error);
    });

    ws.on('message', (data) => {
        const trade = JSON.parse(data);
        updateCandles(trade);
    });
}

function updateCandles(trade) {
    const currentTime = getCurrentTimestamp();
    
    CANDLE_INTERVALS.forEach(({ interval, name }) => {
        const candleStartTime = getCandleStartTime(currentTime, interval);
        
        if (!currentCandles[name] || currentCandles[name].timestamp !== candleStartTime) {
            if (currentCandles[name]) {
                saveCandle(currentCandles[name], name);
            }
            currentCandles[name] = {
                code: MARKET_CODE,
                timestamp: candleStartTime,
                open: trade.trade_price,
                high: trade.trade_price,
                low: trade.trade_price,
                close: trade.trade_price
            };
        } else {
            currentCandles[name].high = Math.max(currentCandles[name].high, trade.trade_price);
            currentCandles[name].low = Math.min(currentCandles[name].low, trade.trade_price);
            currentCandles[name].close = trade.trade_price;
        }
    });
}

function saveCandle(candle, intervalName) {
    const sql = `INSERT OR REPLACE INTO candles_${intervalName} (code, timestamp, open, high, low, close) 
                 VALUES (?, ?, ?, ?, ?, ?)`;
    const values = [candle.code, candle.timestamp, candle.open, candle.high, candle.low, candle.close];

    db.run(sql, values, (err) => {
        if (err) {
            console.error(`캔들 저장 오류 (${intervalName}):`, err.message);
        } else {
            console.log(`캔들 저장 완료 (${intervalName}): ${candle.code} - ${new Date(candle.timestamp * 1000)}`);
        }
    });
}

async function main() {
    try {
        await connectToDatabase();
        await createTables();
        initializeWebSocket();

        setInterval(() => {
            CANDLE_INTERVALS.forEach(({ name }) => {
                if (currentCandles[name]) {
                    saveCandle(currentCandles[name], name);
                }
            });
        }, UPDATE_INTERVAL);

    } catch (error) {
        console.error('초기화 중 오류 발생:', error);
        process.exit(1);
    }
}

main();

process.on('SIGINT', () => {
    console.log('프로그램을 종료합니다...');
    CANDLE_INTERVALS.forEach(({ name }) => {
        if (currentCandles[name]) {
            saveCandle(currentCandles[name], name);
        }
    });
    ws.close();
    db.close((err) => {
        if (err) {
            console.error('데이터베이스 종료 오류:', err.message);
        } else {
            console.log('데이터베이스 연결이 안전하게 종료되었습니다.');
        }
        process.exit();
    });
});
