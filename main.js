const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();

const UPDATE_INTERVAL = 5000; // 5초마다 업데이트
const MARKET_CODE = 'KRW-BTC';

let ws;
let currentCandle = null;
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
        const sql = `CREATE TABLE IF NOT EXISTS candles_1m (
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
                console.error('테이블 생성 오류:', err.message);
                reject(err);
            } else {
                console.log('candles_1m 테이블이 생성되었습니다.');
                resolve();
            }
        });
    });
}

function getCurrentTimestamp() {
    return Math.floor(Date.now() / 1000);
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
        updateCandle(trade);
    });
}

function updateCandle(trade) {
    const currentTime = getCurrentTimestamp();
    const candleStartTime = Math.floor(currentTime / 60) * 60;

    if (!currentCandle || currentCandle.timestamp !== candleStartTime) {
        if (currentCandle) {
            saveCandle(currentCandle);
        }
        currentCandle = {
            code: MARKET_CODE,
            timestamp: candleStartTime,
            open: trade.trade_price,
            high: trade.trade_price,
            low: trade.trade_price,
            close: trade.trade_price
        };
    } else {
        currentCandle.high = Math.max(currentCandle.high, trade.trade_price);
        currentCandle.low = Math.min(currentCandle.low, trade.trade_price);
        currentCandle.close = trade.trade_price;
    }
}

function saveCandle(candle) {
    const sql = `INSERT OR REPLACE INTO candles_1m (code, timestamp, open, high, low, close) 
                 VALUES (?, ?, ?, ?, ?, ?)`;
    const values = [candle.code, candle.timestamp, candle.open, candle.high, candle.low, candle.close];

    db.run(sql, values, (err) => {
        if (err) {
            console.error('캔들 저장 오류:', err.message);
        } else {
            console.log(`캔들 저장 완료: ${candle.code} - ${new Date(candle.timestamp * 1000)}`);
        }
    });
}

async function main() {
    try {
        await connectToDatabase();
        await createTable();
        initializeWebSocket();

        setInterval(() => {
            if (currentCandle) {
                saveCandle(currentCandle);
            }
        }, UPDATE_INTERVAL);

    } catch (error) {
        console.error('초기화 중 오류 발생:', error);
        process.exit(1);
    }
}

main();

process.on('SIGINT', () => {
    console.log('프로그램을 종료합니다...');
    if (currentCandle) {
        saveCandle(currentCandle);
    }
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
