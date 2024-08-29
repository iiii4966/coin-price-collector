const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();

const PRODUCT_ID = 'BTC-USD';
const CANDLE_INTERVAL = 60; // 1분 캔들

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
        const sql = `CREATE TABLE IF NOT EXISTS candles (
            product_id TEXT,
            timestamp INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (product_id, timestamp)
        )`;
        db.run(sql, (err) => {
            if (err) {
                console.error('테이블 생성 오류:', err.message);
                reject(err);
            } else {
                console.log('candles 테이블이 생성되었습니다.');
                resolve();
            }
        });
    });
}

function getCurrentTimestamp() {
    return Math.floor(Date.now() / 1000);
}

function getCandleStartTime(currentTime) {
    return Math.floor(currentTime / CANDLE_INTERVAL) * CANDLE_INTERVAL;
}

function initializeWebSocket() {
    ws = new WebSocket('wss://ws-feed.exchange.coinbase.com');

    ws.on('open', () => {
        console.log('Coinbase WebSocket에 연결되었습니다.');
        const subscribeMessage = {
            type: 'subscribe',
            product_ids: [PRODUCT_ID],
            channels: ['matches']
        };
        ws.send(JSON.stringify(subscribeMessage));
    });

    ws.on('close', () => {
        console.log('Coinbase WebSocket 연결이 닫혔습니다. 재연결 시도 중...');
        setTimeout(initializeWebSocket, 5000);
    });

    ws.on('error', (error) => {
        console.error('WebSocket 오류:', error);
    });

    ws.on('message', (data) => {
        const message = JSON.parse(data);
        if (message.type === 'match') {
            updateCandle(message);
        }
    });
}

function updateCandle(trade) {
    const currentTime = getCurrentTimestamp();
    const candleStartTime = getCandleStartTime(currentTime);
    const price = parseFloat(trade.price);
    const size = parseFloat(trade.size);

    if (!currentCandle || currentCandle.timestamp !== candleStartTime) {
        if (currentCandle) {
            saveCandle(currentCandle);
        }
        currentCandle = {
            product_id: PRODUCT_ID,
            timestamp: candleStartTime,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: size
        };
    } else {
        currentCandle.high = Math.max(currentCandle.high, price);
        currentCandle.low = Math.min(currentCandle.low, price);
        currentCandle.close = price;
        currentCandle.volume += size;
    }
}

function saveCandle(candle) {
    const sql = `INSERT OR REPLACE INTO candles (product_id, timestamp, open, high, low, close, volume) 
                 VALUES (?, ?, ?, ?, ?, ?, ?)`;
    const values = [candle.product_id, candle.timestamp, candle.open, candle.high, candle.low, candle.close, candle.volume];

    db.run(sql, values, (err) => {
        if (err) {
            console.error('캔들 저장 오류:', err.message);
        } else {
            console.log(`캔들 저장 완료: ${candle.product_id} - ${new Date(candle.timestamp * 1000)}`);
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
        }, CANDLE_INTERVAL * 1000);

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
