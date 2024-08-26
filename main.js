const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();

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

// candles 테이블 생성 및 unique index 추가
async function initializeDatabase() {
    try {
        await connectToDatabase();
        await new Promise((resolve, reject) => {
            db.serialize(() => {
                db.run(`CREATE TABLE IF NOT EXISTS candles (
                    code TEXT,
                    timestamp INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL
                )`, (err) => {
                    if (err) reject(err);
                });

                // unique index 추가
                db.run(`CREATE UNIQUE INDEX IF NOT EXISTS idx_code_timestamp ON candles(code, timestamp)`, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        });
        console.log('데이터베이스 초기화 완료');
    } catch (error) {
        console.error('데이터베이스 초기화 오류:', error);
        process.exit(1);
    }
}

// 데이터베이스 초기화 실행
initializeDatabase();
const ws = new WebSocket('wss://api.upbit.com/websocket/v1');

const candles = {};
const candleDuration = 1; // 캔들 기간 (분)

const request = [
    {ticket: 'test'},
    {type: 'trade', codes: ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']},
];

ws.addEventListener('open', () => {
    console.log('업비트 WebSocket에 연결되었습니다.');
    ws.send(JSON.stringify(request));
});

ws.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);

    if (data.type === 'trade') {
        const {code, trade_timestamp, trade_price} = data;
        
        // 캔들 키 생성 (코인-기간)
        const candleKey = `${code}-${candleDuration}`;

        // 캔들 초기화
        if (!candles[candleKey]) {
            candles[candleKey] = {
                code,
                duration: candleDuration,
                open: trade_price,
                high: trade_price,
                low: trade_price,
                close: trade_price,
                timestamp: trade_timestamp,
            };
        }

        // 캔들 업데이트
        const candle = candles[candleKey];
        candle.close = trade_price;
        candle.high = Math.max(candle.high, trade_price);
        candle.low = Math.min(candle.low, trade_price);

        // 1분 캔들 기간 경과 시 OHLC 데이터 출력하고 새로운 캔들로 초기화
        if (candleDuration === 1 && trade_timestamp >= candle.timestamp + 60000) {
            console.log('1분 캔들 OHLC:', {
                code: candle.code,
                timestamp: candle.timestamp,
                open: candle.open,
                high: candle.high,
                low: candle.low,
                close: candle.close,
            });

            // OHLC 데이터를 SQLite에 저장
            db.run(`INSERT INTO candles (code, timestamp, open, high, low, close)
                    VALUES (?, ?, ?, ?, ?, ?)`,
                [candle.code, candle.timestamp, candle.open, candle.high, candle.low, candle.close],
                (err) => {
                    if (err) {
                        console.error('데이터베이스 저장 오류:', err);
                    }
                });

            candles[candleKey] = {
                code,
                duration: candleDuration,
                open: trade_price,
                high: trade_price,
                low: trade_price,
                close: trade_price,
                timestamp: trade_timestamp,
            };
        }
    }
});

ws.addEventListener('close', () => {
    console.log('업비트 WebSocket 연결이 닫혔습니다.');
    db.close(); // 데이터베이스 연결 종료
});

// 5초마다 OHLC 데이터를 SQLite에 저장
setInterval(() => {
    for (const candleKey in candles) {
        const candle = candles[candleKey];
        db.run(`INSERT OR REPLACE INTO candles (code, timestamp, open, high, low, close)
                VALUES (?, ?, ?, ?, ?, ?)`,
            [candle.code, candle.timestamp, candle.open, candle.high, candle.low, candle.close],
            (err) => {
                if (err) {
                    console.error('데이터베이스 저장 오류:', err);
                }
            });
    }
}, 5000);
