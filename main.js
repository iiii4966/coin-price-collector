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

// candles 테이블 생성 및 PRIMARY KEY, UNIQUE 제약 조건 추가
async function initializeDatabase() {
    try {
        await connectToDatabase();
        await new Promise((resolve, reject) => {
            db.serialize(() => {
                db.run(`CREATE TABLE IF NOT EXISTS candles (
                    code TEXT,
                    timestamp INTEGER,
                    duration INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    CONSTRAINT Candle_PK PRIMARY KEY (code, timestamp, duration)
                )`, (err) => {
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

// 데이터베이스 초기화 실행 후 WebSocket 초기화
initializeDatabase().then(() => {
    initializeWebSocket();
});

let ws;
let request;
const candles = {};
candleDurations.forEach(duration => {
    candles[duration] = {};
});
const candleDurations = [1, 3, 5, 10, 15, 30, 60, 240, 1440, 10080]; // 캔들 기간 (분)

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

// 5초마다 모든 OHLC 데이터를 SQLite에 저장
setInterval(() => {
    const currentTime = getCurrentTimestamp();
    const promises = [];

    candleDurations.forEach(duration => {
        for (const candleKey in candles[duration]) {
            const candle = candles[duration][candleKey];
            if (candle.lastUpdated < currentTime - 5) {  // 5초 이상 업데이트되지 않은 캔들만 저장
                promises.push(new Promise((resolve, reject) => {
                    db.run(`INSERT OR REPLACE INTO candles (code, timestamp, duration, open, high, low, close)
                            VALUES (?, ?, ?, ?, ?, ?, ?)`,
                        [candle.code, candle.timestamp, candle.duration, candle.open, candle.high, candle.low, candle.close],
                        function(err) {
                            if (err) {
                                console.error('데이터베이스 저장 오류:', err);
                                reject(err);
                            } else {
                                resolve();
                            }
                        });
                }));
            }
        }
    });

    Promise.all(promises)
        .then(() => console.log(`${promises.length}개의 캔들 데이터가 저장되었습니다.`))
        .catch(error => console.error('데이터 저장 중 오류 발생:', error));

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
