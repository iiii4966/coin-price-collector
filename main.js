const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');

const CANDLE_INTERVAL = 60; // 1분 캔들
const SAVE_INTERVAL = 5; // 5초마다 저장

let ws;
let currentCandles = {};
let db;

async function getCoinbaseProducts() {
    try {
        const response = await axios.get('https://api.exchange.coinbase.com/products');
        const products = response.data;

        const filteredProducts = products.filter(product => 
            product.quote_currency === 'USD' && 
            (product.status === 'online' || product.status === 'offline')
        );

        console.log(`필터링된 상품 수: ${filteredProducts.length}`);
        return filteredProducts.map(product => product.id);
    } catch (error) {
        console.error('API 요청 중 오류 발생:', error.message);
        return [];
    }
}

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

function initializeWebSocket(productIds) {
    ws = new WebSocket('wss://ws-feed.exchange.coinbase.com');

    ws.on('open', () => {
        console.log('Coinbase WebSocket에 연결되었습니다.');
        const subscribeMessage = {
            type: 'subscribe',
            product_ids: productIds,
            channels: ['matches']
        };
        ws.send(JSON.stringify(subscribeMessage));
    });

    ws.on('close', () => {
        console.log('Coinbase WebSocket 연결이 닫혔습니다. 재연결 시도 중...');
        setTimeout(() => initializeWebSocket(productIds), 5000);
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
    const productId = trade.product_id;

    if (!candles[productId]) {
        candles[productId] = {
            current: null,
            previous: null
        };
    }

    if (!candles[productId].current || candles[productId].current.timestamp !== candleStartTime) {
        if (candles[productId].current) {
            candles[productId].previous = candles[productId].current;
        }
        candles[productId].current = {
            product_id: productId,
            timestamp: candleStartTime,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: size
        };
    } else {
        const candle = candles[productId].current;
        candle.high = Math.max(candle.high, price);
        candle.low = Math.min(candle.low, price);
        candle.close = price;
        candle.volume += size;
    }
}

function bulkSaveCandles(candles) {
    return new Promise((resolve, reject) => {
        const sql = `INSERT OR REPLACE INTO candles (product_id, timestamp, open, high, low, close, volume) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)`;
        
        db.serialize(() => {
            db.run('BEGIN TRANSACTION');

            const stmt = db.prepare(sql);
            for (const candle of candles) {
                stmt.run(candle.product_id, candle.timestamp, candle.open, candle.high, candle.low, candle.close, candle.volume);
            }
            stmt.finalize();

            db.run('COMMIT', (err) => {
                if (err) {
                    console.error('Bulk 저장 오류:', err.message);
                    db.run('ROLLBACK');
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    });
}

async function main() {
    try {
        await connectToDatabase();
        await createTable();
        const productIds = await getCoinbaseProducts();
        initializeWebSocket(productIds);

        setInterval(async () => {
            const currentTime = getCurrentTimestamp();
            const candleStartTime = getCandleStartTime(currentTime);
            const candlesToSave = [];
            
            for (const productId in candles) {
                const productCandles = candles[productId];
                if (productCandles.current) {
                    candlesToSave.push(productCandles.current);
                }
                if (productCandles.previous && productCandles.previous.timestamp !== productCandles.current.timestamp) {
                    candlesToSave.push(productCandles.previous);
                }
                
                if (productCandles.current.timestamp !== candleStartTime) {
                    productCandles.previous = productCandles.current;
                    productCandles.current = {
                        product_id: productId,
                        timestamp: candleStartTime,
                        open: productCandles.current.close,
                        high: productCandles.current.close,
                        low: productCandles.current.close,
                        close: productCandles.current.close,
                        volume: 0
                    };
                }
            }
            
            try {
                await bulkSaveCandles(candlesToSave);
                console.log(`${candlesToSave.length}개의 종목 캔들 데이터가 저장되었습니다.`);
            } catch (error) {
                console.error('캔들 데이터 저장 중 오류 발생:', error);
            }
        }, SAVE_INTERVAL * 1000);

    } catch (error) {
        console.error('초기화 중 오류 발생:', error);
        process.exit(1);
    }
}

main();

process.on('SIGINT', async () => {
    console.log('프로그램을 종료합니다...');
    ws.close();

    const candlesToSave = Object.values(candles).flatMap(productCandles => 
        [productCandles.current, productCandles.previous].filter(Boolean)
    );
    if (candlesToSave.length > 0) {
        try {
            await bulkSaveCandles(candlesToSave);
            console.log(`${candlesToSave.length}개의 종목 캔들 데이터가 저장되었습니다.`);
        } catch (error) {
            console.error('종료 시 캔들 데이터 저장 중 오류 발생:', error);
        }
    }

    db.close((err) => {
        if (err) {
            console.error('데이터베이스 종료 오류:', err.message);
        } else {
            console.log('데이터베이스 연결이 안전하게 종료되었습니다.');
        }
        process.exit();
    });
});
