const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');

const CANDLE_INTERVAL = 60; // 1-minute candles
const SAVE_INTERVAL = 5; // Save every 5 seconds

let ws;
let candles = {};
let db;

async function getCoinbaseProducts() {
    try {
        const response = await axios.get('https://api.exchange.coinbase.com/products');
        const products = response.data;

        const filteredProducts = products.filter(product =>
            product.quote_currency === 'USD' &&
            (product.status === 'online' || product.status === 'offline')
        );

        console.log(`Number of filtered products: ${filteredProducts.length}`);
        return filteredProducts.map(product => product.id);
    } catch (error) {
        console.error('Error occurred during API request:', error.message);
        return [];
    }
}

function connectToDatabase() {
    return new Promise((resolve, reject) => {
        db = new sqlite3.Database('candles.db', (err) => {
            if (err) {
                console.error('Database connection error:', err.message);
                reject(err);
            } else {
                console.log('Connected to the database.');
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
                console.error('Table creation error:', err.message);
                reject(err);
            } else {
                console.log('Candles table has been created.');
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
        console.log('Connected to Coinbase WebSocket.');
        const subscribeMessage = {
            type: 'subscribe',
            product_ids: productIds,
            channels: ['matches']
        };
        ws.send(JSON.stringify(subscribeMessage));
    });

    ws.on('close', () => {
        console.log('Coinbase WebSocket connection closed. Attempting to reconnect...');
        reconnectWithBackoff(productIds);
    });

    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
    });

    ws.on('message', (data) => {
        const message = JSON.parse(data);
        if (message.type === 'match') {
            updateCandle(message);
        }
    });
}

function reconnectWithBackoff(productIds, attempt = 1) {
    const timeouts = [0, 2, 4, 8, 16, 32, 60];
    const timeout = timeouts[Math.min(attempt - 1, timeouts.length - 1)] * 1000;

    console.log(`Reconnection attempt ${attempt}, trying again in ${timeout / 1000} seconds.`);

    setTimeout(() => {
        try {
            initializeWebSocket(productIds);
        } catch (error) {
            console.error('Reconnection failed:', error);
            if (attempt < timeouts.length) {
                reconnectWithBackoff(productIds, attempt + 1);
            } else {
                console.error('Maximum retry attempts exceeded. Exiting the program.');
                process.exit(1);
            }
        }
    }, timeout);
}

function updateCandle(trade) {
    const tradeTime = new Date(trade.time).getTime() / 1000;
    const candleStartTime = getCandleStartTime(tradeTime);
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
                    console.error('Bulk save error:', err.message);
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
                console.log(`${candlesToSave.length} product candle data have been saved.`);
            } catch (error) {
                console.error('Error occurred while saving candle data:', error);
            }
        }, SAVE_INTERVAL * 1000);

    } catch (error) {
        console.error('Error occurred during initialization:', error);
        process.exit(1);
    }
}

main();

process.on('SIGINT', async () => {
    console.log('Terminating the program...');
    ws.close();

    const candlesToSave = Object.values(candles).flatMap(productCandles =>
        [productCandles.current, productCandles.previous].filter(Boolean)
    );
    if (candlesToSave.length > 0) {
        try {
            await bulkSaveCandles(candlesToSave);
            console.log(`${candlesToSave.length} product candle data have been saved.`);
        } catch (error) {
            console.error('Error occurred while saving candle data during shutdown:', error);
        }
    }

    db.close((err) => {
        if (err) {
            console.error('Database closure error:', err.message);
        } else {
            console.log('Database connection has been safely closed.');
        }
        process.exit();
    });
});
