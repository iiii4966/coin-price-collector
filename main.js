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
    const code = trade.product_id;

    if (!candles[code]) {
        candles[code] = {
            current: null,
            previous: null
        };
    }

    if (!candles[code].current || candles[code].current.timestamp !== candleStartTime) {
        if (candles[code].current) {
            candles[code].previous = candles[code].current;
            candles[code].previous.inserted = false;
        }
        candles[code].current = {
            code: code,
            tms: candleStartTime,
            op: price,
            hp: price,
            lp: price,
            cp: price,
            tv: size
        };
    } else {
        const candle = candles[code].current;
        candle.hp = Math.max(candle.hp, price);
        candle.lp = Math.min(candle.lp, price);
        candle.cp = price;
        candle.tv += size;
    }
}

function bulkSaveCandles(candles) {
    return new Promise((resolve, reject) => {
        const sql = `INSERT OR REPLACE INTO candles (code, tms, op, hp, lp, cp, tv) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)`;

        db.serialize(() => {
            db.run('BEGIN TRANSACTION');

            const stmt = db.prepare(sql);
            for (const candle of candles) {
                stmt.run(candle.code, candle.tms, candle.op, candle.hp, candle.lp, candle.cp, candle.tv);
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

            for (const code in candles) {
                const candle = candles[code];

                if (candle.current && candle.current.tv !== 0) {
                    candlesToSave.push(candle.current);
                }

                if (candle.previous &&
                    candle.previous.tms !== candle.current.tms &&
                    candle.previous.tv !== 0 &&
                    !candle.previous.inserted
                ) {
                    candlesToSave.push(candle.previous);
                    candle.previous.inserted = true;
                }

                if (candle.current.tms !== candleStartTime) {
                    candle.previous = candle.current
                    candle.previous.inserted = false;
                    candle.current = {
                        code: code,
                        tms: candleStartTime,
                        op: candle.current.cp,
                        hp: candle.current.cp,
                        lp: candle.current.cp,
                        cp: candle.current.cp,
                        tv: 0
                    };
                }
            }

            try {
                await bulkSaveCandles(candlesToSave);
                console.log(`${candlesToSave.length} candle saved.`);
                console.log(`${Object.keys(candles).length} code collected.`);
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
