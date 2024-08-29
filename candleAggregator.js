const sqlite3 = require('sqlite3').verbose();

const CANDLE_INTERVALS = [3, 5, 10, 15, 30, 60];
const UPDATE_INTERVAL = 5000; // 5초마다 업데이트

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
    const promises = CANDLE_INTERVALS.map(interval => {
        return new Promise((resolve, reject) => {
            const sql = `CREATE TABLE IF NOT EXISTS candles_${interval} (
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
                    console.error(`테이블 생성 오류 (${interval}분):`, err.message);
                    reject(err);
                } else {
                    console.log(`candles_${interval} 테이블이 생성되었습니다.`);
                    resolve();
                }
            });
        });
    });

    await Promise.all(promises);
}

function getStartTime(timestamp, interval) {
    return Math.floor(timestamp / (interval * 60)) * (interval * 60);
}

async function aggregateCandles(interval) {
    const currentTime = Math.floor(Date.now() / 1000);
    const startTime = getStartTime(currentTime, interval) - interval * 60;

    return new Promise((resolve, reject) => {
        const sql = `
            SELECT product_id, 
                   timestamp / (? * 60) * (? * 60) as interval_start,
                   open as open,
                   high as high,
                   low as low,
                   close as close,
                   volume as volume
            FROM candles
            WHERE timestamp >= ? AND timestamp < ?
        `;
        console.log(sql, [interval, interval, new Date(startTime * 1000), currentTime])
        db.all(sql, [interval, interval, startTime, currentTime], (err, rows) => {
            if (err) {
                console.error(`${interval}분 캔들 집계 오류:`, err.message);
                reject(err);
            } else {
                // 추가 처리: 각 그룹의 첫 번째와 마지막 캔들에서 open과 close 값을 가져옵니다.
                const processedRows = rows.map(row => ({
                    product_id: row.product_id,
                    timestamp: row.interval_start,
                    open: row.open,
                    high: row.high,
                    low: row.low,
                    close: row.close,
                    volume: row.volume
                }));
                resolve(processedRows);
            }
        });
    });
}

async function saveAggregatedCandles(interval, candles) {
    return new Promise((resolve, reject) => {
        const sql = `INSERT OR REPLACE INTO candles_${interval} 
                     (product_id, timestamp, open, high, low, close, volume) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)`;

        db.serialize(() => {
            db.run('BEGIN TRANSACTION');

            const stmt = db.prepare(sql);
            for (const candle of candles) {
                stmt.run(
                    candle.product_id,
                    candle.timestamp,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume
                );
            }
            stmt.finalize();

            db.run('COMMIT', (err) => {
                if (err) {
                    console.error(`${interval}분 캔들 저장 오류:`, err.message);
                    db.run('ROLLBACK');
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    });
}

async function updateCandles() {
    for (const interval of CANDLE_INTERVALS) {
        try {
            const aggregatedCandles = await aggregateCandles(interval);
            await saveAggregatedCandles(interval, aggregatedCandles);
            console.log(`${interval}분 캔들 업데이트 완료 (${aggregatedCandles.length}개)`);
        } catch (error) {
            console.error(`${interval}분 캔들 업데이트 중 오류 발생:`, error);
        }
    }
}

async function main() {
    try {
        await connectToDatabase();
        await createTables();

        setInterval(updateCandles, UPDATE_INTERVAL);
    } catch (error) {
        console.error('초기화 중 오류 발생:', error);
        process.exit(1);
    }
}

main();

process.on('SIGINT', () => {
    console.log('프로그램을 종료합니다...');
    db.close((err) => {
        if (err) {
            console.error('데이터베이스 종료 오류:', err.message);
        } else {
            console.log('데이터베이스 연결이 안전하게 종료되었습니다.');
        }
        process.exit();
    });
});
