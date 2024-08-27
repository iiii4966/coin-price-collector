const { parentPort } = require('worker_threads');
const sqlite3 = require('sqlite3').verbose();

let db;

function connectToDatabase() {
    return new Promise((resolve, reject) => {
        db = new sqlite3.Database('candles.db', (err) => {
            if (err) {
                console.error('데이터베이스 연결 오류:', err.message);
                reject(err);
            } else {
                console.log('Worker: 데이터베이스에 연결되었습니다.');
                resolve(db);
            }
        });
    });
}

async function createTables() {
    const candleDurations = [1, 3, 5, 10, 15, 30, 60, 240, 1440, 10080];
    for (const duration of candleDurations) {
        await new Promise((resolve, reject) => {
            const sql = `CREATE TABLE IF NOT EXISTS candles_${duration} (
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
                    console.error(`Worker: 테이블 생성 오류 (candles_${duration}):`, err);
                    reject(err);
                } else {
                    console.log(`Worker: candles_${duration} 테이블이 생성되었습니다.`);
                    resolve();
                }
            });
        });
    }
}

function bulkInsertCandles(duration, candlesToInsert) {
    return new Promise((resolve, reject) => {
        const tableName = `candles_${duration}`;
        const placeholders = candlesToInsert.map(() => '(?, ?, ?, ?, ?, ?)').join(', ');
        const sql = `INSERT OR REPLACE INTO ${tableName} (code, timestamp, open, high, low, close) VALUES ${placeholders}`;
        const values = candlesToInsert.flatMap(candle => [candle.code, candle.timestamp, candle.open, candle.high, candle.low, candle.close]);

        db.run(sql, values, function(err) {
            if (err) {
                console.error(`Worker: Bulk insert 오류 (${tableName}):`, err);
                reject(err);
            } else {
                resolve(this.changes);
            }
        });
    });
}

async function processData(data) {
    db.run('BEGIN TRANSACTION');
    const insertPromises = [];

    for (const [duration, candles] of Object.entries(data)) {
        const candlesToInsert = Object.values(candles);
        if (candlesToInsert.length > 0) {
            insertPromises.push(bulkInsertCandles(duration, candlesToInsert));
        }
    }

    try {
        const results = await Promise.all(insertPromises);
        const totalInserted = results.reduce((sum, count) => sum + count, 0);
        db.run('COMMIT');
        console.log(`Worker: ${totalInserted}개의 캔들 데이터가 저장되었습니다.`);
        parentPort.postMessage({ success: true, totalInserted });
    } catch (error) {
        db.run('ROLLBACK');
        console.error('Worker: 데이터 저장 중 오류 발생:', error);
        parentPort.postMessage({ success: false, error: error.message });
    }
}

connectToDatabase()
    .then(() => createTables())
    .then(() => {
        console.log('Worker: 모든 테이블이 생성되었습니다.');
        parentPort.on('message', processData);
    })
    .catch((error) => {
        console.error('Worker: 초기화 중 오류 발생:', error);
        process.exit(1);
    });

process.on('SIGINT', () => {
    console.log('Worker: 프로그램을 종료합니다...');
    db.close((err) => {
        if (err) {
            console.error('Worker: 데이터베이스 연결 종료 중 오류 발생:', err);
        } else {
            console.log('Worker: 데이터베이스 연결이 안전하게 종료되었습니다.');
        }
        process.exit();
    });
});
