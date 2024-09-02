const sqlite3 = require('sqlite3').verbose();

const CANDLE_INTERVALS = [1, 3, 5, 10, 15, 30, 60, 240, 1440, 10080];

function connectToDatabase(dbName = 'candles.db') {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(dbName, (err) => {
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

async function createTables(db) {
    const promises = CANDLE_INTERVALS.map(interval => {
        return new Promise((resolve, reject) => {
            const sql = `CREATE TABLE IF NOT EXISTS candles_${interval} (
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

module.exports = {
    connectToDatabase,
    createTables,
    CANDLE_INTERVALS
};
