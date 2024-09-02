const sqlite3 = require('sqlite3').verbose();

const CANDLE_INTERVALS = [3, 5, 10, 15, 30, 60, 240];
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

function getStartTime(timestamp, interval) {
    if (interval === 240) {
        // 4시간 캔들의 경우 UTC 기준 0, 4, 8, 12, 16, 20시에 맞춤
        const date = new Date(timestamp * 1000);
        const hours = date.getUTCHours();
        const fourHourBlock = Math.floor(hours / 4);
        date.setUTCHours(fourHourBlock * 4, 0, 0, 0);
        return Math.floor(date.getTime() / 1000);
    }
    return Math.floor(timestamp / (interval * 60)) * (interval * 60);
}

async function aggregateCandles(interval) {
    const currentTime = Math.floor(Date.now() / 1000);
    const startTime = getStartTime(currentTime, interval) - interval * 60;

    const baseTables = interval === 240 ? 'candles_60' : 'candles'

    return new Promise((resolve, reject) => {
        const sql = `
            SELECT code, 
                   tms,
                   op,
                   hp,
                   lp,
                   cp,
                   tv
            FROM ${baseTables}
            WHERE tms >= ? AND tms < ? AND code = 'BTC-USD'
        `;

        const formatTime = (timestamp) => {
            const date = new Date(timestamp * 1000);
            return date.toISOString().replace('T', ' ').substr(0, 19) + ' UTC';
        };
        console.log(`Aggregating ${interval}-minute candles from ${formatTime(startTime)} to ${formatTime(currentTime)}`);

        db.all(sql, [startTime, currentTime], (err, rows) => {
            if (err) {
                console.error(`${interval}분 캔들 집계 오류:`, err.message);
                reject(err);
            } else {
                const groupedCandles = {};
                rows.forEach(row => {
                    const intervalStart = Math.floor(row.tms / (interval * 60)) * (interval * 60);
                    const key = `${row.code}-${intervalStart}`;
                    if (!groupedCandles[key]) {
                        groupedCandles[key] = {
                            code: row.code,
                            tms: intervalStart,
                            op: row.op,
                            hp: row.hp,
                            lp: row.lp,
                            cp: row.cp,
                            tv: row.tv
                        };
                    } else {
                        groupedCandles[key].hp = Math.max(groupedCandles[key].hp, row.hp);
                        groupedCandles[key].lp = Math.min(groupedCandles[key].lp, row.lp);
                        groupedCandles[key].cp = row.cp;
                        groupedCandles[key].tv += row.tv;
                    }
                });
                const processedRows = Object.values(groupedCandles);
                resolve(processedRows);
            }
        });
    });
}

async function saveAggregatedCandles(interval, candles) {
    return new Promise((resolve, reject) => {
        const sql = `INSERT OR REPLACE INTO candles_${interval} 
                     (code, tms, op, hp, lp, cp, tv) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)`;

        db.serialize(() => {
            db.run('BEGIN TRANSACTION');

            const stmt = db.prepare(sql);
            for (const candle of candles) {
                stmt.run(
                    candle.code,
                    candle.tms,
                    candle.op,
                    candle.hp,
                    candle.lp,
                    candle.cp,
                    candle.tv
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
