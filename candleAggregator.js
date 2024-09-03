const { connectToDatabase, createTables, CANDLE_INTERVALS } = require('./dbUtils');

const UPDATE_INTERVAL = 5000; // 5초마다 업데이트
const DELETE_INTERVAL = 60000; // 1분마다 삭제 작업 수행
const MAX_CANDLES = 2000; // 유지할 최대 캔들 수

let db;
let deleteTimer = 0;

async function deleteOldCandles() {
    for (const interval of CANDLE_INTERVALS) {
        try {
            const sql = `DELETE FROM candles_${interval}
                         WHERE tms < (SELECT tms FROM candles_${interval}
                                      ORDER BY tms DESC
                                      LIMIT 1 OFFSET ?)`;

            await new Promise((resolve, reject) => {
                db.run(sql, [MAX_CANDLES], function(err) {
                    if (err) {
                        console.error(`${interval}분 캔들 삭제 중 오류 발생:`, err.message);
                        reject(err);
                    } else {
                        console.log(`${interval}분 캔들 ${this.changes}개 삭제됨`);
                        resolve();
                    }
                });
            });
        } catch (error) {
            console.error(`${interval}분 캔들 삭제 중 오류 발생:`, error);
        }
    }
}

function getStartTime(timestamp, interval) {
    const date = new Date(timestamp * 1000);
    if (interval === 10080) {
        // 1주 캔들의 경우 UTC 기준 월요일 00:00:00에 맞춤
        const day = date.getUTCDay();
        const diff = date.getUTCDate() - day + (day === 0 ? -6 : 1); // 월요일로 조정
        date.setUTCDate(diff);
        date.setUTCHours(0, 0, 0, 0);
        return Math.floor(date.getTime() / 1000);
    } else if (interval === 1440) {
        // 1일 캔들의 경우 UTC 기준 00:00:00에 맞춤
        date.setUTCHours(0, 0, 0, 0);
        return Math.floor(date.getTime() / 1000);
    } else if (interval === 240) {
        // 4시간 캔들의 경우 UTC 기준 0, 4, 8, 12, 16, 20시에 맞춤
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

    const baseTables = interval === 240 || interval === 1440 ? 'candles_60' :
                       interval === 10080 ? 'candles_1440' : 'candles_1'

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
            WHERE tms >= ? AND tms < ?
        `;

        console.log(`${interval}분 캔들 집계 중:`, new Date(startTime * 1000), '~', new Date(currentTime * 1000));

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
    for (const interval of CANDLE_INTERVALS.slice(1)) {
        try {
            const aggregatedCandles = await aggregateCandles(interval);
            await saveAggregatedCandles(interval, aggregatedCandles);
            console.log(`${interval}분 캔들 업데이트 완료 (${aggregatedCandles.length}개)`);
        } catch (error) {
            console.error(`${interval}분 캔들 업데이트 중 오류 발생:`, error);
        }
    }

    // 1분마다 오래된 캔들 삭제
    deleteTimer++;
    if (deleteTimer >= (DELETE_INTERVAL / UPDATE_INTERVAL)) {
        await deleteOldCandles();
        deleteTimer = 0;
    }

    console.log()
}

async function main() {
    try {
        db = await connectToDatabase();
        await createTables(db);

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
