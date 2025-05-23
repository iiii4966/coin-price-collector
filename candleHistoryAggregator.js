const { CANDLE_INTERVALS, connectToDatabase } = require('./dbUtils');

async function getAllCodes(db) {
    return new Promise((resolve, reject) => {
        const sql = `SELECT DISTINCT code FROM candles_1`;
        db.all(sql, [], (err, rows) => {
            if (err) {
                reject(err);
            } else {
                resolve(rows.map(row => row.code));
            }
        });
    });
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

async function aggregateAllCandles(db, interval, code) {
    const baseTables =
        interval === 3 || interval === 5 ? 'candles_1' :
        interval === 10 || interval === 15 ? 'candles_5' :
        interval === 30 ? 'candles_15' :
        interval === 60 ? 'candles_30' :
        interval === 240 ? 'candles_60' :
        interval === 1440 ? 'candles_240' :
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
            WHERE code = ?
            ORDER BY tms ASC
        `;

        db.all(sql, [code], (err, rows) => {
            if (err) {
                console.error(`${interval}분 캔들 집계 오류 (${code}):`, err.message);
                reject(err);
            } else {
                const groupedCandles = {};
                rows.forEach(row => {
                    const intervalStart = getStartTime(row.tms, interval);
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

async function saveAggregatedCandles(db, interval, candles) {
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

async function aggregateHistoricalCandles(db) {
    const codes = await getAllCodes(db);
    for (const interval of CANDLE_INTERVALS.slice(1)) {
        try {
            console.log(`${interval}분 캔들 집계 시작...`);
            for (const code of codes) {
                console.log(`  ${code} 처리 중...`);
                const aggregatedCandles = await aggregateAllCandles(db, interval, code);
                await saveAggregatedCandles(db, interval, aggregatedCandles);
                console.log(`  ${code} 완료 (${aggregatedCandles.length}개)`);
            }
            console.log(`${interval}분 캔들 집계 완료`);
        } catch (error) {
            console.error(`${interval}분 캔들 집계 중 오류 발생:`, error);
        }
    }
}

async function main() {
    try {
        const db = await connectToDatabase();
        await aggregateHistoricalCandles(db);
        db.close();
    } catch (error) {
        console.error('오류 발생:', error);
    }
}

module.exports = {
    aggregateHistoricalCandles,
    main
};

if (require.main === module) {
    main();
}
