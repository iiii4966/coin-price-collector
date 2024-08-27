const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const port = 3003;

app.use(express.static('.'));

const db = new sqlite3.Database('candles.db', (err) => {
    if (err) {
        console.error('데이터베이스 연결 오류:', err.message);
    } else {
        console.log('데이터베이스에 연결되었습니다.');
    }
});

app.get('/api/candles/:interval', (req, res) => {
    const interval = req.params.interval;
    const sql = `SELECT * FROM candles_${interval} ORDER BY timestamp DESC LIMIT 100`;

    db.all(sql, [], (err, rows) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        res.json(rows);
    });
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(port, () => {
    console.log(`서버가 http://localhost:3003 에서 실행 중입니다.`);
});

process.on('SIGINT', () => {
    console.log('서버를 종료합니다...');
    db.close((err) => {
        if (err) {
            console.error('데이터베이스 연결 종료 중 오류 발생:', err);
        } else {
            console.log('데이터베이스 연결이 안전하게 종료되었습니다.');
        }
        process.exit();
    });
});
