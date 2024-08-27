const WebSocket = require('ws');
const axios = require('axios');
const { Worker } = require('worker_threads');

const dbWorker = new Worker('./dbWorker.js');

dbWorker.on('message', (message) => {
    if (message.success) {
        console.log(`메인: Worker가 ${message.totalInserted}개의 캔들 데이터를 저장했습니다.`);
    } else {
        console.error('메인: Worker에서 오류 발생:', message.error);
    }
});

// WebSocket 초기화
initializeWebSocket();

let ws;
let request;
const candles = {};

const candleDurations = [1, 3, 5, 10, 15, 30, 60, 240, 1440, 10080]; // 캔들 기간 (분)
candleDurations.forEach(duration => candles[duration] = {});

let marketCodes = [];

async function getMarketCodes() {
    try {
        const response = await axios.get('https://api.upbit.com/v1/market/all?isDetails=false');
        marketCodes = response.data
            .filter(market => market.market.startsWith('KRW-'))
            .map(market => market.market);
        console.log(`총 ${marketCodes.length}개의 KRW 마켓 코드를 가져왔습니다.`);
    } catch (error) {
        console.error('마켓 코드 조회 중 오류 발생:', error);
        process.exit(1);
    }
}

function getCurrentTimestamp() {
    return Math.floor(Date.now() / 1000);
}

async function initializeWebSocket() {
    await getMarketCodes();

    request = [
        {ticket: 'test'},
        {type: 'trade', codes: marketCodes},
    ];

    connect();
}

function connect() {
    ws = new WebSocket('wss://api.upbit.com/websocket/v1');

    ws.addEventListener('open', () => {
        console.log('업비트 WebSocket에 연결되었습니다.');
        ws.send(JSON.stringify(request));
    });

    ws.addEventListener('close', () => {
        console.log('업비트 WebSocket 연결이 닫혔습니다. 재연결 시도 중...');
        setTimeout(connect, 5000);
    });

    ws.addEventListener('error', (error) => {
        console.error('WebSocket 오류:', error);
    });

    ws.addEventListener('message', (event) => {
        const data = JSON.parse(event.data);

        if (data.type === 'trade') {
            const {code, trade_timestamp, trade_price} = data;
            
            candleDurations.forEach(duration => {
                updateCandle(code, trade_timestamp, trade_price, duration);
            });
        }
    });

function updateCandle(code, trade_timestamp, trade_price, duration) {
    const currentTime = getCurrentTimestamp();
    const currentCandleStartTime = Math.floor(currentTime / (duration * 60)) * (duration * 60);
    const candleKey = `${code}-${currentCandleStartTime}`;

    if (!candles[duration][candleKey]) {
        candles[duration][candleKey] = {
            code,
            duration,
            open: trade_price,
            high: trade_price,
            low: trade_price,
            close: trade_price,
            timestamp: currentCandleStartTime,
            lastUpdated: currentTime,
        };
    } else {
        const candle = candles[duration][candleKey];
        candle.close = trade_price;
        candle.high = Math.max(candle.high, trade_price);
        candle.low = Math.min(candle.low, trade_price);
        candle.lastUpdated = currentTime;
    }

    // 이전 캔들 처리
    const previousCandleStartTime = currentCandleStartTime - duration * 60;
    const previousCandleKey = `${code}-${previousCandleStartTime}`;
    if (candles[duration][previousCandleKey]) {
        dbWorker.postMessage({ [duration]: [candles[duration][previousCandleKey]] });
        delete candles[duration][previousCandleKey];
    }
}
}

// 인터벌과 cleanOldCandles 함수 제거

// 프로그램 종료 시 Worker 종료
process.on('SIGINT', () => {
    console.log('메인: 프로그램을 종료합니다...');
    dbWorker.terminate().then(() => {
        console.log('메인: Worker가 안전하게 종료되었습니다.');
        process.exit();
    });
});
