const WebSocket = require('ws');
const axios = require('axios');
const { Worker } = require('worker_threads');

const dbWorker = new Worker('./dbWorker.js');
const UPDATE_INTERVAL = 5000; // 5초마다 업데이트

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

// 모든 캔들 업데이트 및 DB에 저장
function updateAllCandles() {
    const currentTime = getCurrentTimestamp();
    const candlesToUpdate = {};

    candleDurations.forEach(duration => {
        candlesToUpdate[duration] = Object.values(candles[duration]).filter(candle => 
            candle.lastUpdated <= currentTime - duration * 60
        );
        
        // 업데이트된 캔들 제거
        candlesToUpdate[duration].forEach(candle => {
            delete candles[duration][`${candle.code}-${candle.timestamp}`];
        });
    });

    // Worker에 데이터 전송
    if (Object.values(candlesToUpdate).some(arr => arr.length > 0)) {
        dbWorker.postMessage(candlesToUpdate);
    }
}

// 주기적으로 캔들 업데이트
const updateInterval = setInterval(updateAllCandles, UPDATE_INTERVAL);

// 프로그램 종료 시 Worker 종료 및 인터벌 정리
process.on('SIGINT', () => {
    console.log('메인: 프로그램을 종료합니다...');
    clearInterval(updateInterval);
    updateAllCandles(); // 마지막으로 모든 캔들 업데이트
    dbWorker.terminate().then(() => {
        console.log('메인: Worker가 안전하게 종료되었습니다.');
        process.exit();
    });
});
