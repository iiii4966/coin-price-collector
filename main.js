const WebSocket = require('ws');
const ws = new WebSocket('wss://api.upbit.com/websocket/v1');

const candles = {};
const candleDuration = 1; // 캔들 기간 (분)

const request = [
    {ticket: 'test'},
    {type: 'trade', codes: ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']},
];

ws.addEventListener('open', () => {
    console.log('업비트 WebSocket에 연결되었습니다.');
    ws.send(JSON.stringify(request));
});

ws.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);

    if (data.type === 'trade') {
        const {code, trade_timestamp, trade_price} = data;
        
        // 캔들 키 생성 (코인-기간)
        const candleKey = `${code}-${candleDuration}`;

        // 캔들 초기화
        if (!candles[candleKey]) {
            candles[candleKey] = {
                code,
                duration: candleDuration,
                open: trade_price,
                high: trade_price,
                low: trade_price,
                close: trade_price,
                timestamp: trade_timestamp,
            };
        }

        // 캔들 업데이트
        const candle = candles[candleKey];
        candle.close = trade_price;
        candle.high = Math.max(candle.high, trade_price);
        candle.low = Math.min(candle.low, trade_price);

        // 1분 캔들 기간 경과 시 OHLC 데이터 출력하고 새로운 캔들로 초기화
        if (candleDuration === 1 && trade_timestamp >= candle.timestamp + 60000) {
            console.log('1분 캔들 OHLC:', {
                code: candle.code,
                timestamp: candle.timestamp,
                open: candle.open,
                high: candle.high,
                low: candle.low,
                close: candle.close,
            });

            candles[candleKey] = {
                code,
                duration: candleDuration,
                open: trade_price,
                high: trade_price,
                low: trade_price,
                close: trade_price,
                timestamp: trade_timestamp,
            };
        }
    }
});

ws.addEventListener('close', () => {
    console.log('업비트 WebSocket 연결이 닫혔습니다.');
});
