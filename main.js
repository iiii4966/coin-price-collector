const ws = new WebSocket('wss://api.upbit.com/websocket/v1');

const request = [
    {ticket: 'test'},
    {type: 'ticker', codes: ['KRW-BTC']},
];

ws.addEventListener('open', () => {
    console.log('업비트 WebSocket에 연결되었습니다.');
    ws.send(JSON.stringify(request));
});

ws.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    console.log('업비트로부터 받은 데이터:', data);
});

ws.addEventListener('close', () => {
    console.log('업비트 WebSocket 연결이 닫혔습니다.');
});
