const ws = new WebSocket('ws://localhost:8080');

ws.addEventListener('open', () => {
    console.log('WebSocket 연결이 열렸습니다.');
});

ws.addEventListener('message', (event) => {
    console.log('서버로부터 받은 메시지:', event.data);
});

ws.addEventListener('close', () => {
    console.log('WebSocket 연결이 닫혔습니다.');
});
