let chart;

function createChart(data) {
    const ctx = document.getElementById('candleChart').getContext('2d');
    
    if (chart) {
        chart.destroy();
    }

    chart = new Chart(ctx, {
        type: 'candlestick',
        data: {
            datasets: [{
                label: 'BTCKRW',
                data: data.map(d => ({
                    x: new Date(d.timestamp * 1000),
                    o: d.open,
                    h: d.high,
                    l: d.low,
                    c: d.close
                }))
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'minute'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: '가격 (KRW)'
                    }
                }
            }
        }
    });
}

function fetchData(interval) {
    fetch(`/api/candles/${interval}`)
        .then(response => response.json())
        .then(data => {
            createChart(data);
        })
        .catch(error => console.error('데이터 가져오기 오류:', error));
}

document.addEventListener('DOMContentLoaded', () => {
    const buttons = document.querySelectorAll('#interval-buttons button');
    buttons.forEach(button => {
        button.addEventListener('click', () => {
            const interval = button.getAttribute('data-interval');
            fetchData(interval);
        });
    });

    // 초기 로드 시 1분 캔들 데이터 표시
    fetchData(1);
});
