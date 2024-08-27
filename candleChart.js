let chart;

function createChart(data) {
    const ctx = document.getElementById('candleChart').getContext('2d');
    
    if (chart) {
        chart.destroy();
    }

    try {
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
                            unit: 'minute',
                            displayFormats: {
                                minute: 'HH:mm'
                            }
                        },
                        ticks: {
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 10
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: '가격 (KRW)'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                },
                elements: {
                    candlestick: {
                        width: function(ctx) {
                            const visiblePoints = ctx.chart.scales.x.max - ctx.chart.scales.x.min;
                            const barWidth = Math.max(1, Math.round(ctx.chart.width / visiblePoints));
                            return Math.min(barWidth, 15);  // 최대 너비를 15픽셀로 제한
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('차트 생성 오류:', error);
    }
}

function fetchData(interval) {
    fetch(`/api/candles/${interval}m`)
        .then(response => response.json())
        .then(data => {
            if (data && data.length > 0) {
                createChart(data);
            } else {
                console.error('데이터가 비어있습니다.');
            }
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
