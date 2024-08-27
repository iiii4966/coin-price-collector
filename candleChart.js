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
                aspectRatio: 2,  // 차트의 가로:세로 비율을 2:1로 설정
                layout: {
                    padding: {
                        left: 10,
                        right: 30,
                        top: 20,
                        bottom: 10
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'minute',
                            displayFormats: {
                                minute: 'MM-DD HH:mm'
                            }
                        },
                        ticks: {
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 8
                        },
                        distribution: 'linear'
                    },
                    y: {
                        position: 'right',
                        title: {
                            display: true,
                            text: '가격 (KRW)'
                        },
                        ticks: {
                            callback: function(value, index, values) {
                                return value.toLocaleString() + ' ₩';
                            }
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const d = context.raw;
                                return [
                                    `시가: ${d.o.toLocaleString()} ₩`,
                                    `고가: ${d.h.toLocaleString()} ₩`,
                                    `저가: ${d.l.toLocaleString()} ₩`,
                                    `종가: ${d.c.toLocaleString()} ₩`
                                ];
                            }
                        }
                    }
                },
                elements: {
                    candlestick: {
                        width: function(ctx) {
                            const visiblePoints = ctx.chart.scales.x.max - ctx.chart.scales.x.min;
                            const availableWidth = ctx.chart.width - ctx.chart.chartArea.left - 30;
                            let barWidth = Math.max(1, Math.floor(availableWidth / visiblePoints));
                            barWidth = Math.min(barWidth, 30);  // 최대 너비를 30픽셀로 확장
                            return Math.max(barWidth, 3);  // 최소 너비를 3픽셀로 설정
                        }
                    }
                },
                animation: {
                    duration: 0  // 애니메이션 비활성화로 성능 향상
                },
                hover: {
                    animationDuration: 0  // 호버 애니메이션 비활성화
                },
                responsiveAnimationDuration: 0  // 반응형 애니메이션 비활성화
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
