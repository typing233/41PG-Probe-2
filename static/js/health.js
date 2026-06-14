(function() {
    'use strict';

    let databases = [];
    let currentDb = null;
    let currentRange = '24h';
    let chartHistory = null;
    let gaugeChart = null;

    async function init() {
        await loadDatabases();
        setupEventListeners();
        loadData();
    }

    async function loadDatabases() {
        try {
            const res = await fetch('/api/databases');
            databases = await res.json();
            const select = document.getElementById('db-select');
            select.innerHTML = databases.map(db =>
                `<option value="${db.id}">${db.id} (${db.host})</option>`
            ).join('');
            if (databases.length > 0) currentDb = databases[0].id;
        } catch(e) {
            console.error('Failed to load databases:', e);
        }
    }

    function setupEventListeners() {
        document.getElementById('db-select').addEventListener('change', (e) => {
            currentDb = e.target.value;
            loadData();
        });

        document.querySelectorAll('.range-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentRange = btn.dataset.range;
                loadHistory();
            });
        });
    }

    async function loadData() {
        if (!currentDb) return;
        await Promise.all([loadCurrentHealth(), loadHistory(), loadAlerts()]);
    }

    async function loadCurrentHealth() {
        try {
            const res = await fetch(`/api/health/${currentDb}/current`);
            if (!res.ok) {
                document.getElementById('gauge-value').textContent = '--';
                document.getElementById('dimensions-grid').innerHTML = '<div class="empty-state">等待数据采集</div>';
                document.getElementById('anomalies-container').innerHTML = '<div class="empty-state">当前无异常</div>';
                return;
            }
            const json = await res.json();
            const health = json.health;
            renderGauge(health.overall_score);
            renderDimensions(health.dimension_scores);
            renderAnomalies(health.anomalies);
        } catch(e) {
            console.error('Failed to load health:', e);
        }
    }

    function renderGauge(score) {
        document.getElementById('gauge-value').textContent = score != null ? score.toFixed(0) : '--';

        const ctx = document.getElementById('gauge-chart').getContext('2d');
        if (gaugeChart) gaugeChart.destroy();

        const color = score >= 80 ? '#4caf50' : score >= 60 ? '#ffb74d' : '#ef5350';
        gaugeChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                datasets: [{
                    data: [score || 0, 100 - (score || 0)],
                    backgroundColor: [color, '#1e2d3d'],
                    borderWidth: 0
                }]
            },
            options: {
                cutout: '75%',
                rotation: -90,
                circumference: 180,
                responsive: false,
                plugins: { legend: { display: false }, tooltip: { enabled: false } }
            }
        });
    }

    function renderDimensions(dimensions) {
        const grid = document.getElementById('dimensions-grid');
        if (!dimensions) { grid.innerHTML = ''; return; }

        const labels = {
            connections: '连接数', cache_hit: '缓存命中', tps_stability: 'TPS稳定性',
            replication_lag: '复制延迟', bloat: '表膨胀', index_health: '索引健康',
            slow_query_rate: '慢查询率'
        };

        grid.innerHTML = Object.entries(dimensions).map(([key, score]) => {
            const cls = score >= 80 ? 'dim-good' : score >= 60 ? 'dim-warn' : 'dim-bad';
            return `<div class="dim-card ${cls}">
                <div class="dim-score">${score.toFixed(0)}</div>
                <div class="dim-label">${labels[key] || key}</div>
            </div>`;
        }).join('');
    }

    function renderAnomalies(anomalies) {
        const container = document.getElementById('anomalies-container');
        if (!anomalies || anomalies.length === 0) {
            container.innerHTML = '<div class="empty-state">当前无异常</div>';
            return;
        }
        container.innerHTML = anomalies.map(a => {
            const cls = a.severity === 'critical' ? 'anomaly-critical' : 'anomaly-warning';
            return `<div class="anomaly-item ${cls}">
                <span class="anomaly-badge">${a.severity.toUpperCase()}</span>
                <span class="anomaly-msg">${escapeHtml(a.message)}</span>
            </div>`;
        }).join('');
    }

    async function loadHistory() {
        if (!currentDb) return;
        try {
            const res = await fetch(`/api/health/${currentDb}/history?range=${currentRange}`);
            const json = await res.json();
            renderHistoryChart(json.data);
        } catch(e) {
            console.error('Failed to load health history:', e);
        }
    }

    function renderHistoryChart(data) {
        const ctx = document.getElementById('chart-health-history').getContext('2d');
        if (chartHistory) chartHistory.destroy();

        if (!data || data.length === 0) {
            chartHistory = null;
            return;
        }

        const labels = data.map(d => formatTime(d.timestamp));
        const scores = data.map(d => d.overall_score);

        chartHistory = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: '综合评分',
                    data: scores,
                    borderColor: '#4fc3f7',
                    borderWidth: 2,
                    fill: true,
                    backgroundColor: 'rgba(79,195,247,0.1)',
                    pointRadius: 0,
                    tension: 0.3
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { labels: { color: '#8899a6' } } },
                scales: {
                    x: { ticks: { color: '#657786', maxTicksLimit: 12 }, grid: { color: '#1e2d3d' } },
                    y: { min: 0, max: 100, ticks: { color: '#657786' }, grid: { color: '#1e2d3d' } }
                }
            }
        });
    }

    async function loadAlerts() {
        if (!currentDb) return;
        try {
            const res = await fetch(`/api/health/${currentDb}/alerts?limit=50`);
            const json = await res.json();
            renderAlerts(json.alerts);
        } catch(e) {
            console.error('Failed to load alerts:', e);
        }
    }

    function renderAlerts(alerts) {
        const tbody = document.getElementById('alerts-tbody');
        if (!alerts || alerts.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#657786;padding:40px">暂无告警记录</td></tr>';
            return;
        }
        tbody.innerHTML = alerts.map(a => {
            const sevClass = a.severity === 'critical' ? 'severity-critical' : 'severity-warning';
            const ackBtn = a.acknowledged ? '<span class="ack-done">已确认</span>' :
                `<button class="btn btn-sm" onclick="ackAlert(${a.id})">确认</button>`;
            return `<tr class="${a.acknowledged ? 'row-acked' : ''}">
                <td>${formatTime(a.triggered_at)}</td>
                <td>${a.dimension}</td>
                <td><span class="badge ${sevClass}">${a.severity}</span></td>
                <td>${escapeHtml(a.message)}</td>
                <td>${ackBtn}</td>
            </tr>`;
        }).join('');
    }

    window.ackAlert = async function(alertId) {
        try {
            await fetch(`/api/health/${currentDb}/alerts/${alertId}/acknowledge`, {method: 'POST'});
            loadAlerts();
        } catch(e) {
            console.error('Acknowledge error:', e);
        }
    };

    function formatTime(ts) {
        if (!ts) return '--';
        return new Date(ts * 1000).toLocaleString('zh-CN', {
            month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'
        });
    }

    function escapeHtml(str) {
        if (!str) return '';
        return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    setInterval(loadData, 30000);
    document.addEventListener('DOMContentLoaded', init);
})();
