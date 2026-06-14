(function() {
    'use strict';

    let ws = null;
    let currentDb = null;
    let databases = [];
    let chartConnections = null;
    let chartTpsCache = null;
    let chartTables = null;
    let currentRange = '1h';

    async function init() {
        await loadDatabases();
        setupRangeButtons();
        connectWebSocket();
        if (currentDb) {
            loadHistory('1h');
            loadTables();
        }
    }

    async function loadDatabases() {
        try {
            const res = await fetch('/api/databases');
            databases = await res.json();
            renderTabs();
            if (databases.length > 0 && !currentDb) {
                currentDb = databases[0].id;
                renderTabs();
            }
        } catch(e) {
            console.error('Failed to load databases:', e);
        }
    }

    function renderTabs() {
        const container = document.getElementById('db-tabs');
        container.innerHTML = databases.map(db => {
            const statusClass = db.status === 'connected' ? 'ok' :
                               db.status === 'circuit_open' ? 'error' : 'warning';
            const active = db.id === currentDb ? 'active' : '';
            return `<div class="db-tab ${active}" data-id="${db.id}">
                <span class="tab-status ${statusClass}"></span>
                ${db.id}
                ${db.pg_version ? `<small>(PG${db.pg_version})</small>` : ''}
            </div>`;
        }).join('');

        container.querySelectorAll('.db-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                currentDb = tab.dataset.id;
                renderTabs();
                loadHistory(currentRange);
                loadTables();
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({type: 'subscribe', db_ids: [currentDb]}));
                }
            });
        });
    }

    function connectWebSocket() {
        ws = new WebSocket(WS_URL);

        ws.onopen = () => {
            updateWsStatus(true);
            if (currentDb) {
                ws.send(JSON.stringify({type: 'subscribe', db_ids: [currentDb]}));
            }
        };

        ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            if (msg.type === 'metrics_update' && msg.db_id === currentDb) {
                updateCards(msg.data);
            }
        };

        ws.onclose = () => {
            updateWsStatus(false);
            setTimeout(connectWebSocket, 3000);
        };

        ws.onerror = () => { ws.close(); };
    }

    function updateWsStatus(connected) {
        const dot = document.querySelector('.status-dot');
        const text = document.querySelector('.status-text');
        if (connected) {
            dot.className = 'status-dot connected';
            text.textContent = '已连接';
        } else {
            dot.className = 'status-dot disconnected';
            text.textContent = '未连接';
        }
    }

    function updateCards(data) {
        document.getElementById('val-active-conn').textContent = data.active_connections ?? '--';
        document.getElementById('val-total-conn').textContent = `总计: ${data.total_connections ?? '--'}`;
        document.getElementById('val-cache-hit').textContent = `${data.cache_hit_ratio ?? '--'}%`;
        document.getElementById('val-tps').textContent = data.tps != null ? data.tps.toFixed(1) : '--';
        document.getElementById('val-db-size').textContent = formatBytes(data.db_size_bytes);
    }

    function formatBytes(bytes) {
        if (bytes == null || bytes === 0) return '--';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let i = 0;
        let val = bytes;
        while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
        return val.toFixed(1) + ' ' + units[i];
    }

    function formatTime(ts) {
        return new Date(ts * 1000).toLocaleTimeString('zh-CN', {hour: '2-digit', minute: '2-digit'});
    }

    async function loadHistory(range) {
        if (!currentDb) return;
        currentRange = range;
        try {
            const res = await fetch(`/api/metrics/${currentDb}/history?range=${range}`);
            const json = await res.json();
            renderConnectionsChart(json.data);
            renderTpsCacheChart(json.data);
        } catch(e) {
            console.error('Failed to load history:', e);
        }
    }

    function renderConnectionsChart(data) {
        const ctx = document.getElementById('chart-connections').getContext('2d');
        const labels = data.map(d => formatTime(d.timestamp));
        const active = data.map(d => d.active_connections);
        const idle = data.map(d => d.idle_connections);
        const total = data.map(d => d.total_connections);

        if (chartConnections) chartConnections.destroy();
        chartConnections = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    { label: '活跃', data: active, borderColor: '#4fc3f7', borderWidth: 2, fill: false, pointRadius: 0, tension: 0.3 },
                    { label: '空闲', data: idle, borderColor: '#81c784', borderWidth: 2, fill: false, pointRadius: 0, tension: 0.3 },
                    { label: '总计', data: total, borderColor: '#ffb74d', borderWidth: 1, borderDash: [4,4], fill: false, pointRadius: 0, tension: 0.3 },
                ]
            },
            options: chartOptions()
        });
    }

    function renderTpsCacheChart(data) {
        const ctx = document.getElementById('chart-tps-cache').getContext('2d');
        const labels = data.map(d => formatTime(d.timestamp));
        const tps = data.map(d => d.tps);
        const cache = data.map(d => d.cache_hit_ratio);

        if (chartTpsCache) chartTpsCache.destroy();
        chartTpsCache = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    { label: 'TPS', data: tps, borderColor: '#ce93d8', borderWidth: 2, fill: false, pointRadius: 0, tension: 0.3, yAxisID: 'y' },
                    { label: '缓存命中率(%)', data: cache, borderColor: '#4caf50', borderWidth: 2, fill: false, pointRadius: 0, tension: 0.3, yAxisID: 'y1' },
                ]
            },
            options: {
                ...chartOptions(),
                scales: {
                    x: { ticks: { color: '#657786', maxTicksLimit: 12 }, grid: { color: '#1e2d3d' } },
                    y: { position: 'left', ticks: { color: '#ce93d8' }, grid: { color: '#1e2d3d' }, title: { display: true, text: 'TPS', color: '#ce93d8' } },
                    y1: { position: 'right', min: 0, max: 100, ticks: { color: '#4caf50' }, grid: { drawOnChartArea: false }, title: { display: true, text: '命中率 %', color: '#4caf50' } },
                }
            }
        });
    }

    async function loadTables() {
        if (!currentDb) return;
        try {
            const res = await fetch(`/api/metrics/${currentDb}/tables`);
            const json = await res.json();
            renderTablesChart(json.tables);
        } catch(e) {
            console.error('Failed to load tables:', e);
        }
    }

    function renderTablesChart(tables) {
        const ctx = document.getElementById('chart-tables').getContext('2d');
        const labels = tables.map(t => `${t.schema_name}.${t.table_name}`);
        const sizes = tables.map(t => t.total_size / (1024*1024));

        if (chartTables) chartTables.destroy();
        chartTables = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: '总大小 (MB)',
                    data: sizes,
                    backgroundColor: '#4fc3f7aa',
                    borderColor: '#4fc3f7',
                    borderWidth: 1
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    x: { ticks: { color: '#657786' }, grid: { color: '#1e2d3d' } },
                    y: { ticks: { color: '#8899a6', font: { size: 11 } }, grid: { display: false } },
                }
            }
        });
    }

    function chartOptions() {
        return {
            responsive: true,
            interaction: { intersect: false, mode: 'index' },
            plugins: { legend: { labels: { color: '#8899a6', font: { size: 12 } } } },
            scales: {
                x: { ticks: { color: '#657786', maxTicksLimit: 12 }, grid: { color: '#1e2d3d' } },
                y: { ticks: { color: '#657786' }, grid: { color: '#1e2d3d' } }
            }
        };
    }

    function setupRangeButtons() {
        document.querySelectorAll('.range-selector').forEach(selector => {
            selector.querySelectorAll('.range-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    selector.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    loadHistory(btn.dataset.range);
                });
            });
        });
    }

    // Load circuit breaker status periodically
    async function loadCircuitStatus() {
        try {
            const res = await fetch('/api/status');
            const json = await res.json();
            const container = document.getElementById('circuit-status');
            container.innerHTML = Object.entries(json.circuit_breakers).map(([id, cb]) => {
                return `<div class="circuit-badge ${cb.state}">${id}: ${cb.state.toUpperCase()}</div>`;
            }).join('');
        } catch(e) {}
    }

    setInterval(loadCircuitStatus, 10000);
    setInterval(() => { if (currentDb) loadTables(); }, 60000);

    document.addEventListener('DOMContentLoaded', init);
})();
