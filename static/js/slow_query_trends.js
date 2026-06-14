(function() {
    'use strict';

    let databases = [];
    let currentDb = null;
    let currentRange = '24h';
    let currentDimension = 'fingerprint';
    let chartCount = null;
    let chartDuration = null;
    let chartDrilldown = null;

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
                loadData();
            });
        });

        document.querySelectorAll('.dim-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.dim-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentDimension = btn.dataset.dim;
                loadDimensionData();
            });
        });

        document.getElementById('btn-compare').addEventListener('click', runCompare);
    }

    async function loadData() {
        if (!currentDb) return;
        await Promise.all([loadTopPatterns(), loadTrendCharts(), loadDimensionData()]);
    }

    async function loadTopPatterns() {
        try {
            const res = await fetch(`/api/trends/${currentDb}/top?range=${currentRange}&limit=20`);
            const json = await res.json();
            renderTopPatterns(json.patterns);
        } catch(e) {
            console.error('Failed to load top patterns:', e);
        }
    }

    async function loadTrendCharts() {
        try {
            const res = await fetch(`/api/trends/${currentDb}?range=${currentRange}`);
            const json = await res.json();
            renderTrendCharts(json.data);
        } catch(e) {
            console.error('Failed to load trends:', e);
        }
    }

    async function loadDimensionData() {
        if (!currentDb) return;

        let url;
        if (currentDimension === 'fingerprint') {
            url = `/api/trends/${currentDb}/top?range=${currentRange}&limit=20`;
        } else if (currentDimension === 'user') {
            url = `/api/trends/${currentDb}/by-user?range=${currentRange}&limit=20`;
        } else if (currentDimension === 'client') {
            url = `/api/trends/${currentDb}/by-client?range=${currentRange}&top_n=20`;
        }

        try {
            const res = await fetch(url);
            const json = await res.json();
            renderDimensionTable(json);
        } catch(e) {
            console.error('Failed to load dimension data:', e);
        }
    }

    function renderTopPatterns(patterns) {
        const tbody = document.getElementById('top-patterns-tbody');
        if (!patterns || patterns.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#657786;padding:40px">暂无趋势数据</td></tr>';
            return;
        }
        tbody.innerHTML = patterns.map(p => `
            <tr class="clickable-row" data-dim="fingerprint" data-value="${p.fingerprint}">
                <td class="col-query"><div class="query-preview">${escapeHtml((p.query_pattern || '').substring(0, 120))}</div></td>
                <td>${(p.total_occurrences || 0).toLocaleString()}</td>
                <td>${(p.total_time || 0).toFixed(1)}s</td>
                <td>${(p.mean_duration || 0).toFixed(2)}s</td>
                <td>${(p.peak_duration || 0).toFixed(2)}s</td>
            </tr>
        `).join('');

        tbody.querySelectorAll('.clickable-row').forEach(row => {
            row.addEventListener('click', () => {
                drilldown(row.dataset.dim, row.dataset.value);
            });
        });
    }

    function renderDimensionTable(json) {
        const container = document.getElementById('dimension-table');
        const data = json.data || json.patterns || [];

        if (!data || data.length === 0) {
            container.innerHTML = '<div class="empty-state">暂无数据</div>';
            return;
        }

        let headers, rowFn;
        if (currentDimension === 'fingerprint') {
            headers = '<th>查询模式</th><th>出现次数</th><th>总耗时</th><th>平均耗时</th>';
            rowFn = p => `
                <tr class="clickable-row" data-dim="fingerprint" data-value="${p.fingerprint}">
                    <td class="col-query"><div class="query-preview">${escapeHtml((p.query_pattern || '').substring(0, 100))}</div></td>
                    <td>${(p.total_occurrences || 0).toLocaleString()}</td>
                    <td>${(p.total_time || 0).toFixed(1)}s</td>
                    <td>${(p.mean_duration || 0).toFixed(2)}s</td>
                </tr>`;
        } else if (currentDimension === 'user') {
            headers = '<th>用户</th><th>出现次数</th><th>总耗时</th><th>活跃小时数</th>';
            rowFn = p => `
                <tr class="clickable-row" data-dim="user" data-value="${escapeHtml(p.user)}">
                    <td><code>${escapeHtml(p.user)}</code></td>
                    <td>${(p.total_occurrences || 0).toLocaleString()}</td>
                    <td>${(p.total_time || 0).toFixed(1)}s</td>
                    <td>${p.hours_active || 0}</td>
                </tr>`;
        } else if (currentDimension === 'client') {
            headers = '<th>客户端 IP</th><th>出现次数</th><th>总耗时</th><th>活跃小时数</th>';
            rowFn = p => {
                const label = p.client === 'others'
                    ? `others (${p.collapsed_count || '?'} 个 IP 合并)`
                    : p.client;
                return `
                    <tr class="${p.client !== 'others' ? 'clickable-row' : ''}" data-dim="client" data-value="${escapeHtml(p.client)}">
                        <td><code>${escapeHtml(label)}</code></td>
                        <td>${(p.total_occurrences || 0).toLocaleString()}</td>
                        <td>${(p.total_time || 0).toFixed(1)}s</td>
                        <td>${p.hours_active || 0}</td>
                    </tr>`;
            };
        }

        container.innerHTML = `
            <table class="data-table">
                <thead><tr>${headers}</tr></thead>
                <tbody>${data.map(rowFn).join('')}</tbody>
            </table>`;

        container.querySelectorAll('.clickable-row').forEach(row => {
            row.addEventListener('click', () => {
                drilldown(row.dataset.dim, row.dataset.value);
            });
        });
    }

    async function drilldown(dimension, value) {
        if (!currentDb || !value || value === 'others') return;
        try {
            const params = new URLSearchParams({dimension, value, range: currentRange});
            const res = await fetch(`/api/trends/${currentDb}/drilldown?${params}`);
            const json = await res.json();
            renderDrilldownChart(json, dimension, value);

            // For client dimension, also fetch top fingerprints
            if (dimension === 'client') {
                const fpRes = await fetch(`/api/trends/${currentDb}/client-fingerprints?client=${encodeURIComponent(value)}&range=${currentRange}`);
                const fpJson = await fpRes.json();
                renderClientFingerprints(fpJson.fingerprints, value);
            }
        } catch(e) {
            console.error('Drilldown error:', e);
        }
    }

    function renderDrilldownChart(json, dimension, value) {
        const section = document.getElementById('drilldown-section');
        section.style.display = 'block';
        document.getElementById('drilldown-title').textContent =
            `${dimension === 'fingerprint' ? '指纹' : dimension === 'user' ? '用户' : '客户端'}: ${value.substring(0, 40)}`;

        const data = json.data || [];
        if (!data.length) return;

        const labels = data.map(d => formatHour(d.hour_bucket));
        const counts = data.map(d => d.occurrence_count || 0);
        const durations = data.map(d => d.total_duration || 0);

        const ctx = document.getElementById('chart-drilldown').getContext('2d');
        if (chartDrilldown) chartDrilldown.destroy();

        chartDrilldown = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    { label: '出现次数', data: counts, borderColor: '#4fc3f7', borderWidth: 2, fill: false, pointRadius: 1, tension: 0.3, yAxisID: 'y' },
                    { label: '总耗时(s)', data: durations, borderColor: '#ce93d8', borderWidth: 2, fill: false, pointRadius: 1, tension: 0.3, yAxisID: 'y1' },
                ]
            },
            options: {
                responsive: true,
                plugins: { legend: { labels: { color: '#8899a6' } } },
                scales: {
                    x: { ticks: { color: '#657786', maxTicksLimit: 12 }, grid: { color: '#1e2d3d' } },
                    y: { position: 'left', ticks: { color: '#4fc3f7' }, grid: { color: '#1e2d3d' }, title: { display: true, text: '次数', color: '#4fc3f7' } },
                    y1: { position: 'right', ticks: { color: '#ce93d8' }, grid: { drawOnChartArea: false }, title: { display: true, text: '耗时(s)', color: '#ce93d8' } },
                }
            }
        });
    }

    function renderTrendCharts(data) {
        if (!data || data.length === 0) return;

        const hourlyAgg = {};
        data.forEach(d => {
            const h = d.hour_bucket;
            if (!hourlyAgg[h]) hourlyAgg[h] = {count: 0, duration: 0};
            hourlyAgg[h].count += d.occurrence_count || 0;
            hourlyAgg[h].duration += d.total_duration || 0;
        });

        const sorted = Object.entries(hourlyAgg).sort((a, b) => a[0] - b[0]);
        const labels = sorted.map(([ts]) => formatHour(parseFloat(ts)));
        const counts = sorted.map(([, v]) => v.count);
        const durations = sorted.map(([, v]) => v.duration);

        const ctx1 = document.getElementById('chart-trend-count').getContext('2d');
        if (chartCount) chartCount.destroy();
        chartCount = new Chart(ctx1, {
            type: 'bar',
            data: { labels, datasets: [{ label: '出现次数', data: counts, backgroundColor: '#4fc3f7aa', borderColor: '#4fc3f7', borderWidth: 1 }] },
            options: chartOpts('慢查询出现次数')
        });

        const ctx2 = document.getElementById('chart-trend-duration').getContext('2d');
        if (chartDuration) chartDuration.destroy();
        chartDuration = new Chart(ctx2, {
            type: 'line',
            data: { labels, datasets: [{ label: '总耗时(s)', data: durations, borderColor: '#ce93d8', borderWidth: 2, fill: false, pointRadius: 0, tension: 0.3 }] },
            options: chartOpts('总耗时趋势')
        });
    }

    async function runCompare() {
        const r1s = new Date(document.getElementById('range1-start').value).getTime() / 1000;
        const r1e = new Date(document.getElementById('range1-end').value).getTime() / 1000;
        const r2s = new Date(document.getElementById('range2-start').value).getTime() / 1000;
        const r2e = new Date(document.getElementById('range2-end').value).getTime() / 1000;

        if (!r1s || !r1e || !r2s || !r2e) return;

        try {
            const params = new URLSearchParams({
                range1_start: r1s, range1_end: r1e,
                range2_start: r2s, range2_end: r2e
            });
            const res = await fetch(`/api/trends/${currentDb}/compare?${params}`);
            const json = await res.json();
            renderCompare(json);
        } catch(e) {
            console.error('Compare error:', e);
        }
    }

    function renderCompare(data) {
        document.getElementById('compare-results').style.display = 'grid';
        document.getElementById('compare-data1').innerHTML = renderCompareCol(data.range1.patterns);
        document.getElementById('compare-data2').innerHTML = renderCompareCol(data.range2.patterns);
    }

    function renderCompareCol(patterns) {
        if (!patterns || patterns.length === 0) return '<div class="empty-state">无数据</div>';
        return patterns.slice(0, 10).map(p => `
            <div class="compare-item">
                <div class="compare-query">${escapeHtml((p.query_pattern || '').substring(0, 80))}</div>
                <div class="compare-stats">次数: ${p.total_occurrences || 0} | 耗时: ${(p.total_time || 0).toFixed(1)}s</div>
            </div>
        `).join('');
    }

    function renderClientFingerprints(fingerprints, clientAddr) {
        const section = document.getElementById('drilldown-section');
        let fpDiv = document.getElementById('client-fp-breakdown');
        if (!fpDiv) {
            fpDiv = document.createElement('div');
            fpDiv.id = 'client-fp-breakdown';
            fpDiv.style.marginTop = '16px';
            section.appendChild(fpDiv);
        }
        if (!fingerprints || fingerprints.length === 0) {
            fpDiv.innerHTML = '<div class="empty-state">暂无指纹数据</div>';
            return;
        }
        fpDiv.innerHTML = `
            <h4 style="font-size:14px;color:#8899a6;margin-bottom:8px">客户端 ${escapeHtml(clientAddr)} 的主要查询模式</h4>
            <table class="data-table">
                <thead><tr><th>查询模式</th><th>出现次数</th></tr></thead>
                <tbody>${fingerprints.map(fp => `
                    <tr>
                        <td class="col-query"><div class="query-preview">${escapeHtml((fp.query_pattern || fp.fingerprint).substring(0, 100))}</div></td>
                        <td>${(fp.count || 0).toLocaleString()}</td>
                    </tr>
                `).join('')}</tbody>
            </table>`;
    }

    function chartOpts(title) {
        return {
            responsive: true,
            plugins: { legend: { labels: { color: '#8899a6' } }, title: { display: true, text: title, color: '#8899a6' } },
            scales: {
                x: { ticks: { color: '#657786', maxTicksLimit: 12 }, grid: { color: '#1e2d3d' } },
                y: { ticks: { color: '#657786' }, grid: { color: '#1e2d3d' } }
            }
        };
    }

    function formatHour(ts) {
        return new Date(ts * 1000).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
    }

    function escapeHtml(str) {
        if (!str) return '';
        return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    document.addEventListener('DOMContentLoaded', init);
})();
