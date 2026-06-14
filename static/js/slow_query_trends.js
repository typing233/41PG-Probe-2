(function() {
    'use strict';

    let databases = [];
    let currentDb = null;
    let currentRange = '24h';
    let chartCount = null;
    let chartDuration = null;

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

        document.getElementById('btn-compare').addEventListener('click', runCompare);
    }

    async function loadData() {
        if (!currentDb) return;
        await Promise.all([loadTopPatterns(), loadTrendCharts()]);
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

    function renderTopPatterns(patterns) {
        const tbody = document.getElementById('top-patterns-tbody');
        if (!patterns || patterns.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#657786;padding:40px">暂无趋势数据</td></tr>';
            return;
        }
        tbody.innerHTML = patterns.map(p => `
            <tr>
                <td class="col-query"><div class="query-preview">${escapeHtml((p.query_pattern || '').substring(0, 120))}</div></td>
                <td>${(p.total_occurrences || 0).toLocaleString()}</td>
                <td>${(p.total_time || 0).toFixed(1)}s</td>
                <td>${(p.mean_duration || 0).toFixed(2)}s</td>
                <td>${(p.peak_duration || 0).toFixed(2)}s</td>
            </tr>
        `).join('');
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
