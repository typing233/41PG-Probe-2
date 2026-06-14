(function() {
    'use strict';

    let databases = [];
    let currentDb = null;
    let currentWindow = '1h';

    async function init() {
        await loadDatabases();
        setupEventListeners();
        loadQueries();
    }

    async function loadDatabases() {
        try {
            const res = await fetch('/api/databases');
            databases = await res.json();
            const select = document.getElementById('db-select');
            select.innerHTML = databases.map(db =>
                `<option value="${db.id}">${db.id} (${db.host})</option>`
            ).join('');
            if (databases.length > 0) {
                currentDb = databases[0].id;
            }
        } catch(e) {
            console.error('Failed to load databases:', e);
        }
    }

    function setupEventListeners() {
        document.getElementById('db-select').addEventListener('change', (e) => {
            currentDb = e.target.value;
            loadQueries();
        });

        document.querySelectorAll('.window-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.window-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentWindow = btn.dataset.window;
                loadQueries();
            });
        });

        document.getElementById('btn-refresh').addEventListener('click', loadQueries);

        document.getElementById('min-duration').addEventListener('change', loadQueries);

        let searchTimeout;
        document.getElementById('search-input').addEventListener('input', () => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(loadQueries, 500);
        });

        document.getElementById('modal-close').addEventListener('click', closeModal);
        document.getElementById('modal-backdrop').addEventListener('click', closeModal);
    }

    async function loadQueries() {
        if (!currentDb) return;

        const minDuration = document.getElementById('min-duration').value || 0;
        const search = document.getElementById('search-input').value || '';

        const params = new URLSearchParams({
            window: currentWindow,
            min_duration: minDuration,
            search: search,
            limit: 200,
        });

        try {
            const res = await fetch(`/api/slow-queries/${currentDb}?${params}`);
            const json = await res.json();
            renderTable(json.queries);
            document.getElementById('query-count').textContent = `共 ${json.count} 条记录`;
        } catch(e) {
            console.error('Failed to load queries:', e);
        }
    }

    function renderTable(queries) {
        const tbody = document.getElementById('query-tbody');
        if (!queries || queries.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#657786;padding:40px;">暂无慢查询记录</td></tr>';
            return;
        }

        tbody.innerHTML = queries.map((q, idx) => `
            <tr data-idx="${idx}">
                <td class="col-query"><div class="query-preview">${escapeHtml(q.query_text)}</div></td>
                <td class="col-duration">${q.duration_seconds.toFixed(2)}s</td>
                <td class="col-user">${q.username || '-'}</td>
                <td class="col-time">${formatTimestamp(q.query_start)}</td>
                <td class="col-wait">${q.wait_event_type ? q.wait_event_type + '/' + q.wait_event : '-'}</td>
            </tr>
        `).join('');

        tbody.querySelectorAll('tr').forEach((row, idx) => {
            row.addEventListener('click', () => showModal(queries[idx]));
        });
    }

    function showModal(query) {
        document.getElementById('modal-query-text').textContent = query.query_text;
        document.getElementById('modal-meta').innerHTML = `
            <div><span class="label">执行时间</span><span>${query.duration_seconds.toFixed(2)}s</span></div>
            <div><span class="label">触发时间</span><span>${query.query_start ? formatTimestamp(query.query_start) : '-'}</span></div>
            <div><span class="label">用户</span><span>${query.username || '-'}</span></div>
            <div><span class="label">客户端</span><span>${query.client_addr || '-'}</span></div>
            <div><span class="label">采集时间</span><span>${formatTimestamp(query.captured_at)}</span></div>
            <div><span class="label">等待事件</span><span>${query.wait_event_type ? query.wait_event_type + '/' + query.wait_event : '-'}</span></div>
            <div><span class="label">PID</span><span>${query.pid || '-'}</span></div>
            <div><span class="label">指纹</span><span style="font-family:monospace;font-size:11px">${query.fingerprint}</span></div>
        `;
        document.getElementById('query-modal').style.display = 'flex';
    }

    function closeModal() {
        document.getElementById('query-modal').style.display = 'none';
    }

    function formatTimestamp(ts) {
        if (!ts) return '-';
        return new Date(ts * 1000).toLocaleString('zh-CN', {
            month: '2-digit', day: '2-digit',
            hour: '2-digit', minute: '2-digit', second: '2-digit'
        });
    }

    function escapeHtml(str) {
        if (!str) return '';
        return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // Auto-refresh every 30s
    setInterval(loadQueries, 30000);

    document.addEventListener('DOMContentLoaded', init);
})();
