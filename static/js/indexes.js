(function() {
    'use strict';

    let databases = [];
    let currentDb = null;

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
        document.getElementById('category-filter').addEventListener('change', loadData);
        document.getElementById('modal-close').addEventListener('click', closeModal);
        document.getElementById('modal-backdrop').addEventListener('click', closeModal);
    }

    async function loadData() {
        if (!currentDb) return;
        await Promise.all([loadIndexStats(), loadRecommendations()]);
    }

    async function loadIndexStats() {
        try {
            const res = await fetch(`/api/indexes/${currentDb}`);
            const json = await res.json();
            document.getElementById('total-indexes').textContent = json.count;
            renderIndexTable(json.indexes);
        } catch(e) {
            console.error('Failed to load index stats:', e);
        }
    }

    async function loadRecommendations() {
        const category = document.getElementById('category-filter').value;
        const params = new URLSearchParams();
        if (category) params.set('category', category);

        try {
            const res = await fetch(`/api/indexes/${currentDb}/recommendations?${params}`);
            const json = await res.json();
            document.getElementById('total-recs').textContent = json.count;

            let totalSavings = 0;
            json.recommendations.forEach(r => { totalSavings += r.estimated_size_savings || 0; });
            document.getElementById('savings-est').textContent = formatBytes(totalSavings);

            renderRecommendations(json.recommendations);
        } catch(e) {
            console.error('Failed to load recommendations:', e);
        }
    }

    function renderIndexTable(indexes) {
        const tbody = document.getElementById('index-tbody');
        if (!indexes || indexes.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:#657786;padding:40px">暂无数据</td></tr>';
            return;
        }
        tbody.innerHTML = indexes.slice(0, 100).map(idx => {
            const typeLabel = idx.is_primary ? '主键' : idx.is_unique ? '唯一' : '普通';
            const typeClass = idx.is_primary ? 'badge-primary' : idx.is_unique ? 'badge-unique' : '';
            const writeOps = (idx.n_tup_ins || 0) + (idx.n_tup_upd || 0) + (idx.n_tup_del || 0);
            return `<tr>
                <td><code>${escapeHtml(idx.index_name)}</code></td>
                <td>${idx.schema_name}.${idx.table_name}</td>
                <td>${formatBytes(idx.index_size_bytes)}</td>
                <td>${(idx.idx_scan || 0).toLocaleString()}</td>
                <td><span class="badge ${typeClass}">${typeLabel}</span></td>
                <td>${writeOps.toLocaleString()}</td>
            </tr>`;
        }).join('');
    }

    function renderRecommendations(recs) {
        const container = document.getElementById('recommendations-list');
        const noRecs = document.getElementById('no-recs');

        if (!recs || recs.length === 0) {
            container.innerHTML = '';
            noRecs.style.display = 'block';
            return;
        }
        noRecs.style.display = 'none';

        container.innerHTML = recs.map(r => {
            const riskClass = r.risk_level === 'high' ? 'risk-high' :
                             r.risk_level === 'medium' ? 'risk-medium' : 'risk-low';
            const catLabel = {redundant:'冗余',unused:'未使用',mergeable:'可合并',low_freq_critical:'低频关键'}[r.category] || r.category;
            return `<div class="rec-card ${riskClass}">
                <div class="rec-header">
                    <span class="rec-index">${escapeHtml(r.index_name)}</span>
                    <span class="badge badge-${r.risk_level}">${r.risk_level.toUpperCase()}</span>
                    <span class="badge">${catLabel}</span>
                </div>
                <div class="rec-detail">
                    <span class="rec-table">${r.schema_name}.${r.table_name}</span>
                    <span class="rec-savings">节省 ${formatBytes(r.estimated_size_savings)}</span>
                </div>
                <div class="rec-reason">${escapeHtml(r.reason)}</div>
                <div class="rec-actions">
                    <button class="btn btn-sm" onclick="showDDL(${r.id}, '${escapeAttr(r.drop_ddl)}', '${escapeAttr(r.rollback_ddl)}', '${escapeAttr(r.reason)}', '${escapeAttr(r.index_name)}')">查看 DDL</button>
                    <button class="btn btn-sm btn-dismiss" onclick="dismissRec(${r.id})">忽略</button>
                </div>
            </div>`;
        }).join('');
    }

    window.showDDL = function(id, drop, rollback, reason, name) {
        document.getElementById('modal-title').textContent = name;
        document.getElementById('modal-drop-ddl').textContent = drop;
        document.getElementById('modal-rollback-ddl').textContent = rollback;
        document.getElementById('modal-reason').textContent = reason;
        document.getElementById('ddl-modal').style.display = 'flex';
    };

    window.dismissRec = async function(id) {
        try {
            await fetch(`/api/indexes/${currentDb}/dismiss/${id}`, {method: 'POST'});
            loadRecommendations();
        } catch(e) {
            console.error('Dismiss error:', e);
        }
    };

    function closeModal() {
        document.getElementById('ddl-modal').style.display = 'none';
    }

    function formatBytes(bytes) {
        if (bytes == null || bytes === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let i = 0, val = bytes;
        while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
        return val.toFixed(1) + ' ' + units[i];
    }

    function escapeHtml(str) {
        if (!str) return '';
        return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    }

    function escapeAttr(str) {
        if (!str) return '';
        return str.replace(/'/g, "\\'").replace(/"/g, '&quot;').replace(/\n/g, ' ');
    }

    setInterval(loadData, 60000);
    document.addEventListener('DOMContentLoaded', init);
})();
