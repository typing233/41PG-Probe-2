import json
import logging
import time
from typing import Any, Dict, List, Optional

from app.database.connection_manager import DatabaseConnection
from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class HealthScorer:
    def __init__(self, store: SQLiteStore, config):
        self.store = store
        self.weights = config.weights
        self.thresholds = config.thresholds

    async def compute(
        self, db_id: str, conn: DatabaseConnection, latest_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        scores = {}

        scores["connections"] = self._score_connections(latest_metrics, conn)
        scores["cache_hit"] = self._score_cache_hit(latest_metrics)
        scores["tps_stability"] = await self._score_tps_stability(db_id)
        scores["replication_lag"] = await self._score_replication(conn)
        scores["bloat"] = await self._score_bloat(conn)
        scores["index_health"] = await self._score_index_health(db_id)
        scores["slow_query_rate"] = await self._score_slow_queries(db_id)

        overall = sum(
            scores.get(d, 100) * self.weights.get(d, 0)
            for d in self.weights
        )
        overall = round(min(max(overall, 0), 100), 1)

        anomalies = self._detect_anomalies(scores, latest_metrics, conn)

        await self.store.insert_health_score(db_id, overall, scores, anomalies)

        return {
            "overall_score": overall,
            "dimension_scores": scores,
            "anomalies": anomalies,
        }

    def _score_connections(self, metrics: Dict, conn: DatabaseConnection) -> float:
        if not metrics:
            return 100.0
        total = metrics.get("total_connections", 0)
        # Try to get max_connections from config, fallback to estimate
        max_conn = 100
        try:
            if conn.pool:
                max_conn = conn.db_config.pool.max_size * 20
        except Exception:
            pass

        usage_pct = (total / max(max_conn, 1)) * 100
        return round(max(100 - usage_pct, 0), 1)

    def _score_cache_hit(self, metrics: Dict) -> float:
        if not metrics:
            return 100.0
        ratio = metrics.get("cache_hit_ratio", 100)
        # Cache hit ratio IS the score (0-100)
        return round(min(max(ratio, 0), 100), 1)

    async def _score_tps_stability(self, db_id: str) -> float:
        start = time.time() - 600
        history = await self.store.get_metrics_history(db_id, start)
        if len(history) < 3:
            return 100.0

        tps_values = [h.get("tps", 0) for h in history if h.get("tps") is not None]
        if not tps_values:
            return 100.0

        avg = sum(tps_values) / len(tps_values)
        if avg == 0:
            return 100.0

        variance = sum((v - avg) ** 2 for v in tps_values) / len(tps_values)
        stddev = variance ** 0.5
        # Coefficient of variation as instability metric
        cv = (stddev / avg) * 100

        return round(max(100 - cv, 0), 1)

    async def _score_replication(self, conn: DatabaseConnection) -> float:
        if not conn.capabilities.get("has_replication") and not conn.capabilities.get("is_standby"):
            return 100.0

        try:
            if conn.capabilities.get("is_standby"):
                rows = await conn.execute_query("""
                    SELECT CASE
                        WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
                        ELSE EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))
                    END AS lag_seconds
                """)
            else:
                rows = await conn.execute_query("""
                    SELECT COALESCE(
                        MAX(EXTRACT(EPOCH FROM (now() - flush_lag))), 0
                    ) AS lag_seconds
                    FROM pg_stat_replication
                """)

            if rows and rows[0].get("lag_seconds") is not None:
                lag = float(rows[0]["lag_seconds"])
                threshold = self.thresholds.get("replication_lag_seconds", {})
                critical = threshold.get("critical", 60)
                warning = threshold.get("warning", 10)
                if lag >= critical:
                    return 0.0
                if lag >= warning:
                    return round(100 - ((lag - warning) / (critical - warning) * 100), 1)
                return 100.0
        except Exception:
            pass

        return 100.0

    async def _score_bloat(self, conn: DatabaseConnection) -> float:
        query = """
            SELECT
                COALESCE(AVG(
                    CASE WHEN n_live_tup > 0
                         THEN n_dead_tup::float / n_live_tup
                         ELSE 0
                    END
                ), 0) AS avg_bloat_ratio
            FROM pg_stat_user_tables
            WHERE n_live_tup > 1000
        """
        try:
            rows = await conn.execute_query(query)
            if rows:
                ratio = float(rows[0].get("avg_bloat_ratio", 0))
                # Thresholds: warning at 0.3 (30% dead), critical at 0.5 (50% dead)
                threshold = self.thresholds.get("bloat_ratio", {})
                critical = threshold.get("critical", 0.5)
                warning = threshold.get("warning", 0.3)
                if ratio >= critical:
                    return 0.0
                if ratio >= warning:
                    return round(100 - ((ratio - warning) / (critical - warning) * 100), 1)
                # Linear scale below warning
                return round(max(100 - (ratio / warning) * 30, 70), 1)
        except Exception:
            pass
        return 100.0

    async def _score_index_health(self, db_id: str) -> float:
        stats = await self.store.get_latest_index_stats(db_id)
        if not stats:
            return 100.0

        total = len(stats)
        if total == 0:
            return 100.0

        unused = sum(1 for s in stats if s.get("idx_scan", 0) == 0
                     and not s.get("is_unique") and not s.get("is_primary"))

        pct_unused = (unused / total) * 100
        # Thresholds for unused index percentage
        threshold = self.thresholds.get("unused_index_pct", {})
        critical = threshold.get("critical", 40)
        warning = threshold.get("warning", 20)
        if pct_unused >= critical:
            return 0.0
        if pct_unused >= warning:
            return round(100 - ((pct_unused - warning) / (critical - warning) * 100), 1)
        return round(100 - (pct_unused / warning) * 20, 1)

    async def _score_slow_queries(self, db_id: str) -> float:
        queries = await self.store.get_slow_queries(db_id, window_seconds=300)
        rate_per_min = len(queries) / 5.0

        threshold = self.thresholds.get("slow_query_rate_per_min", {})
        critical = threshold.get("critical", 20)
        warning = threshold.get("warning", 5)
        if rate_per_min >= critical:
            return 0.0
        if rate_per_min >= warning:
            return round(100 - ((rate_per_min - warning) / (critical - warning) * 100), 1)
        return 100.0

    def _detect_anomalies(
        self, scores: Dict[str, float],
        metrics: Dict[str, Any], conn: DatabaseConnection
    ) -> List[Dict[str, Any]]:
        """Detect anomalies using properly interpreted thresholds.

        Threshold semantics vary by dimension:
        - cache_hit: {warning: 95, critical: 85} → score BELOW threshold triggers alert
          (higher threshold = stricter, "warning at 95" means anything below 95% is a warning)
        - connections_pct: {warning: 70, critical: 90} → raw usage ABOVE threshold triggers alert
        - bloat_ratio: {warning: 0.3, critical: 0.5} → raw value ABOVE threshold triggers alert
        - slow_query_rate_per_min: {warning: 5, critical: 20} → rate ABOVE threshold triggers alert
        - replication_lag_seconds: {warning: 10, critical: 60} → lag ABOVE threshold triggers alert
        - unused_index_pct: {warning: 20, critical: 40} → pct ABOVE threshold triggers alert
        - tps_drop_pct: {warning: 30, critical: 60} → drop pct ABOVE threshold triggers alert

        We use the SCORE (0-100, higher = healthier) and compare against fixed
        score thresholds derived from the config. The key insight:
        - warning score = when the metric first enters warning territory
        - critical score = when the metric is critically bad (score near 0)
        """
        anomalies = []

        # For each dimension, determine alert by score thresholds
        # We define: critical alert if score < 20, warning if score < 50
        # But we also cross-check with the configured thresholds for proper messaging

        dimension_labels = {
            "connections": "连接数",
            "cache_hit": "缓存命中率",
            "tps_stability": "TPS 稳定性",
            "replication_lag": "复制延迟",
            "bloat": "表膨胀",
            "index_health": "索引健康",
            "slow_query_rate": "慢查询率",
        }

        # Anomaly detection uses the score itself:
        # - The scoring functions already encode the threshold semantics
        #   (e.g., cache_hit score IS the ratio, so score=90 with warning=95 means anomaly)
        # - We apply two score-based severity levels:

        # cache_hit: warning threshold means "cache hit below X% is concerning"
        #   So if score < warning_threshold → warning; if score < critical_threshold → critical
        cache_warn = self.thresholds.get("cache_hit", {}).get("warning", 95)
        cache_crit = self.thresholds.get("cache_hit", {}).get("critical", 85)

        # For other dimensions, we use fixed score bands since their scoring functions
        # already translate raw values to 0-100 scores using the configured thresholds

        for dim, score in scores.items():
            label = dimension_labels.get(dim, dim)
            severity = None

            if dim == "cache_hit":
                # cache_hit score = the ratio itself; thresholds are ratio values
                if score < cache_crit:
                    severity = "critical"
                elif score < cache_warn:
                    severity = "warning"
            else:
                # For all other dimensions: scoring functions already map through thresholds
                # Score < 20 → critical (means raw value exceeded critical threshold)
                # Score < 50 → warning (means raw value exceeded warning threshold)
                if score < 20:
                    severity = "critical"
                elif score < 50:
                    severity = "warning"

            if severity:
                anomalies.append({
                    "dimension": dim,
                    "severity": severity,
                    "score": round(score, 1),
                    "message": f"{label}评分 {score:.0f}（{'严重' if severity == 'critical' else '告警'}）",
                })

        return anomalies
