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
        """Detect anomalies using configured thresholds per dimension.

        Each dimension's scoring function maps raw values to 0-100, where the
        score reaches 0 exactly at the configured critical threshold and
        starts descending at the warning threshold. We reverse-engineer:
        - score at warning boundary = the point the scoring function would produce
          when the raw value equals the warning threshold
        - score at critical boundary = the point when raw value equals critical

        For most dimensions: score < warning_score → warning, score < critical_score → critical
        For cache_hit: score IS the ratio, compare directly to configured thresholds.
        """
        anomalies = []

        dimension_labels = {
            "connections": "连接数",
            "cache_hit": "缓存命中率",
            "tps_stability": "TPS 稳定性",
            "replication_lag": "复制延迟",
            "bloat": "表膨胀",
            "index_health": "索引健康",
            "slow_query_rate": "慢查询率",
        }

        # Define score thresholds per dimension based on how their scoring functions work:
        # The scoring functions produce scores in [0, 100] using the configured thresholds.
        # At the warning threshold → the score function produces a specific value.
        # At the critical threshold → score = 0.
        # We compute the "warning_score" for each dimension:

        dim_score_thresholds = {}

        # cache_hit: score = ratio itself. warning=95 means below 95 is warning.
        cache_cfg = self.thresholds.get("cache_hit", {})
        dim_score_thresholds["cache_hit"] = {
            "warning_score": cache_cfg.get("warning", 95),
            "critical_score": cache_cfg.get("critical", 85),
        }

        # connections: score = 100 - usage_pct. connections_pct: warning=70, critical=90
        conn_cfg = self.thresholds.get("connections_pct", {})
        conn_warn = conn_cfg.get("warning", 70)
        conn_crit = conn_cfg.get("critical", 90)
        dim_score_thresholds["connections"] = {
            "warning_score": 100 - conn_warn,  # 30
            "critical_score": 100 - conn_crit,  # 10
        }

        # tps_stability: score = 100 - CV. tps_drop_pct: warning=30, critical=60
        tps_cfg = self.thresholds.get("tps_drop_pct", {})
        tps_warn = tps_cfg.get("warning", 30)
        tps_crit = tps_cfg.get("critical", 60)
        dim_score_thresholds["tps_stability"] = {
            "warning_score": 100 - tps_warn,  # 70
            "critical_score": 100 - tps_crit,  # 40
        }

        # replication_lag: linear between warning and critical → score at warning = 100, at critical = 0
        # At exactly warning threshold: score = 100 (still fine)
        # Just above warning: score starts dropping
        # At critical: score = 0
        # Midpoint between warning and critical → score ≈ 50
        dim_score_thresholds["replication_lag"] = {
            "warning_score": 50,  # halfway between warning and critical
            "critical_score": 10,
        }

        # bloat: below warning → score ≥ 70, at warning → 70, at critical → 0
        # The function: if ratio >= critical: 0; if ratio >= warning: linear 100→0;
        #   below warning: 70-100. So "entering warning" means score just hit 70.
        bloat_cfg = self.thresholds.get("bloat_ratio", {})
        dim_score_thresholds["bloat"] = {
            "warning_score": 70,
            "critical_score": 10,
        }

        # index_health: at warning pct → score starts dropping from 80
        # at critical pct → score = 0
        dim_score_thresholds["index_health"] = {
            "warning_score": 50,
            "critical_score": 10,
        }

        # slow_query_rate: at warning threshold → score starts dropping
        # at critical → score = 0
        dim_score_thresholds["slow_query_rate"] = {
            "warning_score": 50,
            "critical_score": 10,
        }

        for dim, score in scores.items():
            label = dimension_labels.get(dim, dim)
            severity = None

            thresholds = dim_score_thresholds.get(dim)
            if not thresholds:
                continue

            critical_score = thresholds["critical_score"]
            warning_score = thresholds["warning_score"]

            if score <= critical_score:
                severity = "critical"
            elif score <= warning_score:
                severity = "warning"

            if severity:
                anomalies.append({
                    "dimension": dim,
                    "severity": severity,
                    "score": round(score, 1),
                    "message": f"{label}评分 {score:.0f}（{'严重' if severity == 'critical' else '告警'}）",
                })

        return anomalies
