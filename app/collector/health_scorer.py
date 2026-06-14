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

        anomalies = self._detect_anomalies(scores)

        await self.store.insert_health_score(db_id, overall, scores, anomalies)

        return {
            "overall_score": overall,
            "dimension_scores": scores,
            "anomalies": anomalies,
        }

    def _score_connections(self, metrics: Dict, conn: DatabaseConnection) -> float:
        if not metrics:
            return 100.0
        active = metrics.get("active_connections", 0)
        total = metrics.get("total_connections", 0)
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
        return round(min(ratio, 100), 1)

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
                if lag >= critical:
                    return 0.0
                warning = threshold.get("warning", 10)
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
                return round(max(100 - ratio * 200, 0), 1)
        except Exception:
            pass
        return 100.0

    async def _score_index_health(self, db_id: str) -> float:
        stats = await self.store.get_latest_index_stats(db_id)
        if not stats:
            return 100.0

        total = len(stats)
        unused = sum(1 for s in stats if s.get("idx_scan", 0) == 0
                     and not s.get("is_unique") and not s.get("is_primary"))

        if total == 0:
            return 100.0
        pct_unused = (unused / total) * 100
        return round(max(100 - pct_unused * 2.5, 0), 1)

    async def _score_slow_queries(self, db_id: str) -> float:
        since = time.time() - 300
        queries = await self.store.get_slow_queries(db_id, window_seconds=300)
        rate_per_min = len(queries) / 5.0

        threshold = self.thresholds.get("slow_query_rate_per_min", {})
        critical = threshold.get("critical", 20)
        if rate_per_min >= critical:
            return 0.0
        warning = threshold.get("warning", 5)
        if rate_per_min >= warning:
            return round(100 - ((rate_per_min - warning) / (critical - warning) * 100), 1)
        return 100.0

    def _detect_anomalies(self, scores: Dict[str, float]) -> List[Dict[str, Any]]:
        anomalies = []
        threshold_map = {
            "connections": "connections_pct",
            "cache_hit": "cache_hit",
            "tps_stability": "tps_drop_pct",
            "replication_lag": "replication_lag_seconds",
            "bloat": "bloat_ratio",
            "index_health": "unused_index_pct",
            "slow_query_rate": "slow_query_rate_per_min",
        }

        for dim, score in scores.items():
            t_key = threshold_map.get(dim)
            if not t_key or t_key not in self.thresholds:
                continue
            thresholds = self.thresholds[t_key]
            critical_val = thresholds.get("critical", 0)
            warning_val = thresholds.get("warning", 0)

            if score < 100 - critical_val:
                anomalies.append({
                    "dimension": dim,
                    "severity": "critical",
                    "score": score,
                    "message": f"{dim} 评分 {score}，低于严重阈值",
                })
            elif score < 100 - warning_val:
                anomalies.append({
                    "dimension": dim,
                    "severity": "warning",
                    "score": score,
                    "message": f"{dim} 评分 {score}，触发告警阈值",
                })

        return anomalies
