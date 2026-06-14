import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.config import AlertConfig
from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class AlertState:
    __slots__ = ("violation_count", "last_alert_time", "current_severity")

    def __init__(self):
        self.violation_count: int = 0
        self.last_alert_time: float = 0
        self.current_severity: Optional[str] = None


class AlertManager:
    def __init__(self, config: AlertConfig, store: SQLiteStore, broadcast_callback: Optional[Callable] = None):
        self.config = config
        self.store = store
        self.broadcast_callback = broadcast_callback
        self._states: Dict[Tuple[str, str], AlertState] = {}
        self._hourly_count: int = 0
        self._hour_start: float = time.time()

    async def evaluate(
        self, db_id: str, dimension_scores: Dict[str, float], anomalies: List[Dict]
    ):
        if not self.config.enabled:
            return

        for anomaly in anomalies:
            dim = anomaly["dimension"]
            severity = anomaly["severity"]
            score = anomaly["score"]
            message = anomaly["message"]

            key = (db_id, dim)
            state = self._states.setdefault(key, AlertState())
            state.violation_count += 1

            if state.violation_count < self.config.consecutive_violations:
                continue

            now = time.time()
            if now - state.last_alert_time < self.config.suppression_window:
                continue

            self._refresh_hourly_count()
            if self._hourly_count >= self.config.max_alerts_per_hour:
                continue

            state.last_alert_time = now
            state.current_severity = severity
            self._hourly_count += 1

            threshold = 100 - score
            await self.store.insert_alert(db_id, dim, severity, message, score, threshold)

            if self.broadcast_callback:
                try:
                    await self.broadcast_callback(db_id, {
                        "type": "alert",
                        "dimension": dim,
                        "severity": severity,
                        "message": message,
                        "score": score,
                        "timestamp": now,
                    })
                except Exception:
                    pass

            logger.warning(f"[{db_id}] Alert fired: {dim} {severity} - {message}")

        active_dims = {a["dimension"] for a in anomalies}
        for (d_id, dim), state in list(self._states.items()):
            if d_id == db_id and dim not in active_dims:
                state.violation_count = 0
                state.current_severity = None

    def _refresh_hourly_count(self):
        now = time.time()
        if now - self._hour_start >= 3600:
            self._hourly_count = 0
            self._hour_start = now
