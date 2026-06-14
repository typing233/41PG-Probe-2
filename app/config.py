import os
import re
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class PoolConfig(BaseModel):
    min_size: int = 1
    max_size: int = 5
    command_timeout: int = 10


class DatabaseConfig(BaseModel):
    id: str
    host: str
    port: int = 5432
    database: str
    user: str
    password: str = ""
    ssl: bool = False
    pool: PoolConfig = PoolConfig()

    @field_validator("password", mode="before")
    @classmethod
    def resolve_env_vars(cls, v: str) -> str:
        if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            env_name = v[2:-1]
            return os.environ.get(env_name, "")
        return v

    @property
    def dsn(self) -> str:
        ssl_param = "?ssl=require" if self.ssl else ""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}{ssl_param}"
        )


class CollectionConfig(BaseModel):
    metrics_interval: int = 15
    slow_query_interval: int = 5
    slow_query_threshold: float = 1.0
    top_tables_limit: int = 20
    retention_hours: int = 168


class CircuitBreakerConfig(BaseModel):
    failure_threshold: int = 5
    recovery_timeout: int = 60
    success_threshold: int = 2


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False


class SQLiteConfig(BaseModel):
    path: str = "./data/pgprobe.db"


class IndexAnalysisConfig(BaseModel):
    enabled: bool = True
    collection_interval: int = 300
    analysis_interval: int = 3600
    min_index_size_bytes: int = 8192
    low_scan_threshold: int = 50


class MissingIndexConfig(BaseModel):
    enabled: bool = True
    analysis_interval: int = 3600
    min_seq_scan_count: int = 100
    min_seq_tup_read: int = 10000
    top_queries_limit: int = 50


class SlowQueryTrendsConfig(BaseModel):
    enabled: bool = True
    aggregation_interval: int = 3600
    top_n_clients: int = 10


class HealthScoreConfig(BaseModel):
    enabled: bool = True
    compute_interval: int = 60
    weights: Dict[str, float] = Field(default_factory=lambda: {
        "connections": 0.15,
        "cache_hit": 0.15,
        "tps_stability": 0.10,
        "replication_lag": 0.10,
        "bloat": 0.15,
        "index_health": 0.15,
        "slow_query_rate": 0.20,
    })
    thresholds: Dict[str, Dict[str, float]] = Field(default_factory=lambda: {
        "connections_pct": {"warning": 70, "critical": 90},
        "cache_hit": {"warning": 95, "critical": 85},
        "tps_drop_pct": {"warning": 30, "critical": 60},
        "replication_lag_seconds": {"warning": 10, "critical": 60},
        "bloat_ratio": {"warning": 0.3, "critical": 0.5},
        "unused_index_pct": {"warning": 20, "critical": 40},
        "slow_query_rate_per_min": {"warning": 5, "critical": 20},
    })


class AlertConfig(BaseModel):
    enabled: bool = True
    consecutive_violations: int = 3
    suppression_window: int = 300
    max_alerts_per_hour: int = 20


class AppConfig(BaseModel):
    server: ServerConfig = ServerConfig()
    databases: List[DatabaseConfig] = []
    collection: CollectionConfig = CollectionConfig()
    circuit_breaker: CircuitBreakerConfig = CircuitBreakerConfig()
    sqlite: SQLiteConfig = SQLiteConfig()
    index_analysis: IndexAnalysisConfig = IndexAnalysisConfig()
    missing_index: MissingIndexConfig = MissingIndexConfig()
    slow_query_trends: SlowQueryTrendsConfig = SlowQueryTrendsConfig()
    health_score: HealthScoreConfig = HealthScoreConfig()
    alerts: AlertConfig = AlertConfig()


def load_config(path: str = "config.yaml") -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    return AppConfig(**raw)


class ConfigManager:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config: AppConfig = load_config(config_path)
        self._observers: List[Any] = []

    def reload(self) -> Optional[AppConfig]:
        try:
            new_config = load_config(self.config_path)
            old_config = self.config
            self.config = new_config
            logger.info("Configuration reloaded successfully")
            self._notify_observers(old_config, new_config)
            return new_config
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            return None

    def add_observer(self, callback):
        self._observers.append(callback)

    def _notify_observers(self, old_config: AppConfig, new_config: AppConfig):
        for cb in self._observers:
            try:
                cb(old_config, new_config)
            except Exception as e:
                logger.error(f"Config observer error: {e}")

    def get_database_config(self, db_id: str) -> Optional[DatabaseConfig]:
        for db in self.config.databases:
            if db.id == db_id:
                return db
        return None
