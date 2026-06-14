import os
import re
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, field_validator

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


class AppConfig(BaseModel):
    server: ServerConfig = ServerConfig()
    databases: List[DatabaseConfig] = []
    collection: CollectionConfig = CollectionConfig()
    circuit_breaker: CircuitBreakerConfig = CircuitBreakerConfig()
    sqlite: SQLiteConfig = SQLiteConfig()


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
