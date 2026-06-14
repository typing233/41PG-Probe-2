import logging
from typing import Dict

from app.database.connection_manager import DatabaseConnection

logger = logging.getLogger(__name__)


class VersionDetector:
    async def detect(self, conn: DatabaseConnection) -> Dict[str, bool]:
        capabilities = {
            "pg_stat_statements": False,
            "pg_stat_statements_v1_8": False,
            "wait_events": True,
            "query_id_in_activity": False,
        }

        try:
            rows = await conn.execute_query("SHOW server_version_num")
            version_num = int(rows[0]["server_version_num"])
            major = version_num // 10000
            conn.pg_version = major
            logger.info(f"[{conn.db_id}] PostgreSQL version: {major} (raw: {version_num})")
        except Exception as e:
            logger.warning(f"[{conn.db_id}] Could not detect version: {e}")
            conn.pg_version = 10
            conn.capabilities = capabilities
            return capabilities

        if major >= 14:
            capabilities["query_id_in_activity"] = True

        try:
            ext_rows = await conn.execute_query(
                "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'"
            )
            if ext_rows:
                await conn.execute_query(
                    "SELECT 1 FROM pg_stat_statements LIMIT 0"
                )
                capabilities["pg_stat_statements"] = True
                if major >= 13:
                    capabilities["pg_stat_statements_v1_8"] = True
                logger.info(f"[{conn.db_id}] pg_stat_statements available")
        except Exception:
            logger.info(f"[{conn.db_id}] pg_stat_statements not accessible")

        conn.capabilities = capabilities
        return capabilities
