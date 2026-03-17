"""SQL client for executing queries against databases."""

import logging
import time
from typing import Any, Dict, List, Optional

import sqlalchemy as sa
import sqlalchemy.exc

from slayer.core.models import DatasourceConfig

logger = logging.getLogger(__name__)


class SlayerSQLClient:
    """Executes SQL against databases via SQLAlchemy."""

    def __init__(self, datasource: DatasourceConfig):
        self.datasource = datasource

    def execute(
        self,
        sql: str,
        timeout_seconds: int = 120,
    ) -> List[Dict[str, Any]]:
        return _execute_with_retry(
            sql=sql,
            connection_string=self.datasource.get_connection_string(),
            db_type=self.datasource.type,
            timeout_seconds=timeout_seconds,
        )


def _execute_with_retry(
    sql: str,
    connection_string: str,
    db_type: Optional[str],
    timeout_seconds: int = 120,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
) -> List[Dict[str, Any]]:
    delay = initial_delay
    for attempt in range(max_attempts):
        try:
            return _execute_sql(
                sql=sql,
                connection_string=connection_string,
                db_type=db_type,
                timeout_seconds=timeout_seconds,
            )
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DisconnectionError):
            if attempt == max_attempts - 1:
                raise
            logger.warning("Transient DB error on attempt %d, retrying in %.1fs", attempt + 1, delay)
            time.sleep(delay)
            delay = min(delay * 2, max_delay)


def _execute_sql(
    sql: str,
    connection_string: str,
    db_type: Optional[str],
    timeout_seconds: int = 120,
) -> List[Dict[str, Any]]:
    engine = sa.create_engine(connection_string)
    try:
        with engine.connect() as conn:
            timeout_ms = timeout_seconds * 1000
            if db_type in ("mysql", "mariadb"):
                conn.execute(sa.text(f"SET max_execution_time = {timeout_ms}"))
            elif db_type == "clickhouse":
                conn.execute(sa.text(f"SET max_execution_time = {timeout_seconds}"))
            elif db_type in ("postgres", "postgresql", None):
                try:
                    conn.execute(sa.text(f"SET statement_timeout = {timeout_ms}"))
                except Exception:
                    pass
            result = conn.execute(sa.text(sql))
            columns = list(result.keys())
            return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        engine.dispose()
