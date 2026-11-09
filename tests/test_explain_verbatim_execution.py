"""Explain statements reach the driver verbatim: ``:name`` is no bind parameter, ``%`` no format directive (python.sql §3.13)."""

from __future__ import annotations

import sqlalchemy as sa

from slayer.core.models import DatasourceConfig
from slayer.database.client import SlayerSQLClient
from slayer.sql.dialects.base import ExplainStatements
from tests._engine_helpers import disposable_engine

_DOOR_SQL = "SELECT '(?i)(?:too complicated|too complex)' AS pat, strftime('%Y-%m', '2020-03-15') AS ym, ':name' AS n"
_DOOR_ROW = {"pat": "(?i)(?:too complicated|too complex)", "ym": "2020-03", "n": ":name"}


async def test_an_in_memory_client_explains_verbatim() -> None:
    client = SlayerSQLClient(DatasourceConfig(name="door", type="sqlite", connection_string="sqlite:///:memory:"))
    try:
        rows = (await client.explain(ExplainStatements(statement=_DOOR_SQL))).rows
    finally:
        client.close()
    assert rows == [_DOOR_ROW]


async def test_session_statements_reach_the_driver_verbatim(tmp_path) -> None:
    path = str(tmp_path / "door.db")
    with disposable_engine(f"sqlite:///{path}", poolclass=sa.QueuePool, pool_size=1, max_overflow=0) as engine:
        client = SlayerSQLClient(DatasourceConfig(name="door", type="sqlite", database=path), sa_engine=engine)
        rows = (await client.explain(ExplainStatements(
            statement="SELECT name AS n FROM door_marks",
            session_on="CREATE TEMP TABLE door_marks AS SELECT ':on%' AS name",
            session_off="DROP TABLE temp.door_marks",
        ))).rows
        leftover = (await client.execute("SELECT COUNT(*) AS n FROM sqlite_temp_master")).rows
    assert rows == [{"n": ":on%"}]
    assert leftover == [{"n": 0}]
