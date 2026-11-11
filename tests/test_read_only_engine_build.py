"""Engine-build refusals and the backends without a read-only mode (spec: database/execution)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from slayer.core.errors import SlayerError, UnsupportedDriverError
from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.sql import connection_policy
from slayer.sql.dialects import dialect_for_ds_type


@pytest.fixture
def nothing_connects(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a connection was opened")

    monkeypatch.setattr("pymysql.connections.Connection.connect", _refuse)
    monkeypatch.setattr("aiomysql.connection.Connection._connect", _refuse)


def _build_async(ds: DatasourceConfig) -> None:
    async def _go() -> None:
        client = SlayerSQLClient(ds)
        try:
            client._get_async_engine()
        finally:
            await client.aclose()

    asyncio.run(_go())


# ---------------------------------------------------------------------------
# A MySQL URL with its own init command is refused
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("nothing_connects")
@pytest.mark.parametrize(
    ("scheme", "builder"),
    [("mysql+pymysql", "sync"), ("mysql+aiomysql", "async")],
)
@pytest.mark.parametrize("ds_type", ["mysql", "mariadb"])
def test_a_mysql_url_with_its_own_init_command_is_refused(ds_type: str, scheme: str, builder: str) -> None:
    ds = DatasourceConfig(
        name="own_init", type=ds_type,
        connection_string=f"{scheme}://u@db.invalid/d?init_command=SET%20NAMES%20utf8mb4",
    )
    build = engine_factory.get_engine if builder == "sync" else _build_async
    engine_factory.reset_cache()
    try:
        with pytest.raises(SlayerError, match="own_init"):
            build(ds)
    finally:
        engine_factory.reset_cache()


# ---------------------------------------------------------------------------
# ClickHouse drivers other than clickhouse+http are refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scheme", ["clickhouse+native", "clickhouse+asynch"])
def test_another_clickhouse_driver_is_refused(scheme: str) -> None:
    ds = DatasourceConfig(name="ch", type="clickhouse", connection_string=f"{scheme}://u@db.invalid:9000/d")
    build = _build_async if scheme.endswith("asynch") else engine_factory.get_engine
    engine_factory.reset_cache()
    try:
        with pytest.raises(UnsupportedDriverError) as raised:
            build(ds)
    finally:
        engine_factory.reset_cache()
    assert "clickhouse+http" in raised.value.hint


# ---------------------------------------------------------------------------
# Backends without a read-only mode connect as before
# ---------------------------------------------------------------------------

_UNCHANGED_URLS = {
    "sqlserver": "mssql+pyodbc://u:p@db.invalid/d?driver=ODBC+Driver+18+for+SQL+Server",
    "snowflake": "snowflake://u:p@acct/db/sc?warehouse=wh",
    "bigquery": "bigquery://proj/ds",
    "trino": "trino://u@db.invalid:8080/cat/sc",
    "oracle": "oracle+oracledb://u:p@db.invalid:1521/?service_name=s",
    "redshift": "redshift+redshift_connector://u:p@db.invalid:5439/d",
    "presto": "presto://u@db.invalid:8080/cat/sc",
    "athena": "awsathena+rest://@athena.us-east-1.amazonaws.com:443/sc?s3_staging_dir=s3%3A%2F%2Fb%2F",
    "databricks": "databricks://token:t@db.invalid?http_path=/sql/1&catalog=c&schema=s",
    "spark": "hive://u@db.invalid:10000/d",
}

# ``engine_kwargs`` for each before this change, the dialect object aside.
_UNCHANGED_KWARGS: dict[str, dict[str, Any]] = {
    "sqlserver": {"plugins": ["slayer_connection_policy"], "isolation_level": "AUTOCOMMIT",
                  "skip_autocommit_rollback": True},
    "snowflake": {"plugins": ["slayer_connection_policy"], "isolation_level": "AUTOCOMMIT"},
    "bigquery": {"plugins": ["slayer_connection_policy"]},
    "trino": {"plugins": ["slayer_connection_policy"]},
    "oracle": {"plugins": ["slayer_connection_policy"]},
    "redshift": {"plugins": ["slayer_connection_policy"]},
    "presto": {"plugins": ["slayer_connection_policy"]},
    "athena": {"plugins": ["slayer_connection_policy"]},
    "databricks": {"plugins": ["slayer_connection_policy"]},
    "spark": {"plugins": ["slayer_connection_policy"]},
}

# How many connect-time listeners each buildable engine's pool had before this change.
_UNCHANGED_CONNECT_LISTENERS = {"sqlserver": 3, "snowflake": 3, "trino": 1}


@pytest.mark.parametrize("ds_type", sorted(_UNCHANGED_URLS))
@pytest.mark.parametrize("is_async", [False, True])
def test_engine_arguments_are_unchanged(ds_type: str, is_async: bool) -> None:
    ds = DatasourceConfig(name="ds", type=ds_type, connection_string=_UNCHANGED_URLS[ds_type])
    kwargs = connection_policy.engine_kwargs(
        url=_UNCHANGED_URLS[ds_type], dialect=dialect_for_ds_type(ds_type), datasource=ds, is_async=is_async,
    )
    kwargs.pop("slayer_dialect")
    assert kwargs == _UNCHANGED_KWARGS[ds_type]


@pytest.mark.parametrize("ds_type", sorted(_UNCHANGED_CONNECT_LISTENERS))
def test_connect_time_setup_is_unchanged(ds_type: str) -> None:
    ds = DatasourceConfig(name="ds", type=ds_type, connection_string=_UNCHANGED_URLS[ds_type])
    engine_factory.reset_cache()
    try:
        engine = engine_factory.get_engine(ds)
        on_connect = len(list(engine.pool.dispatch.connect))
        options = dict(engine.get_execution_options())
        begin_listeners = list(engine.dispatch.begin)
    finally:
        engine_factory.reset_cache()
    assert on_connect == _UNCHANGED_CONNECT_LISTENERS[ds_type]
    assert options == {}
    assert begin_listeners == []
