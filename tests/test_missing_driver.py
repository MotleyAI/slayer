"""A missing database driver or dialect plugin raises ``MissingDriverError`` naming the extra to install."""

from __future__ import annotations

import json
import sys
from collections import OrderedDict
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.exc import NoSuchModuleError

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.errors import MissingDriverError, SlayerError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.sql import client as client_module
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects import PostgresDialect
from slayer.storage.yaml_storage import YAMLStorage

_HOST = "missing-driver.invalid"


@pytest.fixture(autouse=True)
def _isolated_engine_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """A cached engine would skip the driver pre-flight."""
    monkeypatch.setattr(engine_factory, "_engine_cache", OrderedDict())


def _block_modules(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
    """Make ``import <name>`` raise ``ModuleNotFoundError``."""
    for name in names:
        monkeypatch.setitem(sys.modules, name, None)


def _hide_plugin(monkeypatch: pytest.MonkeyPatch, entrypoint: str) -> None:
    """Make SQLAlchemy's dialect registry fail to load ``entrypoint`` as if the plugin were absent."""
    real_load = registry.load

    def _load(name: str):
        if name == entrypoint:
            raise NoSuchModuleError(f"Can't load plugin: sqlalchemy.dialects:{name}")
        return real_load(name)

    monkeypatch.setattr(registry, "load", _load)


def _structured(ds_type: str, **kwargs) -> DatasourceConfig:
    return DatasourceConfig(
        name=f"{ds_type}_ds", type=ds_type, host=_HOST, port=1234,
        database="db", username="u", password="p", **kwargs,
    )


def _assert_names(err: MissingDriverError, *, ds: DatasourceConfig, missing: str) -> None:
    msg = str(err)
    assert err.datasource_name == ds.name
    assert err.ds_type == ds.type
    assert err.missing == missing
    assert ds.type is not None
    assert ds.name in msg
    assert ds.type in msg
    assert missing in msg
    assert err.hint in msg


# ---------------------------------------------------------------------------
# Sync path, every extra-backed type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ds_type,blocked_module,extra",
    [
        ("postgres", "psycopg2", "postgres"),
        ("postgresql", "psycopg2", "postgres"),
        ("mysql", "pymysql", "mysql"),
        ("mariadb", "pymysql", "mysql"),
        ("sqlserver", "pyodbc", "sqlserver"),
        ("mssql", "pyodbc", "sqlserver"),
    ],
)
def test_sync_missing_dbapi_names_extra(monkeypatch, ds_type, blocked_module, extra) -> None:
    ds = _structured(ds_type)
    _block_modules(monkeypatch, blocked_module)
    client = SlayerSQLClient(ds)
    with pytest.raises(MissingDriverError) as info:
        client.execute_sync("SELECT 1")
    _assert_names(info.value, ds=ds, missing=blocked_module)
    assert f"pip install 'motley-slayer[{extra}]'" in str(info.value)


@pytest.mark.parametrize(
    "ds_type,entrypoint,scheme,extra",
    [
        ("clickhouse", "clickhouse.http", "clickhouse+http", "clickhouse"),
        ("bigquery", "bigquery", "bigquery", "bigquery"),
    ],
)
def test_sync_missing_plugin_names_scheme_and_extra(
    monkeypatch, ds_type, entrypoint, scheme, extra,
) -> None:
    ds = _structured(ds_type)
    _hide_plugin(monkeypatch, entrypoint)
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing=scheme)
    assert f"pip install 'motley-slayer[{extra}]'" in str(info.value)


def test_snowflake_inline_missing_plugin_names_extra(monkeypatch) -> None:
    pytest.importorskip("snowflake.sqlalchemy")
    ds = DatasourceConfig(
        name="sf_ds", type="snowflake", host="acct", username="u", password="p", database="db",
    )
    _hide_plugin(monkeypatch, "snowflake")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="snowflake")
    assert "pip install 'motley-slayer[snowflake]'" in str(info.value)


def test_installed_plugin_with_missing_import_names_extra(monkeypatch) -> None:
    pytest.importorskip("sqlalchemy_bigquery")
    ds = _structured("bigquery")
    _block_modules(monkeypatch, "sqlalchemy_bigquery")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="sqlalchemy_bigquery")
    assert "pip install 'motley-slayer[bigquery]'" in str(info.value)


def test_missing_transitive_module_of_default_driver_still_names_extra(monkeypatch) -> None:
    ds = _structured("postgres")

    def _fail():
        raise ModuleNotFoundError("No module named 'psycopg2._psycopg'", name="psycopg2._psycopg")

    monkeypatch.setattr(PGDialect_psycopg2, "import_dbapi", classmethod(lambda cls: _fail()))
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="psycopg2._psycopg")
    assert "pip install 'motley-slayer[postgres]'" in str(info.value)


# ---------------------------------------------------------------------------
# Async path: no silent sync fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ds_type,blocked_module,extra",
    [
        ("postgres", "asyncpg", "postgres"),
        ("mysql", "aiomysql", "mysql"),
    ],
)
async def test_async_missing_driver_raises_without_sync_fallback(
    monkeypatch, ds_type, blocked_module, extra,
) -> None:
    ds = _structured(ds_type)
    _block_modules(monkeypatch, blocked_module)
    sync_engine = MagicMock(side_effect=AssertionError("fell back to the sync engine"))
    monkeypatch.setattr(engine_factory, "get_engine", sync_engine)
    client = SlayerSQLClient(ds)
    with pytest.raises(MissingDriverError) as info:
        await client.execute("SELECT 1")
    _assert_names(info.value, ds=ds, missing=blocked_module)
    assert f"pip install 'motley-slayer[{extra}]'" in str(info.value)
    sync_engine.assert_not_called()


async def test_async_psycopg_kept_without_asyncpg(monkeypatch) -> None:
    pytest.importorskip("psycopg")
    ds = DatasourceConfig(
        name="pg3", type="postgres", connection_string=f"postgresql+psycopg://u:p@{_HOST}:5432/d",
    )
    _block_modules(monkeypatch, "asyncpg")
    client = SlayerSQLClient(ds)
    engine = client._get_async_engine()
    try:
        assert engine is not None
        assert engine.url.drivername == "postgresql+psycopg"
    finally:
        await client.aclose()


async def test_async_missing_custom_driver_names_connection_string(monkeypatch) -> None:
    ds = DatasourceConfig(
        name="pg3", type="postgres", connection_string=f"postgresql+psycopg://u:p@{_HOST}:5432/d",
    )
    _block_modules(monkeypatch, "psycopg")
    client = SlayerSQLClient(ds)
    with pytest.raises(MissingDriverError) as info:
        await client.execute("SELECT 1")
    _assert_names(info.value, ds=ds, missing="psycopg")
    assert "connection_string" in str(info.value)
    assert "motley-slayer[" not in str(info.value)


@pytest.mark.parametrize(
    "ds",
    [
        DatasourceConfig(
            name="pg8k", type="postgres", connection_string=f"postgresql+pg8000://u:p@{_HOST}:5432/d",
        ),
        DatasourceConfig(
            name="mysqldb", type="mysql", connection_string=f"mysql+mysqldb://u:p@{_HOST}:3306/d",
        ),
        DatasourceConfig(name="sf_ds", type="snowflake", connection_name="default"),
    ],
    ids=lambda ds: ds.name,
)
async def test_async_execute_runs_sync_in_thread(monkeypatch, ds) -> None:
    sync_engine = MagicMock()
    monkeypatch.setattr(engine_factory, "get_engine", MagicMock(return_value=sync_engine))
    monkeypatch.setattr(
        client_module, "_execute_with_retry_async",
        AsyncMock(side_effect=AssertionError("ran on an async engine")),
    )
    threaded = AsyncMock(return_value="result")
    monkeypatch.setattr(client_module, "_execute_with_retry_threaded", threaded)
    assert await SlayerSQLClient(ds).execute("SELECT 1") == "result"
    threaded.assert_awaited_once()
    assert threaded.await_args is not None
    assert threaded.await_args.kwargs["engine"] is sync_engine


# ---------------------------------------------------------------------------
# Custom driver and Tier-2 hints
# ---------------------------------------------------------------------------


def test_custom_driver_not_attributed_to_extra(monkeypatch) -> None:
    ds = DatasourceConfig(
        name="pg8k", type="postgres", connection_string=f"postgresql+pg8000://u:p@{_HOST}:5432/d",
    )
    _block_modules(monkeypatch, "pg8000")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="pg8000")
    assert "connection_string" in str(info.value)
    assert "motley-slayer[" not in str(info.value)


def test_custom_driver_missing_plugin_not_attributed_to_extra() -> None:
    ds = DatasourceConfig(
        name="pgx", type="postgres", connection_string=f"postgresql+nosuchdriver://u:p@{_HOST}:5432/d",
    )
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="postgresql+nosuchdriver")
    assert "connection_string" in str(info.value)
    assert "motley-slayer[" not in str(info.value)


def test_mariadb_backend_default_driver_names_extra(monkeypatch) -> None:
    ds = DatasourceConfig(
        name="maria", type="mariadb", connection_string=f"mariadb+pymysql://u:p@{_HOST}:3306/d",
    )
    _block_modules(monkeypatch, "pymysql")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="pymysql")
    assert "pip install 'motley-slayer[mysql]'" in str(info.value)


def test_unregistered_type_missing_plugin_gets_generic_hint() -> None:
    ds = _structured("foo")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="foo")
    assert "connection_string" not in info.value.hint
    assert "motley-slayer[" not in str(info.value)
    assert "configuration/datasources" in str(info.value)


def test_tier2_missing_plugin_gets_generic_hint(monkeypatch) -> None:
    ds = _structured("redshift")
    _hide_plugin(monkeypatch, "redshift")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="redshift")
    assert "install" in info.value.hint.lower()
    assert "motley-slayer[" not in str(info.value)
    assert "configuration/datasources" in str(info.value)


# ---------------------------------------------------------------------------
# Lazily imported vendor libraries
# ---------------------------------------------------------------------------


def test_snowflake_connection_name_missing_connector_fails_at_engine_build(monkeypatch) -> None:
    pytest.importorskip("snowflake.sqlalchemy")
    ds = DatasourceConfig(name="sf_ds", type="snowflake", connection_name="default")
    _block_modules(monkeypatch, "snowflake.connector")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="snowflake.connector")
    assert "pip install 'motley-slayer[snowflake]'" in str(info.value)


def test_snowflake_inline_missing_sqlalchemy_package(monkeypatch) -> None:
    ds = DatasourceConfig(
        name="sf_ds", type="snowflake", host="acct", username="u", password="p", database="db",
    )
    _block_modules(monkeypatch, "snowflake.sqlalchemy")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="snowflake.sqlalchemy")
    assert "pip install 'motley-slayer[snowflake]'" in str(info.value)


def test_bigquery_oauth_missing_google_library(monkeypatch) -> None:
    pytest.importorskip("sqlalchemy_bigquery")
    grant = {
        "type": "authorized_user", "client_id": "cid", "client_secret": "sec",
        "refresh_token": "rt", "token_uri": "https://oauth2.googleapis.com/token",
    }
    ds = DatasourceConfig(
        name="bq_ds", type="bigquery", connection_string="bigquery://proj/dataset",
        oauth_credentials_json=json.dumps(grant),
    )
    _block_modules(monkeypatch, "google.oauth2.credentials")
    with pytest.raises(MissingDriverError) as info:
        engine_factory.get_engine(ds)
    _assert_names(info.value, ds=ds, missing="google.oauth2.credentials")
    assert "pip install 'motley-slayer[bigquery]'" in str(info.value)


# ---------------------------------------------------------------------------
# Error type, propagation, surfaces
# ---------------------------------------------------------------------------


def test_missing_driver_error_is_slayer_and_import_error() -> None:
    assert issubclass(MissingDriverError, SlayerError)
    assert issubclass(MissingDriverError, ImportError)


def test_unrelated_import_error_in_build_hook_propagates_unchanged(monkeypatch) -> None:
    ds = _structured("postgres")
    original = ModuleNotFoundError("No module named 'not_a_driver'", name="not_a_driver")

    def _build_engine(self, datasource, *, connection_string):
        raise original

    monkeypatch.setattr(PostgresDialect, "build_engine", _build_engine)
    with pytest.raises(ModuleNotFoundError) as info:
        engine_factory.get_engine(ds)
    assert info.value is original
    assert not isinstance(info.value, MissingDriverError)


async def test_rest_query_reports_install_hint(monkeypatch, tmp_path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(_structured("postgres"))
    await storage.save_model(
        SlayerModel(
            name="orders", data_source="postgres_ds", sql_table="orders",
            columns=[Column(name="id", type=DataType.INT)],
        ),
        _validate=False,
    )
    _block_modules(monkeypatch, "psycopg2", "asyncpg")
    resp = TestClient(create_app(storage=storage)).post(
        "/query", json={"source_model": "orders", "dimensions": ["id"]},
    )
    assert resp.status_code == 400, resp.text
    assert "pip install 'motley-slayer[postgres]'" in resp.json()["detail"]
