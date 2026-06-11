"""Tests for the shared SQLAlchemy engine factory (DEV-1551).

The factory itself is dialect-agnostic. Each dialect's ``SqlDialect``
strategy class carries the runtime hooks; this module covers:

* ``get_engine`` delegates to ``SqlDialect.build_engine``, falling back to
  ``sa.create_engine(connection_string)`` when the dialect returns None.
* The connect-event listener calls ``SqlDialect.apply_session_overrides``,
  but only when the dialect overrides the no-op base.
* Engine caching is keyed on connection_string + a runtime fingerprint so
  two snowflake datasources differing only in warehouse get different
  cached engines.
* Production engine consumers (ingestion, schema_drift, type_refinement,
  CLI, MCP, SlayerSQLClient) all reference ``engine_factory`` instead of
  bare ``sa.create_engine``.
"""

from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory


class TestGetEngine:

    def test_postgres_uses_standard_create_engine(self) -> None:
        engine_factory.reset_cache()
        ds = DatasourceConfig(
            name="pg", type="postgres", host="h", username="u", password="p", database="db",  # NOSONAR(S2068) — test fixture; obvious placeholder value
        )
        eng = engine_factory.get_engine(ds)
        assert isinstance(eng, sa.Engine)
        assert eng.dialect.name == "postgresql"

    def test_sqlite_uses_standard_create_engine(self) -> None:
        engine_factory.reset_cache()
        ds = DatasourceConfig(name="lite", type="sqlite", database=":memory:")
        eng = engine_factory.get_engine(ds)
        assert isinstance(eng, sa.Engine)
        assert eng.dialect.name == "sqlite"

    def test_dialect_build_engine_takes_precedence(self) -> None:
        """When the dialect's ``build_engine`` returns a non-None engine,
        the factory uses it instead of falling back to ``sa.create_engine``."""
        engine_factory.reset_cache()
        ds = DatasourceConfig(name="sf", type="snowflake", connection_name="default")
        fake_engine = MagicMock()
        with patch(
            "slayer.sql.dialects.snowflake.SnowflakeDialect.build_engine",
            return_value=fake_engine,
        ) as build_mock:
            with patch.object(
                engine_factory, "_attach_session_overrides_listener",
            ):
                result = engine_factory.get_engine(ds)
        assert result is fake_engine
        assert build_mock.call_count == 1

    def test_dialect_build_engine_none_falls_through_to_create_engine(self) -> None:
        """When ``build_engine`` returns None, ``sa.create_engine`` runs
        with the connection_string."""
        engine_factory.reset_cache()
        ds = DatasourceConfig(
            name="pg", type="postgres", host="h", username="u", password="p", database="db",  # NOSONAR(S2068) — test fixture; obvious placeholder value
        )
        with patch("slayer.sql.engine_factory.sa.create_engine") as create_engine_mock:
            fake = MagicMock()
            create_engine_mock.return_value = fake
            result = engine_factory.get_engine(ds)
        assert result is fake
        args, kwargs = create_engine_mock.call_args
        assert args[0].startswith("postgresql://")
        assert kwargs.get("pool_pre_ping") is True


class TestSessionOverridesListener:
    """The ``connect`` event listener wires ``apply_session_overrides``
    onto the engine — but only for dialects that override the base no-op."""

    def test_snowflake_engine_attaches_session_overrides_listener(self) -> None:
        """SnowflakeDialect overrides apply_session_overrides, so the
        listener must be registered."""
        engine_factory.reset_cache()
        ds = DatasourceConfig(
            name="sf", type="snowflake",
            connection_name="default", schema_name="MY_SCHEMA",
        )
        with patch(
            "slayer.sql.dialects.snowflake.SnowflakeDialect.build_engine",
            return_value=MagicMock(),
        ):
            with patch.object(engine_factory.sa_event, "listens_for") as listens_for_mock:
                listens_for_mock.return_value = lambda fn: fn
                engine_factory.get_engine(ds)
        listens_for_mock.assert_called_once()
        _engine_arg, event_name = listens_for_mock.call_args.args
        assert event_name == "connect"

    def test_non_snowflake_engine_skips_session_overrides_listener(self) -> None:
        """Postgres / SQLite / etc. don't override apply_session_overrides,
        so no listener attaches."""
        engine_factory.reset_cache()
        ds = DatasourceConfig(
            name="pg", type="postgres",
            host="h", username="u", password="p", database="db",  # NOSONAR(S2068) — test fixture; obvious placeholder value
            warehouse="should_not_fire",
            role="should_not_fire",
        )
        with patch.object(engine_factory.sa_event, "listens_for") as listens_for_mock:
            listens_for_mock.return_value = lambda fn: fn
            engine_factory.get_engine(ds)
        listens_for_mock.assert_not_called()

    def test_session_listener_invokes_dialect_apply_session_overrides(self) -> None:
        """When the engine opens a connection, the listener body must
        delegate to the dialect's ``apply_session_overrides``."""
        engine_factory.reset_cache()
        ds = DatasourceConfig(
            name="sf", type="snowflake",
            connection_name="default", schema_name="MY_SCHEMA",
        )
        real_engine = sa.create_engine("sqlite:///:memory:")
        with patch(
            "slayer.sql.dialects.snowflake.SnowflakeDialect.build_engine",
            return_value=real_engine,
        ):
            with patch(
                "slayer.sql.dialects.snowflake.SnowflakeDialect.apply_session_overrides",
            ) as apply_mock:
                engine = engine_factory.get_engine(ds)
                with engine.connect() as _:
                    pass  # NOSONAR(S108) — empty body is intentional; opening + closing fires the connect-event listener under test
        assert apply_mock.call_count >= 1
        # Listener calls ``apply_session_overrides(dbapi_connection=..., datasource=...)``
        # by name; the datasource is the kwarg, not a positional arg.
        assert apply_mock.call_args.kwargs["datasource"] is ds


class TestCacheKeying:

    def test_same_datasource_returns_same_engine(self) -> None:
        engine_factory.reset_cache()
        ds = DatasourceConfig(
            name="pg", type="postgres", host="h", username="u", password="p", database="db",  # NOSONAR(S2068) — test fixture; obvious placeholder value
        )
        eng1 = engine_factory.get_engine(ds)
        eng2 = engine_factory.get_engine(ds)
        assert eng1 is eng2

    def test_different_connection_names_get_different_engines(self) -> None:
        pytest.importorskip("snowflake.connector")
        pytest.importorskip("snowflake.sqlalchemy")
        engine_factory.reset_cache()
        ds_a = DatasourceConfig(name="sf_a", type="snowflake", connection_name="profile_a")
        ds_b = DatasourceConfig(name="sf_b", type="snowflake", connection_name="profile_b")
        with patch(
            "slayer.sql.dialects.snowflake.SnowflakeDialect.build_engine",
            side_effect=[MagicMock(), MagicMock()],
        ):
            with patch.object(engine_factory, "_attach_session_overrides_listener"):
                eng_a = engine_factory.get_engine(ds_a)
                eng_b = engine_factory.get_engine(ds_b)
        assert eng_a is not eng_b

    def test_different_warehouses_get_different_engines(self) -> None:
        """Two datasources with the same connection_name but different
        warehouses MUST NOT share a cached engine — the connect listener
        would otherwise apply the wrong USE WAREHOUSE."""
        pytest.importorskip("snowflake.connector")
        engine_factory.reset_cache()
        ds_a = DatasourceConfig(
            name="sf", type="snowflake", connection_name="default", warehouse="WH_A",
        )
        ds_b = DatasourceConfig(
            name="sf", type="snowflake", connection_name="default", warehouse="WH_B",
        )
        with patch(
            "slayer.sql.dialects.snowflake.SnowflakeDialect.build_engine",
            side_effect=[MagicMock(), MagicMock()],
        ):
            with patch.object(engine_factory, "_attach_session_overrides_listener"):
                eng_a = engine_factory.get_engine(ds_a)
                eng_b = engine_factory.get_engine(ds_b)
        assert eng_a is not eng_b


class TestCallSiteMigration:
    """Plan item: every direct ``sa.create_engine(connection_string)``
    call site in production code was migrated to
    ``engine_factory.get_engine``. These tests pin the migration at each
    call site by source-reference (importable from engine_factory) — a
    full mock-based call site test is heavier than this checkpoint
    needs to be.
    """

    def test_ingestion_uses_engine_factory(self) -> None:
        from slayer.engine import ingestion
        source = open(ingestion.__file__).read()
        assert "engine_factory.get_engine" in source or "from slayer.sql.engine_factory" in source

    def test_schema_drift_uses_engine_factory(self) -> None:
        from slayer.engine import schema_drift
        source = open(schema_drift.__file__).read()
        assert "engine_factory.get_engine" in source or "from slayer.sql.engine_factory" in source

    def test_type_refinement_uses_engine_factory(self) -> None:
        from slayer.storage import type_refinement
        source = open(type_refinement.__file__).read()
        assert "engine_factory.get_engine" in source or "from slayer.sql.engine_factory" in source

    def test_cli_uses_engine_factory(self) -> None:
        from slayer import cli
        source = open(cli.__file__).read()
        assert "engine_factory" in source

    def test_mcp_server_uses_engine_factory(self) -> None:
        from slayer.mcp import server
        source = open(server.__file__).read()
        assert "engine_factory" in source

    def test_sql_client_uses_engine_factory_for_engine_creation(self) -> None:
        from slayer.sql import client as sql_client
        source = open(sql_client.__file__).read()
        assert "engine_factory" in source
