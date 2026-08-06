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
        # DEV-1551: use ``checkout`` (not ``connect``) so session state
        # is re-applied on every pool checkout, not just on the first
        # physical connection.
        assert event_name == "checkout"

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
                    pass  # NOSONAR(S108) — empty body is intentional; opening + closing fires the checkout-event listener under test
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


class TestCredentialKeying:
    """The cache key's credential leg. Without it, two callers whose only
    difference is *who they authenticate as* share one engine — and one
    silently runs the other's queries under the wrong identity."""

    @staticmethod
    def _bq(name: str, credentials_json: str | None) -> DatasourceConfig:
        return DatasourceConfig(
            name=name, type="bigquery",
            connection_string="bigquery://proj/dset",
            credentials_json=credentials_json,
        )

    def test_same_url_different_credentials_get_different_keys(self) -> None:
        alice = self._bq("bq", '{"type": "service_account", "client_email": "alice@x"}')
        bob = self._bq("bq", '{"type": "service_account", "client_email": "bob@x"}')
        conn = "bigquery://proj/dset"
        assert engine_factory._cache_key(alice, conn) != engine_factory._cache_key(bob, conn)

    def test_key_carries_no_secret_material(self) -> None:
        secret = "super-secret-private-key"
        ds = self._bq("bq", '{"type": "service_account", "private_key": "%s"}' % secret)
        key = engine_factory._cache_key(ds, "bigquery://proj/dset")
        assert secret not in "".join(key)

    def test_credential_free_datasource_keeps_empty_leg(self) -> None:
        ds = DatasourceConfig(name="pg", type="postgres", host="h", database="db")
        key = engine_factory._cache_key(ds, ds.get_connection_string())
        assert key[2] == ""

    def test_query_engine_agrees_with_factory(self) -> None:
        """The two caches must key identically, or a caller can be handed a
        client whose engine was built for someone else's credentials."""
        from slayer.engine.query_engine import _sql_client_cache_key
        ds = self._bq("bq", '{"type": "service_account", "client_email": "alice@x"}')
        assert _sql_client_cache_key(ds) == engine_factory._cache_key(
            ds, ds.get_connection_string(),
        )


class TestCacheBounding:
    """Per-identity keys make cache cardinality track *users*, not
    datasources, so the cache has to be bounded and evictions must actually
    release the pooled connections."""

    @staticmethod
    def _lite(n: int) -> DatasourceConfig:
        return DatasourceConfig(name=f"lite{n}", type="sqlite", database=f"/tmp/slayer-cache-{n}.db")

    def test_cache_evicts_least_recently_used_over_limit(self, monkeypatch) -> None:
        engine_factory.reset_cache()
        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "2")
        first, second, third = (self._lite(i) for i in range(3))
        engine_factory.get_engine(first)
        engine_factory.get_engine(second)
        engine_factory.get_engine(third)
        assert len(engine_factory._engine_cache) == 2
        cached = list(engine_factory._engine_cache)
        assert engine_factory._cache_key(first, first.get_connection_string()) not in cached
        engine_factory.reset_cache()

    def test_reuse_refreshes_recency(self, monkeypatch) -> None:
        """A hit must move the entry to the MRU end, otherwise the cap
        degenerates into FIFO and evicts the hottest engine."""
        engine_factory.reset_cache()
        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "2")
        first, second, third = (self._lite(i) for i in range(3))
        engine_factory.get_engine(first)
        engine_factory.get_engine(second)
        engine_factory.get_engine(first)   # first is now most-recently used
        engine_factory.get_engine(third)
        remaining = list(engine_factory._engine_cache)
        assert engine_factory._cache_key(first, first.get_connection_string()) in remaining
        assert engine_factory._cache_key(second, second.get_connection_string()) not in remaining
        engine_factory.reset_cache()

    def test_eviction_disposes_the_engine(self, monkeypatch) -> None:
        engine_factory.reset_cache()
        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "1")
        first, second = self._lite(0), self._lite(1)
        evicted = engine_factory.get_engine(first)
        with patch.object(evicted, "dispose") as disposed:
            engine_factory.get_engine(second)
        disposed.assert_called_once()
        engine_factory.reset_cache()

    def test_dispose_failure_does_not_break_caching(self, monkeypatch) -> None:
        """A pool that refuses to close must not take the whole factory with
        it — the new engine still has to reach the caller."""
        engine_factory.reset_cache()
        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "1")
        first, second = self._lite(0), self._lite(1)
        stuck = engine_factory.get_engine(first)
        with patch.object(stuck, "dispose", side_effect=RuntimeError("pool stuck")):
            assert isinstance(engine_factory.get_engine(second), sa.Engine)
        engine_factory.reset_cache()

    @pytest.mark.parametrize(argnames="raw", argvalues=["nonsense", "-1", ""])
    def test_bad_limit_env_falls_back_to_default(self, monkeypatch, raw: str) -> None:
        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, raw)
        assert engine_factory._max_cached_engines() == engine_factory.DEFAULT_MAX_CACHED_ENGINES

    def test_zero_limit_disables_caching(self, monkeypatch) -> None:
        engine_factory.reset_cache()
        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "0")
        engine_factory.get_engine(self._lite(0))
        assert len(engine_factory._engine_cache) == 0


class TestInvalidateEngine:
    """Credentials baked into an engine can be revoked out from under it.
    Those engines are poisoned permanently, so retrying through the cache
    reproduces the failure forever unless something evicts them."""

    @staticmethod
    def _lite() -> DatasourceConfig:
        return DatasourceConfig(name="lite", type="sqlite", database="/tmp/slayer-invalidate.db")

    def test_invalidate_removes_and_disposes(self) -> None:
        engine_factory.reset_cache()
        ds = self._lite()
        engine = engine_factory.get_engine(ds)
        with patch.object(engine, "dispose") as disposed:
            assert engine_factory.invalidate_engine(ds) is True
        disposed.assert_called_once()
        assert engine_factory._cache_key(ds, ds.get_connection_string()) not in engine_factory._engine_cache

    def test_invalidate_is_a_noop_when_uncached(self) -> None:
        engine_factory.reset_cache()
        assert engine_factory.invalidate_engine(self._lite()) is False

    def test_next_get_engine_rebuilds(self) -> None:
        engine_factory.reset_cache()
        ds = self._lite()
        before = engine_factory.get_engine(ds)
        engine_factory.invalidate_engine(ds)
        assert engine_factory.get_engine(ds) is not before
        engine_factory.reset_cache()


class TestResetCacheDisposal:

    def test_reset_disposes_only_when_asked(self) -> None:
        engine_factory.reset_cache()
        ds = DatasourceConfig(name="lite", type="sqlite", database="/tmp/slayer-reset.db")
        engine = engine_factory.get_engine(ds)
        with patch.object(engine, "dispose") as disposed:
            engine_factory.reset_cache()
        disposed.assert_not_called()

        engine = engine_factory.get_engine(ds)
        with patch.object(engine, "dispose") as disposed:
            engine_factory.reset_cache(dispose=True)
        disposed.assert_called_once()
