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

import json
import threading
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
        #
        # It receives the snapshot ``get_engine`` took, not the caller's object.
        # That is deliberate: warehouse / role / database / schema_name are all
        # in this engine's cache-key fingerprint, so the USE statements have to
        # keep matching the values it was admitted under.
        applied = apply_mock.call_args.kwargs["datasource"]
        assert applied is not ds
        assert applied == ds


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

    def test_lowering_the_limit_trims_on_the_next_hit(self, monkeypatch) -> None:
        """A cache that only trims on insert stays oversized until the next
        miss. Re-applying the cap on hits makes the new bound take effect on
        the next call."""
        engine_factory.reset_cache()
        sources = [self._lite(i) for i in range(4)]
        for ds in sources:
            engine_factory.get_engine(ds)
        assert len(engine_factory._engine_cache) == 4

        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "2")
        # A pure hit — no miss to piggyback the trim on.
        hot = sources[-1]
        assert engine_factory.get_engine(hot) is not None
        assert len(engine_factory._engine_cache) == 2
        # The entry just touched is the most-recently-used, so it survives.
        assert engine_factory._cache_key(
            datasource=hot, connection_string=hot.get_connection_string(),
        ) in engine_factory._engine_cache
        engine_factory.reset_cache()

    def test_hit_trim_disposes_outside_the_lock(self, monkeypatch) -> None:
        """Trimmed engines are disposed after ``_cache_lock`` is released —
        ``dispose()`` closes sockets and must not run under the lock."""
        engine_factory.reset_cache()
        cold, hot = self._lite(0), self._lite(1)
        engine_factory.get_engine(cold)
        engine_factory.get_engine(hot)

        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "1")
        held: list[bool] = []

        def record_lock_state(*, engine, reason):
            held.append(engine_factory._cache_lock.locked())

        with patch.object(engine_factory, "_dispose_quietly", side_effect=record_lock_state):
            engine_factory.get_engine(hot)
        assert held == [False], "disposal ran while holding the cache lock"
        engine_factory.reset_cache()

    def test_zero_limit_bypasses_reuse_on_a_hit(self, monkeypatch) -> None:
        """With caching off, a previously-cached key must not be served from
        the cache — the entry is dropped and a fresh engine built, rather than
        handing back one we are about to dispose."""
        engine_factory.reset_cache()
        ds = self._lite(0)
        first = engine_factory.get_engine(ds)
        assert len(engine_factory._engine_cache) == 1

        monkeypatch.setenv(engine_factory.MAX_CACHED_ENGINES_ENV, "0")
        second = engine_factory.get_engine(ds)
        assert second is not first, "a zero cap must not reuse the cached engine"
        assert len(engine_factory._engine_cache) == 0
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


class TestLogSafety:
    """Cache keys carry the connection string, which for username/password
    dialects is rendered with the password in plaintext. None of it may reach
    a log line."""

    @staticmethod
    def _pg_with_password(password: str) -> DatasourceConfig:
        return DatasourceConfig(
            name="pg", type="postgres", host="h", username="u",
            password=password, database="db",
        )

    def test_loggable_key_hides_the_connection_string(self) -> None:
        secret = "hunter2-plaintext"  # NOSONAR(S2068) — test fixture
        ds = self._pg_with_password(secret)
        key = engine_factory._cache_key(
            datasource=ds, connection_string=ds.get_connection_string(),
        )
        assert secret in key[0], "precondition: the raw key does carry the password"
        rendered = engine_factory.loggable_key(key)
        for fragment in (secret, "u", "postgresql://"):
            assert fragment not in rendered

    def test_loggable_key_is_stable_and_distinguishing(self) -> None:
        """Useful for correlating log lines: same key -> same id, different
        credentials -> different id."""
        alice = self._pg_with_password("alice-pw")  # NOSONAR(S2068) — test fixture
        # A separate object carrying the same values: the id must follow the
        # credentials, not the identity of the config object.
        alice_again = self._pg_with_password("alice-pw")  # NOSONAR(S2068) — test fixture
        bob = self._pg_with_password("bob-pw")  # NOSONAR(S2068) — test fixture

        def log_id(ds):
            return engine_factory.loggable_key(engine_factory._cache_key(
                datasource=ds, connection_string=ds.get_connection_string(),
            ))

        assert log_id(alice) == log_id(alice_again)
        assert log_id(alice) != log_id(bob)

    def test_reset_disposal_reason_carries_no_credentials(self) -> None:
        """``reset_cache(dispose=True)`` builds its reason string from the
        cache key; ``_dispose_quietly`` writes that into a warning when
        ``dispose()`` raises."""
        secret = "reset-time-secret"  # NOSONAR(S2068) — test fixture
        engine_factory.reset_cache()
        engine_factory.get_engine(self._pg_with_password(secret))
        reasons: list[str] = []
        with patch.object(
            engine_factory, "_dispose_quietly",
            side_effect=lambda *, engine, reason: reasons.append(reason),
        ):
            engine_factory.reset_cache(dispose=True)
        assert reasons, "precondition: disposal ran"
        assert not any(secret in reason for reason in reasons), reasons


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


class TestCacheConcurrency:
    """Engines are reached from worker threads as well as the event loop, so
    the cache's read/bump/insert sequences need one lock around them."""

    @staticmethod
    def _lite(n: int) -> DatasourceConfig:
        return DatasourceConfig(name=f"lite{n}", type="sqlite", database=f"/tmp/slayer-conc-{n}.db")

    @staticmethod
    def _join_all(threads: list[threading.Thread], *, timeout: float = 10.0) -> None:
        """Join every worker and fail if any is still running.

        Without this, ``join(timeout=...)`` silently returns on a deadlocked
        worker and the test passes — which is the one failure mode adding a
        lock introduces. Workers are daemons so a genuine deadlock trips this
        assertion instead of hanging the whole pytest process.
        """
        for thread in threads:
            thread.join(timeout=timeout)
        stuck = [t.name for t in threads if t.is_alive()]
        assert not stuck, f"workers still running after {timeout}s (deadlock?): {stuck}"

    def test_concurrent_misses_yield_one_shared_engine(self) -> None:
        """Two threads missing on the same key must converge on one pool, and
        the losing engine must be disposed rather than orphaned."""
        engine_factory.reset_cache()
        ds = self._lite(0)
        built: list[sa.Engine] = []
        gate = threading.Barrier(2)
        real_build = engine_factory._build_engine

        def slow_build(**kwargs):
            gate.wait(timeout=5)      # force both threads past the first lookup
            engine = real_build(**kwargs)
            built.append(engine)
            return engine

        results: list[sa.Engine] = []
        with (
            patch.object(engine_factory, "_build_engine", side_effect=slow_build),
            patch.object(engine_factory, "_dispose_quietly") as dispose,
        ):
            threads = [
                threading.Thread(
                    target=lambda: results.append(engine_factory.get_engine(ds)),
                    daemon=True,
                )
                for _ in range(2)
            ]
            for t in threads:
                t.start()
            self._join_all(threads)

            assert len(built) == 2, "both threads should have raced past the lookup"
            assert results[0] is results[1], "callers must converge on one engine"
            assert len(engine_factory._engine_cache) == 1
            # The build the cache didn't keep. Leaving it undisposed is the
            # pool leak the double-check exists to prevent, so assert on the
            # engine identity — not on the log-facing reason string.
            loser = next(engine for engine in built if engine is not results[0])
            dispose.assert_called_once()
            assert dispose.call_args.kwargs["engine"] is loser

        engine_factory.reset_cache()

    def test_invalidate_racing_a_lookup_does_not_raise(self) -> None:
        """Pre-lock, an invalidate landing between ``get`` and ``move_to_end``
        raised KeyError."""
        engine_factory.reset_cache()
        sources = [self._lite(i) for i in range(4)]
        for ds in sources:
            engine_factory.get_engine(ds)

        errors: list[BaseException] = []
        stop = threading.Event()

        def hammer(fn):
            try:
                while not stop.is_set():
                    for ds in sources:
                        fn(ds)
            except BaseException as exc:  # noqa: BLE001 — the point is to catch anything
                errors.append(exc)

        threads = [
            threading.Thread(target=hammer, args=(engine_factory.get_engine,), daemon=True),
            threading.Thread(target=hammer, args=(engine_factory.invalidate_engine,), daemon=True),
        ]
        for t in threads:
            t.start()
        stop.wait(timeout=1.0)
        stop.set()
        self._join_all(threads)

        assert not errors, f"concurrent access raised: {errors[:3]}"
        engine_factory.reset_cache()


class TestConfigSnapshot:
    """``get_engine`` snapshots the config before deriving anything from it.
    A ``DatasourceConfig`` is mutable, and engine construction sits between the
    cache key and the dialect's second read of the credentials."""

    @staticmethod
    def _oauth_ds(refresh_token: str) -> DatasourceConfig:
        return DatasourceConfig(
            name="bq", type="bigquery", connection_string="bigquery://proj/dset",
            oauth_credentials_json=json.dumps({
                "type": "authorized_user",  # NOSONAR(S2068) — placeholder grant
                "refresh_token": refresh_token,
                "client_id": "cid",
                "client_secret": "csecret",
            }),
        )

    def test_rotation_mid_build_cannot_desync_key_from_engine(self) -> None:
        """A grant refresh landing while the engine is under construction must
        not leave that engine cached under a fingerprint describing the *other*
        set of credentials — the confusion the credential leg exists to stop."""
        engine_factory.reset_cache()
        ds = self._oauth_ds("before")
        key_for_before = engine_factory._cache_key(
            datasource=ds, connection_string="bigquery://proj/dset",
        )
        seen: dict = {}

        def rotate_then_build(*, datasource, connection_string):
            # Stands in for a concurrent token refresh mutating the caller's
            # config while we are inside _build_engine.
            ds.oauth_credentials_json = json.dumps({"refresh_token": "after"})
            seen["oauth"] = datasource.oauth_credentials_json
            return MagicMock(spec=sa.Engine)

        with patch.object(engine_factory, "_build_engine", side_effect=rotate_then_build):
            engine_factory.get_engine(ds)

        assert list(engine_factory._engine_cache) == [key_for_before]
        # The decisive assertion: the build saw the credentials that key
        # describes. Without the snapshot it would have seen "after" while the
        # entry sat under the "before" key.
        assert json.loads(seen["oauth"])["refresh_token"] == "before"
        engine_factory.reset_cache()

    def test_caller_mutation_does_not_leak_into_the_cached_engine(self) -> None:
        """Mutating the config after the call is self-correcting: the next
        lookup keys off the new credentials, misses, and rebuilds."""
        engine_factory.reset_cache()
        ds = self._oauth_ds("before")
        with patch.object(
            engine_factory, "_build_engine",
            side_effect=lambda **_: MagicMock(spec=sa.Engine),
        ):
            first = engine_factory.get_engine(ds)
            ds.oauth_credentials_json = json.dumps({"refresh_token": "after"})
            second = engine_factory.get_engine(ds)
        assert first is not second, "rotated credentials must not reuse the old engine"
        assert len(engine_factory._engine_cache) == 2
        engine_factory.reset_cache()

    def test_snapshot_is_detached_from_the_callers_object(self) -> None:
        """Sanity-check the copy depth: every field is scalar, so a shallow
        model_copy already detaches. If a mutable field is ever added, this is
        where the assumption breaks."""
        ds = self._oauth_ds("before")
        snapshot = ds.model_copy()
        ds.oauth_credentials_json = "mutated"
        ds.warehouse = "mutated"
        assert snapshot.oauth_credentials_json != "mutated"
        assert snapshot.warehouse != "mutated"

    def test_session_overrides_listener_is_insulated_from_later_mutation(self) -> None:
        """The listener fires on every checkout for the life of the engine. It
        must keep applying the fields this engine was cached under, not whatever
        the caller's config says later — those fields *are* its cache key."""
        pytest.importorskip("snowflake.connector")
        pytest.importorskip("snowflake.sqlalchemy")
        engine_factory.reset_cache()
        ds = DatasourceConfig(
            name="sf", type="snowflake", connection_name="default", warehouse="WH_AT_BUILD",
        )
        real_engine = sa.create_engine("sqlite:///:memory:")
        with (
            patch(
                "slayer.sql.dialects.snowflake.SnowflakeDialect.build_engine",
                return_value=real_engine,
            ),
            patch(
                "slayer.sql.dialects.snowflake.SnowflakeDialect.apply_session_overrides",
            ) as apply_mock,
        ):
            engine = engine_factory.get_engine(ds)
            ds.warehouse = "WH_MUTATED_AFTER"
            with engine.connect() as _:
                pass  # NOSONAR(S108) — opening + closing fires the checkout listener
        assert apply_mock.call_args.kwargs["datasource"].warehouse == "WH_AT_BUILD"
        engine_factory.reset_cache()
