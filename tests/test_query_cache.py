"""DEV-1587 — per-engine in-memory query result cache.

Two layers of coverage:

* **Pure ``QueryCache``** (no DB): key stability / ds-fingerprint
  sensitivity, TTL get/expiry with a fake clock, CTE-alias-excluding table
  parsing, the wildcard table-matching rule across dialects, applicable
  refresh-key selection, refresh-key scan-SQL building, value comparison,
  identity-guarded commit, put/delete/size/clear.
* **Engine integration** (real in-process SQLite): opt-in ``cache=True``
  miss→hit (serves stale, no re-exec), TTL lazy re-exec, refresh-key
  baseline-before-data ordering, ``refresh()`` buckets, multiple keys per
  table, unreferenced-table no-op, re-prepare-from-raw-input + re-key,
  continue-on-error, write-time baseline propagation, ``cache_config``
  reassignment clears, dry_run/explain bypass, mutation isolation,
  per-engine isolation, multi-stage + run-by-name caching, evict/clear,
  sync wrappers, evict-never-connects, identity-guarded refresh commit.
"""

import asyncio
import sqlite3

import pydantic
import pytest
import sqlalchemy

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.policy import ColumnFilterRule, SessionPolicy
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.cache import (
    CacheConfig,
    QueryCache,
    RefreshResult,
    _CacheEntry,
)
from slayer.engine.query_engine import SlayerQueryEngine, _sql_client_cache_key
from slayer.sql.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeClock:
    """Deterministic monotonic clock for TTL tests."""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _make_entry(*, created_at: float = 0.0, sql: str = "SELECT 1", response=None) -> _CacheEntry:
    return _CacheEntry(
        response=response,
        sql=sql,
        ds_fingerprint="fp",
        dialect="sqlite",
        ds_key=("conn", "rt"),
        created_at=created_at,
    )


def _seed_db(db_path) -> None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            amount REAL NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (1, "completed", 100.0, "2025-01-01"),
            (2, "pending", 50.0, "2025-01-02"),
            (3, "completed", 200.0, "2025-01-03"),
        ],
    )
    # A second physical table used only by the "unreferenced-table" test.
    cur.execute("CREATE TABLE aux (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")
    cur.executemany("INSERT INTO aux VALUES (?, ?)", [(1, 10), (2, 20)])
    conn.commit()
    conn.close()


def _orders_model(ds: str = "ds") -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source=ds,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
        ],
    )


async def _build_engine(tmp_path, *, cache_config=None, ds_name="ds", storage_suffix=""):
    db = tmp_path / "orders.db"
    if not db.exists():
        _seed_db(db)
    sdir = tmp_path / f"storage{storage_suffix}"
    sdir.mkdir(exist_ok=True)
    storage = YAMLStorage(base_dir=str(sdir))
    await storage.save_datasource(
        DatasourceConfig(name=ds_name, type="sqlite", database=str(db))
    )
    await storage.save_model(_orders_model(ds_name))
    return SlayerQueryEngine(storage=storage, cache_config=cache_config)


def _sum_query() -> SlayerQuery:
    return SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="amount:sum")])


def _mutate(db_path, sql: str) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(sql)
    conn.commit()
    conn.close()


def _install_spy(monkeypatch):
    """Record every SQL string sent through ``SlayerSQLClient.execute``."""
    calls: list[str] = []
    orig = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await orig(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


def _data_queries(calls):
    return [s for s in calls if "slayer_rk_" not in s]


def _scan_queries(calls):
    return [s for s in calls if "slayer_rk_" in s]


def _fingerprint(ds: DatasourceConfig) -> str:
    return "|".join(_sql_client_cache_key(ds))


# ===========================================================================
# Part A — pure QueryCache (no DB)
# ===========================================================================


class TestKeyAndClock:
    def test_make_key_stable_and_ds_sensitive(self):
        k1 = QueryCache.make_key("SELECT 1", "fpA")
        k2 = QueryCache.make_key("SELECT 1", "fpA")
        k3 = QueryCache.make_key("SELECT 1", "fpB")
        k4 = QueryCache.make_key("SELECT 2", "fpA")
        assert k1 == k2
        assert k1 != k3  # different datasource fingerprint
        assert k1 != k4  # different SQL

    async def test_ttl_expiry_with_fake_clock(self):
        clk = FakeClock()
        c = QueryCache(CacheConfig(ttl_seconds=100), clock=clk)
        e = _make_entry(created_at=c.now())
        await c.put("k", e)
        clk.advance(50)
        assert (await c.get("k")) is e
        clk.advance(60)  # age 110 > 100
        assert (await c.get("k")) is None
        assert c.size() == 0  # expired entry dropped

    async def test_ttl_none_never_expires(self):
        clk = FakeClock()
        c = QueryCache(CacheConfig(ttl_seconds=None), clock=clk)
        e = _make_entry(created_at=c.now())
        await c.put("k", e)
        clk.advance(10_000_000)
        assert (await c.get("k")) is e

    async def test_get_miss_returns_none(self):
        c = QueryCache(CacheConfig())
        assert (await c.get("nope")) is None

    async def test_put_delete_size_clear(self):
        c = QueryCache(CacheConfig())
        await c.put("a", _make_entry())
        await c.put("b", _make_entry())
        assert c.size() == 2
        assert (await c.delete("a")) is True
        assert (await c.delete("a")) is False
        assert c.size() == 1
        c.clear()
        assert c.size() == 0


class TestCommitReplace:
    async def test_identity_guard_and_rekey(self):
        c = QueryCache(CacheConfig())
        e1 = _make_entry(sql="a")
        e2 = _make_entry(sql="a2")
        await c.put("k", e1)
        assert await c.commit_replace(old_key="k", expected=e1, new_key="k", new_entry=e2) is True
        assert c.size() == 1
        # Live is now e2; a stale expected pointer must be refused.
        e3 = _make_entry(sql="a3")
        assert await c.commit_replace(old_key="k", expected=e1, new_key="k", new_entry=e3) is False
        # Re-key move.
        e4 = _make_entry(sql="b")
        assert await c.commit_replace(old_key="k", expected=e2, new_key="k2", new_entry=e4) is True
        assert c.size() == 1
        assert (await c.get("k2")) is e4
        assert (await c.get("k")) is None


class TestTableParsing:
    def test_excludes_cte_and_subquery_aliases(self):
        c = QueryCache(CacheConfig())
        sql = (
            "WITH cte AS (SELECT id FROM public.orders) "
            "SELECT * FROM cte JOIN (SELECT 1 AS x) sub ON TRUE"
        )
        names = [t[2] for t in c.parse_referenced_tables(sql, "postgres")]
        assert "orders" in names
        assert "cte" not in names
        assert "sub" not in names

    def test_values_clause_has_no_physical_table(self):
        c = QueryCache(CacheConfig())
        names = [t[2] for t in c.parse_referenced_tables(
            "SELECT * FROM (VALUES (1), (2)) AS v(x)", "postgres"
        )]
        assert "v" not in names


class TestWildcardMatching:
    def test_unqualified_config_matches_any_qualifier_pg(self):
        c = QueryCache(CacheConfig(refresh_keys=[("orders", "COUNT(*)")]))
        for frm in ("orders", "public.orders", "db.public.orders"):
            assert c.applicable_keys(f"SELECT * FROM {frm}", "postgres") == [
                ("orders", "COUNT(*)")
            ]

    def test_schema_qualified_config_requires_schema_pg(self):
        c = QueryCache(CacheConfig(refresh_keys=[("public.orders", "COUNT(*)")]))
        assert c.applicable_keys("SELECT * FROM public.orders", "postgres") == [
            ("public.orders", "COUNT(*)")
        ]
        assert c.applicable_keys("SELECT * FROM other.orders", "postgres") == []
        assert c.applicable_keys("SELECT * FROM orders", "postgres") == []

    def test_snowflake_unquoted_folds_upper(self):
        c = QueryCache(CacheConfig(refresh_keys=[("orders", "COUNT(*)")]))
        assert c.applicable_keys("SELECT * FROM orders", "snowflake") == [
            ("orders", "COUNT(*)")
        ]
        # Quoted lowercase is preserved (orders != ORDERS) → no match.
        assert c.applicable_keys('SELECT * FROM "orders"', "snowflake") == []

    def test_quoted_config_preserved_exactly(self):
        c = QueryCache(CacheConfig(refresh_keys=[('"Orders"', "COUNT(*)")]))
        assert c.applicable_keys('SELECT * FROM "Orders"', "snowflake") == [
            ('"Orders"', "COUNT(*)")
        ]
        assert c.applicable_keys("SELECT * FROM orders", "snowflake") == []

    def test_bigquery_dotted_paths(self):
        c = QueryCache(CacheConfig(refresh_keys=[("dataset.orders", "COUNT(*)")]))
        assert c.applicable_keys(
            "SELECT * FROM `project`.`dataset`.`orders`", "bigquery"
        ) == [("dataset.orders", "COUNT(*)")]
        assert c.applicable_keys(
            "SELECT * FROM `project`.`other`.`orders`", "bigquery"
        ) == []

    def test_sqlserver_schema_qualified(self):
        c = QueryCache(CacheConfig(refresh_keys=[("dbo.orders", "COUNT(*)")]))
        assert c.applicable_keys("SELECT * FROM dbo.orders", "tsql") == [
            ("dbo.orders", "COUNT(*)")
        ]

    def test_applicable_keeps_duplicate_expressions(self):
        c = QueryCache(
            CacheConfig(
                refresh_keys=[("orders", "MAX(updated_at)"), ("orders", "COUNT(*)")]
            )
        )
        assert c.applicable_keys("SELECT * FROM orders", "postgres") == [
            ("orders", "MAX(updated_at)"),
            ("orders", "COUNT(*)"),
        ]

    def test_unreferenced_table_not_applicable(self):
        c = QueryCache(
            CacheConfig(refresh_keys=[("orders", "COUNT(*)"), ("aux", "COUNT(*)")])
        )
        assert c.applicable_keys("SELECT * FROM orders", "postgres") == [
            ("orders", "COUNT(*)")
        ]


class TestScanSqlAndValues:
    def test_build_scan_sql_postgres(self):
        c = QueryCache(CacheConfig())
        sql = c.build_refresh_key_sql(
            "public.orders", ["MAX(updated_at)", "COUNT(*)"], "postgres"
        )
        assert 'AS "slayer_rk_0"' in sql
        assert 'AS "slayer_rk_1"' in sql
        assert "MAX(updated_at)" in sql
        assert "COUNT(*)" in sql
        assert "public.orders" in sql

    def test_build_scan_sql_mysql_backticks(self):
        c = QueryCache(CacheConfig())
        sql = c.build_refresh_key_sql("orders", ["COUNT(*)"], "mysql")
        assert "`slayer_rk_0`" in sql

    def test_values_differ(self):
        assert QueryCache.values_differ(1, 2) is True
        assert QueryCache.values_differ(1, 1) is False
        assert QueryCache.values_differ("2025-01-03", "2025-01-04") is True
        assert QueryCache.values_differ(3, 2) is True  # detects a decrease


# ===========================================================================
# Part B — engine integration (real in-process SQLite)
# ===========================================================================


class TestBasicCaching:
    async def test_miss_then_hit_serves_stale_without_reexec(self, tmp_path, monkeypatch):
        engine = await _build_engine(tmp_path)
        calls = _install_spy(monkeypatch)
        q = _sum_query()

        r1 = await engine.execute(q, cache=True)
        assert r1.data[0]["orders.amount_sum"] == pytest.approx(350.0)
        assert engine.cache_size == 1
        assert len(_data_queries(calls)) == 1  # the miss ran one data query

        calls.clear()
        _mutate(tmp_path / "orders.db", "INSERT INTO orders VALUES (4, 'pending', 1000.0, '2025-02-01')")

        r2 = await engine.execute(q, cache=True)
        assert r2.data[0]["orders.amount_sum"] == pytest.approx(350.0)  # stale
        assert _data_queries(calls) == []  # hit → no DB execution

        r3 = await engine.execute(q, cache=False)
        assert r3.data[0]["orders.amount_sum"] == pytest.approx(1350.0)  # bypass → fresh
        assert len(_data_queries(calls)) == 1

    async def test_dry_run_and_explain_never_cached(self, tmp_path):
        engine = await _build_engine(tmp_path)
        q = _sum_query()
        await engine.execute(q, cache=True, dry_run=True)
        assert engine.cache_size == 0
        try:
            await engine.execute(q, cache=True, explain=True)
        except Exception:
            pass  # some dialects can't EXPLAIN; the point is nothing is cached
        assert engine.cache_size == 0

    async def test_returned_response_is_independent_copy(self, tmp_path):
        engine = await _build_engine(tmp_path)
        q = _sum_query()
        r1 = await engine.execute(q, cache=True)
        r1.data[0]["orders.amount_sum"] = 999999.0
        r1.data.append({"orders.amount_sum": 0.0})
        r2 = await engine.execute(q, cache=True)  # hit
        assert r2.data[0]["orders.amount_sum"] == pytest.approx(350.0)
        assert len(r2.data) == 1

    async def test_per_engine_isolation(self, tmp_path):
        engine_a = await _build_engine(tmp_path, storage_suffix="_a")
        engine_b = await _build_engine(tmp_path, storage_suffix="_b")
        await engine_a.execute(_sum_query(), cache=True)
        assert engine_a.cache_size == 1
        assert engine_b.cache_size == 0

    async def test_cache_config_reassign_clears(self, tmp_path):
        engine = await _build_engine(tmp_path)
        await engine.execute(_sum_query(), cache=True)
        assert engine.cache_size == 1
        engine.cache_config = CacheConfig(ttl_seconds=5)
        assert engine.cache_size == 0
        assert engine.cache_config.ttl_seconds == 5


class TestTtlLazyReexec:
    async def test_ttl_lazy_reexec_on_read(self, tmp_path, monkeypatch):
        clk = FakeClock()
        engine = await _build_engine(tmp_path)
        engine._cache = QueryCache(config=CacheConfig(ttl_seconds=100), clock=clk)
        q = _sum_query()

        await engine.execute(q, cache=True)  # populate at t=1000
        calls = _install_spy(monkeypatch)

        clk.advance(50)
        await engine.execute(q, cache=True)  # still fresh → hit
        assert _data_queries(calls) == []

        clk.advance(60)  # age 110 > 100 → expired → re-exec
        _mutate(tmp_path / "orders.db", "INSERT INTO orders VALUES (4, 'pending', 1000.0, '2025-02-01')")
        r = await engine.execute(q, cache=True)
        assert len(_data_queries(calls)) == 1
        assert r.data[0]["orders.amount_sum"] == pytest.approx(1350.0)


class TestRefresh:
    async def test_baseline_scanned_before_data_query(self, tmp_path, monkeypatch):
        engine = await _build_engine(
            tmp_path,
            cache_config=CacheConfig(refresh_keys=[("orders", "MAX(updated_at)")]),
        )
        # Warm up the client (no cache) so the spy sees the cached miss cleanly.
        await engine.execute(_sum_query(), cache=False)
        calls = _install_spy(monkeypatch)

        await engine.execute(_sum_query(), cache=True)
        # First recorded query is the refresh-key baseline scan; the data
        # query follows.
        assert "slayer_rk_" in calls[0]
        assert "slayer_rk_" not in calls[1]

    async def test_refresh_key_change_refreshes(self, tmp_path):
        engine = await _build_engine(
            tmp_path,
            cache_config=CacheConfig(refresh_keys=[("orders", "MAX(updated_at)")]),
        )
        q = _sum_query()
        await engine.execute(q, cache=True)

        # A later-timestamped insert moves MAX(updated_at).
        _mutate(tmp_path / "orders.db", "INSERT INTO orders VALUES (4, 'pending', 1000.0, '2025-06-01')")
        result = await engine.refresh()
        assert isinstance(result, RefreshResult)
        assert len(result.refreshed) == 1
        assert result.expired_refreshed == []
        assert result.unchanged == []
        assert result.errors == []

        hit = await engine.execute(q, cache=True)
        assert hit.data[0]["orders.amount_sum"] == pytest.approx(1350.0)

    async def test_refresh_unchanged_when_key_static(self, tmp_path):
        engine = await _build_engine(
            tmp_path,
            cache_config=CacheConfig(refresh_keys=[("orders", "MAX(updated_at)")]),
        )
        q = _sum_query()
        await engine.execute(q, cache=True)
        result = await engine.refresh()
        assert len(result.unchanged) == 1
        assert result.refreshed == []
        assert result.expired_refreshed == []

    async def test_count_key_catches_delete_that_max_misses(self, tmp_path):
        engine = await _build_engine(
            tmp_path,
            cache_config=CacheConfig(
                refresh_keys=[("orders", "MAX(updated_at)"), ("orders", "COUNT(*)")]
            ),
        )
        q = _sum_query()
        await engine.execute(q, cache=True)

        # Delete a NON-max row: MAX(updated_at) stays 2025-01-03, COUNT drops.
        _mutate(tmp_path / "orders.db", "DELETE FROM orders WHERE id = 2")
        result = await engine.refresh()
        assert len(result.refreshed) == 1  # COUNT(*) moved even though MAX didn't
        hit = await engine.execute(q, cache=True)
        assert hit.data[0]["orders.amount_sum"] == pytest.approx(300.0)

    async def test_unreferenced_table_change_no_refresh(self, tmp_path):
        engine = await _build_engine(
            tmp_path,
            cache_config=CacheConfig(
                refresh_keys=[("orders", "MAX(updated_at)"), ("aux", "COUNT(*)")]
            ),
        )
        q = _sum_query()  # references orders only
        await engine.execute(q, cache=True)
        _mutate(tmp_path / "orders.db", "INSERT INTO aux VALUES (3, 30)")
        result = await engine.refresh()
        assert len(result.unchanged) == 1
        assert result.refreshed == []

    async def test_refresh_reprepares_and_rekeys_on_model_edit(self, tmp_path):
        clk = FakeClock()
        engine = await _build_engine(tmp_path)
        engine._cache = QueryCache(config=CacheConfig(ttl_seconds=100), clock=clk)
        q = _sum_query()

        ds = await engine.storage.get_datasource("ds")
        fp = _fingerprint(ds)
        old_sql = (await engine.execute(q, dry_run=True)).sql
        old_key = QueryCache.make_key(old_sql, fp)

        await engine.execute(q, cache=True)
        assert old_key in engine._cache._entries

        # Edit the stored model: an always-applied filter changes the SQL.
        edited = _orders_model("ds").model_copy(update={"filters": ["amount > 60"]})
        await engine.storage.save_model(edited)
        new_sql = (await engine.execute(q, dry_run=True)).sql
        new_key = QueryCache.make_key(new_sql, fp)
        assert new_key != old_key

        clk.advance(200)  # TTL expired → refresh re-preps from raw input
        result = await engine.refresh()
        assert old_key in result.expired_refreshed
        assert engine.cache_size == 1
        assert new_key in engine._cache._entries
        assert old_key not in engine._cache._entries

    async def test_refresh_continue_on_scan_error_keeps_entry(self, tmp_path):
        engine = await _build_engine(
            tmp_path,
            cache_config=CacheConfig(refresh_keys=[("orders", "MAX(updated_at)")]),
        )
        q = _sum_query()
        await engine.execute(q, cache=True)  # baseline captured while table exists
        assert engine.cache_size == 1

        # Drop the table: the refresh-key scan now fails.
        _mutate(tmp_path / "orders.db", "DROP TABLE orders")
        result = await engine.refresh()
        assert any(e.phase == "refresh_key_scan" for e in result.errors)
        assert len(result.unchanged) == 1  # entry kept, not re-executed
        assert engine.cache_size == 1
        # The stale value is still served from cache (no DB touched).
        hit = await engine.execute(q, cache=True)
        assert hit.data[0]["orders.amount_sum"] == pytest.approx(350.0)

    async def test_ttl_expired_with_scan_failure_keeps_entry(self, tmp_path):
        # TTL expiry takes precedence over the refresh-key scan, so the entry
        # is re-executed; when re-execution's own baseline scan then fails
        # (table gone), the stale entry is kept via the re_execute error path.
        clk = FakeClock()
        engine = await _build_engine(tmp_path)
        engine._cache = QueryCache(
            config=CacheConfig(
                ttl_seconds=100, refresh_keys=[("orders", "MAX(updated_at)")]
            ),
            clock=clk,
        )
        q = _sum_query()
        await engine.execute(q, cache=True)

        _mutate(tmp_path / "orders.db", "DROP TABLE orders")
        clk.advance(200)  # TTL expired → re-exec attempted → its scan fails
        result = await engine.refresh()
        assert any(e.phase == "re_execute" for e in result.errors)
        assert result.expired_refreshed == []
        assert engine.cache_size == 1  # stale entry kept, not evicted by refresh

    async def test_refresh_uses_snapshot_of_mutated_input(self, tmp_path):
        # The cached entry deep-copies the original input, so mutating the
        # caller's SlayerQuery after the write must not change refresh replay.
        clk = FakeClock()
        engine = await _build_engine(tmp_path)
        engine._cache = QueryCache(config=CacheConfig(ttl_seconds=100), clock=clk)
        q = _sum_query()
        await engine.execute(q, cache=True)

        ds = await engine.storage.get_datasource("ds")
        fp = _fingerprint(ds)
        original_sql = (await engine.execute(q, dry_run=True)).sql
        original_key = QueryCache.make_key(original_sql, fp)

        # Mutate the caller's query object in place.
        q.filters = ["amount > 60"]

        clk.advance(200)  # TTL expired → refresh replays the stored copy
        await engine.refresh()
        # Replay used the pre-mutation input → same key, not the mutated one.
        assert original_key in engine._cache._entries
        assert engine.cache_size == 1

    async def test_write_time_baseline_failure_propagates(self, tmp_path):
        engine = await _build_engine(
            tmp_path,
            cache_config=CacheConfig(refresh_keys=[("orders", "MAX(nonexistent_col)")]),
        )
        q = _sum_query()
        with pytest.raises(sqlalchemy.exc.SQLAlchemyError):
            await engine.execute(q, cache=True)
        assert engine.cache_size == 0  # nothing stored on a failed baseline scan

    async def test_identity_guarded_commit_not_resurrected(self, tmp_path):
        clk = FakeClock()
        engine = await _build_engine(tmp_path)
        engine._cache = QueryCache(config=CacheConfig(ttl_seconds=100), clock=clk)
        q = _sum_query()
        await engine.execute(q, cache=True)

        orig_reexec = engine._reexecute_entry

        async def clearing_reexec(entry, now):
            out = await orig_reexec(entry, now)
            engine.clear_cache()  # a concurrent clear during refresh's awaits
            return out

        engine._reexecute_entry = clearing_reexec

        clk.advance(200)  # TTL expired → re-exec path
        await engine.refresh()
        # The entry was cleared mid-refresh; the identity guard must NOT
        # resurrect it under the new key.
        assert engine.cache_size == 0


class TestMultiStageAndByName:
    async def test_multi_stage_list_caching(self, tmp_path, monkeypatch):
        engine = await _build_engine(tmp_path)
        inner = SlayerQuery(
            source_model="orders",
            name="raw",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="amount")],
            distinct_dimension_values=False,
        )
        outer = SlayerQuery(
            source_model="raw", measures=[ModelMeasure(formula="amount:sum")]
        )
        r1 = await engine.execute([inner, outer], cache=True)
        assert engine.cache_size == 1
        total = r1.data[0]["raw.amount_sum"]
        assert total == pytest.approx(350.0)

        calls = _install_spy(monkeypatch)
        r2 = await engine.execute([inner, outer], cache=True)  # hit
        assert _data_queries(calls) == []
        assert r2.data[0]["raw.amount_sum"] == total

    async def test_run_by_name_caching(self, tmp_path, monkeypatch):
        engine = await _build_engine(tmp_path)
        await engine.create_model_from_query(
            query=SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="amount:sum")],
                dimensions=[ColumnRef(name="status")],
            ),
            name="orders_by_status",
        )
        r1 = await engine.execute("orders_by_status", cache=True)
        assert engine.cache_size == 1

        calls = _install_spy(monkeypatch)
        r2 = await engine.execute("orders_by_status", cache=True)  # hit
        assert _data_queries(calls) == []
        assert len(r2.data) == len(r1.data)

    async def test_refresh_by_name_picks_up_source_queries_edit(self, tmp_path):
        clk = FakeClock()
        engine = await _build_engine(tmp_path)
        engine._cache = QueryCache(config=CacheConfig(ttl_seconds=100), clock=clk)
        await engine.create_model_from_query(
            query=SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="amount:sum")],
                dimensions=[ColumnRef(name="status")],
            ),
            name="obs",
        )
        await engine.execute("obs", cache=True)
        assert engine.cache_size == 1

        # Edit the backing source_queries: filter out the 'pending' rows.
        await engine.save_model(
            SlayerModel(
                name="obs",
                source_queries=[
                    SlayerQuery(
                        source_model="orders",
                        measures=[ModelMeasure(formula="amount:sum")],
                        dimensions=[ColumnRef(name="status")],
                        filters=["status != 'pending'"],
                    )
                ],
            )
        )
        clk.advance(200)
        result = await engine.refresh()
        assert len(result.expired_refreshed) == 1
        assert engine.cache_size == 1
        hit = await engine.execute("obs", cache=True)
        status_key = next(k for k in hit.data[0] if k.endswith(".status"))
        statuses = {row[status_key] for row in hit.data}
        assert "pending" not in statuses


class TestEvictAndManagement:
    async def test_evict_and_clear_and_size(self, tmp_path):
        engine = await _build_engine(tmp_path)
        q1 = _sum_query()
        q2 = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        await engine.execute(q1, cache=True)
        await engine.execute(q2, cache=True)
        assert engine.cache_size == 2
        assert (await engine.evict(q1)) is True
        assert engine.cache_size == 1
        assert (await engine.evict(q1)) is False  # already gone
        engine.clear_cache()
        assert engine.cache_size == 0

    async def test_evict_never_constructs_a_client(self, tmp_path, monkeypatch):
        engine = await _build_engine(tmp_path)

        class ExplodingClient:
            def __init__(self, *a, **k):
                raise AssertionError("evict must not construct a SQL client")

        monkeypatch.setattr("slayer.engine.query_engine.SlayerSQLClient", ExplodingClient)
        # Nothing cached, no client cached → evict resolves the key (DB-free)
        # and finds no entry, all without connecting.
        assert (await engine.evict(_sum_query())) is False


class TestSyncWrappers:
    def test_execute_sync_evict_sync_refresh_sync(self, tmp_path):
        engine = asyncio.run(
            _build_engine(
                tmp_path,
                cache_config=CacheConfig(refresh_keys=[("orders", "MAX(updated_at)")]),
            )
        )
        q = _sum_query()
        r = engine.execute_sync(q, cache=True, data_source="ds")
        assert r.data[0]["orders.amount_sum"] == pytest.approx(350.0)
        assert engine.cache_size == 1
        result = engine.refresh_sync()
        assert isinstance(result, RefreshResult)
        assert len(result.unchanged) == 1
        assert engine.evict_sync(q, data_source="ds") is True
        assert engine.cache_size == 0


class TestCacheConfigFrozen:
    def test_frozen_and_refresh_keys_coerced_to_tuple(self):
        cfg = CacheConfig(ttl_seconds=5, refresh_keys=[("orders", "COUNT(*)")])
        assert isinstance(cfg.refresh_keys, tuple)
        assert cfg.refresh_keys == (("orders", "COUNT(*)"),)
        assert CacheConfig().refresh_keys == ()
        # Frozen: in-place mutation is rejected (must reassign cache_config,
        # which clears the cache) rather than silently bypassing the setter.
        with pytest.raises(pydantic.ValidationError):
            cfg.ttl_seconds = 10


class TestPolicyInteraction:
    async def test_refresh_key_scan_is_policy_scoped(self, tmp_path, monkeypatch):
        # DEV-1587 × DEV-1578: with a forced-filter policy the refresh-key
        # baseline scan must be rewritten to the same tenant scope as the data
        # query, so a global MAX/COUNT can't mask a tenant-local change.
        engine = await _build_engine(
            tmp_path,
            cache_config=CacheConfig(refresh_keys=[("orders", "MAX(updated_at)")]),
        )
        engine.policy = SessionPolicy(
            data_filters=[ColumnFilterRule(column="status", value="completed")]
        )
        calls = _install_spy(monkeypatch)
        await engine.execute(_sum_query(), cache=True)

        scans = _scan_queries(calls)
        assert scans, "expected a refresh-key scan on the cache miss"
        # Both the scan AND the data query are tenant-scoped (reference the
        # policy column), so their notions of "current state" agree.
        assert all("status" in s for s in scans)
        assert all("status" in s for s in _data_queries(calls))
