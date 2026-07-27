"""Unit tests for the engine-side column-presence probe (DEV-1578).

``SlayerQueryEngine._column_present`` is the ``has_column`` provider for the
forced-filter rewrite: it introspects via ``_safe_get_columns``, matches the
column case-insensitively, resolves the schema (AST schema else datasource
default), returns ``None`` on any introspection failure/empty (UNCACHED), and
caches only confirmed ``True``/``False``. These tests mock ``_safe_get_columns``
so no live schema is required (an empty sqlite file backs ``get_engine`` /
``sa.inspect``).
"""

import logging

import pytest

import slayer.engine.query_engine as qe
from slayer.core.errors import ForcedFilterError
from slayer.core.models import DatasourceConfig
from slayer.core.policy import (
    ColumnFilterRule,
    JoinFilterRule,
    SessionPolicy,
)
from slayer.engine.query_engine import SlayerQueryEngine, _sql_client_cache_key
from slayer.sql.client import SlayerSQLClient
from slayer.sql.session_policy import ScopedTable
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def engine(tmp_path):
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    return SlayerQueryEngine(storage=storage, policy=policy)


def _ds(tmp_path, *, schema_name=None):
    return DatasourceConfig(
        name="ds1",
        type="sqlite",
        database=str(tmp_path / "probe.db"),
        schema_name=schema_name,
    )


def test_column_present_true(engine, tmp_path, monkeypatch):
    monkeypatch.setattr(
        qe, "_safe_get_columns", lambda *a, **k: [{"name": "org"}, {"name": "id"}]
    )
    present = engine._column_present(
        datasource=_ds(tmp_path), scoped_table=ScopedTable(name="orders"), column="org"
    )
    assert present is True


def test_column_present_false(engine, tmp_path, monkeypatch):
    monkeypatch.setattr(
        qe, "_safe_get_columns", lambda *a, **k: [{"name": "id"}, {"name": "amount"}]
    )
    present = engine._column_present(
        datasource=_ds(tmp_path), scoped_table=ScopedTable(name="orders"), column="org"
    )
    assert present is False


def test_column_present_case_insensitive(engine, tmp_path, monkeypatch):
    monkeypatch.setattr(
        qe, "_safe_get_columns", lambda *a, **k: [{"name": "Organization_UUID"}]
    )
    present = engine._column_present(
        datasource=_ds(tmp_path),
        scoped_table=ScopedTable(name="orders"),
        column="organization_uuid",
    )
    assert present is True


def test_column_present_none_on_empty(engine, tmp_path, monkeypatch):
    monkeypatch.setattr(qe, "_safe_get_columns", lambda *a, **k: [])
    present = engine._column_present(
        datasource=_ds(tmp_path), scoped_table=ScopedTable(name="orders"), column="org"
    )
    assert present is None


def test_column_present_none_on_introspection_error(engine, tmp_path, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("introspection blew up")

    monkeypatch.setattr(qe, "_safe_get_columns", boom)
    present = engine._column_present(
        datasource=_ds(tmp_path), scoped_table=ScopedTable(name="orders"), column="org"
    )
    assert present is None


def test_confirmed_result_is_cached(engine, tmp_path, monkeypatch):
    calls = {"n": 0}

    def counting(*a, **k):
        calls["n"] += 1
        return [{"name": "org"}]

    monkeypatch.setattr(qe, "_safe_get_columns", counting)
    ds = _ds(tmp_path)
    st = ScopedTable(name="orders")
    assert engine._column_present(datasource=ds, scoped_table=st, column="org") is True
    assert engine._column_present(datasource=ds, scoped_table=st, column="org") is True
    assert calls["n"] == 1  # second call served from cache


def test_none_result_is_not_cached(engine, tmp_path, monkeypatch):
    """A transient None (can't confirm) must be re-probed, not cached."""
    seq = iter([[], [{"name": "org"}]])  # first empty, then recovers

    def flaky(*a, **k):
        return next(seq)

    monkeypatch.setattr(qe, "_safe_get_columns", flaky)
    ds = _ds(tmp_path)
    st = ScopedTable(name="orders")
    assert engine._column_present(datasource=ds, scoped_table=st, column="org") is None
    # recovered: re-probe (proves the None wasn't cached)
    assert engine._column_present(datasource=ds, scoped_table=st, column="org") is True


def test_cross_catalog_fails_closed(engine, tmp_path, monkeypatch):
    """A ref naming a catalog other than the connection's own can't be
    confirmed by schema-only introspection -> fail closed, without probing."""
    calls = {"n": 0}

    def counting(*a, **k):
        calls["n"] += 1
        return [{"name": "org"}]

    monkeypatch.setattr(qe, "_safe_get_columns", counting)
    ds = _ds(tmp_path)  # database is the probe.db path
    present = engine._column_present(
        datasource=ds,
        scoped_table=ScopedTable(catalog="other_project", name="orders"),
        column="org",
    )
    assert present is None
    assert calls["n"] == 0  # never probed the wrong relation


def test_matching_catalog_introspects_normally(engine, tmp_path, monkeypatch):
    """A ref whose catalog equals the connection's own catalog probes
    normally (no over-blocking)."""
    monkeypatch.setattr(qe, "_safe_get_columns", lambda *a, **k: [{"name": "org"}])
    ds = _ds(tmp_path)
    present = engine._column_present(
        datasource=ds,
        # case-insensitive match against datasource.database
        scoped_table=ScopedTable(catalog=ds.database.upper(), name="orders"),
        column="org",
    )
    assert present is True


def test_schema_resolves_ast_first(engine, tmp_path, monkeypatch):
    seen = {}

    def capture(inspector, sa_engine, table_name, schema):
        seen["schema"] = schema
        return [{"name": "org"}]

    monkeypatch.setattr(qe, "_safe_get_columns", capture)
    engine._column_present(
        datasource=_ds(tmp_path, schema_name="ds_default"),
        scoped_table=ScopedTable(schema_name="ast_schema", name="orders"),
        column="org",
    )
    assert seen["schema"] == "ast_schema"  # AST schema wins


def test_schema_falls_back_to_datasource_default(engine, tmp_path, monkeypatch):
    seen = {}

    def capture(inspector, sa_engine, table_name, schema):
        seen["schema"] = schema
        return [{"name": "org"}]

    monkeypatch.setattr(qe, "_safe_get_columns", capture)
    engine._column_present(
        datasource=_ds(tmp_path, schema_name="ds_default"),
        scoped_table=ScopedTable(name="orders"),  # no AST schema
        column="org",
    )
    assert seen["schema"] == "ds_default"


# ===========================================================================
# ClickHouse correlated-subquery version gate (DEV-1627)
# ===========================================================================


def _join_policy():
    return SessionPolicy(
        data_filters=[
            # Mandatory block backstop (DEV-1627). orders is join-targeted, so
            # this column rule is overridden for it (never consulted).
            ColumnFilterRule(column="organization_uuid", value="orgA"),
            JoinFilterRule(
                target_table="orders",
                join_path=["orders.customer_id = customers.id"],
                column="organization_uuid",
                value="orgA",
            )
        ]
    )


def _ch_ds():
    return DatasourceConfig(
        name="ch1",
        type="clickhouse",
        host="localhost",
        port=9000,
        database="default",
    )


@pytest.fixture
def join_engine(tmp_path):
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    return SlayerQueryEngine(storage=storage, policy=_join_policy())


# -- version parsing ---------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("25.4.1.100", (25, 4)),  # NOSONAR(S1313) — ClickHouse version, not an IP
        ("25.4", (25, 4)),
        ("25.10.2.1", (25, 10)),  # NOSONAR(S1313) — ClickHouse version, not an IP; 25.10 > 25.4
        ("24.8.14.10459", (24, 8)),
        ("25.4.1-lts", (25, 4)),  # prerelease/build suffix
        ("v25.4.1", (25, 4)),  # leading v tolerated
    ],
)
def test_parse_clickhouse_version_valid(raw, expected):
    assert SlayerQueryEngine._parse_clickhouse_version(raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", "garbage", None, "abc.def"])
def test_parse_clickhouse_version_unparseable_is_none(raw):
    assert SlayerQueryEngine._parse_clickhouse_version(raw) is None


# -- guard behaviour ---------------------------------------------------------


@pytest.mark.parametrize("version", [(24, 8), (25, 3), (25, 4), (25, 10)])
def test_guard_gate_by_version(join_engine, version, caplog):
    ds = _ch_ds()
    join_engine._ch_version_cache[_sql_client_cache_key(ds)] = version
    guard = join_engine._clickhouse_correlated_guard(dialect="clickhouse", datasource=ds)
    assert guard is not None
    if version < (25, 4):
        with pytest.raises(ForcedFilterError):
            guard()
    else:
        with caplog.at_level(logging.WARNING):
            guard()  # supported -> warns, does not raise
        assert any(
            "correlated" in r.message.lower()
            or "experimental" in r.message.lower()
            for r in caplog.records
        )


def test_guard_none_version_fails_closed(join_engine):
    ds = _ch_ds()
    join_engine._ch_version_cache[_sql_client_cache_key(ds)] = None
    guard = join_engine._clickhouse_correlated_guard(dialect="clickhouse", datasource=ds)
    with pytest.raises(ForcedFilterError):
        guard()


def test_guard_missing_cache_entry_fails_closed(join_engine):
    """No cached version (preflight never ran / failed) -> fail closed."""
    ds = _ch_ds()
    guard = join_engine._clickhouse_correlated_guard(dialect="clickhouse", datasource=ds)
    with pytest.raises(ForcedFilterError):
        guard()


def test_guard_is_none_for_non_clickhouse(join_engine, tmp_path):
    ds = _ds(tmp_path)  # sqlite
    guard = join_engine._clickhouse_correlated_guard(dialect="sqlite", datasource=ds)
    assert guard is None


# -- async version preflight -------------------------------------------------


async def test_preflight_probes_and_caches_version(join_engine, monkeypatch):
    calls = {"n": 0}

    async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async: replaces the async SlayerSQLClient.execute
        calls["n"] += 1
        assert "version" in sql.lower()
        return [{"version()": "25.4.1.100"}]  # NOSONAR(S1313) — ClickHouse version, not an IP

    monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
    ds = _ch_ds()
    await join_engine._preflight_clickhouse_correlated(dialect="clickhouse", datasource=ds)
    assert join_engine._ch_version_cache[_sql_client_cache_key(ds)] == (25, 4)
    # cached: a second preflight does not re-probe
    await join_engine._preflight_clickhouse_correlated(dialect="clickhouse", datasource=ds)
    assert calls["n"] == 1


async def test_preflight_probe_failure_caches_none(join_engine, monkeypatch):
    async def boom(self, sql, timeout_seconds=120):
        raise RuntimeError("cannot reach clickhouse")

    monkeypatch.setattr(SlayerSQLClient, "execute", boom)
    ds = _ch_ds()
    await join_engine._preflight_clickhouse_correlated(dialect="clickhouse", datasource=ds)
    assert join_engine._ch_version_cache[_sql_client_cache_key(ds)] is None


async def test_preflight_noop_for_non_clickhouse(join_engine, tmp_path, monkeypatch):
    calls = {"n": 0}

    async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async: replaces the async SlayerSQLClient.execute
        calls["n"] += 1
        return [{"version()": "25.4.1"}]

    monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
    await join_engine._preflight_clickhouse_correlated(
        dialect="sqlite", datasource=_ds(tmp_path)
    )
    assert calls["n"] == 0


async def test_preflight_noop_when_no_join_rules(tmp_path, monkeypatch):
    """A column-only policy needs no version probe even on ClickHouse."""
    calls = {"n": 0}

    async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async: replaces the async SlayerSQLClient.execute
        calls["n"] += 1
        return [{"version()": "24.8.1"}]

    monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    engine = SlayerQueryEngine(
        storage=YAMLStorage(base_dir=str(storage_dir)),
        policy=SessionPolicy(
            data_filters=[ColumnFilterRule(column="organization_uuid", value="orgA")]
        ),
    )
    await engine._preflight_clickhouse_correlated(dialect="clickhouse", datasource=_ch_ds())
    assert calls["n"] == 0


# -- _apply_policy wires the guard as on_correlated_emitted ------------------


def test_apply_policy_join_rule_fails_closed_when_version_unknown(
    join_engine, monkeypatch
):
    """_apply_policy must pass the ClickHouse guard to the rewrite: an emitted
    EXISTS with no cached version fails closed."""
    monkeypatch.setattr(join_engine, "_column_present", lambda **k: True)
    ds = _ch_ds()
    with pytest.raises(ForcedFilterError):
        join_engine._apply_policy(
            sql="SELECT * FROM orders", dialect="clickhouse", datasource=ds
        )


def test_apply_policy_join_rule_ok_when_version_supported(join_engine, monkeypatch):
    monkeypatch.setattr(join_engine, "_column_present", lambda **k: True)
    ds = _ch_ds()
    join_engine._ch_version_cache[_sql_client_cache_key(ds)] = (25, 4)
    out = join_engine._apply_policy(
        sql="SELECT * FROM orders", dialect="clickhouse", datasource=ds
    )
    assert "allow_experimental_correlated_subqueries" in out
    assert "EXISTS" in out.upper()


def test_apply_policy_column_only_clickhouse_not_blocked(tmp_path, monkeypatch):
    """A column-only policy on ClickHouse emits no correlated EXISTS, so the
    guard never fires — even with no cached version the query is not blocked."""
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    engine = SlayerQueryEngine(
        storage=YAMLStorage(base_dir=str(storage_dir)),
        policy=SessionPolicy(
            data_filters=[ColumnFilterRule(column="organization_uuid", value="orgA")]
        ),
    )
    monkeypatch.setattr(engine, "_column_present", lambda **k: True)
    out = engine._apply_policy(
        sql="SELECT * FROM orders", dialect="clickhouse", datasource=_ch_ds()
    )
    assert "allow_experimental_correlated_subqueries" not in out
    assert "organization_uuid = 'orgA'" in out  # column filter still applied
