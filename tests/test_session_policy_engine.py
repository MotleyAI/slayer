"""Unit tests for the engine-side forced-filter wiring: the ``has_column`` provider and
the ClickHouse correlated-subquery version gate. ``_safe_get_columns`` is mocked so no
live schema is required.
"""

import logging

import pytest

import slayer.engine.query_engine as qe
from slayer.core.errors import ForcedFilterError
from slayer.core.models import DatasourceConfig
from slayer.core.policy import (
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
)
from slayer.engine.query_engine import SlayerQueryEngine, _sql_client_cache_key
from slayer.sql.client import SlayerSQLClient
from slayer.sql.session_policy import ScopedTable
from slayer.storage.yaml_storage import YAMLStorage


def _mk_engine(tmp_path, policy):
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir(exist_ok=True)
    return SlayerQueryEngine(storage=YAMLStorage(base_dir=str(storage_dir)), policy=policy)


@pytest.fixture
def engine(tmp_path):
    return _mk_engine(
        tmp_path, SessionPolicy(ruleset=ColumnFilterRuleset(column="org", value="x"))
    )


def _ds(tmp_path, *, schema_name=None):
    return DatasourceConfig(
        name="ds1",
        type="sqlite",
        database=str(tmp_path / "probe.db"),
        schema_name=schema_name,
    )


# -- _column_present ---------------------------------------------------------


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
    assert calls["n"] == 1


def test_none_result_is_not_cached(engine, tmp_path, monkeypatch):
    seq = iter([[], [{"name": "org"}]])

    def flaky(*a, **k):
        return next(seq)

    monkeypatch.setattr(qe, "_safe_get_columns", flaky)
    ds = _ds(tmp_path)
    st = ScopedTable(name="orders")
    assert engine._column_present(datasource=ds, scoped_table=st, column="org") is None
    assert engine._column_present(datasource=ds, scoped_table=st, column="org") is True


def test_cross_catalog_fails_closed(engine, tmp_path, monkeypatch):
    calls = {"n": 0}

    def counting(*a, **k):
        calls["n"] += 1
        return [{"name": "org"}]

    monkeypatch.setattr(qe, "_safe_get_columns", counting)
    present = engine._column_present(
        datasource=_ds(tmp_path),
        scoped_table=ScopedTable(catalog="other_project", name="orders"),
        column="org",
    )
    assert present is None
    assert calls["n"] == 0


def test_schema_resolves_ast_first(engine, tmp_path, monkeypatch):
    seen = {}

    def capture(inspector, sa_engine, table_name, ref):
        # DEV-1758: the 4th arg is now a SchemaRef carrying the parsed schema.
        seen["schema"] = ref.name if ref else None
        return [{"name": "org"}]

    monkeypatch.setattr(qe, "_safe_get_columns", capture)
    engine._column_present(
        datasource=_ds(tmp_path, schema_name="ds_default"),
        scoped_table=ScopedTable(schema_name="ast_schema", name="orders"),
        column="org",
    )
    assert seen["schema"] == "ast_schema"


# ===========================================================================
# ClickHouse correlated-subquery version gate
# ===========================================================================


def _join_policy():
    return SessionPolicy(
        ruleset=JoinFilterRuleset(
            table="customers",
            column="organization_uuid",
            value="orgA",
            joins=[
                JoinFilterRule(
                    target_table="orders",
                    join_path=["orders.customer_id = customers.id"],
                )
            ],
        )
    )


def _ch_ds():
    return DatasourceConfig(
        name="ch1", type="clickhouse", host="localhost", port=9000, database="default"
    )


@pytest.fixture
def join_engine(tmp_path):
    return _mk_engine(tmp_path, _join_policy())


# -- _policy_has_join_rules --------------------------------------------------


def test_policy_has_join_rules_true_for_join_ruleset(join_engine):
    assert join_engine._policy_has_join_rules() is True


def test_policy_has_join_rules_false_for_column_ruleset(engine):
    assert engine._policy_has_join_rules() is False


def test_policy_has_join_rules_false_when_no_policy(tmp_path):
    eng = _mk_engine(tmp_path, None)
    assert eng._policy_has_join_rules() is False


def test_policy_has_join_rules_false_for_anchor_only(tmp_path):
    """A join ruleset with no join rules (anchor + whitelist only) emits no
    correlated EXISTS, so it needs no ClickHouse preflight."""
    eng = _mk_engine(
        tmp_path,
        SessionPolicy(
            ruleset=JoinFilterRuleset(
                table="customers",
                column="organization_uuid",
                value="orgA",
                whitelist=["exchange_rates"],
            )
        ),
    )
    assert eng._policy_has_join_rules() is False


# -- version parsing ---------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("25.4.1.100", (25, 4)),  # NOSONAR(S1313) — ClickHouse version, not an IP
        ("25.4", (25, 4)),
        ("24.8.14.10459", (24, 8)),
        ("25.4.1-lts", (25, 4)),
        ("v25.4.1", (25, 4)),
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
            guard()
        assert any(
            "correlated" in r.message.lower() or "experimental" in r.message.lower()
            for r in caplog.records
        )


def test_guard_none_version_fails_closed(join_engine):
    ds = _ch_ds()
    join_engine._ch_version_cache[_sql_client_cache_key(ds)] = None
    guard = join_engine._clickhouse_correlated_guard(dialect="clickhouse", datasource=ds)
    with pytest.raises(ForcedFilterError):
        guard()


def test_guard_missing_cache_entry_fails_closed(join_engine):
    guard = join_engine._clickhouse_correlated_guard(
        dialect="clickhouse", datasource=_ch_ds()
    )
    with pytest.raises(ForcedFilterError):
        guard()


def test_guard_is_none_for_non_clickhouse(join_engine, tmp_path):
    guard = join_engine._clickhouse_correlated_guard(
        dialect="sqlite", datasource=_ds(tmp_path)
    )
    assert guard is None


# -- async version preflight -------------------------------------------------


async def test_preflight_probes_and_caches_version(join_engine, monkeypatch):
    calls = {"n": 0}

    async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async
        calls["n"] += 1
        assert "version" in sql.lower()
        return [{"version()": "25.4.1.100"}]  # NOSONAR(S1313) — ClickHouse version

    monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
    ds = _ch_ds()
    await join_engine._preflight_clickhouse_correlated(dialect="clickhouse", datasource=ds)
    assert join_engine._ch_version_cache[_sql_client_cache_key(ds)] == (25, 4)
    await join_engine._preflight_clickhouse_correlated(dialect="clickhouse", datasource=ds)
    assert calls["n"] == 1


async def test_preflight_probe_failure_caches_none(join_engine, monkeypatch):
    async def boom(self, sql, timeout_seconds=120):
        raise RuntimeError("cannot reach clickhouse")

    monkeypatch.setattr(SlayerSQLClient, "execute", boom)
    ds = _ch_ds()
    await join_engine._preflight_clickhouse_correlated(dialect="clickhouse", datasource=ds)
    assert join_engine._ch_version_cache[_sql_client_cache_key(ds)] is None


async def test_preflight_noop_when_column_ruleset(engine, monkeypatch):
    """A column ruleset needs no version probe even on ClickHouse."""
    calls = {"n": 0}

    async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async
        calls["n"] += 1
        return [{"version()": "24.8.1"}]

    monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
    await engine._preflight_clickhouse_correlated(dialect="clickhouse", datasource=_ch_ds())
    assert calls["n"] == 0


async def test_preflight_noop_for_anchor_only_join_ruleset(tmp_path, monkeypatch):
    calls = {"n": 0}

    async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async
        calls["n"] += 1
        return [{"version()": "24.8.1"}]

    monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
    eng = _mk_engine(
        tmp_path,
        SessionPolicy(
            ruleset=JoinFilterRuleset(
                table="customers",
                column="organization_uuid",
                value="orgA",
                whitelist=["exchange_rates"],
            )
        ),
    )
    await eng._preflight_clickhouse_correlated(dialect="clickhouse", datasource=_ch_ds())
    assert calls["n"] == 0


# -- _apply_policy dispatch --------------------------------------------------


def test_apply_policy_no_policy_returns_verbatim(tmp_path):
    """With no policy the SQL is returned verbatim (zero overhead, no parse)."""
    eng = _mk_engine(tmp_path, None)
    sql = "SELECT  *  FROM   orders"
    out = eng._apply_policy(sql=sql, dialect="sqlite", datasource=_ds(tmp_path))
    assert out == sql


def test_apply_policy_column_ruleset_probes(engine, tmp_path, monkeypatch):
    monkeypatch.setattr(engine, "_column_present", lambda **k: True)
    out = engine._apply_policy(
        sql="SELECT * FROM orders", dialect="sqlite", datasource=_ds(tmp_path)
    )
    assert "WHERE org = 'x'" in out


def test_apply_policy_join_ruleset_does_not_probe(join_engine, tmp_path, monkeypatch):
    """The join path never calls _column_present."""
    def boom(**k):
        raise AssertionError("_column_present must not be called for a join ruleset")

    monkeypatch.setattr(join_engine, "_column_present", boom)
    out = join_engine._apply_policy(
        sql="SELECT * FROM orders", dialect="sqlite", datasource=_ds(tmp_path)
    )
    assert "EXISTS" in out.upper()


def test_apply_policy_join_rule_fails_closed_when_version_unknown(
    join_engine, monkeypatch
):
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


def test_apply_policy_column_only_clickhouse_not_blocked(engine, monkeypatch):
    monkeypatch.setattr(engine, "_column_present", lambda **k: True)
    out = engine._apply_policy(
        sql="SELECT * FROM orders", dialect="clickhouse", datasource=_ch_ds()
    )
    assert "allow_experimental_correlated_subqueries" not in out
    assert "org = 'x'" in out
