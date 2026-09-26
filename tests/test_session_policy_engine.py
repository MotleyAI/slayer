"""Unit tests for the engine-side forced-filter wiring: the ``has_column`` provider and
the policy dispatch. ``_safe_get_columns`` is mocked so no live schema is required.
"""


import pytest

import slayer.engine.query_engine as qe
from slayer.core.models import DatasourceConfig
from slayer.core.policy import (
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
)
from slayer.engine.query_engine import SlayerQueryEngine
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
        # The 4th arg is a SchemaRef carrying the parsed schema.
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
# ClickHouse join rules
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


def test_apply_policy_join_rule_clickhouse_needs_no_version(join_engine, monkeypatch):
    """No profile has been probed, yet the join rule applies: it needs no server setting."""
    monkeypatch.setattr(join_engine, "_column_present", lambda **k: True)
    out = join_engine._apply_policy(
        sql="SELECT * FROM orders", dialect="clickhouse", datasource=_ch_ds()
    )
    assert "allow_experimental_correlated_subqueries" not in out
    assert "EXISTS" not in out.upper()
    assert "_rls_src.customer_id GLOBAL IN (SELECT toNullable(_rls_j0.id)" in out


def test_apply_policy_column_only_clickhouse_not_blocked(engine, monkeypatch):
    monkeypatch.setattr(engine, "_column_present", lambda **k: True)
    out = engine._apply_policy(
        sql="SELECT * FROM orders", dialect="clickhouse", datasource=_ch_ds()
    )
    assert "allow_experimental_correlated_subqueries" not in out
    assert "org = 'x'" in out
