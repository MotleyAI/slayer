"""Unit tests for the engine-side column-presence probe (DEV-1578).

``SlayerQueryEngine._column_present`` is the ``has_column`` provider for the
forced-filter rewrite: it introspects via ``_safe_get_columns``, matches the
column case-insensitively, resolves the schema (AST schema else datasource
default), returns ``None`` on any introspection failure/empty (UNCACHED), and
caches only confirmed ``True``/``False``. These tests mock ``_safe_get_columns``
so no live schema is required (an empty sqlite file backs ``get_engine`` /
``sa.inspect``).
"""

import pytest

import slayer.engine.query_engine as qe
from slayer.core.models import DatasourceConfig
from slayer.core.policy import ColumnFilterRule, SessionPolicy
from slayer.engine.query_engine import SlayerQueryEngine
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
