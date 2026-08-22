"""SQL-cased boolean literals in Mode-B filters survive the typed pipeline.

PR #316 taught the legacy parser (``core/formula.py``) to treat ``true`` /
``false`` in any casing as SQL boolean literals instead of column
references. The DEV-1450 typed pipeline resolves names in
``engine/syntax.py`` and never learned the carve-out, so ``... or false``
died at bind time with ``UnknownReferenceError``. Found by the DEV-1811
engine A/B audit (branch vs motley-slayer 0.9.12).
"""

from __future__ import annotations

import asyncio
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.syntax import Cmp, Literal, Ref, parse_filter_expr
from slayer.storage.yaml_storage import YAMLStorage


class TestParserBooleanLiterals:
    @pytest.mark.parametrize("spelling", ["true", "TRUE", "True"])
    def test_true_any_casing_is_a_literal(self, spelling):
        result = parse_filter_expr(f"is_active = {spelling}")
        assert result == Cmp(op="==", left=Ref(name="is_active"), right=Literal(value=True))

    @pytest.mark.parametrize("spelling", ["false", "FALSE", "False"])
    def test_false_any_casing_is_a_literal(self, spelling):
        result = parse_filter_expr(f"is_active = {spelling}")
        assert result == Cmp(op="==", left=Ref(name="is_active"), right=Literal(value=False))

    def test_bare_boolean_in_disjunction_is_not_a_ref(self):
        result = parse_filter_expr("status = 'x' or false")
        assert Literal(value=False) in result.operands


def _make_engine() -> SlayerQueryEngine:
    tmp = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmp)
    asyncio.run(storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")))
    asyncio.run(storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="is_active", sql="is_active", type=DataType.BOOLEAN),
        ],
    )))
    return SlayerQueryEngine(storage=storage)


class TestSQLEmission:
    def test_column_compared_to_sql_cased_true(self):
        engine = _make_engine()
        query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}],
                            filters=["is_active = true"])
        sql = engine.execute_sync(query=query, dry_run=True).sql
        assert "TRUE" in sql.upper()

    def test_bare_false_in_disjunction(self):
        engine = _make_engine()
        query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}],
                            filters=["status = 'food' or false"])
        sql = engine.execute_sync(query=query, dry_run=True).sql
        assert "FALSE" in sql.upper()

    def test_python_cased_literal_keeps_working(self):
        engine = _make_engine()
        query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}],
                            filters=["is_active = True"])
        sql = engine.execute_sync(query=query, dry_run=True).sql
        assert "TRUE" in sql.upper()
