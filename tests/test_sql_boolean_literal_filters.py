"""SQL-cased boolean literals in Mode-B filters survive the typed pipeline."""

from __future__ import annotations

import asyncio
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.core.syntax import BoolOp, Cmp, Literal, Ref, parse_expr
from slayer.storage.yaml_storage import YAMLStorage


class TestParserBooleanLiterals:
    @pytest.mark.parametrize("spelling", ["true", "TRUE", "True"])
    def test_true_any_casing_is_a_literal(self, spelling):
        result = parse_expr(f"is_active = {spelling}")
        assert result == Cmp(op="==", left=Ref(name="is_active"), right=Literal(value=True))

    @pytest.mark.parametrize("spelling", ["false", "FALSE", "False"])
    def test_false_any_casing_is_a_literal(self, spelling):
        result = parse_expr(f"is_active = {spelling}")
        assert result == Cmp(op="==", left=Ref(name="is_active"), right=Literal(value=False))

    def test_bare_boolean_in_disjunction_is_not_a_ref(self):
        result = parse_expr("status = 'x' or false")
        assert isinstance(result, BoolOp)
        assert Literal(value=False) in result.operands


def _make_engine() -> SlayerQueryEngine:
    tmp = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmp)
    asyncio.run(storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")))
    asyncio.run(storage.save_dataset(SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="is_active", sql="is_active", datatype=DataType.BOOLEAN),
        ],
    )))
    return SlayerQueryEngine(storage=storage)


class TestSQLEmission:
    def test_column_compared_to_sql_cased_true(self):
        engine = _make_engine()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(*)"}],
            "filter": ["is_active = true"],
        })
        sql = engine.execute_sync(query=query, dry_run=True).sql
        assert "TRUE" in sql.upper()

    def test_bare_false_in_disjunction(self):
        engine = _make_engine()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(*)"}],
            "filter": ["status = 'food' or false"],
        })
        sql = engine.execute_sync(query=query, dry_run=True).sql
        assert "FALSE" in sql.upper()

    def test_python_cased_literal_keeps_working(self):
        engine = _make_engine()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(*)"}],
            "filter": ["is_active = True"],
        })
        sql = engine.execute_sync(query=query, dry_run=True).sql
        assert "TRUE" in sql.upper()
