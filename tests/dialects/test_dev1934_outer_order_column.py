"""Outer-wrap ORDER BY resolves hidden hoists via the projected alias set, not inner SQL text."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import OrderItem, SlayerQuery, TimeDimension
from slayer.sql.dialects import SqlDialect, get_dialect
from slayer.sql.dialects.bigquery import BigqueryDialect
from tests._engine_helpers import _engine_generate

TIER1 = ["postgres", "sqlite", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]


def _order(sql: str, dialect: str) -> exp.Order:
    return sqlglot.parse_one(sql, dialect=dialect).args["order"]


class TestProjectedAliasSet:
    def test_hidden_hoist_resolves_via_projected(self) -> None:
        out = SqlDialect().emit_outer_wrap(
            inner_sql='SELECT 1 AS "orders.id", 2 AS "orders.hidden"',
            public=["orders.id"],
            projected=["orders.id", "orders.hidden"],
            order=_order('SELECT 1 ORDER BY _base."orders.hidden" DESC', "postgres"),
            limit=None,
            offset_arg=None,
        )
        assert 'ORDER BY\n  "orders.hidden" DESC' in out, out

    def test_bigquery_hidden_hoist_keeps_full_alias(self) -> None:
        out = BigqueryDialect().emit_outer_wrap(
            inner_sql="SELECT `_base`.`orders.created_at` AS `orders.created_at`, 1 AS x FROM _base",
            public=["x"],
            projected=["x", "orders.created_at"],
            order=_order("SELECT 1 FROM t ORDER BY `_base`.`orders.created_at` DESC", "bigquery"),
            limit=None,
            offset_arg=None,
        )
        assert "ORDER BY\n  `orders.created_at` DESC" in out, out

    def test_inner_sql_text_is_not_scanned(self) -> None:
        # The quoted alias appears in the inner text but is not projected.
        out = BigqueryDialect().emit_outer_wrap(
            inner_sql="SELECT `orders.created_at` AS `x` FROM t -- `orders.created_at`",
            public=["x"],
            projected=["x"],
            order=_order("SELECT 1 FROM t ORDER BY `_base`.`orders.created_at` DESC", "bigquery"),
            limit=None,
            offset_arg=None,
        )
        assert "ORDER BY\n  `created_at` DESC" in out, out

    def test_outer_order_column_takes_projected(self) -> None:
        col = sqlglot.parse_one("_base.\"orders.hidden\"", into=exp.Column)
        out = SqlDialect()._outer_order_column(
            col=col, public=["orders.id"], projected=["orders.id", "orders.hidden"],
        )
        assert out.sql(dialect="postgres") == '"orders.hidden"'


def _model() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
    )


class TestGeneratorSuppliesProjected:
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_order_by_hidden_hoist_names_an_inner_alias(
        self, dialect: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seen: list[list[str]] = []
        cls = type(get_dialect(dialect))
        original = cls.emit_outer_wrap

        def _spy(self, **kwargs):  # noqa: ANN001, ANN003
            seen.append(list(kwargs["projected"]))
            return original(self, **kwargs)

        monkeypatch.setattr(cls, "emit_outer_wrap", _spy)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="cumsum(revenue:sum)", name="running")],
            order=[OrderItem(column="revenue:max", direction="desc")], limit=3,
        )
        sql = await _engine_generate(query=query, model=_model(), dialect=dialect)
        assert seen, "outer wrap not reached"
        outer = sqlglot.parse_one(sql, dialect=dialect)
        inner = outer.find(exp.Subquery).this
        inner_aliases = {p.alias_or_name for p in inner.expressions}
        assert inner_aliases <= set(seen[-1])
        order_names = {o.this.name for o in outer.args["order"].expressions}
        assert order_names <= inner_aliases, sql
