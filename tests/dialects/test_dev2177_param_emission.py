"""Boolean aggregation parameters in emitted SQL, on every dialect (spec: aggregations/boolean-inputs)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import List

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.core.models import Aggregation, AggregationParam
from slayer.sql.dialects import SQLGLOT_NAMES

from tests._dev2046_fixtures import customers_model, m, orders_model, orders_q
from tests._engine_helpers import _engine_generate

INT_TYPES = frozenset(exp.DataType.INTEGER_TYPES) - {exp.DataType.Type.BIT}


async def _statement(dialect: str, *formulas: str, aggregations: Sequence[Aggregation] = ()) -> Expr:
    model = orders_model()
    model.aggregations.extend(aggregations)
    query = orders_q(measures=[m(f, f"v{i}") for i, f in enumerate(formulas)])
    sql = await _engine_generate(query=query, model=model, extra_models=[customers_model()], dialect=dialect)
    return sqlglot.parse_one(sql, read=dialect)


def _flag_reads(tree: Expr) -> List[exp.Column]:
    reads = [c for c in tree.find_all(exp.Column) if c.name == "flag"]
    assert reads, tree.sql()
    return reads


def _int_cast(node: Expr | None) -> bool:
    return isinstance(node, exp.Cast) and node.to is not None and node.to.this in INT_TYPES


@pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
@pytest.mark.parametrize("formula", [
    "weighted_avg(amount, weight=flag)", "covar_samp(amount, other=flag)",
    "covar_pop(amount, other=flag)", "corr(amount, other=flag)",
])
async def test_parameter_takes_the_integer(dialect: str, formula: str) -> None:
    tree = await _statement(dialect, formula)
    bare = [c.sql(dialect=dialect) for c in _flag_reads(tree) if not _int_cast(c.parent)]
    assert bare == [], tree.sql(dialect=dialect)


@pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
async def test_builtin_named_override_placeholders_take_the_integer(dialect: str) -> None:
    override = Aggregation(name="weighted_avg", formula="SUM({value} * {weight}) * 1.0 / SUM({weight})",
                           params=[AggregationParam(name="weight", sql="1")])
    tree = await _statement(dialect, "weighted_avg(amount, weight=flag)", aggregations=[override])
    bare = [c.sql(dialect=dialect) for c in _flag_reads(tree) if not _int_cast(c.parent)]
    assert bare == [], tree.sql(dialect=dialect)


@pytest.mark.parametrize("dialect", ["postgres", "duckdb", "sqlite", "tsql", "bigquery"])
async def test_custom_aggregation_placeholder_stays_boolean(dialect: str) -> None:
    custom = Aggregation(name="true_amount", formula="SUM(CASE WHEN {flag} THEN {value} END)",
                         params=[AggregationParam(name="flag", sql="TRUE")])
    tree = await _statement(dialect, "true_amount(amount, flag=flag)", aggregations=[custom])
    cast = [c.sql(dialect=dialect) for c in _flag_reads(tree) if _int_cast(c.parent)]
    assert cast == [], tree.sql(dialect=dialect)
