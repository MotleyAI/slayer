"""Boolean aggregation inputs and predicate values in emitted SQL, on every dialect.

Spec: aggregations/boolean-inputs › "Numeric aggregations lower a boolean input to an
integer" (Emitted SQL on every dialect); sql/predicate-values.
"""

from __future__ import annotations

from typing import List

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.core.models import Aggregation
from slayer.sql.dialects import SQLGLOT_NAMES

from tests._dev2046_fixtures import customers_model, m, orders_model, orders_q
from tests._engine_helpers import _engine_generate

INT_TYPES = frozenset(exp.DataType.INTEGER_TYPES) - {exp.DataType.Type.BIT}
BOOLEAN_TYPES = frozenset({exp.DataType.Type.BOOLEAN, exp.DataType.Type.BIT})
LOWERED = (exp.Sum, exp.Avg, exp.Min, exp.Max)
BOOLEAN_SOURCES = ["flag", "big_order", "coalesce(flag, false)", "amount > 15"]


def _bool_type(dialect: str):
    """The type ``dialect`` renders a cast to BOOLEAN as (INTEGER on SQLite, BIT on T-SQL, …)."""
    sql = sqlglot.transpile("SELECT CAST(x AS BOOLEAN)", read="duckdb", write=dialect)[0]
    cast = sqlglot.parse_one(sql, read=dialect).find(exp.Cast)
    assert cast is not None
    return cast.to.this


async def _statement(dialect: str, *formulas: str, **kw) -> Expr:
    model = kw.pop("model", None) or orders_model()
    query = orders_q(measures=[m(f, f"v{i}") for i, f in enumerate(formulas)], **kw)
    sql = await _engine_generate(query=query, model=model, extra_models=[customers_model()], dialect=dialect)
    return sqlglot.parse_one(sql, read=dialect)


def _int_cast(node: Expr) -> bool:
    return isinstance(node, exp.Cast) and node.to.this in INT_TYPES


def _nodes(tree: Expr, kind) -> List[Expr]:
    found = list(tree.find_all(kind))
    assert found, f"no {kind} in:\n{tree.sql()}"
    return found


class TestLoweringOnEveryDialect:
    @pytest.mark.parametrize("source", BOOLEAN_SOURCES)
    @pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
    async def test_sum_avg_take_integer_input(self, dialect: str, source: str) -> None:
        tree = await _statement(dialect, f"sum({source})", f"avg({source})")
        for agg in (*_nodes(tree, exp.Sum), *_nodes(tree, exp.Avg)):
            assert _int_cast(agg.this), f"{agg.sql(dialect=dialect)} takes a raw boolean"
            parent = agg.parent
            assert not (isinstance(parent, exp.Cast) and parent.to.this in BOOLEAN_TYPES), (
                f"boolean cast over {agg.sql(dialect=dialect)}"
            )

    @pytest.mark.parametrize("source", BOOLEAN_SOURCES)
    @pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
    async def test_min_max_cast_back_to_boolean(self, dialect: str, source: str) -> None:
        tree = await _statement(dialect, f"min({source})", f"max({source})")
        bool_type = _bool_type(dialect)
        for agg in (*_nodes(tree, exp.Min), *_nodes(tree, exp.Max)):
            assert _int_cast(agg.this), f"{agg.sql(dialect=dialect)} takes a raw boolean"
            parent = agg.parent
            assert isinstance(parent, exp.Cast), f"{agg.sql(dialect=dialect)} not converted back to {bool_type}"
            assert parent.to.this == bool_type, f"{agg.sql(dialect=dialect)} not converted back to {bool_type}"

    @pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
    async def test_having_lowers_like_projection(self, dialect: str) -> None:
        """The post-aggregation filter path applies the same lowering."""
        tree = await _statement(
            dialect, "count(*)", dimensions=["region"], filters=["sum(flag) > 1", "max(flag) = true"],
        )
        having = _nodes(tree, exp.Having)[0]
        for agg in (*_nodes(having, exp.Sum), *_nodes(having, exp.Max)):
            assert _int_cast(agg.this), f"{agg.sql(dialect=dialect)} takes a raw boolean in HAVING"


class TestStatisticalBuildersTakeTheInteger:
    """The stat, dialect-hook and formula builders read a boolean input as its integer."""

    @pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
    @pytest.mark.parametrize("formula", ["stddev_samp(flag)", "weighted_avg(flag, weight=amount)"])
    async def test_every_dialect(self, dialect: str, formula: str) -> None:
        tree = await _statement(dialect, formula)
        assert _bare_flags(tree) == [], tree.sql(dialect=dialect)

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "sqlite", "snowflake"])
    @pytest.mark.parametrize("formula", ["median(flag)", "percentile(flag, p=0.5)"])
    async def test_percentile_family(self, dialect: str, formula: str) -> None:
        tree = await _statement(dialect, formula)
        assert _bare_flags(tree) == [], tree.sql(dialect=dialect)


def _bare_flags(tree: Expr) -> List[str]:
    """Every ``flag`` column read outside an integer cast."""
    return [
        c.sql() for c in tree.find_all(exp.Column)
        if c.name == "flag" and not (c.parent is not None and _int_cast(c.parent))
    ]


class TestBooleansInNumericPositions:
    @pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
    async def test_arithmetic_and_scalar_operands(self, dialect: str) -> None:
        tree = await _statement(dialect, "sum(flag * amount)", "max(round(flag))", "sum(coalesce(flag, 0))")
        assert _bare_flags(tree) == [], tree.sql(dialect=dialect)

    @pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
    async def test_boolean_compared_with_a_boolean_stays_boolean(self, dialect: str) -> None:
        tree = await _statement(dialect, "count(*)", filters=["flag = true"])
        where = _nodes(tree, exp.Where)[0]
        assert not [c for c in where.find_all(exp.Cast) if _int_cast(c)], where.sql(dialect=dialect)


    @pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
    async def test_having_boolean_compared_with_a_boolean_stays_boolean(self, dialect: str) -> None:
        """``max(flag) = true`` compares two booleans: neither side reads as an integer."""
        tree = await _statement(dialect, "count(*)", dimensions=["region"], filters=["max(flag) = true"])
        having = _nodes(tree, exp.Having)[0]
        # Only the aggregate's own lowering / restore casts may appear (SQLite / MySQL spell BOOLEAN as an int).
        stray = [c for c in having.find_all(exp.Cast) if not isinstance(c.parent, exp.Max) and not isinstance(c.this, exp.Max)]
        assert stray == [], having.sql(dialect=dialect)


class TestUnchangedAggregations:
    """Count family, first / last and custom aggregations receive the boolean unchanged."""

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "tsql", "bigquery"])
    async def test_no_integer_cast_around_the_boolean(self, dialect: str) -> None:
        model = orders_model()
        model.aggregations.append(Aggregation(name="trues", formula="COUNT(CASE WHEN {value} THEN 1 END)"))
        tree = await _statement(
            dialect, "count(flag)", "count_distinct(flag)", "count_distinct_approx(flag)",
            "first(flag)", "last(flag)", "trues(flag)", "sum(flag)", model=model,
        )
        lowered = {id(s.this) for s in _nodes(tree, exp.Sum) if _int_cast(s.this)}
        assert lowered, "sum(flag) must lower its input"
        stray = [
            cast.sql(dialect=dialect) for cast in tree.find_all(exp.Cast)
            if _int_cast(cast) and id(cast) not in lowered
            and isinstance(cast.this, exp.Column) and cast.this.name == "flag"
        ]
        assert stray == []


class TestMixedCoalesce:
    async def test_no_lowering(self) -> None:
        """Scenario: mixed-type coalesce is not boolean — aggregated as plain numeric."""
        tree = await _statement("postgres", "sum(coalesce(flag, amount))")
        (agg,) = _nodes(tree, exp.Sum)
        assert isinstance(agg.this, exp.Coalesce)


# --------------------------------------------------------------------------- #
# sql/predicate-values
# --------------------------------------------------------------------------- #
_PREDICATES = (exp.Predicate, exp.Connector, exp.Not)


def _in_condition_position(node: Expr) -> bool:
    child, parent = node, node.parent
    while parent is not None:
        if isinstance(parent, exp.If) and child is parent.this:
            return True
        if isinstance(parent, (exp.Where, exp.Having, exp.Join)):
            return True
        child, parent = parent, parent.parent
    return False


def _bare_value_predicates(tree: Expr) -> List[str]:
    return [
        p.sql(dialect="tsql")
        for select in tree.find_all(exp.Select)
        for projection in select.expressions
        for p in projection.find_all(*_PREDICATES)
        if not _in_condition_position(p)
    ]


def _is_bit_value(node: Expr) -> bool:
    """``CAST(CASE WHEN p THEN 1 WHEN NOT p THEN 0 END AS BIT)``, possibly re-cast to BIT."""
    while isinstance(node, exp.Cast) and node.to.this is exp.DataType.Type.BIT:
        if isinstance(node.this, exp.Case):
            ifs = node.this.args.get("ifs") or []
            return len(ifs) == 2 and node.this.args.get("default") is None
        node = node.this
    return False


def _projection(tree: Expr, suffix: str) -> Expr:
    outer = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    assert outer is not None
    for projection in outer.expressions:
        if projection.alias_or_name.endswith(suffix):
            return projection.unalias()
    raise AssertionError(f"no projection ending {suffix!r} in {outer.sql(dialect='tsql')}")


class TestSqlServerPredicateValues:
    async def test_projected_boolean_measure(self) -> None:
        tree = await _statement("tsql", "sum(amount) > 50", dimensions=["region"])
        assert _is_bit_value(_projection(tree, "v0"))
        assert _bare_value_predicates(tree) == []

    @pytest.mark.parametrize("agg", ["sum", "count"])
    async def test_predicate_aggregation_input(self, agg: str) -> None:
        tree = await _statement("tsql", f"{agg}(amount > 15)", dimensions=["region"])
        node = _nodes(tree, exp.Sum if agg == "sum" else exp.Count)[0]
        inner = node.this.this if agg == "sum" and _int_cast(node.this) else node.this
        assert _is_bit_value(inner), node.sql(dialect="tsql")
        assert _bare_value_predicates(tree) == []

    async def test_conditions_stay_bare(self) -> None:
        tree = await _statement(
            "tsql", "sum(amount) > 50", "sum(amount)", dimensions=["region"],
            filters=["amount > 15", "sum(amount) > 50"],
        )
        assert isinstance(_nodes(tree, exp.Where)[0].this, exp.GT)
        assert isinstance(_nodes(tree, exp.Having)[0].this, exp.GT)
        assert _bare_value_predicates(tree) == []

    async def test_case_condition_stays_bare(self) -> None:
        tree = await _statement("tsql", "sum(iif(amount > 15, 1, 0))", "sum(amount > 15)")
        assert _bare_value_predicates(tree) == []
        conditions = [i.this for i in tree.find_all(exp.If)]
        assert any(isinstance(c, exp.GT) for c in conditions)

    async def test_join_condition_stays_bare(self) -> None:
        tree = await _statement("tsql", "sum(amount > 15)", dimensions=["customers.name"])
        (join,) = _nodes(tree, exp.Join)
        assert isinstance(join.args["on"], exp.EQ)
        assert _bare_value_predicates(tree) == []

    async def test_arithmetic_operand_is_a_value(self) -> None:
        """A boolean arithmetic operand is its integer: the INT of the predicate's BIT value."""
        tree = await _statement("tsql", "(sum(amount) > 50) + 0", dimensions=["region"])
        (add,) = _nodes(_projection(tree, "v0"), exp.Add)
        operand = add.this.unnest() if isinstance(add.this, exp.Paren) else add.this
        assert _int_cast(operand), add.sql(dialect="tsql")
        assert _is_bit_value(operand.this), add.sql(dialect="tsql")
        assert _bare_value_predicates(tree) == []

    async def test_nested_predicate_wrapped_once(self) -> None:
        """Only the outermost predicate in a value position becomes a value; its
        operands stay bare inside the CASE condition."""
        tree = await _statement("tsql", "(sum(amount) > 50) and (count(*) > 1)", dimensions=["region"])
        value = _projection(tree, "v0")
        assert _is_bit_value(value)
        case = value.find(exp.Case)
        assert case is not None
        condition = case.args["ifs"][0].this
        condition = condition.unnest() if isinstance(condition, exp.Paren) else condition
        assert isinstance(condition, exp.And)
        assert not list(condition.find_all(exp.Case))
        assert _bare_value_predicates(tree) == []

    async def test_postgres_projection_unchanged(self) -> None:
        tree = await _statement("postgres", "sum(amount) > 50", dimensions=["region"])
        value = _projection(tree, "v0")
        while isinstance(value, exp.Cast):
            value = value.this
        assert isinstance(value, exp.GT)
