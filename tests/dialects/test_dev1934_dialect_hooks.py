"""Typed-AST dialect aggregate / date hooks: per-dialect emission goldens."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects import _ALL_DIALECTS, SqlDialect, get_dialect
from slayer.sql.dialects.base import _build_covar_decomposition

_GOLDEN = json.loads(
    (Path(__file__).parent.parent / "golden" / "dev1934_dialect_hooks.json").read_text(),
)
_NAMES = [d.sqlglot_name for d in _ALL_DIALECTS]
_STAT1 = ["stddev_samp", "stddev_pop", "var_samp", "var_pop"]
_STAT2 = ["corr", "covar_samp", "covar_pop"]
_NOT_IMPL = "NotImplementedError"


def _amount() -> exp.Column:
    return exp.column("amount", table="orders")


def _quantity() -> exp.Column:
    return exp.column("quantity", table="orders")


def _created() -> exp.Column:
    return exp.column("created_at", table="orders")


def _check(name: str, key: str, build) -> None:  # noqa: ANN001
    expected = _GOLDEN[f"{name}|{key}"]
    if expected == _NOT_IMPL:
        with pytest.raises(NotImplementedError):
            build()
        return
    assert build().sql(dialect=name) == expected


def _assert_tree_consistent(root: exp.Expression) -> None:
    seen: set[int] = set()
    for node in root.walk():
        assert id(node) not in seen, f"node shared within the tree: {node!r}"
        seen.add(id(node))
        for child in node.iter_expressions():
            assert child.parent is node, f"stale parent pointer under {node.key}"


@pytest.mark.parametrize("name", _NAMES)
class TestAggregateHooks:
    def test_approx_count_distinct(self, name: str) -> None:
        d = get_dialect(name)
        _check(name, "approx", lambda: d.build_approx_count_distinct(col_expr=_amount()))

    def test_median(self, name: str) -> None:
        d = get_dialect(name)
        _check(name, "median", lambda: d.build_median(inner=_amount()))

    def test_percentile_keeps_literal_spelling(self, name: str) -> None:
        d = get_dialect(name)
        _check(name, "percentile", lambda: d.build_percentile(
            p=exp.Literal.number("0.50"), col_expr=_amount(),
        ))

    @pytest.mark.parametrize("agg", _STAT1)
    def test_stat_1arg(self, name: str, agg: str) -> None:
        d = get_dialect(name)
        _check(name, agg, lambda: d.build_stat_agg_1arg(agg_name=agg, col_expr=_amount()))

    @pytest.mark.parametrize("agg", _STAT2)
    def test_covar_2arg(self, name: str, agg: str) -> None:
        d = get_dialect(name)
        _check(name, agg, lambda: d.build_covar_2arg(
            agg_name=agg, col_expr=_amount(), other_expr=_quantity(),
        ))

    @pytest.mark.parametrize("agg", _STAT2)
    def test_covar_2arg_tree_is_consistent_and_does_not_adopt_inputs(
        self, name: str, agg: str,
    ) -> None:
        col, other = _amount(), _quantity()
        out = get_dialect(name).build_covar_2arg(agg_name=agg, col_expr=col, other_expr=other)
        _assert_tree_consistent(out)
        assert col.parent is None and other.parent is None
        assert all(c is not col and c is not other for c in out.find_all(exp.Column))

    @pytest.mark.parametrize("granularity", list(TimeGranularity))
    def test_date_trunc(self, name: str, granularity: TimeGranularity) -> None:
        d = get_dialect(name)
        _check(name, f"trunc|{granularity.value}", lambda: d.build_date_trunc(
            col_expr=_created(), granularity=granularity,
        ))

    def test_date_trunc_expression_operand_is_cast(self, name: str) -> None:
        d = get_dialect(name)
        _check(name, "trunc_expr|month", lambda: d.build_date_trunc(
            col_expr=exp.Literal.string("2024-01-15"), granularity=TimeGranularity.MONTH,
        ))

    @pytest.mark.parametrize("granularity", [
        TimeGranularity.DAY, TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY,
        TimeGranularity.QUARTER, TimeGranularity.MONTH,
    ])
    @pytest.mark.parametrize("offset", [2, -1])
    def test_time_offset(self, name: str, granularity: TimeGranularity, offset: int) -> None:
        d = get_dialect(name)
        _check(name, f"offset|{granularity.value}|{offset}", lambda: d.build_time_offset_expr(
            col_expr=_created(), offset=offset, granularity=granularity,
        ))

    def test_date_trunc_tree_is_consistent(self, name: str) -> None:
        for g in TimeGranularity:
            _assert_tree_consistent(
                get_dialect(name).build_date_trunc(col_expr=_created(), granularity=g),
            )


class TestCovarDecomposition:
    @pytest.mark.parametrize("agg", _STAT2)
    def test_compound_operands_keep_grouping_and_copies(self, agg: str) -> None:
        col = sqlglot.parse_one("orders.price - orders.discount")
        other = sqlglot.parse_one("orders.quantity + 1")
        out = _build_covar_decomposition(
            col_expr=col, other_expr=other, agg=agg,
            var_fn_samp="VAR_SAMP", var_fn_pop="VAR_POP", stddev_fn="STDDEV_SAMP",
        )
        _assert_tree_consistent(out)
        assert col.parent is None and other.parent is None
        for case in out.find_all(exp.Case):
            then = case.args["ifs"][0].args["true"]
            names = {c.name for c in then.find_all(exp.Column)}
            assert names in ({"price", "discount"}, {"quantity"})
        reparsed = sqlglot.parse_one(out.sql(dialect="mysql"), dialect="mysql")
        assert reparsed.sql(dialect="mysql") == out.sql(dialect="mysql")


class TestApproxDistinctConfig:
    @pytest.mark.parametrize("name", _NAMES)
    def test_template_field_removed(self, name: str) -> None:
        assert not hasattr(get_dialect(name), "approx_count_distinct_template")

    def test_native_flag_is_a_bool_defaulting_off(self) -> None:
        assert SqlDialect().approx_count_distinct_native is False

    @pytest.mark.parametrize("name", ["clickhouse", "redshift", "trino", "presto", "spark",
                                      "databricks", "bigquery", "snowflake", "duckdb"])
    def test_native_dialects_emit_approx_distinct_node(self, name: str) -> None:
        out = get_dialect(name).build_approx_count_distinct(col_expr=_amount())
        assert isinstance(out, exp.ApproxDistinct)

    @pytest.mark.parametrize("name", ["postgres", "sqlite", "mysql"])
    def test_exact_fallback_is_count_distinct_node(self, name: str) -> None:
        out = get_dialect(name).build_approx_count_distinct(col_expr=_amount())
        assert isinstance(out, exp.Count) and isinstance(out.this, exp.Distinct)
