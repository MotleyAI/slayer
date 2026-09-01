"""DEV-1706 Stage 2 — DEV-1527 local fix: typed parametric-aggregation kwargs.

The defect: ``AggRenderSpec.agg_kwargs`` was flattened through
``agg_kwarg_canonical_str`` at spec-build, which collapses a derived
(``ColumnSqlKey``) kwarg to its bare name — emitting ``orders.region_weight``
(a non-existent column) and never pulling the crossed join.

The fix (D-C / D-I): ``AggRenderSpec.agg_kwargs`` becomes
``Dict[str, ResolvedAggKwarg]`` with a 2-kind tag —

* ``kind="expr"`` — a trusted, scope-resolved ``exp.Expression`` for a column-ref
  kwarg (the join registers as a side effect → base FROM). Embedded directly.
* ``kind="str"`` — everything else (scalars via the retained
  ``agg_kwarg_canonical_str``, existing strings), consumed EXACTLY as today
  (``_SAFE_AGG_PARAM_RE`` guard + ``_resolve_sql`` / formula substitution).

A bare ``str`` value coerces to ``kind="str"`` so the legacy shim and the direct
injection test keep working. Only the EMISSION round-trip of
``agg_kwarg_canonical_str`` is deleted; the naming-only use survives (D-H) and is
frozen by golden tests here.

The e2e promotion test itself (``weighted_avg(weight=region_weight)``) is
un-pinned in place at
``tests/test_sql_generator.py::TestMeasureSourceSqlJoinInference::
test_agg_param_derived_column_path_alias_xfail``.
"""

from __future__ import annotations

import re

import pytest
from pydantic import ValidationError
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.sql.generator import AggRenderSpec, ResolvedAggKwarg
from tests._engine_helpers import _engine_generate


def _norm(s: str) -> str:
    return " ".join(s.split())


# --------------------------------------------------------------------------- #
# Fixtures — orders → customers → regions (+ a 1:N orders → line_items chain).
# --------------------------------------------------------------------------- #
def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="weight", sql="weight", type=DataType.DOUBLE),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _line_items() -> SlayerModel:
    return SlayerModel(
        name="line_items", sql_table="line_items", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="order_id", sql="order_id", type=DataType.DOUBLE),
            Column(name="qty", sql="qty", type=DataType.DOUBLE),
        ],
    )


def _orders(*, extra=None, joins_extra=None) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
        Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
    ]
    cols += extra or []
    joins = [ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])]
    joins += joins_extra or []
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test", columns=cols, joins=joins,
    )


# --------------------------------------------------------------------------- #
# ResolvedAggKwarg — the 2-kind tagged value + coercion (D-I).
# --------------------------------------------------------------------------- #
class TestResolvedAggKwarg:
    def test_expr_kind_holds_expression(self) -> None:
        col = exp.column("weight", table="customers__regions")
        rk = ResolvedAggKwarg(kind="expr", value=col)
        assert rk.kind == "expr"
        assert rk.value.sql(dialect="postgres") == "customers__regions.weight"

    def test_str_kind_holds_string(self) -> None:
        rk = ResolvedAggKwarg(kind="str", value="quantity")
        assert rk.kind == "str"
        assert rk.value == "quantity"

    def test_agg_render_spec_coerces_bare_str_to_str_kind(self) -> None:
        # Legacy shim (generator.py:226) + injection test pass a bare str dict;
        # a before-validator coerces to kind="str" so they keep working.
        spec = AggRenderSpec(
            sql="price", name="price", model_name="sales",
            aggregation="weighted_avg", alias="sales.price_weighted_avg",
            agg_kwargs={"weight": "quantity"},
        )
        val = spec.agg_kwargs["weight"]
        assert isinstance(val, ResolvedAggKwarg)
        assert val.kind == "str"
        assert val.value == "quantity"

    def test_agg_render_spec_accepts_expr_kind(self) -> None:
        spec = AggRenderSpec(
            sql="price", name="price", model_name="sales",
            aggregation="weighted_avg", alias="sales.price_weighted_avg",
            agg_kwargs={"weight": ResolvedAggKwarg(
                kind="expr", value=exp.column("quantity", table="sales"))},
        )
        assert spec.agg_kwargs["weight"].kind == "expr"

    def test_agg_render_spec_rejects_non_str_non_wrapper_kwarg(self) -> None:
        # The coercion validator turns a bare str into kind="str" but must NOT
        # silently accept a bool/None (they raise at spec-build via
        # agg_kwarg_canonical_str; a direct construction is rejected defensively).
        with pytest.raises((ValidationError, ValueError, TypeError)):
            AggRenderSpec(
                sql="price", name="price", model_name="sales",
                aggregation="weighted_avg", alias="sales.price_weighted_avg",
                agg_kwargs={"weight": True},
            )


# --------------------------------------------------------------------------- #
# DEV-1527 fix — e2e (siblings of the promoted xfail).
# --------------------------------------------------------------------------- #
class TestDev1527KwargExpansion:
    async def test_local_noncrossing_derived_kwarg_expands(self) -> None:
        # A LOCAL derived column kwarg (no join) also collapses to a bare name
        # today (orders.dbl_qty, non-existent). After the fix it expands.
        orders = _orders(extra=[
            Column(name="dbl_qty", sql="quantity * 2", type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:weighted_avg(weight=dbl_qty)")],
        )
        sql = await _engine_generate(query=query, model=orders)
        assert "quantity * 2" in sql
        assert "orders.dbl_qty" not in sql
        assert "LEFT JOIN" not in sql  # local — pulls no join

    async def test_percentile_numeric_param_unchanged(self) -> None:
        # str-kind path is byte-identical to today (regression guard).
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:percentile(p=0.95)")],
        )
        sql = await _engine_generate(query=query, model=_orders())
        assert "PERCENTILE_CONT" in sql
        assert "0.95" in sql

    async def test_weighted_avg_plain_column_unchanged(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:weighted_avg(weight=quantity)")],
        )
        sql = await _engine_generate(query=query, model=_orders())
        assert "SUM(" in sql
        assert "NULLIF(" in sql
        assert "orders.quantity" in sql

    async def test_corr_other_param_renders_both_operands(self) -> None:
        # Non-vacuous: both the source (amount) and the ``other=`` operand
        # (quantity) must appear in the CORR call, str-kind, byte-identical.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:corr(other=quantity)")],
        )
        sql = await _engine_generate(query=query, model=_orders())
        assert "CORR(" in sql.upper()
        assert "orders.amount" in sql
        assert "orders.quantity" in sql

    async def test_corr_other_derived_crossing_expr_kind(self) -> None:
        # kind="expr" through _build_stat_agg: ``other=region_weight`` is a derived
        # column crossing a join → its expansion is embedded as the second CORR
        # operand AND the join is registered (base-pulled).
        orders = _orders(extra=[
            Column(name="region_weight", sql="customers.regions.weight",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:corr(other=region_weight)")],
        )
        sql = await _engine_generate(
            query=query, model=orders, extra_models=[_customers(), _regions()],
        )
        assert "CORR(" in sql.upper()
        assert "customers__regions.weight" in sql          # expanded, not bare
        assert "orders.region_weight" not in sql            # not the broken form
        assert "LEFT JOIN customers" in sql                 # join base-pulled

    async def test_composite_crossing_kwarg_expands_each_operand(self) -> None:
        # A crossing derived kwarg inside an AGGREGATE-phase COMPOSITE
        # (``weighted_avg(weight=region_weight) + quantity:sum``) must expand the
        # same way the direct form does. The composite render path threads the
        # host scope's resolved_agg_kwargs down to each operand leaf, so the
        # weight operand is the expanded joined ref — not the bare, non-existent
        # ``orders.region_weight`` (Codex finding on the ROW-collector fold PR).
        orders = _orders(extra=[
            Column(name="region_weight", sql="customers.regions.weight",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(
                formula="amount:weighted_avg(weight=region_weight) + quantity:sum")],
        )
        sql = await _engine_generate(
            query=query, model=orders, extra_models=[_customers(), _regions()],
        )
        assert "customers__regions.weight" in sql          # kwarg expanded
        assert "orders.region_weight" not in sql           # not the broken bare form
        assert "SUM(orders.quantity)" in sql               # sibling local operand intact
        assert "LEFT JOIN customers" in sql                 # first hop base-pulled
        # terminal hop too — a first-hop-only registration would still emit an
        # unresolved customers__regions.weight (CodeRabbit).
        assert "LEFT JOIN regions AS customers__regions" in sql

    async def test_having_crossing_kwarg_expands(self) -> None:
        # DEV-1709: the crossing-kwarg aggregate isolates, so the aggregate
        # filter routes to the combined outer WHERE against the CTE's output
        # column (DEV-1503 rule) — never HAVING. The expanded, join-anchored
        # kwarg (customers__regions.weight) renders inside the CTE; the bare
        # non-existent orders.region_weight never appears.
        orders = _orders(extra=[
            Column(name="region_weight", sql="customers.regions.weight",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            dimensions=["id"],
            measures=[ModelMeasure(formula="amount:weighted_avg(weight=region_weight)")],
            filters=["amount:weighted_avg(weight=region_weight) > 5"],
        )
        sql = await _engine_generate(
            query=query, model=orders, extra_models=[_customers(), _regions()],
        )
        norm = _norm(sql)
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        assert "HAVING" not in norm, sql
        assert "customers__regions.weight" in _extract_cm_body(sql), sql
        assert "orders.region_weight" not in sql
        # The aggregate filter lands in the outer WHERE on the CTE column.
        outer = sql[sql.rfind("\n)") + 2:]
        assert "WHERE" in outer, sql
        assert "> 5" in outer, sql

    async def test_custom_aggregation_str_param_override_substitutes(self) -> None:
        # kind="str" template path through _build_formula_agg: a model-defined
        # custom aggregation's ``{scale}`` param, overridden at query time with a
        # numeric literal, substitutes as today (guarded by _SAFE_AGG_PARAM_RE).
        orders = _orders()
        orders.aggregations = [Aggregation(
            name="scaled_sum", formula="SUM({value}) / {scale}",
            params=[AggregationParam(name="scale", sql="1")],
        )]
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:scaled_sum(scale=100)")],
        )
        sql = await _engine_generate(query=query, model=orders)
        assert "SUM(orders.amount)" in _norm(sql)
        assert "/ 100" in _norm(sql)


# --------------------------------------------------------------------------- #
# F1 — a crossing kwarg ISOLATES the aggregate into a host-rooted CTE
# (DEV-1709 Stage 5, widened Law-3 trigger); inside that CTE the kwarg's
# join base-pulls with multiply-per-match semantics (F1 decision — 1:N
# semantics unchanged, only the scope moved). SQL-shape pin; executed
# multiply-per-match values live in tests/integration/test_integration_duckdb.py.
# --------------------------------------------------------------------------- #
def _extract_cm_body(sql: str) -> str:
    """First `_cm_*` CTE body, balanced-paren walk (mirrors the helper in
    tests/test_sql_generator.py)."""
    m = re.search(r"_cm_\w+\s+AS\s*\(", sql)
    assert m, f"No _cm_ CTE in:\n{sql}"
    start = sql.index("(", m.start()) + 1
    depth, i = 1, start
    while i < len(sql):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[start:i]
        i += 1
    raise AssertionError(f"Unbalanced parens in:\n{sql}")


class TestDev1527F1BasePull:
    async def test_crossing_kwarg_over_unproven_to_many_hop_fails_closed(self) -> None:
        # 1:N: one order → many line_items. ``li_weight`` crosses that join,
        # so the fanned SUM is ambiguous — fails closed per role (DEV-1838 D5;
        # the class-(d) crossed-argument ledger row). The proven-hop control
        # below pins the still-legal shape.
        orders = _orders(
            extra=[Column(name="li_weight", sql="line_items.qty", type=DataType.DOUBLE)],
            joins_extra=[ModelJoin(
                target_model="line_items", join_pairs=[["id", "order_id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:weighted_avg(weight=li_weight)")],
        )
        with pytest.raises(ValueError) as ei:
            await _engine_generate(
                query=query, model=orders, extra_models=[_line_items()],
            )
        message = str(ei.value)
        assert "line_items" in message
        assert any(w in message.lower() for w in ("cardinality", "unique", "declare"))

    async def test_ndotn_control_regionweight_still_base_pull(self) -> None:
        # N:1 control (region_weight): isolates identically — the trigger is
        # structural (any crossing input), not cardinality-driven.
        orders = _orders(extra=[
            Column(name="region_weight", sql="customers.regions.weight",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:weighted_avg(weight=region_weight)")],
        )
        sql = await _engine_generate(
            query=query, model=orders, extra_models=[_customers(), _regions()],
        )
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        assert "customers__regions.weight" in _extract_cm_body(sql), sql
