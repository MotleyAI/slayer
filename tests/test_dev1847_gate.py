"""DEV-1847 task 1.5/2.1 — the narrowed parse/bind gate (spec: aggregations/
expression-aggregation + queries/partitioned-aggregates fail-closed scenarios)."""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.engine.binding import _source_is_reaggregation
from slayer.engine.syntax import AggCall, TransformCall, parse_expr

from tests._dev1847_fixtures import (
    INNER_CR,
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    gen,
    sales_q,
)


class TestFullyAttachedAccepted:
    def test_pure_attached_source_parses(self):
        """A fully attached source parses into the ordinary AggCall shape."""
        parsed = parse_expr(f"avg({INNER_CR})")
        assert isinstance(parsed, AggCall)
        assert parsed.agg == "avg"
        # the source is the inner (attached) partitioned aggregate.
        assert isinstance(parsed.source, AggCall)

    def test_pure_attached_composite_parses(self):
        """A composite of attached aggregates is also accepted."""
        parse_expr("avg(sum(amount, partition_by=[city, region]) + "
                   "sum(amount, partition_by=region))")

    def test_direct_comparison_and_boolean_sources_parse(self):
        """A comparison / boolean composite is a legal attached source
        DIRECTLY — no scalar-call wrapper — while a row-level comparison
        source keeps its typed rejection."""
        for f in (f"count({INNER_CR} > 0)",
                  f"sum({INNER_CR} > 45 and {INNER_CR} < 100)"):
            parsed = parse_expr(f)
            assert isinstance(parsed, AggCall)
            assert _source_is_reaggregation(parsed.source)
        with pytest.raises(ValueError, match="cannot aggregate a Cmp"):
            parse_expr("sum(amount > 45)")

    def test_comparison_and_boolean_composites_route_as_reaggregation(self):
        """Cmp/BoolOp composition leaves route to the re-aggregation binder,
        mirroring the parse gate's traversal."""
        cmp_call = parse_expr(f"sum(iif({INNER_CR} > 45, 1, 0))")
        assert isinstance(cmp_call, AggCall)
        assert _source_is_reaggregation(cmp_call.source)
        bool_call = parse_expr(
            f"sum(iif({INNER_CR} > 45 and {INNER_CR} < 100, 1, 0))")
        assert isinstance(bool_call, AggCall)
        assert _source_is_reaggregation(bool_call.source)

    async def test_reaggregation_compiles_to_sql(self):
        """Accepted end-to-end: emits SQL rather than raising the gate."""
        sql = await gen(sales_q(dimensions=["region"],
                                measures=[ModelMeasure(formula=f"avg({INNER_CR})",
                                                       name="acr")]))
        assert "SELECT" in sql.upper()


class TestRejections:
    def test_nested_transform_rejected(self):
        """A transform inside the aggregated expression stays rejected."""
        with pytest.raises(ValueError, match="(?i)nest|transform"):
            parse_expr("sum(cumsum(amount) - 1)")

    def test_mixed_row_and_attached_rejected(self):
        """Scenario: Nested aggregation rejected — a source mixing a row-level
        reference with an attached value is rejected (DEV-1859's boundary)."""
        with pytest.raises(ValueError, match="(?i)mix|row|attach|nest"):
            parse_expr("sum(amount * avg(amount, partition_by=city))")

    async def test_column_parameter_on_outer_rejected(self):
        """Scenario: Column-reference outer parameter fails closed — explicit
        ``weight=id`` on the outer custom aggregation is a typed plan-time
        error, never invalid SQL over ``_base``."""
        query = sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=f"wavg({INNER_CR}, weight=id)", name="w")])
        with pytest.raises(SlayerError, match="column-reference parameter"):
            await gen(query)

    async def test_column_default_parameter_on_outer_rejected(self):
        """Scenario: Column-reference outer parameter fails closed — the
        aggregation definition's parameter DEFAULT is a column."""
        query = sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"wavg({INNER_CR})", name="w")])
        with pytest.raises(SlayerError, match="defaults to column"):
            await gen(query)

    async def test_outer_window_fails_closed(self):
        """Scenario: Outer window and outer filter fail closed — window= over an
        attached operand is a typed error naming the combination, never a
        NotImplementedError."""
        query = sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=f"sum({INNER_CR}, window='90d')", name="w")])
        with pytest.raises((SlayerError, ValueError)) as ei:
            await gen(query)
        msg = str(ei.value)
        assert not isinstance(ei.value, NotImplementedError)
        # The pure-attached source is accepted; the rejection is the specific
        # window-over-aggregate one, NOT the generic expression-nesting gate
        # (whose message echoes 'window' from the formula — so match the phrase).
        assert "nested inside the expression aggregated" not in msg
        assert "window" in msg.lower()


class TestCrossModelAndFilteredOperandStillRejected:
    """Pre-existing expression-aggregation boundaries DEV-1847 leaves intact."""

    async def test_cross_model_expression_rejected(self):
        """The error states cross-model expression aggregation is unsupported."""
        query = SlayerQuery(
            source_model="corders",
            dimensions=[ColumnRef(name="customer_id")],
            measures=[ModelMeasure(formula="sum(amount - customers.region_id)",
                                   name="x")])
        with pytest.raises((SlayerError, ValueError), match="(?i)cross-model"):
            await gen(query)

    async def test_filtered_column_operand_rejected(self):
        """q_amount carries a column-level filter; the error names the column."""
        query = sales_q(dimensions=["region"],
                        measures=[ModelMeasure(formula="sum(q_amount - 1)",
                                               name="x")])
        with pytest.raises((SlayerError, ValueError), match="q_amount"):
            await gen(query)


class TestFirstLastDispatchUnchanged:
    @pytest.mark.parametrize("fn", ["first", "last"])
    def test_first_last_over_aggregate_parses_as_transform(self, fn):
        """first/last over an aggregated first arg is the transform."""
        parsed = parse_expr(f"{fn}({INNER_CR})")
        assert isinstance(parsed, TransformCall)
        assert parsed.op == fn

    def test_last_over_plain_column_is_aggregation(self):
        """The non-aggregated first argument still routes to the aggregation."""
        parsed = parse_expr("last(amount)")
        assert isinstance(parsed, AggCall)


class TestUnknownOuterAggregationRejectedAtBinding:
    async def test_unknown_custom_name_over_aggregate_errors(self):
        """Unknown custom-aggregation candidates are rejected at binding."""
        query = sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"magic_fn({INNER_CR})", name="x")])
        with pytest.raises((SlayerError, ValueError), match="(?i)unknown aggregation"):
            await gen(query)
