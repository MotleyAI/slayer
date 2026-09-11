"""DEV-1847 task 1.5/2.1 — the narrowed parse/bind gate (spec: aggregations/
expression-aggregation + queries/partitioned-aggregates fail-closed scenarios)."""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
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
        with pytest.raises(SlayerError, match="column-reference parameter"):
            await gen(sales_q(
                dimensions=["region"],
                measures=[ModelMeasure(
                    formula=f"wavg({INNER_CR}, weight=id)", name="w")]))

    async def test_column_default_parameter_on_outer_rejected(self):
        """Scenario: Column-reference outer parameter fails closed — the
        aggregation definition's parameter DEFAULT is a column."""
        with pytest.raises(SlayerError, match="defaults to column"):
            await gen(sales_q(
                dimensions=["region"],
                measures=[ModelMeasure(formula=f"wavg({INNER_CR})", name="w")]))

    async def test_outer_window_fails_closed(self):
        """Scenario: Outer window and outer filter fail closed — window= over an
        attached operand is a typed error naming the combination, never a
        NotImplementedError."""
        with pytest.raises((SlayerError, ValueError)) as ei:
            await gen(sales_q(
                dimensions=["region"],
                measures=[ModelMeasure(
                    formula=f"sum({INNER_CR}, window='90d')", name="w")]))
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
        with pytest.raises((SlayerError, ValueError), match="(?i)cross-model"):
            await gen(SlayerQuery(
                source_model="corders",
                dimensions=[ColumnRef(name="customer_id")],
                measures=[ModelMeasure(formula="sum(amount - customers.region_id)",
                                       name="x")]))

    async def test_filtered_column_operand_rejected(self):
        """q_amount carries a column-level filter; the error names the column."""
        with pytest.raises((SlayerError, ValueError), match="q_amount"):
            await gen(sales_q(dimensions=["region"],
                              measures=[ModelMeasure(formula="sum(q_amount - 1)",
                                                     name="x")]))


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
        with pytest.raises((SlayerError, ValueError), match="(?i)unknown aggregation"):
            await gen(sales_q(
                dimensions=["region"],
                measures=[ModelMeasure(formula=f"magic_fn({INNER_CR})", name="x")]))
