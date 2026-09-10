"""DEV-1847 task 1.5/2.1 — the narrowed parse/bind gate. A fully-attached
aggregation source is accepted (re-aggregation); transforms, mixed row/attached
sources, and the outer window= combination are rejected with typed errors;
first/last keep their transform dispatch.

Spec: openspec …/specs/aggregations/expression-aggregation — "Unsupported
expression shapes fail with clear errors" (MODIFIED); queries/partitioned-
aggregates — "Outer window and outer filter fail closed", "First and last keep
transform dispatch".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.engine.syntax import AggCall, TransformCall, parse_expr

from tests._dev1847_fixtures import (
    INNER_CR,
    ModelMeasure,
    SlayerQuery,
    gen,
    sales_q,
)


class TestFullyAttachedAccepted:
    def test_pure_attached_source_parses(self):
        """Scenario: Fully attached source accepted — avg(sum(..., partition_by=…))
        parses into the ordinary AggCall shape, not rejected by the gate."""
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
        """Accepted end-to-end: the re-aggregation emits SQL rather than raising
        the expression gate."""
        sql = await gen(sales_q(dimensions=["region"],
                                measures=[ModelMeasure(formula=f"avg({INNER_CR})",
                                                       name="acr")]))
        assert "SELECT" in sql.upper()


class TestRejections:
    def test_nested_transform_rejected(self):
        """Scenario: Nested aggregation rejected — a transform inside the
        aggregated expression stays rejected."""
        with pytest.raises(ValueError, match="(?i)nest|transform"):
            parse_expr("sum(cumsum(amount) - 1)")

    def test_mixed_row_and_attached_rejected(self):
        """Scenario: Nested aggregation rejected — a source mixing a row-level
        reference with an attached value is rejected (DEV-1859's boundary)."""
        with pytest.raises(ValueError, match="(?i)mix|row|attach|nest"):
            parse_expr("sum(amount * avg(amount, partition_by=city))")

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
        """Scenario: Cross-model expression rejected — the error states
        cross-model expression aggregation is not supported."""
        with pytest.raises((SlayerError, ValueError), match="(?i)cross-model"):
            await gen(SlayerQuery(
                source_model="corders", dimensions=["customer_id"],
                measures=[ModelMeasure(formula="sum(amount - customers.region_id)",
                                       name="x")]))

    async def test_filtered_column_operand_rejected(self):
        """Scenario: Filtered-column operand rejected — q_amount carries a
        column-level filter, so the error names the column."""
        with pytest.raises((SlayerError, ValueError), match="q_amount"):
            await gen(sales_q(dimensions=["region"],
                              measures=[ModelMeasure(formula="sum(q_amount - 1)",
                                                     name="x")]))


class TestFirstLastDispatchUnchanged:
    @pytest.mark.parametrize("fn", ["first", "last"])
    def test_first_last_over_aggregate_parses_as_transform(self, fn):
        """Scenario: First and last keep transform dispatch — first/last over an
        aggregated first argument is the transform, not an aggregation."""
        parsed = parse_expr(f"{fn}({INNER_CR})")
        assert isinstance(parsed, TransformCall)
        assert parsed.op == fn

    def test_last_over_plain_column_is_aggregation(self):
        """The non-aggregated first argument still routes to the aggregation."""
        parsed = parse_expr("last(amount)")
        assert isinstance(parsed, AggCall)
