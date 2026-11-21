"""The narrowed parse/bind gate (spec: aggregations/
expression-aggregation + queries/partitioned-aggregates fail-closed scenarios)."""

from __future__ import annotations

import re

import pytest

from slayer.core.errors import SlayerError
from slayer.core.keys import AggregateKey, TransformKey
from slayer.core.scope import DatasetScope
from slayer.engine.binding import _source_is_reaggregation, bind_expr
from slayer.core.syntax import AggCall, parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from slayer.core.query import QueryDimension, QueryMetric

from tests._dev1847_fixtures import (
    dev1847_datasets,
    INNER_CR,
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
        DIRECTLY — no scalar-call wrapper — and a row-level comparison is a
        legal row-level source."""
        for f in (f"count({INNER_CR} > 0)",
                  f"sum({INNER_CR} > 45 and {INNER_CR} < 100)"):
            parsed = parse_expr(f)
            assert isinstance(parsed, AggCall)
            assert _source_is_reaggregation(parsed.source)
        row_level = parse_expr("sum(sales.amount > 45)")
        assert isinstance(row_level, AggCall)
        assert not _source_is_reaggregation(row_level.source)

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
        sql = await gen(sales_q(dimensions=["sales.region"],
                                metrics=[QueryMetric(metric=f"avg({INNER_CR})",
                                                       alias="acr")]))
        assert "SELECT" in sql.upper()


class TestRejections:
    def test_mixed_row_and_attached_accepted(self):
        """A source mixing a row-level reference with an attached value is a
        row-grain aggregation and parses. A nested transform in a
        source now parses too; only a row-level leaf under it is
        rejected, at plan time — see test_dev1832_transform_source."""
        parsed = parse_expr("sum(sales.amount * avg(sales.amount, partition_by=sales.city))")
        assert isinstance(parsed, AggCall)
        assert parsed.agg == "sum"

    async def test_column_parameter_on_outer_rejected(self):
        """Scenario: Column-reference outer parameter fails closed — explicit
        ``weight=id`` is undetermined at the operand grain; the one-rule error
        names the parameter, the grain, and the partition_by remedy,
        ref-free."""
        query = sales_q(
            dimensions=["sales.region"],
            metrics=[QueryMetric(
                metric=f"wavg({INNER_CR}, weight=sales.id)", alias="w")])
        with pytest.raises(SlayerError) as ei:
            await gen(query)
        msg = str(ei.value)
        assert "weight" in msg
        assert "grain" in msg.lower()
        assert "partition_by" in msg
        assert not re.search(r"DEV-\d+", msg)

    async def test_column_default_parameter_on_outer_rejected(self):
        """Scenario: Column-reference outer parameter fails closed — ``wavg``'s
        weight default (the ``amount`` field) is undetermined at the operand
        grain; same one-rule error."""
        query = sales_q(
            dimensions=["sales.region"],
            metrics=[QueryMetric(metric=f"wavg({INNER_CR})", alias="w")])
        with pytest.raises(SlayerError) as ei:
            await gen(query)
        msg = str(ei.value)
        assert "weight" in msg
        assert "grain" in msg.lower()
        assert "partition_by" in msg
        assert not re.search(r"DEV-\d+", msg)

    async def test_outer_window_fails_closed(self):
        """Scenario: Outer window and outer filter fail closed — window= over an
        attached operand is a typed error naming the combination, never a
        NotImplementedError."""
        query = sales_q(
            dimensions=["sales.region"],
            metrics=[QueryMetric(
                metric=f"sum({INNER_CR}, window='90d')", alias="w")])
        with pytest.raises((SlayerError, ValueError)) as ei:
            await gen(query)
        msg = str(ei.value)
        assert not isinstance(ei.value, NotImplementedError)
        # The pure-attached source is accepted; the rejection is the specific
        # window-over-aggregate one, NOT the generic expression-nesting gate
        # (whose message echoes 'window' from the expression — so match the phrase).
        assert "nested inside the expression aggregated" not in msg
        assert "window" in msg.lower()


class TestCrossModelAndFilteredOperandNowAccepted:
    """The two v1 boundaries the narrowed gate kept are lifted (positive
    coverage in test_dev1832_cross_model_exec / test_dev1832_column_filter)."""

    async def test_cross_model_expression_accepted(self):
        """A dotted joined-dataset leaf inside the expression now compiles."""
        query = SlayerQuery(
            population="corders",
            dimensions=[QueryDimension(field="corders.customer_id")],
            metrics=[QueryMetric(metric="sum(corders.amount - customers.region_id)",
                                   alias="x")])
        assert "SELECT" in (await gen(query)).upper()

    async def test_filtered_column_operand_accepted(self):
        """A filtered field operand desugars to CASE WHEN, not a rejection."""
        query = sales_q(dimensions=["sales.region"],
                        metrics=[QueryMetric(metric="sum(sales.q_amount - 1)",
                                               alias="x")])
        assert "CASE WHEN" in (await gen(query)).upper()


def _bound(expression: str):
    models = dev1847_datasets()
    bundle = ResolvedSourceBundle(dialect="postgres", population=models[0], referenced_datasets=models[1:])
    return bind_expr(
        parse_expr(expression), scope=DatasetScope(population=models[0]), bundle=bundle,
    ).value_key


class TestFirstLastDispatchUnchanged:
    @pytest.mark.parametrize("fn", ["first", "last"])
    def test_first_last_over_aggregate_binds_as_transform(self, fn):
        """first/last over an aggregated first arg is the transform."""
        key = _bound(f"{fn}({INNER_CR})")
        assert isinstance(key, TransformKey)
        assert key.op == fn

    def test_last_over_plain_column_is_aggregation(self):
        """The non-aggregated first argument still routes to the aggregation."""
        key = _bound("last(sales.amount)")
        assert isinstance(key, AggregateKey)
        assert key.agg == "last"


class TestUnknownOuterAggregationRejectedAtBinding:
    async def test_unknown_custom_name_over_aggregate_errors(self):
        """Unknown custom-aggregation candidates are rejected at binding."""
        query = sales_q(
            dimensions=["sales.region"],
            metrics=[QueryMetric(metric=f"magic_fn({INNER_CR})", alias="x")])
        with pytest.raises((SlayerError, ValueError), match="(?i)unknown function"):
            await gen(query)
