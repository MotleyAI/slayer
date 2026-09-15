"""DEV-1859 task 1.4 — plan-structure pins for the row-grain branch (design
D1/D2): a mixed root never becomes a re-aggregation carrier; its constituents
are row-phase attaches; the outer aggregate stays inline; producers stay pure.

Spec: openspec …/specs/queries/partitioned-aggregates — "Never the
fully-attached carrier"; aggregations/expression-aggregation — "Mixed row and
attached source accepted".
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.keys import AggregateKey, ColumnKey, walk_value_keys
from slayer.engine.plan import plan_query
from slayer.engine.syntax import AggCall, parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1847_fixtures import (
    INNER_UP_CITY,
    INNER_UP_PRODUCT,
    MIXED_SUM,
    ModelMeasure,
    dev1847_models,
    gen,
    sales_q,
)

TWO_CONSTITUENTS = (
    f"sum(quantity * ({INNER_UP_PRODUCT} + {INNER_UP_CITY}))"
)


def _bundle() -> ResolvedSourceBundle:
    models = dev1847_models()
    return ResolvedSourceBundle(source_model=models[0],
                                referenced_models=models[1:])


def _plan(formula: str):
    return plan_query(
        query=sales_q(dimensions=["region"],
                      measures=[ModelMeasure(formula=formula, name="m")]),
        bundle=_bundle())


def _references_quantity(key) -> bool:
    return any(isinstance(k, ColumnKey) and k.leaf == "quantity"
               for k in walk_value_keys(key))


class TestParseAcceptance:
    def test_mixed_source_parses_as_aggregation(self):
        """Scenario: Mixed row and attached source accepted — the parse gate
        no longer rejects the mixing."""
        parsed = parse_expr(MIXED_SUM)
        assert isinstance(parsed, AggCall)
        assert parsed.agg == "sum"

    def test_nested_transform_in_source_stays_rejected(self):
        with pytest.raises(ValueError, match="(?i)transform"):
            parse_expr("sum(cumsum(quantity) - 1)")


class TestRowGrainPlanShape:
    def test_constituent_is_a_row_phase_attach(self):
        planned = _plan(MIXED_SUM)
        [attach] = planned.regroup_attach_plans
        assert attach.attach_phase == "row"
        assert len(attach.join_pairs) == 1  # the complete [product] grain

    def test_mixed_root_is_not_a_reaggregation_root(self):
        """The substitution consumes the INNER aggregate only — never the
        whole mixed root (whose carrier value would be cell-over-cell)."""
        planned = _plan(MIXED_SUM)
        subs = [s for a in planned.regroup_attach_plans for s in a.substitutions]
        assert subs
        for sub in subs:
            assert isinstance(sub.original_key, AggregateKey)
            assert sub.original_key.agg == "avg"
            assert not _references_quantity(sub.original_key)

    def test_outer_slot_stays_an_inline_aggregate(self):
        planned = _plan(MIXED_SUM)
        [attach] = planned.regroup_attach_plans
        [sub] = attach.substitutions
        outer = [
            s for s in planned.aggregate_slots
            if isinstance(s.key, AggregateKey) and s.key.agg == "sum"
            and any(k == sub.placeholder for k in walk_value_keys(s.key))
        ]
        assert len(outer) == 1
        assert _references_quantity(outer[0].key)

    def test_producer_groups_by_neither_placeholder_nor_row_leaf(self):
        planned = _plan(MIXED_SUM)
        [attach] = planned.regroup_attach_plans
        [sub] = attach.substitutions
        producer = attach.producer_plan
        for slot in [*producer.row_slots, *producer.aggregate_slots,
                     *producer.combined_expression_slots]:
            assert not _references_quantity(slot.key)
            assert not any(k == sub.placeholder
                           for k in walk_value_keys(slot.key))

    def test_each_constituent_gets_its_own_row_attach(self):
        planned = _plan(TWO_CONSTITUENTS)
        attaches = planned.regroup_attach_plans
        assert len(attaches) == 2
        assert all(a.attach_phase == "row" for a in attaches)


class TestEmittedGroupByStaysPure:
    """Scenario: Never the fully-attached carrier — SQL-level belt over the
    plan pins: no relation's GROUP BY touches the row leaf or a placeholder."""

    @pytest.mark.parametrize("formula", [MIXED_SUM, TWO_CONSTITUENTS])
    async def test_no_group_by_references_the_row_leaf(self, formula):
        sql = await gen(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=formula, name="m")]))
        ast = sqlglot.parse_one(sql, read="postgres")
        groups = list(ast.find_all(exp.Group))
        assert groups
        for group in groups:
            assert "__regroup__" not in group.sql(), sql
            cols = {c.name for c in group.find_all(exp.Column)}
            assert "quantity" not in cols, sql


class TestFirstOverMixedKeepsExpressionError:
    async def test_first_over_mixed_source_expression_error(self):
        """Scenario: Ranked aggregation over a mixed source keeps the
        expression error — never a mixing error, never wrong values."""
        with pytest.raises(ValueError, match="not supported over an expression"):
            await gen(sales_q(
                dimensions=["region"],
                measures=[ModelMeasure(
                    formula=f"first(quantity * {INNER_UP_PRODUCT})",
                    name="f")]))
