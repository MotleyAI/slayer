"""Pure ``QueryRefinement`` / ``refine_query`` merge rules (no DB)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from slayer.core.errors import RefinementConflictError
from slayer.core.models import ModelMeasure
from slayer.core.query import (
    ColumnRef,
    ComputedDimension,
    OrderItem,
    QueryRefinement,
    SlayerQuery,
    TimeDimension,
    refine_query,
)

REVENUE = {"formula": "sum(amount)", "name": "revenue"}
MONTH_TD = {"dimension": "ordered_at", "granularity": "month"}


def saved(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def merge(base: SlayerQuery, **refinement: Any) -> SlayerQuery:
    return refine_query(saved=base, refinement=QueryRefinement.model_validate(refinement))


def conflict(base: SlayerQuery, **refinement: Any) -> str:
    with pytest.raises(RefinementConflictError) as info:
        merge(base, **refinement)
    return str(info.value)


def dim_names(q: SlayerQuery) -> list[str | None]:
    return [d.name for d in q.dimensions or []]


def td_keys(q: SlayerQuery) -> list[tuple[str, str]]:
    return [(td.dimension.full_name, str(td.granularity)) for td in q.time_dimensions or []]


def only_td(q: SlayerQuery) -> TimeDimension:
    (td,) = q.time_dimensions or []
    return td


class TestQueryRefinementModel:
    @pytest.mark.parametrize("key, value", [
        ("source_model", "orders"),
        ("name", "x"),
        ("version", 4),
        ("variables", {"status": "paid"}),
        ("strict", True),
        ("bogus", 1),
    ])
    def test_forbidden_keys_rejected(self, key: str, value: Any) -> None:
        with pytest.raises(ValidationError):
            QueryRefinement.model_validate({key: value})

    def test_input_coercions_match_query(self) -> None:
        r = QueryRefinement.model_validate({
            "measures": ["count(*)"],
            "dimensions": ["region", {"expression": "amount * 2", "name": "dbl"}],
            "order": [{"revenue": "desc"}],
        })
        assert r.measures == [ModelMeasure(formula="count(*)")]
        assert r.dimensions == [ColumnRef(name="region"), ComputedDimension(expression="amount * 2", name="dbl")]
        assert r.order == [OrderItem(column=ColumnRef(name="revenue"), direction="desc")]

    def test_functional_granularity_in_dimensions_becomes_time_dimension(self) -> None:
        r = QueryRefinement.model_validate({"dimensions": ["region", "year(ordered_at)"]})
        assert [d.name for d in r.dimensions or []] == ["region"]
        assert [(td.dimension.name, str(td.granularity)) for td in r.time_dimensions or []] == [
            ("ordered_at", "year"),
        ]

    def test_time_dimension_string_form(self) -> None:
        r = QueryRefinement.model_validate({"time_dimensions": ["year(ordered_at)"]})
        assert [(td.dimension.name, str(td.granularity)) for td in r.time_dimensions or []] == [
            ("ordered_at", "year"),
        ]

    def test_window_filter_rejected(self) -> None:
        with pytest.raises(ValidationError):
            QueryRefinement.model_validate({"filters": ["sum(amount) OVER (PARTITION BY region) > 1"]})

    def test_supplied_fields_recorded_including_null(self) -> None:
        assert QueryRefinement.model_validate({"limit": None}).model_fields_set == {"limit"}
        assert QueryRefinement.model_validate({}).model_fields_set == set()


class TestDimensions:
    def test_union_appends_new_entries(self) -> None:
        assert dim_names(merge(saved(dimensions=["status"]), dimensions=["region"])) == ["status", "region"]

    def test_added_to_query_without_dimensions(self) -> None:
        assert dim_names(merge(saved(measures=[REVENUE]), dimensions=["region"])) == ["region"]

    @pytest.mark.parametrize("saved_ref, refined_ref", [
        ("region", "orders.region"),
        ("orders.region", "region"),
        ("region", "region"),
    ])
    def test_source_prefix_identity_collapses(self, saved_ref: str, refined_ref: str) -> None:
        base = saved(dimensions=[saved_ref], measures=[REVENUE])
        merged = merge(base, dimensions=[refined_ref])
        assert merged.dimensions == base.dimensions

    def test_same_column_different_label_conflicts(self) -> None:
        message = conflict(saved(dimensions=["region"]), dimensions=[{"name": "region", "label": "Region"}])
        assert "dimensions" in message
        assert "region" in message

    def test_identical_computed_dimension_collapses(self) -> None:
        base = saved(dimensions=[{"expression": "amount * 2", "name": "dbl"}])
        assert merge(base, dimensions=[{"expression": "amount * 2", "name": "dbl"}]).dimensions == base.dimensions

    def test_unnamed_computed_dimension_collapses_on_expression(self) -> None:
        base = saved(dimensions=["amount * 2"])
        assert merge(base, dimensions=["amount * 2"]).dimensions == base.dimensions

    def test_computed_dimension_name_clash_conflicts(self) -> None:
        message = conflict(
            saved(dimensions=[{"expression": "amount * 2", "name": "dbl"}]),
            dimensions=[{"expression": "amount * 3", "name": "dbl"}],
        )
        assert "dimensions" in message
        assert "dbl" in message
        assert "amount * 2" in message
        assert "amount * 3" in message


class TestMeasures:
    def test_union_appends_new_entries(self) -> None:
        merged = merge(saved(measures=[REVENUE]), measures=["count(*)"])
        assert [m.formula for m in merged.measures or []] == ["sum(amount)", "count(*)"]

    def test_identical_named_measure_collapses(self) -> None:
        base = saved(measures=[REVENUE])
        assert merge(base, measures=[REVENUE]).measures == base.measures

    def test_identical_unnamed_measure_collapses(self) -> None:
        base = saved(measures=["count(*)"])
        assert merge(base, measures=["count(*)"]).measures == base.measures

    def test_same_formula_other_name_is_a_new_measure(self) -> None:
        merged = merge(saved(measures=[REVENUE]), measures=[{"formula": "sum(amount)", "name": "rev2"}])
        assert [m.name for m in merged.measures or []] == ["revenue", "rev2"]

    def test_unnamed_formula_spelling_a_saved_name_is_a_new_measure(self) -> None:
        merged = merge(saved(measures=[REVENUE]), measures=["revenue"])
        assert [(m.name, m.formula) for m in merged.measures or []] == [("revenue", "sum(amount)"), (None, "revenue")]

    def test_same_name_different_formula_conflicts(self) -> None:
        message = conflict(saved(measures=[REVENUE]), measures=[{"formula": "count(*)", "name": "revenue"}])
        for part in ("measures", "revenue", "sum(amount)", "count(*)"):
            assert part in message

    @pytest.mark.parametrize("field, value", [
        ("label", "Revenue"),
        ("description", "Paid revenue"),
        ("type", "DOUBLE"),
        ("meta", {"unit": "usd"}),
    ])
    def test_named_measure_metadata_mismatch_conflicts(self, field: str, value: Any) -> None:
        message = conflict(saved(measures=[REVENUE]), measures=[{**REVENUE, field: value}])
        assert "measures" in message
        assert "revenue" in message

    def test_unnamed_measure_metadata_mismatch_conflicts(self) -> None:
        message = conflict(saved(measures=["count(*)"]), measures=[{"formula": "count(*)", "label": "Orders"}])
        assert "measures" in message
        assert "count(*)" in message
        assert "Orders" in message


class TestTimeDimensions:
    def test_new_granularity_appended(self) -> None:
        merged = merge(saved(time_dimensions=[MONTH_TD], measures=[REVENUE]), time_dimensions=["year(ordered_at)"])
        assert td_keys(merged) == [("ordered_at", "month"), ("ordered_at", "year")]

    def test_functional_dimension_merges_as_time_dimension(self) -> None:
        merged = merge(saved(time_dimensions=[MONTH_TD], measures=[REVENUE]), dimensions=["year(ordered_at)"])
        assert td_keys(merged) == [("ordered_at", "month"), ("ordered_at", "year")]
        assert not merged.dimensions

    def test_functional_dimension_on_saved_key_collapses(self) -> None:
        base = saved(time_dimensions=[MONTH_TD], measures=[REVENUE])
        assert merge(base, dimensions=["month(ordered_at)"]).time_dimensions == base.time_dimensions

    def test_date_range_taken_from_refinement(self) -> None:
        merged = merge(
            saved(time_dimensions=[MONTH_TD], measures=[REVENUE]),
            time_dimensions=[{**MONTH_TD, "date_range": ["2025-02-01", "2025-03-31"]}],
        )
        assert only_td(merged).date_range == ["2025-02-01", "2025-03-31"]

    def test_saved_date_range_kept_when_refinement_omits_it(self) -> None:
        base = saved(time_dimensions=[{**MONTH_TD, "date_range": ["2025-01-01", "2025-02-28"]}], measures=[REVENUE])
        assert merge(base, time_dimensions=[MONTH_TD]).time_dimensions == base.time_dimensions

    def test_prefixed_column_is_the_same_key(self) -> None:
        base = saved(time_dimensions=[MONTH_TD], measures=[REVENUE])
        merged = merge(base, time_dimensions=[{"dimension": "orders.ordered_at", "granularity": "month",
                                               "date_range": ["2025-02-01", "2025-03-31"]}])
        assert only_td(merged).date_range == ["2025-02-01", "2025-03-31"]

    @pytest.mark.parametrize("saved_range, refined_range", [
        (["2025-01-01", "2025-02-28"], ["2025-01-01", "2025-02-28"]),
        (["2025-01"], "2025-01"),
        ("2025-01", ["2025-01"]),
    ])
    def test_equal_date_range_after_normalization(self, saved_range: Any, refined_range: Any) -> None:
        base = saved(time_dimensions=[{**MONTH_TD, "date_range": saved_range}], measures=[REVENUE])
        merged = merge(base, time_dimensions=[{**MONTH_TD, "date_range": refined_range}])
        assert merged.time_dimensions == base.time_dimensions

    def test_conflicting_date_range(self) -> None:
        message = conflict(
            saved(time_dimensions=[{**MONTH_TD, "date_range": ["2025-01-01", "2025-02-28"]}], measures=[REVENUE]),
            time_dimensions=[{**MONTH_TD, "date_range": ["2025-02-01", "2025-03-31"]}],
        )
        assert "time_dimensions" in message
        assert "ordered_at@month" in message
        assert "2025-01-01" in message
        assert "2025-03-31" in message
        assert "filter" in message

    def test_label_taken_from_either_side(self) -> None:
        base = saved(time_dimensions=[{**MONTH_TD, "label": "Month"}], measures=[REVENUE])
        assert merge(base, time_dimensions=[MONTH_TD]).time_dimensions == base.time_dimensions
        merged = merge(saved(time_dimensions=[MONTH_TD], measures=[REVENUE]),
                       time_dimensions=[{**MONTH_TD, "label": "Month"}])
        assert only_td(merged).label == "Month"

    def test_conflicting_label(self) -> None:
        message = conflict(
            saved(time_dimensions=[{**MONTH_TD, "label": "Month"}], measures=[REVENUE]),
            time_dimensions=[{**MONTH_TD, "label": "Period"}],
        )
        assert "time_dimensions" in message
        assert "ordered_at@month" in message


class TestFilters:
    def test_filters_and_together_in_order(self) -> None:
        merged = merge(saved(measures=[REVENUE], filters=["status = 'paid'"]), filters=["amount >= 50"])
        assert merged.filters == ["status = 'paid'", "amount >= 50"]

    def test_exact_duplicate_dropped(self) -> None:
        merged = merge(saved(measures=[REVENUE], filters=["status = 'paid'"]),
                       filters=["status = 'paid'", "amount >= 50"])
        assert merged.filters == ["status = 'paid'", "amount >= 50"]

    def test_added_to_query_without_filters(self) -> None:
        assert merge(saved(measures=[REVENUE]), filters=["amount >= 50"]).filters == ["amount >= 50"]


class TestReplacedSettings:
    ORDERED = {"measures": [REVENUE], "dimensions": ["region"],
               "order": [{"column": "revenue", "direction": "desc"}], "limit": 2, "offset": 1}

    def test_order_replaced(self) -> None:
        merged = merge(saved(**self.ORDERED), order=[{"column": "region", "direction": "asc"}])
        assert merged.order == [OrderItem(column=ColumnRef(name="region"), direction="asc")]

    @pytest.mark.parametrize("cleared", [[], None])
    def test_order_cleared(self, cleared: Any) -> None:
        assert not merge(saved(**self.ORDERED), order=cleared).order

    @pytest.mark.parametrize("field, value", [("limit", 5), ("offset", 3)])
    def test_limit_offset_replaced(self, field: str, value: int) -> None:
        assert getattr(merge(saved(**self.ORDERED), **{field: value}), field) == value

    @pytest.mark.parametrize("field", ["limit", "offset"])
    def test_limit_offset_cleared_by_null(self, field: str) -> None:
        assert getattr(merge(saved(**self.ORDERED), **{field: None}), field) is None

    def test_omitted_settings_kept(self) -> None:
        base = saved(**self.ORDERED)
        merged = merge(base, filters=["amount >= 50"])
        assert (merged.order, merged.limit, merged.offset) == (base.order, base.limit, base.offset)

    @pytest.mark.parametrize("field, saved_value, refined_value", [
        ("to_many_handling", "associate", "error"),
        ("to_many_handling", "associate", "broadcast"),
        ("whole_periods_only", True, False),
        ("main_time_dimension", "ordered_at", "shipped_at"),
    ])
    def test_scalar_replaced_only_when_supplied(self, field: str, saved_value: Any, refined_value: Any) -> None:
        base = saved(time_dimensions=[MONTH_TD], measures=[REVENUE], **{field: saved_value})
        assert getattr(merge(base, **{field: refined_value}), field) == refined_value
        assert getattr(merge(base, filters=["amount >= 50"]), field) == saved_value

    def test_distinct_dimension_values_replaced(self) -> None:
        base = saved(dimensions=["region"])
        assert merge(base, distinct_dimension_values=False).distinct_dimension_values is False
        assert merge(base, dimensions=["status"]).distinct_dimension_values is True


class TestWholeQuery:
    def test_empty_refinement_is_lossless(self) -> None:
        base = SlayerQuery.model_validate({
            "name": "final", "source_model": "orders", "dimensions": ["region"],
            "time_dimensions": [{**MONTH_TD, "date_range": ["2025-01-01", "2025-02-28"], "label": "Month"}],
            "measures": [REVENUE, "count(*)"], "filters": ["status = '{status}'"],
            "variables": {"status": "paid"}, "order": [{"column": "revenue", "direction": "desc"}],
            "limit": 2, "offset": 1, "to_many_handling": "associate", "whole_periods_only": True,
        })
        merged = merge(base)
        assert merged == base
        assert merged.model_dump() == base.model_dump()

    def test_non_refinable_fields_survive(self) -> None:
        base = SlayerQuery.model_validate({
            "name": "final", "source_model": {"source_name": "orders"}, "measures": [REVENUE],
            "variables": {"status": "paid"},
        })
        merged = merge(base, dimensions=["region"])
        assert (merged.name, merged.source_model, merged.variables, merged.version) == (
            base.name, base.source_model, base.variables, base.version,
        )

    def test_merged_query_revalidated(self) -> None:
        refinement = QueryRefinement.model_validate({"distinct_dimension_values": False})
        base = saved(dimensions=["region"], measures=["count(*)"])
        with pytest.raises(ValueError):
            refine_query(saved=base, refinement=refinement)

    def test_inputs_not_mutated(self) -> None:
        base = saved(dimensions=["region"], measures=[REVENUE], filters=["status = 'paid'"])
        refinement = QueryRefinement.model_validate({"dimensions": ["status"], "filters": ["amount > 1"]})
        base_before, refinement_before = base.model_dump(), refinement.model_dump()
        refine_query(saved=base, refinement=refinement)
        assert base.model_dump() == base_before
        assert refinement.model_dump() == refinement_before
