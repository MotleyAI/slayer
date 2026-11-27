"""Query input objects validate assignments; a failed assignment raises and leaves the object unchanged.

Spec: compatibility/ossie-vocabulary › Query objects validate assignments.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from slayer.core.assignment import AtomicAssignment
from slayer.core.enums import TimeGranularity
from slayer.core.query import (
    DatasetExtension, OrderItem, QueryDimension, QueryMetric, QueryRefinement, SlayerQuery, TimeDimension,
)
from tests._ossie_shapes_fixtures import execute, shop_engine_store
from tests._query_input_fixtures import BASE, RANGE, TD


def snapshot(obj: BaseModel) -> tuple[dict[str, Any], set[str]]:
    return copy.deepcopy(obj.model_dump(mode="python")), set(obj.model_fields_set)


def ranged_td() -> TimeDimension:
    return TimeDimension.model_validate({**TD, "date_range": RANGE})


#: (object builder, attribute, invalid value) per query input object.
INVALID_ASSIGNMENTS: dict[str, tuple[Callable[[], BaseModel], str, Any]] = {
    "SlayerQuery": (lambda: SlayerQuery.model_validate({**BASE, "limit": 5}), "limit", "abc"),
    "QueryRefinement": (lambda: QueryRefinement.model_validate({"limit": 5}), "limit", "abc"),
    "QueryDimension": (lambda: QueryDimension.model_validate({"field": "orders.status"}), "alias", "a.b"),
    "QueryMetric": (lambda: QueryMetric.model_validate({"metric": "count(orders.*)"}), "metric", 5),
    "TimeDimension": (ranged_td, "granularity", 5),
    "OrderItem": (lambda: OrderItem.model_validate({"field": "orders.status"}), "direction", "sideways"),
    "DatasetExtension": (lambda: DatasetExtension.model_validate({"dataset": "orders"}), "dataset", 5),
}


@pytest.mark.parametrize("name", sorted(INVALID_ASSIGNMENTS))
def test_a_query_input_object_shares_the_dataset_side_atomic_assignment(name: str) -> None:
    build, _, _ = INVALID_ASSIGNMENTS[name]
    assert isinstance(build(), AtomicAssignment)


class TestInvalidAssignments:
    @pytest.mark.parametrize("name", sorted(INVALID_ASSIGNMENTS))
    def test_an_invalid_assignment_raises_and_leaves_the_object_unchanged(self, name: str) -> None:
        build, attribute, bad = INVALID_ASSIGNMENTS[name]
        obj = build()
        before = snapshot(obj)
        with pytest.raises(ValidationError):
            setattr(obj, attribute, bad)
        assert snapshot(obj) == before

    @pytest.mark.parametrize("bad", [["2024-01-01", 5], ["bogus"]], ids=["mistyped_bound", "not_a_time_point"])
    def test_a_bad_date_range_is_refused_and_the_old_range_kept(self, bad: list[Any]) -> None:
        td = ranged_td()
        before = snapshot(td)
        with pytest.raises(ValidationError):
            td.date_range = bad
        assert td.date_range == RANGE
        assert snapshot(td) == before

    def test_a_bad_limit_is_refused_and_the_old_limit_kept(self) -> None:
        query = SlayerQuery.model_validate({**BASE, "limit": 5})
        before = snapshot(query)
        with pytest.raises(ValidationError):
            query.limit = "abc"  # pyright: ignore[reportAttributeAccessIssue]  # deliberate mistyped value
        assert query.limit == 5
        assert snapshot(query) == before

    def test_a_conflict_found_by_a_whole_query_check_restores_the_query(self) -> None:
        query = SlayerQuery.model_validate({**BASE, "time_dimensions": [{**TD, "date_range": RANGE}]})
        before = snapshot(query)
        conflicting = [ranged_td(), TimeDimension.model_validate({**TD, "date_range": ["2025"]})]
        with pytest.raises(ValidationError):
            query.time_dimensions = conflicting
        assert snapshot(query) == before


class TestValidAssignments:
    def test_a_valid_assignment_is_coerced_like_construction(self) -> None:
        td = ranged_td()
        td.date_range = "2024"  # pyright: ignore[reportAttributeAccessIssue]  # the single-period string form
        assert td.date_range == ["2024"]
        td.granularity = "week"
        assert isinstance(td.granularity, TimeGranularity)
        assert td.granularity is TimeGranularity.WEEK

    def test_an_expression_assigned_to_an_order_field_is_captured_like_construction(self) -> None:
        item = OrderItem.model_validate({"field": "orders.status"})
        item.field = "count(orders.*)"
        assert item.raw_expression == OrderItem.model_validate({"field": "count(orders.*)"}).raw_expression
        assert item.ref is None

    def test_a_reference_assigned_after_an_expression_drops_the_stale_capture(self) -> None:
        item = OrderItem.model_validate({"field": "count(orders.*)"})
        item.field = "orders.status"
        assert item.raw_expression is None
        assert item.ref is not None
        assert item.ref.full_name == "orders.status"

    def test_a_failed_order_field_assignment_keeps_the_old_capture(self) -> None:
        item = OrderItem.model_validate({"field": "count(orders.*)"})
        before = snapshot(item)
        with pytest.raises(ValidationError):
            item.field = 5  # pyright: ignore[reportAttributeAccessIssue]  # deliberate mistyped value
        assert snapshot(item) == before

    def test_an_explicit_raw_expression_at_construction_is_kept(self) -> None:
        item = OrderItem.model_validate({"field": "total", "raw_expression": "sum(orders.amount)"})
        assert item.raw_expression == "sum(orders.amount)"

    async def test_a_new_granularity_and_one_sided_range_apply_and_run(self, tmp_path: Path) -> None:
        td = ranged_td()
        td.granularity = "week"
        td.date_range = ["2024-01-01", None]
        assert (td.granularity, td.date_range) == (TimeGranularity.WEEK, ["2024-01-01", None])
        storage = await shop_engine_store(tmp_path)
        response = await execute(storage, SlayerQuery.model_validate({**BASE, "time_dimensions": [td]}))
        assert sum(int(r["orders._count"]) for r in response.data) == 5
