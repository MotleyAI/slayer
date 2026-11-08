"""Literal ``{var}`` placeholders inside a time-dimension expression (spec: queries/variables)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.query import SlayerQuery, extract_placeholder_names
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2015_fixtures import DS_GRANULARITIES
from tests._dev2119_fixtures import BACKENDS, by_bucket, engine_for, m, q, td

SHIFTED = "coalesce(shipped_at, created_at + INTERVAL '{days}' DAY)"
KEY = "orders.coalesce_shipped_at_created_at_interval_days_day"


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with engine_for(request.param) as e:
        yield e


def _query(**kw: Any) -> SlayerQuery:
    return q(measures=[m("count(*)")], **kw)


class TestConstruction:
    @pytest.mark.parametrize("entry", [td(SHIFTED), f"month({SHIFTED})"], ids=["object", "functional"])
    def test_literal_placeholder_accepted(self, entry: Any) -> None:
        query = _query(time_dimensions=[entry])
        assert query.time_dimensions is not None
        assert query.time_dimensions[0].dimension == SHIFTED

    def test_functional_form_in_dimensions_moves_to_time_dimensions(self) -> None:
        query = _query(dimensions=[f"month({SHIFTED})"])
        assert query.dimensions is None
        assert query.time_dimensions is not None
        assert query.time_dimensions[0].dimension == SHIFTED

    def test_placeholder_names_include_the_expression(self) -> None:
        assert extract_placeholder_names(_query(time_dimensions=[td(SHIFTED)])) == {"days"}


class TestExecution:
    #: Order 3 (created 2025-02-10) is the one unshipped row; the others bucket by ``shipped_at``.
    @pytest.mark.parametrize(("days", "expected"), [
        (0, {"2025-01": 1.0, "2025-02": 2.0, "2025-03": 2.0, "2025-04": 1.0}),
        (30, {"2025-01": 1.0, "2025-02": 1.0, "2025-03": 3.0, "2025-04": 1.0}),
    ])
    async def test_substituted_value_buckets_and_template_names(
        self, engine: SlayerQueryEngine, days: int, expected: dict,
    ) -> None:
        resp = await engine.execute(_query(time_dimensions=[td(SHIFTED)], variables={"days": days}))
        assert by_bucket(resp, KEY, "orders.v") == expected

    async def test_named_time_dimension_keeps_its_name(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_query(time_dimensions=[td(SHIFTED, name="effective_at")],
                                           variables={"days": 30}))
        assert by_bucket(resp, "orders.effective_at", "orders.v")["2025-03"] == 3.0

    async def test_missing_variable_is_rejected(self, engine: SlayerQueryEngine) -> None:
        query = _query(time_dimensions=[td(SHIFTED)])
        with pytest.raises(ValueError, match="days"):
            await engine.execute(query)


class TestTemplateIdentity:
    async def test_custom_granularity_call_names_from_its_template(self) -> None:
        async with engine_for("duckdb", datasource_fields=DS_GRANULARITIES) as engine:
            keys = {days: (await engine.execute(_query(dimensions=[f"fiscal_year({SHIFTED})"],
                                                       variables={"days": days}))).columns
                    for days in (0, 30)}
        assert KEY in keys[0]
        assert keys[0] == keys[30]

    async def test_equal_values_keep_distinct_templates_apart(self, engine: SlayerQueryEngine) -> None:
        shifted = "created_at + INTERVAL '{%s}' DAY"
        resp = await engine.execute(_query(time_dimensions=[td(shifted % "a"), td(shifted % "b")],
                                           variables={"a": 0, "b": 0}))
        assert resp.columns == [
            "orders.created_at_interval_a_day.month", "orders.created_at_interval_b_day.month", "orders.v",
        ]
