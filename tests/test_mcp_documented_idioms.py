"""Every query idiom the MCP surface documents runs and returns the documented result."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

import pytest

from slayer.core.errors import AssociatedGrainWarning, BroadcastGrainWarning
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests import _mcp_idiom_fixtures as fx

_YEAR_2025 = [{"dimension": "order_date", "granularity": "month", "date_range": "2025"}]
_MONTHLY = [{"dimension": "order_date", "granularity": "month"}]
_REGION_CUSTOMER = ["customers.regions.name", "customers.name"]


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    async for e in fx.make_idiom_engine():
        yield e


@pytest.fixture
async def namesake_engine() -> AsyncIterator[SlayerQueryEngine]:
    async for e in fx.make_idiom_engine(with_namesake=True):
        yield e


@pytest.fixture
async def last_year_engine() -> AsyncIterator[SlayerQueryEngine]:
    async for e in fx.make_idiom_engine(with_last_year=True):
        yield e


@pytest.fixture
async def items_engine() -> AsyncIterator[SlayerQueryEngine]:
    async for e in fx.make_items_engine():
        yield e


@pytest.fixture
async def child_engine() -> AsyncIterator[SlayerQueryEngine]:
    async for e in fx.make_child_engine():
        yield e


@pytest.fixture
async def child_engine_all_regioned() -> AsyncIterator[SlayerQueryEngine]:
    async for e in fx.make_child_engine(with_regionless=False):
        yield e


async def _run(engine: SlayerQueryEngine, query: dict[str, Any] | list[dict[str, Any]]) -> SlayerResponse:
    if isinstance(query, list):
        return await engine.execute(query=[SlayerQuery.model_validate(q) for q in query])
    return await engine.execute(query=SlayerQuery.model_validate(query))


def _kinds(resp: SlayerResponse) -> set[str]:
    return {w.kind for w in resp.warnings}


async def test_change_change_pct_and_time_shift_look_back_before_date_range(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "time_dimensions": _YEAR_2025,
        "measures": [
            {"formula": fx.CHANGE, "name": "chg"},
            {"formula": fx.CHANGE_PCT, "name": "chg_pct"},
            {"formula": fx.TIME_SHIFT, "name": "prev"},
        ],
    })
    jan = fx.rows_by(resp, key="order_date")["2025-01"]
    assert fx.value(jan, ".chg") == 105
    assert fx.value(jan, ".chg_pct") == pytest.approx(105 / 45)
    assert fx.value(jan, ".prev") == 45


async def test_forward_time_shift_reads_after_date_range(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders",
        "time_dimensions": [{"dimension": "order_date", "granularity": "month", "date_range": ["2025-02-01", "2025-03-31"]}],
        "measures": [{"formula": "time_shift(sum(amount), 1)", "name": "next"}],
    })
    assert fx.value(fx.rows_by(resp, key="order_date")["2025-03"], ".next") == 60


@pytest.mark.parametrize(("formula", "month", "paid", "unfiltered"), [
    (fx.CHANGE, "2025-04", None, 30),
    (fx.CHANGE_PCT, "2025-04", None, pytest.approx(1.0)),
    (fx.TIME_SHIFT, "2025-04", None, 30),
    ("time_shift(sum(amount), 1)", "2025-01", None, 70),
])
async def test_non_time_filter_restricts_calendar_look_backs(
    engine: SlayerQueryEngine, formula: str, month: str, paid: Any, unfiltered: Any,
) -> None:
    async def value(filters: list[str]) -> Any:
        resp = await _run(engine, {
            "source_model": "orders",
            "time_dimensions": [{"dimension": "order_date", "granularity": "month", "date_range": ["2025-01-01", "2025-04-30"]}],
            "measures": [{"formula": formula, "name": "v"}], "filters": filters,
        })
        return fx.value(fx.rows_by(resp, key="order_date")[month], ".v")

    assert await value(["status == 'paid'"]) == paid
    assert await value([]) == unfiltered


async def test_trailing_window_reads_rows_before_date_range(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "time_dimensions": _YEAR_2025,
        "measures": [{"formula": fx.TRAILING_WINDOW, "name": "w"}, {"formula": "count_distinct(customer_id)", "name": "nw"}],
    })
    jan = fx.rows_by(resp, key="order_date")["2025-01"]
    assert fx.value(jan, ".w") == 3
    assert fx.value(jan, ".nw") == 2


async def test_cumsum_starts_at_the_range_start(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "time_dimensions": _YEAR_2025,
        "measures": [{"formula": fx.CUMSUM, "name": "run"}],
    })
    by_month = fx.rows_by(resp, key="order_date")
    assert fx.value(by_month["2025-01"], ".run") == 150
    assert fx.value(by_month["2025-02"], ".run") == 220


async def test_anti_join_keeps_rows_without_a_joined_row(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {"source_model": "customers", "dimensions": ["name"], "filters": [fx.ANTI_JOIN]})
    assert {fx.value(r, ".name") for r in resp.data} == {"Dan", "Eve"}


async def test_root_model_keeps_every_row(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "regions", "dimensions": ["name"],
        "measures": [
            {"formula": "count(customers.id)", "name": "n_customers"},
            {"formula": fx.POPULATION_COUNT, "name": "n_orders"},
            {"formula": fx.CONDITIONAL, "name": "orders_2025"},
        ],
    })
    rows = fx.rows_by(resp, key=".name")
    assert {k: fx.value(r, ".n_customers") for k, r in rows.items()} == {"North": 2, "South": 2, "East": 1, "West": 0}
    assert {k: fx.value(r, ".n_orders") for k, r in rows.items()} == {"North": 8, "South": 3, "East": 0, "West": 0}
    # A conditional sum over no rows is NULL, not 0.
    assert {k: fx.value(r, ".orders_2025") for k, r in rows.items()} == {
        "North": 6, "South": 2, "East": None, "West": None,
    }


async def test_inferred_root_keeps_rows_without_facts(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "dimensions": ["regions.name"], "measures": [{"formula": "count(customers.id)", "name": "n_customers"}],
    })
    rows = fx.rows_by(resp, key=".name")
    assert {k: fx.value(r, ".n_customers") for k, r in rows.items()} == {"North": 2, "South": 2, "East": 1, "West": 0}


def test_source_model_guidance_drops_factless_rows_only_at_a_fact_root() -> None:
    text = SlayerQuery.model_fields["source_model"].description or ""
    assert text.index("drop out") < text.index("omit to infer")


async def test_order_by_an_undisplayed_aggregate_with_limit(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": ["customers.name"], "measures": ["count(*)"],
        "order": [{"column": "sum(amount)", "direction": "desc"}], "limit": 2,
    })
    assert [fx.value(r, ".name") for r in resp.data] == ["Carol", "Alice"]
    assert not any("amount" in c for c in resp.columns)


async def test_rank_filter_keeps_top_n_per_group(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": _REGION_CUSTOMER,
        "measures": [{"formula": "sum(amount)", "name": "total"}], "filters": [fx.RANK_FILTER],
    })
    assert {fx.value(r, "customers.name") for r in resp.data} == {"Alice", "Carol"}


async def test_share_of_group_and_of_grand_total(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": _REGION_CUSTOMER,
        "measures": [
            {"formula": fx.SHARE_OF_GROUP, "name": "share_region"},
            {"formula": fx.SHARE_OF_TOTAL, "name": "share_total"},
        ],
    })
    rows = fx.rows_by(resp, key="customers.name")
    assert {k: fx.value(r, ".share_region") for k, r in rows.items()} == {
        "Alice": pytest.approx(170 / 300), "Bob": pytest.approx(130 / 300), "Carol": pytest.approx(1.0),
    }
    assert {k: fx.value(r, ".share_total") for k, r in rows.items()} == {
        "Alice": pytest.approx(170 / 490), "Bob": pytest.approx(130 / 490), "Carol": pytest.approx(190 / 490),
    }


async def test_cross_model_aggregate_broadcasts_by_default(engine: SlayerQueryEngine) -> None:
    with pytest.warns(BroadcastGrainWarning):
        resp = await _run(engine, {
            "source_model": "orders", "dimensions": ["status"],
            "measures": [{"formula": fx.CROSS_MODEL, "name": "credit"}],
        })
    assert {k: fx.value(r, ".credit") for k, r in fx.rows_by(resp, key=".status").items()} == {
        "paid": 1500, "open": 1500,
    }
    assert "broadcast" in _kinds(resp)


async def test_cross_model_aggregate_splits_under_associate(engine: SlayerQueryEngine) -> None:
    with pytest.warns(AssociatedGrainWarning):
        resp = await _run(engine, {
            "source_model": "orders", "dimensions": ["status"], "to_many_handling": "associate",
            "measures": [{"formula": fx.CROSS_MODEL, "name": "credit"}],
        })
    assert {k: fx.value(r, ".credit") for k, r in fx.rows_by(resp, key=".status").items()} == {
        "paid": 600, "open": 600,
    }


async def test_nested_aggregation_averages_inner_values(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": ["customers.regions.name"],
        "measures": [{"formula": fx.NESTED, "name": "avg_customer_total"}],
    })
    rows = fx.rows_by(resp, key="regions.name")
    assert {k: fx.value(r, ".avg_customer_total") for k, r in rows.items()} == {
        "North": pytest.approx(150), "South": pytest.approx(190),
    }


async def test_per_customer_examples_keep_namesakes_apart(namesake_engine: SlayerQueryEngine) -> None:
    nested = await _run(namesake_engine, {
        "source_model": "orders", "dimensions": ["customers.regions.name"],
        "measures": [{"formula": fx.NESTED, "name": "avg_customer_total"}],
    })
    rows = fx.rows_by(nested, key="regions.name")
    assert fx.value(rows["North"], ".avg_customer_total") == pytest.approx((170 + 130 + 40) / 3)
    bands = await _run(namesake_engine, {
        "source_model": "orders", "dimensions": [{"expression": fx.SPEND_BAND, "name": "spend_band"}],
        "measures": ["count(*)"],
    })
    assert {k: fx.value(r, "._count") for k, r in fx.rows_by(bands, key=".spend_band").items()} == {
        "high": 8, "low": 4,
    }


async def test_time_spine_returns_every_bucket_of_the_range(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "time_dimensions": [json.loads(fx.SPINE_TIME_DIMENSION)],
        "measures": [{"formula": fx.SPINE_MEASURE, "name": "total"}, {"formula": "count(orders.id)", "name": "n"}],
    })
    col = next(c for c in resp.columns if c.endswith("timestamp"))
    by_month = {str(r[col])[:7]: (fx.value(r, ".total"), fx.value(r, ".n")) for r in resp.data}
    assert len(resp.data) == 12
    assert sorted(by_month) == [f"2025-{m:02d}" for m in range(1, 13)]
    assert by_month["2025-05"] == (None, 0)
    assert by_month["2025-01"] == (150, 2)
    assert by_month["2025-06"] == (105, 3)


async def test_several_child_models_each_over_their_own_rows(child_engine: SlayerQueryEngine) -> None:
    measures = [
        {"formula": "sum(orders.amount)", "name": "ordered"},
        {"formula": fx.SECOND_CHILD, "name": "returned"},
    ]
    resp = await _run(child_engine, {"source_model": "customers", "dimensions": ["name"], "measures": measures})
    assert {
        fx.value(r, ".name"): (fx.value(r, ".ordered"), fx.value(r, ".returned")) for r in resp.data
    } == {"Alice": (30, 7), "Bob": (35, None), "Carol": (40, 8), "Dan": (50, None)}
    total = await _run(child_engine, {"source_model": "customers", "measures": measures})
    assert [(fx.value(r, ".ordered"), fx.value(r, ".returned")) for r in total.data] == [(168, 15)]


async def test_parentless_child_rows_land_in_the_null_parent_row(child_engine: SlayerQueryEngine) -> None:
    measures = [{"formula": "sum(orders.amount)", "name": "ordered"}]
    resp = await _run(child_engine, {"source_model": "customers", "dimensions": ["regions.name"], "measures": measures})
    assert {fx.value(r, ".name"): fx.value(r, ".ordered") for r in resp.data} == {
        "North": 65, "South": 40, None: 63,
    }
    total = await _run(child_engine, {"source_model": "customers", "measures": measures})
    assert [fx.value(r, ".ordered") for r in total.data] == [168]


async def test_parentless_child_rows_without_a_null_population_row(
    child_engine_all_regioned: SlayerQueryEngine,
) -> None:
    measures = [{"formula": "sum(orders.amount)", "name": "ordered"}]
    resp = await _run(child_engine_all_regioned, {
        "source_model": "customers", "dimensions": ["regions.name"], "measures": measures,
    })
    assert {fx.value(r, ".name"): fx.value(r, ".ordered") for r in resp.data} == {"North": 65, "South": 40}
    total = await _run(child_engine_all_regioned, {"source_model": "customers", "measures": measures})
    assert [fx.value(r, ".ordered") for r in total.data] == [168]


async def test_cross_model_ratio(child_engine: SlayerQueryEngine) -> None:
    resp = await _run(child_engine, {
        "source_model": "customers", "measures": [{"formula": fx.CROSS_MODEL_RATIO, "name": "per_customer"}],
    })
    assert [fx.value(r, ".per_customer") for r in resp.data] == [pytest.approx(168 / 4)]


async def test_unrelated_model_ratio(child_engine: SlayerQueryEngine) -> None:
    resp = await _run(child_engine, {
        "source_model": "customers", "measures": [{"formula": fx.UNRELATED_RATIO, "name": "per_customer"}],
    })
    assert [fx.value(r, ".per_customer") for r in resp.data] == [pytest.approx(6 / 4)]
    assert "unrelated_dataset" in _kinds(resp)


async def test_outer_stage_reads_flattened_inner_dimension(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, [
        {
            "name": "per_customer", "source_model": "orders",
            "dimensions": _REGION_CUSTOMER,
            "measures": [{"formula": "sum(amount)", "name": "total"}],
        },
        {
            "source_model": "per_customer", "dimensions": [fx.STAGE_COLUMN],
            "measures": ["count(*)", {"formula": "sum(total)", "name": "region_total"}],
        },
    ])
    rows = fx.rows_by(resp, key=fx.STAGE_COLUMN)
    assert {k: (fx.value(r, "._count"), fx.value(r, ".region_total")) for k, r in rows.items()} == {
        "North": (2, 300), "South": (1, 190),
    }


async def test_filter_on_a_transform_value_applies_after_it(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "time_dimensions": _MONTHLY,
        "measures": [{"formula": fx.CUMSUM, "name": "run"}], "filters": [f"{fx.CUMSUM} > 300"],
    })
    assert {k: fx.value(r, ".run") for k, r in fx.rows_by(resp, key="order_date").items()} == {
        "2025-03": 325, "2025-04": 385, "2025-06": 490,
    }


@pytest.mark.parametrize(("formula", "filtered", "unfiltered"), [
    (fx.CUMSUM, (70, 100), (295, 325)),
    ("lag(sum(amount), 1)", (None, 70), (150, 70)),
    ("lead(sum(amount), 1)", (30, None), (30, 60)),
    ("consecutive_periods(sum(amount) > 0)", (1, 2), (5, 6)),
    ("rank(sum(amount), direction='desc')", (1, 2), (3, 6)),
    ("ntile(sum(amount), n=2)", (2, 1), (2, 1)),
    ("sum(amount, partition_by=[])", (100, 100), (490, 490)),
    (fx.CHANGE, (-80, -40), (-80, -40)),
    (fx.CHANGE_PCT, pytest.approx((-80 / 150, -40 / 70)), pytest.approx((-80 / 150, -40 / 70))),
    (fx.TIME_SHIFT, (150, 70), (150, 70)),
    ("time_shift(sum(amount), 1)", (30, 60), (30, 60)),
    ("sum(amount, window='3m')", (265, 250), (265, 250)),
])
async def test_time_filter_restricts_cross_row_values_except_calendar_look_backs(
    engine: SlayerQueryEngine, formula: str, filtered: tuple[Any, Any], unfiltered: tuple[Any, Any],
) -> None:
    async def feb_mar(filters: list[str]) -> tuple[Any, Any]:
        resp = await _run(engine, {
            "source_model": "orders", "time_dimensions": _MONTHLY,
            "measures": [{"formula": formula, "name": "v"}], "filters": filters,
        })
        rows = fx.rows_by(resp, key="order_date")
        return fx.value(rows["2025-02"], ".v"), fx.value(rows["2025-03"], ".v")

    assert await feb_mar(["order_date >= '2025-02-01'", "order_date < '2025-04-01'"]) == filtered
    assert await feb_mar([]) == unfiltered


@pytest.mark.parametrize(("formula", "filtered", "unfiltered"), [
    ("rank(sum(amount), direction='desc')", 1, 2),
    ("dense_rank(sum(amount), direction='desc')", 1, 2),
    ("ntile(sum(amount), n=3)", 1, 2),
    ("sum(amount, partition_by=[])", 170, 490),
    (fx.SHARE_OF_GROUP, pytest.approx(1.0), pytest.approx(170 / 300)),
    ("avg(sum(amount, partition_by=[customers.name]), partition_by=[])", pytest.approx(170), pytest.approx(490 / 3)),
])
async def test_dimension_filter_restricts_peer_values(
    engine: SlayerQueryEngine, formula: str, filtered: Any, unfiltered: Any,
) -> None:
    async def alice(filters: list[str]) -> Any:
        resp = await _run(engine, {
            "source_model": "orders", "dimensions": _REGION_CUSTOMER,
            "measures": [{"formula": formula, "name": "v"}], "filters": filters,
        })
        return fx.value(fx.rows_by(resp, key="customers.name")["Alice"], ".v")

    assert await alice(["customers.name == 'Alice'"]) == filtered
    assert await alice([]) == unfiltered


async def test_non_time_filter_restricts_window_rows_before_the_range(engine: SlayerQueryEngine) -> None:
    async def jan(filters: list[str]) -> Any:
        resp = await _run(engine, {
            "source_model": "orders", "time_dimensions": _YEAR_2025,
            "measures": [{"formula": "sum(amount, window='3m')", "name": "w"}], "filters": filters,
        })
        return fx.value(fx.rows_by(resp, key="order_date")["2025-01"], ".w")

    assert await jan(["status == 'paid'"]) == 195
    assert await jan([]) == 215


async def test_rank_among_all_rows_needs_a_stage(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, [
        {
            "name": "ranked", "source_model": "orders", "dimensions": ["customers.name"],
            "measures": [{"formula": "rank(sum(amount), direction='desc')", "name": "rnk"}],
        },
        {
            "source_model": "ranked", "dimensions": ["customers__name", "rnk"],
            "filters": ["customers__name == 'Alice'"],
        },
    ])
    assert [(fx.value(r, "customers__name"), fx.value(r, ".rnk")) for r in resp.data] == [("Alice", 2)]


async def test_year_over_year_time_shift(last_year_engine: SlayerQueryEngine) -> None:
    resp = await _run(last_year_engine, {
        "source_model": "orders", "time_dimensions": _YEAR_2025,
        "measures": [{"formula": "time_shift(sum(amount), -1, 'year')", "name": "yoy"}],
    })
    assert {k: fx.value(r, ".yoy") for k, r in fx.rows_by(resp, key="order_date").items()} == {
        "2025-01": 8, "2025-02": None, "2025-03": None, "2025-04": None, "2025-06": 12,
    }


async def test_ntile_puts_the_lowest_values_first(child_engine: SlayerQueryEngine) -> None:
    resp = await _run(child_engine, {
        "source_model": "customers", "dimensions": ["name"],
        "measures": [{"formula": "ntile(sum(orders.amount), n=4)", "name": "quartile"}],
    })
    assert {fx.value(r, ".name"): fx.value(r, ".quartile") for r in resp.data} == {
        "Alice": 1, "Bob": 2, "Carol": 3, "Dan": 4,
    }


async def test_filter_clause_restricts_the_aggregated_rows(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders",
        "measures": [{"formula": fx.AGG_FILTER, "name": "paid"}, {"formula": "count(*)", "name": "all"}],
    })
    assert [(fx.value(r, ".paid"), fx.value(r, ".all")) for r in resp.data] == [(7, 11)]


async def test_granularity_calls_bucket_like_a_time_dimension(engine: SlayerQueryEngine) -> None:
    async def monthly(body: dict[str, Any]) -> dict[str, Any]:
        resp = await _run(engine, {"source_model": "orders", "measures": ["count(*)"], **body})
        col = next(c for c in resp.columns if "order_date" in c)
        return {str(r[col])[:7]: fx.value(r, "._count") for r in resp.data}

    expected = {
        "2024-10": 1, "2024-11": 1, "2024-12": 1,
        "2025-01": 2, "2025-02": 1, "2025-03": 1, "2025-04": 1, "2025-06": 3,
    }
    assert await monthly({"dimensions": ["month(order_date)"]}) == expected
    assert await monthly({"time_dimensions": ["month(order_date)"]}) == expected
    assert await monthly({"time_dimensions": _MONTHLY}) == expected


async def test_computed_dimension_over_a_fixed_grain_aggregate(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, {
        "source_model": "orders", "dimensions": [{"expression": fx.SPEND_BAND, "name": "spend_band"}],
        "measures": ["count(*)"],
    })
    assert {k: fx.value(r, "._count") for k, r in fx.rows_by(resp, key=".spend_band").items()} == {
        "high": 8, "low": 3,
    }


@pytest.mark.parametrize(("condition", "count"), [
    ("order_date >= '2025-Q1'", 8),
    ("order_date <= '2025-Q1'", 7),
    ("order_date in '2025-Q1'", 4),
])
async def test_time_points_are_half_open_periods(engine: SlayerQueryEngine, condition: str, count: int) -> None:
    resp = await _run(engine, {"source_model": "orders", "measures": ["count(*)"], "filters": [condition]})
    assert [fx.value(r, "._count") for r in resp.data] == [count]


async def _codes(engine: SlayerQueryEngine, filters: list[str], variables: dict[str, Any] | None = None) -> set[str]:
    resp = await _run(engine, {
        "source_model": "items", "dimensions": ["code"], "filters": filters,
        **({"variables": variables} if variables is not None else {}),
    })
    return {fx.value(r, ".code") for r in resp.data}


async def test_regex_braces_are_pattern_text(items_engine: SlayerQueryEngine) -> None:
    assert await _codes(items_engine, ["code ~ '^A[0-9]{3}'"]) == {"A123", "A1234"}


@pytest.mark.parametrize(("condition", "codes"), [
    ("flag IS NOT TRUE", {"A12", "B123", "XA123"}),
    ("status IS DISTINCT FROM 'paid'", {"A12", "A1234", "XA123"}),
])
async def test_null_aware_boolean_and_distinctness_tests(
    items_engine: SlayerQueryEngine, condition: str, codes: set[str],
) -> None:
    assert await _codes(items_engine, [condition]) == codes


@pytest.mark.parametrize(("condition", "codes"), [
    ("contains(tags, 'x')", {"A123", "A12"}),
    ("tags.value = 'x'", {"A123", "A12"}),
    ("contains_any(tags, ['x', 'y'])", {"A123", "A12", "B123"}),
    ("contains_all(tags, ['x', 'y'])", {"A123"}),
])
async def test_list_column_filters(items_engine: SlayerQueryEngine, condition: str, codes: set[str]) -> None:
    assert await _codes(items_engine, [condition]) == codes


async def test_quoted_variable_binds_one_string_literal(items_engine: SlayerQueryEngine) -> None:
    assert await _codes(items_engine, ["status = '{s}'"], {"s": "paid"}) == {"A123", "B123"}


async def test_list_variable_fills_a_whole_in_list(items_engine: SlayerQueryEngine) -> None:
    assert await _codes(items_engine, [fx.IN_LIST_VARIABLE], {"statuses": ["paid", "shipped"]}) == {
        "A123", "B123", "XA123",
    }


async def test_empty_list_variable_in_an_in_list_fails_naming_it(items_engine: SlayerQueryEngine) -> None:
    with pytest.raises(Exception, match="statuses") as info:
        await _codes(items_engine, [fx.IN_LIST_VARIABLE], {"statuses": []})
    assert "empty" in str(info.value).lower()


async def test_empty_list_variable_binds_as_a_list_function_argument(items_engine: SlayerQueryEngine) -> None:
    assert await _codes(items_engine, ["contains_all(tags, {wanted})"], {"wanted": []}) == {
        "A123", "A12", "B123", "A1234",
    }
