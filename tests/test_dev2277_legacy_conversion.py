"""Pre-2.0 root-relative spellings convert to their qualified twin when a population is named.

Spec: queries/references › Pre-2.0 root-relative spellings convert when a population is named;
compatibility/removal-tripwire › A root-relative spelling used twice in one query, A marked notice emits no FutureWarning.
"""

from __future__ import annotations

import json
import warnings
from typing import Any

import pytest

from slayer.core.errors import SlayerError, UnknownReferenceError
from slayer.core.query import SlayerQuery
from slayer.engine.legacy_references import Conversions, anchor_names, convert_query
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1954_fixtures import execution_spy
from tests._dev2137_fixtures import list_engine
from tests._dev2187_fixtures import orders_engine
from tests._dev2277_fixtures import (
    BACKENDS,
    DS,
    conversions,
    cycle_engine,
    execute_recording,
    future_warnings,
    notices,
    response_conversions,
    seed_engine,
)
from tests._stored_upgrade_fixtures import raw_ossie, source_queries


def m(expression: str, alias: str) -> dict[str, str]:
    return {"metric": expression, "alias": alias}


def _orders(**body: Any) -> dict[str, Any]:
    return {"population": "orders", **body}


def _rows(resp: SlayerResponse) -> list[tuple]:
    return sorted(tuple(str(r[c]) for c in resp.columns) for r in resp.data)


async def assert_converts(engine: SlayerQueryEngine, legacy: Any, qualified: Any,
                          expected: set[tuple[str, str]]) -> SlayerResponse:
    """``legacy`` runs as ``qualified`` and reports exactly ``expected`` once each, on both channels."""
    twin, twin_caught = await execute_recording(engine, qualified)
    assert conversions(twin_caught) == []
    assert response_conversions(twin) == []
    resp, caught = await execute_recording(engine, legacy)
    assert sorted(conversions(caught)) == sorted(expected)
    assert sorted(response_conversions(resp)) == sorted(expected)
    assert resp.sql == twin.sql
    assert resp.columns == twin.columns
    assert _rows(resp) == _rows(twin)
    return resp


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with orders_engine(request.param) as eng:
        yield eng


@pytest.fixture(params=BACKENDS)
async def seed(request):
    async with seed_engine(request.param) as eng:
        yield eng


# --------------------------------------------------------------------------- #
# Every form in every position
# --------------------------------------------------------------------------- #
STATUS = ("status", "orders.status")
AMOUNT = ("amount", "orders.amount")
CREATED = ("created_at", "orders.created_at")
COUNT = ("count(*)", "count(orders.*)")
MONTH_Q = "date_trunc('month', orders.created_at)"

#: position → (legacy body, qualified body, expected conversions); population ``orders``.
POSITIONS: dict[str, tuple[dict, dict, set[tuple[str, str]]]] = {
    "dimension": ({"dimensions": ["status"]}, {"dimensions": ["orders.status"]}, {STATUS}),
    "column-time-dimension": (
        {"time_dimensions": [{"field": "created_at", "granularity": "month"}], "metrics": ["count(orders.*)"]},
        {"time_dimensions": [{"field": "orders.created_at", "granularity": "month"}], "metrics": ["count(orders.*)"]},
        {CREATED},
    ),
    "expression-time-dimension": (
        {"time_dimensions": ["date_trunc('month', created_at)"], "metrics": ["count(orders.*)"]},
        {"time_dimensions": [MONTH_Q], "metrics": ["count(orders.*)"]},
        {CREATED},
    ),
    "aggregation-argument": ({"metrics": ["sum(amount)"]}, {"metrics": ["sum(orders.amount)"]}, {AMOUNT}),
    "row-count": ({"metrics": ["count(*)"]}, {"metrics": ["count(orders.*)"]}, {COUNT}),
    "dataset-metric": ({"metrics": ["revenue"]}, {"metrics": ["orders.revenue"]}, {("revenue", "orders.revenue")}),
    "arithmetic-metric": (
        {"metrics": [m("sum(amount) / count(orders.*)", "avg_amount")]},
        {"metrics": [m("sum(orders.amount) / count(orders.*)", "avg_amount")]},
        {AMOUNT},
    ),
    "aggregate-filter": (
        {"metrics": [m("sum(orders.amount) FILTER (WHERE status = 'paid')", "p")]},
        {"metrics": [m("sum(orders.amount) FILTER (WHERE orders.status = 'paid')", "p")]},
        {STATUS},
    ),
    "partition-by": (
        {"dimensions": ["orders.status", "orders.customer_id"],
         "metrics": [m("sum(orders.amount, partition_by=[status])", "p")]},
        {"dimensions": ["orders.status", "orders.customer_id"],
         "metrics": [m("sum(orders.amount, partition_by=[orders.status])", "p")]},
        {STATUS},
    ),
    "transform": (
        {"time_dimensions": [MONTH_Q], "metrics": [m("cumsum(sum(amount))", "c")]},
        {"time_dimensions": [MONTH_Q], "metrics": [m("cumsum(sum(orders.amount))", "c")]},
        {AMOUNT},
    ),
    "filter": (
        {"metrics": ["count(orders.*)"], "filter": ["amount > 15"]},
        {"metrics": ["count(orders.*)"], "filter": ["orders.amount > 15"]},
        {AMOUNT},
    ),
    "order": (
        {"dimensions": ["orders.status"], "metrics": ["count(orders.*)"], "order": [{"field": "status", "direction": "desc"}]},
        {"dimensions": ["orders.status"], "metrics": ["count(orders.*)"],
         "order": [{"field": "orders.status", "direction": "desc"}]},
        {STATUS},
    ),
    "main-time-dimension": (
        {"time_dimensions": [MONTH_Q], "metrics": [m("change(sum(orders.amount))", "c")],
         "main_time_dimension": "created_at"},
        {"time_dimensions": [MONTH_Q], "metrics": [m("change(sum(orders.amount))", "c")],
         "main_time_dimension": "orders.created_at"},
        {CREATED},
    ),
    "computed-dimension": (
        {"dimensions": [{"field": "amount * 2", "alias": "dbl"}], "metrics": ["count(orders.*)"]},
        {"dimensions": [{"field": "orders.amount * 2", "alias": "dbl"}], "metrics": ["count(orders.*)"]},
        {AMOUNT},
    ),
}


@pytest.mark.parametrize("position", sorted(POSITIONS))
async def test_a_legacy_spelling_converts_in_every_position(engine: SlayerQueryEngine, position: str) -> None:
    legacy, qualified, expected = POSITIONS[position]
    await assert_converts(engine, _orders(**legacy), _orders(**qualified), expected)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("legacy", "qualified"), [
    ({"dimensions": ["buyer.name"], "metrics": ["count(orders.*)"]},
     {"dimensions": ["orders.buyer.name"], "metrics": ["count(orders.*)"]}),
    ({"metrics": [m("avg(buyer.score)", "s")]}, {"metrics": [m("avg(orders.buyer.score)", "s")]}),
    ({"metrics": ["count(orders.*)"], "filter": ["buyer.name = 'ann'"]},
     {"metrics": ["count(orders.*)"], "filter": ["orders.buyer.name = 'ann'"]}),
], ids=["dimension", "aggregation-argument", "filter"])
async def test_an_edge_first_path_converts(backend: str, legacy: dict, qualified: dict) -> None:
    async with orders_engine(backend, edge_name="buyer") as eng:
        name = "buyer.score" if "avg" in json.dumps(legacy) else "buyer.name"
        await assert_converts(eng, _orders(**legacy), _orders(**qualified), {(name, f"orders.{name}")})


@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_edge_declared_on_the_other_side_converts(backend: str) -> None:
    async with orders_engine(backend, edge_name="buyer") as eng:
        await assert_converts(
            eng, {"population": "customers", "metrics": [m("count(buyer.id)", "n")]},
            {"population": "customers", "metrics": [m("count(customers.buyer.id)", "n")]},
            {("buyer.id", "customers.buyer.id")},
        )


async def test_a_list_column_first_path_converts() -> None:
    async with list_engine() as eng:
        await assert_converts(
            eng,
            {"population": "contractors", "dimensions": ["skills.value"], "metrics": ["count(contractors.*)"]},
            {"population": "contractors", "dimensions": ["contractors.skills.value"],
             "metrics": ["count(contractors.*)"]},
            {("skills.value", "contractors.skills.value")},
        )


# --------------------------------------------------------------------------- #
# Scenarios over the Appendix B seed
# --------------------------------------------------------------------------- #
async def test_legacy_bare_names_convert(seed: SlayerQueryEngine) -> None:
    resp = await assert_converts(
        seed,
        _orders(dimensions=["status"], metrics=["count(*)", "sum(amount)"]),
        _orders(dimensions=["orders.status"], metrics=["count(orders.*)", "sum(orders.amount)"]),
        {STATUS, COUNT, AMOUNT},
    )
    assert resp.columns == ["orders.status", "orders._count", "orders.amount_sum"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_edge_first_path_converts_on_the_seed(backend: str) -> None:
    async with seed_engine(backend, buyer=True) as eng:
        resp = await assert_converts(
            eng, _orders(dimensions=["buyer.region"]), _orders(dimensions=["orders.buyer.region"]),
            {("buyer.region", "orders.buyer.region")},
        )
    assert resp.columns == ["orders.buyer.region"]


async def test_a_dataset_first_path_never_converts(seed: SlayerQueryEngine) -> None:
    resp, caught = await execute_recording(seed, _orders(dimensions=["customers.region"]))
    assert future_warnings(caught) == []
    assert response_conversions(resp) == []
    assert resp.columns == ["customers.region"]


async def test_a_population_spelled_in_another_case_reads_its_datasets_names(seed: SlayerQueryEngine) -> None:
    # ``nosuch`` is neither a dataset nor a field or edge of ``orders``: nothing to convert.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(UnknownReferenceError):
            await seed.execute({"population": "ORDERS", "dimensions": ["nosuch.region"]})
    assert conversions(caught) == []


async def test_each_stage_converts_against_its_own_population(engine: SlayerQueryEngine) -> None:
    await assert_converts(
        engine,
        [{"name": "s1", "population": "orders", "dimensions": ["status"], "metrics": [m("sum(amount)", "tot")]},
         {"population": "s1", "dimensions": ["status"], "metrics": [m("count(*)", "n")]}],
        [{"name": "s1", "population": "orders", "dimensions": ["orders.status"],
          "metrics": [m("sum(orders.amount)", "tot")]},
         {"population": "s1", "dimensions": ["s1.status"], "metrics": [m("count(s1.*)", "n")]}],
        {STATUS, AMOUNT, ("status", "s1.status"), ("count(*)", "count(s1.*)")},
    )


async def test_a_form_used_twice_warns_once(engine: SlayerQueryEngine) -> None:
    await assert_converts(
        engine,
        _orders(dimensions=["status"], metrics=["count(*)"], filter=["status = 'paid'"],
                order=[{"field": "status"}]),
        _orders(dimensions=["orders.status"], metrics=["count(orders.*)"], filter=["orders.status = 'paid'"],
                order=[{"field": "orders.status"}]),
        {STATUS, COUNT},
    )


async def test_an_entry_aliased_as_its_own_field_converts(engine: SlayerQueryEngine) -> None:
    """A 1.x alias naming a field by its own name renamed nothing: the twin drops it."""
    await assert_converts(
        engine,
        _orders(dimensions=[{"field": "status", "alias": "status"}], metrics=[m("sum(amount)", "total")],
                filter=["status <> 'x'"]),
        _orders(dimensions=["orders.status"], metrics=[m("sum(orders.amount)", "total")],
                filter=["orders.status <> 'x'"]),
        {STATUS, AMOUNT},
    )


async def test_a_comparison_argument_of_an_aggregation_converts(engine: SlayerQueryEngine) -> None:
    await assert_converts(
        engine,
        _orders(metrics=[m("sum(status = 'paid')", "paid"), m("sum(amount, window='30d')", "w")],
                time_dimensions=[{"field": "orders.created_at", "granularity": "day"}]),
        _orders(metrics=[m("sum(orders.status = 'paid')", "paid"), m("sum(orders.amount, window='30d')", "w")],
                time_dimensions=[{"field": "orders.created_at", "granularity": "day"}]),
        {STATUS, AMOUNT},
    )


# --------------------------------------------------------------------------- #
# A bare name an alias shares reads what it read before 2.0
# --------------------------------------------------------------------------- #
def _column(resp: SlayerResponse, suffix: str) -> list[float]:
    key = next(k for k in resp.columns if k.endswith(suffix))
    return [float(r[key]) for r in resp.data]


async def test_a_metric_reads_the_population_metric_a_sibling_alias_shares(engine: SlayerQueryEngine) -> None:
    resp = await assert_converts(
        engine,
        _orders(metrics=[m("count(*)", "revenue"), m("revenue * 10", "r10")]),
        _orders(metrics=[m("count(orders.*)", "revenue"), m("orders.revenue * 10", "r10")]),
        {COUNT, ("revenue", "orders.revenue")},
    )
    assert _column(resp, "r10") == [1000.0]


async def test_a_filter_reads_a_population_field_before_an_alias_of_the_same_name(engine: SlayerQueryEngine) -> None:
    resp = await assert_converts(
        engine,
        _orders(metrics=[m("sum(amount)", "amount")], filter=["amount > 15"]),
        _orders(metrics=[m("sum(orders.amount)", "amount")], filter=["orders.amount > 15"]),
        {AMOUNT},
    )
    assert _column(resp, "amount") == [90.0]


async def test_a_filter_reads_an_alias_the_population_does_not_declare(engine: SlayerQueryEngine) -> None:
    resp = await assert_converts(
        engine,
        _orders(dimensions=["status"], metrics=[m("count(*)", "n")], filter=["n > 1"]),
        _orders(dimensions=["orders.status"], metrics=[m("count(orders.*)", "n")], filter=["n > 1"]),
        {STATUS, COUNT},
    )
    assert [r["orders.status"] for r in resp.data] == ["paid"]


async def test_an_order_reads_an_alias_before_a_population_metric(engine: SlayerQueryEngine) -> None:
    resp = await assert_converts(
        engine,
        _orders(dimensions=["status"], metrics=[m("count(*)", "revenue")],
                order=[{"field": "revenue", "direction": "desc"}]),
        _orders(dimensions=["orders.status"], metrics=[m("count(orders.*)", "revenue")],
                order=[{"field": "revenue", "direction": "desc"}]),
        {STATUS, COUNT},
    )
    assert _column(resp, "revenue") == [3.0, 1.0]


async def test_the_main_time_dimension_reads_a_population_field_before_an_alias(engine: SlayerQueryEngine) -> None:
    def body(field: str, main: str) -> dict[str, Any]:
        return _orders(time_dimensions=[{"field": field, "granularity": "month", "alias": "created_at"}],
                       metrics=[m("change(sum(orders.amount))", "c")], main_time_dimension=main)

    await assert_converts(engine, body("created_at", "created_at"),
                          body("orders.created_at", "orders.created_at"), {CREATED})


async def test_a_quoted_alias_matches_only_its_exact_spelling(engine: SlayerQueryEngine) -> None:
    resp = await assert_converts(
        engine,
        _orders(dimensions=["status"], metrics=[m("count(*)", "Amount")], filter=['"Amount" > 1']),
        _orders(dimensions=["orders.status"], metrics=[m("count(orders.*)", "Amount")], filter=['"Amount" > 1']),
        {STATUS, COUNT},
    )
    assert [r["orders.status"] for r in resp.data] == ["paid"]


async def test_a_stage_population_offers_its_output_columns_as_fields(engine: SlayerQueryEngine) -> None:
    s1 = {"name": "s1", "population": "orders", "dimensions": ["orders.id"],
          "metrics": [m("sum(orders.amount)", "rev")]}
    resp = await assert_converts(
        engine,
        [s1, {"population": "s1", "metrics": [m("sum(rev)", "rev")], "filter": ["rev > 25"]}],
        [s1, {"population": "s1", "metrics": [m("sum(s1.rev)", "rev")], "filter": ["s1.rev > 25"]}],
        {("rev", "s1.rev")},
    )
    assert _column(resp, "rev") == [70.0]


async def test_a_stage_population_spelled_in_another_case_offers_its_output_columns(
        engine: SlayerQueryEngine) -> None:
    s1 = {"name": "s1", "population": "orders", "dimensions": ["orders.id"],
          "metrics": [m("sum(orders.amount)", "rev")]}
    resp = await assert_converts(
        engine,
        [s1, {"population": "S1", "metrics": [m("sum(rev)", "rev")], "filter": ["rev > 25"]}],
        [s1, {"population": "S1", "metrics": [m("sum(S1.rev)", "rev")], "filter": ["S1.rev > 25"]}],
        {("rev", "S1.rev")},
    )
    assert _column(resp, "rev") == [70.0]


async def test_an_extension_metric_is_read_only_by_the_stage_declaring_it(engine: SlayerQueryEngine) -> None:
    s1 = SlayerQuery.model_validate({
        "name": "s1", "population": "orders", "dimensions": ["orders.status"], "metrics": [m("bonus", "b")],
        "extensions": [{"dataset": "orders", "metrics": [{"name": "bonus", "expression": "sum(orders.amount)"}]}]})
    s2 = SlayerQuery.model_validate(_orders(metrics=[m("count(orders.*)", "bonus"), m("bonus * 10", "b10")]))
    anchors = await anchor_names(engine.storage, [s1, s2], data_source=None)

    def metrics(q: SlayerQuery) -> list[str]:
        return [x.metric for x in convert_query(q, anchors=anchors, conversions=Conversions(report=False)).metrics or []]

    assert metrics(s1) == ["orders.bonus"]
    assert metrics(s2) == ["count(orders.*)", "bonus * 10"]


async def test_a_query_backed_population_offers_its_output_columns_as_fields(engine: SlayerQueryEngine) -> None:
    resp = await assert_converts(
        engine,
        {"population": "qb", "metrics": [m("sum(tot)", "tot")], "filter": ["tot > 50"]},
        {"population": "qb", "metrics": [m("sum(qb.tot)", "tot")], "filter": ["qb.tot > 50"]},
        {("tot", "qb.tot")},
    )
    assert _column(resp, "tot") == [80.0]


async def test_a_root_relative_spelling_used_twice_in_one_query(seed: SlayerQueryEngine) -> None:
    _resp, caught = await execute_recording(seed, _orders(metrics=["count(*)"], filter=["count(*) > 1"]))
    texts = future_warnings(caught)
    assert len(texts) == 1
    assert all(token in texts[0] for token in ("count(*)", "count(orders.*)", "SLayer 3.0"))


# --------------------------------------------------------------------------- #
# A root-less query converts nothing
# --------------------------------------------------------------------------- #
async def _refused_without_deprecation(engine: SlayerQueryEngine, body: dict[str, Any]) -> str:
    """``body`` fails with a ``SlayerError`` and emits no deprecation; the error text."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(SlayerError) as info:
            await engine.execute(body)
    assert future_warnings(list(caught)) == []
    assert conversions(list(caught)) == []
    return str(info.value)


@pytest.mark.parametrize(("body", "needle"), [
    ({"dimensions": ["customers.region"], "metrics": ["count(customer_id)"]}, "customers.customer_id"),
    ({"dimensions": ["customers.region"], "metrics": ["total_revenue"]}, "orders.total_revenue"),
    ({"dimensions": ["customers.region"], "metrics": ["count(*)"]}, "count(customers.*)"),
    ({"metrics": ["count(*)"]}, "count(*)"),
], ids=["bare-field", "bare-dataset-metric", "row-count", "row-count-no-dimension"])
async def test_a_rootless_query_converts_nothing(seed: SlayerQueryEngine, body: dict, needle: str) -> None:
    assert needle in await _refused_without_deprecation(seed, body)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_rootless_edge_first_path_converts_nothing(backend: str) -> None:
    async with seed_engine(backend, buyer=True) as eng:
        text = await _refused_without_deprecation(eng, {"dimensions": ["buyer.region"]})
    assert "orders.buyer.region" in text


async def test_a_rootless_list_column_first_path_converts_nothing() -> None:
    async with list_engine() as eng:
        text = await _refused_without_deprecation(eng, {"dimensions": ["skills.value"]})
    assert "skills" in text


# --------------------------------------------------------------------------- #
# Saving, refinement and the cache see the qualified twin
# --------------------------------------------------------------------------- #
async def _raw_text(engine: SlayerQueryEngine, name: str) -> str:
    raw = await raw_ossie(engine.storage, name, data_source=DS)
    return json.dumps([raw, source_queries(raw["dataset"])], sort_keys=True)


LEGACY_SAVED = _orders(dimensions=["status"], metrics=["sum(amount)", "count(*)", "total_revenue"])
QUALIFIED_SAVED = _orders(dimensions=["orders.status"],
                          metrics=["sum(orders.amount)", "count(orders.*)", "orders.total_revenue"])


async def test_a_saved_query_reading_another_dataset_converts_nothing(seed: SlayerQueryEngine) -> None:
    """A query-backed dataset has no datasource of its own; its queries still anchor at the visible datasets."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await seed.create_dataset_from_query(
            query=_orders(dimensions=["customers.region"], metrics=["sum(orders.amount)"]), name="joined")
    assert conversions(list(caught)) == []
    assert '"customers.region"' in await _raw_text(seed, "joined")


async def test_a_saved_query_stores_the_qualified_twin(seed: SlayerQueryEngine) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await seed.create_dataset_from_query(query=LEGACY_SAVED, name="legacy")
    assert sorted(conversions(list(caught))) == sorted(
        {STATUS, AMOUNT, COUNT, ("total_revenue", "orders.total_revenue")})
    text = await _raw_text(seed, "legacy")
    assert '"sum(orders.amount)"' in text
    assert '"count(orders.*)"' in text
    assert '"sum(amount)"' not in text
    assert '"count(*)"' not in text


async def test_a_converted_save_equals_its_twins_save(seed: SlayerQueryEngine) -> None:
    await seed.create_dataset_from_query(query=LEGACY_SAVED, name="same")
    legacy = await _raw_text(seed, "same")
    await seed.create_dataset_from_query(query=QUALIFIED_SAVED, name="same")
    assert await _raw_text(seed, "same") == legacy


@pytest.mark.parametrize(("refinement", "expected"), [
    ({"metrics": ["sum(amount)"]}, {AMOUNT}),
    ({"dimensions": ["status"]}, {STATUS}),
], ids=["metric", "dimension"])
async def test_a_refinement_matches_a_converted_entry(seed: SlayerQueryEngine, refinement: dict,
                                                      expected: set[tuple[str, str]]) -> None:
    await seed.create_dataset_from_query(
        query=_orders(dimensions=["orders.status"], metrics=["sum(orders.amount)"]), name="by_status")
    plain = await seed.execute("by_status")
    refined, caught = await execute_recording(seed, "by_status", refine=refinement)
    assert refined.sql == plain.sql
    assert refined.columns == plain.columns
    assert sorted(conversions(caught)) == sorted(expected)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_refinement_converts_an_edge_declared_on_the_other_side(backend: str) -> None:
    async with orders_engine(backend, edge_name="buyer") as eng:
        await eng.create_dataset_from_query(
            query={"population": "customers", "metrics": [m("count(customers.buyer.id)", "n")]}, name="buyers")
        plain = await eng.execute("buyers")
        refined, caught = await execute_recording(eng, "buyers", refine={"metrics": [m("count(buyer.id)", "n")]})
    assert refined.sql == plain.sql
    assert conversions(caught) == [("buyer.id", "customers.buyer.id")]


async def test_a_converted_query_shares_its_twins_cached_result(seed: SlayerQueryEngine,
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    first = await seed.execute(_orders(dimensions=["orders.status"], metrics=["sum(orders.amount)"]), cache=True)
    calls = execution_spy(monkeypatch)
    second, caught = await execute_recording(seed, _orders(dimensions=["status"], metrics=["sum(amount)"]), cache=True)
    assert calls == []
    assert seed.cache_size == 1
    assert (second.columns, second.data) == (first.columns, first.data)
    assert sorted(conversions(caught)) == sorted({STATUS, AMOUNT})
    assert sorted(response_conversions(second)) == sorted({STATUS, AMOUNT})


# --------------------------------------------------------------------------- #
# The changed-reference notice is no FutureWarning
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_changed_reference_notice_emits_no_future_warning(backend: str) -> None:
    async with cycle_engine(backend) as eng:
        resp, caught = await execute_recording(eng, {"population": "x", "metrics": ["count(x.*)"],
                                                     "filter": ["z.amount > 0"]})
    assert future_warnings(caught) == []
    assert len(notices(resp, "z.amount", "x.z.amount")) == 1


async def test_an_unchanged_reference_carries_no_notice(seed: SlayerQueryEngine) -> None:
    resp = await seed.execute(_orders(dimensions=["customers.region"]))
    assert notices(resp) == []
