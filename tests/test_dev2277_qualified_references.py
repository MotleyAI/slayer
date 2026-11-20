"""Qualified query references: anchoring, routing, bare names, keys, legacy conversion (spec: queries/references)."""

from __future__ import annotations

import warnings
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import NameCollisionError, SlayerError, UnresolvableDimensionRelationshipError
from slayer.core.models import DatasetField, DatasetMetric
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev2277_fixtures import (
    BACKENDS,
    add_other_datasource,
    by,
    conversions,
    customers_dataset,
    cycle_engine,
    execute_recording,
    future_warnings,
    hidden,
    notices,
    response_conversions,
    returns_dataset,
    scoped,
    seed_datasets,
    seed_engine,
    table,
)


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with seed_engine(request.param) as eng:
        yield eng


@pytest.fixture(params=BACKENDS)
def backend(request) -> str:
    return request.param


async def _raises(engine: SlayerQueryEngine, query: Any, exc: type[BaseException] = SlayerError) -> BaseException:
    with pytest.raises(exc) as info:
        await engine.execute(query)
    return info.value


def _key_ending(resp: SlayerResponse, leaf: str) -> str:
    hits = [c for c in resp.columns if c == leaf or c.endswith(f".{leaf}")]
    assert len(hits) == 1, (leaf, resp.columns)
    return hits[0]


# --------------------------------------------------------------------------- #
# A query reference is anchored at its first segment
# --------------------------------------------------------------------------- #
async def test_an_ossie_query_runs_with_ossie_meaning(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]})
    assert resp.columns == ["customers.region", "orders.total_revenue"]
    revenue = by(resp, "customers.region", "orders.total_revenue")
    assert (revenue["EAST"], revenue["WEST"]) == (350.0, 300.0)


async def test_the_population_name_anchors_its_own_fields(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"]})
    assert "JOIN" not in (resp.sql or "").upper()
    assert by(resp, "orders.status", "orders._count") == {"completed": 2.0, "open": 2.0}


async def test_a_stage_precedes_a_same_named_dataset(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute([
        {"name": "customers", "population": "orders", "dimensions": ["orders.status"]},
        {"population": "customers", "dimensions": ["customers.status"]},
    ])
    assert table(resp, "customers.status") == {("completed",), ("open",)}


async def test_an_edge_name_is_not_an_anchor(backend: str) -> None:
    async with seed_engine(backend, buyer=True) as eng:
        err = await _raises(eng, {"dimensions": ["buyer.region"]})
    assert "buyer" in str(err)
    assert "orders.buyer.region" in str(err)


async def test_a_dataset_of_another_datasource_is_not_an_anchor(engine: SlayerQueryEngine) -> None:
    await add_other_datasource(engine)
    other = await _raises(engine, {"population": "customers", "metrics": ["count(events.*)"]})
    unknown = await _raises(engine, {"population": "customers", "metrics": ["count(evnts.*)"]})
    assert type(other) is type(unknown)
    assert str(other) == str(unknown).replace("evnts", "events")


async def test_a_hidden_dataset_is_reported_as_unknown(backend: str) -> None:
    datasets = seed_datasets(returns=hidden(returns_dataset()))
    async with seed_engine(backend, datasets=datasets) as eng, scoped(eng) as caller:
        hidden_err = await _raises(caller, {"dimensions": ["returns.amount"]})
        unknown_err = await _raises(caller, {"dimensions": ["retrns.amount"]})
    assert type(hidden_err) is type(unknown_err)
    assert str(hidden_err) == str(unknown_err).replace("retrns", "returns")


async def test_first_segments_follow_the_identifier_case_rule(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"dimensions": ["Customers.region"]})
    assert {row[resp.columns[0]] for row in resp.data} == {"EAST", "WEST", "NORTH"}
    quoted = await _raises(engine, {"dimensions": ['"Customers".region']})
    unknown = await _raises(engine, {"dimensions": ['"Nosuch".region']})
    assert type(quoted) is type(unknown)
    assert str(quoted) == str(unknown).replace("Nosuch", "Customers")


async def test_stored_definitions_stay_dataset_local(backend: str) -> None:
    customers = customers_dataset(
        fields=[DatasetField(name="region_label", datatype=DataType.STRING, expression="upper(region)")],
        filters=["region <> 'TEST'"],
        metrics=[DatasetMetric(name="aov", expression="sum(orders.amount) / count(*)")],
    )
    async with seed_engine(backend, datasets=seed_datasets(customers=customers)) as eng:
        resp, caught = await execute_recording(
            eng, {"population": "orders", "dimensions": ["customers.region_label"], "metrics": ["customers.aov"]},
        )
    assert by(resp, _key_ending(resp, "region_label"), _key_ending(resp, "aov")) == {"EAST": 175.0, "WEST": 300.0}
    assert future_warnings(caught) == []


# --------------------------------------------------------------------------- #
# The anchor is reached by its only to-one path
# --------------------------------------------------------------------------- #
REGION_COUNTS = {"EAST": 3.0, "WEST": 1.0}


async def test_one_to_one_path_wins_over_fanning_routes(backend: str) -> None:
    async with seed_engine(backend, managers=True) as eng:
        resp = await eng.execute({"population": "orders", "dimensions": ["customers.region"],
                                  "metrics": ["count(orders.*)"]})
    assert by(resp, "customers.region", "orders._count") == REGION_COUNTS


async def test_two_to_one_paths_are_refused_even_with_a_direct_relationship(backend: str) -> None:
    async with seed_engine(backend, stores=True) as eng:
        err = await _raises(eng, {"population": "orders", "dimensions": ["customers.region"]},
                            UnresolvableDimensionRelationshipError)
    assert "orders.customers.region" in str(err)
    assert "orders.stores.customers.region" in str(err)


async def test_the_explicit_route_resolves_the_ambiguity(backend: str) -> None:
    async with seed_engine(backend, stores=True) as eng:
        resp = await eng.execute({"population": "orders", "dimensions": ["orders.customers.region"],
                                  "metrics": ["count(orders.*)"]})
    assert by(resp, "orders.customers.region", "orders._count") == REGION_COUNTS


async def test_a_unique_fanning_route_is_taken(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"population": "customers", "dimensions": ["customers.customer_id"],
                                 "metrics": ["sum(orders.amount)"]})
    assert by(resp, "customers.customer_id", "orders.amount_sum") == {
        "C1": 300.0, "C2": 50.0, "C3": 300.0, "C4": None,
    }


async def test_several_fanning_routes_are_refused(backend: str) -> None:
    async with seed_engine(backend, managers=True) as eng:
        err = await _raises(eng, {"population": "customers", "metrics": ["sum(orders.amount)"]},
                            UnresolvableDimensionRelationshipError)
    assert "customers.orders.amount" in str(err)
    assert "customers.managers.orders.amount" in str(err)


async def test_a_directed_cycle_takes_its_to_one_path(backend: str) -> None:
    async with cycle_engine(backend) as eng:
        resp = await eng.execute({"population": "x", "dimensions": ["x.v"],
                                  "metrics": ["sum(z.amount)", "sum(x.z.amount)"]})
    assert by(resp, "x.v", "z.amount_sum") == {"x1": None, "x2": 30.0}
    assert by(resp, "x.v", "x.z.amount_sum") == {"x1": None, "x2": 30.0}


async def test_inference_and_binding_agree(backend: str) -> None:
    async with seed_engine(backend, stores=True) as eng:
        rootless = await _raises(eng, {"dimensions": ["orders.status", "customers.region"]})
        rooted = await _raises(eng, {"population": "orders", "dimensions": ["orders.status", "customers.region"]},
                               UnresolvableDimensionRelationshipError)
        explicit = {"dimensions": ["orders.status", "orders.customers.region"]}
        inferred = await eng.execute(explicit)
        named = await eng.execute({"population": "orders", **explicit})
    for err in (rootless, rooted):
        assert "orders.customers.region" in str(err)
        assert "orders.stores.customers.region" in str(err)
    assert inferred.sql == named.sql
    assert inferred.data == named.data


# --------------------------------------------------------------------------- #
# A bare name is a metric
# --------------------------------------------------------------------------- #
async def test_an_alias_is_referenced_bare(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({
        "dimensions": ["customers.region"],
        "metrics": [{"metric": "orders.total_revenue", "alias": "revenue"}],
        "filter": ["revenue > 100"], "order": [{"field": "revenue", "direction": "asc"}],
    })
    assert [(r["customers.region"], float(r["revenue"])) for r in resp.data] == [("WEST", 300.0), ("EAST", 350.0)]


@pytest.mark.parametrize("expression", ["cumsum(revenue)", "revenue * 2"])
async def test_an_alias_is_not_referenceable_in_another_metric(engine: SlayerQueryEngine, expression: str) -> None:
    query = {"population": "orders", "metrics": [{"metric": "orders.total_revenue", "alias": "revenue"},
                                                 {"metric": expression, "alias": "x"}]}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await _raises(engine, query)
    assert conversions(list(caught)) == []


async def test_an_unknown_bare_name_suggests_qualified_names(engine: SlayerQueryEngine) -> None:
    err = await _raises(engine, {"metrics": ["max(amount)"]})
    assert "amount" in str(err)
    assert "orders.amount" in str(err)
    assert "returns.amount" in str(err)


# --------------------------------------------------------------------------- #
# A row count names its dataset
# --------------------------------------------------------------------------- #
async def test_a_qualified_count_runs_without_a_population(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"dimensions": ["customers.region"], "metrics": ["count(orders.*)"]})
    counts = by(resp, "customers.region", "orders._count")
    assert (counts["EAST"], counts["WEST"]) == (3.0, 1.0)


async def test_an_unqualified_count_is_refused_without_a_population(engine: SlayerQueryEngine) -> None:
    with_dimension = await _raises(engine, {"dimensions": ["customers.region"], "metrics": ["count(*)"]})
    without = await _raises(engine, {"metrics": ["count(*)"]})
    assert "count(customers.*)" in str(with_dimension)
    assert type(with_dimension) is type(without)
    assert "count(*)" in str(without)


# --------------------------------------------------------------------------- #
# Result keys never depend on the population
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("population", ["customers", "orders"])
async def test_the_same_query_keys_alike_under_every_population(engine: SlayerQueryEngine, population: str) -> None:
    resp = await engine.execute({"population": population, "dimensions": ["customers.region"],
                                 "metrics": ["sum(orders.amount)", "orders.total_revenue"]})
    assert resp.columns == ["customers.region", "orders.amount_sum", "orders.total_revenue"]


async def test_a_row_count_keys_by_its_anchor(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"population": "customers", "metrics": ["count(orders.*)", "count(customers.*)"]})
    assert resp.columns == ["orders._count", "customers._count"]


async def test_an_alias_keys_as_itself(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"population": "customers",
                                 "metrics": [{"metric": "sum(orders.amount)", "alias": "revenue", "label": "Revenue"}]})
    assert resp.columns == ["revenue"]
    assert list(resp.attributes.metrics) == ["revenue"]


async def test_expression_keys_carry_their_common_anchor(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"population": "customers", "metrics": [
        "sum(orders.amount) / count(orders.*)", "sum(orders.amount) / count(customers.*)",
    ]})
    common, mixed = resp.columns
    assert common == "orders.amount_sum_count"
    assert "." not in mixed
    assert "orders" in mixed
    assert "customers" in mixed


async def test_different_anchors_are_different_columns(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"population": "orders",
                                 "dimensions": ["customers.region", "orders.customers.region"]})
    assert resp.columns == ["customers.region", "orders.customers.region"]
    assert all(r["customers.region"] == r["orders.customers.region"] for r in resp.data)


async def test_a_role_playing_route_keys_by_its_route(backend: str) -> None:
    async with seed_engine(backend, buyer=True, seller=True) as eng:
        resp = await eng.execute({"dimensions": ["orders.buyer.region", "orders.seller.region"]})
    assert resp.columns == ["orders.buyer.region", "orders.seller.region"]


# --------------------------------------------------------------------------- #
# A later stage reads a stage by its name and keeps its column names
# --------------------------------------------------------------------------- #
async def test_stage_columns_keep_their_names(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute([
        {"name": "s1", "population": "orders",
         "dimensions": ["orders.status", "customers.region"],
         "metrics": [{"metric": "sum(orders.amount)", "alias": "tot"}, "count(orders.*)"]},
        {"population": "s1", "dimensions": ["s1.status", "s1.customers__region", "s1.tot", "s1._count"]},
    ])
    assert table(resp, "s1.status", "s1.customers__region", "s1.tot", "s1._count") == {
        ("completed", "EAST", 150.0, 2.0), ("open", "EAST", 200.0, 1.0), ("open", "WEST", 300.0, 1.0),
    }


async def test_two_anchors_flattening_to_one_stage_column_are_refused(engine: SlayerQueryEngine) -> None:
    err = await _raises(engine, [
        {"name": "s1", "population": "orders", "dimensions": ["customers.region", "orders.customers.region"]},
        {"population": "s1", "dimensions": ["s1.customers__region"]},
    ], NameCollisionError)
    assert "orders.customers.region" in str(err)
    assert "alias" in str(err)


# --------------------------------------------------------------------------- #
# Pre-2.0 root-relative spellings convert when a population is named
# --------------------------------------------------------------------------- #
async def test_legacy_bare_names_convert(engine: SlayerQueryEngine) -> None:
    legacy, caught = await execute_recording(
        engine, {"population": "orders", "dimensions": ["status"], "metrics": ["count(*)", "sum(amount)"]})
    qualified = await engine.execute({"population": "orders", "dimensions": ["orders.status"],
                                      "metrics": ["count(orders.*)", "sum(orders.amount)"]})
    assert legacy.sql == qualified.sql
    assert legacy.columns == qualified.columns == ["orders.status", "orders._count", "orders.amount_sum"]
    assert legacy.data == qualified.data
    news = sorted(new for _, new in conversions(caught))
    assert len(news) == 3
    assert any("orders.status" in n for n in news)
    assert any("count(orders.*)" in n for n in news)
    assert any("orders.amount" in n for n in news)
    assert len(response_conversions(legacy)) == 3


async def test_an_edge_first_path_converts(backend: str) -> None:
    async with seed_engine(backend, buyer=True) as eng:
        legacy, caught = await execute_recording(eng, {"population": "orders", "dimensions": ["buyer.region"]})
        qualified = await eng.execute({"population": "orders", "dimensions": ["orders.buyer.region"]})
    assert legacy.sql == qualified.sql
    assert legacy.columns == qualified.columns == ["orders.buyer.region"]
    assert conversions(caught) == [("buyer.region", "orders.buyer.region")]
    assert response_conversions(legacy) == [("buyer.region", "orders.buyer.region")]


async def test_a_dataset_first_path_never_converts(engine: SlayerQueryEngine) -> None:
    resp, caught = await execute_recording(engine, {"population": "orders", "dimensions": ["customers.region"]})
    assert future_warnings(caught) == []
    assert response_conversions(resp) == []
    assert resp.columns == ["customers.region"]


async def test_a_saved_query_stores_the_qualified_twin(engine: SlayerQueryEngine) -> None:
    await engine.create_dataset_from_query(query={"population": "orders", "metrics": ["sum(amount)"]}, name="rev")
    saved = await engine.storage.get_dataset("rev")
    assert saved is not None
    assert saved.source_queries
    assert [m.metric for m in saved.source_queries[-1].metrics or []] == ["sum(orders.amount)"]


async def test_a_refinement_matches_a_converted_entry(engine: SlayerQueryEngine) -> None:
    await engine.create_dataset_from_query(
        query={"population": "orders", "dimensions": ["orders.status"], "metrics": ["sum(orders.amount)"]}, name="rev",
    )
    plain = await engine.execute("rev")
    refined, caught = await execute_recording(engine, "rev", refine={"metrics": ["sum(amount)"]})
    assert refined.columns == plain.columns
    assert refined.sql == plain.sql
    assert conversions(caught) in ([("amount", "orders.amount")], [("sum(amount)", "sum(orders.amount)")])


# --------------------------------------------------------------------------- #
# A reference whose meaning changed announces it
# --------------------------------------------------------------------------- #
async def test_the_directed_cycle_is_announced(backend: str) -> None:
    async with cycle_engine(backend) as eng:
        resp, caught = await execute_recording(eng, {"population": "x", "dimensions": ["x.v"],
                                                     "metrics": ["sum(z.amount)"]})
    announced = notices(resp, "z.amount", "x.z.amount")
    assert len(announced) == 1
    assert "x → y → z" in announced[0] or "x.y.z" in announced[0]
    assert future_warnings(caught) == []


async def test_an_unchanged_reference_is_silent(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute({"population": "orders", "dimensions": ["customers.region"]})
    assert notices(resp) == []
