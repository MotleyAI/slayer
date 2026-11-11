"""An extension join is matched against the edges between its two models, in either orientation, on its key pairs."""

from __future__ import annotations

import pytest

from slayer.core.errors import AmbiguousJoinPathError, CircularJoinPathError, SlayerError
from slayer.core.models import ModelJoin
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev2102_fixtures import (
    AMOUNT_BY_TIER,
    BILLING_PAIRS,
    CUSTOMER_PAIRS,
    DIALECTS,
    SHIPPING_PAIRS,
    by_key,
    customers,
    ext,
    join,
    q,
    regions,
    rows,
    shop,
    with_models,
)

REGION_PAIRS = [["region_id", "id"]]
REVERSE_CUSTOMER_PAIRS = [["id", "customer_id"]]


@pytest.fixture(params=DIALECTS)
def dialect(request) -> str:
    return request.param


async def _sql(engine: SlayerQueryEngine, query) -> str:
    resp = await engine.execute(query, dry_run=True)
    assert resp.sql is not None
    return resp.sql


async def _error(engine: SlayerQueryEngine, query) -> str:
    with pytest.raises(SlayerError) as info:
        await engine.execute(query)
    return str(info.value)


def _tier_amounts(**fields) -> dict:
    return dict(source_model="orders", dimensions=["customers.tier"],
                measures=[{"formula": "sum(amount)", "name": "a"}], **fields)


class TestStoredEdgeReuse:
    async def test_repeating_a_stored_join_is_that_edge(self, dialect) -> None:
        repeat = q(**_tier_amounts(extensions=[ext("orders", joins=[join("customers", CUSTOMER_PAIRS)])]))
        async with shop(dialect) as engine:
            resp = await engine.execute(repeat)
            assert await _sql(engine, repeat) == await _sql(engine, q(**_tier_amounts()))
        assert by_key(resp, "orders.customers.tier", "orders.a") == AMOUNT_BY_TIER

    async def test_a_differing_unnamed_join_stays_parallel_and_fails_closed(self, dialect) -> None:
        query = q(**_tier_amounts(extensions=[ext("orders", joins=[join("customers", [["referrer_id", "id"]])])]))
        async with shop(dialect) as engine:
            with pytest.raises(AmbiguousJoinPathError):
                await engine.execute(query)

    async def test_a_reversed_declaration_is_the_stored_edge(self, dialect) -> None:
        plain = dict(source_model="customers", dimensions=["name"],
                     measures=[{"formula": "count(orders.id)", "name": "n"}])
        reversed_ = q(**plain, extensions=[ext("customers", joins=[join("orders", REVERSE_CUSTOMER_PAIRS)])])
        async with shop(dialect) as engine:
            resp = await engine.execute(reversed_)
            assert await _sql(engine, reversed_) == await _sql(engine, q(**plain))
        assert rows(resp, "customers.name", "customers.n") == [
            ("Ann", 2), ("Bob", 1), ("Cid", 1), ("Dee", 1), ("Eve", 1)]

    @pytest.mark.parametrize("cardinality", ["many_to_one", None])
    async def test_type_and_compatible_cardinality_do_not_split_the_edge(self, dialect, cardinality) -> None:
        declared = join("customers", CUSTOMER_PAIRS, join_type="left", description="restated", meta={"k": 1})
        if cardinality:
            declared["cardinality"] = cardinality
        query = q(**_tier_amounts(extensions=[ext("orders", joins=[declared])]))
        async with shop(dialect) as engine:
            assert await _sql(engine, query) == await _sql(engine, q(**_tier_amounts()))

    async def test_an_inner_declaration_on_the_stored_pairs_keeps_one_edge(self, dialect) -> None:
        query = q(**_tier_amounts(extensions=[ext("orders", joins=[
            join("customers", CUSTOMER_PAIRS, join_type="inner")])]))
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert by_key(resp, "orders.customers.tier", "orders.a") == {
            tier: amount for tier, amount in AMOUNT_BY_TIER.items() if tier is not None}


class TestAliases:
    async def test_alias_resolves_to_the_stored_edge_under_its_canonical_spelling(self, dialect) -> None:
        aliased = q(source_model="orders", dimensions=["c.name", "customers.tier"],
                    extensions=[ext("orders", joins=[join("customers", CUSTOMER_PAIRS, name="c")])])
        canonical = q(source_model="orders", dimensions=["customers.name", "customers.tier"])
        async with shop(dialect) as engine:
            resp = await engine.execute(aliased)
            assert await _sql(engine, aliased) == await _sql(engine, canonical)
        assert resp.columns == ["orders.customers.name", "orders.customers.tier"]

    async def test_alias_selects_one_of_two_parallel_stored_edges(self, dialect) -> None:
        aliased = q(source_model="orders", dimensions=["b.city"],
                    extensions=[ext("orders", joins=[join("addresses", BILLING_PAIRS, name="b")])])
        async with shop(dialect) as engine:
            resp = await engine.execute(aliased)
            assert await _sql(engine, aliased) == await _sql(
                engine, q(source_model="orders", dimensions=["billing_address.city"]))
        assert resp.columns == ["orders.billing_address.city"]

    async def test_alias_is_local_to_its_query_object(self, dialect) -> None:
        stage = q(name="s", source_model="orders", dimensions=["status", "c.tier"],
                  extensions=[ext("orders", joins=[join("customers", CUSTOMER_PAIRS, name="c")])])
        root = q(source_model="orders", dimensions=["c.tier"],
                 extensions=[ext("orders", joins=[join("s", [["status", "status"]])])])
        async with shop(dialect) as engine:
            with pytest.raises(SlayerError):
                await engine.execute([stage, root])

    async def test_a_declared_name_equal_to_the_target_model_is_its_spelling(self, dialect) -> None:
        query = q(source_model="customers", dimensions=["name", "regions.name"],
                  extensions=[ext("customers", joins=[join("regions", REGION_PAIRS, name="regions")])])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert resp.columns == ["customers.name", "customers.regions.name"]


class TestConflicts:
    async def test_several_candidates_with_equal_pairs_are_ambiguous(self, dialect) -> None:
        query = q(source_model="shipments", dimensions=["id"],
                  extensions=[ext("shipments", joins=[join("addresses", [["address_id", "id"]])])])
        async with shop(dialect) as engine:
            message = await _error(engine, query)
        assert "origin" in message, message
        assert "dest" in message, message

    async def test_a_named_declaration_with_different_pairs_is_an_error(self, dialect) -> None:
        query = q(source_model="orders", dimensions=["id"], extensions=[ext("orders", joins=[
            join("addresses", [["ship_region_id", "id"]], name="billing_address")])])
        async with shop(dialect) as engine:
            message = await _error(engine, query)
        assert "billing_address" in message, message

    async def test_conflicting_cardinality_is_an_error(self, dialect) -> None:
        query = q(source_model="customers", dimensions=["name"], extensions=[ext("customers", joins=[
            join("orders", REVERSE_CUSTOMER_PAIRS, cardinality="many_to_one")])])
        async with shop(dialect) as engine:
            message = await _error(engine, query)
        assert "many_to_one" in message, message
        assert "one_to_many" in message, message
        assert "orders" in message, message

    async def test_an_alias_equal_to_another_edges_name_is_an_error(self, dialect) -> None:
        query = q(source_model="orders", dimensions=["id"], extensions=[ext("orders", joins=[
            join("addresses", SHIPPING_PAIRS, name="billing_address")])])
        async with shop(dialect) as engine:
            message = await _error(engine, query)
        assert "billing_address" in message, message

    async def test_a_new_edge_named_like_a_stored_edge_elsewhere_is_an_error(self, dialect) -> None:
        query = q(source_model="customers", dimensions=["name"], extensions=[ext("customers", joins=[
            join("regions", REGION_PAIRS, name="billing_address")])])
        async with shop(dialect) as engine:
            message = await _error(engine, query)
        assert "billing_address" in message, message

    @pytest.mark.parametrize("name", ["countries", "s"], ids=["model-name", "stage-name"])
    async def test_a_new_edge_named_like_a_model_or_stage_is_an_error(self, dialect, name: str) -> None:
        stage = q(name="s", source_model="orders", dimensions=["customer_id"])
        root = q(source_model="customers", dimensions=["name"], extensions=[ext("customers", joins=[
            join("regions", REGION_PAIRS, name=name), join("s", [["id", "customer_id"]])])])
        async with shop(dialect) as engine:
            message = await _error(engine, [stage, root])
        assert repr(name) in message or f"'{name}'" in message or f"`{name}`" in message, message

    async def test_two_names_for_one_new_relationship_are_an_error(self, dialect) -> None:
        query = q(source_model="customers", dimensions=["name"], extensions=[
            ext("customers", joins=[join("regions", REGION_PAIRS, name="home_region")]),
            ext("regions", joins=[join("customers", [["id", "region_id"]], name="residents")]),
        ])
        async with shop(dialect) as engine:
            message = await _error(engine, query)
        assert "home_region" in message, message
        assert "residents" in message, message


class TestNewEdges:
    async def test_one_relationship_declared_from_both_sides_is_one_edge(self, dialect) -> None:
        query = q(source_model="customers", dimensions=["name", "home_region.name"], extensions=[
            ext("customers", joins=[join("regions", REGION_PAIRS, name="home_region")]),
            ext("regions", joins=[join("customers", [["id", "region_id"]])]),
        ])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert rows(resp, "customers.name", "customers.home_region.name") == [
            ("Ann", "North"), ("Bob", "South"), ("Cid", None), ("Dee", None), ("Eve", "East")]

    async def test_a_new_edge_is_traversed_as_left(self, dialect) -> None:
        query = q(source_model="customers", dimensions=["name", "regions.name"],
                  extensions=[ext("customers", joins=[join("regions", REGION_PAIRS)])])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert {r["customers.name"] for r in resp.data} == {"Ann", "Bob", "Cid", "Dee", "Eve"}

    async def test_multi_hop_query_local_chain(self, dialect) -> None:
        query = q(source_model="customers", dimensions=["name", "regions.countries.name"], extensions=[
            ext("customers", joins=[join("regions", REGION_PAIRS)]),
            ext("regions", joins=[join("countries", [["country_id", "id"]])]),
        ])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert rows(resp, "customers.name", "customers.regions.countries.name") == [
            ("Ann", "Norland"), ("Bob", "Southia"), ("Cid", None), ("Dee", None), ("Eve", None)]

    async def test_cyclic_query_local_chain_fails_like_a_stored_cycle(self, dialect) -> None:
        dimensions = ["name", "home_region.manager.name"]
        plain = q(source_model="customers", dimensions=dimensions)
        local = q(source_model="customers", dimensions=dimensions, extensions=[
            ext("customers", joins=[join("regions", REGION_PAIRS, name="home_region")]),
            ext("regions", joins=[join("customers", [["manager_id", "id"]], name="manager")]),
        ])
        stored = with_models(
            customers(ModelJoin(target_model="regions", name="home_region", join_pairs=REGION_PAIRS)),
            regions().model_copy(update={"joins": [
                ModelJoin(target_model="customers", name="manager", join_pairs=[["manager_id", "id"]])]}),
        )
        async with shop(dialect) as engine:
            with pytest.raises(CircularJoinPathError):
                await engine.execute(local)
        async with shop(dialect, stored) as engine:
            with pytest.raises(CircularJoinPathError):
                await engine.execute(plain)

    async def test_a_query_written_join_to_a_stage_wins_over_a_same_named_stored_edge(self, dialect) -> None:
        stage = q(name="customers", source_model="orders", dimensions=["customer_id"],
                  measures=[{"formula": "sum(amount)", "name": "spent"}])
        root = q(source_model="orders", dimensions=["customers.spent"],
                 measures=[{"formula": "sum(amount)", "name": "a"}],
                 extensions=[ext("orders", joins=[join("customers", [["customer_id", "customer_id"]])])])
        async with shop(dialect) as engine:
            resp = await engine.execute([stage, root])
        assert by_key(resp, "orders.customers.spent", "orders.a") == {
            30.0: 60.0, 5.0: 5.0, 40.0: 40.0, 15.0: 15.0, None: 7.0}
