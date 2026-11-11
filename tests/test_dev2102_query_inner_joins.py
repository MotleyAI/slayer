"""A query-declared INNER join restricts the population as the field filter "the target is matched"."""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import JoinCardinality, JoinType
from slayer.core.errors import AmbiguousJoinPathError, SlayerError
from slayer.core.models import ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev2102_fixtures import (
    ACTIVE_VIP_CUSTOMERS,
    ALL_CUSTOMERS,
    BIG_SPENDERS,
    CUSTOMER_PAIRS,
    DIALECTS,
    DS,
    ORDERS_OF_VIP_CUSTOMERS,
    ORDERS_WITH_REGIONED_CUSTOMER,
    PART_PAIRS,
    VIP_CUSTOMERS,
    addresses,
    customers,
    ext,
    join,
    q,
    rows,
    shop,
    values,
    vip_list,
    with_models,
)

VIP_PAIRS = [["id", "customer_id"]]
REGION_PAIRS = [["region_id", "id"]]


@pytest.fixture(params=DIALECTS)
def dialect(request) -> str:
    return request.param


def _vip(join_type: str = "inner") -> dict:
    return ext("customers", joins=[join("vip_list", VIP_PAIRS, join_type=join_type)])


def _tables(sql: str | None, name: str) -> int:
    assert sql is not None
    return sum(1 for t in sqlglot.parse_one(sql).find_all(exp.Table) if t.name == name)


async def _outcome(engine: SlayerQueryEngine, query: SlayerQuery) -> Any:
    """Sorted rows, or the raised error's type."""
    try:
        resp = await engine.execute(query)
    except SlayerError as e:
        return type(e)
    return rows(resp, *resp.columns)


class TestRootAndStoredEdges:
    async def test_an_inner_join_a_later_extension_names_restricts_by_that_name(self, dialect) -> None:
        query = q(source_model="events", dimensions=["id"], extensions=[
            ext("events", joins=[join("users", [["user_id", "id"]], join_type="inner"),
                                 join("users", [["id", "id"]], name="other")]),
            ext("users", joins=[join("events", [["id", "user_id"]], name="primary")])])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert values(resp, "events.id") == {1}

    async def test_inner_on_root_keeps_matched_rows_once(self, dialect) -> None:
        async with shop(dialect) as engine:
            resp = await engine.execute(q(source_model="customers", extensions=[_vip()], dimensions=["name"]))
        assert sorted(r["customers.name"] for r in resp.data) == sorted(VIP_CUSTOMERS)

    async def test_left_declaration_never_restricts(self, dialect) -> None:
        async with shop(dialect) as engine:
            resp = await engine.execute(q(source_model="customers", extensions=[_vip("left")], dimensions=["name"]))
        assert values(resp, "customers.name") == ALL_CUSTOMERS

    async def test_inner_over_a_stored_left_edge_restricts_without_a_parallel_edge(self, dialect) -> None:
        query = q(source_model="orders", extensions=[ext("orders", joins=[
            join("customers", CUSTOMER_PAIRS, name="c", join_type="inner")])],
            dimensions=["id", "c.name", "customers.tier"])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
            dry = await engine.execute(query, dry_run=True)
        assert resp.columns == ["orders.id", "orders.customers.name", "orders.customers.tier"]
        assert rows(resp, *resp.columns) == [
            (1, "Ann", "gold"), (2, "Ann", "gold"), (3, "Bob", "silver"),
            (4, "Cid", "gold"), (5, "Dee", "bronze"), (6, "Eve", "silver"),
        ]
        assert _tables(dry.sql, "customers") == 1, dry.sql

    async def test_a_reused_stored_inner_edge_still_adds_its_restriction(self, dialect) -> None:
        stored_inner = customers(ModelJoin(target_model="vip_list", join_pairs=VIP_PAIRS, join_type=JoinType.INNER))
        async with shop(dialect, with_models(stored_inner)) as engine:
            untraversed = await engine.execute(q(source_model="customers", dimensions=["name"]))
            declared = await engine.execute(q(source_model="customers", extensions=[_vip()], dimensions=["name"]))
        assert values(untraversed, "customers.name") == ALL_CUSTOMERS
        assert values(declared, "customers.name") == VIP_CUSTOMERS

    async def test_a_stored_inner_edge_restricts_when_traversed_under_a_left_declaration(self, dialect) -> None:
        stored_inner = customers(ModelJoin(target_model="vip_list", join_pairs=VIP_PAIRS, join_type=JoinType.INNER))
        async with shop(dialect, with_models(stored_inner)) as engine:
            resp = await engine.execute(q(
                source_model="customers", extensions=[_vip("left")], dimensions=["name", "vip_list.note"]))
        assert rows(resp, "customers.name", "customers.vip_list.note") == [
            ("Ann", "a"), ("Ann", "b"), ("Bob", "c"), ("Eve", "d"),
        ]


class TestPaths:
    async def test_fanning_path_restricts_by_existence_and_never_multiplies(self, dialect) -> None:
        query = q(source_model="customers",
                  extensions=[ext("orders", joins=[join("vip_orders", [["id", "order_id"]], join_type="inner")])],
                  dimensions=["name"], measures=[{"formula": "count(orders.id)", "name": "n"}])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert rows(resp, "customers.name", "customers.n") == [("Ann", 1), ("Bob", 1)]

    async def test_multi_hop_chain_through_a_stored_left_hop(self, dialect) -> None:
        query = q(source_model="orders",
                  extensions=[ext("customers", joins=[join("regions", REGION_PAIRS, join_type="inner")])],
                  dimensions=["id"])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert values(resp, "orders.id") == ORDERS_WITH_REGIONED_CUSTOMER

    async def test_declaring_model_two_hops_from_the_root(self, dialect) -> None:
        query = q(source_model="orders", dimensions=["id"], measures=[{"formula": "count(*)", "name": "n"}],
                  extensions=[ext("customers", joins=[join("regions", REGION_PAIRS, join_type="left")]),
                              ext("regions", joins=[join("countries", [["country_id", "id"]], join_type="inner")])])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert rows(resp, "orders.id", "orders.n") == [(1, 1), (2, 1), (3, 1)]

    async def test_adding_a_measure_never_changes_the_rows(self, dialect) -> None:
        extensions = [ext("customers", joins=[join("regions", REGION_PAIRS, join_type="inner")])]
        base = {"source_model": "orders", "extensions": extensions, "dimensions": ["status"]}
        async with shop(dialect) as engine:
            plain = await engine.execute(q(**base, measures=[{"formula": "count(*)", "name": "n"}]))
            more = await engine.execute(q(**base, measures=[
                {"formula": "count(*)", "name": "n"}, {"formula": "sum(customers.credit)", "name": "cr"}]))
        assert rows(plain, "orders.status", "orders.n") == rows(more, "orders.status", "orders.n")
        assert rows(plain, "orders.status", "orders.n") == [("new", 1), ("ok", 3)]

    async def test_inner_to_a_sibling_stage(self, dialect) -> None:
        stage = q(name="big_spenders", source_model="orders", dimensions=["customer_id"],
                  measures=[{"formula": "sum(amount)", "name": "spent"}], filters=["spent > 25"])
        root = q(source_model="customers", extensions=[ext("customers", joins=[
            join("big_spenders", [["id", "customer_id"]], join_type="inner")])], dimensions=["name"])
        async with shop(dialect) as engine:
            resp = await engine.execute([stage, root])
        assert values(resp, "customers.name") == BIG_SPENDERS

    async def test_inner_to_a_query_backed_model_on_a_flattened_key(self, dialect) -> None:
        qb = SlayerModel(name="big_spenders_qb", data_source=DS, source_queries=[SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customers.id"],
            "measures": [{"formula": "sum(amount)", "name": "spent"}], "filters": ["spent > 25"]})])
        root = q(source_model="customers", extensions=[ext("customers", joins=[
            join("big_spenders_qb", [["id", "customers__id"]], join_type="inner")])], dimensions=["name"])
        async with shop(dialect, with_models(qb)) as engine:
            resp = await engine.execute(root)
        assert values(resp, "customers.name") == BIG_SPENDERS

    async def test_target_model_filters_count_as_unmatched(self, dialect) -> None:
        async with shop(dialect, with_models(vip_list(filters=["active = 1"]))) as engine:
            resp = await engine.execute(q(source_model="customers", extensions=[_vip()], dimensions=["name"]))
        assert values(resp, "customers.name") == ACTIVE_VIP_CUSTOMERS

    @pytest.mark.parametrize("target", ["part_keys_b", "part_keys"], ids=["new-edge", "reversed-stored-edge"])
    async def test_composite_keys_with_partial_nulls_are_unmatched(self, dialect, target: str) -> None:
        query = q(source_model="parts", extensions=[ext("parts", joins=[join(target, PART_PAIRS, join_type="inner")])],
                  dimensions=["id"])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert values(resp, "parts.id") == {1}


class TestReferenceBinding:
    """The restriction binds exactly like ``M.<edge>.<target key> IS NOT NULL`` written in the same query."""

    async def _assert_as_written(self, engine: SlayerQueryEngine, *, root: str, m: str, target: str,
                                 pairs: list[list[str]], key: str, dims: list[str]) -> Any:
        declared = q(source_model=root, dimensions=dims,
                     extensions=[ext(m, joins=[join(target, pairs, join_type="inner")])])
        written = q(source_model=root, dimensions=dims, filters=[f"{m}.{target}.{key} IS NOT NULL"],
                    extensions=[ext(m, joins=[join(target, pairs, join_type="left")])])
        got, expected = await _outcome(engine, declared), await _outcome(engine, written)
        assert got == expected
        return got

    async def test_adjacent_model_with_a_second_indirect_route(self, dialect) -> None:
        models = with_models(addresses(ModelJoin(target_model="customers", join_pairs=CUSTOMER_PAIRS)))
        async with shop(dialect, models) as engine:
            got = await self._assert_as_written(
                engine, root="orders", m="customers", target="vip_list", pairs=VIP_PAIRS, key="customer_id",
                dims=["id"])
        assert {r[0] for r in got} == ORDERS_OF_VIP_CUSTOMERS

    async def test_adjacent_named_parallel_edges_are_ambiguous_naming_the_declaration(self, dialect) -> None:
        async with shop(dialect) as engine:
            got = await self._assert_as_written(
                engine, root="orders", m="addresses", target="regions", pairs=REGION_PAIRS, key="id", dims=["id"])
            assert got is AmbiguousJoinPathError
            declared = q(source_model="orders", dimensions=["id"], extensions=[
                ext("addresses", joins=[join("regions", REGION_PAIRS, join_type="inner")])])
            with pytest.raises(AmbiguousJoinPathError) as info:
                await engine.execute(declared)
        message = str(info.value).lower()
        assert "inner" in message, message
        assert "addresses" in message, message
        assert "regions" in message, message

    async def test_one_safe_and_one_fanning_parallel_edge(self, dialect) -> None:
        models = with_models(
            customers(ModelJoin(target_model="addresses", name="home_address", join_pairs=[["home_address_id", "id"]],
                                cardinality=JoinCardinality.MANY_TO_ONE)),
            addresses(ModelJoin(target_model="customers", join_pairs=CUSTOMER_PAIRS,
                                cardinality=JoinCardinality.MANY_TO_ONE)),
        )
        async with shop(dialect, models) as engine:
            got = await self._assert_as_written(
                engine, root="customers", m="addresses", target="regions", pairs=REGION_PAIRS, key="id",
                dims=["name"])
            assert got is AmbiguousJoinPathError
            declared = q(source_model="customers", dimensions=["name"], extensions=[
                ext("addresses", joins=[join("regions", REGION_PAIRS, join_type="inner")])])
            with pytest.raises(AmbiguousJoinPathError) as info:
                await engine.execute(declared)
        message = str(info.value).lower()
        assert "inner" in message, message
        assert "addresses" in message, message
        assert "regions" in message, message

    async def test_unreachable_declaration_fails_naming_it(self, dialect) -> None:
        query = q(source_model="orders", dimensions=["id"],
                  extensions=[ext("events", joins=[join("users", [["user_id", "id"]], join_type="inner")])])
        async with shop(dialect) as engine:
            with pytest.raises(SlayerError) as info:
                await engine.execute(query)
        message = str(info.value).lower()
        assert "inner" in message, message
        assert "events" in message, message
        assert "users" in message, message


class TestModes:
    async def test_rootless_query_reads_the_restriction(self, dialect) -> None:
        async with shop(dialect) as engine:
            rootless = await engine.execute(q(extensions=[_vip()], dimensions=["customers.name"]))
            rooted = await engine.execute(q(source_model="customers", extensions=[_vip()], dimensions=["name"]))
        assert rootless.population == "customers"
        assert sorted(r[rootless.columns[0]] for r in rootless.data) == sorted(
            r["customers.name"] for r in rooted.data) == sorted(VIP_CUSTOMERS)

    async def test_raw_row_mode_applies_the_restriction(self, dialect) -> None:
        async with shop(dialect) as engine:
            resp = await engine.execute(q(source_model="customers", extensions=[_vip()], dimensions=["name"],
                                          distinct_dimension_values=False))
        assert sorted(r["customers.name"] for r in resp.data) == sorted(VIP_CUSTOMERS)

    async def test_declared_inner_on_the_stored_edge_keeps_left_traversal(self, dialect) -> None:
        query = q(source_model="orders", dimensions=["customers.tier"],
                  extensions=[ext("orders", joins=[join("customers", CUSTOMER_PAIRS, join_type="inner")])],
                  measures=[{"formula": "count(*)", "name": "n"}])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
            dry = await engine.execute(query, dry_run=True)
        assert rows(resp, "orders.customers.tier", "orders.n") == [("bronze", 1), ("gold", 3), ("silver", 2)]
        assert dry.sql is not None
        joins = [j for j in sqlglot.parse_one(dry.sql).find_all(exp.Join)
                 if isinstance(j.this, exp.Table) and j.this.name == "customers"]
        assert joins, dry.sql
        assert all(j.side == "LEFT" for j in joins), dry.sql
