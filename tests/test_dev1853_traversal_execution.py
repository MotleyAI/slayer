"""DEV-1853 — end-to-end reverse traversal over forward-only declarations.

A query rooted at the declared target resolves paths over the inverted edge:
LEFT keeps the root side whole, INNER restricts to matched pairs, oriented
cardinality drives exact-vs-broadcast attribution, and multi-hop reverse
paths and reverse cross-model aggregates work without any declared reverse
join.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import JoinCardinality, JoinType
from slayer.core.models import Column, ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1836_fixtures import broadcast_warnings
from tests._dev1853_fixtures import (
    CHAIN_AMOUNT_BY_NAME,
    CHAIN_AMOUNT_BY_NAME_WITH_ORPHAN,
    CHAIN_AMOUNT_BY_TIER_EXACT,
    CHAIN_MULTIHOP,
    CHAIN_POP_BROADCAST_TOTAL,
    CHAIN_REVERSE_AGG_TOTAL,
    CHAIN_REVERSE_DIMS_INNER,
    CHAIN_REVERSE_DIMS_LEFT,
    CHAIN_SPEND_BY_REVERSE_STATUS,
    CHAIN_UNPROVEN_BROADCAST_TOTAL,
    chain_engine,
    rows_set,
)


@pytest.fixture
async def fwd_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await chain_engine(d)


@pytest.fixture
async def inner_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await chain_engine(d, join_type=JoinType.INNER)


def _joins_on(sql: str, table: str) -> list[exp.Join]:
    tree = sqlglot.parse_one(sql, dialect="sqlite")
    return [j for j in tree.find_all(exp.Join)
            if isinstance(j.this, exp.Table) and j.this.name == table]


class TestReverseDimensionResolves:
    async def test_left_keeps_the_root_side_whole(self, fwd_engine) -> None:
        resp = await fwd_engine.execute(SlayerQuery(
            source_model="customers", dimensions=["name", "orders.status"]))
        assert rows_set(resp, "customers.name", "customers.orders.status") == \
            CHAIN_REVERSE_DIMS_LEFT

    async def test_inner_restricts_to_matched_pairs(self, inner_engine) -> None:
        resp = await inner_engine.execute(SlayerQuery(
            source_model="customers", dimensions=["name", "orders.status"]))
        assert rows_set(resp, "customers.name", "customers.orders.status") == \
            CHAIN_REVERSE_DIMS_INNER

    async def test_orphan_orders_never_appear_from_customers_root(
        self, fwd_engine,
    ) -> None:
        resp = await fwd_engine.execute(SlayerQuery(
            source_model="customers", dimensions=["name"],
            measures=[{"formula": "orders.amount:sum", "name": "t"}]))
        got = {r["customers.name"]: r["customers.t"] for r in resp.data}
        assert got == CHAIN_AMOUNT_BY_NAME
        assert not broadcast_warnings(resp)


class TestRootRelativeJoinType:
    async def test_reverse_left_emits_left_from_customers(self, fwd_engine) -> None:
        resp = await fwd_engine.execute(
            SlayerQuery(source_model="customers",
                        dimensions=["name", "orders.status"]),
            dry_run=True)
        assert resp.sql is not None
        assert "RIGHT" not in resp.sql.upper()
        joins = _joins_on(resp.sql, "orders")
        assert joins, resp.sql
        assert all((j.side or "").upper() == "LEFT" for j in joins), resp.sql

    async def test_reverse_inner_emits_inner_and_never_right(
        self, inner_engine,
    ) -> None:
        resp = await inner_engine.execute(
            SlayerQuery(source_model="customers",
                        dimensions=["name", "orders.status"]),
            dry_run=True)
        assert resp.sql is not None
        assert "RIGHT" not in resp.sql.upper()
        joins = _joins_on(resp.sql, "orders")
        assert joins, resp.sql
        assert all((j.kind or "").upper() == "INNER" for j in joins), resp.sql


class TestOrientedCardinalityValues:
    async def test_reverse_dimension_over_declared_to_one_stays_exact(
        self,
    ) -> None:
        # Only stored edge: customers → orders (one_to_many); customers.id is
        # NOT a PK, so the m:1 proof of the orders → customers hop is the
        # flipped declaration alone.
        with tempfile.TemporaryDirectory() as d:
            engine = await chain_engine(
                d, declare="reverse",
                cardinality=JoinCardinality.MANY_TO_ONE,
                customers_id_pk=False)
            resp = await engine.execute(SlayerQuery(
                source_model="orders", dimensions=["customers.name"],
                measures=[{"formula": "amount:sum", "name": "t"}]))
            got = {r["orders.customers.name"]: r["orders.t"] for r in resp.data}
            assert got == CHAIN_AMOUNT_BY_NAME_WITH_ORPHAN
            assert not broadcast_warnings(resp)

    async def test_inverted_declared_one_to_many_gives_exact_attribution(
        self,
    ) -> None:
        # Producer at orders attributes customers.tier over the inverted
        # (declared 1:m → oriented m:1) hop: exact per-tier values, no
        # broadcast — the class (c) "broadcast → exact" divergence.
        with tempfile.TemporaryDirectory() as d:
            engine = await chain_engine(
                d, declare="reverse",
                cardinality=JoinCardinality.MANY_TO_ONE,
                customers_id_pk=False)
            resp = await engine.execute(SlayerQuery(
                source_model="customers", dimensions=["tier"],
                measures=[{"formula": "orders.amount:sum", "name": "t"}]))
            got = {r["customers.tier"]: r["customers.t"] for r in resp.data}
            assert got == CHAIN_AMOUNT_BY_TIER_EXACT
            assert not broadcast_warnings(resp)

    async def test_undeclared_unproven_orientation_broadcasts(self) -> None:
        # Same query shape, but the attribution hop carries no declaration and
        # covers no unique set — unproven fails closed into broadcast.
        with tempfile.TemporaryDirectory() as d:
            engine = await chain_engine(
                d, declare="forward", cardinality=None, customers_id_pk=False)
            resp = await engine.execute(SlayerQuery(
                source_model="customers", dimensions=["tier"],
                measures=[{"formula": "orders.amount:sum", "name": "t"}]))
            assert broadcast_warnings(resp)
            values = {r["customers.t"] for r in resp.data}
            assert values == {CHAIN_UNPROVEN_BROADCAST_TOTAL}

    async def test_metric_crossing_inverted_fan_out_broadcasts(
        self, fwd_engine,
    ) -> None:
        # regions.pop:sum cannot attribute orders.status: the path from
        # regions crosses two fan-out orientations.
        resp = await fwd_engine.execute(SlayerQuery(
            source_model="customers", dimensions=["orders.status"],
            measures=[{"formula": "regions.pop:sum", "name": "rp"}]))
        assert broadcast_warnings(resp)
        values = {r["customers.rp"] for r in resp.data}
        assert values == {CHAIN_POP_BROADCAST_TOTAL}
        statuses = {r["customers.orders.status"] for r in resp.data}
        assert statuses == {"ok", "new", None}

    async def test_local_measure_with_reverse_fan_out_dim_stays_exact(
        self, fwd_engine,
    ) -> None:
        # Grain-safe dedup across the inverted hop: each customer's spend
        # counts once per status it has, never join-multiplied.
        resp = await fwd_engine.execute(SlayerQuery(
            source_model="customers", dimensions=["orders.status"],
            measures=[{"formula": "spend:sum", "name": "s"}]))
        got = {r["customers.orders.status"]: r["customers.s"]
               for r in resp.data}
        assert got == CHAIN_SPEND_BY_REVERSE_STATUS
        assert not broadcast_warnings(resp)


class TestMultiHopReverse:
    async def test_two_reverse_hops_from_regions(self, fwd_engine) -> None:
        resp = await fwd_engine.execute(SlayerQuery(
            source_model="regions",
            dimensions=["name", "customers.orders.status"]))
        assert rows_set(resp, "regions.name",
                        "regions.customers.orders.status") == CHAIN_MULTIHOP


class TestReverseClosure:
    async def test_reverse_cross_model_aggregate_binds(self, fwd_engine) -> None:
        # The source bundle from customers must include orders (reverse BFS).
        resp = await fwd_engine.execute(SlayerQuery(
            source_model="customers",
            measures=[{"formula": "orders.amount:sum", "name": "t"}]))
        assert len(resp.data) == 1
        assert resp.data[0]["customers.t"] == \
            pytest.approx(CHAIN_REVERSE_AGG_TOTAL)

    async def test_reverse_saved_measure_reference_binds(self) -> None:
        # A saved measure on orders, referenced across the reverse hop.
        with tempfile.TemporaryDirectory() as d:
            engine = await chain_engine(d)
            storage = engine.storage
            orders = await storage.get_model("orders", data_source="test")
            assert orders is not None
            orders.measures = [
                *orders.measures,
                ModelMeasure(name="total_amount", formula="amount:sum"),
            ]
            await storage.save_model(orders)
            resp = await engine.execute(SlayerQuery(
                source_model="customers", dimensions=["name"],
                measures=[{"formula": "orders.total_amount", "name": "t"}]))
            got = {r["customers.name"]: r["customers.t"] for r in resp.data}
            assert got == CHAIN_AMOUNT_BY_NAME

    async def test_drift_touched_set_expands_in_reverse(self, fwd_engine) -> None:
        # Schema-drift attribution reaches declaring models from the target
        # side of their edges.
        touched = {"regions"}
        await fwd_engine._expand_join_graph(touched=touched, data_source="test")
        assert {"customers", "orders"} <= touched

    async def test_mode_a_column_over_a_reverse_only_path(self) -> None:
        # Mode-A free SQL resolves the same reverse hop Mode-B does.
        with tempfile.TemporaryDirectory() as d:
            engine = await chain_engine(d)
            storage = engine.storage
            cust = await storage.get_model("customers", data_source="test")
            assert cust is not None
            cust.columns = [
                *cust.columns, Column(name="o_status", sql="orders.status"),
            ]
            await storage.save_model(cust)
            resp = await engine.execute(SlayerQuery(
                source_model="customers", dimensions=["name", "o_status"]))
            assert rows_set(
                resp, "customers.name", "customers.o_status",
            ) == CHAIN_REVERSE_DIMS_LEFT
