"""Ingestion names every member of a parallel set that holds a FK-backed edge."""

from __future__ import annotations

import pytest

import slayer.engine.ingestion as ingestion_mod
from slayer.core.models import ModelJoin

from tests._dev1853_fixtures import rows_set
from tests._dev2073_fixtures import (
    BILLING_CITIES,
    BILLING_SHIPPING,
    CHAIN,
    SHIPPING_CITIES,
    TWO_CYCLE,
    live,
)

BILLING = ("orders", "addresses", (("billing_address_id", "id"),))
SHIPPING = ("orders", "addresses", (("shipping_address_id", "id"),))

ADDRESSES_ONLY = """
CREATE TABLE addresses (id INTEGER PRIMARY KEY, city TEXT);
INSERT INTO addresses VALUES (1, 'Oslo'), (2, 'Lima');
"""


class TestParallelFksAreAddressable:
    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_billing_and_shipping_are_named_and_queryable(self, dialect: str) -> None:
        async with live(BILLING_SHIPPING, dialect=dialect) as lv:
            await lv.ingest()
            edges = await lv.edges()
            assert edges[BILLING] == "billing_address"
            assert edges[SHIPPING] == "shipping_address"
            resp = await lv.query(
                source_model="orders",
                dimensions=["id", "billing_address.city", "shipping_address.city"],
            )
            assert rows_set(resp, "orders.id", "orders.billing_address.city") == BILLING_CITIES
            assert rows_set(resp, "orders.id", "orders.shipping_address.city") == SHIPPING_CITIES

    async def test_two_cycle_edges_are_named_by_stem(self) -> None:
        async with live(TWO_CYCLE) as lv:
            await lv.ingest()
            edges = await lv.edges()
            assert edges[("transactions", "coupon_usages", (("coupon_usage_id", "id"),))] == "coupon_usage"
            assert edges[("coupon_usages", "transactions", (("transaction_id", "id"),))] == "transaction"

    async def test_singleton_edge_stays_unnamed(self) -> None:
        async with live(CHAIN) as lv:
            await lv.ingest()
            assert await lv.edges() == {("orders", "customers", (("customer_id", "id"),)): None}
            resp = await lv.query(source_model="orders", dimensions=["id", "customers.name"])
            assert rows_set(resp, "orders.id", "orders.customers.name") == {
                (1, "Alice"), (2, "Alice"), (3, "Bob"),
            }


class TestNamingRespectsAuthoredJoins:
    async def test_hand_authored_member_of_a_fk_parallel_set_is_named(self) -> None:
        async with live(ADDRESSES_ONLY + """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, legacy_addr INTEGER, amount REAL);
            INSERT INTO orders VALUES (1, 2, 10.0);
        """) as lv:
            await lv.ingest()
            orders = await lv.model("orders")
            legacy = ModelJoin(target_model="addresses", join_pairs=[["legacy_addr", "id"]])
            await lv.storage.save_model(orders.model_copy(update={"joins": [legacy]}))
            lv.run("ALTER TABLE orders ADD COLUMN billing_address_id INTEGER REFERENCES addresses(id);"
                   "UPDATE orders SET billing_address_id = 1;")

            result = await lv.ingest()

            assert result.errors == []
            edges = await lv.edges()
            assert edges[("orders", "addresses", (("legacy_addr", "id"),))] == "legacy_addr"
            assert edges[BILLING] == "billing_address"
            resp = await lv.query(source_model="orders", dimensions=["legacy_addr.city", "billing_address.city"])
            assert rows_set(resp, "orders.legacy_addr.city", "orders.billing_address.city") == {("Lima", "Oslo")}

    async def test_purely_hand_authored_parallel_set_is_untouched(self) -> None:
        async with live(ADDRESSES_ONLY + """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, a1 INTEGER, a2 INTEGER);
        """) as lv:
            await lv.ingest()
            orders = await lv.model("orders")
            joins = [ModelJoin(target_model="addresses", join_pairs=[[col, "id"]]) for col in ("a1", "a2")]
            await lv.storage.save_model(orders.model_copy(update={"joins": joins}))

            await lv.ingest()

            assert [j.name for j in (await lv.model("orders")).joins] == [None, None]

    async def test_user_set_name_is_kept(self) -> None:
        async with live(ADDRESSES_ONLY + """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, billing_address_id INTEGER REFERENCES addresses(id));
        """) as lv:
            await lv.ingest()
            orders = await lv.model("orders")
            named = [j.model_copy(update={"name": "bill_to"}) for j in orders.joins]
            await lv.storage.save_model(orders.model_copy(update={"joins": named}))
            lv.run("ALTER TABLE orders ADD COLUMN shipping_address_id INTEGER REFERENCES addresses(id);")

            await lv.ingest()

            edges = await lv.edges()
            assert edges[BILLING] == "bill_to"
            assert edges[SHIPPING] == "shipping_address"


class TestGeneratedNamesAvoidCollisions:
    async def test_stem_equal_to_a_model_name_falls_back(self) -> None:
        async with live("""
            CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE customer (id INTEGER PRIMARY KEY);
            CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES people(id),
                                 buyer_id INTEGER REFERENCES people(id));
        """) as lv:
            await lv.ingest()
            edges = await lv.edges()
            assert edges[("orders", "people", (("customer_id", "id"),))] == "orders_customer"
            assert edges[("orders", "people", (("buyer_id", "id"),))] == "buyer"

    async def test_both_named_candidates_taken_falls_back_to_a_numeric_suffix(self) -> None:
        async with live("""
            CREATE TABLE people (id INTEGER PRIMARY KEY);
            CREATE TABLE buyer (id INTEGER PRIMARY KEY);
            CREATE TABLE orders_buyer (id INTEGER PRIMARY KEY);
            CREATE TABLE orders (id INTEGER PRIMARY KEY, buyer_id INTEGER REFERENCES people(id),
                                 seller_id INTEGER REFERENCES people(id));
        """) as lv:
            await lv.ingest()
            edges = await lv.edges()
            assert edges[("orders", "people", (("buyer_id", "id"),))] == "orders_buyer_2"
            assert edges[("orders", "people", (("seller_id", "id"),))] == "seller"

    async def test_stem_suffix_is_stripped_case_insensitively_and_empty_stem_keeps_the_column(self) -> None:
        async with live(ADDRESSES_ONLY + """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, Billing_ID INTEGER REFERENCES addresses(id),
                                 _fk INTEGER REFERENCES addresses(id));
        """) as lv:
            await lv.ingest()
            edges = await lv.edges()
            assert edges[("orders", "addresses", (("Billing_ID", "id"),))] == "Billing"
            assert edges[("orders", "addresses", (("_fk", "id"),))] == "_fk"

    async def test_stem_used_by_an_incident_edge_falls_back(self) -> None:
        async with live(ADDRESSES_ONLY + """
            CREATE TABLE invoices (id INTEGER PRIMARY KEY);
            CREATE TABLE orders (id INTEGER PRIMARY KEY, invoice_id INTEGER REFERENCES invoices(id));
        """) as lv:
            await lv.ingest()
            orders = await lv.model("orders")
            named = [j.model_copy(update={"name": "billing_address"}) for j in orders.joins]
            await lv.storage.save_model(orders.model_copy(update={"joins": named}))
            lv.run("ALTER TABLE orders ADD COLUMN billing_address_id INTEGER REFERENCES addresses(id);"
                   "ALTER TABLE orders ADD COLUMN shipping_address_id INTEGER REFERENCES addresses(id);")

            result = await lv.ingest()

            assert result.errors == []
            edges = await lv.edges()
            assert edges[("orders", "invoices", (("invoice_id", "id"),))] == "billing_address"
            assert edges[BILLING] == "orders_billing_address"
            assert edges[SHIPPING] == "shipping_address"

    async def test_sibling_stems_collide(self) -> None:
        async with live(ADDRESSES_ONLY + """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, addr_id INTEGER REFERENCES addresses(id),
                                 addr_fk INTEGER REFERENCES addresses(id));
        """) as lv:
            await lv.ingest()
            edges = await lv.edges()
            assert edges[("orders", "addresses", (("addr_fk", "id"),))] == "addr"
            assert edges[("orders", "addresses", (("addr_id", "id"),))] == "orders_addr"

    async def test_composite_fk_stem_joins_column_stems(self) -> None:
        async with live("""
            CREATE TABLE order_lines (order_id INTEGER, line_no INTEGER, qty INTEGER, PRIMARY KEY (order_id, line_no));
            CREATE TABLE shipments (id INTEGER PRIMARY KEY, order_id INTEGER, line_no INTEGER,
                                    ret_order_id INTEGER, ret_line_no INTEGER,
                                    FOREIGN KEY (order_id, line_no) REFERENCES order_lines(order_id, line_no),
                                    FOREIGN KEY (ret_order_id, ret_line_no) REFERENCES order_lines(order_id, line_no));
        """) as lv:
            await lv.ingest()
            edges = await lv.edges()
            pairs = (("order_id", "order_id"), ("line_no", "line_no"))
            assert edges[("shipments", "order_lines", pairs)] == "order_line_no"

    async def test_scan_order_does_not_change_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        schema = TWO_CYCLE + """
            CREATE TABLE addresses (id INTEGER PRIMARY KEY);
            CREATE TABLE orders (id INTEGER PRIMARY KEY, addr_id INTEGER REFERENCES addresses(id),
                                 addr_fk INTEGER REFERENCES addresses(id));
        """
        async with live(schema) as lv:
            await lv.ingest()
            forward = await lv.edges()

        original = ingestion_mod.list_ingestable_objects
        monkeypatch.setattr(
            ingestion_mod, "list_ingestable_objects",
            lambda **kw: list(reversed(original(**kw))),
        )
        async with live(schema) as lv:
            await lv.ingest()
            assert await lv.edges() == forward
        assert sum(name is not None for name in forward.values()) == 4
