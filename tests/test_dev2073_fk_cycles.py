"""FK cycles ingest verbatim; one edge per FK relationship; FK-reflection failure loses only joins."""

from __future__ import annotations

import pytest
from sqlalchemy.engine.reflection import Inspector

from slayer.core.models import ModelJoin

from tests._dev1853_fixtures import rows_set
from tests._dev2073_fixtures import (
    CHAIN,
    LATEST_CHILD,
    MUTUAL_PK,
    SALES_CHAIN,
    THREE_CYCLE,
    TWO_CYCLE,
    live,
)


class TestCyclesIngestVerbatim:
    async def test_two_cycle_keeps_both_fks_and_every_other_join(self) -> None:
        async with live(TWO_CYCLE) as lv:
            result = await lv.ingest()
            assert result.errors == []
            edges = await lv.edges()
            assert set(edges) == {
                ("transactions", "coupon_usages", (("coupon_usage_id", "id"),)),
                ("coupon_usages", "transactions", (("transaction_id", "id"),)),
                ("sales", "stores", (("store_id", "id"),)),
            }

    async def test_two_cycle_forward_edge_reads_the_referenced_row(self) -> None:
        async with live(TWO_CYCLE) as lv:
            await lv.ingest()
            resp = await lv.query(source_model="transactions", dimensions=["id", "coupon_usage.code"])
            assert rows_set(resp, "transactions.id", "transactions.coupon_usage.code") == {
                (1, "A"), (2, "B"), (3, None),
            }

    async def test_two_cycle_reverse_edge_reads_the_referencing_rows(self) -> None:
        async with live(TWO_CYCLE) as lv:
            await lv.ingest()
            resp = await lv.query(source_model="transactions", dimensions=["id", "transaction.code"])
            assert rows_set(resp, "transactions.id", "transactions.transaction.code") == {
                (1, "B"), (2, "A"), (3, "C"),
            }

    async def test_unrelated_chain_is_intact_in_both_directions(self) -> None:
        async with live(TWO_CYCLE) as lv:
            await lv.ingest()
            sales = await lv.model("sales")
            assert [j.name for j in sales.joins] == [None]
            resp = await lv.query(source_model="sales", dimensions=["id", "stores.name"])
            assert rows_set(resp, "sales.id", "sales.stores.name") == {
                (1, "North"), (2, "North"), (3, "South"),
            }
            resp = await lv.query(
                source_model="stores", dimensions=["name"],
                measures=[{"formula": "sales.total:sum", "name": "t"}],
            )
            assert {r["stores.name"]: r["stores.t"] for r in resp.data} == {"North": 25.0, "South": 4.0}

    async def test_three_cycle_keeps_every_edge(self) -> None:
        async with live(THREE_CYCLE) as lv:
            await lv.ingest()
            assert set(await lv.edges()) == {
                ("employees", "depts", (("dept_id", "id"),)),
                ("depts", "offices", (("office_id", "id"),)),
                ("offices", "employees", (("manager_id", "id"),)),
            }
            resp = await lv.query(source_model="employees", dimensions=["name", "depts.offices.city"])
            assert rows_set(resp, "employees.name", "employees.depts.offices.city") == {
                ("Ann", "Paris"), ("Ben", "Rome"), ("Cy", "Paris"),
            }

    async def test_three_cycle_reverse_chain_aggregates(self) -> None:
        async with live(THREE_CYCLE) as lv:
            await lv.ingest()
            resp = await lv.query(
                source_model="offices", dimensions=["city"],
                measures=[{"formula": "depts.employees.salary:sum", "name": "s"}],
            )
            assert {r["offices.city"]: r["offices.s"] for r in resp.data} == {"Paris": 150.0, "Rome": 80.0}

    async def test_latest_child_pointer_keeps_the_main_fk(self) -> None:
        async with live(LATEST_CHILD) as lv:
            await lv.ingest()
            resp = await lv.query(source_model="orders", dimensions=["id", "customer.name"])
            assert rows_set(resp, "orders.id", "orders.customer.name") == {
                (1, "Alice"), (2, "Alice"), (3, "Bob"),
            }


class TestOneEdgePerFkRelationship:
    async def test_mutual_pk_fks_ingest_as_one_edge(self) -> None:
        async with live(MUTUAL_PK) as lv:
            result = await lv.ingest()
            assert result.errors == []
            models = await lv.models()
            assert {"users", "profiles"} <= set(models)
            # Both halves one_to_one: the deterministic tie-break keeps the profiles half.
            assert set(await lv.edges()) == {("profiles", "users", (("user_id", "id"),))}

    async def test_inverse_pair_keeps_the_to_one_half(self) -> None:
        async with live("""
            CREATE TABLE badges (id INTEGER PRIMARY KEY REFERENCES users(badge_id), label TEXT);
            CREATE TABLE users (id INTEGER PRIMARY KEY, badge_id INTEGER REFERENCES badges(id), name TEXT);
        """) as lv:
            result = await lv.ingest()
            assert result.errors == []
            assert set(await lv.edges()) == {("users", "badges", (("badge_id", "id"),))}

    async def test_mutual_pk_fks_reingest_cleanly(self) -> None:
        async with live(MUTUAL_PK) as lv:
            await lv.ingest()
            before = await lv.edges()
            result = await lv.ingest()
            assert result.errors == []
            assert all(not a.new_joins for a in result.additions)
            assert {"users", "profiles"} <= set(await lv.models())
            assert len(before) == 1
            assert await lv.edges() == before

    @pytest.mark.parametrize("name", [None, "placed_orders"])
    async def test_fk_stored_in_reverse_orientation_is_not_readded(self, name: str | None) -> None:
        async with live(CHAIN) as lv:
            await lv.ingest()
            orders = await lv.model("orders")
            await lv.storage.save_model(orders.model_copy(update={"joins": []}))
            customers = await lv.model("customers")
            reverse = ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]], name=name)
            await lv.storage.save_model(customers.model_copy(update={"joins": [reverse]}))

            result = await lv.ingest()

            assert result.errors == []
            assert (await lv.model("orders")).joins == []
            assert [(j.target_model, j.name) for j in (await lv.model("customers")).joins] == [("orders", name)]


class TestNestedFieldColumns:
    async def test_struct_subfields_are_not_table_columns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original = Inspector.get_columns

        def with_subfield(self, table_name, schema=None, **kw):
            cols = original(self, table_name, schema=schema, **kw)
            # BigQuery reflects a STRUCT's subfields as extra ``parent.child`` columns.
            return [*cols, {**cols[-1], "name": f"{cols[-1]['name']}.city"}] if table_name == "orders" else cols

        monkeypatch.setattr(Inspector, "get_columns", with_subfield)
        async with live(CHAIN) as lv:
            for _ in range(2):
                result = await lv.ingest()
                assert result.errors == []
                assert result.skipped == []
            assert {c.name for c in (await lv.model("orders")).columns} == {"id", "customer_id", "amount"}


class TestFkReflectionFailure:
    async def test_failing_table_ingests_without_joins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original = Inspector.get_foreign_keys

        def flaky(self, table_name, schema=None, **kw):
            if table_name == "orders":
                raise RuntimeError("FK reflection unavailable")
            return original(self, table_name, schema=schema, **kw)

        monkeypatch.setattr(Inspector, "get_foreign_keys", flaky)
        async with live(CHAIN + SALES_CHAIN) as lv:
            result = await lv.ingest()
            assert all(s.table_name != "orders" for s in result.skipped)
            orders = await lv.model("orders")
            assert {c.name for c in orders.columns} >= {"id", "customer_id", "amount"}
            assert orders.joins == []
            assert ("sales", "stores", (("store_id", "id"),)) in await lv.edges()
