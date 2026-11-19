"""Anchor routing: a reference's first segment is reached by its only to-one path, else its only route.

Specs: queries/references › The anchor is reached by its only to-one path; queries/dotted-dimension-routing;
models/join-traversal; queries/unrelated-datasets.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import PopulationInferenceError, SlayerError, UnresolvableDimensionRelationshipError
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev1954_fixtures import POP_MAX_BY_RNAME, by_key, make_exec_engine, table_count
from tests._dev2102_fixtures import ext, join, q, rows, shop
from tests._dev2277_fixtures import (
    BACKENDS,
    CUSTOMERS,
    ORDERS,
    RETURNS,
    add_other_datasource,
    customers_dataset,
    cycle_engine,
    hidden,
    orders_dataset,
    returns_dataset,
    scoped,
    seed_engine,
    table,
)
from tests._engine_helpers import seeded_exec_engine
from tests._unrelated_datasets_fixtures import main_engine

_M2O = RelationshipCardinality.MANY_TO_ONE


async def _raises(engine: SlayerQueryEngine, body: Any, exc: type[Exception] = SlayerError) -> Exception:
    with pytest.raises(exc) as info:
        await engine.execute(body)
    return info.value


# --------------------------------------------------------------------------- #
# Chain schema: Invoice → Subscription → Customer → Consumer; Payment → Invoice, Payment → Method
# --------------------------------------------------------------------------- #
_CHAIN_TABLES = [
    TableSpec(name="Consumer", columns=[("id", "INT"), ("name", "TEXT"), ("amount", "DOUBLE")],
              rows=[(1, "Ann", 5.0), (2, "Bob", 7.0)]),
    TableSpec(name="Customer", columns=[("id", "INT"), ("consumerId", "INT"), ("name", "TEXT")],
              rows=[(1, 1, "c1"), (2, 2, "c2")]),
    TableSpec(name="Subscription", columns=[("id", "INT"), ("customerId", "INT")], rows=[(1, 1), (2, 2)]),
    TableSpec(name="Invoice", columns=[("id", "INT"), ("subscriptionId", "INT"), ("customerId", "INT"),
                                       ("amount", "DOUBLE")],
              rows=[(1, 1, 1, 10.0), (2, 1, 1, 20.0), (3, 2, 2, 30.0)]),
    TableSpec(name="Payment", columns=[("id", "INT"), ("invoiceId", "INT"), ("methodId", "INT")],
              rows=[(1, 1, 1), (2, 3, 2)]),
    TableSpec(name="Method", columns=[("id", "INT"), ("kind", "TEXT")], rows=[(1, "card"), (2, "cash")]),
]


def _fields(*spec: tuple[str, DataType]) -> list[DatasetField]:
    return [DatasetField(name=n, datatype=t, primary_key=(n == "id")) for n, t in spec]


def _to(target: str, column: str) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[column], to_columns=["id"], cardinality=_M2O)


def _chain_datasets(*, direct_customer: bool = False) -> list[SlayerDataset]:
    i, f, s = DataType.INTEGER, DataType.FLOAT, DataType.STRING
    invoice_rels = [_to("Subscription", "subscriptionId")]
    if direct_customer:
        invoice_rels.append(_to("Customer", "customerId"))
    return [
        SlayerDataset(name="Consumer", sql_table="Consumer", data_source="test",
                      fields=_fields(("id", i), ("name", s), ("amount", f)),
                      metrics=[DatasetMetric(name="aov", expression="sum(amount) / count(*)", datatype=DataType.FLOAT)]),
        SlayerDataset(name="Customer", sql_table="Customer", data_source="test",
                      fields=_fields(("id", i), ("consumerId", i), ("name", s)),
                      relationships=[_to("Consumer", "consumerId")]),
        SlayerDataset(name="Subscription", sql_table="Subscription", data_source="test",
                      fields=_fields(("id", i), ("customerId", i)), relationships=[_to("Customer", "customerId")]),
        SlayerDataset(name="Invoice", sql_table="Invoice", data_source="test",
                      fields=_fields(("id", i), ("subscriptionId", i), ("customerId", i), ("amount", f)),
                      relationships=invoice_rels),
        SlayerDataset(name="Payment", sql_table="Payment", data_source="test",
                      fields=_fields(("id", i), ("invoiceId", i), ("methodId", i)),
                      relationships=[_to("Invoice", "invoiceId"), _to("Method", "methodId")]),
        SlayerDataset(name="Method", sql_table="Method", data_source="test", fields=_fields(("id", i), ("kind", s))),
    ]


@contextlib.asynccontextmanager
async def _chain_engine(backend: str, *, direct_customer: bool = False) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, _CHAIN_TABLES),
        datasets=_chain_datasets(direct_customer=direct_customer),
    ) as (engine, _db):
        yield engine


def _invoice(**body: Any) -> dict[str, Any]:
    return {"population": "Invoice", **body}


@pytest.fixture(params=BACKENDS)
def backend(request) -> str:
    return request.param


# --------------------------------------------------------------------------- #
# The anchor is reached by its only to-one path (seed schema)
# --------------------------------------------------------------------------- #
class TestToOnePathRule:
    async def test_one_to_one_path_wins_over_fanning_routes(self, backend: str) -> None:
        async with seed_engine(backend, managers=True) as eng:
            resp = await eng.execute({"population": "orders", "dimensions": ["customers.region"],
                                      "metrics": ["count(orders.*)"]})
        assert resp.columns == ["customers.region", "orders._count"]
        assert table(resp, "customers.region", "orders._count") == {("EAST", 3.0), ("WEST", 1.0)}

    async def test_two_to_one_paths_are_refused_even_with_a_direct_relationship(self, backend: str) -> None:
        async with seed_engine(backend, stores=True) as eng:
            err = await _raises(eng, {"population": "orders", "dimensions": ["customers.region"]},
                                UnresolvableDimensionRelationshipError)
        assert "orders.customers.region" in str(err)
        assert "orders.stores.customers.region" in str(err)

    async def test_the_explicit_route_resolves_the_ambiguity(self, backend: str) -> None:
        async with seed_engine(backend, stores=True) as eng:
            resp = await eng.execute({"population": "orders", "dimensions": ["orders.customers.region"],
                                      "metrics": ["count(orders.*)"]})
        assert resp.columns == ["orders.customers.region", "orders._count"]
        assert table(resp, "orders.customers.region", "orders._count") == {("EAST", 3.0), ("WEST", 1.0)}

    async def test_a_unique_fanning_route_is_taken(self, backend: str) -> None:
        async with seed_engine(backend) as eng:
            resp = await eng.execute({"population": "customers", "dimensions": ["customers.customer_id"],
                                      "metrics": ["sum(orders.amount)"]})
        assert resp.columns == ["customers.customer_id", "orders.amount_sum"]
        assert table(resp, "customers.customer_id", "orders.amount_sum") == {
            ("C1", 300.0), ("C2", 50.0), ("C3", 300.0), ("C4", None)}

    async def test_several_fanning_routes_are_refused(self, backend: str) -> None:
        async with seed_engine(backend, managers=True) as eng:
            err = await _raises(eng, {"population": "customers", "metrics": ["sum(orders.amount)"]},
                                UnresolvableDimensionRelationshipError)
        assert "customers.orders.amount" in str(err)
        assert "customers.managers.orders.amount" in str(err)

    async def test_an_adjacent_to_one_edge_wins_over_an_adjacent_fanning_edge(self, backend: str) -> None:
        fanning = DatasetRelationship(to="customers", name="managed", from_columns=["manager_id"],
                                      to_columns=["manager_id"])
        base = orders_dataset(buyer=True)
        orders = base.model_copy(update={"relationships": [*base.relationships, fanning]})
        datasets = [customers_dataset(), orders, returns_dataset()]
        async with seed_engine(backend, datasets=datasets) as eng:
            resp = await eng.execute({"population": "orders", "dimensions": ["customers.region"],
                                      "metrics": ["count(orders.*)"]})
        assert table(resp, "customers.region", "orders._count") == {("EAST", 3.0), ("WEST", 1.0)}

    async def test_adjacent_parallel_to_one_edges_stay_ambiguous(self, backend: str) -> None:
        async with seed_engine(backend, buyer=True, seller=True) as eng:
            err = await _raises(eng, {"population": "orders", "dimensions": ["customers.region"]},
                                UnresolvableDimensionRelationshipError)
        assert "orders.buyer.region" in str(err)
        assert "orders.seller.region" in str(err)


class TestDirectedCycle:
    async def test_the_anchor_takes_the_to_one_path(self, backend: str) -> None:
        async with cycle_engine(backend) as eng:
            resp = await eng.execute({"population": "x", "dimensions": ["x.v"],
                                      "metrics": ["sum(z.amount)", "sum(x.z.amount)"]})
        assert resp.columns == ["x.v", "z.amount_sum", "x.z.amount_sum"]
        assert table(resp, "x.v", "z.amount_sum", "x.z.amount_sum") == {("x1", 10.0, None), ("x2", 20.0, 30.0)}

    async def test_a_rootless_query_across_the_cycle_fails_closed(self, backend: str) -> None:
        body = {"dimensions": ["x.v", "y.v", "z.v"]}
        async with cycle_engine(backend) as eng:
            await _raises(eng, body, PopulationInferenceError)
            resp = await eng.execute({"population": "x", **body})
        assert table(resp, "x.v", "y.v", "z.v") == {("x1", "y1", "z1"), ("x2", "y2", "z2")}


# --------------------------------------------------------------------------- #
# Which first segments are anchors
# --------------------------------------------------------------------------- #
_EVENTS = TableSpec(name="events", columns=[("id", "INT")], rows=[(1,), (2,), (3,)])


class TestAnchors:
    async def test_no_route_makes_the_anchor_unrelated(self, backend: str) -> None:
        events = SlayerDataset(name="events", sql_table="events", data_source="test",
                               fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)])
        tables = [CUSTOMERS, ORDERS, RETURNS, _EVENTS]
        datasets = [customers_dataset(), orders_dataset(), returns_dataset(), events]
        async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, tables),
                                      datasets=datasets) as (eng, _db):
            resp = await eng.execute({"population": "customers",
                                      "metrics": ["count(customers.*)", "count(events.*)"]})
        assert resp.columns == ["customers._count", "events._count"]
        assert table(resp, "customers._count", "events._count") == {(4.0, 3.0)}

    async def test_a_dataset_of_another_datasource_is_an_unknown_dataset(self, backend: str) -> None:
        async with seed_engine(backend) as eng:
            await add_other_datasource(eng, dataset="events")
            other = await _raises(eng, {"population": "customers", "metrics": ["count(events.*)"]})
            unknown = await _raises(eng, {"population": "customers", "metrics": ["count(zzevents.*)"]})
        assert type(other) is type(unknown)
        assert str(other) == str(unknown).replace("zzevents", "events")

    @pytest.mark.parametrize("root", [{"population": "customers"}, {}], ids=["rooted", "rootless"])
    async def test_a_hidden_dataset_is_reported_as_unknown(self, backend: str, root: dict[str, Any]) -> None:
        datasets = [customers_dataset(), orders_dataset(), hidden(returns_dataset())]
        async with seed_engine(backend, datasets=datasets) as eng, scoped(eng) as caller:
            hid = await _raises(caller, {**root, "dimensions": ["returns.amount"]})
            unknown = await _raises(caller, {**root, "dimensions": ["zzreturns.amount"]})
        assert type(hid) is type(unknown)
        assert str(hid) == str(unknown).replace("zzreturns", "returns")

    async def test_an_unquoted_first_segment_matches_case_insensitively(self, backend: str) -> None:
        async with seed_engine(backend) as eng:
            resp = await eng.execute({"population": "orders", "dimensions": ["Customers.region"],
                                      "metrics": ["count(orders.*)"]})
        region = resp.columns[0]
        assert region.lower() == "customers.region"
        assert table(resp, region, "orders._count") == {("EAST", 3.0), ("WEST", 1.0)}

    async def test_a_quoted_first_segment_matches_only_exactly(self, backend: str) -> None:
        async with seed_engine(backend) as eng:
            quoted = await _raises(eng, {"population": "orders", "dimensions": ['"Customers".region']})
            unknown = await _raises(eng, {"population": "orders", "dimensions": ['"Nosuch".region']})
        assert type(quoted) is type(unknown)
        assert "Customers" in str(quoted)

    async def test_a_stage_precedes_a_same_named_dataset(self, backend: str) -> None:
        stages: list[Any] = [
            {"name": "customers", "population": "orders", "dimensions": ["orders.status"],
             "metrics": [{"metric": "count(orders.*)", "alias": "n"}]},
            {"population": "customers", "dimensions": ["customers.status"],
             "metrics": [{"metric": "sum(customers.n)", "alias": "t"}]},
        ]
        async with seed_engine(backend) as eng:
            resp = await eng.execute(stages)
        assert resp.columns == ["customers.status", "t"]
        assert table(resp, "customers.status", "t") == {("completed", 2.0), ("open", 2.0)}

    async def test_a_stage_name_anchors_its_columns(self, backend: str) -> None:
        stages: list[Any] = [
            {"name": "s1", "population": "orders", "dimensions": ["orders.status"],
             "metrics": [{"metric": "sum(orders.amount)", "alias": "tot"}]},
            {"population": "s1", "dimensions": ["s1.status", "s1.tot"]},
        ]
        async with seed_engine(backend) as eng:
            resp = await eng.execute(stages)
        assert table(resp, "s1.status", "s1.tot") == {("completed", 150.0), ("open", 500.0)}


# --------------------------------------------------------------------------- #
# queries/dotted-dimension-routing (chain schema)
# --------------------------------------------------------------------------- #
class TestDottedDimensionRouting:
    async def test_a_short_form_with_one_route_keys_by_its_anchor(self, backend: str) -> None:
        full_path = "Invoice.Subscription.Customer.Consumer.name"
        async with _chain_engine(backend) as eng:
            short = await eng.execute(_invoice(dimensions=["Consumer.name"], metrics=["sum(Invoice.amount)"]))
            full = await eng.execute(_invoice(dimensions=[full_path], metrics=["sum(Invoice.amount)"]))
        assert short.columns == ["Consumer.name", "Invoice.amount_sum"]
        assert full.columns == [full_path, "Invoice.amount_sum"]
        expected = {("Ann", 30.0), ("Bob", 30.0)}
        assert table(short, "Consumer.name", "Invoice.amount_sum") == expected
        assert table(full, full_path, "Invoice.amount_sum") == expected
        for physical in ("Subscription", "Customer", "Consumer"):
            assert table_count(short.sql or "", physical, dialect=backend) == \
                table_count(full.sql or "", physical, dialect=backend)

    async def test_a_unique_route_through_a_reverse_hop(self, backend: str) -> None:
        async with _chain_engine(backend) as eng:
            short = await eng.execute(_invoice(dimensions=["Invoice.id", "Method.kind"]))
            full = await eng.execute(_invoice(dimensions=["Invoice.id", "Invoice.Payment.Method.kind"]))
        assert short.columns == ["Invoice.id", "Method.kind"]
        expected = {(1.0, "card"), (2.0, None), (3.0, "cash")}
        assert table(short, "Invoice.id", "Method.kind") == expected
        assert table(full, "Invoice.id", "Invoice.Payment.Method.kind") == expected

    async def test_a_direct_relationship_has_no_precedence(self, backend: str) -> None:
        async with _chain_engine(backend, direct_customer=True) as eng:
            err = await _raises(eng, _invoice(dimensions=["Customer.name"]), UnresolvableDimensionRelationshipError)
        assert isinstance(err, UnresolvableDimensionRelationshipError)
        assert err.suggested_path == "Invoice.Customer.name"

    async def test_an_ambiguous_suggestion_starts_at_the_population(self, backend: str) -> None:
        async with _chain_engine(backend, direct_customer=True) as eng:
            err = await _raises(eng, _invoice(dimensions=["Consumer.name"]), UnresolvableDimensionRelationshipError)
        assert isinstance(err, UnresolvableDimensionRelationshipError)
        assert err.suggested_path == "Invoice.Customer.Consumer.name"

    async def test_a_broken_chain_from_the_population_suggests_the_short_form(self, backend: str) -> None:
        async with _chain_engine(backend) as eng:
            err = await _raises(eng, _invoice(dimensions=["Invoice.Customer.Consumer.name"]),
                                UnresolvableDimensionRelationshipError)
        assert isinstance(err, UnresolvableDimensionRelationshipError)
        assert err.suggested_path == "Consumer.name"

    async def test_routed_aggregates_key_by_their_anchor(self, backend: str) -> None:
        async with _chain_engine(backend) as eng:
            resp = await eng.execute(_invoice(metrics=["sum(Consumer.amount)", "count(Consumer.*)"]))
        assert resp.columns == ["Consumer.amount_sum", "Consumer._count"]

    async def test_a_routed_saved_metric_keys_by_its_anchor_and_keeps_its_type(self, backend: str) -> None:
        async with _chain_engine(backend) as eng:
            resp = await eng.execute(_invoice(metrics=["Consumer.aov"]))
            saved = await eng.save_dataset(SlayerDataset(name="qb", data_source="test", source_queries=[
                SlayerQuery.model_validate(_invoice(metrics=["Consumer.aov"]))]))
        assert resp.columns == ["Consumer.aov"]
        assert [c.datatype for c in saved.fields] == [DataType.FLOAT]


class TestJoinedStageFlatColumns:
    _STAGE = {"name": "s", "population": "orders", "dimensions": ["orders.customer_id", "customers.name"],
              "metrics": [{"metric": "count(orders.*)", "alias": "n"}]}

    @pytest.mark.parametrize("root", ["addresses", "customers"])
    async def test_a_joined_stage_column_keys_by_the_stage(self, backend: str, root: str) -> None:
        pairs = [["customer_id", "customer_id"]] if root == "addresses" else [["id", "customer_id"]]
        payload: list[Any] = [q(**self._STAGE), q(population=root, dimensions=[f"{root}.id", "s.customers__name"],
                                       extensions=[ext(root, relationships=[join("s", pairs)])])]
        async with shop(backend) as eng:
            resp = await eng.execute(payload)
        assert resp.columns == [f"{root}.id", "s.customers__name"]
        assert rows(resp, f"{root}.id", "s.customers__name")


# --------------------------------------------------------------------------- #
# models/join-traversal (regions edge named ``hr``)
# --------------------------------------------------------------------------- #
@pytest.fixture(params=BACKENDS)
async def hr_engine(request):
    async for eng in make_exec_engine(request):
        yield eng


class TestNamedEdgeKeys:
    async def test_a_named_edge_typed_by_its_model_name_keys_under_the_edge_name(
            self, hr_engine: SlayerQueryEngine) -> None:
        resp = await hr_engine.execute({"population": "orders", "dimensions": ["customers.regions.rname"],
                                        "metrics": ["max(customers.regions.pop)"]})
        assert resp.columns == ["customers.hr.rname", "customers.hr.pop_max"]
        assert by_key(resp, "customers.hr.rname", "customers.hr.pop_max") == POP_MAX_BY_RNAME
        canon = await hr_engine.execute({"population": "orders", "dimensions": ["customers.hr.rname"],
                                         "metrics": ["max(customers.hr.pop)"]})
        assert resp.attributes == canon.attributes

    async def test_an_auto_routed_short_form_through_a_named_edge(self, hr_engine: SlayerQueryEngine) -> None:
        short: SlayerResponse = await hr_engine.execute({"population": "orders", "dimensions": ["regions.rname"]})
        full = await hr_engine.execute({"population": "orders", "dimensions": ["orders.customers.hr.rname"]})
        assert short.columns == ["regions.rname"]
        assert {r["regions.rname"] for r in short.data} == {r["orders.customers.hr.rname"] for r in full.data}
        assert "customers__hr" in (short.sql or "")


# --------------------------------------------------------------------------- #
# queries/unrelated-datasets (keys follow the anchor or alias)
# --------------------------------------------------------------------------- #
class TestUnrelatedDatasetKeys:
    async def test_a_grand_total_ratio_keys_by_its_alias(self, backend: str) -> None:
        async with main_engine(backend) as eng:
            resp = await eng.execute({"population": "customers", "metrics": [
                {"metric": "count(events.id) / count(customers.*)", "alias": "events_per_customer"}]})
        assert resp.columns == ["events_per_customer"]
        assert table(resp, "events_per_customer") == {(5 / 3,)}

    async def test_a_path_past_the_product_hop_keys_by_its_anchor(self, backend: str) -> None:
        async with main_engine(backend) as eng:
            crossed = await eng.execute({"population": "customers", "metrics": ["count(events.users.id)"]})
            direct = await eng.execute({"population": "events", "metrics": ["count(users.id)"]})
        assert crossed.columns == ["events.users.id_count"]
        assert direct.columns == ["users.id_count"]
        assert table(crossed, "events.users.id_count") == table(direct, "users.id_count")

    async def test_a_query_local_stage_is_an_unrelated_dataset(self, backend: str) -> None:
        stages: list[Any] = [
            {"name": "c", "population": "customers", "metrics": [{"metric": "count(customers.*)", "alias": "nc"}]},
            {"name": "e", "population": "events", "metrics": [{"metric": "count(events.*)", "alias": "ne"}]},
            {"population": "c", "dimensions": ["c.nc"], "metrics": [{"metric": "max(e.ne)", "alias": "ne"}]},
        ]
        async with main_engine(backend) as eng:
            resp = await eng.execute(stages)
        assert resp.columns == ["c.nc", "ne"]
        assert table(resp, "c.nc", "ne") == {(3.0, 5.0)}
