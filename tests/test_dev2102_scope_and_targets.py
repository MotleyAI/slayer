"""Extensions of any dataset a stage reads: scope, resolution order, rejected targets and save-time refusal."""

from __future__ import annotations

from collections import Counter

import pytest

from slayer.core.errors import DatasetNotFoundError, SlayerError, UnknownReferenceError
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.stage_ordering import topologically_order_stages
from slayer.usage_reporting.features import query_features

from tests._dev2102_fixtures import (
    DIALECTS,
    DS,
    TOTAL_AMOUNT,
    TOTAL_CREDIT,
    base_datasets,
    by_key,
    customers,
    ext,
    relationship_to,
    orders,
    q,
    respelled_dataset,
    rows,
    shop,
    with_datasets,
)

TIER_LABEL = {"name": "tier_label", "expression": {"dialects": [{"dialect": "DUCKDB", "expression": "upper(tier)"}, {"dialect": "SQLITE", "expression": "upper(tier)"}]}, "datatype": "String"}
AMOUNT = {"metric": "sum(orders.amount)", "alias": "a"}
CREDIT_TOTAL = {"name": "credit_total", "expression": "sum(credit)"}
COUNT_X = {"name": "x", "expression": "count(*)"}


@pytest.fixture(params=DIALECTS)
def dialect(request) -> str:
    return request.param


def _cust_spend() -> SlayerDataset:
    return SlayerDataset(name="cust_spend", data_source=DS, source_queries=[SlayerQuery.model_validate({
        "population": "orders", "dimensions": ["orders.customer_id"],
        "metrics": [{"metric": "sum(orders.amount)", "alias": "spent"}]})])


class TestExtendingAnyModel:
    async def test_extending_a_joined_model_adds_a_dimension(self, dialect) -> None:
        stored = customers().model_copy(update={"fields": [*customers().fields, DatasetField.model_validate(TIER_LABEL)]})
        computed = q(population="orders", dimensions=[{"field": "upper(customers.tier)", "alias": "tier_label"}],
                     metrics=[AMOUNT])
        async with shop(dialect) as engine:
            got = by_key(await engine.execute(computed), "tier_label", "a")
        async with shop(dialect, with_datasets(stored)) as engine:
            expected = by_key(await engine.execute(q(
                population="orders", dimensions=["customers.tier_label"], metrics=[AMOUNT])),
                "customers.tier_label", "a")
        assert got == expected == {"GOLD": 35.0, "SILVER": 45.0, "BRONZE": 40.0, None: 7.0}

    async def test_extending_a_joined_model_adds_a_measure(self, dialect) -> None:
        async with shop(dialect) as engine:
            extended = await engine.execute(q(
                population="orders", metrics=[{"metric": "customers.credit_total", "alias": "ct"}],
                extensions=[ext("customers", metrics=[{"name": "credit_total", "expression": "sum(credit)"}])]))
            direct = await engine.execute(q(
                population="orders", metrics=[{"metric": "sum(customers.credit)", "alias": "ct"}]))
        assert float(extended.data[0]["ct"]) == float(direct.data[0]["ct"]) == TOTAL_CREDIT

    async def test_extending_the_root_equals_the_legacy_form(self, dialect) -> None:
        joins = [relationship_to("regions", [["ship_region_id", "id"]])]
        fields = {"dimensions": ["regions.name"], "metrics": [AMOUNT]}
        async with shop(dialect) as engine:
            current = await engine.execute(q(population="orders", extensions=[ext("orders", relationships=joins)], **fields))
            with pytest.warns(FutureWarning):
                legacy_query = q(population=ext("orders", relationships=joins), **fields)
            legacy = await engine.execute(legacy_query)
        assert rows(current, *current.columns) == rows(legacy, *legacy.columns)
        assert by_key(current, "regions.name", "a") == {
            "North": 15.0, "South": 35.0, "East": 40.0, None: 37.0}

    async def test_extension_of_a_query_backed_join_target_applies_once(self, dialect) -> None:
        query = q(population="orders", dimensions=["orders.customer_id"],
                  metrics=[AMOUNT, {"metric": "cust_spend.spent2", "alias": "s2"}], extensions=[
            ext("orders", relationships=[relationship_to("cust_spend", [["customer_id", "customer_id"]])]),
            ext("cust_spend", metrics=[{"name": "spent2", "expression": "sum(spent * 2)"}]),
        ])
        async with shop(dialect, with_datasets(_cust_spend())) as engine:
            resp = await engine.execute(query)
        assert by_key(resp, "orders.customer_id", "s2") == {
            1: 60.0, 2: 60.0, 3: 10.0, 4: 80.0, 5: 30.0, None: 14.0}

    async def test_query_backed_measure_extension_in_a_non_root_stage(self, dialect) -> None:
        mm = {"name": "mm", "expression": "sum(spent)"}
        over_model: list[SlayerQuery | dict] = [
            q(name="s", population="cust_spend", dimensions=["cust_spend.customer_id"], metrics=["cust_spend.mm"],
              extensions=[ext("cust_spend", metrics=[mm])]),
            q(population="s", metrics=[{"metric": "sum(s.mm)", "alias": "t"}]),
        ]
        explicit: list[SlayerQuery | dict] = [
            q(name="inner", population="orders", dimensions=["orders.customer_id"],
              metrics=[{"metric": "sum(orders.amount)", "alias": "spent"}]),
            q(name="s", population="inner", dimensions=["inner.customer_id"], metrics=["inner.mm"],
              extensions=[ext("inner", metrics=[mm])]),
            q(population="s", metrics=[{"metric": "sum(s.mm)", "alias": "t"}]),
        ]
        async with shop(dialect, with_datasets(_cust_spend())) as engine:
            got = await engine.execute(over_model)
            expected = await engine.execute(explicit)
        assert float(got.data[0]["t"]) == float(expected.data[0]["t"]) == TOTAL_AMOUNT


class TestScope:
    async def test_extension_is_invisible_to_the_next_stage(self, dialect) -> None:
        stage = q(name="s1", population="orders", dimensions=["orders.status"], metrics=[AMOUNT],
                  extensions=[ext("customers", metrics=[CREDIT_TOTAL])])
        root = q(population="orders", metrics=[{"metric": "customers.credit_total", "alias": "ct"}],
                 extensions=[ext("orders", relationships=[relationship_to("s1", [["status", "status"]])])])
        async with shop(dialect) as engine:
            with pytest.raises(UnknownReferenceError):
                await engine.execute([stage, root])

    async def test_extension_is_invisible_to_a_stored_query_backed_stage(self, dialect) -> None:
        qb = SlayerDataset(name="tl_rev", data_source=DS, source_queries=[SlayerQuery.model_validate({
            "population": "orders", "dimensions": ["orders.status"],
            "metrics": [{"metric": "customers.credit_total", "alias": "a"}]})])
        query = q(population="tl_rev", metrics=[{"metric": "sum(tl_rev.a)", "alias": "t"}],
                  extensions=[ext("customers", metrics=[CREDIT_TOTAL])])
        async with shop(dialect, with_datasets(qb)) as engine:
            with pytest.raises(SlayerError, match="credit_total"):
                await engine.execute(query)

    async def test_extension_is_invisible_to_a_saved_measure(self, dialect) -> None:
        stored = orders().model_copy(update={"metrics": [
            DatasetMetric(name="labels", expression="count_distinct(regions.name)")]})
        query = q(population="orders", metrics=["orders.labels"],
                  extensions=[ext("orders", relationships=[relationship_to("regions", [["ship_region_id", "id"]])])])
        async with shop(dialect, with_datasets(stored)) as engine:
            with pytest.raises(SlayerError, match="regions"):
                await engine.execute(query)

    async def test_a_stage_shadows_a_same_named_model(self, dialect) -> None:
        stage = q(name="customers", population="orders", dimensions=["orders.customer_id"],
                  metrics=[{"metric": "sum(orders.amount)", "alias": "spent"}])
        root = q(population="customers", metrics=[{"metric": "customers.double_spent", "alias": "t"}],
                 extensions=[ext("customers", metrics=[{"name": "double_spent", "expression": "sum(spent * 2)"}])])
        async with shop(dialect) as engine:
            resp = await engine.execute([stage, root])
        assert float(resp.data[0]["t"]) == 2 * TOTAL_AMOUNT

    async def test_a_join_another_model_declares_into_the_sibling_source_is_walkable(self, dialect) -> None:
        stage = q(name="s", population="orders", dimensions=["orders.customer_id"])
        root = q(population="s", dimensions=["s.customer_id", "customers.name"],
                 extensions=[ext("customers", relationships=[relationship_to("s", [["id", "customer_id"]])])])
        async with shop(dialect) as engine:
            resp = await engine.execute([stage, root])
        assert {r["customers.name"] for r in resp.data} == {"Ann", "Bob", "Cid", "Dee", "Eve", None}


class TestModelNameCase:
    """An extension names a dataset as any unquoted reference does: the stored spelling in any case."""

    @pytest.mark.parametrize("spelled", ["Customers", "customers", "CUSTOMERS"])
    async def test_an_extension_reaches_a_model_saved_in_another_case(self, dialect, spelled: str) -> None:
        query = q(population="orders", metrics=[{"metric": "Customers.credit_total", "alias": "ct"}],
                  extensions=[ext(spelled, metrics=[CREDIT_TOTAL])])
        async with shop(dialect, respelled_dataset(base_datasets(), name="customers", spelling="Customers")) as engine:
            resp = await engine.execute(query)
        assert float(resp.data[0]["ct"]) == TOTAL_CREDIT

    async def test_a_stage_joins_a_model_saved_in_another_case(self, dialect) -> None:
        stage = q(name="big_orders", population="orders", dimensions=["orders.id", "orders.customer_id"])
        root = q(population="big_orders", dimensions=["customers.name"],
                 extensions=[ext("big_orders", relationships=[relationship_to("customers", [["customer_id", "id"]])])])
        async with shop(dialect, respelled_dataset(base_datasets(), name="customers", spelling="Customers")) as engine:
            resp = await engine.execute([stage, root])
        assert {r[resp.columns[0]] for r in resp.data} == {"Ann", "Bob", "Cid", "Dee", "Eve", None}

    async def test_a_stage_shadows_a_stored_model_whatever_the_case(self, dialect) -> None:
        stage = q(name="customers", population="addresses", dimensions=["addresses.id", "addresses.city"])
        root = q(population="customers", metrics=[{"metric": "customers.cities", "alias": "t"}],
                 extensions=[ext("CUSTOMERS", metrics=[{"name": "cities", "expression": "count_distinct(upper(city))"}])])
        async with shop(dialect, respelled_dataset(base_datasets(), name="customers", spelling="Customers")) as engine:
            resp = await engine.execute([stage, root])
        assert resp.data[0]["t"] == 4


class TestRejectedTargets:
    @pytest.mark.parametrize("extension", [
        ext("nope", metrics=[COUNT_X]),
        ext("orders", relationships=[relationship_to("nope", [["customer_id", "id"]])]),
    ], ids=["source_name", "join-target"])
    async def test_unknown_extension_target(self, dialect, extension: dict) -> None:
        async with shop(dialect) as engine:
            query = q(population="orders", dimensions=["orders.status"], extensions=[extension])
            with pytest.raises(DatasetNotFoundError, match="nope"):
                await engine.execute(query)

    async def test_extending_the_time_spine_is_rejected(self, dialect) -> None:
        query = q(population="orders", dimensions=["orders.status"],
                  extensions=[ext("time_spine", metrics=[COUNT_X])])
        async with shop(dialect) as engine:
            with pytest.raises(SlayerError, match="time_spine") as info:
                await engine.execute(query)
        assert not isinstance(info.value, DatasetNotFoundError)


class TestSaveTimeRefusal:
    @pytest.mark.parametrize(("extension", "named"), [
        (ext("orders", relationships=[relationship_to("addresses", [["ship_region_id", "id"]], name="billing_address")]),
         "billing_address"),
        (ext("nope", metrics=[COUNT_X]), "nope"),
        (ext("events", relationships=[relationship_to("users", [["user_id", "id"]], join_type="inner")]), "events"),
    ], ids=["edge-conflict", "unresolvable", "unreachable-inner"])
    async def test_save_refuses_a_stage_whose_extension_always_fails(self, dialect, extension, named) -> None:
        stages = [
            q(name="conflicted", population="orders", dimensions=["orders.status"], metrics=[AMOUNT],
              extensions=[extension]),
            q(population="conflicted", metrics=[{"metric": "sum(conflicted.a)", "alias": "t"}]),
        ]
        async with shop(dialect) as engine:
            with pytest.raises(ValueError) as info:
                await engine.create_dataset_from_query(query=stages, name="bad_qb")
            assert await engine.storage.get_dataset("bad_qb", data_source=DS) is None
        message = str(info.value)
        assert "conflicted" in message, message
        assert named in message, message


class TestStageOrdering:
    def test_a_stage_follows_every_sibling_its_extensions_read(self) -> None:
        a = q(name="a", population="orders", dimensions=["orders.customer_id"])
        c = q(name="c", population="customers", dimensions=["customers.id"])
        d = q(name="d", population="customers", dimensions=["customers.id"])
        e = q(name="e", population="orders", dimensions=["orders.status"])
        b = q(name="b", population="a", extensions=[
            ext("a", relationships=[relationship_to("c", [["customer_id", "id"]]), relationship_to("d", [["customer_id", "id"]])]),
            ext("e", metrics=[COUNT_X]),
        ])
        ordered = [s.name for s in topologically_order_stages([b, e, d, c, a, q(population="b")])]
        assert all(ordered.index(name) < ordered.index("b") for name in "acde"), ordered

    @pytest.mark.parametrize("order", [("b", "c", "a"), ("c", "a", "b"), ("a", "b", "c")])
    async def test_a_stage_joining_siblings_through_its_extensions_executes(self, dialect, order) -> None:
        stages = {
            "a": q(name="a", population="orders", dimensions=["orders.customer_id"],
                   metrics=[{"metric": "sum(orders.amount)", "alias": "spent"}]),
            "c": q(name="c", population="customers", dimensions=["customers.id"],
                   metrics=[{"metric": "max(customers.tier)", "alias": "top_tier"}]),
            "b": q(name="b", population="a", dimensions=["c.top_tier"], metrics=[{"metric": "sum(a.spent)", "alias": "t"}],
                   extensions=[ext("a", relationships=[relationship_to("c", [["customer_id", "id"]])])]),
        }
        root = q(population="b", metrics=[{"metric": "sum(b.t)", "alias": "total"}])
        async with shop(dialect) as engine:
            resp = await engine.execute([*(stages[n] for n in order), root])
        assert float(resp.data[0]["total"]) == TOTAL_AMOUNT


class TestUsageFeatures:
    @pytest.mark.parametrize("source_name", ["orders", "customers"], ids=["root", "joined"])
    def test_extensions_count_as_inline_sources_with_their_measures(self, source_name: str) -> None:
        found = query_features({
            "population": "orders", "metrics": ["count(orders.*)"],
            "extensions": [{"dataset": source_name, "metrics": [{"name": "m2", "expression": "max(round(credit))"}]}],
        })
        assert "inline_source" in found.flags
        assert Counter(found.functions) == Counter(["round"])
        assert Counter(found.aggregations) == Counter(["count", "max"])

    def test_a_plain_query_is_not_an_inline_source(self) -> None:
        assert "inline_source" not in query_features({"population": "orders", "metrics": ["count(orders.*)"]}).flags
