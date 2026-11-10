"""Extensions of any model a stage reads: scope, resolution order, rejected targets and save-time refusal."""

from __future__ import annotations

from collections import Counter

import pytest

from slayer.core.errors import ModelNotFoundError, SlayerError, UnknownReferenceError
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.stage_ordering import topologically_order_stages
from slayer.usage_reporting.features import query_features

from tests._dev2102_fixtures import (
    DIALECTS,
    DS,
    TOTAL_AMOUNT,
    TOTAL_CREDIT,
    by_key,
    customers,
    ext,
    join,
    orders,
    q,
    rows,
    shop,
    with_models,
)

TIER_LABEL = {"name": "tier_label", "sql": "upper(tier)", "type": "TEXT"}
AMOUNT = {"formula": "sum(amount)", "name": "a"}


@pytest.fixture(params=DIALECTS)
def dialect(request) -> str:
    return request.param


def _cust_spend() -> SlayerModel:
    return SlayerModel(name="cust_spend", data_source=DS, source_queries=[SlayerQuery.model_validate({
        "source_model": "orders", "dimensions": ["customer_id"],
        "measures": [{"formula": "sum(amount)", "name": "spent"}]})])


class TestExtendingAnyModel:
    async def test_extending_a_joined_model_adds_a_dimension(self, dialect) -> None:
        stored = customers().model_copy(update={"columns": [*customers().columns, Column.model_validate(TIER_LABEL)]})
        extended = q(source_model="orders", dimensions=["customers.tier_label"], measures=[AMOUNT],
                     extensions=[ext("customers", columns=[TIER_LABEL])])
        async with shop(dialect) as engine:
            got = by_key(await engine.execute(extended), "orders.customers.tier_label", "orders.a")
        async with shop(dialect, with_models(stored)) as engine:
            expected = by_key(await engine.execute(q(
                source_model="orders", dimensions=["customers.tier_label"], measures=[AMOUNT])),
                "orders.customers.tier_label", "orders.a")
        assert got == expected == {"GOLD": 35.0, "SILVER": 45.0, "BRONZE": 40.0, None: 7.0}

    async def test_extending_a_joined_model_adds_a_measure(self, dialect) -> None:
        async with shop(dialect) as engine:
            extended = await engine.execute(q(
                source_model="orders", measures=[{"formula": "customers.credit_total", "name": "ct"}],
                extensions=[ext("customers", measures=[{"name": "credit_total", "formula": "sum(credit)"}])]))
            direct = await engine.execute(q(
                source_model="orders", measures=[{"formula": "sum(customers.credit)", "name": "ct"}]))
        assert float(extended.data[0]["orders.ct"]) == float(direct.data[0]["orders.ct"]) == TOTAL_CREDIT

    async def test_extending_the_root_equals_the_legacy_form(self, dialect) -> None:
        joins = [join("regions", [["ship_region_id", "id"]])]
        fields = {"dimensions": ["regions.name"], "measures": [AMOUNT]}
        async with shop(dialect) as engine:
            current = await engine.execute(q(source_model="orders", extensions=[ext("orders", joins=joins)], **fields))
            with pytest.warns(FutureWarning):
                legacy_query = q(source_model=ext("orders", joins=joins), **fields)
            legacy = await engine.execute(legacy_query)
        assert rows(current, *current.columns) == rows(legacy, *legacy.columns)
        assert by_key(current, "orders.regions.name", "orders.a") == {
            "North": 15.0, "South": 35.0, "East": 40.0, None: 37.0}

    async def test_extension_of_a_query_backed_join_target_applies_once(self, dialect) -> None:
        query = q(source_model="orders", dimensions=["cust_spend.spent2"], measures=[AMOUNT], extensions=[
            ext("orders", joins=[join("cust_spend", [["customer_id", "customer_id"]])]),
            ext("cust_spend", columns=[{"name": "spent2", "sql": "spent * 2", "type": "DOUBLE"}]),
        ])
        async with shop(dialect, with_models(_cust_spend())) as engine:
            resp = await engine.execute(query)
        assert by_key(resp, "orders.cust_spend.spent2", "orders.a") == {
            60.0: 60.0, 10.0: 5.0, 80.0: 40.0, 30.0: 15.0, None: 7.0}

    async def test_query_backed_measure_extension_in_a_non_root_stage(self, dialect) -> None:
        mm = {"name": "mm", "formula": "sum(spent)"}
        over_model: list[SlayerQuery | dict] = [
            q(name="s", source_model="cust_spend", dimensions=["customer_id"], measures=["mm"],
              extensions=[ext("cust_spend", measures=[mm])]),
            q(source_model="s", measures=[{"formula": "sum(mm)", "name": "t"}]),
        ]
        explicit: list[SlayerQuery | dict] = [
            q(name="inner", source_model="orders", dimensions=["customer_id"],
              measures=[{"formula": "sum(amount)", "name": "spent"}]),
            q(name="s", source_model="inner", dimensions=["customer_id"], measures=["mm"],
              extensions=[ext("inner", measures=[mm])]),
            q(source_model="s", measures=[{"formula": "sum(mm)", "name": "t"}]),
        ]
        async with shop(dialect, with_models(_cust_spend())) as engine:
            got = await engine.execute(over_model)
            expected = await engine.execute(explicit)
        assert float(got.data[0]["s.t"]) == float(expected.data[0]["s.t"]) == TOTAL_AMOUNT


class TestScope:
    async def test_extension_is_invisible_to_the_next_stage(self, dialect) -> None:
        stage = q(name="s1", source_model="orders", dimensions=["status"], measures=[AMOUNT],
                  extensions=[ext("customers", columns=[TIER_LABEL])])
        root = q(source_model="orders", dimensions=["customers.tier_label"],
                 extensions=[ext("orders", joins=[join("s1", [["status", "status"]])])])
        async with shop(dialect) as engine:
            with pytest.raises(UnknownReferenceError):
                await engine.execute([stage, root])

    async def test_extension_is_invisible_to_a_stored_query_backed_stage(self, dialect) -> None:
        qb = SlayerModel(name="tl_rev", data_source=DS, source_queries=[SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customers.tier_label"], "measures": [AMOUNT]})])
        query = q(source_model="tl_rev", measures=[{"formula": "sum(a)", "name": "t"}],
                  extensions=[ext("customers", columns=[TIER_LABEL])])
        async with shop(dialect, with_models(qb)) as engine:
            with pytest.raises(SlayerError, match="tier_label"):
                await engine.execute(query)

    async def test_extension_is_invisible_to_a_saved_measure(self, dialect) -> None:
        stored = orders().model_copy(update={"measures": [
            ModelMeasure(name="labels", formula="count_distinct(customers.tier_label)")]})
        query = q(source_model="orders", measures=["labels"], extensions=[ext("customers", columns=[TIER_LABEL])])
        async with shop(dialect, with_models(stored)) as engine:
            with pytest.raises(SlayerError, match="tier_label"):
                await engine.execute(query)

    async def test_a_stage_shadows_a_same_named_model(self, dialect) -> None:
        stage = q(name="customers", source_model="orders", dimensions=["customer_id"],
                  measures=[{"formula": "sum(amount)", "name": "spent"}])
        root = q(source_model="customers", measures=[{"formula": "sum(double_spent)", "name": "t"}],
                 extensions=[ext("customers", columns=[{"name": "double_spent", "sql": "spent * 2", "type": "DOUBLE"}])])
        async with shop(dialect) as engine:
            resp = await engine.execute([stage, root])
        assert float(resp.data[0]["customers.t"]) == 2 * TOTAL_AMOUNT

    async def test_the_inline_source_wins_over_a_stored_model(self, dialect) -> None:
        inline = {"name": "customers", "data_source": DS, "sql_table": "customers", "columns": [
            {"name": "id", "type": "INT", "primary_key": True}, {"name": "credit", "type": "DOUBLE"},
            {"name": "credit10", "sql": "credit * 10", "type": "DOUBLE"}]}
        query = q(source_model=inline, measures=[{"formula": "c10", "name": "t"}],
                  extensions=[ext("customers", measures=[{"name": "c10", "formula": "sum(credit10)"}])])
        async with shop(dialect) as engine:
            resp = await engine.execute(query)
        assert float(resp.data[0]["customers.t"]) == 10 * TOTAL_CREDIT


class TestRejectedTargets:
    @pytest.mark.parametrize("extension", [
        ext("nope", columns=[TIER_LABEL]),
        ext("orders", joins=[join("nope", [["customer_id", "id"]])]),
    ], ids=["source_name", "join-target"])
    async def test_unknown_extension_target(self, dialect, extension: dict) -> None:
        async with shop(dialect) as engine:
            query = q(source_model="orders", dimensions=["status"], extensions=[extension])
            with pytest.raises(ModelNotFoundError, match="nope"):
                await engine.execute(query)

    async def test_extending_the_time_spine_is_rejected(self, dialect) -> None:
        query = q(source_model="orders", dimensions=["status"],
                  extensions=[ext("time_spine", columns=[{"name": "x", "sql": "1", "type": "INT"}])])
        async with shop(dialect) as engine:
            with pytest.raises(SlayerError, match="time_spine") as info:
                await engine.execute(query)
        assert not isinstance(info.value, ModelNotFoundError)


class TestSaveTimeRefusal:
    @pytest.mark.parametrize(("extension", "named"), [
        (ext("orders", joins=[join("addresses", [["ship_region_id", "id"]], name="billing_address")]),
         "billing_address"),
        (ext("nope", columns=[TIER_LABEL]), "nope"),
        (ext("events", joins=[join("users", [["user_id", "id"]], join_type="inner")]), "events"),
    ], ids=["edge-conflict", "unresolvable", "unreachable-inner"])
    async def test_save_refuses_a_stage_whose_extension_always_fails(self, dialect, extension, named) -> None:
        stages = [
            q(name="conflicted", source_model="orders", dimensions=["status"], measures=[AMOUNT],
              extensions=[extension]),
            q(source_model="conflicted", measures=[{"formula": "sum(a)", "name": "t"}]),
        ]
        async with shop(dialect) as engine:
            with pytest.raises(ValueError) as info:
                await engine.create_model_from_query(query=stages, name="bad_qb")
            assert await engine.storage.get_model("bad_qb", data_source=DS) is None
        message = str(info.value)
        assert "conflicted" in message, message
        assert named in message, message


class TestStageOrdering:
    def test_a_stage_follows_every_sibling_its_extensions_read(self) -> None:
        a = q(name="a", source_model="orders", dimensions=["customer_id"])
        c = q(name="c", source_model="customers", dimensions=["id"])
        d = q(name="d", source_model="customers", dimensions=["id"])
        e = q(name="e", source_model="orders", dimensions=["status"])
        b = q(name="b", source_model="a", extensions=[
            ext("a", joins=[join("c", [["customer_id", "id"]]), join("d", [["customer_id", "id"]])]),
            ext("e", columns=[{"name": "x", "sql": "1", "type": "INT"}]),
        ])
        ordered = [s.name for s in topologically_order_stages([b, e, d, c, a, q(source_model="b")])]
        assert all(ordered.index(name) < ordered.index("b") for name in "acde"), ordered

    @pytest.mark.parametrize("order", [("b", "c", "a"), ("c", "a", "b"), ("a", "b", "c")])
    async def test_a_stage_joining_siblings_through_its_extensions_executes(self, dialect, order) -> None:
        stages = {
            "a": q(name="a", source_model="orders", dimensions=["customer_id"],
                   measures=[{"formula": "sum(amount)", "name": "spent"}]),
            "c": q(name="c", source_model="customers", dimensions=["id", "tier"]),
            "b": q(name="b", source_model="a", dimensions=["c.tier"], measures=[{"formula": "sum(spent)", "name": "t"}],
                   extensions=[ext("a", joins=[join("c", [["customer_id", "id"]])])]),
        }
        root = q(source_model="b", measures=[{"formula": "sum(t)", "name": "total"}])
        async with shop(dialect) as engine:
            resp = await engine.execute([*(stages[n] for n in order), root])
        assert float(resp.data[0]["b.total"]) == TOTAL_AMOUNT


class TestUsageFeatures:
    @pytest.mark.parametrize("source_name", ["orders", "customers"], ids=["root", "joined"])
    def test_extensions_count_as_inline_sources_with_their_measures(self, source_name: str) -> None:
        found = query_features({
            "source_model": "orders", "measures": ["count(*)"],
            "extensions": [{"source_name": source_name, "measures": [{"name": "m2", "formula": "max(round(credit))"}]}],
        })
        assert "inline_source" in found.flags
        assert Counter(found.functions) == Counter(["round"])
        assert Counter(found.aggregations) == Counter(["count", "max"])

    def test_a_plain_query_is_not_an_inline_source(self) -> None:
        assert "inline_source" not in query_features({"source_model": "orders", "measures": ["count(*)"]}).flags
