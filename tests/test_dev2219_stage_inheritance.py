"""Stages inherit edges on pass-through key columns; cardinality re-derives from the grain; declared edges win.

Spec: openspec …/specs/queries/multi-stage — "Stages inherit edges on pass-through keys",
"Inherited edges re-derive their cardinality from the stage grain", "Declared edges win over inherited ones";
…/specs/queries/extensions — "Extensions are scoped to their query object".
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import AmbiguousRelationshipPathError, UnrelatedDatasetError

from tests._dev2219_fixtures import (
    CUSTOMER_REVENUE_BY_REGION,
    DIALECTS,
    INVOICE_TOTAL_BY_BILLING,
    REVENUE_BY_NAME,
    REVENUE_BY_ORDER_REGION,
    REVENUE_BY_REGION,
    ZONE_BY_COUNT,
    by,
    customers,
    ext,
    join,
    kinds,
    m,
    orders,
    outcome,
    over,
    per_customer,
    q,
    shop,
    with_datasets,
)

@pytest.fixture(params=DIALECTS)
async def engine(request):
    async with shop(request.param) as e:
        yield e


# --------------------------------------------------------------------------- #
# Pass-through.
# --------------------------------------------------------------------------- #
class TestPassThrough:
    async def test_grouped_by_foreign_key_reaches_its_target(self, engine) -> None:
        resp = await engine.execute([per_customer(), over("per_customer", dimensions=["customers.region"])])
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION
        assert not kinds(resp) & {"broadcast", "unrelated_dataset", "associated"}

    async def test_renamed_key_is_inherited(self, engine) -> None:
        stage = per_customer(dimensions=[{"field": "customer_id", "alias": "cid"}])
        resp = await engine.execute([stage, over("per_customer", dimensions=["customers.region"])])
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION

    @pytest.mark.parametrize("expr", ["CAST(customer_id AS TEXT)", "customer_id + 0"])
    async def test_cast_or_computed_key_is_not_inherited(self, engine, expr: str) -> None:
        stage = per_customer(dimensions=[{"field": expr, "alias": "cid"}])
        stages = [stage, over("per_customer", dimensions=["customers.region"])]
        with pytest.raises(UnrelatedDatasetError):
            await engine.execute(stages)

    async def test_time_bucketed_key_is_not_inherited(self, engine) -> None:
        stage = q(name="monthly", population="orders",
                  time_dimensions=[{"field": "ordered_at", "granularity": "month"}],
                  metrics=[m("sum(amount)", "revenue")])
        stages = [stage, over("monthly", dimensions=["calendar.is_holiday"])]
        with pytest.raises(UnrelatedDatasetError):
            await engine.execute(stages)

    async def test_unbucketed_time_key_is_inherited(self, engine) -> None:
        stage = q(name="daily", population="orders", dimensions=["ordered_at"], metrics=[m("sum(amount)", "revenue")])
        resp = await engine.execute([stage, over("daily", dimensions=["calendar.is_holiday"])])
        assert by(resp, key="calendar.is_holiday", value="total") == {0: 105.0, 1: 22.0}

    async def test_metric_column_is_not_a_pass_through(self, engine) -> None:
        stage = q(name="s", population="orders", dimensions=["status"], metrics=[m("max(customer_id)", "cid")])
        stages = [stage, over("s", dimensions=["customers.region"], metric="count(*)")]
        with pytest.raises(UnrelatedDatasetError):
            await engine.execute(stages)

    async def test_a_filter_only_key_is_not_a_pass_through(self, engine) -> None:
        stage = q(name="s", population="orders", dimensions=["status"], filter=["customer_id > 0"],
                  metrics=[m("sum(amount)", "revenue")])
        stages = [stage, over("s", dimensions=["customers.region"])]
        with pytest.raises(UnrelatedDatasetError):
            await engine.execute(stages)

    async def test_targets_own_key_is_inherited(self, engine) -> None:
        stage = q(name="s", population="orders", dimensions=["customers.id"], metrics=[m("sum(amount)", "revenue")])
        resp = await engine.execute([stage, over("s", dimensions=["customers.region"])])
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION

    async def test_targets_own_key_reaches_onward(self, engine) -> None:
        stage = q(name="s", population="orders", dimensions=["customers.id"], metrics=[m("sum(amount)", "revenue")])
        resp = await engine.execute([stage, over("s", dimensions=["customers.regions.name"])])
        assert by(resp, key="customers.regions.name", value="total") == REVENUE_BY_REGION

    async def test_onward_traversal_through_an_inherited_edge(self, engine) -> None:
        resp = await engine.execute([per_customer(), over("per_customer", dimensions=["customers.regions.name"])])
        assert by(resp, key="customers.regions.name", value="total") == REVENUE_BY_REGION

    async def test_reverse_direction_is_inherited(self, engine) -> None:
        stage = q(name="s", population="customers", dimensions=["id", "region"], metrics=[m("count(*)", "n")])
        resp = await engine.execute([stage, over("s", dimensions=["region"], metric="sum(orders.amount)")])
        assert by(resp, key="region", value="total") == CUSTOMER_REVENUE_BY_REGION

    async def test_relationship_of_the_stages_own_extension_is_inherited(self, engine) -> None:
        stage = q(name="s", population="orders", extensions=[ext("orders", join("regions", [("region_id", "id")]))],
                  dimensions=["region_id"], metrics=[m("sum(amount)", "revenue")])
        resp = await engine.execute([stage, over("s", dimensions=["regions.name"])])
        assert by(resp, key="regions.name", value="total") == REVENUE_BY_ORDER_REGION

    async def test_extension_relationship_needs_its_key_passed_through(self, engine) -> None:
        stage = q(name="s", population="orders", extensions=[ext("orders", join("regions", [("region_id", "id")]))],
                  dimensions=["status"], metrics=[m("sum(amount)", "revenue")])
        stages = [stage, over("s", dimensions=["regions.name"])]
        with pytest.raises(UnrelatedDatasetError):
            await engine.execute(stages)

    async def test_inheritance_is_transitive(self, engine) -> None:
        s1 = per_customer("s1")
        s2 = q(name="s2", population="s1", dimensions=["customer_id"], metrics=[m("sum(revenue)", "rev2")])
        s3 = q(name="s3", population="s2", dimensions=["customer_id"], metrics=[m("sum(rev2)", "rev3")])
        resp = await engine.execute([s1, s2, s3, over("s3", dimensions=["customers.region"], metric="sum(rev3)")])
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION

    async def test_one_source_edge_through_two_upstream_stages_is_inherited_once(self, engine) -> None:
        a = per_customer("a")
        b = q(name="b", population="orders", dimensions=["customer_id"], metrics=[m("count(*)", "nb")])
        d = q(name="d", population="a", extensions=[ext("a", join("b", [("customer_id", "customer_id")]))],
              dimensions=["customer_id", "b.customer_id"], metrics=[m("sum(revenue)", "rev_d")])
        resp = await engine.execute([a, b, d, over("d", dimensions=["customers.region"], metric="sum(rev_d)")])
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION

    async def test_stored_target_is_never_re_resolved_to_a_same_named_stage(self, engine) -> None:
        shadow = q(name="customers", population="orders", dimensions=["status"], metrics=[m("count(*)", "n")])
        resp = await engine.execute([shadow, per_customer(), over("per_customer", dimensions=["customers.region"])])
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION

    async def test_equivalent_to_an_explicit_extension_relationship(self, engine) -> None:
        inherited = [per_customer(), over("per_customer", dimensions=["customers.region"])]
        declared = [per_customer(), over(
            "per_customer", dimensions=["customers.region"],
            extensions=[ext("per_customer", join("customers", [("customer_id", "id")]))])]
        assert await outcome(engine, inherited) == await outcome(engine, declared)
        assert (await outcome(engine, inherited))[0] == "ok"


class TestFilters:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_filters_do_not_remove_an_inherited_edge(self, dialect: str) -> None:
        datasets = with_datasets(orders(filters=["customer_id IS NOT NULL"]), customers(filters=["tier <> 'bronze'"]))
        stage = per_customer(filter=["amount > 6"])
        async with shop(dialect, datasets) as engine:
            through_stage = await engine.execute([stage, over("per_customer", dimensions=["customers.tier"])])
            direct = await engine.execute(over("orders", dimensions=["customers.tier"], metric="sum(amount)",
                                               filter=["amount > 6"]))
        expected = by(direct, key="customers.tier", value="total")
        assert expected == {"gold": 30.0, "silver": 45.0, None: 40.0}
        assert by(through_stage, key="customers.tier", value="total") == expected


# --------------------------------------------------------------------------- #
# Cardinality.
# --------------------------------------------------------------------------- #
class TestCardinality:
    @staticmethod
    def _from_customers(stage: str) -> Any:
        return q(population="customers", dimensions=["name", f"{stage}.revenue"], metrics=[m("count(*)", "n")])

    async def test_grouping_by_the_key_makes_the_edge_one_to_one(self, engine) -> None:
        resp = await engine.execute([per_customer(), self._from_customers("per_customer")])
        assert {r["customers.name"]: r["customers.per_customer.revenue"] for r in resp.data} == REVENUE_BY_NAME
        assert {r["customers.n"] for r in resp.data} == {1}
        assert not kinds(resp) & {"broadcast", "associated"}

    async def test_a_grain_wider_than_the_key_keeps_the_stage_side_many(self, engine) -> None:
        stage = per_customer(dimensions=["customer_id", "status"])
        resp = await engine.execute([stage, self._from_customers("per_customer")])
        assert "associated" in kinds(resp)

    async def test_a_raw_row_stage_keeps_the_stage_side_many(self, engine) -> None:
        stage = q(name="rr", population="orders", dimensions=["customer_id", {"field": "amount", "alias": "revenue"}],
                  distinct_dimension_values=False)
        resp = await engine.execute([stage, self._from_customers("rr")])
        assert "associated" in kinds(resp)

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize(("grain", "stage_side_many"), [
        (["customer_id", "status"], False), (["customer_id", "status", "region_id"], True)])
    async def test_a_composite_key_is_one_only_when_it_covers_the_grain(
        self, dialect: str, grain: list[str], stage_side_many: bool,
    ) -> None:
        root = q(population="quotas", dimensions=["quota", "cs.revenue"], metrics=[m("count(*)", "n")])
        async with shop(dialect, with_datasets(orders(to_quotas=True))) as engine:
            resp = await engine.execute([per_customer("cs", dimensions=grain), root])
        assert ("associated" in kinds(resp)) is stage_side_many
        if not stage_side_many:
            got = {r["quotas.quota"]: r["quotas.cs.revenue"] for r in resp.data}
            assert got == {5: 10.0, 6: 20.0, 7: 30.0, 8: 5.0, 9: 40.0, 10: 15.0}

    async def test_to_many_metric_of_a_wide_stage_is_exact(self, engine) -> None:
        stage = per_customer(dimensions=["customer_id", "status"])
        root = q(population="customers", dimensions=["name"], metrics=[m("sum(per_customer.revenue)", "rev")])
        resp = await engine.execute([stage, root])
        assert by(resp, key="name", value="rev") == REVENUE_BY_NAME


# --------------------------------------------------------------------------- #
# Declared edges win.
# --------------------------------------------------------------------------- #
class TestDeclaredEdgesWin:
    @staticmethod
    def _stage() -> Any:
        return per_customer(metrics=[m("sum(amount)", "revenue"), m("count(*)", "n")])

    async def test_declared_edge_of_the_same_name_wins(self, engine) -> None:
        zone = q(name="by_region", population="orders", dimensions=["region_id"],
                 extensions=[ext("orders", join("regions", [("region_id", "id")], name="zone"))],
                 metrics=[m("sum(amount)", "revenue"), m("count(*)", "n")])
        root = q(population="by_region", dimensions=["region_id", "zone.name"], metrics=[m("sum(revenue)", "t")],
                 extensions=[ext("by_region", join("regions", [("n", "id")], name="zone"))])
        inherited = await engine.execute([zone, root.model_copy(update={"extensions": []})])
        assert {r["by_region.region_id"]: r["by_region.zone.name"] for r in inherited.data} == {
            1: "North", 2: "South", 3: "East", None: None}
        resp = await engine.execute([zone, root])
        got = {r["by_region.region_id"]: r["by_region.zone.name"] for r in resp.data}
        assert got == ZONE_BY_COUNT

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_an_unnamed_identical_declared_edge_keeps_the_inherited_name_as_alias(self, dialect: str) -> None:
        extension = [ext("per_customer", join("customers", [("customer_id", "id")]))]
        async with shop(dialect, with_datasets(orders(customer_edge_name="buyer"))) as engine:
            by_target = await engine.execute([per_customer(), over("per_customer", dimensions=["customers.region"],
                                                                   extensions=extension)])
            by_alias = await engine.execute([per_customer(), over("per_customer", dimensions=["buyer.region"],
                                                                  extensions=extension)])
        assert by(by_target, key="customers.region", value="total") == REVENUE_BY_REGION
        assert by(by_alias, key="buyer.region", value="total") == REVENUE_BY_REGION

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_identical_declared_edge_in_a_consuming_extension_applies_once(self, dialect: str) -> None:
        root = over("per_customer", dimensions=["customers.region"],
                    extensions=[ext("per_customer", join("customers", [("customer_id", "id")]))])
        async with shop(dialect) as engine:
            resp = await engine.execute([per_customer(), root])
            undeclared = await engine.execute([per_customer(), over("per_customer", dimensions=["customers.region"])])
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION
        assert by(undeclared, key="customers.region", value="total") == REVENUE_BY_REGION

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_differently_named_edges_on_the_same_keys_stay_parallel(self, dialect: str) -> None:
        extension = [ext("per_customer", join("customers", [("customer_id", "id")], name="payer"))]
        async with shop(dialect, with_datasets(orders(customer_edge_name="buyer"))) as engine:
            stages = [self._stage(), over("per_customer", dimensions=["customers.name"], extensions=extension)]
            with pytest.raises(AmbiguousRelationshipPathError):
                await engine.execute(stages)
            for edge in ("buyer", "payer"):
                resp = await engine.execute([self._stage(), over("per_customer", dimensions=[f"{edge}.name"],
                                                                 extensions=extension)])
                assert by(resp, key=f"{edge}.name", value="total") == {**REVENUE_BY_NAME, None: 7.0}

    async def test_parallel_distinct_edges_need_their_names(self, engine) -> None:
        stage = q(name="inv", population="invoices", dimensions=["billing_customer_id", "shipping_customer_id"],
                  metrics=[m("sum(total)", "amt")])
        stages = [stage, over("inv", dimensions=["customers.name"], metric="sum(amt)")]
        with pytest.raises(AmbiguousRelationshipPathError):
            await engine.execute(stages)
        resp = await engine.execute([stage, over("inv", dimensions=["billing.name"], metric="sum(amt)")])
        assert by(resp, key="billing.name", value="total") == INVOICE_TOTAL_BY_BILLING
