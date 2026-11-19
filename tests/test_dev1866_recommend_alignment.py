"""DEV-1866 — recommend_root_model shares the population selection core.

Columns are determination items (to-one, hop-counted); saved measures and
aggregation-suffixed items are reachability-only attachments that never steer
the recommendation. The recommended root must equal the inferred population for
the same dimension items, so the two surfaces cannot disagree.
"""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery
from slayer.engine.population.inference import infer_population
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1866_fixtures import DS_CHAIN, DS_FAN, make_inference_storage


@pytest.fixture
async def engine():
    return SlayerQueryEngine(storage=await make_inference_storage())


def _path_for(rec, input_item: str):
    (ip,) = [p for p in rec.item_paths if p.input_item == input_item]
    return ip


class TestRecommendAlignment:
    async def test_saved_measures_are_attachments_and_dont_steer(self, engine: SlayerQueryEngine) -> None:
        """dim on regions + saved measures on orders. The old all-items objective
        min-hops over mentioned models {regions, orders} and picks orders; the
        dimension-only rule determines only regions.name (0 hops) ⇒ regions, with
        the reachable-only measures marked as attachments."""
        rec = await engine.recommend_population(
            ["regions.name", "orders.revenue", "orders.avg_amt"],
            data_source=DS_CHAIN,
        )
        assert rec.population == "regions"
        assert _path_for(rec, "regions.name").attachment is False
        assert _path_for(rec, "orders.revenue").attachment is True
        assert _path_for(rec, "orders.avg_amt").attachment is True

    async def test_aggregation_suffixed_item_is_an_attachment(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.region", "sum(orders.amount)"], data_source=DS_CHAIN,
        )
        assert rec.population == "customers"
        assert _path_for(rec, "sum(orders.amount)").attachment is True

    async def test_recommendation_matches_inference(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.region"], data_source=DS_CHAIN,
        )
        choice = await infer_population(
            query=SlayerQuery(dimensions=["customers.region"]),
            storage=engine.storage,
        )
        assert rec.population == choice.dataset_name == "customers"

    async def test_recommendation_matches_inference_multi_item(self, engine: SlayerQueryEngine) -> None:
        items = ["customers.region", "customers.regions.name"]
        rec = await engine.recommend_population(items, data_source=DS_CHAIN)
        choice = await infer_population(
            query=SlayerQuery(dimensions=items), storage=engine.storage,
        )
        assert rec.population == choice.dataset_name == "customers"

    async def test_feasible_root_hint_overrides(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.region"], data_source=DS_CHAIN, root_hint="orders",
        )
        assert rec.population == "orders"

    async def test_feasible_root_hint_with_determination_and_attachment(self, engine: SlayerQueryEngine) -> None:
        """A bridge that determines the dimension and reaches the measure is feasible."""
        rec = await engine.recommend_population(
            ["regions.name", "orders.revenue"],
            data_source=DS_CHAIN, root_hint="customers",
        )
        assert rec.population == "customers"

    async def test_infeasible_root_hint_falls_back_with_warning(self, engine: SlayerQueryEngine) -> None:
        rec = await engine.recommend_population(
            ["customers.region"], data_source=DS_CHAIN, root_hint="regions",
        )
        assert rec.population == "customers"
        assert any("regions" in w for w in rec.warnings)

    async def test_malformed_root_hint_raises(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError):
            await engine.recommend_population(
                ["customers.region"], data_source=DS_CHAIN,
                root_hint="does_not_exist",
            )

    async def test_no_common_root_reports_coverage(self, engine: SlayerQueryEngine) -> None:
        """Sibling dims have no single to-one determiner ⇒ not reachable."""
        rec = await engine.recommend_population(
            ["basket_items.sku", "basket_pays.method"], data_source=DS_FAN,
        )
        assert rec.reachable is False
        assert rec.population is None
        assert rec.coverage
        # Per-item criterion: basket_items determines its own sku but not the sibling method.
        assert any(
            "basket_items.sku" in c.reachable_items
            and "basket_pays.method" in c.unreachable_items
            for c in rec.coverage
        )
