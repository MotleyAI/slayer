"""Anchors derive from the saved-metric-excluding classification.

Covers the ``queries/population`` datasource-scoping delta: a filter reference
resolving to a saved metric (including via a named relationship) contributes no anchor
to datasource scoping or sibling-stage detection, plus guard pins on the
two-phase restructure.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.query import SlayerQuery
from slayer.engine.population.inference import infer_population

from tests._dev1866_fixtures import DS_CHAIN, make_inference_storage
from tests._engine_helpers import owned_query_engine


@pytest.fixture
async def storage():
    return await make_inference_storage()


class TestSavedMeasureAnchors:
    async def test_named_join_saved_measure_does_not_steer_datasource_scoping(
        self, storage
    ) -> None:
        """Spec: Saved-measure filter does not steer datasource scoping —
        ``tickets.reporter.handled`` (foreign datasource, named relationship) must
        contribute no anchor, so scoping matches the filterless query."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filter=["tickets.reporter.handled > 0"],
            ),
            storage=storage,
        )
        assert choice.dataset_name == "customers"
        assert choice.data_source == DS_CHAIN

    async def test_saved_measure_colliding_with_sibling_stage_name(
        self, storage
    ) -> None:
        """Spec: Saved-measure name colliding with a sibling stage — the
        sibling diagnostic must not fire off a saved-metric-only anchor."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filter=["orders.revenue > 100"],
            ),
            storage=storage,
            sibling_stage_names={"orders"},
        )
        assert choice.dataset_name == "customers"


class TestFoldPins:
    async def test_real_column_sharing_aggregation_name_keeps_its_anchor(
        self, storage
    ) -> None:
        """Field precedence: ``agents.score`` is a real field (despite the
        same-named custom aggregation), so its anchor stays and cross-datasource
        scoping still fails closed."""
        query = SlayerQuery(
            dimensions=["customers.region"], filter=["agents.score > 0"],
        )
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        assert ei.value.reason is PopulationErrorReason.NO_DATASOURCE

    @pytest.mark.xfail(strict=True, reason="a saved metric binds in a filter only when also projected until DEV-2387")
    async def test_saved_measure_only_filter_has_the_unit_as_its_anchor(
        self, storage
    ) -> None:
        """A saved-measure filter alone contributes no anchor: the query's anchor is the unit, not a
        population error."""
        async with owned_query_engine(storage=storage) as engine:
            resp = await engine.execute(SlayerQuery(filter=["orders.revenue > 100"]), dry_run=True)
        assert resp.population is None