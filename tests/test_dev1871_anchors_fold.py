"""DEV-1871 — anchors derive from the saved-measure-excluding classification.

Covers the ``queries/population`` datasource-scoping delta: a filter reference
resolving to a saved measure (including via a named join) contributes no anchor
to datasource scoping or sibling-stage detection. xfail(strict) tests flip with
tasks group 18; the rest are guard pins on the two-phase restructure.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.query import SlayerQuery
from slayer.engine.population import infer_population

from tests._dev1866_fixtures import DS_CHAIN, make_inference_storage

_FOLD_XFAIL = pytest.mark.xfail(
    strict=True, reason="anchor classification fold lands with DEV-1871 group 18",
)


@pytest.fixture
async def storage():
    return await make_inference_storage()


class TestSavedMeasureAnchors:
    @_FOLD_XFAIL
    async def test_named_join_saved_measure_does_not_steer_datasource_scoping(
        self, storage
    ) -> None:
        """Spec: Saved-measure filter does not steer datasource scoping —
        ``tickets.reporter.handled`` (foreign datasource, named join) must
        contribute no anchor, so scoping matches the filterless query."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filters=["tickets.reporter.handled > 0"],
            ),
            storage=storage,
        )
        assert choice.model_name == "customers"
        assert choice.data_source == DS_CHAIN

    @_FOLD_XFAIL
    async def test_saved_measure_colliding_with_sibling_stage_name(
        self, storage
    ) -> None:
        """Spec: Saved-measure name colliding with a sibling stage — the
        sibling diagnostic must not fire off a saved-measure-only anchor."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filters=["orders.revenue > 100"],
            ),
            storage=storage,
            sibling_stage_names={"orders"},
        )
        assert choice.model_name == "customers"


class TestFoldPins:
    async def test_real_column_sharing_aggregation_name_keeps_its_anchor(
        self, storage
    ) -> None:
        """Column precedence: ``agents.score`` is a real column (despite the
        same-named custom aggregation), so its anchor stays and cross-datasource
        scoping still fails closed."""
        query = SlayerQuery(
            dimensions=["customers.region"], filters=["agents.score > 0"],
        )
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        assert ei.value.reason is PopulationErrorReason.NO_DATASOURCE

    async def test_saved_measure_only_filter_stays_empty_determination(
        self, storage
    ) -> None:
        """Precedence order survives the fold: nothing to infer from beats any
        datasource verdict."""
        query = SlayerQuery(filters=["orders.revenue > 100"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        assert ei.value.reason is PopulationErrorReason.EMPTY_DETERMINATION
