"""DEV-1866 — population inference fails closed with a typed, payload-bearing error.

Every ambiguity that leaves the population under-determined raises
``PopulationInferenceError`` with a ``reason`` kind, the relevant ``candidates``
/ ``datasources``, and a stable ``PopulationInferenceError:`` message prefix.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.query import SlayerQuery
from slayer.engine.population import infer_population

from tests._dev1866_fixtures import (
    DS_CHAIN,
    DS_WIDGETS_A,
    DS_WIDGETS_B,
    make_inference_storage,
)


@pytest.fixture
async def storage():
    return await make_inference_storage()


def _assert_stable_prefix(err: PopulationInferenceError) -> None:
    assert str(err).startswith("PopulationInferenceError:")


class TestFailClosed:
    async def test_tie_names_both_candidates(self, storage) -> None:
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(dimensions=["prof.bio", "acct.email"]),
                storage=storage,
            )
        err = ei.value
        assert err.reason is PopulationErrorReason.TIE
        assert set(err.candidates) == {"acct", "prof"}
        assert "source_model" in str(err)
        _assert_stable_prefix(err)

    async def test_no_viable_candidate_lists_the_siblings(self, storage) -> None:
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(
                    dimensions=["basket_items.sku", "basket_pays.method"]
                ),
                storage=storage,
            )
        err = ei.value
        assert err.reason is PopulationErrorReason.NO_VIABLE_CANDIDATE
        assert set(err.candidates) == {"basket", "basket_items", "basket_pays"}
        _assert_stable_prefix(err)

    async def test_unknown_cardinality_hop_is_not_determination(self, storage) -> None:
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(dimensions=["hits.page", "sess.token"]),
                storage=storage,
            )
        assert ei.value.reason is PopulationErrorReason.NO_VIABLE_CANDIDATE
        _assert_stable_prefix(ei.value)

    async def test_empty_determination_set_has_dedicated_error(self, storage) -> None:
        """Only measures, no dims/field-filters ⇒ nothing to infer from."""
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(
                    measures=[{"formula": "orders.amount:sum", "name": "rev"}],
                ),
                storage=storage,
                data_source=DS_CHAIN,
            )
        err = ei.value
        assert err.reason is PopulationErrorReason.EMPTY_DETERMINATION
        assert not err.candidates  # dedicated message, no full model listing
        assert "infer" in str(err).lower()
        _assert_stable_prefix(err)

    async def test_ambiguous_hop_fails_closed(self, storage) -> None:
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(dimensions=["shipment.weight", "depot.name"]),
                storage=storage,
            )
        err = ei.value
        assert err.reason is PopulationErrorReason.AMBIGUOUS_PATH
        assert "shipment" in str(err) and "depot" in str(err)
        _assert_stable_prefix(err)

    async def test_ambiguous_datasource_names_both(self, storage) -> None:
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(dimensions=["widgets.x"]),
                storage=storage,
            )
        err = ei.value
        assert err.reason is PopulationErrorReason.AMBIGUOUS_DATASOURCE
        assert set(err.datasources) == {DS_WIDGETS_A, DS_WIDGETS_B}
        _assert_stable_prefix(err)

    async def test_zero_candidate_datasources_fails_closed(self, storage) -> None:
        """No single datasource holds every referenced anchor model."""
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(dimensions=["widgets.x", "customers.region"]),
                storage=storage,
            )
        assert ei.value.reason is PopulationErrorReason.NO_DATASOURCE
        _assert_stable_prefix(ei.value)

    async def test_datasource_argument_pins_scoping(self, storage) -> None:
        """A collision resolves when the execution datasource is supplied."""
        choice = await infer_population(
            query=SlayerQuery(dimensions=["widgets.x"]),
            storage=storage,
            data_source=DS_WIDGETS_A,
        )
        assert choice.model_name == "widgets"
        assert choice.data_source == DS_WIDGETS_A

    async def test_sibling_anchored_stage_refs_fail_closed(self, storage) -> None:
        """A stage omitting source_model whose dims anchor at a sibling name."""
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(dimensions=["s1.total"]),
                storage=storage,
                sibling_stage_names={"s1"},
            )
        err = ei.value
        assert err.reason is PopulationErrorReason.SIBLING_STAGE
        assert "s1" in str(err)
        assert "source_model" in str(err)  # advice: name the sibling explicitly
        _assert_stable_prefix(err)

    async def test_measure_only_without_datasource_is_empty_determination(self, storage) -> None:
        """A measures-only query has nothing to infer from — even with no
        data_source it is EMPTY_DETERMINATION, never a datasource error."""
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(
                    measures=[{"formula": "orders.amount:sum", "name": "rev"}],
                ),
                storage=storage,
            )
        assert ei.value.reason is PopulationErrorReason.EMPTY_DETERMINATION

    async def test_measure_referencing_sibling_does_not_fail_closed(self, storage) -> None:
        """A measure whose anchor matches a sibling stage name must not trigger
        SIBLING_STAGE — measures never anchor inference."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                measures=[{"formula": "s1.total:sum", "name": "t"}],
            ),
            storage=storage,
            sibling_stage_names={"s1"},
        )
        assert choice.model_name == "customers"

    async def test_string_literal_is_not_a_sibling_anchor(self, storage) -> None:
        """A dotted value inside a string literal must not be read as a reference
        (so it cannot spuriously anchor at a sibling stage)."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filters=["customers.region = 's1.total'"],
            ),
            storage=storage,
            sibling_stage_names={"s1"},
        )
        assert choice.model_name == "customers"

    async def test_error_payload_exposes_all_three_fields(self, storage) -> None:
        """reason/candidates/datasources are always present on the typed error."""
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(
                query=SlayerQuery(dimensions=["prof.bio", "acct.email"]),
                storage=storage,
            )
        err = ei.value
        assert isinstance(err.reason, PopulationErrorReason)
        assert isinstance(err.candidates, list)
        assert isinstance(err.datasources, list)
