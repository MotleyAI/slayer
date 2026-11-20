"""Population inference fails closed with a typed, payload-bearing error.

Every ambiguity that leaves the population under-determined raises
``PopulationInferenceError`` with a ``reason`` kind, the relevant ``candidates``
/ ``datasources``, and a stable ``PopulationInferenceError:`` message prefix.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.population.inference import infer_population
from slayer.engine.query_engine import SlayerResponse

from tests._dev1866_fixtures import (
    DS_CHAIN,
    DS_WIDGETS_A,
    DS_WIDGETS_B,
    make_inference_storage,
)
from tests._engine_helpers import owned_query_engine


@pytest.fixture
async def storage():
    return await make_inference_storage()


async def _dry_run(storage, query: SlayerQuery, **kwargs) -> SlayerResponse:
    async with owned_query_engine(storage=storage) as engine:
        return await engine.execute(query, dry_run=True, **kwargs)


def _assert_stable_prefix(err: PopulationInferenceError) -> None:
    assert str(err).startswith("PopulationInferenceError:")


class TestFailClosed:
    async def test_tie_names_both_candidates(self, storage) -> None:
        query = SlayerQuery(dimensions=["prof.bio", "acct.email"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        err = ei.value
        assert err.reason is PopulationErrorReason.TIE
        assert set(err.candidates) == {"acct", "prof"}
        assert "population" in str(err)
        _assert_stable_prefix(err)

    async def test_no_viable_candidate_lists_the_siblings(self, storage) -> None:
        query = SlayerQuery(dimensions=["basket_items.sku", "basket_pays.method"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        err = ei.value
        assert err.reason is PopulationErrorReason.NO_VIABLE_CANDIDATE
        assert set(err.candidates) == {"basket", "basket_items", "basket_pays"}
        _assert_stable_prefix(err)

    async def test_unknown_cardinality_hop_counts_by_association(self, storage) -> None:
        resp = await _dry_run(storage, SlayerQuery.model_validate({"dimensions": ["hits.page", "sess.token"]}))
        assert (resp.population, resp.population_inferred) == ("hits", True)

    async def test_a_metrics_only_query_has_the_unit_as_its_anchor(self, storage) -> None:
        """Only metrics, no dims: the rows are the unit, never a population error."""
        query = SlayerQuery.model_validate({"metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]})
        resp = await _dry_run(storage, query, data_source=DS_CHAIN)
        assert (resp.population, resp.population_inferred) == (None, True)

    async def test_ambiguous_hop_fails_closed(self, storage) -> None:
        query = SlayerQuery(dimensions=["shipment.weight", "depot.name"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        err = ei.value
        assert err.reason is PopulationErrorReason.NO_VIABLE_CANDIDATE
        assert "shipment" in str(err)
        assert "depot" in str(err)
        assert "origin_depot_id" in str(err)
        assert "dest_depot_id" in str(err)
        _assert_stable_prefix(err)

    async def test_ambiguous_datasource_names_both(self, storage) -> None:
        query = SlayerQuery(dimensions=["widgets.x"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        err = ei.value
        assert err.reason is PopulationErrorReason.AMBIGUOUS_DATASOURCE
        assert set(err.datasources) == {DS_WIDGETS_A, DS_WIDGETS_B}
        _assert_stable_prefix(err)

    async def test_an_aggregate_filter_dataset_scopes_the_datasource(self, storage) -> None:
        """A dataset only an aggregate-bearing filter reads still picks the one datasource holding it."""
        await storage.save_dataset(SlayerDataset(
            name="gadgets", data_source=DS_WIDGETS_B, sql_table="gadgets",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="v", datatype=DataType.FLOAT)],
        ))
        query = SlayerQuery.model_validate({"dimensions": ["widgets.x"], "metrics": ["count(widgets.*)"],
                                            "filter": "sum(gadgets.v) > 0"})
        choice = await infer_population(query=query, storage=storage)
        assert choice.data_source == DS_WIDGETS_B

    async def test_zero_candidate_datasources_fails_closed(self, storage) -> None:
        """No single datasource holds every referenced anchor dataset."""
        query = SlayerQuery(dimensions=["widgets.x", "customers.region"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        assert ei.value.reason is PopulationErrorReason.NO_DATASOURCE
        _assert_stable_prefix(ei.value)

    async def test_datasource_argument_pins_scoping(self, storage) -> None:
        """A collision resolves when the execution datasource is supplied."""
        choice = await infer_population(
            query=SlayerQuery(dimensions=["widgets.x"]),
            storage=storage,
            data_source=DS_WIDGETS_A,
        )
        assert choice.dataset_name == "widgets"
        assert choice.data_source == DS_WIDGETS_A

    async def test_sibling_anchored_stage_refs_fail_closed(self, storage) -> None:
        """A stage omitting source_model whose dims anchor at a sibling name."""
        query = SlayerQuery(dimensions=["s1.total"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage, sibling_stage_names={"s1"})
        err = ei.value
        assert err.reason is PopulationErrorReason.SIBLING_STAGE
        assert "s1" in str(err)
        assert "population" in str(err)  # advice: name the sibling explicitly
        _assert_stable_prefix(err)

    async def test_measure_only_without_datasource_scopes_by_its_metrics(self, storage) -> None:
        """A metrics-only query with no data_source scopes by its metric homes."""
        query = SlayerQuery.model_validate({"metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]})
        resp = await _dry_run(storage, query)
        assert resp.population is None

    async def test_aggregate_filter_only_has_the_unit_as_its_anchor(self, storage) -> None:
        """Aggregate-only filter, no dims, no data_source: the unit, not a population error."""
        resp = await _dry_run(storage, SlayerQuery(filter=["sum(orders.amount) > 100"]))
        assert resp.population is None

    async def test_measure_referencing_sibling_fails_closed(self, storage) -> None:
        """A metric homed on a sibling stage refuses: its rows come from a stage inference cannot see; name the
        sibling as the population."""
        query = SlayerQuery.model_validate({
            "dimensions": ["customers.region"],
            "metrics": [{"metric": "sum(s1.total)", "alias": "t"}],
        })
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage, sibling_stage_names={"s1"})
        assert ei.value.reason is PopulationErrorReason.SIBLING_STAGE

    async def test_string_literal_is_not_a_sibling_anchor(self, storage) -> None:
        """A dotted value inside a string literal must not be read as a reference
        (so it cannot spuriously anchor at a sibling stage)."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filter=["customers.region = 's1.total'"],
            ),
            storage=storage,
            sibling_stage_names={"s1"},
        )
        assert choice.dataset_name == "customers"

    async def test_short_form_cross_model_dim_routes_when_rootless(self, storage) -> None:
        """Rootless inference routes short forms per candidate,
        so bare ``regions.name`` and the full path infer the same population."""
        short = await infer_population(
            query=SlayerQuery(dimensions=["orders.status", "regions.name"]),
            storage=storage,
        )
        full = await infer_population(
            query=SlayerQuery(dimensions=["orders.status", "customers.regions.name"]),
            storage=storage,
        )
        assert short.dataset_name == full.dataset_name == "orders"

    async def test_error_payload_exposes_all_three_fields(self, storage) -> None:
        """reason/candidates/datasources are always present on the typed error."""
        query = SlayerQuery(dimensions=["prof.bio", "acct.email"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        err = ei.value
        assert isinstance(err.reason, PopulationErrorReason)
        assert isinstance(err.candidates, list)
        assert isinstance(err.datasources, list)
