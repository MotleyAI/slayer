"""DEV-1871 — rootless population inference is route-aware.

Covers the ``queries/population`` delta (short-form dims probe via the same
safe-route enumeration binding applies, per candidate) and the
``queries/dotted-dimension-routing`` delta scenario "Inference and binding
agree on a routed short form".

Tests marked xfail(strict) pin the target behaviour and flip green when the
route-aware probe lands (tasks group 19); unmarked tests are guard pins that
already hold and must survive the restructure.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.query import SlayerQuery
from slayer.engine.dimension_routing import short_form_route_or_none
from slayer.engine.population import evaluate_candidates, infer_population

from tests._dev1866_fixtures import (
    DS_CHAIN,
    chain_models_by_name,
    make_chain_exec_engine,
)
from tests._dev1871_fixtures import (
    DS_LIT,
    fanroute_models_by_name,
    lit_models_by_name,
    make_routing_storage,
)

_ROUTE_XFAIL = pytest.mark.xfail(
    strict=True, reason="route-aware rootless inference lands with DEV-1871 group 19",
)


@pytest.fixture
async def storage():
    return await make_routing_storage()


@pytest.fixture(params=["sqlite", "duckdb"])
async def chain_engine(request):
    async for engine in make_chain_exec_engine(request.param):
        yield engine


def _canon(rows: list[dict]) -> list[tuple]:
    return sorted((tuple(sorted(r.items())) for r in rows), key=repr)


@_ROUTE_XFAIL
class TestShortFormParity:
    """Spec: Short-form dimension infers like its full path."""

    async def test_short_form_infers_like_full_path(self, storage) -> None:
        short = await infer_population(
            query=SlayerQuery(dimensions=["orders.status", "regions.name"]),
            storage=storage,
        )
        full = await infer_population(
            query=SlayerQuery(dimensions=["orders.status", "customers.regions.name"]),
            storage=storage,
        )
        assert short.model_name == full.model_name == "orders"
        assert short.data_source == DS_CHAIN

    async def test_raw_row_mode_routes_identically(self, storage) -> None:
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["orders.status", "regions.name"],
                distinct_dimension_values=False,
            ),
            storage=storage,
        )
        assert choice.model_name == "orders"


class TestLiteralPrecedence:
    """Spec: Literal resolution beats short-form reinterpretation."""

    async def test_direct_edge_is_never_reinterpreted_through_a_route(
        self, storage
    ) -> None:
        """stores' direct unknown-cardinality edge to plants is used literally;
        the to-one route via hub must not be substituted for it."""
        models = lit_models_by_name()
        assert short_form_route_or_none(
            root=models["stores"], target_model="plants", models_by_name=models,
        ) == ["hub", "plants"]

        query = SlayerQuery(dimensions=["stores.kind", "plants.name"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        assert ei.value.reason is PopulationErrorReason.NO_VIABLE_CANDIDATE

    async def test_full_routed_path_resolves_the_same_query(self, storage) -> None:
        choice = await infer_population(
            query=SlayerQuery(dimensions=["stores.kind", "hub.plants.name"]),
            storage=storage,
        )
        assert choice.model_name == "stores"
        assert choice.data_source == DS_LIT


class TestFanningRoute:
    """Spec: Unique but fanning route is not determination."""

    async def test_unique_fanning_route_fails_closed_for_inference(
        self, storage
    ) -> None:
        """Binding routes firm → desks (unique route, fans out at office →
        desks); determination must reject it — and the full-path spelling
        fails identically."""
        models = fanroute_models_by_name()
        assert short_form_route_or_none(
            root=models["firm"], target_model="desks", models_by_name=models,
        ) is not None

        for dims in (
            ["firm.title", "desks.id_tag"],
            ["firm.title", "office.desks.id_tag"],
        ):
            with pytest.raises(PopulationInferenceError) as ei:
                await infer_population(
                    query=SlayerQuery(dimensions=dims), storage=storage,
                )
            assert ei.value.reason is PopulationErrorReason.NO_VIABLE_CANDIDATE
            assert set(ei.value.candidates) == {"desks", "firm", "office"}


class TestVerdictConsistency:
    """Spec: a fanning-unique route is unreachable, never ambiguous, under
    either spelling — pinned at the probe-verdict level."""

    def test_unreachable_verdict_matches_between_spellings(self) -> None:
        models = fanroute_models_by_name()
        for item in ("firm.title", "office.firm.title"):
            verdict = evaluate_candidates(
                items=[item], candidates=["desks"], models_by_name=models,
            )[0]
            assert not verdict.viable
            assert verdict.unreachable_items == [item]
            assert not verdict.ambiguous_blocked


@_ROUTE_XFAIL
class TestAmbiguousRoutes:
    async def test_two_safe_routes_fail_closed_as_ambiguous(self, storage) -> None:
        """A routed probe with two fan-out-free routes reports the same
        AMBIGUOUS verdict a literal parallel-edge hop does."""
        query = SlayerQuery(dimensions=["sale.code", "city.name"])
        with pytest.raises(PopulationInferenceError) as ei:
            await infer_population(query=query, storage=storage)
        err = ei.value
        assert err.reason is PopulationErrorReason.AMBIGUOUS_PATH
        assert "sale" in str(err)
        assert "city.name" in str(err)


@_ROUTE_XFAIL
class TestRoutedHops:
    def test_hops_counted_along_the_selected_route(self) -> None:
        """Spec: hops are counted along the selected route (orders reaches
        regions.name in 2 routed hops; customers literally in 1; regions in 0)."""
        verdicts = {
            v.model_name: v
            for v in evaluate_candidates(
                items=["regions.name"],
                candidates=["customers", "orders", "regions"],
                models_by_name=chain_models_by_name(),
            )
        }
        assert all(v.viable for v in verdicts.values())
        assert verdicts["regions"].total_hops == 0
        assert verdicts["customers"].total_hops == 1
        assert verdicts["orders"].total_hops == 2


@_ROUTE_XFAIL
class TestInferenceBindingAgreement:
    """Spec (dotted-dimension-routing): Inference and binding agree on a routed short form."""

    async def test_rootless_routed_short_form_matches_explicit_twin(
        self, chain_engine
    ) -> None:
        rootless = SlayerQuery(dimensions=["orders.status", "regions.name"])
        explicit = SlayerQuery(
            source_model="orders", dimensions=["orders.status", "regions.name"],
        )
        r = await chain_engine.execute(rootless)
        e = await chain_engine.execute(explicit)
        assert r.sql == e.sql
        assert _canon(r.data) == _canon(e.data)
        assert r.columns == e.columns
        assert r.population == e.population == "orders"
        assert r.population_inferred is True
        assert e.population_inferred is False

        status_col = next(c for c in r.columns if c.endswith(".status"))
        region_col = next(c for c in r.columns if c.endswith("regions.name"))
        assert {(row[status_col], row[region_col]) for row in r.data} == {
            ("ok", "North"), ("cancel", "North"), ("ok", "South"),
        }
