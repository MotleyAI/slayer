"""DEV-1931 — definition-default home resolution: bare-vs-dotted split +
host-local retention.

A non-overridden definition default resolves from the OWNING model (the source
anchor), a qualifier the owner cannot reach forward falls back to the query root
(a leading root-model name self-strips to root-local), and a genuine host-local
``()`` default is retained so it widens the home exactly as spelling that column
explicitly would. The same owning-model resolution governs input safety, so a
fanning definition default fails closed even when the home widens away from the
declaring model.

Home paths are asserted from ``Aggregate.home_path`` (mirroring
test_dev1832_home); executed values compare each default against its explicit
``weight=`` twin (or an independent expression oracle). The fail-closed cases
raise during SQL generation.
"""

from __future__ import annotations

import pytest

from slayer.core.keys import AggregateKey
from slayer.engine.elaborate import elaborate_query
from slayer.ir.terms import Aggregate

from tests._dev1832_fixtures import (
    ModelMeasure,
    SlayerQuery,
    bundle,
    gen,
    make_exec_engine,
    orders_q,
)
from tests._dev1931_fixtures import (
    WSUM_HOST_VALUE,
    WSUM_MIXED_VALUE,
    ambiguous_owner_models,
    dev1931_models,
)


def _home_path(formula: str, *, dimensions: list[str] | None = None) -> tuple:
    """The ``home_path`` of a measure's outer aggregate over the DEV-1931 graph."""
    elab = elaborate_query(
        query=orders_q(
            dimensions=dimensions or [],
            measures=[ModelMeasure(formula=formula, name="m")]),
        bundle=bundle(dev1931_models()))
    assert elab.prebound is not None
    root = elab.prebound.declared_measures[-1].bound.value_key
    assert isinstance(root, AggregateKey), root
    term = elab.terms[root]
    assert isinstance(term, Aggregate), term
    return term.home_path


class TestDefinitionDefaultHomePath:
    """The home the default resolves to, once per definition default frame."""

    def test_root_local_direct_default_widens_to_root(self):
        # orders.amount is the query root's own column → the default is retained
        # as a genuine `()` home candidate, exactly like the explicit weight.
        assert _home_path("customers.spend:wsum_host") == ()
        assert _home_path("customers.spend:wsum_host(weight=orders.amount)") == ()

    def test_root_local_expression_default_widens_to_root(self):
        assert _home_path("customers.spend:wsum_host_expr") == ()

    def test_mixed_frame_expression_default_widens_to_root(self):
        # spend resolves owner-local, orders.amount root-local; the root-local ref
        # retains `()`, so the home is the root — per-reference, not whole-from-root.
        assert _home_path("customers.spend:wsum_mixed") == ()

    def test_owner_reachable_dotted_default_homes_like_explicit(self):
        # regions.pop resolves owner-relative to customers.regions.pop (deeper than
        # the source), so it homes exactly as spelling that explicitly would.
        assert (_home_path("customers.spend:wsum_regions_pop")
                == _home_path(
                    "customers.spend:wsum_regions_pop(weight=customers.regions.pop)")
                == ("customers",))

    def test_bare_default_stays_owner_local(self):
        # A bare `spend` resolves to the owner's customers.spend — never resurrected
        # as a bogus root-local `()` by the retention change.
        assert _home_path("customers.spend:wsum_bare") == ("customers",)

    def test_bare_default_at_anchor_never_widens(self):
        # A multi-leaf expression source anchored at customers with a bare default:
        # the bare default sits at the anchor and never widens the home.
        assert _home_path(
            "wsum_bare(customers.spend + customers.discount)") == ("customers",)


class TestDefinitionDefaultExecution:
    """Executed values: each default matches its explicit twin / an oracle."""

    @pytest.fixture(params=["sqlite", "duckdb"])
    async def engine(self, request):
        async for e in make_exec_engine(request, models=dev1931_models()):
            yield e

    @staticmethod
    async def _value(engine, formula: str) -> float:
        resp = await engine.execute(
            orders_q(measures=[ModelMeasure(formula=formula, name="w")]))
        return float(resp.data[0]["orders.w"])

    async def test_root_local_direct_default_matches_explicit(self, engine):
        explicit = await self._value(
            engine, "customers.spend:wsum_host(weight=orders.amount)")
        assert explicit == pytest.approx(WSUM_HOST_VALUE)
        default = await self._value(engine, "customers.spend:wsum_host")
        assert default == pytest.approx(explicit)

    async def test_root_local_expression_default_matches_explicit(self, engine):
        explicit = await self._value(
            engine, "customers.spend:wsum_host_expr(weight=orders.amount)")
        assert explicit == pytest.approx(WSUM_HOST_VALUE)
        default = await self._value(engine, "customers.spend:wsum_host_expr")
        assert default == pytest.approx(explicit)

    async def test_mixed_frame_default_resolves_per_reference(self, engine):
        # Independent oracle (a plain cross-model expression sum, works today):
        # spend broadcast onto each order + the order's own amount.
        oracle = await self._value(
            engine, "sum(customers.spend * (customers.spend + amount))")
        assert oracle == pytest.approx(WSUM_MIXED_VALUE)
        default = await self._value(engine, "customers.spend:wsum_mixed")
        assert default == pytest.approx(oracle)

    async def test_owner_reachable_dotted_default_matches_explicit(self, engine):
        explicit = await self._value(
            engine, "customers.spend:wsum_regions_pop(weight=customers.regions.pop)")
        default = await self._value(engine, "customers.spend:wsum_regions_pop")
        assert default == pytest.approx(explicit)

    async def test_bare_default_matches_explicit(self, engine):
        explicit = await self._value(
            engine, "customers.spend:wsum_bare(weight=customers.spend)")
        default = await self._value(engine, "customers.spend:wsum_bare")
        assert default == pytest.approx(explicit)


class TestFanningDefinitionDefaultFailsClosed:
    """A fanning definition default fails closed even when the home widens (F1):
    the fanning default is resolved on the declaring model, not omitted from
    safety because another input widened the home."""

    def test_home_widens_to_customers(self):
        # w1=customers.spend pulls the home up from regions to customers.
        assert _home_path("customers.regions.pop:wfan_widen") == ("customers",)

    async def test_fanning_default_fails_closed(self):
        with pytest.raises(ValueError, match="(?i)unproven join hop|fanning") as ei:
            await gen(
                orders_q(measures=[ModelMeasure(
                    formula="customers.regions.pop:wfan_widen", name="m")]),
                models=dev1931_models())
        assert "region_events" in str(ei.value)


class TestUnresolvableDefaultFailsClosed:
    """A qualifier the owner-first resolution can reach from neither owner nor
    root, a partially-resolvable owner reference, and an ambiguous owner hop all
    fail closed — never a silent `()` and never a silent re-anchor at the root."""

    async def test_qualifier_unreachable_from_owner_and_root(self):
        with pytest.raises(ValueError):
            await gen(
                orders_q(measures=[ModelMeasure(
                    formula="customers.spend:wsum_nowhere", name="m")]),
                models=dev1931_models())

    async def test_partially_resolvable_owner_reference(self):
        # regions.plans.fee: the first hop `regions` resolves from the owner
        # customers, the second hop `plans` is missing on regions → fail closed.
        with pytest.raises(ValueError):
            await gen(
                orders_q(measures=[ModelMeasure(
                    formula="customers.spend:wsum_partial", name="m")]),
                models=dev1931_models())

    async def test_ambiguous_owner_hop_never_reanchors_at_root(self):
        # ag is ambiguous from the owner o but cleanly reachable from the root r;
        # the default must fail closed, never re-anchor at r.
        with pytest.raises(ValueError):
            await gen(
                SlayerQuery(source_model="r", measures=[ModelMeasure(
                    formula="o.val:wscore", name="m")]),
                models=ambiguous_owner_models())
