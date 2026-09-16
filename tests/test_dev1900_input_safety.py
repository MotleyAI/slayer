"""DEV-1900 — aggregate inputs fail closed on a fanning derived dependency.

Every input role — positional/keyword argument, the aggregate's source, a
definition default (bare, dotted, or expression), a measure-level column filter,
a re-aggregation parameter — that names a derived column whose definition
crosses the 1:N ``regions → region_events`` hop must fail closed with the
existing unproven-join-hop / parameter error, in every mode; a definition no
dialect can parse fails with the new ``check_input_dependencies_analyzable``
typed error. To-one derived dependencies stay legal.

Covers ``queries/cross-model-aggregates`` (Unsafe aggregate inputs fail closed)
and the re-aggregation-parameter scenarios of ``queries/semantics``.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.engine.elaborate_env import check_input_dependencies_analyzable

from tests._dev1892_fixtures import assert_grain_residue, assert_ref_free
from tests._dev1900_fixtures import (
    ModelMeasure,
    REAGG_BAD,
    REAGG_DERIVED_POP_WAVG,
    REAGG_GOOD,
    make_exec_engine,
    orders_q,
    unparseable_derived_models,
)

UNPROVEN = "unproven join hop"
HOP = "region_events"
MODES = ["broadcast", "associate", "error"]

CM_KWARG = ModelMeasure(
    formula="customers.spend:weighted_avg(weight=customers.regions.bad_pop)", name="w")
CM_POSITIONAL = ModelMeasure(
    formula="customers.spend:weighted_avg(customers.regions.bad_pop)", name="w")
CM_KWARG_CHAIN = ModelMeasure(
    formula="customers.spend:weighted_avg(weight=customers.regions.bad_pop2)", name="w")
LOCAL_KWARG = ModelMeasure(
    formula="amount:weighted_avg(weight=customers.regions.bad_pop)", name="w")
SOURCE_FANNING = ModelMeasure(formula="customers.regions.bad_pop:sum", name="w")  # source crosses the hop
CM_DEFAULT = ModelMeasure(formula="customers.spend:wsumx", name="w")       # dotted default regions.bad_pop
CM_DEFAULT_BARE = ModelMeasure(formula="customers.regions.pop:wbadbare", name="w")  # bare owner-local default bad_pop
CM_DEFAULT_EXPR = ModelMeasure(formula="customers.spend:wsumy", name="w")  # expr default regions.bad_pop * 1
CM_FILTER = ModelMeasure(formula="customers.bad_pop_spend:sum", name="w")  # measure-local regions.bad_pop > 0
CM_GOOD = ModelMeasure(formula="customers.spend:wgood", name="w")          # default regions.derived_pop (to-one)
CM_UNPARSE = ModelMeasure(formula="customers.spend:wunparse", name="w")
CM_EXPR_UNPARSE = ModelMeasure(formula="customers.spend:wexpr_unparse", name="w")  # unparseable expr default → unnameable

#: wgood (to-one derived default) executes today; behaviour-preserving pin.
WGOOD_TOONE_DEFAULT = 134000.0


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_engine(request):
    async for e in make_exec_engine(request, models=unparseable_derived_models()):
        yield e


def _assert_unproven(exc: ValueError) -> None:
    msg = str(exc)
    assert UNPROVEN in msg, msg
    assert HOP in msg, f"error must name the fanning hop {HOP!r}: {msg!r}"
    assert_ref_free(msg)


async def _assert_fails_closed(engine, measure, *, mode=None):
    kw = {} if mode is None else {"to_many_handling": mode}
    q = orders_q(measures=[measure], **kw)
    with pytest.raises(ValueError) as ei:
        await engine.execute(q)
    _assert_unproven(ei.value)


class TestPathBearingDerivedArgument:
    @pytest.mark.parametrize("mode", MODES)
    async def test_cross_model_kwarg(self, engine, mode):
        await _assert_fails_closed(engine, CM_KWARG, mode=mode)

    async def test_cross_model_positional(self, engine):
        await _assert_fails_closed(engine, CM_POSITIONAL)

    async def test_chain_kwarg(self, engine):
        """bad_pop2 = bad_pop * 2 inherits the crossing recursively."""
        await _assert_fails_closed(engine, CM_KWARG_CHAIN)

    @pytest.mark.parametrize("mode", MODES)
    async def test_local_kwarg(self, engine, mode):
        await _assert_fails_closed(engine, LOCAL_KWARG, mode=mode)


class TestFanningDerivedSource:
    async def test_source_crossing_fails_closed(self, engine):
        """A cross-model aggregate whose SOURCE is a fanning derived column fans
        the aggregate exactly as a crossing argument does."""
        await _assert_fails_closed(engine, SOURCE_FANNING)


class TestDefinitionDefault:
    @pytest.mark.parametrize("mode", MODES)
    async def test_dotted_default(self, engine, mode):
        await _assert_fails_closed(engine, CM_DEFAULT, mode=mode)

    async def test_bare_default(self, engine):
        await _assert_fails_closed(engine, CM_DEFAULT_BARE)

    @pytest.mark.parametrize("mode", MODES)
    async def test_expression_default(self, engine, mode):
        await _assert_fails_closed(engine, CM_DEFAULT_EXPR, mode=mode)


class TestMeasureLocalFilter:
    @pytest.mark.parametrize("mode", MODES)
    async def test_fanning_derived_filter(self, engine, mode):
        await _assert_fails_closed(engine, CM_FILTER, mode=mode)


class TestUnanalyzableDefinition:
    async def test_unparseable_definition_typed_error(self, unparse_engine):
        """No dialect parses regions.unparseable; the dependency is unsafe, not
        empty — the typed analyzability error names the column and the dialect
        failure, never the raw parser error."""
        q = orders_q(measures=[CM_UNPARSE])
        with pytest.raises(ValueError) as ei:
            await unparse_engine.execute(q)
        msg = str(ei.value)
        assert "unparseable" in msg, f"error must name the column: {msg!r}"
        assert "dialect" in msg, f"error must name the analyzability failure: {msg!r}"
        assert_ref_free(msg)

    async def test_unparseable_column_filter_typed_error(self, unparse_engine):
        """A source column's ``filter=`` referencing an unanalyzable derived
        column is an input dependency too — fail closed, never stamped ``()``."""
        q = orders_q(measures=[ModelMeasure(
            formula="customers.flagged_spend:sum", name="fs")])
        with pytest.raises(ValueError) as ei:
            await unparse_engine.execute(q)
        msg = str(ei.value)
        assert "flagged_spend" in msg, f"error must name the filtered column: {msg!r}"
        assert "dialect" in msg, f"error must name the analyzability failure: {msg!r}"
        assert_ref_free(msg)

    async def test_unnameable_unanalyzable_input_fails_closed(self, unparse_engine):
        """An unparseable EXPRESSION default has no single nameable column
        (expr_refs is (None,)): the closure is None while the diagnostic returns
        None, so the guard must still fail closed rather than leak the failure to
        the renderer (CodeRabbit; without the fix the guard is a no-op)."""
        q = orders_q(measures=[CM_EXPR_UNPARSE])
        with pytest.raises(ValueError, match="no supported dialect can analyse"):
            await unparse_engine.execute(q)


class TestInputDependencyCheckerAlwaysRaises:
    """``check_input_dependencies_analyzable`` runs ONLY once the input closure
    is None, so it always fails closed — a nameable column enriches the message,
    a missing one still raises (CodeRabbit; the old ``column is None`` early
    return silently passed an unanalysable dependency)."""

    def test_named_column_raises_naming_it(self):
        with pytest.raises(ValueError, match="derived column 'bad_pop'"):
            check_input_dependencies_analyzable(alias="w", column="bad_pop")

    def test_unnameable_dependency_still_raises(self):
        with pytest.raises(ValueError, match="has an input dependency"):
            check_input_dependencies_analyzable(alias="w", column=None)


class TestReaggregationParameter:
    async def test_fanning_derived_parameter_fails_closed(self, engine):
        q = orders_q(measures=[REAGG_BAD])
        with pytest.raises(SlayerError) as ei:
            await engine.execute(q)
        assert_grain_residue(ei.value, param="weight")


class TestLegalDerivedDependencies:
    async def test_to_one_derived_default_executes(self, engine):
        resp = await engine.execute(orders_q(measures=[CM_GOOD]))
        assert float(resp.data[0]["orders.w"]) == pytest.approx(WGOOD_TOONE_DEFAULT)

    async def test_local_only_derived_reagg_parameter_executes(self, engine):
        """weight = regions.derived_pop (= pop * 2, local) over a grain pinning
        regions by its key: each region cell weighted by twice its population."""
        resp = await engine.execute(orders_q(measures=[REAGG_GOOD]))
        assert float(resp.data[0]["orders.ra"]) == pytest.approx(REAGG_DERIVED_POP_WAVG)
