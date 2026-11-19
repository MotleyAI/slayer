"""Aggregate inputs fail closed on a fanning derived dependency.

Every input role — positional/keyword argument, the aggregate's source, a
definition default (bare, dotted, or expression), a metric-level field filter,
a re-aggregation parameter — that names a derived field whose definition
crosses the 1:N ``regions → region_events`` hop must fail closed with the
existing unproven-relationship-hop / parameter error, in every mode; a definition no
dialect can parse fails with the new ``check_input_dependencies_analyzable``
typed error. To-one derived dependencies stay legal.

Covers ``queries/cross-model-aggregates`` (Unsafe aggregate inputs fail closed)
and the re-aggregation-parameter scenarios of ``queries/semantics``.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError, UnanalyzableAggregationParameterError
from slayer.engine.elaborate_env import check_input_dependencies_analyzable

from slayer.core.query import QueryMetric

from tests._dev1892_fixtures import assert_grain_residue, assert_ref_free
from tests._dev1900_fixtures import (
    REAGG_BAD,
    REAGG_DERIVED_POP_WAVG,
    REAGG_GOOD,
    make_exec_engine,
    orders_q,
    unparseable_derived_datasets,
)

UNPROVEN = "unproven relationship hop"
HOP = "region_events"
MODES = ["broadcast", "associate", "error"]

CM_KWARG = QueryMetric(
    metric="weighted_avg(customers.spend, weight=customers.regions.bad_pop)", alias="w")
CM_POSITIONAL = QueryMetric(
    metric="weighted_avg(customers.spend, customers.regions.bad_pop)", alias="w")
CM_KWARG_CHAIN = QueryMetric(
    metric="weighted_avg(customers.spend, weight=customers.regions.bad_pop2)", alias="w")
LOCAL_KWARG = QueryMetric(
    metric="weighted_avg(amount, weight=customers.regions.bad_pop)", alias="w")
SOURCE_FANNING = QueryMetric(metric="sum(customers.regions.bad_pop)", alias="w")  # source crosses the hop
CM_DEFAULT = QueryMetric(metric="wsumx(customers.spend)", alias="w")       # dotted default regions.bad_pop
CM_DEFAULT_BARE = QueryMetric(metric="wbadbare(customers.regions.pop)", alias="w")  # bare owner-local default bad_pop
CM_DEFAULT_EXPR = QueryMetric(metric="wsumy(customers.spend)", alias="w")  # expr default regions.bad_pop * 1
CM_FILTER = QueryMetric(metric="sum(customers.bad_pop_spend)", alias="w")  # metric-local regions.bad_pop > 0
CM_GOOD = QueryMetric(metric="wgood(customers.spend)", alias="w")          # default regions.derived_pop (to-one)
CM_UNPARSE = QueryMetric(metric="wunparse(customers.spend)", alias="w")
CM_EXPR_UNPARSE = QueryMetric(metric="wexpr_unparse(customers.spend)", alias="w")  # unparseable expr default → unnameable

#: wgood (to-one derived default) executes today; behaviour-preserving pin.
WGOOD_TOONE_DEFAULT = 134000.0


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_engine(request):
    async for e in make_exec_engine(request, datasets=unparseable_derived_datasets()):
        yield e


def _assert_unproven(exc: ValueError) -> None:
    msg = str(exc)
    assert UNPROVEN in msg, msg
    assert HOP in msg, f"error must name the fanning hop {HOP!r}: {msg!r}"
    assert_ref_free(msg)


async def _assert_fails_closed(engine, metric, *, mode=None):
    kw = {} if mode is None else {"to_many_handling": mode}
    q = orders_q(metrics=[metric], **kw)
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
        """A cross-dataset aggregate whose SOURCE is a fanning derived field fans
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
        empty — the typed analyzability error names the field and the dialect
        failure, never the raw parser error."""
        q = orders_q(metrics=[CM_UNPARSE])
        with pytest.raises(ValueError) as ei:
            await unparse_engine.execute(q)
        msg = str(ei.value)
        assert "unparseable" in msg, f"error must name the field: {msg!r}"
        assert "dialect" in msg, f"error must name the analyzability failure: {msg!r}"
        assert_ref_free(msg)

    async def test_unparseable_column_filter_typed_error(self, unparse_engine):
        """A source field's ``filter=`` referencing an unanalyzable derived
        field is an input dependency too — fail closed, never stamped ``()``."""
        q = orders_q(metrics=[QueryMetric(
            metric="sum(customers.flagged_spend)", alias="fs")])
        with pytest.raises(ValueError) as ei:
            await unparse_engine.execute(q)
        msg = str(ei.value)
        assert "flagged_spend" in msg, f"error must name the filtered field: {msg!r}"
        assert "dialect" in msg, f"error must name the analyzability failure: {msg!r}"
        assert_ref_free(msg)

    async def test_unnameable_unanalyzable_input_fails_closed(self, unparse_engine):
        """An unparseable EXPRESSION default has no single nameable field
        (expr_refs is (None,)): the closure is None while the diagnostic returns
        None, so the guard must still fail closed rather than leak the failure to
        the renderer (CodeRabbit; without the fix the guard is a no-op)."""
        q = orders_q(metrics=[CM_EXPR_UNPARSE])
        with pytest.raises(UnanalyzableAggregationParameterError, match="weight"):
            await unparse_engine.execute(q)


class TestInputDependencyCheckerAlwaysRaises:
    """``check_input_dependencies_analyzable`` runs ONLY once the input closure
    is None, so it always fails closed — a nameable field enriches the message,
    a missing one still raises (CodeRabbit; the old ``column is None`` early
    return silently passed an unanalysable dependency)."""

    def test_named_column_raises_naming_it(self):
        with pytest.raises(ValueError, match="derived field 'bad_pop'"):
            check_input_dependencies_analyzable(alias="w", field="bad_pop")

    def test_unnameable_dependency_still_raises(self):
        with pytest.raises(ValueError, match="has an input dependency"):
            check_input_dependencies_analyzable(alias="w", field=None)


class TestReaggregationParameter:
    async def test_fanning_derived_parameter_fails_closed(self, engine):
        q = orders_q(metrics=[REAGG_BAD])
        with pytest.raises(SlayerError) as ei:
            await engine.execute(q)
        assert_grain_residue(ei.value, param="weight")


class TestLegalDerivedDependencies:
    async def test_to_one_derived_default_executes(self, engine):
        resp = await engine.execute(orders_q(metrics=[CM_GOOD]))
        assert float(resp.data[0]["orders.w"]) == pytest.approx(WGOOD_TOONE_DEFAULT)

    async def test_local_only_derived_reagg_parameter_executes(self, engine):
        """weight = regions.derived_pop (= pop * 2, local) over a grain pinning
        regions by its key: each region cell weighted by twice its population."""
        resp = await engine.execute(orders_q(metrics=[REAGG_GOOD]))
        assert float(resp.data[0]["orders.ra"]) == pytest.approx(REAGG_DERIVED_POP_WAVG)
