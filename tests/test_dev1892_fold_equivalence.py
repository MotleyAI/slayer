"""DEV-1892 task 1.4 — the folded pick path renders every value-source shape
identically, whether or not a parameter rides alongside: a to-one-path measure
filter and ``*:count`` keep their executed values (fold regression), a
cross-model to-one derived source carries a weight, and a definition default
anchored on the owner binds the TARGET column even when the host shares the bare
name.

Spec: queries/attribution-modes — "Association eligibility and input handling";
queries/semantics — "Definition-default parameter follows the same rule".
"""

from __future__ import annotations

import pytest

from tests._dev1841_fixtures import (
    ASSOC_COUNT_BY_STATUS,
    ModelMeasure,
    assoc_q,
    make_exec_engine,
    status_key,
)
from tests._dev1892_fixtures import (
    ASSOC_WSUM_BY_STATUS,
    expr_default_name_models,
    shared_default_name_models,
    toone_default_models,
    toone_filter_models,
)

#: wsum3 weight default = to-one path regions.pop, picked once per entity:
#: SUM(spend*pop) over distinct customers (c4's NULL pop drops its term).
ASSOC_WSUM3_BY_STATUS = {"ok": 56000.0, "new": 25000.0}

#: north_spend = spend filtered to the to-one path regions.name='North' (r1):
#: ok distinct {c1 100, c2 150, c6 30} North; new {c1 100, c2 150} North.
NORTH_SPEND_BY_STATUS = {"ok": 280.0, "new": 250.0}
#: weighted_avg(customers.regions.pop, weight=spend) — value is a multi-hop
#: to-one derived source. ok SUM(pop*spend)/SUM(spend)=56000/420; new drops c4's
#: NULL-pop term but keeps its spend in the denom -> 25000/290.
WAVG_POPVAL_BY_STATUS = {"ok": 56000.0 / 420.0, "new": 25000.0 / 290.0}


@pytest.fixture(params=["sqlite", "duckdb"])
async def assoc_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def toone_filter_engine(request):
    async for engine in make_exec_engine(request, models=toone_filter_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def shared_default_engine(request):
    async for engine in make_exec_engine(request, models=shared_default_name_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def expr_default_engine(request):
    async for engine in make_exec_engine(request, models=expr_default_name_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def toone_default_engine(request):
    async for engine in make_exec_engine(request, models=toone_default_models()):
        yield engine


def _vals(resp, measure):
    return {k[0]: v[measure] for k, v in status_key(resp).items()}


class TestBaseValueFoldRegression:
    async def test_star_count_still_counts_distinct_entities(self, assoc_engine):
        """The fold's no-value (``*:count``) branch is unchanged."""
        resp = await assoc_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.*:count", name="nc")]))
        vals = _vals(resp, "orders.nc")
        for status, n in ASSOC_COUNT_BY_STATUS.items():
            assert int(vals[status]) == n

    async def test_to_one_path_measure_filter_unchanged(self, toone_filter_engine):
        """A measure-local filter over a target-relative to-one path
        (regions.name='North') restricts the association identically after the
        fold."""
        resp = await toone_filter_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.north_spend:sum", name="ns")]))
        vals = _vals(resp, "orders.ns")
        for status, spend in NORTH_SPEND_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(spend)


class TestParameterOverSourceShapes:
    async def test_multi_hop_derived_value_with_weight(self, assoc_engine):
        """A multi-hop to-one derived source (regions.pop) carries a spend
        weight, picked once per entity; NULL-region c4 contributes a NULL numerator
        term."""
        resp = await assoc_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.regions.pop:weighted_avg(weight=customers.spend)",
                name="w")]))
        vals = _vals(resp, "orders.w")
        for status, expected in WAVG_POPVAL_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_default_binds_owner_column_not_host(self, shared_default_engine):
        """The ``wsum`` weight default (bare name ``spend``) binds customers.spend
        even though the host (orders) also declares a ``spend`` column — the
        owner-anchored default, never the host's per-order value."""
        resp = await shared_default_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum", name="w")]))
        vals = _vals(resp, "orders.w")
        for status, expected in ASSOC_WSUM_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_target_relative_expression_default(self, expr_default_engine):
        """An expression default (``spend * 1``) anchors on the owner (customers)
        and evaluates once per entity — SUM(spend * spend)."""
        resp = await expr_default_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum2", name="w")]))
        vals = _vals(resp, "orders.w")
        for status, expected in ASSOC_WSUM_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_to_one_path_default_joins_from_owner(self, toone_default_engine):
        """A definition default over a to-one path (``regions.pop``) anchors on the
        owner (customers) and joins its hop, picked once per entity —
        SUM(spend * pop)."""
        resp = await toone_default_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum3", name="w")]))
        vals = _vals(resp, "orders.w")
        for status, expected in ASSOC_WSUM3_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)
