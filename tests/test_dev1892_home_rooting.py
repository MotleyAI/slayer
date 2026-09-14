"""DEV-1892 — the full home rule (Axiom 2): a cross-model aggregate roots at the
dataset determining every input, in every mode; a source beyond the home renders
inline as a host-locus aggregate joining the to-one path. Executed oracles +
shape pins. Spec: queries/attribution-modes, queries/semantics."""

from __future__ import annotations

import pytest

from slayer.core.keys import ColumnKey
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1840_fixtures import dev1840_models, gen
from tests._dev1841_fixtures import (
    ModelMeasure,
    assoc_q,
    bcast_q,
    broadcast_warnings,
    make_exec_engine,
    status_key,
)
from tests._dev1892_fixtures import (
    derived_default_models,
    fanning_derived_source_models,
    toone_filter_models,
)

#: wsum5 default = regions.derived_pop (= pop*2): SUM(spend * pop*2) over distinct
#: customers = 2× the physical-pop total.
DERIVED_DEFAULT_BY_STATUS = {"ok": 112000.0, "new": 50000.0}

#: C broadcast: weighted_avg(regions.pop, weight=spend) over all 7 customers
#: (c4's NULL pop drops its numerator term, its spend stays in the denominator).
CBCAST_ALL_CUSTOMERS = 67000.0 / 515.0
#: host home (roots at orders, status attributable, no broadcast):
#: SUM(spend*amount)/SUM(amount) per status (o8's NULL customer → denom only).
HOST_HOME_BY_STATUS = {"ok": 7660.0 / 82.0, "new": 7350.0 / 85.0}
#: filtered source (north_spend on regions.name='North') × host weight:
#: SUM(north_spend*amount) per status, North customers (c1,c2,c6) only.
FILTERED_WSUM_BY_STATUS = {"ok": 6160.0, "new": 5750.0}
#: source-owned default beyond the source (wsum4 scale=regions.pop):
#: SUM(spend*amount*pop) per status (c4 has no region → NULL term).
WSUM4_BY_STATUS = {"ok": 916000.0, "new": 575000.0}

_C_BROADCAST = "customers.regions.pop:weighted_avg(weight=customers.spend)"


def _orders_bundle() -> ResolvedSourceBundle:
    models = dev1840_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _association_attaches(planned) -> list:
    return [a for a in planned.regroup_attach_plans
            if getattr(a.kernel, "kind", None) == "association"]


def _vals(resp, measure):
    return {k[0]: v[measure] for k, v in status_key(resp).items()}


@pytest.fixture(params=["sqlite", "duckdb"])
async def orders_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def filter_engine(request):
    async for engine in make_exec_engine(request, models=toone_filter_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def derived_default_engine(request):
    async for engine in make_exec_engine(request, models=derived_default_models()):
        yield engine


class TestBroadcastHomeRule:
    async def test_source_deeper_than_home_broadcasts_from_customers(self, orders_engine):
        """C broadcast roots at customers (weight=spend forces the home shallower than the source) and repeats across status."""
        resp = await orders_engine.execute(bcast_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=_C_BROADCAST, name="w")]))
        vals = _vals(resp, "orders.w")
        for v in vals.values():
            assert float(v) == pytest.approx(CBCAST_ALL_CUSTOMERS)
        [w] = broadcast_warnings(resp)
        assert "status" in str(w)

    async def test_host_home_is_attributable_and_not_broadcast(self, orders_engine):
        """Home == host (orders determines both) → status attributable, a per-status weighted avg with no broadcast warning."""
        resp = await orders_engine.execute(bcast_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:weighted_avg(weight=amount)", name="w")]))
        vals = _vals(resp, "orders.w")
        for status, expected in HOST_HOME_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)
        assert not broadcast_warnings(resp)


class TestHostLocusSourceRendering:
    async def test_filtered_source_with_home_above_it(self, filter_engine):
        """A measure-local filter (north_spend) renders from the host home, joining the customers→regions hop."""
        resp = await filter_engine.execute(bcast_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.north_spend:wsum(weight=amount)", name="w")]))
        vals = _vals(resp, "orders.w")
        for status, expected in FILTERED_WSUM_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_source_owned_default_beyond_source(self, filter_engine):
        """A source-owned default over a to-one path (wsum4 scale=regions.pop) resolves on the source model and joins its hop."""
        resp = await filter_engine.execute(bcast_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:wsum4(weight=amount)", name="w")]))
        vals = _vals(resp, "orders.w")
        for status, expected in WSUM4_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)


class TestHomeRootingPlanShape:
    def test_associate_entity_key_is_the_home_entity(self) -> None:
        """C in associate mode deduplicates by the home (customers) entity."""
        planned = plan_query(
            query=assoc_q(dimensions=["status"],
                          measures=[ModelMeasure(formula=_C_BROADCAST, name="w")]),
            bundle=_orders_bundle())
        [attach] = _association_attaches(planned)
        assert attach.kernel.entity_keys == [ColumnKey(path=("customers",), leaf="id")]

    async def test_broadcast_producer_reads_from_customers(self) -> None:
        """The broadcast producer CTE reads FROM customers with a LEFT JOIN regions, never FROM regions."""
        sql = await gen(bcast_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=_C_BROADCAST, name="w")]), dialect="sqlite")
        assert "FROM customers AS customers" in sql
        assert "LEFT JOIN regions" in sql
        assert "FROM regions" not in sql
        assert_scope_closed(sql)


class TestDerivedColumnEdgeCases:
    async def test_derived_dotted_default_expands(self, derived_default_engine):
        """A to-one-path default to a DERIVED column expands its SQL (pop*2), not a
        bare (nonexistent) column reference."""
        resp = await derived_default_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum5", name="w")]))
        vals = _vals(resp, "orders.w")
        for status, expected in DERIVED_DEFAULT_BY_STATUS.items():
            assert float(vals[status]) == pytest.approx(expected)

    async def test_fanning_derived_source_fails_closed(self) -> None:
        """A host-locus source whose Column.sql crosses a fanning hop is rejected,
        never emitted as a multiplying join."""
        with pytest.raises(ValueError, match="unproven join hop"):
            await gen(
                bcast_q(dimensions=["status"], measures=[ModelMeasure(
                    formula="customers.bad_spend:weighted_avg(weight=amount)",
                    name="w")]),
                models=fanning_derived_source_models())


class TestCardinalityInvariant:
    async def test_adding_broadcast_measure_keeps_rows_and_siblings(self, orders_engine):
        """Adding the broadcast C measure keeps the status rows and amount:sum sibling values (system principle 8)."""
        base = await orders_engine.execute(bcast_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        withw = await orders_engine.execute(bcast_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:sum", name="tot"),
                      ModelMeasure(formula=_C_BROADCAST, name="w")]))
        a = _vals(base, "orders.tot")
        b = _vals(withw, "orders.tot")
        assert set(a) == set(b)
        for k in a:
            assert float(a[k]) == pytest.approx(float(b[k]))
