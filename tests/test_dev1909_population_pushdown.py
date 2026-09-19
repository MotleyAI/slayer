"""DEV-1909 — the query population's row filters restrict by association.

A row-level filter conjunct reaching the population root only across a fanning
(or unproven) hop restricts the population by a correlated semi-join (EXISTS) on
the host base query, so every aggregate over the population rows — inline or in a
host-rooted regroup producer (partitioned, windowed, first/last, host-grain wrap,
nested) — counts each population row once, in every ``to_many_handling`` mode and
whatever the query selects. The DEV-1900 interim guard is retired: its fanning
arm becomes the semi-join, its unanalyzable arm a typed error, and an out-of-scope
conjunct (OR/NOT mixing local and cross-path refs, or several branches) fails
closed only when it would multiply the population.

Covers ``queries/semantics`` (Filters restrict by association or fail loudly;
Grain guarantee) and ``queries/cross-model-aggregates`` (Producer filter routing).
Executed dual-engine (SQLite + DuckDB) against the DEV-1900 fixture graph.
"""

from __future__ import annotations

import warnings as _warnings

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.errors import (
    AssociatedGrainWarning,
    BroadcastGrainWarning,
    SlayerError,
    UnreachableFilterDroppedWarning,
)
from slayer.core.query import ColumnRef, TimeDimension
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1892_fixtures import assert_ref_free
from tests._engine_helpers import _join_aliases
from tests._dev1900_fixtures import (
    AMOUNT_SUM,
    ModelMeasure,
    POP_FILTER_ASSOC_BY_STATUS,
    POP_FILTER_COUNT_BY_TIER,
    POP_FILTER_DERIVED_ASSOC,
    POP_FILTER_LAST_BY_TIER,
    POP_FILTER_NESTED_INNER_BY_TIER,
    POP_FILTER_OUT_OF_SCOPE_TIERS,
    POP_FILTER_PARTITIONED_BY_TIER,
    POP_FILTER_PRODUCER_ONLY,
    POP_FILTER_RAW_ROWS,
    POP_FILTER_STRUCTURAL_ASSOC,
    POP_FILTER_TWO_BRANCH,
    POP_FILTER_WINDOWED_BY_MONTH,
    TO_ONE_FILTER_AMOUNT,
    cust_q,
    dev1900_models,
    dropped_filter_warnings,
    make_exec_engine,
    month_key,
    orders_q,
    pushed_filter_infos,
    rows_by,
    unparseable_derived_models,
)

MODES = ["broadcast", "associate", "error"]
OK = "orders.status = 'ok'"
OR_MIX = "tier = 'bronze' or orders.status = 'ok'"

SPEND = ModelMeasure(formula="spend:sum", name="sp")
PARTITIONED = ModelMeasure(formula="spend:sum(partition_by=tier)", name="pt")
COUNT = ModelMeasure(formula="*:count", name="n")
WINDOWED = ModelMeasure(formula="spend:sum(window='1y')", name="w")
LAST = ModelMeasure(formula="spend:last(customers.signup_at)", name="l")
NESTED = ModelMeasure(formula="avg(sum(spend, partition_by=tier))", name="ac")
ORDERS_AMOUNT = ModelMeasure(formula="orders.amount:sum", name="oa")

#: Python-warning carriers the semi-join push must never emit.
_SLAYER_WARNS = (
    BroadcastGrainWarning, AssociatedGrainWarning, UnreachableFilterDroppedWarning)


def _orders_month_td():
    return [TimeDimension(
        dimension=ColumnRef(name="orders.ordered_at"),
        granularity=TimeGranularity.MONTH)]


def _signup_month_td():
    return [TimeDimension(
        dimension=ColumnRef(name="signup_at"),
        granularity=TimeGranularity.MONTH)]


@pytest.fixture(params=["sqlite", "duckdb"])
async def backend(request):
    async for e in make_exec_engine(request):
        yield request.param, e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_backend(request):
    async for e in make_exec_engine(request, models=unparseable_derived_models()):
        yield request.param, e


async def _dry(engine, query, dialect: str) -> str:
    dry = await engine.execute(query, dry_run=True)
    assert dry.sql is not None, "dry_run returned no SQL"
    assert_scope_closed(dry.sql, dialect=dialect)
    return dry.sql


def _pop_infos(resp) -> list:
    """The population-push entries: a semi_join_pushed entry naming no aggregate."""
    return [i for i in pushed_filter_infos(resp) if i.measure is None]


def _bundle1900() -> ResolvedSourceBundle:
    m = dev1900_models()
    src = next(x for x in m if x.name == "customers")
    return ResolvedSourceBundle(
        source_model=src, referenced_models=[x for x in m if x.name != "customers"])


class TestDateRangeMaskAccounting:
    """A fanning date-range population filter rides a host-rooted producer's EXISTS
    and drops from the producer's masks; ``n_date_range_masks`` must count only the
    SURVIVING date-range masks, so an ordinary mask is never misread as a frame
    bound (readers: sql.generator lowering, _plan_src_row_filters)."""

    def _producer_plan(self):
        q = cust_q(
            time_dimensions=[{
                "dimension": "orders.ordered_at", "granularity": "month",
                "date_range": ["2020-01-01", "2020-12-31"]}],
            dimensions=["tier"],
            measures=[PARTITIONED],
            filters=["tier = 'gold'"])
        planned = plan_query(query=q, bundle=_bundle1900())
        (attach,) = planned.regroup_attach_plans
        return attach.producer_plan

    def test_pushed_date_range_leaves_no_producer_frame_bound(self):
        pp = self._producer_plan()
        assert pp.semi_join_filters, "the fanning date-range must ride the EXISTS"
        assert pp.n_date_range_masks == 0, "no date-range mask survives the push"
        # the surviving local tier mask must sit OUTSIDE the frame-bound prefix.
        frame_bound_ids = {m.slot_id for m in pp.masks[:pp.n_date_range_masks]}
        assert frame_bound_ids == set()
        assert len(pp.masks) == 1


class TestOrderWrapInheritsPopulation:
    """A host-grain ORDER-BY wrap over a joined sort key is a host-rooted producer;
    it inherits the population disposition (D1) and rides the EXISTS rather than
    inlining the fanning filter (which would multiply its wrapped aggregate)."""

    def _wrap_producer_plan(self):
        q = cust_q(
            dimensions=["tier"], measures=[SPEND], filters=[OK],
            order=[{"column": "orders.amount", "direction": "asc"}])
        planned = plan_query(query=q, bundle=_bundle1900())
        (attach,) = planned.regroup_attach_plans
        return attach.producer_plan

    def test_wrap_rides_exists_not_inline_fanning_filter(self):
        pp = self._wrap_producer_plan()
        assert pp.semi_join_filters, "the wrap must inherit the population EXISTS"
        # the fanning conjunct rode the EXISTS, so it is not an inline producer mask.
        assert pp.masks == []


class TestHostBaseRestrictsByAssociation:
    @pytest.mark.parametrize("mode", MODES)
    async def test_structural_fanning_filter(self, backend, mode):
        """customers · orders.status='ok' · spend:sum = 420 (never 520), each mode."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[SPEND], filters=[OK], to_many_handling=mode))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(
            POP_FILTER_STRUCTURAL_ASSOC)
        (info,) = _pop_infos(resp)
        assert "status" in info.filter_text

    @pytest.mark.parametrize("mode", MODES)
    async def test_derived_fanning_filter(self, backend, mode):
        """orders · customers.regions.bad_pop>0 · amount:sum = 120 (never 220)."""
        _, engine = backend
        resp = await engine.execute(orders_q(
            measures=[AMOUNT_SUM], filters=["customers.regions.bad_pop > 0"],
            to_many_handling=mode))
        assert float(resp.data[0]["orders.amt"]) == pytest.approx(
            POP_FILTER_DERIVED_ASSOC)
        (info,) = _pop_infos(resp)
        assert "bad_pop" in info.filter_text

    async def test_base_restricts_by_exists_and_does_not_join_orders(self, backend):
        """The host base carries a correlated EXISTS and never LEFT JOINs orders."""
        dialect, engine = backend
        sql = await _dry(engine, cust_q(measures=[SPEND], filters=[OK]), dialect)
        assert "EXISTS" in sql.upper(), sql
        assert "orders" not in _join_aliases(sql, dialect=dialect), sql

    async def test_population_push_is_not_a_python_warning(self, backend):
        """The push is response-only — never a Python-level warning, any mode."""
        _, engine = backend
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(cust_q(measures=[SPEND], filters=[OK]))
        assert _pop_infos(resp), "expected the population semi_join_pushed entry"
        assert [w for w in caught if issubclass(w.category, _SLAYER_WARNS)] == []


class TestSameRowBinding:
    async def test_filter_and_dimension_on_one_branch(self, backend):
        """dims=[orders.status], spend:sum, orders.amount=20 → one cell 'new'."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["orders.status"], measures=[SPEND],
            filters=["orders.amount = 20"]))
        assert {r["customers.orders.status"] for r in resp.data} == {"new"}


class TestToOneStaysInline:
    async def test_provably_to_one_filter_stays_inline(self, backend):
        """A to-one filter is a plain row restriction: SQL joins, no EXISTS push."""
        dialect, engine = backend
        q = orders_q(measures=[AMOUNT_SUM], filters=["customers.tier = 'gold'"])
        resp = await engine.execute(q)
        assert float(resp.data[0]["orders.amt"]) == pytest.approx(TO_ONE_FILTER_AMOUNT)
        assert _pop_infos(resp) == []
        assert "EXISTS" not in (await _dry(engine, q, dialect)).upper()


class TestProducersInheritDisposition:
    async def test_partitioned_by_tier(self, backend):
        """sum(spend, partition_by=tier) · ok = gold 190 / silver 230 (never 290);
        two entries: the producer measure and the population (no aggregate)."""
        dialect, engine = backend
        q = cust_q(dimensions=["tier"], measures=[PARTITIONED], filters=[OK])
        resp = await engine.execute(q)
        by = rows_by(resp, "customers.tier")
        assert set(by) == {(t,) for t in POP_FILTER_PARTITIONED_BY_TIER}
        for tier, spend in POP_FILTER_PARTITIONED_BY_TIER.items():
            assert float(by[(tier,)]["customers.pt"]) == pytest.approx(spend), tier
        measures = {i.measure for i in pushed_filter_infos(resp)}
        assert None in measures, measures
        assert "pt" in measures, measures
        sql = await _dry(engine, q, dialect)
        assert "EXISTS" in sql.upper()
        assert "orders" not in _join_aliases(sql, dialect=dialect), sql

    async def test_count_by_tier(self, backend):
        """*:count over the distinct population per tier = gold 3 / silver 2."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"], measures=[COUNT], filters=[OK]))
        by = rows_by(resp, "customers.tier")
        assert set(by) == {(t,) for t in POP_FILTER_COUNT_BY_TIER}
        for tier, n in POP_FILTER_COUNT_BY_TIER.items():
            assert int(by[(tier,)]["customers.n"]) == n, tier

    async def test_windowed_local_axis_inherits_restriction(self, backend):
        """sum(spend, window='1y') bucketed by customers.signup_at month over the
        ok population: each customer once, trailing-1y cumulative 100/250/310/420;
        the producer carries the EXISTS and never joins orders (both entries)."""
        dialect, engine = backend
        q = cust_q(
            time_dimensions=_signup_month_td(), measures=[WINDOWED], filters=[OK])
        resp = await engine.execute(q)
        by_month = {
            month_key(r["customers.signup_at"]): r["customers.w"]
            for r in resp.data if r["customers.signup_at"] is not None
        }
        assert set(by_month) == set(POP_FILTER_WINDOWED_BY_MONTH)
        for month, spend in POP_FILTER_WINDOWED_BY_MONTH.items():
            assert float(by_month[month]) == pytest.approx(spend), month
        measures = {i.measure for i in pushed_filter_infos(resp)}
        assert None in measures, measures
        assert "w" in measures, measures
        sql = await _dry(engine, q, dialect)
        assert "EXISTS" in sql.upper()
        assert "orders" not in _join_aliases(sql, dialect=dialect), sql

    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("with_filter", [True, False])
    async def test_windowed_fanning_axis_fails_closed(self, backend, mode, with_filter):
        """A window over a fanning time axis (orders.ordered_at from customers)
        fails closed in every mode, filter or not: its axis is not attributable
        from the population root; the error names the time dimension (decision 12)."""
        _, engine = backend
        kw = {"filters": [OK]} if with_filter else {}
        query = cust_q(
            time_dimensions=_orders_month_td(), measures=[WINDOWED],
            to_many_handling=mode, **kw)
        with pytest.raises((SlayerError, ValueError)) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        assert "ordered_at" in msg, msg
        assert_ref_free(msg)

    async def test_first_last_producer_with_sibling_sum(self, backend):
        """A first/last producer over a local axis coexists with the population
        disposition: last(spend) picks are per tier, the sibling sum is unmultiplied."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"], measures=[SPEND, LAST], filters=[OK]))
        by = rows_by(resp, "customers.tier")
        assert set(by) == {(t,) for t in POP_FILTER_PARTITIONED_BY_TIER}
        for tier, spend in POP_FILTER_PARTITIONED_BY_TIER.items():
            assert float(by[(tier,)]["customers.sp"]) == pytest.approx(spend), tier
        for tier, last in POP_FILTER_LAST_BY_TIER.items():
            assert float(by[(tier,)]["customers.l"]) == pytest.approx(last), tier

    async def test_nested_producer_inherits_restriction(self, backend):
        """avg(sum(spend, partition_by=tier)) · dims=[tier]: inner totals count each
        customer once (gold 190, not 290); the EXISTS rides the producer body."""
        dialect, engine = backend
        q = cust_q(dimensions=["tier"], measures=[NESTED], filters=[OK])
        resp = await engine.execute(q)
        by = rows_by(resp, "customers.tier")
        assert set(by) == {(t,) for t in POP_FILTER_NESTED_INNER_BY_TIER}
        for tier, inner in POP_FILTER_NESTED_INNER_BY_TIER.items():
            assert float(by[(tier,)]["customers.ac"]) == pytest.approx(inner), tier
        sql = await _dry(engine, q, dialect)
        assert "EXISTS" in sql.upper()
        assert "orders" not in _join_aliases(sql, dialect=dialect), sql

    async def test_two_branches_restrict_independently(self, backend):
        """spend:sum · [ok, regions.region_events.value>=50] = 280; one entry each."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[SPEND],
            filters=[OK, "regions.region_events.value >= 50"]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(POP_FILTER_TWO_BRANCH)
        assert len(_pop_infos(resp)) == 2


class TestProducerOnlySpine:
    async def test_producer_only_value(self, backend):
        """orders.amount:sum over ok orders = 82 (population restriction on the spine)."""
        _, engine = backend
        resp = await engine.execute(cust_q(measures=[ORDERS_AMOUNT], filters=[OK]))
        assert float(resp.data[0]["customers.oa"]) == pytest.approx(POP_FILTER_PRODUCER_ONLY)

    async def test_producer_only_no_match_yields_zero_rows(self, backend):
        """A predicate no order passes yields zero rows — never one NULL-carrying row."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[ORDERS_AMOUNT], filters=["orders.status = 'nonesuch'"]))
        assert resp.data == []


class TestRawRowMode:
    async def test_raw_rows_never_multiplied(self, backend):
        """distinct_dimension_values=False · ok = one row per distinct population
        customer (5), never one per matching order (6)."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"], filters=[OK], distinct_dimension_values=False))
        assert len(resp.data) == POP_FILTER_RAW_ROWS


class TestOutOfScopeResidue:
    async def test_dims_only_keeps_applying(self, backend):
        """An out-of-scope OR conjunct still restricts dims-only result rows."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"], filters=[OR_MIX]))
        assert {r["customers.tier"] for r in resp.data} == POP_FILTER_OUT_OF_SCOPE_TIERS

    async def test_dropped_from_host_producer_and_warns(self, backend):
        """A host-rooted producer drops the out-of-scope conjunct with the
        dropped-filter warning while the result rows stay restricted."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"], measures=[PARTITIONED], filters=[OR_MIX]))
        assert {r["customers.tier"] for r in resp.data} == POP_FILTER_OUT_OF_SCOPE_TIERS
        assert dropped_filter_warnings(resp), "expected the dropped-filter warning"

    async def test_producer_errors_under_error_mode(self, backend):
        """error mode turns the dropped out-of-scope conjunct into an error."""
        _, engine = backend
        query = cust_q(
            dimensions=["tier"], measures=[PARTITIONED], filters=[OR_MIX],
            to_many_handling="error")
        with pytest.raises((SlayerError, ValueError)):
            await engine.execute(query)

    @pytest.mark.parametrize("mode", MODES)
    async def test_inline_aggregate_fails_closed(self, backend, mode):
        """OR-mix over the population with a plain inline aggregate fails closed,
        naming the filter, the reason (the OR/NOT mix) and the remedy; no issue ref."""
        _, engine = backend
        query = cust_q(measures=[SPEND], filters=[OR_MIX], to_many_handling=mode)
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        assert "status" in msg, msg
        assert "OR/NOT" in msg, msg
        assert "split" in msg.lower(), msg
        assert "branch" in msg.lower(), msg
        assert_ref_free(msg)

    async def test_raw_row_fails_closed(self, backend):
        """OR-mix in raw-row mode fails closed rather than returning fanned rows."""
        _, engine = backend
        query = cust_q(
            dimensions=["tier"], filters=[OR_MIX], distinct_dimension_values=False)
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        assert_ref_free(str(ei.value))


class TestUnanalyzableFailsClosed:
    @pytest.mark.parametrize("with_measure", [True, False])
    @pytest.mark.parametrize("mode", MODES)
    async def test_unanalyzable_dependency(self, unparse_backend, with_measure, mode):
        """A filter on a derived column no dialect can parse fails closed, with or
        without an aggregate, in every mode — naming the filter and the column."""
        _, engine = unparse_backend
        kw = {"filters": ["customers.regions.unparseable > 0"], "to_many_handling": mode}
        q = (orders_q(measures=[AMOUNT_SUM], **kw) if with_measure
             else orders_q(dimensions=["customers.tier"], **kw))
        with pytest.raises(ValueError) as ei:
            await engine.execute(q)
        msg = str(ei.value)
        assert "analyse" in msg, msg
        assert "unparseable" in msg, msg
        assert_ref_free(msg)


class TestUnaffectedShapes:
    async def test_dimension_only_query_unaffected(self, backend):
        """A fanning filter with no measures executes, restricted by association."""
        _, engine = backend
        resp = await engine.execute(cust_q(dimensions=["tier"], filters=[OK]))
        assert {r["customers.tier"] for r in resp.data} == {"gold", "silver"}


class TestBoundaryAssociationArmUntouched:
    async def test_association_producer_gets_no_population_semi_join(self, backend):
        """associate spend:sum by orders.status filtered orders.amount in (20,30):
        new 100 / ok 150 (dev-1910, unchanged) and no population entry is added."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["orders.status"], measures=[SPEND],
            filters=["orders.amount in (20, 30)"], to_many_handling="associate"))
        by = rows_by(resp, "customers.orders.status")
        for status, spend in POP_FILTER_ASSOC_BY_STATUS.items():
            assert float(by[(status,)]["customers.sp"]) == pytest.approx(spend), status
        assert _pop_infos(resp) == [], "no population semi-join on the association arm"
