"""The query population itself restricts by association.

A row-level conjunct reaching the population root only across a fanning hop
pushes as a correlated semi-join on the host base (each population row once);
every host-rooted producer inherits it. Same-branch conjuncts bind inline;
out-of-scope and unanalyzable ones fail closed. Covers ``queries/semantics`` and
``queries/cross-model-aggregates``; oracles in ``tests/_dev1900_fixtures.py``.
"""

from __future__ import annotations

import statistics
import tempfile
import warnings as _warnings

import pytest

from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.keys import ColumnKey
from slayer.engine.compile.stages import PopulationFilters, _PopulationConjunct
from slayer.ir.bound import bound_filter_from_key
from slayer.ir.planned import SemiJoinHop
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1841_fixtures import pushed_filter_infos
from tests._dev1853_fixtures import parallel_engine
from tests._dev1892_fixtures import assert_ref_free
from tests._engine_helpers import _join_aliases
from tests._dev1900_fixtures import (
    Column,
    DataType,
    ModelMeasure,
    SlayerModel,
    POP_FILTER_ASSOC_SAME_BRANCH,
    POP_FILTER_COUNT_BY_TIER,
    POP_FILTER_DERIVED_ASSOC,
    POP_FILTER_MIXED_CONJUNCT_SPEND,
    POP_FILTER_NESTED_BY_TIER,
    POP_FILTER_NESTED_TIER_MEAN,
    POP_FILTER_OUT_OF_SCOPE_TIERS,
    POP_FILTER_PARTITIONED_BY_TIER,
    POP_FILTER_PRODUCER_ONLY_AMOUNT,
    POP_FILTER_RAW_ROWS,
    POP_FILTER_SAME_ROW_ONE_CELL,
    POP_FILTER_STRUCTURAL_ASSOC,
    POP_FILTER_TWO_BRANCH_SPEND,
    POP_FILTER_WINDOWED_APRIL,
    TO_ONE_FILTER_AMOUNT,
    cust_q,
    dropped_filter_warnings,
    make_exec_engine,
    orders_q,
    rows_by,
    unparseable_derived_models,
)

MODES = ["broadcast", "associate", "error"]


def _slayer_warnings(caught) -> list:
    """Captured warnings whose class is defined in the ``slayer`` package."""
    return [w for w in caught if w.category.__module__.startswith("slayer")]

SPEND = ModelMeasure(formula="spend:sum", name="w")
LOCAL_AMOUNT = ModelMeasure(formula="amount:sum", name="amt")
PARTITIONED = ModelMeasure(formula="sum(spend, partition_by=tier)", name="w")
WINDOWED = ModelMeasure(formula="sum(spend, window='1y')", name="w")
NESTED = ModelMeasure(formula="avg(sum(spend, partition_by=tier))", name="w")
NEST_HOST_TARGET = ModelMeasure(
    formula="avg(orders.amount:sum, partition_by=tier)", name="w")
LAST_SPEND = ModelMeasure(formula="spend:last", name="v")
COUNT = ModelMeasure(formula="*:count", name="n")
ORDERS_AMOUNT = ModelMeasure(formula="orders.amount:sum", name="oa")
SIGNUP_MONTH = {"dimension": "signup_at", "granularity": "month"}
ORDERED_MONTH = {"dimension": "orders.ordered_at", "granularity": "month"}
OK = "orders.status = 'ok'"


@pytest.fixture(params=["sqlite", "duckdb"])
async def backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_backend(request):
    async for engine in make_exec_engine(request, models=unparseable_derived_models()):
        yield request.param, engine


@pytest.fixture
async def parallel_backend():
    with tempfile.TemporaryDirectory() as d:
        yield await parallel_engine(d, named=False)


async def _dry_sql(engine, dialect, query) -> str:
    """Emitted SQL for ``query`` on ``engine``, asserted scope-closed."""
    resp = await engine.execute(query, dry_run=True)
    assert resp.sql, "dry run produced no SQL"
    assert_scope_closed(resp.sql, dialect=dialect)
    return resp.sql


def _assert_semi_join(sql: str, *, not_joined: str) -> None:
    """The base restricts by a correlated EXISTS, never a join on ``not_joined``."""
    assert "EXISTS" in sql.upper(), f"no semi-join in:\n{sql}"
    assert not_joined not in _join_aliases(sql, dialect="sqlite"), (
        f"{not_joined!r} is joined into the base (fan), expected a semi-join:\n{sql}"
    )


def _cell(resp, dim_key: str, measure_key: str) -> dict:
    return {k[0]: v[measure_key] for k, v in rows_by(resp, dim_key).items()}


class TestPopulationRestrictedByAssociation:
    """S5 / S6 — a fanning population filter restricts the host base by EXISTS."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_structural_fanning_filter(self, backend, mode):
        dialect, engine = backend
        q = cust_q(measures=[SPEND], filters=[OK], to_many_handling=mode)
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(q)
        assert float(resp.data[0]["customers.w"]) == pytest.approx(
            POP_FILTER_STRUCTURAL_ASSOC)
        (info,) = pushed_filter_infos(resp)
        assert info.measure is None
        assert "status" in info.filter_text
        assert _slayer_warnings(caught) == []
        _assert_semi_join(await _dry_sql(engine, dialect, q), not_joined="orders")

    @pytest.mark.parametrize("mode", MODES)
    async def test_derived_fanning_filter(self, backend, mode):
        dialect, engine = backend
        q = orders_q(measures=[LOCAL_AMOUNT],
                     filters=["customers.regions.bad_pop > 0"],
                     to_many_handling=mode)
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(q)
        assert float(resp.data[0]["orders.amt"]) == pytest.approx(
            POP_FILTER_DERIVED_ASSOC)
        (info,) = pushed_filter_infos(resp)
        assert info.measure is None
        assert "bad_pop" in info.filter_text
        assert _slayer_warnings(caught) == []
        _assert_semi_join(await _dry_sql(engine, dialect, q),
                          not_joined="region_events")


class TestProducersInheritDisposition:
    """C15 / C16 / C11 — every producer rooted at the population takes the push."""

    async def test_partitioned_producer(self, backend):
        dialect, engine = backend
        q = cust_q(measures=[PARTITIONED], dimensions=["tier"], filters=[OK])
        resp = await engine.execute(q)
        vals = _cell(resp, "customers.tier", "customers.w")
        assert {k: pytest.approx(v) for k, v in vals.items()} == {
            k: pytest.approx(v) for k, v in POP_FILTER_PARTITIONED_BY_TIER.items()}
        assert {i.measure for i in pushed_filter_infos(resp)} == {None, "w"}
        _assert_semi_join(await _dry_sql(engine, dialect, q), not_joined="orders")

    async def test_windowed_producer_april(self, backend):
        dialect, engine = backend
        q = cust_q(measures=[WINDOWED], time_dimensions=[SIGNUP_MONTH], filters=[OK])
        resp = await engine.execute(q)
        april = _cell(resp, "customers.signup_at", "customers.w")
        key = next(k for k in april if str(k).startswith("2024-04"))
        assert float(april[key]) == pytest.approx(POP_FILTER_WINDOWED_APRIL)
        _assert_semi_join(await _dry_sql(engine, dialect, q), not_joined="orders")

    async def test_first_last_producer_over_time_axis(self, backend):
        """spend:last is fan-insensitive; the push removes the fanning join from the producer body."""
        dialect, engine = backend
        q = cust_q(measures=[LAST_SPEND], time_dimensions=[SIGNUP_MONTH], filters=[OK])
        resp = await engine.execute(q)
        picks = _cell(resp, "customers.signup_at", "customers.v")
        # c4 (bronze, no ok order) and c7 (no orders) are excluded from March/April.
        march = next(v for k, v in picks.items() if str(k).startswith("2024-03"))
        assert float(march) == pytest.approx(60.0)
        _assert_semi_join(await _dry_sql(engine, dialect, q), not_joined="orders")

    async def test_nested_producer_inherits(self, backend):
        """avg(sum(spend, partition_by=tier)) — inner totals count each customer once."""
        dialect, engine = backend
        q = cust_q(measures=[NESTED], dimensions=["tier"], filters=[OK])
        resp = await engine.execute(q)
        vals = _cell(resp, "customers.tier", "customers.w")
        assert {k: pytest.approx(v) for k, v in vals.items()} == {
            k: pytest.approx(v) for k, v in POP_FILTER_NESTED_BY_TIER.items()}
        assert statistics.mean(float(v) for v in vals.values()) == pytest.approx(
            POP_FILTER_NESTED_TIER_MEAN)
        _assert_semi_join(await _dry_sql(engine, dialect, q), not_joined="orders")

    async def test_host_target_nesting_body(self, backend):
        """A host-rooted producer nesting a target-rooted one carries the semi-join in its outer body."""
        dialect, engine = backend
        q = cust_q(measures=[NEST_HOST_TARGET], dimensions=["tier"], filters=[OK])
        sql = await _dry_sql(engine, dialect, q)
        assert "EXISTS" in sql.upper(), f"no semi-join in the host body:\n{sql}"

    async def test_count_over_population(self, backend):
        """*:count of the population per tier — each customer once over the restricted rows."""
        _, engine = backend
        resp = await engine.execute(
            cust_q(measures=[COUNT], dimensions=["tier"], filters=[OK]))
        vals = _cell(resp, "customers.tier", "customers.n")
        assert {k: int(v) for k, v in vals.items()} == POP_FILTER_COUNT_BY_TIER

    async def test_association_producer_same_branch(self, backend):
        """C17 — amount-in-(20,30) rows bind per status: c1's 'new' and c2's 'ok' each in its own cell."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[SPEND], dimensions=["orders.status"],
            filters=["orders.amount in (20, 30)"], to_many_handling="associate"))
        vals = _cell(resp, "customers.orders.status", "customers.w")
        assert {k: pytest.approx(v) for k, v in vals.items()} == {
            k: pytest.approx(v) for k, v in POP_FILTER_ASSOC_SAME_BRANCH.items()}


class TestSameRowBinding:
    """A conjunct a consumer's grain materialises binds inline to one row."""

    @pytest.mark.parametrize("mode", ["associate", "broadcast"])
    async def test_single_same_branch_filter_binds_one_cell(self, backend, mode):
        """S8 — the only amount-20 order is c1's 'new'; the status dimension binds it to one surviving cell."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[SPEND], dimensions=["orders.status"],
            filters=["orders.amount = 20"], to_many_handling=mode))
        vals = _cell(resp, "customers.orders.status", "customers.w")
        assert {k: pytest.approx(v) for k, v in vals.items()} == {
            k: pytest.approx(v) for k, v in POP_FILTER_SAME_ROW_ONE_CELL.items()}

    async def test_two_ok_orders_same_status_cell(self, backend):
        """Decision 5 — the status dimension materialises the fanning branch, so c1 counts once (420, not 520)."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[SPEND], dimensions=["orders.status"], filters=[OK]))
        vals = _cell(resp, "customers.orders.status", "customers.w")
        assert {k: float(v) for k, v in vals.items()} == {
            "ok": pytest.approx(POP_FILTER_STRUCTURAL_ASSOC)}

    async def test_mixed_conjunct_splits_inline_and_pushed(self, backend):
        """One filter string, inline conjunct (tier='gold') + pushed (status='ok'): gold customers with an ok order."""
        dialect, engine = backend
        q = cust_q(measures=[SPEND], filters=["tier = 'gold' and orders.status = 'ok'"])
        resp = await engine.execute(q)
        assert float(resp.data[0]["customers.w"]) == pytest.approx(
            POP_FILTER_MIXED_CONJUNCT_SPEND)
        (info,) = pushed_filter_infos(resp)
        assert info.measure is None
        _assert_semi_join(await _dry_sql(engine, dialect, q), not_joined="orders")

    async def test_two_branches_restrict_independently(self, backend):
        """S9 — an ok order AND a region event value>=50 (North only), each branch independent, each customer once."""
        dialect, engine = backend
        q = cust_q(measures=[SPEND],
                   filters=[OK, "regions.region_events.value >= 50"])
        resp = await engine.execute(q)
        assert float(resp.data[0]["customers.w"]) == pytest.approx(
            POP_FILTER_TWO_BRANCH_SPEND)
        assert len(pushed_filter_infos(resp)) == 2
        _assert_semi_join(await _dry_sql(engine, dialect, q), not_joined="orders")


class TestRawRowsAndDimensionsOnly:
    async def test_raw_rows_not_multiplied(self, backend):
        """S2 — one row per population customer, never one per matching order."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"], distinct_dimension_values=False, filters=[OK]))
        assert len(resp.data) == POP_FILTER_RAW_ROWS

    async def test_dimension_only_query_unchanged(self, backend):
        """S13 — a dims-only query keeps its restricted tier set."""
        _, engine = backend
        resp = await engine.execute(cust_q(dimensions=["tier"], filters=[OK]))
        assert {r["customers.tier"] for r in resp.data} == {"gold", "silver"}

    async def test_out_of_scope_conjunct_without_aggregate_applies(self, backend):
        """S12 — an OR-mixed conjunct with no inline aggregate stays applied through the join."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"],
            filters=["tier = 'bronze' or orders.status = 'ok'"]))
        assert {r["customers.tier"] for r in resp.data} == POP_FILTER_OUT_OF_SCOPE_TIERS

    async def test_out_of_scope_conjunct_dropped_from_producer(self, backend):
        """S12 — a producer drops the unpushable OR-mixed conjunct (with warning); the host base keeps it."""
        _, engine = backend
        resp = await engine.execute(cust_q(
            measures=[PARTITIONED], dimensions=["tier"],
            filters=["tier = 'bronze' or orders.status = 'ok'"]))
        assert {r["customers.tier"] for r in resp.data} == POP_FILTER_OUT_OF_SCOPE_TIERS
        dropped = dropped_filter_warnings(resp)
        assert dropped, "the producer must drop the out-of-scope conjunct and warn"
        assert any("bronze" in w.filter_text for w in dropped)

    async def test_fanning_axis_window_keeps_frame_rows(self, backend):
        """C18 — a date_range on the window's fanning time axis bounds visible buckets, keeping earlier frame rows."""
        _, engine = backend
        full = await engine.execute(cust_q(
            measures=[WINDOWED], time_dimensions=[ORDERED_MONTH], filters=[OK]))
        ranged = await engine.execute(cust_q(
            measures=[WINDOWED],
            time_dimensions=[{**ORDERED_MONTH,
                              "date_range": ["2024-03-01", "2024-12-31"]}],
            filters=[OK]))
        full_by = _cell(full, "customers.orders.ordered_at", "customers.w")
        ranged_by = _cell(ranged, "customers.orders.ordered_at", "customers.w")
        assert set(ranged_by) < set(full_by)
        for bucket, value in ranged_by.items():
            assert float(value) == pytest.approx(float(full_by[bucket]))


class TestProducerOnlySpine:
    async def test_producer_only_value_and_empty(self, backend):
        """C19 — one row with the producer's value; a predicate no order passes yields zero rows, not one NULL."""
        _, engine = backend
        resp = await engine.execute(cust_q(measures=[ORDERS_AMOUNT], filters=[OK]))
        assert float(resp.data[0]["customers.oa"]) == pytest.approx(
            POP_FILTER_PRODUCER_ONLY_AMOUNT)
        empty = await engine.execute(cust_q(
            measures=[ORDERS_AMOUNT], filters=["orders.status = 'nonesuch'"]))
        assert empty.data == []


class TestFailClosed:
    async def test_to_one_filter_stays_inline(self, backend):
        """S7 — a provably to-one filter applies as a plain row restriction."""
        _, engine = backend
        resp = await engine.execute(orders_q(
            measures=[LOCAL_AMOUNT], filters=["customers.tier = 'gold'"]))
        assert float(resp.data[0]["orders.amt"]) == pytest.approx(TO_ONE_FILTER_AMOUNT)
        assert pushed_filter_infos(resp) == []

    @pytest.mark.parametrize("mode", MODES)
    async def test_unanalyzable_filter_fails_closed(self, unparse_backend, mode):
        """S10 — an unparseable derived dependency fails closed in every mode, naming the filter and column."""
        _, engine = unparse_backend
        for kw in ({"measures": [LOCAL_AMOUNT]}, {"dimensions": ["customers.tier"]}):
            with pytest.raises(ValueError) as ei:
                await engine.execute(orders_q(
                    filters=["customers.regions.unparseable > 0"],
                    to_many_handling=mode, **kw))
            msg = str(ei.value)
            assert "unparseable" in msg
            assert "analyse" in msg
            assert_ref_free(msg)

    @pytest.mark.parametrize("mode", MODES)
    async def test_out_of_scope_conjunct_with_aggregate_fails_closed(self, backend, mode):
        """S11 — an OR-mixed conjunct with a plain aggregate inline fails closed, naming filter, reason and remedy."""
        _, engine = backend
        with pytest.raises(ValueError) as ei:
            await engine.execute(cust_q(
                measures=[SPEND],
                filters=["tier = 'bronze' or orders.status = 'ok'"],
                to_many_handling=mode))
        msg = str(ei.value)
        assert "pushdown scope" in msg
        assert "Split" in msg or "restate" in msg
        assert_ref_free(msg)

    @pytest.mark.parametrize("mode", MODES)
    async def test_ambiguous_host_correlation_fails_closed(self, parallel_backend, mode):
        """C8 — the population reaches the filtered model only across parallel edges; fail closed, never guess a hop."""
        with pytest.raises(AmbiguousJoinPathError) as ei:
            await parallel_backend.execute(cust_q(
                measures=[SPEND], filters=[OK], to_many_handling=mode))
        msg = str(ei.value)
        assert "orders" in msg
        assert "2 edges" in msg
        assert_ref_free(msg)


class TestPerConjunctMaterialisation:
    """Materialisation is per conjunct, not per semi-join group (D2): two
    conjuncts sharing a first hop but of different depth split — the one the
    grain binds goes inline, the deeper one alone forms the EXISTS."""

    @staticmethod
    def _pop() -> PopulationFilters:
        host = SlayerModel(
            name="customers", data_source="test", sql_table="customers",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)])
        shallow = ColumnKey(leaf="status", path=("orders",))
        deep = ColumnKey(leaf="x", path=("orders", "items"))
        gid = ("orders", (("id", "order_id"),))
        orders_hop = SemiJoinHop(
            target_model="orders", join_pairs=(("id", "order_id"),),
            node_path=("orders",))
        items_hop = SemiJoinHop(
            target_model="items", join_pairs=(("id", "oid"),),
            node_path=("orders", "items"))
        conj = [
            _PopulationConjunct(
                key=shallow, text="orders.status = 'ok'", is_date_bound=False,
                origin_index=0, origin_bf=bound_filter_from_key(shallow),
                disposition="semi_join", group_id=gid, key_rewritten=shallow,
                semi_join_text="orders.status = 'ok'", hops=(orders_hop,),
                fanning_paths=(("orders",),)),
            _PopulationConjunct(
                key=deep, text="orders.items.x = 1", is_date_bound=False,
                origin_index=1, origin_bf=bound_filter_from_key(deep),
                disposition="semi_join", group_id=gid, key_rewritten=deep,
                semi_join_text="orders.items.x = 1", hops=(orders_hop, items_hop),
                fanning_paths=(("orders",), ("orders", "items"))),
        ]
        return PopulationFilters(
            host_model=host, conjuncts=conj, passthrough=[])

    def test_shallow_binds_inline_deep_pushes(self) -> None:
        pop = self._pop()
        shallow = pop.conjuncts[0].key
        deep = pop.conjuncts[1].key
        # A producer grained by orders.status materialises only ("orders",).
        view = pop.producer_view(grain_paths=frozenset({("orders",)}))
        assert [bf.value_key for bf in view.inherited] == [shallow]
        assert len(view.semi_joins) == 1
        assert view.semi_joins[0].conjuncts == [deep]

    def test_nothing_materialised_pushes_both_in_one_group(self) -> None:
        pop = self._pop()
        shallow, deep = pop.conjuncts[0].key, pop.conjuncts[1].key
        view = pop.producer_view(grain_paths=frozenset())
        assert view.inherited == []
        assert len(view.semi_joins) == 1  # shared first hop → one EXISTS
        assert view.semi_joins[0].conjuncts == [shallow, deep]

    def test_materialised_sibling_hop_not_joined_in_the_exists(self) -> None:
        """Sibling branches under one first hop: materialising one sibling's grain
        must not leave its (now unused) hop inner-joined in the other's EXISTS."""
        host = SlayerModel(
            name="customers", data_source="test", sql_table="customers",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)])
        gid = ("orders", (("id", "order_id"),))
        orders_hop = SemiJoinHop(
            target_model="orders", join_pairs=(("id", "order_id"),),
            node_path=("orders",))
        items_hop = SemiJoinHop(
            target_model="items", join_pairs=(("id", "oid"),),
            node_path=("orders", "items"))
        ship_hop = SemiJoinHop(
            target_model="shipments", join_pairs=(("id", "sid"),),
            node_path=("orders", "shipments"))
        item_x = ColumnKey(leaf="x", path=("orders", "items"))
        ship_y = ColumnKey(leaf="y", path=("orders", "shipments"))
        pop = PopulationFilters(host_model=host, passthrough=[], conjuncts=[
            _PopulationConjunct(
                key=item_x, text="i", is_date_bound=False, origin_index=0,
                origin_bf=bound_filter_from_key(item_x), disposition="semi_join",
                group_id=gid, key_rewritten=item_x, semi_join_text="i",
                hops=(orders_hop, items_hop),
                fanning_paths=(("orders",), ("orders", "items"))),
            _PopulationConjunct(
                key=ship_y, text="s", is_date_bound=False, origin_index=1,
                origin_bf=bound_filter_from_key(ship_y), disposition="semi_join",
                group_id=gid, key_rewritten=ship_y, semi_join_text="s",
                hops=(orders_hop, ship_hop),
                fanning_paths=(("orders",), ("orders", "shipments"))),
        ])
        # A producer grained by orders.items binds item_x inline; only ship_y pushes.
        view = pop.producer_view(
            grain_paths=frozenset({("orders",), ("orders", "items")}))
        assert [bf.value_key for bf in view.inherited] == [item_x]
        (sj,) = view.semi_joins
        assert sj.conjuncts == [ship_y]
        assert {h.node_path for h in sj.hops} == {("orders",), ("orders", "shipments")}
