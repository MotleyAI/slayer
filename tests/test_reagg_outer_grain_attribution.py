"""A re-aggregation's outer grain is judged against its operand dataset, not the query root.

Rooted at ``customers``, every ``account_snapshots`` key is reached only across the fanning
``customers → account_snapshots`` hop, yet the operand grain determines it. SQLite + DuckDB.
Covers ``queries/partitioned-aggregates``.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import PartitionKeyError, ReaggregationError
from slayer.core.keys import AggregateKey, ColumnKey, Grain, TimeTruncKey
from slayer.engine.plan import plan_query

from tests._dev1847_fixtures import (
    ASSOCIATE_AVG_CITY_BY_REGION,
    make_exec_engine as sales_exec_engine,
    reagg,
    region_key,
    sales_q,
)
from tests._reagg_outer_grain_fixtures import (
    ASSOCIATED_MAX_Q_BY_MONTH,
    BAND_EXPR,
    BROADCAST_MAX_Q_BY_MONTH,
    COUNT_MAX_Q_BY_ACCOUNT,
    DEPTH3_MAX_ACCOUNT_TOTAL_BY_NAME,
    KEYLESS_SUM_MAX_Q,
    MAX_Q,
    MONTH_TD,
    P,
    Q,
    RANK_OF_ACCOUNT_MAX,
    SUM_LAST_P,
    SUM_MAX_P,
    SUM_MAX_Q_BY_ACCOUNT,
    SUM_MAX_Q_BY_BAND,
    SUM_SUM_P,
    SUM_SUM_P_BY_MONTH,
    SUM_WINDOWED_Q,
    WAVG_BY_ACCOUNT_MAX,
    approx_cells,
    bundle,
    cells,
    col,
    m,
    outer_grain_engine,
    query,
    warnings_of,
)

MODES = ["broadcast", "error", "associate"]
ACCOUNT = "account_snapshots.account_id"
NAME_KEY = ColumnKey(leaf="name")
ACCOUNT_KEY = ColumnKey(path=("account_snapshots",), leaf="account_id")
MONTH_KEY = TimeTruncKey(
    column=ColumnKey(path=("account_snapshots",), leaf="snapshot_date"), granularity="month")
BY_NAME_MONTH = ("name", "snapshot_date")
BY_NAME_ACCOUNT = ("name", "account_id")
SUM_MAX_Q_BY_OUTER_ACCOUNT = f"sum({MAX_Q}, partition_by=[{ACCOUNT}])"
EXPLICIT_UNDETERMINED = f"sum({MAX_Q}, partition_by=[name, account_snapshots.snapshot_date])"
SALES_EXPLICIT_UNDETERMINED = "avg(sum(amount, partition_by=city), partition_by=[region])"
CY_NULL_MONTH = {("Cy", None): None}


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async with outer_grain_engine(request.param) as e:
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def sales_engine(request):
    async for e in sales_exec_engine(request):
        yield e


def _by_month(formula: str, **kw):
    return query(dimensions=["name"], time_dimensions=[MONTH_TD], measures=[m(formula)], **kw)


def _by_account(*measures, **kw):
    return query(dimensions=["name", ACCOUNT], measures=list(measures), **kw)


def _clean(resp) -> None:
    """No reserved placeholder name in the result, any warning, or the emitted SQL."""
    texts = [*resp.columns, *(w.human_message() for w in resp.warnings or []), resp.sql or ""]
    assert not any("__regroup__" in t for t in texts), texts


def _assert_undetermined_key_error(err: BaseException, *, key: str, grain_member: str) -> None:
    assert isinstance(err, PartitionKeyError), repr(err)
    msg = str(err)
    assert "__regroup__" not in msg
    assert key in msg
    assert "operand grain" in msg
    assert grain_member in msg
    assert "inner partition_by" in msg
    assert "associate" in msg


def _outer_attach(pq, alias: str = "v"):
    (attach,) = [a for a in pq.regroup_attach_plans if a.alias_hint == alias]
    return attach


class TestJoinedTimeBucket:
    @pytest.mark.parametrize("inner,expected", [
        ("sum", SUM_SUM_P), ("max", SUM_MAX_P), ("last", SUM_LAST_P),
    ])
    async def test_bucket_in_operand_grain(self, engine, inner, expected):
        resp = await engine.execute(_by_month(f"sum({inner}(account_snapshots.balance, {P}))"))
        approx_cells(cells(resp, keys=BY_NAME_MONTH), {**expected, **CY_NULL_MONTH})
        _clean(resp)

    @pytest.mark.parametrize("inner", ["sum", "max"])
    async def test_equals_the_snapshot_rooted_query(self, engine, inner):
        rooted = await engine.execute(query(
            source_model="account_snapshots", dimensions=["customers.name"],
            time_dimensions=[{"dimension": "snapshot_date", "granularity": "month"}],
            measures=[m(f"sum({inner}(balance, partition_by=[account_id, customer_id, snapshot_date]))")]))
        resp = await engine.execute(_by_month(f"sum({inner}(account_snapshots.balance, {P}))"))
        got = cells(resp, keys=BY_NAME_MONTH)
        want = cells(rooted, keys=BY_NAME_MONTH)
        approx_cells({k: v for k, v in got.items() if k[0] != "Cy"}, want)

    async def test_windowed_inner_contributes_the_bucket(self, engine):
        resp = await engine.execute(_by_month(
            f"sum(sum(account_snapshots.balance, window='60d', {Q}))"))
        approx_cells(cells(resp, keys=BY_NAME_MONTH), {**SUM_WINDOWED_Q, **CY_NULL_MONTH})

    async def test_month_only_outer_grain(self, engine):
        resp = await engine.execute(query(
            time_dimensions=[MONTH_TD],
            measures=[m(f"sum(sum(account_snapshots.balance, {P}))")]))
        base = await engine.execute(query(
            time_dimensions=[MONTH_TD], measures=[m("account_snapshots.balance:sum")]))
        got = cells(resp, keys=("snapshot_date",))
        approx_cells(got, {(k,): v for k, v in SUM_SUM_P_BY_MONTH.items()})
        assert set(got) == set(cells(base, keys=("snapshot_date",)))

    async def test_adding_the_measure_keeps_rows_and_siblings(self, engine):
        base = await engine.execute(query(
            dimensions=["name"], time_dimensions=[MONTH_TD],
            measures=[m("account_snapshots.balance:sum", "b")]))
        both = await engine.execute(query(
            dimensions=["name"], time_dimensions=[MONTH_TD],
            measures=[m("account_snapshots.balance:sum", "b"),
                      m(f"sum(max(account_snapshots.balance, {P}))")]))
        assert cells(both, keys=BY_NAME_MONTH, value="b") == cells(base, keys=BY_NAME_MONTH, value="b")


class TestPlainToManyColumn:
    async def test_values_and_row_set(self, engine):
        resp = await engine.execute(_by_account(m(f"sum({MAX_Q})")))
        bare = await engine.execute(_by_account())
        got = cells(resp, keys=BY_NAME_ACCOUNT)
        approx_cells(got, SUM_MAX_Q_BY_ACCOUNT)
        n, a = col(bare.columns, "name"), col(bare.columns, "account_id")
        assert set(got) == {(r[n], r[a]) for r in bare.data}


class TestUngrainedUndeterminedDimension:
    async def test_associate(self, engine):
        resp = await engine.execute(_by_month(f"sum({MAX_Q})", to_many_handling="associate"))
        got = cells(resp, keys=BY_NAME_MONTH)
        approx_cells({k: v for k, v in got.items() if k[0] != "Cy"}, ASSOCIATED_MAX_Q_BY_MONTH)
        assert warnings_of(resp, "associated")

    async def test_default_broadcasts_with_warning(self, engine):
        resp = await engine.execute(_by_month(f"sum({MAX_Q})"))
        approx_cells(cells(resp, keys=BY_NAME_MONTH), {**BROADCAST_MAX_Q_BY_MONTH, **CY_NULL_MONTH})
        (w,) = warnings_of(resp, "broadcast")
        assert "snapshot_date" in w.human_message()

    async def test_error_mode_refuses(self, engine):
        q = _by_month(f"sum({MAX_Q})", to_many_handling="error")
        with pytest.raises(ReaggregationError, match="snapshot_date"):
            await engine.execute(q)


class TestExplicitOuterKeys:
    async def test_determined_key_across_a_fanning_hop(self, engine):
        resp = await engine.execute(_by_account(m(SUM_MAX_Q_BY_OUTER_ACCOUNT)))
        approx_cells(cells(resp, keys=BY_NAME_ACCOUNT), SUM_MAX_Q_BY_ACCOUNT)
        _clean(resp)

    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    async def test_undetermined_fanning_key_errors(self, engine, mode):
        q = _by_month(EXPLICIT_UNDETERMINED, to_many_handling=mode)
        with pytest.raises(PartitionKeyError) as ei:
            await engine.execute(q)
        _assert_undetermined_key_error(
            ei.value, key="account_snapshots.snapshot_date", grain_member="account_id")

    async def test_undetermined_fanning_key_associates(self, engine):
        resp = await engine.execute(_by_month(EXPLICIT_UNDETERMINED, to_many_handling="associate"))
        got = cells(resp, keys=BY_NAME_MONTH)
        approx_cells({k: v for k, v in got.items() if k[0] != "Cy"}, ASSOCIATED_MAX_Q_BY_MONTH)

    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    async def test_undetermined_to_one_key_errors(self, sales_engine, mode):
        q = sales_q(
            dimensions=["region"], to_many_handling=mode,
            measures=[reagg("avg", "sum(amount, partition_by=city)", name="acr",
                            partition_by="[region]")])
        with pytest.raises(PartitionKeyError) as ei:
            await sales_engine.execute(q)
        _assert_undetermined_key_error(ei.value, key="region", grain_member="city")

    async def test_undetermined_to_one_key_associates(self, sales_engine):
        resp = await sales_engine.execute(sales_q(
            dimensions=["region"], to_many_handling="associate",
            measures=[reagg("avg", "sum(amount, partition_by=city)", name="acr",
                            partition_by="[region]")]))
        vals = {k[0]: v["sales.acr"] for k, v in region_key(resp).items()}
        for region, expected in ASSOCIATE_AVG_CITY_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)


class TestPositions:
    async def test_order_key(self, engine):
        resp = await engine.execute(_by_account(
            m(f"sum({MAX_Q})"), order=[{"column": f"sum({MAX_Q})", "direction": "desc"}]))
        v = col(resp.columns, "v")
        assert [r[v] for r in resp.data if r[v] is not None] == [700.0, 160.0, 80.0, 40.0, 25.0]

    async def test_order_only_key(self, engine):
        resp = await engine.execute(query(
            dimensions=["name"], time_dimensions=[MONTH_TD],
            measures=[m("account_snapshots.balance:sum", "b")],
            order=[{"column": f"sum(max(account_snapshots.balance, {P}))", "direction": "desc"}]))
        n, t = col(resp.columns, "name"), col(resp.columns, "snapshot_date")
        order = [(r[n], str(r[t])[:7]) for r in resp.data]
        assert order == [("Bob", "2024-02"), ("Bob", "2024-01"), ("Ann", "2024-01"),
                         ("Ann", "2024-02"), ("Dee", "2024-02"), ("Dee", "2024-01"), ("Cy", "None")]
        _clean(resp)

    async def test_arithmetic(self, engine):
        resp = await engine.execute(_by_account(m(f"sum({MAX_Q})"), m(f"sum({MAX_Q}) * 2", "w")))
        plain = cells(resp, keys=BY_NAME_ACCOUNT)
        doubled = cells(resp, keys=BY_NAME_ACCOUNT, value="w")
        approx_cells(doubled, {k: None if x is None else 2 * x for k, x in SUM_MAX_Q_BY_ACCOUNT.items()})
        approx_cells(plain, SUM_MAX_Q_BY_ACCOUNT)

    async def test_filter_alongside_the_projected_measure(self, engine):
        resp = await engine.execute(_by_account(m(f"sum({MAX_Q})"), filters=[f"sum({MAX_Q}) > 100"]))
        approx_cells(cells(resp, keys=BY_NAME_ACCOUNT), {("Ann", 10): 160.0, ("Bob", 20): 700.0})

    async def test_count_takes_the_empty_value(self, engine):
        resp = await engine.execute(query(dimensions=[ACCOUNT], measures=[m(f"count({MAX_Q})")]))
        got = cells(resp, keys=("account_id",))
        assert {k[0]: int(x) for k, x in got.items()} == COUNT_MAX_Q_BY_ACCOUNT


class TestOtherConsumerShapes:
    async def test_depth_three(self, engine):
        middle = (f"sum(sum(account_snapshots.balance, {P}), "
                  f"partition_by=[{ACCOUNT}, id])")
        resp = await engine.execute(query(dimensions=["name"], measures=[m(f"max({middle})")]))
        approx_cells(cells(resp, keys=("name",)),
                     {(k,): x for k, x in DEPTH3_MAX_ACCOUNT_TOTAL_BY_NAME.items()})
        _clean(resp)

    async def test_computed_dimension(self, engine):
        size = {"expression": f"CASE WHEN {SUM_MAX_Q_BY_OUTER_ACCOUNT} > 100 "
                              f"THEN 'big' ELSE 'small' END", "name": "size"}
        resp = await engine.execute(query(dimensions=["name", ACCOUNT, size]))
        n, a, s = (col(resp.columns, k) for k in ("name", "account_id", "size"))
        got = {(r[n], r[a]): r[s] for r in resp.data}
        assert got == {("Ann", 10): "big", ("Ann", 11): "small", ("Bob", 20): "big",
                       ("Dee", 30): "small", ("Dee", 31): "small", ("Cy", None): "small"}
        _clean(resp)

    async def test_transform_input(self, engine):
        resp = await engine.execute(_by_account(m(f"rank({SUM_MAX_Q_BY_OUTER_ACCOUNT})")))
        got = cells(resp, keys=BY_NAME_ACCOUNT)
        assert {k: int(x) for k, x in got.items() if k[0] != "Cy"} == RANK_OF_ACCOUNT_MAX
        _clean(resp)

    async def test_aggregate_parameter(self, engine):
        resp = await engine.execute(query(dimensions=["name"], measures=[m(
            f"weighted_avg(account_snapshots.balance, weight={SUM_MAX_Q_BY_OUTER_ACCOUNT})")]))
        approx_cells(cells(resp, keys=("name",)),
                     {**{(k,): x for k, x in WAVG_BY_ACCOUNT_MAX.items()}, ("Cy",): None})
        _clean(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_fanning_constituent_key_still_errors(self, engine, mode):
        q = query(dimensions=["name"], to_many_handling=mode,
                  measures=[m(f"sum(count(name, partition_by=[id, {ACCOUNT}]))")])
        with pytest.raises(PartitionKeyError, match="account_snapshots.account_id") as ei:
            await engine.execute(q)
        assert "__regroup__" not in str(ei.value)


class TestExpressionAndKeylessGrain:
    async def test_band_determined_through_an_attached_expression(self, engine):
        resp = await engine.execute(query(
            dimensions=["name", {"expression": BAND_EXPR, "name": "band"}],
            measures=[m(f"sum({MAX_Q})")]))
        approx_cells(cells(resp, keys=("name", "band")), {**SUM_MAX_Q_BY_BAND, ("Cy", "lo"): None})
        _clean(resp)

    async def test_keyless(self, engine):
        resp = await engine.execute(query(measures=[m(f"sum({MAX_Q})")]))
        (row,) = resp.data
        assert float(row["customers.v"]) == pytest.approx(KEYLESS_SUM_MAX_Q)


class TestPlanStructure:
    def _outer_slot(self, formula: str, **kw):
        return self._outer_slot_of(_by_month(formula, **kw))

    @staticmethod
    def _outer_slot_of(q):
        producer = _outer_attach(plan_query(query=q, bundle=bundle())).producer_plan
        (slot,) = producer.aggregate_slots
        assert isinstance(slot.key, AggregateKey)
        return slot.key, producer

    def test_explicit_determined_key_is_the_settled_grain(self):
        key, _ = self._outer_slot_of(_by_account(m(SUM_MAX_Q_BY_OUTER_ACCOUNT)))
        assert key.partition_keys == Grain.of([ACCOUNT_KEY])

    def test_one_outer_answer_slot_with_the_settled_grain(self):
        key, _ = self._outer_slot(f"sum(sum(account_snapshots.balance, {P}))")
        assert key.agg == "sum"
        assert key.partition_keys == Grain.of([NAME_KEY, MONTH_KEY])

    def test_broadcast_dimension_left_out_of_the_settled_grain(self):
        key, _ = self._outer_slot(f"sum({MAX_Q})")
        assert key.partition_keys == Grain.of([NAME_KEY])

    def test_associated_dimension_in_the_settled_grain(self):
        key, _ = self._outer_slot(f"sum({MAX_Q})", to_many_handling="associate")
        assert key.partition_keys == Grain.of([NAME_KEY, MONTH_KEY])

    def test_keyless_settled_grain_is_empty(self):
        pq = plan_query(query=query(measures=[m(f"sum({MAX_Q})")]), bundle=bundle())
        (slot,) = _outer_attach(pq).producer_plan.aggregate_slots
        assert isinstance(slot.key, AggregateKey)
        assert slot.key.partition_keys == Grain.EMPTY

    @pytest.mark.parametrize("formula", [
        f"sum(sum(account_snapshots.balance, {P}))", f"sum({MAX_Q})",
    ])
    async def test_no_placeholder_in_emitted_sql(self, engine, formula):
        resp = await engine.execute(_by_month(formula, to_many_handling="associate"))
        assert resp.sql
        assert "__regroup__" not in resp.sql
        _clean(resp)
