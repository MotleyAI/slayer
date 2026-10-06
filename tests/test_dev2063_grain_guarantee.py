"""Grain guarantee: a value evaluated only for a filter, an order key or an attach
join never changes the result's grain (spec: ``queries/semantics`` › Grain
guarantee). Executed on SQLite + DuckDB over ``tests/_dev1739_fixtures.py``
(per-customer order totals c1=90, c2=70, c3=50; tiers gold=140, silver=70;
regions RegN=160, RegS=50); plan-level checks read ``PlannedQuery.grain``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Callable, Optional

import pytest
from pydantic import ValidationError

from slayer.core.errors import MaterialisationStageError
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    Phase,
)
from slayer.core.query import ModelMeasure, SlayerQuery
from slayer.engine.plan import plan_query
from slayer.ir.planned import (
    PlannedQuery,
    RegroupAttachPlan,
    RegroupSubstitution,
    Stage,
    StageKind,
    ValueSlot,
)
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.generator import generate_from_planned

from tests._dev1739_fixtures import dev1739_models, make_exec_engine, month_key, month_td


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _band(key: str, threshold: int) -> dict:
    return {
        "expression": f"case when sum(amount, partition_by={key}) > {threshold} "
                      "then 'high' else 'other' end",
        "name": "band",
    }


REVENUE = ModelMeasure(formula="sum(amount)", name="revenue")


def _q(**kw) -> SlayerQuery:
    return SlayerQuery(source_model="orders", **kw)


def _rows(resp, *cols) -> list:
    """Every result row as a tuple — a list, so a repeated row stays visible."""
    return sorted((tuple(r[f"orders.{c}"] for c in cols) for r in resp.data), key=repr)


# --------------------------------------------------------------------------- #
# Shape A — a row filter on a computed dimension's partition key.
# --------------------------------------------------------------------------- #
class TestPartitionKeyFilter:
    @pytest.mark.parametrize("flt", ["customer_id is not null", "customer_id + 0 > 0"])
    async def test_filter_on_partition_key_keeps_band_grain(self, exec_engine, flt) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[_band("customer_id", 50)], measures=[REVENUE], filters=[flt],
        ))
        assert _rows(resp, "band", "revenue") == [("high", 160.0), ("other", 50.0)]

    async def test_restricting_filter_keeps_band_grain(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[_band("customer_id", 50)], measures=[REVENUE],
            filters=["customer_id = 1 or customer_id = 2"],
        ))
        assert _rows(resp, "band", "revenue") == [("high", 160.0)]

    async def test_single_band_is_one_row(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[_band("customer_id", 10)], measures=[REVENUE],
            filters=["customer_id is not null"],
        ))
        assert _rows(resp, "band", "revenue") == [("high", 210.0)]

    async def test_measure_partitioned_by_band_under_partition_key_filter(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", _band("customer_id", 50)],
            measures=[ModelMeasure(formula="sum(amount)", name="s"),
                      ModelMeasure(formula="amount:sum(partition_by=band)", name="bt")],
            filters=["customer_id is not null"],
        ))
        assert _rows(resp, "region", "band", "s", "bt") == [
            ("North", "high", 100.0, 160.0),
            ("South", "other", 50.0, 50.0),
            (None, "high", 60.0, 160.0),
        ]


class TestGrainConsumersUnderPartitionKeyFilter:
    """Each grain consumer (ranked kernel, transform, trailing window, frame bound)
    beside the band; ``customer_id`` is never NULL, so the filter is value-neutral."""

    async def test_ranked_measure(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[_band("customer_id", 50)],
            measures=[ModelMeasure(formula="amount:last(ordered_at)", name="v")],
            filters=["customer_id is not null"],
        ))
        assert _rows(resp, "band", "v") == [("high", 60.0), ("other", 25.0)]

    @pytest.mark.parametrize("formula,extra_filters,expected", [
        ("cumsum(amount:sum)", [], [
            ("high", "2024-01", 30.0), ("high", "2024-02", 100.0), ("high", "2024-03", 160.0),
            ("other", "2024-01", 25.0), ("other", "2024-03", 50.0),
        ]),
        ("amount:sum(window='60d')", [], [
            ("high", "2024-01", 30.0), ("high", "2024-02", 100.0), ("high", "2024-03", 130.0),
            ("other", "2024-01", 25.0), ("other", "2024-03", 25.0),
        ]),
        ("amount:sum(window='60d')", ["ordered_at >= '2024-02-01'"], [
            ("high", "2024-02", 100.0), ("high", "2024-03", 130.0), ("other", "2024-03", 25.0),
        ]),
    ])
    async def test_time_ordered_measure(self, exec_engine, formula, extra_filters, expected) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[_band("customer_id", 50)], time_dimensions=month_td(),
            measures=[ModelMeasure(formula=formula, name="v")],
            filters=["customer_id is not null", *extra_filters],
        ))
        got = sorted(((r["orders.band"], month_key(r["orders.ordered_at"]), float(r["orders.v"]))
                      for r in resp.data), key=repr)
        assert got == expected


class TestJoinedPartitionKeyFilter:
    @pytest.mark.parametrize("key,threshold", [
        ("customers.tier", 50),
        ("customers.regions.name", 40),
    ])
    async def test_band_spanning_two_key_values_is_one_row(self, exec_engine, key, threshold) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[_band(key, threshold)], measures=[REVENUE],
            filters=[f"{key} is not null"],
        ))
        assert _rows(resp, "band", "revenue") == [("high", 210.0)]


# --------------------------------------------------------------------------- #
# Shape B — an order key beside a first/last measure.
# --------------------------------------------------------------------------- #
class TestOrderKeyBesideRankedMeasure:
    @pytest.mark.parametrize("order_col", ["city", "customers.regions.name"])
    @pytest.mark.parametrize("formula,expected", [
        ("amount:last(ordered_at)", 60.0),
        ("amount:first(ordered_at)", 10.0),
    ])
    async def test_zero_dimension_query_is_one_row(
        self, exec_engine, formula, expected, order_col,
    ) -> None:
        resp = await exec_engine.execute(_q(
            measures=[ModelMeasure(formula=formula, name="v")],
            order=[{"column": order_col, "direction": "desc"}],
        ))
        assert _rows(resp, "v") == [(expected,)]

    async def test_one_row_per_dimension_value(self, exec_engine) -> None:
        # Regression guard: a dimensioned query already wraps the sort key.
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:last(ordered_at)", name="v")],
            order=[{"column": "city", "direction": "desc"}],
        ))
        assert _rows(resp, "region", "v") == [("North", 30.0), ("South", 25.0), (None, 60.0)]


# --------------------------------------------------------------------------- #
# Producer whose grain holds a computed dimension (regression guard).
# --------------------------------------------------------------------------- #
class TestProducerGrainCoverage:
    async def test_measure_partitioned_by_computed_dimension(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", _band("customer_id", 50)],
            measures=[ModelMeasure(formula="sum(amount)", name="s"),
                      ModelMeasure(formula="amount:sum(partition_by=band)", name="bt")],
        ))
        assert _rows(resp, "region", "band", "s", "bt") == [
            ("North", "high", 100.0, 160.0),
            ("South", "other", 50.0, 50.0),
            (None, "high", 60.0, 160.0),
        ]


# --------------------------------------------------------------------------- #
# PlannedQuery.grain — the planner-owned grain fact.
# --------------------------------------------------------------------------- #
def _bundle() -> ResolvedSourceBundle:
    models = dev1739_models()
    return ResolvedSourceBundle(dialect="duckdb", source_model=models[0], referenced_models=models[1:])


def _plan(**kw) -> PlannedQuery:
    return plan_query(query=_q(**kw), bundle=_bundle())


def _slot_id_by_alias(pq: PlannedQuery, alias: str) -> str:
    [sid] = [s.id for s in pq.row_slots if alias in s.public_aliases]
    return sid


class TestPlannedGrain:
    def test_grain_is_dimension_positions_in_order(self) -> None:
        pq = _plan(
            dimensions=["region", {"expression": "region", "name": "r2"}, _band("customer_id", 50)],
            time_dimensions=month_td(), measures=[REVENUE],
        )
        assert pq.grain == [
            _slot_id_by_alias(pq, "region"),
            _slot_id_by_alias(pq, "band"),
            _slot_id_by_alias(pq, "ordered_at"),
        ]

    @pytest.mark.parametrize("formula", ["sum(amount)", "amount:last(ordered_at)"])
    def test_grouped_zero_dimension_grain_is_empty(self, formula) -> None:
        pq = _plan(measures=[ModelMeasure(formula=formula, name="v")])
        assert pq.grain == []

    def test_raw_rows_grain_is_none(self) -> None:
        pq = _plan(dimensions=["region", "city"], distinct_dimension_values=False)
        assert pq.grain is None

    def test_partition_key_filter_column_is_not_a_base_column(self) -> None:
        pq = _plan(
            dimensions=[_band("customer_id", 50)], measures=[REVENUE],
            filters=["customer_id is not null"],
        )
        assert pq.grain == [_slot_id_by_alias(pq, "band")]
        [hidden] = [s for s in pq.row_slots if s.key == ColumnKey(path=(), leaf="customer_id")]
        assert hidden.hidden
        assert hidden.needs_column is False


# --------------------------------------------------------------------------- #
# The grain-determination invariant (hand-built plans).
# --------------------------------------------------------------------------- #
_BASE = Stage(kind=StageKind.BASE)
_REGION = ColumnKey(path=(), leaf="region")
_CITY = ColumnKey(path=(), leaf="city")
_SUM = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum")
_PLACEHOLDER = ColumnKey(path=(), leaf=f"{REGROUP_LEAF_PREFIX}0__amount_sum")


def _stage_error(build: Callable[[], object]) -> str:
    """The ``MaterialisationStageError`` message ``build`` raises, unwrapped from pydantic."""
    with pytest.raises((MaterialisationStageError, ValidationError)) as ei:
        build()
    err = ei.value
    if isinstance(err, ValidationError):
        err = err.errors()[0].get("ctx", {}).get("error")
    assert isinstance(err, MaterialisationStageError), repr(err)
    return str(err)


def _dim(sid: str = "d", key: ColumnKey = _REGION) -> ValueSlot:
    return ValueSlot(id=sid, key=key, declared_name=key.leaf, public_name=key.leaf,
                     public_aliases=[key.leaf], is_dimension=True, phase=Phase.ROW,
                     stage=_BASE, needs_column=True)


def _measure(sid: str = "m") -> ValueSlot:
    return ValueSlot(id=sid, key=_SUM, declared_name="s", public_name="s", public_aliases=["s"],
                     phase=Phase.AGGREGATE, stage=_BASE, needs_column=True)


def _hidden(sid: str, key, *, needs_column: bool = True) -> ValueSlot:
    return ValueSlot(id=sid, key=key, declared_name=f"__{sid}", hidden=True, phase=Phase.ROW,
                     stage=_BASE, needs_column=needs_column)


def _producer() -> PlannedQuery:
    return PlannedQuery(source_relation="orders", row_slots=[_dim("p_d")],
                        aggregate_slots=[_measure("p_m")], projection=["p_d", "p_m"], grain=["p_d"])


def _row_attach(host_key: ColumnKey = _REGION) -> RegroupAttachPlan:
    return RegroupAttachPlan(
        producer_plan=_producer(), alias_hint="p", attach_phase="row",
        join_pairs=[(host_key, "p_d")],
        substitutions=[RegroupSubstitution(placeholder=_PLACEHOLDER, producer_slot_id="p_m",
                                           original_key=_SUM, empty_value=None)],
    )


def _grouped(*extra_rows: ValueSlot, grain: Optional[list] = None,
             attaches: tuple = ()) -> PlannedQuery:
    return PlannedQuery(
        source_relation="orders", row_slots=[_dim(), *extra_rows], aggregate_slots=[_measure()],
        regroup_attach_plans=list(attaches), projection=["d", "m"],
        grain=["d"] if grain is None else grain,
    )


class TestGrainDeterminationInvariant:
    def test_undetermined_hidden_base_column_is_rejected(self) -> None:
        assert "'h'" in _stage_error(lambda: _grouped(_hidden("h", _CITY)))

    def test_undetermined_composite_is_rejected(self) -> None:
        composite = ArithmeticKey(op="+", operands=(_CITY, LiteralKey(value=Decimal(1))))
        _stage_error(lambda: _grouped(_hidden("h", composite)))

    def test_zero_dimension_grouped_plan_rejects_hidden_base_column(self) -> None:
        _stage_error(lambda: PlannedQuery(
            source_relation="orders", row_slots=[_hidden("h", _CITY)],
            aggregate_slots=[_measure()], projection=["m"], grain=[],
        ))

    def test_raw_rows_plan_is_exempt(self) -> None:
        PlannedQuery(source_relation="orders", row_slots=[_dim(), _hidden("h", _CITY)],
                     projection=["d"], grain=None)

    def test_hidden_value_without_a_column_is_accepted(self) -> None:
        _grouped(_hidden("h", _CITY, needs_column=False))

    def test_literal_is_accepted(self) -> None:
        _grouped(_hidden("h", LiteralKey(value=Decimal(1))))

    def test_composite_of_grain_members_is_accepted(self) -> None:
        _grouped(_hidden("h", ArithmeticKey(op="||", operands=(_REGION, LiteralKey(value="x")))))

    def test_row_attach_placeholder_on_grain_key_is_accepted(self) -> None:
        _grouped(_hidden("ph", _PLACEHOLDER), attaches=(_row_attach(),))

    def test_composite_of_placeholder_and_literal_is_accepted(self) -> None:
        composite = ArithmeticKey(op="+", operands=(_PLACEHOLDER, LiteralKey(value=Decimal(1))))
        _grouped(_hidden("ph", composite), attaches=(_row_attach(),))

    def test_row_attach_placeholder_on_non_grain_key_is_rejected(self) -> None:
        _stage_error(lambda: _grouped(_hidden("ph", _PLACEHOLDER),
                                      attaches=(_row_attach(host_key=_CITY),)))

    def test_violation_in_a_producer_plan_is_rejected(self) -> None:
        producer = PlannedQuery.model_construct(
            source_relation="orders", row_slots=[_dim("p_d"), _hidden("p_h", _CITY)],
            aggregate_slots=[_measure("p_m")], projection=["p_d", "p_m"], grain=["p_d"],
        )
        attach = RegroupAttachPlan.model_construct(producer_plan=producer, alias_hint="p",
                                                   attach_phase="combined")
        assert "'p_h'" in _stage_error(lambda: _grouped(attaches=(attach,)))


class TestRenderTimeInvariant:
    def test_model_copy_bypass_is_refused_at_render(self) -> None:
        pq = _plan(dimensions=["region"], measures=[REVENUE])
        bad = pq.model_copy(update={"row_slots": [*pq.row_slots, _hidden("h", _CITY)]})
        bundle = _bundle()
        with pytest.raises(MaterialisationStageError):
            generate_from_planned(planned_query=bad, bundle=bundle, dialect="duckdb")

    def test_model_copy_bypass_in_a_producer_is_refused_at_render(self) -> None:
        pq = _plan(dimensions=[_band("customer_id", 50)], measures=[REVENUE])
        [attach] = pq.regroup_attach_plans
        producer = attach.producer_plan
        bad_producer = producer.model_copy(
            update={"row_slots": [*producer.row_slots, _hidden("h", _CITY)]})
        bad = pq.model_copy(update={
            "regroup_attach_plans": [attach.model_copy(update={"producer_plan": bad_producer})],
        })
        bundle = _bundle()
        with pytest.raises(MaterialisationStageError):
            generate_from_planned(planned_query=bad, bundle=bundle, dialect="duckdb")
