"""DEV-1450 stage 7b.10 — window-transform generator slice parity tests.

Asserts that ``generate_from_planned(plan_query(q, bundle), dialect=...)``
emits SQL whitespace-canonical-equal to the legacy ``_enrich`` ->
``SQLGenerator.generate`` path for every window transform shape this slice
covers: cumsum, lag, lead, rank, percent_rank, dense_rank, ntile, first,
last. Plus the cross-cutting concerns the slice depends on -- planner-side
``TransformKey.time_key`` wiring (carry-over gap from 7b.4), Kahn-style
step-CTE batching for parity with legacy ``_generate_with_computed``,
auto-partition by query dimensions (NOT time dimensions), hidden
transform-input materialization, POST-phase filter routing, and the
NotImplementedError pins for 7b.11 / 7b.12.

Out of scope (later slices):
* cross-model transform inputs -- 7b.12
* exhaustive dialect parity -- 7b.13

(7b.11 lifted the deferral for time_shift / consecutive_periods /
change — those are now exercised in
``tests/test_generator2_self_join.py``.)

Deleted alongside ``tests/parity_oracle.py`` at the end of 7b.15.
"""

from __future__ import annotations

from typing import List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import (
    ColumnRef,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import generate_from_planned
from tests.parity_oracle import (
    assert_sql_equivalent,
    build_storage_with_models,
    legacy_sql_for,
    norm_sql,
)


# ---------------------------------------------------------------------------
# Model fixtures (mirror tests/test_generator2_time_dims.py::_orders)
# ---------------------------------------------------------------------------


def _orders(
    *,
    default_td: str | None = None,
    extra_columns: List[Column] | None = None,
    extra_measures: List[ModelMeasure] | None = None,
) -> SlayerModel:
    cols = [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="customer_id", type=DataType.INT),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="qty", type=DataType.DOUBLE),
        Column(name="status", type=DataType.TEXT),
        Column(name="region", type=DataType.TEXT),
        Column(name="created_at", type=DataType.TIMESTAMP),
        Column(name="event_at", type=DataType.TIMESTAMP),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=cols,
        default_time_dimension=default_td,
        measures=extra_measures or [],
    )


def _bundle(model: SlayerModel | None = None) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=model or _orders(),
        referenced_models=[],
    )


def _td_month() -> TimeDimension:
    return TimeDimension(
        dimension=ColumnRef(name="created_at"),
        granularity=TimeGranularity.MONTH,
    )


# ---------------------------------------------------------------------------
# Per-op parity (postgres dialect)
# ---------------------------------------------------------------------------


async def test_cumsum_local_parity(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_lag_default_periods_parity(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "lag(amount:sum)", "name": "prev_amt"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


def test_lag_explicit_periods_typed_only() -> None:
    """The new pipeline binder is kwarg-only for ``periods``; legacy
    accepts only positional ``lag(col, N)``. Parity via the oracle is
    therefore not possible for explicit non-default ``periods``. Assert
    structural correctness instead -- the integer threads through to
    the OVER clause as ``LAG("...", 3)``.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "lag(amount:sum, periods=3)", "name": "lag3"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    assert 'LAG("orders.amount_sum", 3) OVER' in n


def test_lead_explicit_periods_typed_only() -> None:
    """Same divergence as lag: kwarg-only on the new pipeline."""
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "lead(amount:sum, periods=2)", "name": "next2"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    assert 'LEAD("orders.amount_sum", 2) OVER' in n


async def test_rank_local_parity(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "rank(amount:sum)", "name": "rk"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_percent_rank_local_parity(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "percent_rank(amount:sum)", "name": "prk"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_dense_rank_local_parity(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "dense_rank(amount:sum)", "name": "drk"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_ntile_n4_parity(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "ntile(amount:sum, n=4)", "name": "bucket"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_first_with_td_parity(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "first(amount:sum)", "name": "earliest"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_last_with_td_parity(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "last(amount:sum)", "name": "latest"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# TD wiring + validation (planner-side carry-over from 7b.4)
# ---------------------------------------------------------------------------


def test_cumsum_without_time_dimension_raises() -> None:
    """Planner enforces the legacy "requires an unambiguous time
    dimension" error for time-needing transforms in the new pipeline
    (7b.10 closes the 7b.4 carry-over gap). Regex pinned to the exact
    legacy phrase at ``enrichment.py:566`` so existing user-facing error
    strings round-trip.
    """
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    with pytest.raises(ValueError, match=r"requires an unambiguous time dimension"):
        plan_query(query=query, bundle=_bundle())


async def test_multi_td_with_main_time_dimension_picks_correctly(
    tmp_path,
) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            _td_month(),
            TimeDimension(
                dimension=ColumnRef(name="event_at"),
                granularity=TimeGranularity.DAY,
            ),
        ],
        main_time_dimension="created_at",
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_multi_td_via_model_default_time_dimension(tmp_path) -> None:
    model = _orders(default_td="created_at")
    storage = await build_storage_with_models(tmp_path, model)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            _td_month(),
            TimeDimension(
                dimension=ColumnRef(name="event_at"),
                granularity=TimeGranularity.DAY,
            ),
        ],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=model, query=query)
    planned = plan_query(query=query, bundle=_bundle(model))
    new = generate_from_planned(planned, bundle=_bundle(model), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Compound / batching -- Kahn-style step CTE merge for legacy parity
# ---------------------------------------------------------------------------


async def test_two_independent_window_transforms_share_one_step_cte(
    tmp_path,
) -> None:
    """cumsum + lag on the same input slot must batch into ONE step CTE
    (legacy _generate_with_computed batches every layer ready at the same
    iteration). Verifies the per-slot TransformLayer output of the planner
    is reconciled into legacy's batched CTE shape at render time.
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
            {"formula": "lag(amount:sum)", "name": "prev"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)
    # Independent structural check: exactly one ``step1`` CTE, not two.
    n = norm_sql(new).lower()
    assert n.count(" step1 as ") == 1, (
        f"expected one step1 CTE, got {n.count(' step1 as ')}"
    )
    assert " step2 as " not in n, "independent transforms must not stack into step2"


def test_nested_transforms_get_separate_step_ctes() -> None:
    """``cumsum(lag(amount:sum))`` -- the cumsum slot depends on the lag
    slot, so they cannot share a CTE. Asserts the structural property:
    one step1 CTE materialises the inner lag, one step2 CTE materialises
    the outer cumsum referencing step1's alias.

    Typed-only structural because the legacy generator names the inner
    auto-materialised lag slot ``_inner_<parent_name>``
    (``_inner_running_lag``) while the typed pipeline uses
    ``_<op>_inner`` (``_lag_inner``) from ``planning._canonical_name``.
    Both are correct hidden-slot aliases; the byte-for-byte name differs.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(lag(amount:sum))", "name": "running_lag"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql).lower()
    assert n.count(" step1 as ") == 1
    assert n.count(" step2 as ") == 1
    # step1 materialises the inner lag (hidden alias starts with _lag).
    step1_body = n.split(" step1 as (", 1)[1].split(")", 1)[0]
    assert "lag(" in step1_body
    # step2 materialises the cumsum with the user-supplied name.
    assert '"orders.running_lag"' in n


# ---------------------------------------------------------------------------
# Auto-partition semantics -- dimensions only (NOT TDs)
# ---------------------------------------------------------------------------


async def test_cumsum_auto_partitions_by_query_dimensions_not_tds(
    tmp_path,
) -> None:
    """Legacy ``partition_aliases = [d.alias for d in dimensions]`` --
    the TD-trunc slot is EXCLUDED from PARTITION BY (only used in ORDER
    BY of the OVER clause). Catches a regression where the new generator
    would auto-partition by every row-phase slot including TimeTruncKey.
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)
    # Independent check: PARTITION BY mentions status, NOT created_at.
    n = norm_sql(new)
    # PARTITION BY clause exists exactly for the cumsum.
    assert 'PARTITION BY "orders.status"' in n
    # The TD alias must NOT appear in any PARTITION BY clause.
    partition_clauses = [
        chunk for chunk in n.split("PARTITION BY")[1:]
    ]
    for chunk in partition_clauses:
        # Inspect only the partition-list up to the next "ORDER BY" / ")".
        head = chunk.split("ORDER BY")[0].split(")")[0]
        assert "created_at" not in head, (
            f"PARTITION BY must not include the TD alias; got: {head!r}"
        )


async def test_rank_no_partition_by_default(tmp_path) -> None:
    """Rank-family transforms partition empty by default even when the
    query has dimensions (legacy: rank-family rank across the whole
    result set; ``partition_by`` is opt-in).
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "rank(amount:sum)", "name": "rk"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)
    n = norm_sql(new)
    # rank() OVER has no PARTITION BY clause.
    assert "RANK() OVER" in n
    # Find the rank() OVER (...) and confirm no PARTITION BY inside.
    after_rank = n.split("RANK() OVER")[1].split(")")[0]
    assert "PARTITION BY" not in after_rank


# ---------------------------------------------------------------------------
# Explicit partition_by (rank-family only -- legacy-parity surface)
# ---------------------------------------------------------------------------


async def test_rank_explicit_partition_by_dim(tmp_path) -> None:
    """``rank(amount:sum, partition_by=region)`` with region as a query
    dimension partitions by ``orders.region`` (legacy parity).
    Non-rank explicit ``partition_by`` is intentional DEV-1450 typed
    divergence and is not parity-tested here.
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="region")],
        measures=[
            {"formula": "amount:sum"},
            {
                "formula": "rank(amount:sum, partition_by=region)",
                "name": "rk_by_region",
            },
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


def test_partition_by_hidden_column_materialized_in_base_cte() -> None:
    """``rank(amount:sum, partition_by=region)`` with region NOT in
    ``dimensions`` -- the column is referenced by the OVER clause but
    not requested in the public projection. Base CTE must materialize
    ``orders.region`` (so step1 can reference it) but the outer SELECT
    must omit it from the public projection.

    Typed-only because legacy rejects ``partition_by`` columns not
    already in query dimensions (``enrichment.py:548``); the typed
    pipeline allows it via ProjectionPlanner's hidden-slot
    materialization (``planning.py:476``).
    """
    query = SlayerQuery(
        source_model="orders",
        measures=[
            {"formula": "amount:sum"},
            {
                "formula": "rank(amount:sum, partition_by=region)",
                "name": "rk_by_region",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # region is materialized in the base CTE projection (so step1's
    # OVER can reference it).
    assert '"orders.region"' in n
    # The outermost SELECT (between top "SELECT" and the first "FROM")
    # lists ONLY public projection aliases. region must be absent there.
    outermost_select = n.split(" FROM ", 1)[0]
    assert '"orders.rk_by_region"' in outermost_select
    assert '"orders.amount_sum"' in outermost_select
    assert '"orders.region"' not in outermost_select


async def test_batching_two_aggregates_one_step_cte(tmp_path) -> None:
    """``cumsum(amount:sum)`` + ``cumsum(qty:sum)`` -- two different
    base aggregates, both ready off base. Must batch into ONE step CTE.
    Distinct from the same-aggregate batching test: this one verifies
    the batching predicate handles multiple input slots independently.
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "qty:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running_amt"},
            {"formula": "cumsum(qty:sum)", "name": "running_qty"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)
    n = norm_sql(new).lower()
    assert n.count(" step1 as ") == 1
    assert " step2 as " not in n


async def test_cumsum_no_dims_no_partition_by(tmp_path) -> None:
    """Measure-only cumsum with one TD and no dimensions -- the OVER
    clause must have ORDER BY but no PARTITION BY (auto-partition by
    empty dim set is the empty clause).
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)
    n = norm_sql(new)
    # SUM(...) OVER appears for the cumsum. Inspect its OVER body.
    over_chunks = n.split("SUM(\"orders.amount_sum\") OVER (")[1:]
    assert over_chunks, "expected at least one cumsum OVER clause"
    first_over = over_chunks[0].split(")")[0]
    assert "PARTITION BY" not in first_over
    assert "ORDER BY" in first_over


def test_cumsum_over_named_model_measure_by_reference() -> None:
    """Saved ``ModelMeasure(name="revenue", formula="amount:sum")`` plus
    query measure ``{"formula": "cumsum(revenue)"}``. The inner
    ``revenue`` reference resolves to the named measure's AggregateKey
    -- same slot identity as the projected revenue measure. Pins that
    named-measure indirection threads through to transform inputs (P9).

    Typed-only because legacy uses the ModelMeasure's name ``revenue``
    as the base CTE alias (``orders.revenue``) while the typed pipeline
    uses the canonical aggregation alias (``orders.amount_sum``). The
    ModelMeasure-name-as-alias contract is a pre-existing 7b.8 gap.
    """
    model = _orders(
        extra_measures=[
            ModelMeasure(name="revenue", formula="amount:sum"),
        ],
    )
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "revenue"},
            {"formula": "cumsum(revenue)", "name": "running"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle(model))
    sql = generate_from_planned(planned, bundle=_bundle(model), dialect="postgres")
    n = norm_sql(sql)
    # The projected revenue measure materialises ONE aggregate -- the
    # cumsum input is the same slot, so SUM(orders.amount) appears once
    # in the base CTE.
    assert n.count("SUM(orders.amount)") == 1
    # The cumsum OVER references that same aggregate alias.
    assert "SUM(" in n and "OVER" in n
    # The projected cumsum surfaces under its user-supplied name.
    assert '"orders.running"' in n


def test_planner_attaches_active_time_dimension_slot_id() -> None:
    """``PlannedQuery.active_time_dimension_slot_id`` is set when the
    stage has a resolvable active TD (single TD or multi + main/default),
    and None when no TD is present. Pins the planner-side contract that
    7b.10 introduces.
    """
    # Single TD -> slot id set, pointing at the TD's TimeTruncKey slot.
    with_td = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[{"formula": "amount:sum"}],
    )
    planned_with = plan_query(query=with_td, bundle=_bundle())
    assert planned_with.active_time_dimension_slot_id is not None
    # Look up the slot and confirm it's a row-phase TimeTruncKey on
    # created_at.
    td_slot = next(
        s for s in planned_with.row_slots
        if s.id == planned_with.active_time_dimension_slot_id
    )
    from slayer.core.keys import TimeTruncKey  # local import: planner test only

    assert isinstance(td_slot.key, TimeTruncKey)
    assert td_slot.key.column.leaf == "created_at"

    # No TD -> None.
    no_td = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[{"formula": "amount:sum"}],
    )
    planned_no = plan_query(query=no_td, bundle=_bundle())
    assert planned_no.active_time_dimension_slot_id is None


# ---------------------------------------------------------------------------
# Slot wiring -- user names, hidden input materialization, CAST wrapping
# ---------------------------------------------------------------------------


async def test_window_transform_with_user_name_alias(tmp_path) -> None:
    """``{"formula": "cumsum(amount:sum)", "name": "running"}`` projects
    the cumsum slot as ``orders.running`` (P4: the user-supplied name
    governs the public alias).
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)
    n = norm_sql(new)
    assert '"orders.running"' in n
    # Canonical-form alias must NOT appear when user supplied a name.
    assert '"orders.cumsum_amount_sum"' not in n


def test_window_transform_input_slot_materialized_even_if_hidden() -> None:
    """When the user filters on ``cumsum(amount:sum) > 100`` without
    projecting the cumsum measure, the base CTE must still materialize
    ``amount_sum`` so the step1 CTE's OVER clause can reference it.
    Hidden in the outer SELECT, present in the base CTE.

    Typed-only because legacy emits the hidden cumsum under a synthetic
    ``_ft<n>`` filter-temp alias scheme while the typed pipeline uses
    ``_<op>_inner`` (canonical name for hidden TransformKey slots). The
    SQL alias names differ; the structural property is the same.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[{"formula": "amount:sum"}],
        filters=["cumsum(amount:sum) > 100"],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Base CTE materialises the aggregate input.
    assert 'SUM(orders.amount) AS "orders.amount_sum"' in n
    # step1 CTE materialises the cumsum OVER and the WHERE references
    # the hidden alias.
    assert "SUM(" in n and "OVER" in n
    assert "WHERE" in n and "> 100" in n
    # Outer projection does NOT include the cumsum (only amount_sum and
    # the TD).
    outermost = norm_sql(sql).split(" FROM ", 1)[0]
    assert '"orders.amount_sum"' in outermost
    assert '"orders.created_at"' in outermost
    # The hidden cumsum alias is not surfaced publicly.
    assert "cumsum" not in outermost.lower()


@pytest.mark.skip(
    reason=(
        "DEV-1450: ModelMeasure.type propagation to ValueSlot.type is a "
        "pre-existing 7b.8 gap (DeclaredMeasure does not carry type; "
        "intern() defaults slot.type to None). Tracked outside 7b.10 -- "
        "this slice does not introduce the gap and cannot close it "
        "without revisiting the planner's measure-binding contract."
    ),
)
async def test_window_transform_with_typed_modelmeasure_wraps_in_cast(
    tmp_path,
) -> None:
    """A ModelMeasure declaring ``type=DataType.DOUBLE`` should propagate
    to ``ValueSlot.type``; the generator would then wrap the OVER
    expression in ``CAST(... AS DOUBLE)`` (matches legacy
    _generate_with_computed:1530).
    """
    pass


# ---------------------------------------------------------------------------
# Post-filters / order / limit
# ---------------------------------------------------------------------------


def test_post_filter_on_transform_slot() -> None:
    """``filters=["cumsum(amount:sum) > 100"]`` is a POST-phase filter --
    must apply on a wrapper SELECT after the CTE chain, before order/limit.

    Typed-only because the typed pipeline intern-dedupes the projected
    ``cumsum(amount:sum) name="running"`` and the filter's
    ``cumsum(amount:sum)`` into ONE slot per DEV-1450 P9 (transforms
    operate over slots, not strings); legacy materialises them as TWO
    identical OVER expressions (one as ``running``, one as ``_ft0``).
    Both are correct; the SQL shapes differ.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
        filters=["cumsum(amount:sum) > 100"],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # _filtered wrapper is present and references the running alias.
    assert " AS _filtered" in n
    assert "WHERE" in n
    # The condition references the running slot (intern-deduped with
    # the filter's cumsum -- one SUM(...) OVER in step1, one reference
    # in WHERE).
    assert '"orders.running"' in n
    assert "> 100" in n
    # DEV-1450 P9 dedup: exactly ONE cumsum OVER in step1 (the projected
    # ``running``), no separate ``_ft0`` materialisation.
    sum_over_count = n.count("SUM(\"orders.amount_sum\") OVER")
    assert sum_over_count == 1, (
        f"expected exactly one cumsum OVER (P9 intern-dedup), got "
        f"{sum_over_count}\nsql:\n{sql}"
    )


async def test_order_by_transform_slot(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
        order=[OrderItem(column="running", direction="desc")],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_order_and_limit_with_transform(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
        order=[OrderItem(column="running", direction="desc")],
        limit=5,
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Dialect cycle -- exhaustive parity is 7b.13; sanity-check the OVER shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dialect", ["postgres", "sqlite", "duckdb"], ids=["postgres", "sqlite", "duckdb"],
)
async def test_window_dialect_cycle(dialect: str, tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    legacy = await legacy_sql_for(
        engine=engine, model=_orders(), query=query, dialect=dialect,
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect=dialect)
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# (7b.10 deferral pins for ``time_shift`` / ``consecutive_periods`` /
# ``change`` removed in 7b.11 — that slice's tests now exercise those
# transforms positively in ``tests/test_generator2_self_join.py``.)
# ---------------------------------------------------------------------------
# Identity -- transform input alias resolution via registry, not text
# ---------------------------------------------------------------------------


def test_composite_transform_input_renders_inline() -> None:
    """``cumsum(amount:sum / qty:sum)`` -- the transform's ``input`` is
    an ``ArithmeticKey`` rather than a slottable leaf. The window step
    renders the input expression INLINE against the operands' already-
    materialised aliases (the Kahn readiness check guarantees both
    aggregates land in a prior CTE), so no extra inner CTE is needed.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "qty:sum"},
            {
                "formula": "cumsum(amount:sum / qty:sum)",
                "name": "rolling_ratio",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    # No NotImplementedError; the cumsum window sums the inline division
    # of the two materialised aggregate aliases.
    assert "SUM(" in sql
    assert "amount_sum" in sql and "qty_sum" in sql
    assert "OVER (" in sql
    assert "rolling_ratio" in sql


def test_lag_rejects_non_integer_periods() -> None:
    """The binder does not validate ``periods`` for lag/lead (only
    ``ntile.n`` and ``time_shift.periods``). The renderer rejects
    non-integral values to prevent silent truncation
    (``periods=2.5 -> 2``) or bool acceptance (``periods=True -> 1``).
    """
    from decimal import Decimal

    from slayer.core.keys import (
        AggregateKey,
        ColumnKey,
        Phase,
        TimeTruncKey,
        TransformKey,
    )
    from slayer.engine.planned import (
        BoundExpr as PlannedBoundExpr,
        PlannedQuery,
        TransformLayer,
        ValueSlot,
    )

    # Hand-build a TransformKey with a non-integral Decimal periods kwarg
    # -- the planner path does not produce this today (binder accepts
    # only integer-shaped scalars), but the renderer should reject it
    # explicitly so a future binder relaxation can't silently truncate.
    agg = AggregateKey(
        source=ColumnKey(leaf="amount"),
        agg="sum",
    )
    td_col = ColumnKey(leaf="created_at")
    td_key = TimeTruncKey(column=td_col, granularity="month")
    bad_lag = TransformKey(
        op="lag",
        input=agg,
        kwargs=(("periods", Decimal("2.5")),),
        time_key=td_key,
    )
    agg_slot = ValueSlot(
        id="s1",
        key=agg,
        expression=PlannedBoundExpr(value_key=agg),
        phase=Phase.AGGREGATE,
        declared_name="amount_sum",
        public_aliases=("amount_sum",),
    )
    td_slot = ValueSlot(
        id="s0",
        key=td_key,
        expression=PlannedBoundExpr(value_key=td_key),
        phase=Phase.ROW,
        declared_name="created_at",
        public_aliases=("created_at",),
    )
    bad_slot = ValueSlot(
        id="s2",
        key=bad_lag,
        expression=PlannedBoundExpr(value_key=bad_lag),
        phase=Phase.POST,
        declared_name="bad_lag",
        public_aliases=("bad_lag",),
    )
    pq = PlannedQuery(
        source_relation="orders",
        row_slots=[td_slot],
        aggregate_slots=[agg_slot],
        combined_expression_slots=[bad_slot],
        transform_layers=[TransformLayer(op="lag", slot_ids=["s2"])],
        projection=["s0", "s1", "s2"],
        active_time_dimension_slot_id="s0",
    )
    with pytest.raises(ValueError, match="must be an integer"):
        generate_from_planned(pq, bundle=_bundle(), dialect="postgres")


def test_lag_rejects_bool_periods() -> None:
    """``periods=True`` is an ``int`` subclass in Python but never a
    sensible offset; reject explicitly."""
    from slayer.core.keys import (
        AggregateKey,
        ColumnKey,
        Phase,
        TimeTruncKey,
        TransformKey,
    )
    from slayer.engine.planned import (
        BoundExpr as PlannedBoundExpr,
        PlannedQuery,
        TransformLayer,
        ValueSlot,
    )

    agg = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")
    td_col = ColumnKey(leaf="created_at")
    td_key = TimeTruncKey(column=td_col, granularity="month")
    bad_lag = TransformKey(
        op="lag",
        input=agg,
        kwargs=(("periods", True),),
        time_key=td_key,
    )
    agg_slot = ValueSlot(
        id="s1", key=agg,
        expression=PlannedBoundExpr(value_key=agg),
        phase=Phase.AGGREGATE,
        declared_name="amount_sum",
        public_aliases=("amount_sum",),
    )
    td_slot = ValueSlot(
        id="s0", key=td_key,
        expression=PlannedBoundExpr(value_key=td_key),
        phase=Phase.ROW,
        declared_name="created_at",
        public_aliases=("created_at",),
    )
    bad_slot = ValueSlot(
        id="s2", key=bad_lag,
        expression=PlannedBoundExpr(value_key=bad_lag),
        phase=Phase.POST,
        declared_name="bad_lag",
        public_aliases=("bad_lag",),
    )
    pq = PlannedQuery(
        source_relation="orders",
        row_slots=[td_slot],
        aggregate_slots=[agg_slot],
        combined_expression_slots=[bad_slot],
        transform_layers=[TransformLayer(op="lag", slot_ids=["s2"])],
        projection=["s0", "s1", "s2"],
        active_time_dimension_slot_id="s0",
    )
    with pytest.raises(ValueError, match="got bool"):
        generate_from_planned(pq, bundle=_bundle(), dialect="postgres")


def test_duplicate_public_aliases_for_one_slot_both_surface() -> None:
    """DEV-1450 C13: two declared measures whose formulas intern to the
    same ``AggregateKey`` share one slot internally; both user-supplied
    names appear in the public projection.

    Legacy rejects this scenario with a "canonicalises to the same
    aggregation" error; the typed pipeline accepts it per C13.

    With a window transform in the chain, the CTE chain must preserve
    BOTH aliases (the bug Codex flagged: the pre-fix step-CTE
    carry-forward dict overwrote one alias).
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum", "name": "a"},
            {"formula": "amount:sum", "name": "b"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Both user-supplied aliases must surface in the outermost SELECT.
    outermost = n.split(" FROM ", 1)[0]
    assert '"orders.a"' in outermost
    assert '"orders.b"' in outermost
    assert '"orders.running"' in outermost


def test_transform_input_alias_uses_registry_lookup() -> None:
    """A ``cumsum(amount:sum)`` slot's input is the same AggregateKey
    as the projected ``amount:sum`` measure. The OVER clause references
    the canonical base alias resolved via registry lookup -- not by
    re-rendering the formula text. Pins P9 (transforms operate over
    slots, not strings).

    Structural-only because parity is already covered by
    ``test_cumsum_local_parity``; this test pins the specific
    "canonical base alias is used" invariant in isolation.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # The OVER clause must reference the canonical alias of the
    # projected amount:sum slot -- ``orders.amount_sum`` -- not any
    # rename or canonical-cumsum form.
    assert 'SUM("orders.amount_sum") OVER' in n
