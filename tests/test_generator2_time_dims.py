"""DEV-1450 stage 7b.9 — time-dimension generator slice parity tests.

Asserts that ``generate_from_planned(plan_query(q, bundle), dialect=...)``
emits SQL whitespace-canonical-equal to the legacy ``_enrich`` →
``SQLGenerator.generate`` path for every shape that the time-dim slice
covers: row-phase TD dimensions (DATE_TRUNC / STRFTIME), date_range
filters (now via typed ``BetweenKey``), multi-TD disambiguation,
ORDER-BY-on-TD, and ``SlayerModel.filters`` (Mode-A SQL, always-applied
WHERE).

Out of scope (later slices): joined TDs (7b.12), window transforms
(7b.10), self-join CTE transforms (7b.11), cross-model CTEs (7b.12),
exhaustive dialect parity (7b.13).

Deleted alongside ``tests/parity_oracle.py`` at the end of 7b.15.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    LiteralKey,
    Phase,
)
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import (
    ColumnRef,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.engine.binding import BoundFilter, walk_value_keys
from slayer.engine.planning import ValueRegistry, filter_referenced_slot_ids
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
# Model fixtures
# ---------------------------------------------------------------------------


def _orders(
    *,
    model_filters: List[str] | None = None,
    default_td: str | None = None,
    extra_columns: List[Column] | None = None,
    extra_measures: List[ModelMeasure] | None = None,
) -> SlayerModel:
    cols = [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="customer_id", type=DataType.INT),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="status", type=DataType.TEXT),
        Column(name="created_at", type=DataType.TIMESTAMP),
        Column(name="event_at", type=DataType.TIMESTAMP),
        Column(name="deleted_at", type=DataType.TIMESTAMP),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=cols,
        filters=model_filters or [],
        default_time_dimension=default_td,
        measures=extra_measures or [],
    )


def _bundle(model: SlayerModel | None = None) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=model or _orders(),
        referenced_models=[],
    )


# ---------------------------------------------------------------------------
# Parity sweep — granularity x dialect.
#
# Postgres path exercises native DATE_TRUNC; SQLite exercises STRFTIME
# (plus the dedicated week / quarter branches in _build_date_trunc).
# DuckDB/MySQL/ClickHouse share Postgres-style DATE_TRUNC and are
# covered by the smoke-cycle test at the bottom; exhaustive parity is
# 7b.13.
# ---------------------------------------------------------------------------


_GRANULARITIES = ["year", "quarter", "month", "week", "day", "hour", "minute", "second"]


@pytest.mark.parametrize("granularity", _GRANULARITIES, ids=_GRANULARITIES)
async def test_td_granularity_postgres(granularity: str, tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity(granularity),
            ),
        ],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


@pytest.mark.parametrize("granularity", _GRANULARITIES, ids=_GRANULARITIES)
async def test_td_granularity_sqlite(granularity: str, tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity(granularity),
            ),
        ],
    )
    legacy = await legacy_sql_for(
        engine=engine, model=_orders(), query=query, dialect="sqlite",
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="sqlite")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# TD + measures (GROUP BY truncated expression alongside aggregates)
# ---------------------------------------------------------------------------


_TD_PLUS_MEASURE_CASES: list[tuple[str, Dict[str, Any]]] = [
    ("td_plus_sum", dict(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[{"formula": "amount:sum"}],
    )),
    ("td_plus_star_count", dict(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[{"formula": "*:count"}],
    )),
    ("td_plus_dim_plus_measure", dict(
        source_model="orders",
        dimensions=["status"],
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[{"formula": "amount:sum"}],
    )),
]


@pytest.mark.parametrize(
    "case_label,query_kwargs",
    _TD_PLUS_MEASURE_CASES,
    ids=[c[0] for c in _TD_PLUS_MEASURE_CASES],
)
async def test_td_plus_measure_parity(
    case_label: str, query_kwargs: Dict[str, Any], tmp_path,
) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(**query_kwargs)
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# date_range filter — typed BetweenKey rendering
# ---------------------------------------------------------------------------


async def test_td_date_range_alone(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-01", "2024-12-31"],
            ),
        ],
        measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_td_date_range_plus_user_filter(tmp_path) -> None:
    """Legacy WHERE order: date_range first, then user filter."""
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-01", "2024-12-31"],
            ),
        ],
        measures=[{"formula": "amount:sum"}],
        filters=["status == 'paid'"],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_td_date_range_string_quoting(tmp_path) -> None:
    """exp.Between(low=Literal.string(...), high=Literal.string(...))
    must emit single-quoted literals on postgres — same as legacy's
    hand-built ``f\"... '{start}' AND '{end}'\"`` form.
    """
    # tmp_path is unused (we don't touch storage in this assertion) —
    # parameter kept for parity with the surrounding test signature
    # convention (async fixtures touch tmp_path consistently).
    _ = tmp_path
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-01", "2024-12-31"],
            ),
        ],
        measures=[{"formula": "amount:sum"}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert "'2024-01-01'" in new
    assert "'2024-12-31'" in new
    assert " BETWEEN " in new.upper()


# ---------------------------------------------------------------------------
# Multi-TD disambiguation — main_time_dimension and default_time_dimension
# ---------------------------------------------------------------------------


async def test_multi_td_main_time_dimension_explicit(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
            TimeDimension(
                dimension=ColumnRef(name="event_at"),
                granularity=TimeGranularity.DAY,
            ),
        ],
        main_time_dimension="created_at",
        measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_multi_td_default_time_dimension_setting(tmp_path) -> None:
    model = _orders(default_td="created_at")
    storage = await build_storage_with_models(tmp_path, model)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
            TimeDimension(
                dimension=ColumnRef(name="event_at"),
                granularity=TimeGranularity.DAY,
            ),
        ],
        measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=model, query=query)
    planned = plan_query(query=query, bundle=_bundle(model))
    new = generate_from_planned(planned, bundle=_bundle(model), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# ORDER BY on TD
# ---------------------------------------------------------------------------


async def test_order_by_td(tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[{"formula": "amount:sum"}],
        order=[OrderItem(column="created_at", direction="asc")],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# SlayerModel.filters (Mode-A SQL, always-applied WHERE)
# ---------------------------------------------------------------------------


async def test_model_filter_only(tmp_path) -> None:
    model = _orders(model_filters=["deleted_at IS NULL"])
    storage = await build_storage_with_models(tmp_path, model)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=model, query=query)
    planned = plan_query(query=query, bundle=_bundle(model))
    new = generate_from_planned(planned, bundle=_bundle(model), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_model_filter_plus_user_filter(tmp_path) -> None:
    """Legacy WHERE order: model filter first, then user filter."""
    model = _orders(model_filters=["deleted_at IS NULL"])
    storage = await build_storage_with_models(tmp_path, model)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
        filters=["status == 'paid'"],
    )
    legacy = await legacy_sql_for(engine=engine, model=model, query=query)
    planned = plan_query(query=query, bundle=_bundle(model))
    new = generate_from_planned(planned, bundle=_bundle(model), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_model_filter_plus_date_range_plus_user_filter(tmp_path) -> None:
    """Full legacy WHERE order: date_range → model.filter → user.filter."""
    model = _orders(model_filters=["deleted_at IS NULL"])
    storage = await build_storage_with_models(tmp_path, model)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-01", "2024-12-31"],
            ),
        ],
        measures=[{"formula": "amount:sum"}],
        filters=["status == 'paid'"],
    )
    legacy = await legacy_sql_for(engine=engine, model=model, query=query)
    planned = plan_query(query=query, bundle=_bundle(model))
    new = generate_from_planned(planned, bundle=_bundle(model), dialect="postgres")
    assert_sql_equivalent(legacy, new)


def test_model_filter_rejects_measure_ref(tmp_path) -> None:
    """parse_sql_predicate + planner-time check rejects model filters
    that reference a ModelMeasure on the same model. Legacy raised at
    enrichment.py:1147-1153; new planner replicates."""
    model = _orders(
        model_filters=["rev > 100"],
        extra_measures=[ModelMeasure(name="rev", formula="amount:sum")],
    )
    with pytest.raises(ValueError, match=r"references measure 'rev'"):
        plan_query(query=SlayerQuery(source_model="orders"), bundle=_bundle(model))


def test_model_filter_rejects_windowed_column_ref(tmp_path) -> None:
    """Legacy enrichment.py:1205-1219 rejects filters referencing a
    Column whose sql contains a window function. New planner does the
    same for model.filters."""
    model = _orders(
        model_filters=["ranked > 5"],
        extra_columns=[
            Column(
                name="ranked",
                type=DataType.INT,
                sql="RANK() OVER (ORDER BY amount)",
            ),
        ],
    )
    with pytest.raises(ValueError, match=r"window function"):
        plan_query(query=SlayerQuery(source_model="orders"), bundle=_bundle(model))


def test_model_filter_rejects_dsl_colon_syntax(tmp_path) -> None:
    """parse_sql_predicate rejects DSL constructs (colon aggregation,
    transform calls). Model filters are Mode-A SQL only.

    The colon syntax is rejected at ``SlayerModel`` construction time
    (Pydantic before-validator at ``slayer/core/models.py``) — the
    typed pipeline never sees an invalid model. Pinning both layers
    catches the case where a hand-constructed model bypasses
    validation.
    """
    with pytest.raises(ValueError, match=r"aggregation colon syntax"):
        _orders(model_filters=["amount:sum > 100"])


# ---------------------------------------------------------------------------
# whole_periods_only — pre-snap integration
# ---------------------------------------------------------------------------


async def test_whole_periods_pre_snap(tmp_path) -> None:
    """``whole_periods_only=True`` is consumed BEFORE the planner sees the
    query — the upstream call to ``snap_to_whole_periods()`` in
    ``query_engine._execute_pipeline`` does the rounding. The planner /
    generator must consume already-snapped TDs and never re-snap.
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    raw_query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-15", "2024-03-15"],
            ),
        ],
        whole_periods_only=True,
        measures=[{"formula": "amount:sum"}],
    )
    # Both paths consume the SAME snapped query — mirrors the engine
    # boundary where snap_to_whole_periods() runs before normalization.
    snapped = raw_query.snap_to_whole_periods()
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=snapped)
    planned = plan_query(query=snapped, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Dialect cycle smoke — TD + measure across all Tier-1 dialects
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dialect", ["postgres", "sqlite", "duckdb", "mysql", "clickhouse"],
)
async def test_dialect_cycle_td_with_measure(dialect: str, tmp_path) -> None:
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(
        engine=engine, model=_orders(), query=query, dialect=dialect,
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect=dialect)
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Regression / shape tests — direct planner assertions, not parity.
# ---------------------------------------------------------------------------


def test_planner_emits_between_key_for_date_range() -> None:
    """date_range → BoundFilter with a typed BetweenKey value_key (not
    ArithmeticKey(and, [GE, LE])). The shape change closes the parity
    gap with legacy's ``BETWEEN`` rendering."""
    q = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-01", "2024-12-31"],
            ),
        ],
    )
    planned = plan_query(query=q, bundle=_bundle())
    assert len(planned.filters_by_phase) == 1
    fp = planned.filters_by_phase[0]
    assert fp.expression is not None
    key = fp.expression.value_key
    assert isinstance(key, BetweenKey), (
        f"expected BetweenKey, got {type(key).__name__}"
    )
    assert isinstance(key.column, ColumnKey)
    assert key.column.path == ()
    assert key.column.leaf == "created_at"
    assert isinstance(key.low, LiteralKey)
    assert key.low.value == "2024-01-01"
    assert isinstance(key.high, LiteralKey)
    assert key.high.value == "2024-12-31"


def test_between_key_filter_referenced_slot_ids_walks_column() -> None:
    """``filter_referenced_slot_ids`` must traverse ``BetweenKey.column``
    so the underlying ColumnKey's interned slot id surfaces — otherwise
    cross-model routing (7b.12) misclassifies date_range filters.

    Drives ``filter_referenced_slot_ids`` directly: build a registry
    that interns ``ColumnKey(created_at)``, build a BoundFilter whose
    value_key is a ``BetweenKey(column=that_key, ...)``, and assert the
    helper returns the matching slot id.
    """
    registry = ValueRegistry()
    col = ColumnKey(path=(), leaf="created_at")
    created_at_sid = registry.intern(
        key=col, declared_name="created_at",
        phase=Phase.ROW, public_name="created_at",
    )
    bk = BetweenKey(
        column=col,
        low=LiteralKey(value="2024-01-01"),
        high=LiteralKey(value="2024-12-31"),
    )
    bf = BoundFilter(
        value_key=bk,
        phase=Phase.ROW,
        referenced_keys=(col,),
    )
    result = filter_referenced_slot_ids(bf, registry)
    assert result == {created_at_sid}, (
        f"filter_referenced_slot_ids must surface {created_at_sid!r} "
        f"from BetweenKey.column traversal, got {result!r}"
    )


def test_model_filter_appears_before_user_filter_in_where(tmp_path) -> None:
    """Direct WHERE-text assertion: date_range → model.filter → user.filter."""
    model = _orders(model_filters=["deleted_at IS NULL"])
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-01", "2024-12-31"],
            ),
        ],
        measures=[{"formula": "amount:sum"}],
        filters=["status == 'paid'"],
    )
    planned = plan_query(query=query, bundle=_bundle(model))
    new = generate_from_planned(
        planned, bundle=_bundle(model), dialect="postgres",
    )
    norm = norm_sql(new)
    # Substring ordering: date_range filter (BETWEEN) before model
    # filter (deleted_at IS NULL) before user filter (status =).
    pos_between = norm.upper().find(" BETWEEN ")
    pos_model = norm.find("deleted_at IS NULL")
    pos_user = norm.find("status = 'paid'")
    assert pos_between != -1, f"expected BETWEEN in WHERE: {norm}"
    assert pos_model != -1, f"expected model filter in WHERE: {norm}"
    assert pos_user != -1, f"expected user filter in WHERE: {norm}"
    assert pos_between < pos_model < pos_user, (
        f"WHERE order wrong: between={pos_between} model={pos_model} "
        f"user={pos_user} in {norm!r}"
    )


def test_between_key_phase_is_row() -> None:
    """BetweenKey is always a row-phase predicate."""
    col = ColumnKey(path=(), leaf="created_at")
    bk = BetweenKey(
        column=col,
        low=LiteralKey(value="2024-01-01"),
        high=LiteralKey(value="2024-12-31"),
    )
    assert bk.phase == Phase.ROW


# ---------------------------------------------------------------------------
# Round-2 codex findings — additional coverage
# ---------------------------------------------------------------------------


def test_user_filter_with_ge_le_does_not_intern_as_between() -> None:
    """A user-authored filter ``amount >= 10 and amount <= 20`` must
    stay as ``ArithmeticKey(and, [GE, LE])`` — the DSL parser doesn't
    produce ``BetweenKey``. Only the planner-emitted date_range filter
    uses ``BetweenKey``. (R2-H1.)
    """
    q = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
        filters=["amount >= 10 and amount <= 20"],
    )
    planned = plan_query(query=q, bundle=_bundle())
    assert len(planned.filters_by_phase) == 1
    fp = planned.filters_by_phase[0]
    assert fp.expression is not None
    key = fp.expression.value_key
    assert isinstance(key, ArithmeticKey), (
        f"user >= AND <= filter must stay ArithmeticKey, got "
        f"{type(key).__name__}"
    )
    assert key.op == "and"
    # No BetweenKey anywhere in the tree.
    for sub in walk_value_keys(key):
        assert not isinstance(sub, BetweenKey), (
            f"BetweenKey leaked into user filter tree: {sub!r}"
        )


async def test_multi_td_each_with_date_range(tmp_path) -> None:
    """Two TDs, each with date_range → two BetweenKey filters. WHERE
    order: both BETWEEN clauses first (one per TD), then user/model
    filters. (R2-H2 parity side.)
    """
    storage = await build_storage_with_models(tmp_path, _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-01", "2024-03-01"],
            ),
            TimeDimension(
                dimension=ColumnRef(name="event_at"),
                granularity=TimeGranularity.DAY,
                date_range=["2024-02-01", "2024-04-01"],
            ),
        ],
        measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


def test_multi_td_each_with_date_range_planner_shape() -> None:
    """Planner emits exactly two BetweenKey filters, one per TD, in
    TD declaration order. (R2-H2 shape side.)
    """
    q = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-01-01", "2024-03-01"],
            ),
            TimeDimension(
                dimension=ColumnRef(name="event_at"),
                granularity=TimeGranularity.DAY,
                date_range=["2024-02-01", "2024-04-01"],
            ),
        ],
    )
    planned = plan_query(query=q, bundle=_bundle())
    between_filters = [
        fp for fp in planned.filters_by_phase
        if fp.expression is not None
        and isinstance(fp.expression.value_key, BetweenKey)
    ]
    assert len(between_filters) == 2
    # Order matches TD declaration order — first BetweenKey targets
    # created_at, second targets event_at.
    first_key = between_filters[0].expression.value_key
    second_key = between_filters[1].expression.value_key
    assert isinstance(first_key, BetweenKey)
    assert isinstance(second_key, BetweenKey)
    assert isinstance(first_key.column, ColumnKey)
    assert isinstance(second_key.column, ColumnKey)
    assert first_key.column.leaf == "created_at"
    assert second_key.column.leaf == "event_at"


def test_walk_value_keys_traverses_between_key() -> None:
    """``walk_value_keys`` yields the BetweenKey itself plus its
    column, low, and high keys. Pinned separately from
    ``filter_referenced_slot_ids`` so a regression in the walker
    doesn't hide behind the slot-id helper. (R2-M3.)
    """
    col = ColumnKey(path=(), leaf="created_at")
    lo = LiteralKey(value="2024-01-01")
    hi = LiteralKey(value="2024-12-31")
    bk = BetweenKey(column=col, low=lo, high=hi)
    seen = list(walk_value_keys(bk))
    assert col in seen, f"walk_value_keys must yield BetweenKey.column, got {seen!r}"
    assert lo in seen, f"walk_value_keys must yield BetweenKey.low, got {seen!r}"
    assert hi in seen, f"walk_value_keys must yield BetweenKey.high, got {seen!r}"


def test_model_filter_rejects_raw_over_window_function() -> None:
    """parse_sql_predicate rejects raw OVER(...). Rejection happens at
    ``SlayerModel`` construction time via the same parser; the typed
    pipeline never receives a windowed model.filter. (R2-M4.)
    """
    with pytest.raises(ValueError, match=r"window function"):
        _orders(model_filters=["RANK() OVER (ORDER BY amount) > 1"])


async def test_model_filter_qualifies_repeated_bare_name_not_already_qualified(
    tmp_path,
) -> None:
    """Edge case for ``_qualify_mode_a_sql_filter``: a model filter
    containing the same bare column name twice plus an already-qualified
    occurrence. The bare names get qualified; the already-qualified
    reference is left alone. (R2-M5.)

    Constructed via direct generator call rather than parity (legacy
    qualifies via the same regex, so this also serves as a parity
    smoke). The filter shape ``deleted_at IS NULL OR deleted_at = '0'``
    repeats the bare name; a dotted reference like ``orders.foo`` is
    appended via OR so the regex must NOT touch it.
    """
    model = _orders(
        model_filters=[
            "deleted_at IS NULL OR deleted_at = '1970-01-01' OR orders.id < 0",
        ],
    )
    storage = await build_storage_with_models(tmp_path, model)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}])
    legacy = await legacy_sql_for(engine=engine, model=model, query=query)
    planned = plan_query(query=query, bundle=_bundle(model))
    new = generate_from_planned(planned, bundle=_bundle(model), dialect="postgres")
    # Parity: both should qualify both bare deleted_at occurrences and
    # leave the orders.id reference untouched.
    assert_sql_equivalent(legacy, new)


def test_model_filter_rejects_derived_column_reference() -> None:
    """A model filter referencing a column whose ``Column.sql`` is set
    must raise ``NotImplementedError`` — legacy inlines the derived
    SQL via ``resolve_filter_columns``; the 7b.9 path only knows how
    to qualify bare names. Deferred to a follow-up. (R2-M6.)
    """
    model = _orders(
        model_filters=["full_name IS NOT NULL"],
        extra_columns=[
            Column(
                name="full_name",
                type=DataType.TEXT,
                sql="first_name || ' ' || last_name",
            ),
            Column(name="first_name", type=DataType.TEXT),
            Column(name="last_name", type=DataType.TEXT),
        ],
    )
    with pytest.raises(NotImplementedError, match=r"derived"):
        plan_query(query=SlayerQuery(source_model="orders"), bundle=_bundle(model))


async def test_model_filter_accepts_trivial_base_column_reference(
    tmp_path,
) -> None:
    """A column whose ``Column.sql`` is exactly its own name (e.g.
    ``Column(name=\"deleted_at\", sql=\"deleted_at\")``) is "trivial
    base" — it counts as a regular table column for filter
    qualification, not as a derived column. Legacy treats them
    identically; the new planner must too.

    Pinned by ``_is_trivial_base`` in
    ``slayer/engine/column_expansion.py``. Codex round-2 found that
    rejecting them universally as derived would break legitimate
    model filters on columns whose SQL is a bare identifier
    matching the column name.
    """
    model = SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            # Trivial-base: sql is exactly the column's own bare name.
            Column(name="deleted_at", type=DataType.TIMESTAMP, sql="deleted_at"),
        ],
        filters=["deleted_at IS NULL"],
    )
    # The plan_query call must succeed (no NotImplementedError):
    storage = await build_storage_with_models(tmp_path, model)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders", measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=model, query=query)
    planned = plan_query(query=query, bundle=ResolvedSourceBundle(source_model=model))
    new = generate_from_planned(
        planned, bundle=ResolvedSourceBundle(source_model=model), dialect="postgres",
    )
    assert_sql_equivalent(legacy, new)
