"""DEV-1450 stage 7b.8 — local-only generator slice parity tests.

Asserts that ``generate_from_planned(plan_query(q, bundle), dialect=...)``
emits SQL whitespace-canonical-equal to the legacy
``SQLGenerator.generate(_enrich(q, model))`` path for every shape in the
local-only slice: single-model dimensions + aggregates + row filters +
ORDER BY + LIMIT/OFFSET + dim-only deduplication.

Out of scope (later slices): time dimensions (7b.9), window transforms
(7b.10), self-join CTE transforms (7b.11), cross-model CTEs (7b.12),
dialect-specific aggregation rendering (7b.13).

Deleted alongside ``tests/parity_oracle.py`` at the end of 7b.15.
"""

from __future__ import annotations

from typing import Any, Dict

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import MeasureNameCollidesWithColumnError
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    Phase,
    SqlExprKey,
)
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import OrderItem, SlayerQuery
from slayer.engine.planned import (
    BoundExpr,
    PlannedQuery,
    ValueSlot,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import generate_from_planned
from tests.parity_oracle import (
    assert_sql_equivalent,
    build_storage_with_models,
    legacy_sql_for,
)


# ---------------------------------------------------------------------------
# Model fixtures — mirror tests/test_stage_planner.py:29-82 so a query that
# parses under one fixture also plans under the other without surprises.
# ---------------------------------------------------------------------------


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="region_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            ),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="revenue", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ],
    )


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions",
        data_source="prod",
        sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders(),
        referenced_models=[_customers(), _regions()],
    )


# ---------------------------------------------------------------------------
# Parity fixtures — 13 local-only shapes.
# ---------------------------------------------------------------------------


# (label, kwargs to SlayerQuery). One per @pytest.mark.parametrize id.
_PARITY_CASES: list[tuple[str, Dict[str, Any]]] = [
    # 1. dim-only dedup → emits GROUP BY before LIMIT.
    ("dim_only", dict(
        source_model="orders",
        dimensions=["status"],
    )),
    # 2. single aggregate, no GROUP BY.
    ("single_sum", dict(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
    )),
    # 3. COUNT(*) — alias is ``orders._count``.
    ("star_count", dict(
        source_model="orders",
        measures=[{"formula": "*:count"}],
    )),
    # 4. user ``name`` override on a measure.
    ("rename_sum", dict(
        source_model="orders",
        measures=[{"formula": "amount:sum", "name": "rev"}],
    )),
    # 5. dim + two measures — emits GROUP BY.
    ("dim_plus_two_measures", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}, {"formula": "*:count", "name": "n"}],
    )),
    # 6. row filter → WHERE.
    ("single_row_filter", dict(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
        filters=["status == 'paid'"],
    )),
    # 7. compound row filter (boolean AND of two comparisons).
    ("compound_row_filter", dict(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
        filters=["amount > 10 and status != 'cancelled'"],
    )),
    # 8. ORDER BY on measure alias + LIMIT.
    ("order_by_measure_limit", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
        order=[OrderItem(column="amount:sum", direction="desc")],
        limit=10,
    )),
    # 8b. ORDER BY on measure alias WITHOUT LIMIT — Codex LOW fold-in.
    # _apply_order_limit emits ORDER independently of LIMIT, so this
    # exercises the order path in isolation.
    ("order_by_measure_no_limit", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
        order=[OrderItem(column="amount:sum", direction="desc")],
    )),
    # 9. ORDER BY on dimension.
    ("order_by_dim", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
        order=[OrderItem(column="status", direction="asc")],
    )),
    # 10. pagination only — limit + offset.
    ("pagination", dict(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
        limit=5,
        offset=20,
    )),
    # 11. count_distinct.
    ("count_distinct", dict(
        source_model="orders",
        measures=[{"formula": "amount:count_distinct"}],
    )),
    # 11b. count_distinct + GROUP BY — Codex LOW fold-in. _build_agg
    # has separate paths for count / count_distinct / *:count and
    # this pins COUNT(DISTINCT ...) under a grouping clause.
    ("count_distinct_with_dim", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customer_id:count_distinct"}],
    )),
    # 12. three aggregations on the same column.
    ("multi_agg_same_col", dict(
        source_model="orders",
        measures=[
            {"formula": "amount:avg"},
            {"formula": "amount:min"},
            {"formula": "amount:max"},
        ],
    )),
]


@pytest.mark.parametrize(
    "case_label,query_kwargs",
    _PARITY_CASES,
    ids=[c[0] for c in _PARITY_CASES],
)
async def test_local_only_parity(case_label, query_kwargs, tmp_path):
    """Each query shape: legacy SQL == new SQL (modulo whitespace)."""
    storage = await build_storage_with_models(
        tmp_path, _regions(), _customers(), _orders(),
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(**query_kwargs)
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Dialect-cycle smoke — one representative fixture across every Tier-1
# dialect. Confirms _dialect_for_type / _rewrite_log_aliases /
# rewrite_sqlite_json_extract integrate the same way on the new path.
# Exhaustive dialect parity is 7b.13.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dialect", ["postgres", "sqlite", "duckdb", "mysql", "clickhouse"],
)
async def test_local_only_dialect_smoke(dialect, tmp_path):
    storage = await build_storage_with_models(
        tmp_path, _regions(), _customers(), _orders(),
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "*:count", "name": "n"},
        ],
    )
    legacy = await legacy_sql_for(
        engine=engine, model=_orders(), query=query, dialect=dialect,
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect=dialect)
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Regression tests for known planner gaps surfaced in the 7b.7 checkpoint.
# These pin the fixes that 7b.8 must land so the parity cases above pass.
# ---------------------------------------------------------------------------


def test_planner_order_by_aggregate_canonical_alias_resolves():
    """ORDER BY ``amount_sum`` (the canonical alias of ``amount:sum``)
    must resolve through ``plan_query`` against the registry's
    projection — not against model scope. The 7b.7 checkpoint flagged
    this as a planner gap.
    """
    q = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
        order=[OrderItem(column="amount:sum", direction="desc")],
    )
    planned = plan_query(query=q, bundle=_bundle())
    assert len(planned.order) == 1
    sid = planned.order[0].slot_id
    matching = [
        s for s in planned.aggregate_slots
        if s.id == sid and isinstance(s.key, AggregateKey)
        and s.key.agg == "sum"
        and isinstance(s.key.source, ColumnKey)
        and s.key.source.leaf == "amount"
    ]
    assert matching, (
        f"order entry slot_id={sid!r} did not bind to the amount:sum "
        f"aggregate slot; got order entries {planned.order!r} and "
        f"aggregate_slots {[(s.id, s.declared_name, s.key) for s in planned.aggregate_slots]!r}"
    )


def test_planner_model_measure_expansion_wired():
    """A query measure referencing a saved ``ModelMeasure`` by bare
    name must expand pre-binding so the inner formula resolves. The
    7b.7 checkpoint flagged ``expand_model_measures`` as not wired
    into ``_declared_measures_from_query``.
    """
    orders_with_named_measure = SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        measures=[ModelMeasure(name="rev", formula="amount:sum")],
    )
    bundle = ResolvedSourceBundle(source_model=orders_with_named_measure)
    q = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "rev"}],
    )
    planned = plan_query(query=q, bundle=bundle)
    # The named measure must have expanded to AggregateKey(amount, sum).
    agg_slots = [
        s for s in planned.aggregate_slots
        if isinstance(s.key, AggregateKey)
        and s.key.agg == "sum"
        and isinstance(s.key.source, ColumnKey)
        and s.key.source.leaf == "amount"
    ]
    assert len(agg_slots) == 1, (
        f"named measure 'rev' did not expand to amount:sum; got "
        f"aggregate_slots {[(s.declared_name, s.key) for s in planned.aggregate_slots]!r}"
    )


def test_generator_rejects_column_filter_key_with_dev1450_marker():
    """``column_filter_key`` on ``AggregateKey`` is deferred to 7b.12
    (cross-model slice). To prevent silent SQL parity drift the local-
    only generator must raise ``NotImplementedError`` mentioning the
    stage marker if it sees a non-None ``column_filter_key``.

    This is the hand-built guard test — it pins the explicit
    deferral message. The companion xfail below covers the actual
    planner gap (``_bind_agg`` currently returns ``column_filter_key=
    None`` unconditionally).
    """
    sql_expr = SqlExprKey(canonical_sql="status = 'paid'")
    agg_key = AggregateKey(
        source=ColumnKey(path=(), leaf="amount"),
        agg="sum",
        column_filter_key=sql_expr,
    )
    slot = ValueSlot(
        id="s1",
        key=agg_key,
        declared_name="amount_sum",
        public_name="amount_sum",
        public_aliases=["amount_sum"],
        phase=Phase.AGGREGATE,
        type=DataType.DOUBLE,
        expression=BoundExpr(value_key=agg_key),
    )
    planned = PlannedQuery(
        source_relation="orders",
        aggregate_slots=[slot],
        projection=["s1"],
    )
    with pytest.raises(NotImplementedError) as exc:
        generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    msg = str(exc.value)
    assert "DEV-1450" in msg
    assert "7b.12" in msg or "column_filter_key" in msg.lower()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "DEV-1450 stage 7b.12 will wire Column.filter into "
        "AggregateKey.column_filter_key in slayer/engine/binding.py::_bind_agg. "
        "Today _bind_agg returns column_filter_key=None unconditionally, so "
        "the planner-path test fails. The hand-built guard test above pins "
        "the generator rejection; this xfail pins the planner gap. When 7b.12 "
        "lands the strict=True converts this to a real assertion."
    ),
)
def test_planner_populates_column_filter_key_for_filtered_column():
    """Real planner path: a ``Column.filter`` on the aggregated column
    must surface as ``AggregateKey.column_filter_key`` so the
    generator (7b.12) can render the CASE-WHEN wrapper.

    Codex MEDIUM fold-in: the hand-built guard alone doesn't catch the
    silent planner drop. This xfail exercises the real path so the
    gap is visible (and auto-converts when fixed).
    """
    orders_with_filtered_col = SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(
                name="amount",
                type=DataType.DOUBLE,
                filter="status = 'paid'",
            ),
            Column(name="status", type=DataType.TEXT),
        ],
    )
    bundle = ResolvedSourceBundle(source_model=orders_with_filtered_col)
    q = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
    )
    planned = plan_query(query=q, bundle=bundle)
    agg_slot = planned.aggregate_slots[0]
    assert isinstance(agg_slot.key, AggregateKey)
    assert agg_slot.key.column_filter_key is not None, (
        "Column.filter was dropped — _bind_agg needs to propagate "
        "the filter into AggregateKey.column_filter_key."
    )


def test_multi_alias_same_key_emits_both_aliases_no_parity():
    """P4 / C13: declaring the same structural key twice with different
    ``name``s emits ONE slot but TWO SELECT entries — one per alias.
    Legacy ``_enrich`` raises on this (collision), so parity is not
    asserted; instead pin the new generator's emitted SQL contains
    both aliases.

    DEV-1443 raises a collision when an alias matches a source column,
    so we use names that do not collide with ``orders``' columns.
    """
    q = SlayerQuery(
        source_model="orders",
        measures=[
            {"formula": "amount:sum", "name": "rev"},
            {"formula": "amount:sum", "name": "revenue"},
        ],
    )
    planned = plan_query(query=q, bundle=_bundle())
    # One shared slot, two public aliases.
    agg_slots = [
        s for s in planned.aggregate_slots
        if isinstance(s.key, AggregateKey)
        and s.key.agg == "sum"
        and isinstance(s.key.source, ColumnKey)
        and s.key.source.leaf == "amount"
    ]
    assert len(agg_slots) == 1, (
        f"multi-alias same-key should intern one slot; got "
        f"{[(s.declared_name, s.public_aliases) for s in planned.aggregate_slots]!r}"
    )
    assert sorted(agg_slots[0].public_aliases) == ["rev", "revenue"]
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    # Both aliases appear in the emitted SQL (quoted-identifier or bare).
    assert '"orders.rev"' in new or " AS orders.rev" in new or '"rev"' in new
    assert (
        '"orders.revenue"' in new
        or " AS orders.revenue" in new
        or '"revenue"' in new
    )


# ---------------------------------------------------------------------------
# Negative-collision guards (preserved DEV-1443 behavior in the new path).
# ---------------------------------------------------------------------------


def test_planner_rejects_measure_name_colliding_with_source_column():
    """A user-supplied ``name`` that matches a source column on the
    same model raises ``MeasureNameCollidesWithColumnError``.

    Pinned here because the negative case must keep working under
    the new generator path (the generator never sees the planner-
    rejected query).
    """
    q = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum", "name": "status"}],
    )
    with pytest.raises(MeasureNameCollidesWithColumnError):
        plan_query(query=q, bundle=_bundle())


