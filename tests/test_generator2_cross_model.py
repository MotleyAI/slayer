"""DEV-1450 stage 7b.12 — cross-model CTE generator slice tests.

This file exercises the cross-model rendering inside
``generate_from_planned``. The planner already builds
``CrossModelAggregatePlan`` records (stage 7b.5); this slice teaches the
generator to render them as ``WITH _cm_<alias> AS (...)`` CTEs that the
combined ``_base LEFT JOIN _cm_<alias>`` step pulls back into the public
projection.

Scope (mirrors the persisted plan):

* ``CrossModelAggregatePlan`` → one CTE per plan. For each plan the CTE
  selects the ``shared_grain_slots`` of the host as join-back keys (under
  their host alias names so the outer ``LEFT JOIN`` lines up), aggregates
  the target measure (with optional ``Column.filter`` CASE-WHEN), and
  groups by the join-back keys.
* Multi-hop joins via ``CrossModelAggregatePlan.join_chain`` — every
  intermediate hop is a ``LEFT JOIN`` inside the CTE body so a measure on
  ``orders → customers → regions`` walks the full chain.
* ``Column.filter`` on the aggregated column is wired into
  ``AggregateKey.column_filter_key`` at bind time and rendered as
  ``SUM(CASE WHEN <filter> THEN <col> END)`` inside the CTE. This closes
  the 7b.9 ``column_filter_key`` deferral that the local-only slice
  guarded explicitly.
* ``SlayerModel.filters`` on the target model propagate as a
  CTE-local ``WHERE``.
* Joined time dimensions (``customers.created_at`` with grain) participate
  in the cross-model planner's ``shared_grain_slots`` and surface in the
  outer projection — this closes the 7b.9 joined-TD deferral.
* ``HAVING``-phase filters routed to the CTE (``customers.revenue:sum >=
  100``) render inside the CTE rather than at the host.
* DEV-1445 acceptance: a renamed cross-model measure
  (``{"formula": "customers.revenue:sum", "name": "rev"}``) is reachable
  by EITHER the dotted form OR the user alias in filters and ORDER BY;
  both bind to the same slot and produce the same CTE HAVING clause.

Out of scope (later slices / follow-up tickets):

* Dialect-specific aggregation rendering (PERCENTILE / quantile / MySQL
  rejection) — 7b.13.
* Cross-model measure with ``Column.filter`` inside a transform
  (``change(customers.filtered_revenue:sum)``) — 7b.11 already covered
  the local self-join case; cross-model + transform is a 7b.15 acceptance
  flavour (DEV-1446 cross-cutting).
* ``ColumnSqlKey`` derived columns aggregated cross-model — same as
  above, deferred.

Each ``_cm_<alias>`` legacy CTE is asserted via parity against the
unmodified production legacy path (``engine._enrich`` +
``SQLGenerator.generate``) imported through ``tests/parity_oracle.py``.
Structural tests (DEV-1445 alias-as-filter, multi-alias same-key cross-
model) where the legacy raises are asserted on the new generator's
emitted SQL only — comments call out the divergence.

The file is deleted alongside ``tests/parity_oracle.py`` at the end of
7b.15.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import AggregateKey
from slayer.core.models import Column, ModelJoin, SlayerModel
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
# SQL-shape helpers — same shape as test_generator2_self_join.py.
# ---------------------------------------------------------------------------


_CTE_DEF_RE = re.compile(r"(?:WITH |, )([A-Za-z_][A-Za-z0-9_]*) AS \(")


def _cte_names(n: str) -> List[str]:
    """Return CTE names defined in a normalised SQL string in order."""
    return _CTE_DEF_RE.findall(n)


def _cte_body(n: str, name: str) -> str:
    """Body of the named CTE between ``<name> AS (`` and its matching close."""
    needle = f"{name} AS ("
    idx = n.find(needle)
    if idx < 0:
        raise AssertionError(f"CTE {name!r} not found in SQL: {n!r}")
    start = idx + len(needle)
    depth = 1
    i = start
    while i < len(n) and depth > 0:
        c = n[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return n[start:i]
        i += 1
    raise AssertionError(f"Unbalanced parens in CTE {name!r}")


# ---------------------------------------------------------------------------
# Model fixtures — mirror tests/test_stage_planner.py and the 7b.8 local
# fixtures but extend ``customers`` / ``regions`` with the columns 7b.12
# needs (``created_at`` for joined TDs, ``population`` for multi-hop agg,
# ``deleted_at`` for SlayerModel.filters, ``status`` for Column.filter).
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
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            ),
        ],
    )


def _customers(
    *,
    revenue_filter: str | None = None,
    model_filters: List[str] | None = None,
) -> SlayerModel:
    revenue_col = Column(
        name="revenue",
        type=DataType.DOUBLE,
        filter=revenue_filter,
    )
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            revenue_col,
            Column(name="status", type=DataType.TEXT),
            Column(name="deleted_at", type=DataType.TIMESTAMP),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ],
        filters=model_filters or [],
    )


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions",
        data_source="prod",
        sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="population", type=DataType.INT),
        ],
    )


def _bundle(
    *,
    revenue_filter: str | None = None,
    customers_filters: List[str] | None = None,
) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders(),
        referenced_models=[
            _customers(
                revenue_filter=revenue_filter,
                model_filters=customers_filters,
            ),
            _regions(),
        ],
    )


async def _seed_storage(
    tmp_path,
    *,
    revenue_filter: str | None = None,
    customers_filters: List[str] | None = None,
):
    """Seed the storage backend in leaf-first order (regions, customers, orders).

    The bundle and the legacy ``engine._enrich`` path must see the SAME
    set of models (including the optional ``Column.filter`` /
    ``SlayerModel.filters`` overrides) — otherwise the parity assertion
    is comparing different inputs. Helper keeps the call sites compact.
    """
    storage = await build_storage_with_models(
        tmp_path,
        _regions(),
        _customers(
            revenue_filter=revenue_filter,
            model_filters=customers_filters,
        ),
        _orders(),
    )
    return storage


# ---------------------------------------------------------------------------
# Parity fixtures — every shape the legacy can render gets compared
# whitespace-canonical-equal.
# ---------------------------------------------------------------------------


_PARITY_CASES: list[tuple[str, Dict[str, Any]]] = [
    # 1. one cross-model measure only — orders → customers, SUM(revenue).
    ("cm_single_measure", dict(
        source_model="orders",
        measures=[{"formula": "customers.revenue:sum"}],
    )),
    # 2. cross-model + local dimension — GROUP BY local dim flows into
    #    the CTE's shared_grain_slots so the join-back key is the
    #    surviving dim alias on both sides.
    ("cm_with_local_dim", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.revenue:sum"}],
    )),
    # 3. cross-model + local measure — _base CTE aggregates locally,
    #    _cm CTE aggregates the cross-model measure, outer joins both.
    ("cm_plus_local_measure", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "customers.revenue:sum"},
        ],
    )),
    # 4. cross-model + ORDER BY local measure + LIMIT.
    ("cm_order_limit", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "customers.revenue:sum"},
        ],
        order=[OrderItem(column="amount:sum", direction="desc")],
        limit=5,
    )),
    # 5. count_distinct on cross-model column — exercises the
    #    aggregation-name branch inside the CTE.
    ("cm_count_distinct", dict(
        source_model="orders",
        measures=[{"formula": "customers.id:count_distinct"}],
    )),
    # 6. row-phase filter on host-local column — flows to _base CTE
    #    WHERE; the cross-model CTE stays unaffected (filter does NOT
    #    propagate per FilterRoute.DROP_HOST_LOCAL).
    ("cm_with_local_row_filter", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.revenue:sum"}],
        filters=["status == 'paid'"],
    )),
]


# The binder doesn't yet support the dotted-star form ``customers.*``
# (``_resolve_dotted`` rejects the trailing ``*`` segment). Legacy
# enrichment accepts it via a different path. Tracked for a binder
# slice (DEV-1450 7b.15 / DEV-1438 cross-cutting); pinned as xfail.


@pytest.mark.xfail(
    strict=True,
    reason=(
        "DEV-1450 binder gap: ``customers.*:count`` fails in "
        "``_resolve_dotted`` with ``UnknownReferenceError`` because "
        "the dotted-star form is not yet a recognised StarSource on "
        "joined paths. Legacy supports it via a separate path; the "
        "new binder needs an explicit ``DottedStarRef`` shape. "
        "Tracked as a binder follow-up to land in 7b.15 alongside the "
        "rest of the DEV-1445 acceptance work."
    ),
)
async def test_cross_model_star_count(tmp_path):
    """``customers.*:count`` exercises the StarKey aggregation source
    path inside a cross-model CTE. Result key is
    ``orders.customers._count``.
    """
    storage = await _seed_storage(tmp_path)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.*:count"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# Cases the LEGACY enrichment path either rejects or routes
# differently from the typed pipeline. These get structural-only
# tests instead of parity. Mapping:
#
# - ``cm_renamed_measure``: legacy emits the canonical alias
#   ``orders.customers.revenue_sum`` for cross-model renamed measures
#   (a pre-DEV-1445 oddity); the typed pipeline surfaces the renamed
#   ``orders.rev`` at the combined SELECT per the result-key contract.
# - ``cm_having_filter_on_agg_ref``: legacy raises ``Filter
#   'customers.revenue_sum' references column 'revenue_sum' on
#   'customers', which doesn't resolve...``. DEV-1445 in the typed
#   pipeline accepts the colon form via the structural-key contract
#   and routes to the CTE's HAVING.
# - ``cm_where_filter_on_target_path``: legacy keeps the filter at
#   ``_base`` with a LEFT JOIN to the target; the typed pipeline
#   propagates it INSIDE the cross-model CTE as WHERE per the
#   inherited_filter_policy decision table.
# - ``cm_where_and_having_combined``: composes the two above.


@pytest.mark.parametrize(
    "case_label,query_kwargs",
    _PARITY_CASES,
    ids=[c[0] for c in _PARITY_CASES],
)
async def test_cross_model_parity(case_label, query_kwargs, tmp_path):
    """Each cross-model shape: legacy SQL == new SQL (modulo whitespace)."""
    storage = await _seed_storage(tmp_path)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(**query_kwargs)
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Multi-hop — orders → customers → regions
# ---------------------------------------------------------------------------


async def test_multi_hop_cross_model_aggregate_renders_full_chain(tmp_path):
    """``orders → customers → regions.population:sum`` -- the CTE walks
    BOTH hops inside its body. Legacy emits a single ``_cm_`` CTE with
    ``FROM orders LEFT JOIN customers LEFT JOIN regions GROUP BY ...``;
    the new path must match.
    """
    storage = await _seed_storage(tmp_path)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.regions.population:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


def test_multi_hop_with_local_dim_and_having(tmp_path):
    """Multi-hop aggregate + local dim + HAVING on the multi-hop agg.

    Pins both that the CTE renders the multi-hop target's column, AND
    that a HAVING filter referencing the cross-model agg-ref
    propagates into the CTE as HAVING. Legacy raises on the colon-
    form filter against a multi-hop target (DEV-1445 territory), so
    this is structural-only.
    """
    bundle = _bundle()
    query = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.regions.population:sum"}],
        filters=["customers.regions.population:sum > 1000"],
    )
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    n = norm_sql(new)
    cm_defs = [c for c in _cte_names(n) if c.startswith("_cm_")]
    assert len(cm_defs) == 1
    body_upper = _cte_body(n, cm_defs[0]).upper()
    assert "SUM(REGIONS.POPULATION)" in body_upper, (
        f"expected SUM(regions.population) in multi-hop CTE; got {body_upper!r}"
    )
    assert " HAVING " in body_upper


# ---------------------------------------------------------------------------
# Column.filter wired through AggregateKey.column_filter_key (closes the
# 7b.9 deferral)
# ---------------------------------------------------------------------------


async def test_local_column_filter_renders_case_when(tmp_path):
    """LOCAL aggregation of a column with a ``Column.filter`` set —
    ``SUM(orders.amount) FILTER ... → SUM(CASE WHEN <filter> THEN
    orders.amount END)`` inside the base CTE / base SELECT.

    This is the local-only flavour: confirms that ``_bind_agg``
    propagates the filter into ``AggregateKey.column_filter_key`` and
    the generator emits the CASE-WHEN wrap. The xfail in
    ``test_generator2_local.py::test_planner_populates_column_filter_key_for_filtered_column``
    becomes a passing assertion when 7b.12 lands; this parity test
    pins the SQL shape directly.
    """
    orders_with_filtered = SlayerModel(
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
    storage = await build_storage_with_models(tmp_path, orders_with_filtered)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(
        engine=engine, model=orders_with_filtered, query=query,
    )
    bundle = ResolvedSourceBundle(source_model=orders_with_filtered)
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    assert_sql_equivalent(legacy, new)
    # Sanity: the CASE WHEN survives normalisation.
    upper = norm_sql(new).upper()
    assert "CASE WHEN" in upper
    assert "STATUS = 'PAID'" in upper


async def test_cross_model_column_filter_renders_case_when_in_cte(tmp_path):
    """Cross-model aggregate of a column with ``Column.filter`` —
    the CASE-WHEN goes INSIDE the ``_cm_`` CTE, wrapping the aggregate
    argument. This is the cross-model flavour of the prior test.

    The legacy path emits this via ``_has_cross_model_filter`` / CASE-
    WHEN; parity asserts identical output.
    """
    storage = await _seed_storage(
        tmp_path, revenue_filter="status = 'active'",
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.revenue:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    bundle = _bundle(revenue_filter="status = 'active'")
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    assert_sql_equivalent(legacy, new)
    # The CASE-WHEN must live INSIDE the _cm_ CTE body (not at the
    # base / outer SELECT) — Codex LOW fold-in.
    n = norm_sql(new)
    cm_defs = [c for c in _cte_names(n) if c.startswith("_cm_")]
    assert len(cm_defs) == 1
    body_upper = _cte_body(n, cm_defs[0]).upper()
    assert "CASE WHEN" in body_upper
    assert "STATUS = 'ACTIVE'" in body_upper


# ---------------------------------------------------------------------------
# Target SlayerModel.filters propagate as CTE WHERE
# ---------------------------------------------------------------------------


def test_target_path_row_filter_propagates_to_cte_where(tmp_path):
    """Codex HIGH fold-in: a ROW filter on the joined-target path
    (``customers.status == 'active'``) propagates into the cross-model
    CTE as ``WHERE``, NOT into the host base. The CTE must NOT GROUP
    BY the filtered column (it's filter-only, not a shared grain), and
    the outer join-back must NOT reference the filtered column as a
    join key (``_base`` doesn't project it). Legacy keeps the filter at
    the host base with a LEFT JOIN — the typed pipeline propagates per
    the inherited_filter_policy decision table; structural assertion
    only.
    """
    bundle = _bundle()
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.revenue:sum"}],
        filters=["customers.status == 'active'"],
    )
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    n = norm_sql(new)
    cm_defs = [c for c in _cte_names(n) if c.startswith("_cm_")]
    assert len(cm_defs) == 1
    body = _cte_body(n, cm_defs[0])
    body_upper = body.upper()
    # WHERE inside the CTE carries the target-path predicate.
    assert " WHERE " in body_upper
    assert "STATUS = 'ACTIVE'" in body_upper
    # The filter-only column must NOT be GROUP BY'd inside the CTE
    # (it's not a shared grain) and the outer join must be CROSS JOIN
    # (no shared grain columns to join on).
    assert " GROUP BY " not in body_upper, (
        f"CTE must not GROUP BY filter-only column; body: {body!r}"
    )
    assert "CROSS JOIN" in n.upper(), (
        f"outer must CROSS JOIN when no shared grain; SQL: {n!r}"
    )


async def test_target_model_filters_propagate_to_cte_where(tmp_path):
    """The target model's ``SlayerModel.filters`` (always-applied
    WHERE) flow into the cross-model CTE's WHERE clause. Legacy path
    inlines them through ``_build_where_and_having`` filtered by
    available tables; the new path renders them via
    ``CrossModelAggregatePlan.target_model_filters``.
    """
    storage = await _seed_storage(
        tmp_path, customers_filters=["deleted_at IS NULL"],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.revenue:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    bundle = _bundle(customers_filters=["deleted_at IS NULL"])
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    assert_sql_equivalent(legacy, new)
    # Pin the filter INSIDE the _cm_ CTE body (in its WHERE), not just
    # somewhere in the SQL — Codex LOW fold-in.
    n = norm_sql(new)
    cm_defs = [c for c in _cte_names(n) if c.startswith("_cm_")]
    assert len(cm_defs) == 1
    body_upper = _cte_body(n, cm_defs[0]).upper()
    assert "DELETED_AT IS NULL" in body_upper


# ---------------------------------------------------------------------------
# Joined time dimensions (closes 7b.9 deferral)
# ---------------------------------------------------------------------------


async def test_joined_time_dimension_with_cross_model_aggregate(tmp_path):
    """``customers.created_at`` month-truncated time dimension joined
    with a cross-model aggregate ``customers.revenue:sum``. Both rely on
    the same join chain; the generator must:

    * walk ``orders → customers`` once in the base SELECT to surface
      the truncated time dimension (legacy emits it on the base side via
      the resolved-joins chain);
    * emit a ``_cm_`` CTE for the aggregate that shares the joined TD
      as its grain so the join-back lines up.

    Pins that joined-TD dimension refs in the host projection are no
    longer rejected with the 7b.12 marker.
    """
    storage = await _seed_storage(tmp_path)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="customers.created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[{"formula": "customers.revenue:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


async def test_joined_dimension_in_projection_no_aggregate(tmp_path):
    """``customers.region_id`` as a plain dimension on a host-aggregate-
    only query exercises the joined-ROW-dim branch of
    ``_build_base_select_for_planned``. Without an aggregate on the
    joined target, no cross-model CTE is emitted — the join chain is
    walked once in the base SELECT. Closes the
    ``path != ()`` ``ColumnKey`` deferral.
    """
    storage = await _seed_storage(tmp_path)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="customers.region_id")],
        measures=[{"formula": "amount:sum"}],
    )
    legacy = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# DEV-1445 acceptance — renamed cross-model measure: filter and order by
# either the alias or the dotted form bind to one slot, produce one CTE.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "DEV-1450 stage 7b.15 (DEV-1445 acceptance): the binder does "
        "not yet resolve user-declared measure aliases (e.g. ``rev``) "
        "in filter scope — ``bind_filter`` only walks ModelScope's "
        "model columns, so a filter ``rev >= 100`` fails with "
        "``UnknownReferenceError`` before reaching the cross-model "
        "planner's HAVING route. Plan: 7b.15 lands the alias-in-filter "
        "binding under ``tests/test_dev1445_*.py`` alongside the full "
        "acceptance suite. Pinned here so the gap stays visible."
    ),
)
def test_dev1445_alias_filter_and_dotted_filter_share_one_cte(tmp_path):
    """DEV-1445 acceptance (planner shape — no parity, legacy diverges).

    ``{"formula": "customers.revenue:sum", "name": "rev"}`` plus a
    filter ``["rev >= 100"]`` AND a filter on the dotted form must
    intern to ONE cross-model aggregate slot. The new generator emits
    ONE ``_cm_`` CTE; the HAVING inside the CTE contains the predicate.

    Legacy path raises on the alias-in-filter form (the resolver hits a
    user-alias before walking the join graph); the typed pipeline
    accepts both forms and dedupes them onto the same slot.
    """
    bundle = _bundle()
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
        filters=["rev >= 100"],
    )
    planned = plan_query(query=query, bundle=bundle)
    # Exactly one cross-model aggregate plan.
    assert len(planned.cross_model_aggregate_plans) == 1, (
        f"expected one CMA plan; got {planned.cross_model_aggregate_plans!r}"
    )
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    n = norm_sql(new)
    # The CTE chain has exactly one _cm_ definition.
    cm_defs = [c for c in _cte_names(n) if c.startswith("_cm_")]
    assert len(cm_defs) == 1, f"expected one _cm_ CTE; got {_cte_names(n)}"
    # HAVING propagated into the CTE body (the filter references the
    # renamed cross-model aggregate slot).
    body = _cte_body(n, cm_defs[0])
    assert " HAVING " in body.upper(), (
        f"expected HAVING inside _cm_ CTE; body was {body!r}"
    )
    # The renamed alias surfaces in the outer projection.
    assert '"orders.rev"' in n


@pytest.mark.xfail(
    strict=True,
    reason=(
        "DEV-1450 stage 7b.15 (DEV-1445 acceptance): same gap as the "
        "alias-only-filter case above — when both forms appear "
        "together, the alias form still fails to bind under the "
        "current binder. Resolution lands in 7b.15."
    ),
)
def test_dev1445_alias_and_dotted_filter_together_share_one_cte(tmp_path):
    """Same DEV-1445 acceptance, both filter forms together. The
    structural-key contract (P2) means both filters reference the same
    slot, so only ONE HAVING clause is emitted.
    """
    bundle = _bundle()
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
        filters=["rev >= 100", "customers.revenue:sum >= 100"],
    )
    planned = plan_query(query=query, bundle=bundle)
    assert len(planned.cross_model_aggregate_plans) == 1
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    n = norm_sql(new)
    cm_defs = [c for c in _cte_names(n) if c.startswith("_cm_")]
    assert len(cm_defs) == 1
    body = _cte_body(n, cm_defs[0]).upper()
    # The two filters dedupe at the slot level: ONE HAVING branch and
    # the predicate text appears exactly once even when both filter
    # forms were given. Codex MEDIUM fold-in — assert idempotence at
    # the predicate level so a duplicated HAVING (two copies of
    # ``>= 100`` joined by AND) does not pass.
    assert body.count(" HAVING ") == 1
    assert body.count(">= 100") == 1


def test_dev1445_alias_order_by_renamed_cross_model_measure(tmp_path):
    """DEV-1445 acceptance (ORDER BY half).

    The renamed cross-model measure ``{"formula": "customers.revenue:sum",
    "name": "rev"}`` is ordered by its USER ALIAS (``rev``) — not the
    dotted form. The alias resolves to the same cross-model aggregate
    slot; the outer ORDER BY renders against the joined-back ``_cm_``
    alias under the user name.

    Legacy raises on alias-as-ORDER-BY for cross-model (the resolver
    hits the user alias before walking the join graph); so this is
    structural-only (no parity).
    """
    bundle = _bundle()
    query = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
        order=[OrderItem(column="rev", direction="desc")],
        limit=5,
    )
    planned = plan_query(query=query, bundle=bundle)
    # The order entry must bind to the cross-model aggregate slot.
    assert len(planned.order) == 1
    assert len(planned.cross_model_aggregate_plans) == 1
    plan = planned.cross_model_aggregate_plans[0]
    assert planned.order[0].slot_id == plan.aggregate_slot_id, (
        f"alias 'rev' did not bind to the cross-model aggregate slot; "
        f"order[0].slot_id={planned.order[0].slot_id!r}, "
        f"plan.aggregate_slot_id={plan.aggregate_slot_id!r}"
    )
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    n = norm_sql(new).upper()
    # The outer ORDER BY surfaces against the renamed user alias.
    assert "ORDER BY" in n
    assert '"ORDERS.REV"' in n or '"REV"' in n


def test_multi_alias_same_key_cross_model_shares_one_cte(tmp_path):
    """P4 / C13 cross-model flavour: declaring the SAME cross-model
    aggregate twice under different ``name``s interns to ONE slot and
    ONE ``_cm_`` CTE; both aliases surface in the outer projection.

    Legacy raises on alias collision; this is structural-only (no
    parity). Mirrors the 7b.11 self-join test
    ``test_dev1450_c13_two_declared_time_shift_aliases_share_one_slot``.
    """
    bundle = _bundle()
    query = SlayerQuery(
        source_model="orders",
        measures=[
            {"formula": "customers.revenue:sum", "name": "rev_a"},
            {"formula": "customers.revenue:sum", "name": "rev_b"},
        ],
    )
    planned = plan_query(query=query, bundle=bundle)
    # Exactly one cross-model aggregate plan (shared slot identity).
    assert len(planned.cross_model_aggregate_plans) == 1
    # The shared slot carries BOTH user aliases.
    plan = planned.cross_model_aggregate_plans[0]
    by_id = {s.id: s for s in planned.aggregate_slots}
    slot = by_id[plan.aggregate_slot_id]
    assert sorted(slot.public_aliases) == ["rev_a", "rev_b"], (
        f"expected both aliases on one slot; got {slot.public_aliases!r}"
    )
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    n = norm_sql(new)
    # Only ONE _cm_ CTE.
    cm_defs = [c for c in _cte_names(n) if c.startswith("_cm_")]
    assert len(cm_defs) == 1, f"expected one _cm_ CTE; got {_cte_names(n)}"
    # Both aliases surface in the outer projection.
    assert '"orders.rev_a"' in n
    assert '"orders.rev_b"' in n


# ---------------------------------------------------------------------------
# Order by cross-model aggregate — exercise the outer ORDER BY against the
# joined-back ``_cm_`` alias.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "DEV-1450 stage 7b.15: ``OrderItem(column='customers.revenue:sum')`` "
        "currently mangles in ``ColumnRef``'s string before-validator "
        "(strips the path + colon → ``'revenue_sum'``), so the planner "
        "fails to bind a cross-model aggregate ORDER BY. The canonical "
        "alias path ``customers.revenue_sum`` works, but the colon form "
        "in the OrderItem string constructor is broken. Resolution "
        "lands with the DEV-1438 / DEV-1443 cross-cutting fix in 7b.15."
    ),
)
def test_order_by_cross_model_aggregate(tmp_path):
    """``ORDER BY customers.revenue:sum DESC LIMIT 5`` — the order key
    is a cross-model aggregate slot. The typed pipeline emits ``ORDER
    BY "orders.customers.revenue_sum" DESC LIMIT 5`` directly at the
    combined SELECT; legacy wraps in ``SELECT ... FROM (...) AS
    _outer`` via ``_apply_outer_projection_trim`` so SQL bit-for-bit
    equality is not achievable. Structural assertion only — the
    ordering reference is correct and limit applies.
    """
    bundle = _bundle()
    query = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.revenue:sum"}],
        order=[OrderItem(column="customers.revenue:sum", direction="desc")],
        limit=5,
    )
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    n = norm_sql(new).upper()
    assert "ORDER BY" in n
    assert '"ORDERS.CUSTOMERS.REVENUE_SUM" DESC' in n
    assert "LIMIT 5" in n
    cm_defs = [c for c in _cte_names(norm_sql(new)) if c.startswith("_cm_")]
    assert len(cm_defs) == 1


# ---------------------------------------------------------------------------
# Removal of 7b.12 deferral markers — these used to raise; they must not.
# ---------------------------------------------------------------------------


async def test_cross_model_aggregate_no_longer_raises_7b12_marker(tmp_path):
    """The local-only generator slice (7b.8) explicitly raised
    ``NotImplementedError('DEV-1450 stage 7b.12: cross_model_aggregate_plans
    ... deferred to the cross-model slice.')`` when a planned query
    carried a cross-model plan. After 7b.12 ships, that branch must not
    fire on a normal cross-model query.

    Smoke-only — parity coverage above already pins the SQL shape.
    Pinning the deferral-removal explicitly catches regressions where
    the cutover would silently leave the guard in place.
    """
    storage = await _seed_storage(tmp_path)
    engine = SlayerQueryEngine(storage=storage)  # noqa: F841  (legacy parity not asserted here)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.revenue:sum"}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    # Must not raise. If 7b.12 leaves the cross-model deferral in
    # place this call surfaces a NotImplementedError with the marker.
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert isinstance(sql, str) and sql.strip()


def test_joined_td_no_longer_raises_7b12_marker():
    """The local-only generator slice raised ``NotImplementedError(
    'DEV-1450 stage 7b.12: joined TD refs ... deferred to the cross-
    model slice.')`` for a ``TimeTruncKey.column.path != ()``. After
    7b.12 ships, that branch must not fire — the joined TD renders
    through the cross-model planner's shared-grain machinery (and on the
    base side when no aggregate on the same target is present).
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="customers.created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[{"formula": "customers.revenue:sum"}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    # Must not raise.
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert isinstance(sql, str) and sql.strip()


def test_column_filter_key_no_longer_raises_7b12_marker():
    """The local-only generator slice raised when ``AggregateKey``
    carried ``column_filter_key != None``. After 7b.12 ships the binder
    populates ``column_filter_key`` from ``Column.filter`` AND the
    generator renders the CASE-WHEN — neither side should fire the
    deferral.
    """
    orders_with_filtered = SlayerModel(
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
    bundle = ResolvedSourceBundle(source_model=orders_with_filtered)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
    )
    planned = plan_query(query=query, bundle=bundle)
    sql = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    assert isinstance(sql, str) and sql.strip()
    upper = norm_sql(sql).upper()
    assert "CASE WHEN" in upper


# ---------------------------------------------------------------------------
# Planner-side: Column.filter now surfaces on AggregateKey.column_filter_key.
# The xfail in test_generator2_local.py becomes a passing assertion when
# 7b.12 lands; this is the cross-model flavour pinned in the new file.
# ---------------------------------------------------------------------------


def test_planner_populates_column_filter_key_for_cross_model_filtered_column():
    """Cross-model flavour of the planner contract: a ``Column.filter``
    on the aggregated cross-model column must surface as
    ``AggregateKey.column_filter_key`` so the generator renders the
    CASE-WHEN inside the ``_cm_`` CTE.
    """
    bundle = _bundle(revenue_filter="status = 'active'")
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "customers.revenue:sum"}],
    )
    planned = plan_query(query=query, bundle=bundle)
    assert len(planned.cross_model_aggregate_plans) == 1
    # The aggregate slot referenced by the plan must have a non-None
    # column_filter_key populated from Column.filter on customers.revenue.
    plan = planned.cross_model_aggregate_plans[0]
    by_id = {s.id: s for s in planned.aggregate_slots}
    agg_slot = by_id.get(plan.aggregate_slot_id)
    assert agg_slot is not None, "cross-model plan's aggregate slot not found"
    assert isinstance(agg_slot.key, AggregateKey)
    assert agg_slot.key.column_filter_key is not None, (
        "Column.filter on customers.revenue was dropped — _bind_agg must "
        "look up the target-model column and propagate its filter into "
        "AggregateKey.column_filter_key."
    )
