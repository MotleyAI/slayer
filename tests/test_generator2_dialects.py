"""DEV-1450 stage 7b.13 -- dialect-parity tests for the new generator path.

Pins the contract:

* ``generate_from_planned(plan_query(q, bundle), dialect=D)`` emits SQL
  whitespace-canonical-equal to ``SQLGenerator(D).generate(_enrich(q))``
  for every parameterised aggregation across Tier-1 dialects
  (postgres / sqlite / duckdb / mysql / clickhouse) -- AND for the
  cross-model rerooted-CTE form of the same aggregations.
* MySQL raises ``NotImplementedError`` for ``percentile`` / ``median`` /
  ``corr`` / ``covar_samp`` / ``covar_pop`` on BOTH paths with the same
  legacy substring (``"is not supported on MySQL"``).
* ``log10(x)`` and ``log2(x)`` written by the user in ``Column.sql``
  round-trip as function-call form on every dialect in
  ``_LOG10_NATIVE_DIALECTS`` / ``_LOG2_NATIVE_DIALECTS``; dialects
  outside those allowlists (oracle for log10/log2; tsql for log2) fall
  back to canonical ``LOG(N, x)``.
* ``json_extract(col, '$.path')`` in ``Column.sql`` is preserved on
  SQLite as the function-call form, NEVER rewritten to the ``col -> '$.path'``
  operator (which silently returns JSON-quoted strings and breaks
  equality matches).
* ``_build_time_offset_expr`` dialect branches (postgres ``INTERVAL`` vs
  sqlite ``DATETIME(... '+N units')``) emit identical SQL via the new
  pipeline as via legacy for the ``change(measure)`` desugar form.

The 7b.13 implementation work this file forces:

1. ``slayer/core/refs.py`` -- new helper ``agg_kwarg_canonical_str``
   converting AggregateKey kwarg values (Decimal / int / float / str /
   ColumnKey) into the SQL-string form ``EnrichedMeasure.agg_kwargs``
   (a ``Dict[str, str]``) requires AND that ``canonical_agg_name``'s
   downstream display path expects.

2. ``slayer/sql/generator.py:_synthesize_enriched_measure_from_planned``
   (``:5510-5629``) -- drop both the kwarg deferral and the
   ``_BUILTIN_BAREARG_AGGS_LOCAL_SLICE`` agg-name deferral; validate
   that every kwarg ``ColumnKey`` has ``path == source.path`` (raise
   ``AggregationNotAllowedError`` otherwise); stringify kwargs via
   the new helper.

3. Cross-model rerooting (``generator.py:3900-3908``) also reroots
   ``key.kwargs``, stripping the cross-model target prefix from each
   ColumnKey kwarg so the synth helper sees ``path == source.path == ()``.

4. Two existing canonical-alias sites
   (``generator.py:3753`` and ``cross_model_planner.py:286-287``) stop
   calling ``str(v)`` on ColumnKey kwargs -- which would otherwise
   surface as Pydantic-repr garbage -- and use the new helper.

Deleted alongside ``tests/parity_oracle.py`` at the end of 7b.15.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import AggregationNotAllowedError
from slayer.core.keys import AggregateKey, ColumnKey
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
from slayer.sql.generator import SQLGenerator, generate_from_planned
from tests.parity_oracle import (
    assert_sql_equivalent,
    build_storage_with_models,
    legacy_sql_for,
    norm_sql,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


_TIER1_DIALECTS: Tuple[str, ...] = (
    "postgres",
    "sqlite",
    "duckdb",
    "mysql",
    "clickhouse",
)


# Aggregations the legacy ``_build_*`` family rejects on MySQL with
# ``NotImplementedError`` (``_build_percentile`` :2418, ``_build_median``
# :2371, ``_build_stat_agg`` :2472). The new path must mirror BOTH the
# error type AND the substring shape.
_MYSQL_UNSUPPORTED_AGGS: Tuple[str, ...] = (
    "percentile",
    "median",
    "corr",
    "covar_samp",
    "covar_pop",
)


_MYSQL_UNSUPPORTED_SUBSTRING = "is not supported on MySQL"


# ---------------------------------------------------------------------------
# Model fixtures
# ---------------------------------------------------------------------------


def _orders() -> SlayerModel:
    """Host model. Columns chosen to cover every kwarg-bearing aggregation
    plus log/JSON preservation cases:

    * ``amount`` / ``quantity`` -- value + 2nd-leg columns for corr / covar / weighted_avg.
    * ``status`` -- group-by dim AND filter source.
    * ``region_id`` -- join key to ``customers``.
    * ``created_at`` -- TIMESTAMP for the time_shift dialect branches.
    * ``meta`` -- TEXT carrying JSON; feeds the json_extract preservation case.
    * ``log_amount`` (derived) -- ``sql="log10(amount)"``; one model variant
      replaces this with ``log2(amount)``.
    """
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="quantity", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="region_id", type=DataType.INT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="meta", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            ),
        ],
    )


def _orders_with_derived(derived_sql: str, derived_name: str = "log_amount") -> SlayerModel:
    """Like ``_orders`` but with one derived column injected. Used by the
    log-alias and JSON-extract tests.
    """
    base = _orders()
    cols = list(base.columns) + [
        Column(name=derived_name, sql=derived_sql, type=DataType.DOUBLE),
    ]
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=cols,
        joins=list(base.joins),
    )


def _orders_with_filtered_amount() -> SlayerModel:
    """Variant with ``Column.filter`` on ``amount`` -- exercises filtered-
    aggregate × kwarg interaction (legacy CASE-WHEN-wraps both legs).
    """
    base = _orders()
    new_cols: List[Column] = []
    for col in base.columns:
        if col.name == "amount":
            new_cols.append(
                Column(
                    name="amount",
                    type=DataType.DOUBLE,
                    filter="status = 'paid'",
                ),
            )
        else:
            new_cols.append(col)
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=new_cols,
        joins=list(base.joins),
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
            Column(name="quantity", type=DataType.DOUBLE),
        ],
    )


def _bundle(host: Optional[SlayerModel] = None) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=host or _orders(),
        referenced_models=[_customers()],
    )


# ---------------------------------------------------------------------------
# Helper: route MySQL × {percentile/median/corr/covar} cases via raises
# ---------------------------------------------------------------------------


_KWARG_UNSUPPORTED_MARKERS = ("percentile", "median", "corr", "covar")


def _is_mysql_unsupported(case_label: str, dialect: str) -> bool:
    if dialect != "mysql":
        return False
    return any(m in case_label for m in _KWARG_UNSUPPORTED_MARKERS)


# ---------------------------------------------------------------------------
# Local aggregation parity cases
# ---------------------------------------------------------------------------


# (case_label, query_kwargs). One per parametrize id. Each case is
# small enough that GROUP BY is trivial (no dimension by default) so
# the focus stays on the aggregation expression itself.
_LOCAL_AGG_CASES: List[Tuple[str, Dict[str, Any]]] = [
    ("percentile_p_05", dict(
        source_model="orders",
        measures=[{"formula": "amount:percentile(p=0.5)"}],
    )),
    ("percentile_p_095", dict(
        source_model="orders",
        measures=[{"formula": "amount:percentile(p=0.95)"}],
    )),
    ("median_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:median"}],
    )),
    ("weighted_avg_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:weighted_avg(weight=quantity)"}],
    )),
    ("corr_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:corr(other=quantity)"}],
    )),
    ("covar_samp_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:covar_samp(other=quantity)"}],
    )),
    ("covar_pop_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:covar_pop(other=quantity)"}],
    )),
    ("stddev_samp_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:stddev_samp"}],
    )),
    ("stddev_pop_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:stddev_pop"}],
    )),
    ("var_samp_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:var_samp"}],
    )),
    ("var_pop_local", dict(
        source_model="orders",
        measures=[{"formula": "amount:var_pop"}],
    )),
    ("percentile_with_dim", dict(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:percentile(p=0.5)"}],
    )),
]


@pytest.mark.parametrize(
    "case_label,query_kwargs",
    _LOCAL_AGG_CASES,
    ids=[c[0] for c in _LOCAL_AGG_CASES],
)
@pytest.mark.parametrize("dialect", _TIER1_DIALECTS)
async def test_local_aggregation_dialect_parity(
    case_label, query_kwargs, dialect, tmp_path,
):
    storage = await build_storage_with_models(
        tmp_path, _customers(), _orders(),
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(**query_kwargs)
    if _is_mysql_unsupported(case_label, dialect):
        with pytest.raises(NotImplementedError) as legacy_exc:
            await legacy_sql_for(
                engine=engine, model=_orders(), query=query, dialect=dialect,
            )
        assert _MYSQL_UNSUPPORTED_SUBSTRING in str(legacy_exc.value)
        planned = plan_query(query=query, bundle=_bundle())
        with pytest.raises(NotImplementedError) as new_exc:
            generate_from_planned(planned, bundle=_bundle(), dialect=dialect)
        assert _MYSQL_UNSUPPORTED_SUBSTRING in str(new_exc.value)
        return
    legacy = await legacy_sql_for(
        engine=engine, model=_orders(), query=query, dialect=dialect,
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect=dialect)
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Filtered-column × kwarg parity (Codex MED #4 fold-in)
# ---------------------------------------------------------------------------


# Aggregations that take a 2nd-leg kwarg + a filtered value column.
# Legacy wraps BOTH legs in CASE WHEN; the synth adapter's path-validation
# + stringification must preserve that.
_FILTERED_KWARG_CASES: List[Tuple[str, str]] = [
    ("filtered_corr", "amount:corr(other=quantity)"),
    ("filtered_covar_samp", "amount:covar_samp(other=quantity)"),
    ("filtered_weighted_avg", "amount:weighted_avg(weight=quantity)"),
    ("filtered_percentile", "amount:percentile(p=0.5)"),
]


@pytest.mark.parametrize(
    "case_label,formula",
    _FILTERED_KWARG_CASES,
    ids=[c[0] for c in _FILTERED_KWARG_CASES],
)
@pytest.mark.parametrize("dialect", ("postgres", "sqlite"))
async def test_filtered_two_column_agg_parity(
    case_label, formula, dialect, tmp_path,
):
    model = _orders_with_filtered_amount()
    storage = await build_storage_with_models(tmp_path, _customers(), model)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": formula}],
    )
    bundle = ResolvedSourceBundle(
        source_model=model, referenced_models=[_customers()],
    )
    legacy = await legacy_sql_for(
        engine=engine, model=model, query=query, dialect=dialect,
    )
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect=dialect)
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Cross-model aggregation parity
# ---------------------------------------------------------------------------


# Cross-model parametric aggs (percentile, corr, covar, weighted_avg)
# CANNOT parity-test against the legacy oracle: legacy
# ``query_engine.py:2160`` drops the agg signature suffix from
# cross-model aliases, while the new pipeline preserves it (7b.5 fix
# at ``cross_model_planner.py:_aggregate_alias``). The two paths
# produce different CTE / outer-alias shapes for the same query.
# Cross-model bare aggregations (no kwargs/args) DO parity-match
# because no signature suffix is involved.
_CROSS_MODEL_AGG_CASES: List[Tuple[str, Dict[str, Any]]] = [
    ("cm_median", dict(
        source_model="orders",
        measures=[{"formula": "customers.revenue:median"}],
    )),
    ("cm_stddev_samp", dict(
        source_model="orders",
        measures=[{"formula": "customers.revenue:stddev_samp"}],
    )),
    ("cm_stddev_pop", dict(
        source_model="orders",
        measures=[{"formula": "customers.revenue:stddev_pop"}],
    )),
    ("cm_var_samp", dict(
        source_model="orders",
        measures=[{"formula": "customers.revenue:var_samp"}],
    )),
    ("cm_var_pop", dict(
        source_model="orders",
        measures=[{"formula": "customers.revenue:var_pop"}],
    )),
]


# Cross-model PARAMETRIC aggregates -- tested structurally (typed-only)
# because parity vs legacy is not achievable (legacy drops kwarg
# suffix). Asserts the new pipeline produces ONE CrossModelAggregatePlan
# per measure with the correct aggregation, kwargs threaded into the
# slot's key, and the CTE / outer alias including the kwarg signature.
_CROSS_MODEL_PARAMETRIC_CASES: List[Tuple[str, str, str]] = [
    # (case_label, formula, expected_substring_in_emitted_sql)
    ("cm_percentile",
     "customers.revenue:percentile(p=0.5)",
     "PERCENTILE_CONT(0.5)"),
    ("cm_corr",
     "customers.revenue:corr(other=customers.region_id)",
     "CORR(customers.revenue, customers.region_id)"),
    ("cm_covar_samp",
     "customers.revenue:covar_samp(other=customers.region_id)",
     "COVAR_SAMP(customers.revenue, customers.region_id)"),
    ("cm_covar_pop",
     "customers.revenue:covar_pop(other=customers.region_id)",
     "COVAR_POP(customers.revenue, customers.region_id)"),
    ("cm_weighted_avg",
     "customers.revenue:weighted_avg(weight=customers.quantity)",
     "SUM(customers.revenue * customers.quantity)"),
]


@pytest.mark.parametrize(
    "case_label,query_kwargs",
    _CROSS_MODEL_AGG_CASES,
    ids=[c[0] for c in _CROSS_MODEL_AGG_CASES],
)
@pytest.mark.parametrize("dialect", _TIER1_DIALECTS)
async def test_cross_model_aggregation_dialect_parity(
    case_label, query_kwargs, dialect, tmp_path,
):
    storage = await build_storage_with_models(
        tmp_path, _customers(), _orders(),
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(**query_kwargs)
    if _is_mysql_unsupported(case_label, dialect):
        with pytest.raises(NotImplementedError) as legacy_exc:
            await legacy_sql_for(
                engine=engine, model=_orders(), query=query, dialect=dialect,
            )
        assert _MYSQL_UNSUPPORTED_SUBSTRING in str(legacy_exc.value)
        planned = plan_query(query=query, bundle=_bundle())
        with pytest.raises(NotImplementedError) as new_exc:
            generate_from_planned(planned, bundle=_bundle(), dialect=dialect)
        assert _MYSQL_UNSUPPORTED_SUBSTRING in str(new_exc.value)
        return
    legacy = await legacy_sql_for(
        engine=engine, model=_orders(), query=query, dialect=dialect,
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect=dialect)
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# Cross-model PARAMETRIC aggregates (structural, not parity)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case_label,formula,expected_body",
    _CROSS_MODEL_PARAMETRIC_CASES,
    ids=[c[0] for c in _CROSS_MODEL_PARAMETRIC_CASES],
)
def test_cross_model_parametric_agg_structural(
    case_label, formula, expected_body,
):
    """Cross-model parametric aggs (percentile, corr, covar, weighted_avg)
    emit the correct aggregation SQL in the CTE body. Legacy
    ``query_engine.py:2160`` drops the agg signature suffix from
    cross-model canonical aliases, so the new pipeline's alias
    (kwarg-suffixed, per 7b.5) diverges from legacy in shape -- no
    parity assertion possible.

    Structural assertions:
    * The planner produces exactly one ``CrossModelAggregatePlan`` for
      the measure.
    * The emitted SQL contains the dialect's aggregation invocation
      for the chosen agg with the kwarg column threaded through (for
      kwarg-bearing aggs) or the literal value (for percentile).

    Postgres-only structural pin -- other Tier-1 dialects diverge in
    aggregate-function spelling (duckdb ``QUANTILE_CONT``, sqlite UDF
    ``percentile_cont``, clickhouse parametric ``quantile(p)(x)``)
    and are covered separately via the local-aggregation parity
    matrix that routes through ``_build_stat_agg`` /
    ``_build_percentile``.
    """
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": formula}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    assert len(planned.cross_model_aggregate_plans) == 1, (
        f"expected exactly one cross-model plan; got "
        f"{planned.cross_model_aggregate_plans!r}"
    )
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert expected_body in sql, (
        f"expected {expected_body!r} substring in emitted SQL; got: {sql!r}"
    )


# ---------------------------------------------------------------------------
# MySQL NotImplementedError parity for every flagged aggregation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("agg", _MYSQL_UNSUPPORTED_AGGS)
async def test_mysql_unsupported_aggregations_raise(agg, tmp_path):
    """Both legacy AND new paths must raise ``NotImplementedError`` with
    the same legacy substring on MySQL for these aggregations.
    """
    storage = await build_storage_with_models(
        tmp_path, _customers(), _orders(),
    )
    engine = SlayerQueryEngine(storage=storage)
    if agg == "percentile":
        formula = "amount:percentile(p=0.5)"
    elif agg in ("corr", "covar_samp", "covar_pop"):
        formula = f"amount:{agg}(other=quantity)"
    else:
        formula = f"amount:{agg}"
    query = SlayerQuery(source_model="orders", measures=[{"formula": formula}])
    with pytest.raises(NotImplementedError) as legacy_exc:
        await legacy_sql_for(
            engine=engine, model=_orders(), query=query, dialect="mysql",
        )
    assert _MYSQL_UNSUPPORTED_SUBSTRING in str(legacy_exc.value)
    planned = plan_query(query=query, bundle=_bundle())
    with pytest.raises(NotImplementedError) as new_exc:
        generate_from_planned(planned, bundle=_bundle(), dialect="mysql")
    assert _MYSQL_UNSUPPORTED_SUBSTRING in str(new_exc.value)


# ---------------------------------------------------------------------------
# Log-alias preservation across Tier-1 dialects
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fn", ("log10", "log2"))
@pytest.mark.parametrize("dialect", _TIER1_DIALECTS)
async def test_log_alias_preservation_tier1(fn, dialect, tmp_path):
    """A user-authored ``log10(x)`` / ``log2(x)`` in a Mode A SQL
    fragment round-trips as the function-call form on every Tier-1
    dialect (per ``_LOG10_NATIVE_DIALECTS`` /
    ``_LOG2_NATIVE_DIALECTS``).

    ``_rewrite_log_aliases`` runs inside ``SQLGenerator._parse``;
    whether the call originates from ``Column.sql`` (derived column),
    ``Column.filter``, or ``SlayerModel.filters`` (this exerciser), the
    rewrite applies uniformly. Model-level filter is used here because
    the new generator's row-phase ``ColumnSqlKey`` path is deferred
    and Mode B's ``SCALAR_FUNCTIONS`` form (``log10(amount:sum) + 1``)
    needs arithmetic-over-aggregate support that's also deferred. The
    Mode A filter route reaches the rewriter via the same ``_parse``
    boundary either way.
    """
    base = _orders()
    orders_with_log_filter = SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=list(base.columns),
        joins=list(base.joins),
        filters=[f"{fn}(amount) > 0"],
    )
    storage = await build_storage_with_models(
        tmp_path, _customers(), orders_with_log_filter,
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
    )
    bundle = ResolvedSourceBundle(
        source_model=orders_with_log_filter,
        referenced_models=[_customers()],
    )
    legacy = await legacy_sql_for(
        engine=engine, model=orders_with_log_filter, query=query, dialect=dialect,
    )
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect=dialect)
    assert_sql_equivalent(legacy, new)
    # The function-call form survives in WHERE; not normalised to LOG(N, x).
    assert f"{fn.upper()}(" in new.upper() or f"{fn}(" in new, (
        f"log-alias {fn!r} not preserved on {dialect!r}; emitted: {new!r}"
    )


# ---------------------------------------------------------------------------
# Log-alias fallback edges (oracle / tsql)
# ---------------------------------------------------------------------------


# (dialect, fn). Only the cases where the allowlist excludes the dialect.
_LOG_FALLBACK_CASES: List[Tuple[str, str]] = [
    ("oracle", "log10"),  # oracle NOT in _LOG10_NATIVE_DIALECTS
    ("oracle", "log2"),   # oracle NOT in _LOG2_NATIVE_DIALECTS
    ("tsql", "log2"),     # tsql in _LOG10 but NOT in _LOG2
]


@pytest.mark.parametrize("dialect,fn", _LOG_FALLBACK_CASES)
async def test_log_alias_fallback_edges(dialect, fn, tmp_path):
    """Dialects outside the per-base allowlist fall back to the canonical
    sqlglot ``LOG(base, x)`` form -- preserved identically by legacy and
    the new pipeline. Uses the same Mode A model-level filter exerciser
    as ``test_log_alias_preservation_tier1``.
    """
    base = _orders()
    orders_with_log_filter = SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=list(base.columns),
        joins=list(base.joins),
        filters=[f"{fn}(amount) > 0"],
    )
    storage = await build_storage_with_models(
        tmp_path, _customers(), orders_with_log_filter,
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
    )
    bundle = ResolvedSourceBundle(
        source_model=orders_with_log_filter,
        referenced_models=[_customers()],
    )
    legacy = await legacy_sql_for(
        engine=engine, model=orders_with_log_filter, query=query, dialect=dialect,
    )
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect=dialect)
    assert_sql_equivalent(legacy, new)
    # Canonical fallback emitted -- LOG(...) call present in the SQL.
    assert "LOG(" in new.upper(), (
        f"expected canonical LOG(N, x) fallback on {dialect}/{fn}; got: {new!r}"
    )


# ---------------------------------------------------------------------------
# JSON-extract preservation on SQLite
# ---------------------------------------------------------------------------


async def test_json_extract_preservation_sqlite(tmp_path):
    """SQLite's ``->`` operator returns JSON-quoted strings that silently
    break equality matches; SLayer preserves the function-call
    ``json_extract(col, '$.path')`` form via
    ``rewrite_sqlite_json_extract``. Both paths must keep it intact.

    ``json_extract`` is dialect-specific and lives in Mode A only --
    NOT in the ``SCALAR_FUNCTIONS`` allowlist. The exerciser here uses
    a model-level ``filters`` entry (a Mode A SQL predicate that
    routes through ``parse_sql_predicate`` -> ``_parse`` ->
    ``rewrite_sqlite_json_extract``) so the rewriter applies inside
    the emitted WHERE clause.
    """
    base = _orders()
    orders_with_json_filter = SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=list(base.columns),
        joins=list(base.joins),
        filters=["json_extract(meta, '$.status') = 'active'"],
    )
    storage = await build_storage_with_models(
        tmp_path, _customers(), orders_with_json_filter,
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum"}],
    )
    bundle = ResolvedSourceBundle(
        source_model=orders_with_json_filter,
        referenced_models=[_customers()],
    )
    legacy = await legacy_sql_for(
        engine=engine,
        model=orders_with_json_filter,
        query=query,
        dialect="sqlite",
    )
    planned = plan_query(query=query, bundle=bundle)
    new = generate_from_planned(planned, bundle=bundle, dialect="sqlite")
    assert_sql_equivalent(legacy, new)
    # Function-call form preserved in WHERE; operator form NEVER emitted.
    assert "json_extract" in new.lower()
    assert " -> '" not in new, (
        f"sqlite json_extract was rewritten to '->' operator: {new!r}"
    )


# ---------------------------------------------------------------------------
# time_shift dialect branches (postgres INTERVAL vs sqlite DATETIME)
# ---------------------------------------------------------------------------


def _td_month() -> TimeDimension:
    return TimeDimension(
        dimension=ColumnRef(name="created_at"),
        granularity=TimeGranularity.MONTH,
    )


def test_time_shift_dialect_branches_postgres() -> None:
    """Postgres branch of ``_build_time_offset_expr`` (generator.py:~1019)
    emits an ``INTERVAL`` expression. ``change(amount:sum)`` desugars to
    ``amount:sum - time_shift(amount:sum, periods=-1)``, materialising a
    ``shifted_*`` CTE whose GROUP BY truncates ``created_at + INTERVAL
    '1 MONTH'``.

    Typed-only structural assertion: the existing 7b.11 ``change``
    parity gap (hidden-slot naming divergence ``_ts_delta`` vs
    ``_time_shift_inner``) prevents a direct parity comparison here.
    The dialect-branch contract (``INTERVAL`` keyword) is what this
    slice pins -- the legacy-vs-new alias divergence is tracked
    separately for DEV-1452 cleanup.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "change(amount:sum)", "name": "delta"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    upper = n.upper()
    # Postgres ``_build_time_offset_expr`` emits INTERVAL.
    assert "INTERVAL" in upper, (
        f"postgres time-shift branch did not emit INTERVAL; got: {n!r}"
    )
    assert (
        "+ INTERVAL '1 MONTH'" in upper
        or "+ INTERVAL 1 MONTH" in upper
        or "+ INTERVAL '1' MONTH" in upper
    ), (
        f"postgres month-offset shape unexpected; got: {n!r}"
    )


def test_time_shift_dialect_branches_sqlite() -> None:
    """SQLite branch of ``_build_time_offset_expr`` (generator.py:~1012)
    emits a ``DATETIME(col, '+N units')`` call instead of postgres-style
    ``INTERVAL`` (sqlite has no INTERVAL arithmetic). The shifted CTE's
    GROUP BY wraps the offset expression in ``STRFTIME``.

    Same typed-only rationale as
    ``test_time_shift_dialect_branches_postgres`` -- parity blocked by
    the 7b.11 hidden-slot naming gap.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "change(amount:sum)", "name": "delta"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="sqlite")
    n = norm_sql(sql)
    # SQLite branch wraps the column in DATE(col, 'N unit') for offset;
    # never INTERVAL (sqlite has no INTERVAL arithmetic).
    assert "DATE(orders.created_at," in n, (
        f"sqlite time-shift branch did not emit DATE(col, ...) offset; "
        f"got: {n!r}"
    )
    assert "INTERVAL" not in n.upper(), (
        f"sqlite time-shift unexpectedly emitted INTERVAL; got: {n!r}"
    )
    # The offset literal "1 months" appears in the DATE call
    # (``periods=-1`` -> shifted CTE adds +1 month per 7b.11
    # comment at ``generator.py:218-220``).
    assert "'1 months'" in n, (
        f"sqlite month-offset literal not in DATE call; got: {n!r}"
    )


# ---------------------------------------------------------------------------
# Synth-adapter direct unit tests (Codex MED #4 fold-in)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "formula,kwarg_name,kwarg_value",
    [
        ("amount:corr(other=quantity)", "other", "quantity"),
        ("amount:covar_samp(other=quantity)", "other", "quantity"),
        ("amount:covar_pop(other=quantity)", "other", "quantity"),
        ("amount:weighted_avg(weight=quantity)", "weight", "quantity"),
    ],
)
def test_synth_adapter_propagates_kwarg_columns(
    formula, kwarg_name, kwarg_value,
):
    """Plan a kwarg-bearing aggregation, call the synth adapter directly,
    and assert ``EnrichedMeasure.agg_kwargs`` carries the column kwarg in
    the SQL-string form ``_build_formula_agg`` / ``_build_stat_agg``
    expects. Pins HIGH #1 + HIGH #2 + HIGH #3 fold-ins structurally.
    """
    query = SlayerQuery(source_model="orders", measures=[{"formula": formula}])
    planned = plan_query(query=query, bundle=_bundle())
    agg_slot = planned.aggregate_slots[0]
    assert isinstance(agg_slot.key, AggregateKey)
    synth = SQLGenerator(dialect="postgres")._synthesize_enriched_measure_from_planned(
        slot=agg_slot,
        key=agg_slot.key,
        source_model=_orders(),
        source_relation="orders",
        full_alias=f"orders.{agg_slot.declared_name}",
    )
    assert synth.agg_kwargs == {kwarg_name: kwarg_value}, (
        f"synth adapter dropped or mis-stringified kwarg "
        f"{kwarg_name!r}: got {synth.agg_kwargs!r}"
    )


def test_synth_adapter_propagates_scalar_kwarg():
    """``percentile(p=0.5)`` -- a Decimal scalar must arrive in
    ``agg_kwargs`` as a string matching ``_SAFE_AGG_PARAM_RE``.
    """
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:percentile(p=0.5)"}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    agg_slot = planned.aggregate_slots[0]
    synth = SQLGenerator(dialect="postgres")._synthesize_enriched_measure_from_planned(
        slot=agg_slot,
        key=agg_slot.key,
        source_model=_orders(),
        source_relation="orders",
        full_alias=f"orders.{agg_slot.declared_name}",
    )
    # Must be a string (Dict[str, str] type discipline) and parseable
    # back to 0.5; tolerate either "0.5" or "0.50" representation.
    assert "p" in synth.agg_kwargs
    raw = synth.agg_kwargs["p"]
    assert isinstance(raw, str)
    assert Decimal(raw) == Decimal("0.5"), (
        f"percentile p kwarg lost precision: {raw!r}"
    )


def test_synth_adapter_rejects_path_mismatched_kwarg():
    """Hand-build an ``AggregateKey`` where the kwarg ``ColumnKey`` has a
    path that does not match the source path. The synth adapter must
    raise ``AggregationNotAllowedError`` -- silently coercing the kwarg
    would let a host-rooted column flow into an aggregate semantically
    rooted on a joined target.
    """
    # Local aggregate (source.path == ()), kwarg points at customers.region_id
    # (path == ("customers",)). Cross-model rerooting only fires when the
    # aggregate ITSELF is cross-model; on a local aggregate, this kwarg
    # path mismatch must surface immediately.
    bogus_key = AggregateKey(
        source=ColumnKey(path=(), leaf="amount"),
        agg="corr",
        args=(),
        kwargs=(("other", ColumnKey(path=("customers",), leaf="region_id")),),
        column_filter_key=None,
    )
    # Build a real slot from a real planning call, then swap the key.
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:corr(other=quantity)"}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    agg_slot = planned.aggregate_slots[0]
    bogus_slot = agg_slot.model_copy(update={"key": bogus_key})
    with pytest.raises(AggregationNotAllowedError) as exc:
        SQLGenerator(dialect="postgres")._synthesize_enriched_measure_from_planned(
            slot=bogus_slot,
            key=bogus_key,
            source_model=_orders(),
            source_relation="orders",
            full_alias="orders.bogus",
        )
    # The error must mention the kwarg name AND the offending path.
    msg = str(exc.value)
    assert "other" in msg
    assert "customers" in msg


# ---------------------------------------------------------------------------
# Canonical-alias helper structural pin (HIGH #3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "formula,positive_substrings",
    [
        # (formula, list of canonical-alias fragments that must appear)
        # Cross-model parametric aggs include the kwarg suffix in the
        # alias (new pipeline 7b.5 improvement over legacy's collision-
        # prone shape). The helper renders ColumnKey kwargs as bare
        # ``leaf`` (no Pydantic repr).
        (
            "customers.revenue:percentile(p=0.5)",
            ["revenue_percentile_p_0_5"],
        ),
        (
            "customers.revenue:corr(other=customers.region_id)",
            # The cross-model alias renderer at
            # ``cross_model_planner.py:_aggregate_alias`` runs BEFORE
            # cross-model rerooting -- it sees the original
            # ``ColumnKey(path=("customers",), leaf="region_id")``
            # and renders the kwarg as path-bearing
            # ``customers.region_id`` (helper produces dotted form).
            # ``agg_signature_suffix`` then sanitises the ``.`` to
            # ``_`` so the canonical contains
            # ``other_customers_region_id``. Distinct kwarg paths
            # (e.g. ``customers.region_id`` vs ``customers.name``)
            # therefore produce distinct CTE aliases (7b.5 contract).
            ["revenue_corr_other_customers_region_id"],
        ),
        (
            "customers.revenue:weighted_avg(weight=customers.quantity)",
            ["revenue_weighted_avg_weight_customers_quantity"],
        ),
    ],
)
def test_canonical_alias_uses_helper_not_str_repr(
    formula, positive_substrings,
):
    """A cross-model kwarg-bearing aggregate must produce a canonical
    alias that:

    1. Does NOT contain Pydantic-repr fragments like ``path=`` / ``leaf=``.
       Naive ``str(ColumnKey)`` would surface those; the helper must not.
    2. DOES contain the expected canonical fragments (positive check) --
       pins that the helper not only avoids junk but emits the right
       form. Without this, a regression where the helper accidentally
       returned ``""`` or the agg name only would pass the negative
       check but break alias resolution.

    Both call sites (``slayer/sql/generator.py:3753`` and
    ``slayer/engine/cross_model_planner.py:286``) route through
    ``agg_kwarg_canonical_str``; this test exercises both via the
    emitted SQL. Typed-only (no legacy parity): legacy drops the agg
    signature suffix from cross-model aliases at
    ``query_engine.py:2160``, so direct parity is not achievable.
    """
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": formula}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    # Negative: no Pydantic-repr leakage.
    assert "path=" not in new, (
        f"Pydantic repr leaked into canonical alias: {new!r}"
    )
    assert "leaf=" not in new, (
        f"Pydantic repr leaked into canonical alias: {new!r}"
    )
    # Positive: every expected canonical fragment appears in the SQL.
    for substring in positive_substrings:
        assert substring in new, (
            f"expected canonical alias fragment {substring!r} in SQL; "
            f"got: {new!r}"
        )


# ---------------------------------------------------------------------------
# Single regression: percentile p value interns across calls
# ---------------------------------------------------------------------------


def test_percentile_p_05_vs_095_intern_to_distinct_slots():
    """``percentile(p=0.5)`` and ``percentile(p=0.95)`` differ at the
    structural-key level (different scalar value); the planner must
    intern them as TWO ``AggregateKey`` slots, not one.
    """
    query = SlayerQuery(
        source_model="orders",
        measures=[
            {"formula": "amount:percentile(p=0.5)", "name": "p50"},
            {"formula": "amount:percentile(p=0.95)", "name": "p95"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    percentile_slots = [
        s for s in planned.aggregate_slots
        if isinstance(s.key, AggregateKey) and s.key.agg == "percentile"
    ]
    assert len(percentile_slots) == 2, (
        f"expected two distinct percentile slots; got {percentile_slots!r}"
    )
    # Different ``p`` Decimal values in the kwargs tuples.
    p_values = {
        dict(s.key.kwargs).get("p") for s in percentile_slots
    }
    assert p_values == {Decimal("0.5"), Decimal("0.95")}


# ---------------------------------------------------------------------------
# Order-by on parameterised aggregate (interaction with ProjectionPlanner)
# ---------------------------------------------------------------------------


async def test_order_by_parameterised_aggregate_parity(tmp_path):
    """``ORDER BY amount:percentile(p=0.5) DESC LIMIT 5`` must resolve
    against the projected percentile slot. Pins that the slot's
    canonical alias survives the ORDER BY round-trip through the
    new pipeline (and that the new path doesn't introduce a stray
    hidden slot the legacy path wouldn't).
    """
    storage = await build_storage_with_models(
        tmp_path, _customers(), _orders(),
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:percentile(p=0.5)"}],
        order=[OrderItem(column="amount:percentile(p=0.5)", direction="desc")],
        limit=5,
    )
    legacy = await legacy_sql_for(
        engine=engine, model=_orders(), query=query, dialect="postgres",
    )
    planned = plan_query(query=query, bundle=_bundle())
    new = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    assert_sql_equivalent(legacy, new)


# ---------------------------------------------------------------------------
# agg_kwarg_canonical_str -- direct unit tests (HIGH #1 fold-in)
# ---------------------------------------------------------------------------


def _import_helper():
    """Lazy import so the test file collects cleanly even when the helper
    is missing in the implementation (TDD-style: the unit tests below
    fail with the expected ImportError signal during the test-first
    phase, and turn green when ``slayer/core/refs.py`` gains the helper).
    """
    from slayer.core.refs import agg_kwarg_canonical_str  # noqa: PLC0415
    return agg_kwarg_canonical_str


def test_agg_kwarg_canonical_str_decimal():
    fn = _import_helper()
    assert fn(Decimal("0.5")) == "0.5"
    assert fn(Decimal("0.95")) == "0.95"
    assert fn(Decimal("100")) == "100"


def test_agg_kwarg_canonical_str_int_and_float():
    fn = _import_helper()
    assert fn(0) == "0"
    assert fn(100) == "100"
    assert fn(-3) == "-3"
    assert fn(0.5) == "0.5"


def test_agg_kwarg_canonical_str_str():
    fn = _import_helper()
    assert fn("quantity") == "quantity"
    assert fn("customers.region_id") == "customers.region_id"


def test_agg_kwarg_canonical_str_column_key_local():
    fn = _import_helper()
    assert fn(ColumnKey(path=(), leaf="quantity")) == "quantity"
    assert fn(ColumnKey(path=(), leaf="region_id")) == "region_id"


def test_agg_kwarg_canonical_str_column_key_joined():
    """Path-bearing ColumnKey -> dotted form. Only callers that
    legitimately need joined paths (the canonical-alias renderer
    pre-rerooting) reach this branch.
    """
    fn = _import_helper()
    assert fn(ColumnKey(path=("customers",), leaf="region_id")) == "customers.region_id"
    assert fn(ColumnKey(path=("customers", "regions"), leaf="name")) == "customers.regions.name"


def test_agg_kwarg_canonical_str_decimal_scientific_notation():
    """``Decimal("1E-7")``'s ``str()`` form is ``"1E-7"`` which does NOT
    match the generator's ``_SAFE_AGG_PARAM_RE`` (no scientific
    notation in the allowlist). The helper formats with ``:f`` to
    emit plain decimal notation that round-trips through the
    SQL-injection guard. Pins the Codex MEDIUM #2 fold-in.
    """
    fn = _import_helper()
    assert fn(Decimal("1E-7")) == "0.0000001"
    assert fn(Decimal("1.0E+3")) == "1000"
    # Plain-notation Decimals are unchanged.
    assert fn(Decimal("0.5")) == "0.5"
    assert fn(Decimal("100")) == "100"


@pytest.mark.parametrize("bad_value", [True, False, None])
def test_agg_kwarg_canonical_str_rejects_bool_and_none(bad_value):
    """``bool`` and ``None`` are valid Python scalars but never valid
    aggregation kwarg values in SQL. The helper raises ``TypeError`` to
    surface the misuse early -- legacy never accepted these (and
    ``AggregateKey``'s structural-key normalisation at
    ``slayer/core/keys.py:139-142`` keeps them distinct from numeric
    values precisely so they fail loud here, not silently elsewhere).
    """
    fn = _import_helper()
    with pytest.raises(TypeError):
        fn(bad_value)


# ---------------------------------------------------------------------------
# Scalar normalization spot check (MED #4 fold-in)
# ---------------------------------------------------------------------------


def test_planner_normalizes_int_to_decimal():
    """``percentile(p=1)`` (int) must produce ``Decimal(1)`` in the
    AggregateKey kwargs, per ``slayer/core/keys.py:99-100``. Pins that
    the binder's ``_bind_agg_arg`` path (binding.py:685) routes the
    literal through ``normalize_scalar``.
    """
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:percentile(p=1)"}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    agg_slot = planned.aggregate_slots[0]
    assert isinstance(agg_slot.key, AggregateKey)
    kwargs_dict = dict(agg_slot.key.kwargs)
    assert kwargs_dict.get("p") == Decimal(1), (
        f"int 1 not normalised to Decimal(1); got {kwargs_dict!r}"
    )


def test_planner_normalizes_float_via_string():
    """``percentile(p=0.1)`` (float) must produce ``Decimal("0.1")``,
    NOT ``Decimal(0.1)`` (binary float approximation -- 17 digits of
    junk). Pins the ``Decimal(str(value))`` recipe at
    ``slayer/core/keys.py:102``.
    """
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:percentile(p=0.1)"}],
    )
    planned = plan_query(query=query, bundle=_bundle())
    agg_slot = planned.aggregate_slots[0]
    kwargs_dict = dict(agg_slot.key.kwargs)
    p_val = kwargs_dict.get("p")
    assert p_val == Decimal("0.1"), (
        f"float 0.1 normalisation lost precision: {p_val!r}"
    )
    # Tighter check: the Decimal must round-trip through str without
    # surfacing binary-approximation digits.
    assert str(p_val) == "0.1", (
        f"expected str(p_val) == '0.1'; got {str(p_val)!r}"
    )


# ---------------------------------------------------------------------------
# SAFE_AGG_PARAM_RE compatibility (MED #1 fold-in)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "formula,kwarg_name",
    [
        ("amount:percentile(p=0.5)", "p"),
        ("amount:percentile(p=0.95)", "p"),
        ("amount:percentile(p=1)", "p"),
        ("amount:corr(other=quantity)", "other"),
        ("amount:weighted_avg(weight=quantity)", "weight"),
    ],
)
def test_synth_kwargs_match_safe_param_regex(formula, kwarg_name):
    """The stringified kwarg values flowing into ``EnrichedMeasure.agg_kwargs``
    MUST satisfy ``_SAFE_AGG_PARAM_RE`` (``slayer/sql/generator.py:131``);
    ``_validate_agg_param_value`` at ``:178`` raises ``ValueError`` on
    anything that doesn't match. Without this check, a future regression
    in the canonicalisation helper could produce strings that match the
    parity assertion but fail at SQL-injection guard time.
    """
    from slayer.sql.generator import _SAFE_AGG_PARAM_RE  # noqa: PLC0415

    query = SlayerQuery(source_model="orders", measures=[{"formula": formula}])
    planned = plan_query(query=query, bundle=_bundle())
    agg_slot = planned.aggregate_slots[0]
    synth = SQLGenerator(dialect="postgres")._synthesize_enriched_measure_from_planned(
        slot=agg_slot,
        key=agg_slot.key,
        source_model=_orders(),
        source_relation="orders",
        full_alias=f"orders.{agg_slot.declared_name}",
    )
    raw = synth.agg_kwargs[kwarg_name]
    assert _SAFE_AGG_PARAM_RE.match(raw), (
        f"kwarg {kwarg_name!r} value {raw!r} does not match "
        f"_SAFE_AGG_PARAM_RE -- _validate_agg_param_value will reject it."
    )


# ---------------------------------------------------------------------------
# Query-level path-mismatch DSL case (LOW #3 fold-in)
# ---------------------------------------------------------------------------


def test_local_agg_with_joined_kwarg_path_raises():
    """User-facing failure mode: writing
    ``amount:weighted_avg(weight=customers.quantity)`` on a local
    aggregate (``amount`` is on ``orders``, ``customers.quantity`` is
    via a join) must surface a typed error at planning/synth time.
    Local aggregates can only correlate columns on their own model;
    mixing a joined kwarg into a local aggregate is meaningless SQL.
    """
    query = SlayerQuery(
        source_model="orders",
        measures=[{
            "formula": "amount:weighted_avg(weight=customers.quantity)",
        }],
    )
    planned = plan_query(query=query, bundle=_bundle())
    with pytest.raises(AggregationNotAllowedError):
        generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
