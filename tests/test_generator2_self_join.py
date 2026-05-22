"""DEV-1450 stage 7b.11 — self-join CTE transform generator slice tests.

Covers ``time_shift`` (kwarg-only on the new pipeline), the
planner-desugared ``change`` / ``change_pct`` (which lowers to
``time_shift`` + arithmetic), and ``consecutive_periods`` (a pair of
staged window CTEs that compute a reset group then sum within it).

Each transform here is rendered by ``generate_from_planned`` as one or
more dedicated self-join / staged CTEs that sit between the base
``WITH base AS (...)`` and the public outer projection. The slice
invariants pinned by this file:

* ``time_shift`` emits a ``shifted_<...>`` CTE that re-aggregates the
  source table with the time-column expression offset by ``-periods``
  (so a backward shift renders ``+ INTERVAL``), and a ``sjoin_<...>``
  CTE that LEFT JOINs the base on the time-truncated key (DEV-1450 C6:
  plus every ``partition_keys`` column threaded through from the
  TransformKey).
* ``change`` / ``change_pct`` desugar at plan time to
  ``measure - time_shift(measure, periods=-1)``; the inner time_shift
  becomes a hidden slot and the outer ArithmeticKey renders against
  the sjoin CTE's measure alias (DEV-1446: one time_shift slot per
  distinct aggregate even if reused in filter or other transforms).
* ``consecutive_periods`` emits ``cp_reset_<...>`` + ``cp_value_<...>``
  CTEs. When the TransformKey input is a comparison expression
  (``amount:sum > 0``), the CASE WHEN predicate uses ``COALESCE(<expr>,
  FALSE)`` (boolean shape); when the input is a non-boolean expression
  (e.g. the bare aggregate ``amount:sum``), the CASE WHEN uses
  ``<expr> IS NOT NULL AND <expr> <> 0`` (numeric shape). A non-boolean
  *composite* input (e.g. ``amount:sum - qty:sum``) is rejected — the
  predicate-classification rule applies only to slottable leaf inputs
  and to top-level comparison ``ArithmeticKey`` nodes.
* **date_range invariant** (the 7b.3c contract): a ``BetweenKey`` ROW
  filter derived from ``TimeDimension.date_range`` applies to the OUTER
  projection only — the shifted / consecutive-periods inner CTEs read
  raw rows without it. This is how shifted edge periods retain valid
  shifted values when the user only requested a narrow date range.
* Other ROW-phase filters (e.g. ``status = 'active'``) DO flow into
  the inner CTEs so the shifted aggregation matches the base
  aggregation's row population.

Out of scope (later slices):
* Cross-model time_shift / change inputs — 7b.12.
* Exhaustive dialect parity for INTERVAL expressions — 7b.13.

The 7b.10 NotImplementedError pin for self-join transform ops is lifted
by this slice's implementation. The 7b.10 ``composite-input transforms``
pin remains for non-boolean composite inputs — comparison-shaped
``ArithmeticKey`` inputs to ``consecutive_periods`` are accepted as a
special case (the predicate-classification rule above).

Deleted alongside ``tests/parity_oracle.py`` at the end of 7b.15.
"""

from __future__ import annotations

import re
from typing import List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import (
    ColumnRef,
    SlayerQuery,
    TimeDimension,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import generate_from_planned
from tests.parity_oracle import norm_sql


# ---------------------------------------------------------------------------
# SQL-shape helpers (avoid brittle ``.split(" FROM ")`` patterns and
# CTE-name-vs-reference miscounts).
# ---------------------------------------------------------------------------


_CTE_DEF_RE = re.compile(r"(?:WITH |, )([A-Za-z_][A-Za-z0-9_]*) AS \(")


def _cte_names(n: str) -> List[str]:
    """Return CTE names defined in a normalised SQL string in order.

    Matches the ``WITH <name> AS (`` / ``, <name> AS (`` definition
    sites. Counts only definitions, NOT references to those names
    elsewhere (``LEFT JOIN <name>``, ``FROM <name>``).
    """
    return _CTE_DEF_RE.findall(n)


def _cte_body(n: str, name: str) -> str:
    """Return the body of the named CTE (between ``<name> AS (`` and
    its matching close paren). Handles nested parentheses.
    """
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


def _outermost_select(n: str) -> str:
    """Return the outermost SELECT clause (between ``SELECT`` and the
    first matched ``FROM (``).

    The new generator wraps the CTE chain as
    ``SELECT <public_aliases> FROM (WITH base AS (...) ... ) AS _outer``
    so the outermost SELECT is whatever precedes the first ``FROM (``
    in the normalised SQL. Splitting on plain ``" FROM "`` would
    incorrectly capture the first CTE's projection when no outer wrap
    exists (defensive against future generator changes).
    """
    idx = n.find("FROM (")
    if idx < 0:
        # No outer wrap — fall back to the whole SQL up to the first
        # FROM (still useful for sanity assertions in simple cases).
        return n.split(" FROM ", 1)[0]
    return n[:idx].rstrip()


# ---------------------------------------------------------------------------
# Model fixtures (mirror tests/test_generator2_window.py::_orders)
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
# time_shift — direct user form (kwarg-only on the new pipeline)
# ---------------------------------------------------------------------------


def test_time_shift_minus_one_month_emits_shifted_and_sjoin_ctes() -> None:
    """``time_shift(amount:sum, periods=-1)`` -- the shifted CTE
    re-aggregates with the time column offset (a backward shift renders
    ``+ INTERVAL`` so the GROUP BY buckets align), and a ``sjoin_`` CTE
    LEFT JOINs the base on the time-truncated key.

    Typed-only structural: the legacy formula parser does not accept
    kwarg form for ``time_shift``, so parity via the oracle is impossible.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "prev"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Base CTE present.
    assert "WITH base AS" in n
    # CTE chain (count definitions, not references): exactly one base,
    # one shifted_<name>, one sjoin_<name>.
    names = _cte_names(n)
    assert "base" in names
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    sjoin_defs = [c for c in names if c.startswith("sjoin_")]
    assert len(shifted_defs) == 1, f"expected one shifted_ CTE; got {names}"
    assert len(sjoin_defs) == 1, f"expected one sjoin_ CTE; got {names}"
    # Shifted CTE body offsets the time column by **+1 MONTH** (the
    # opposite sign of periods=-1, so its GROUP BY produces the prior
    # period's bucket and the equality join lines it up with base).
    shifted_body = _cte_body(n, shifted_defs[0])
    upper = shifted_body.upper()
    assert "INTERVAL" in upper
    # Sign is positive (added to created_at), one MONTH unit.
    assert "+ INTERVAL '1 MONTH'" in upper or "+ INTERVAL 1 MONTH" in upper or (
        # sqlglot may emit ``orders.created_at + INTERVAL '1' MONTH``
        # (single-quoted number, unquoted unit). Accept that form too.
        "+ INTERVAL '1' MONTH" in upper
    )
    # Sjoin CTE LEFT JOINs base + shifted and uses the TD alias as
    # the equality key on BOTH sides.
    sjoin_body = _cte_body(n, sjoin_defs[0])
    assert "LEFT JOIN" in sjoin_body
    assert f'base."orders.created_at" = {shifted_defs[0]}."orders.created_at"' in sjoin_body, (
        f"expected time-key equality join on TD alias; sjoin body: {sjoin_body!r}"
    )
    # Outer projection includes the user-supplied alias.
    assert '"orders.prev"' in _outermost_select(n)


def test_time_shift_plus_two_month_renders_subtract_interval() -> None:
    """A forward shift (``periods=+2``) renders ``- INTERVAL 2 MONTH``
    in the shifted CTE (the GROUP BY produces a bucket for the future
    period; joining on equality lines it up with the base).
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "time_shift(amount:sum, periods=2)", "name": "next2"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    assert len(shifted_defs) == 1
    shifted_body = _cte_body(n, shifted_defs[0]).upper()
    # Subtract two months — periods=+2 means a forward shift; the
    # shifted CTE aggregates `created_at - INTERVAL '2 MONTH'`.
    assert "INTERVAL" in shifted_body
    assert (
        "- INTERVAL '2 MONTH'" in shifted_body
        or "- INTERVAL 2 MONTH" in shifted_body
        or "- INTERVAL '2' MONTH" in shifted_body
    ), f"expected -INTERVAL 2 MONTH in shifted body; got: {shifted_body!r}"
    # Output projects the alias.
    assert '"orders.next2"' in _outermost_select(n)


def test_time_shift_quarter_granularity_uses_three_months_offset() -> None:
    """Quarter granularity: ``periods=-1`` -> shifted CTE offsets by 3
    months (mirrors legacy ``_build_time_offset_expr`` semantics where
    quarter is stored as ``MONTH * 3``)."""
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.QUARTER,
            ),
        ],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "prev_q"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    assert len(shifted_defs) == 1
    shifted_body = _cte_body(n, shifted_defs[0]).upper()
    # Quarter shift = 3 months at the INTERVAL level (legacy parity).
    assert (
        "+ INTERVAL '3 MONTH'" in shifted_body
        or "+ INTERVAL 3 MONTH" in shifted_body
        or "+ INTERVAL '3' MONTH" in shifted_body
    ), f"expected +INTERVAL 3 MONTH in shifted body; got: {shifted_body!r}"


# ---------------------------------------------------------------------------
# change / change_pct — planner-desugared time_shift + arithmetic
# ---------------------------------------------------------------------------


def test_change_emits_self_join_and_arithmetic_step() -> None:
    """``change(amount:sum)`` desugars to ``amount:sum - time_shift(
    amount:sum, periods=-1)``. The renderer materialises:

    * base CTE with ``SUM(orders.amount) AS "orders.amount_sum"``
    * a shifted CTE for the lowered time_shift
    * a sjoin CTE that LEFT JOINs base + shifted on the TD alias
    * a step CTE (or inline expression) computing the subtraction
    * outer projection with the user-supplied ``orders.delta`` alias
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
    # Exactly one SUM(orders.amount) IN THE BASE CTE (DEV-1446: shared
    # AggregateKey identity across the projected measure and the
    # transform input means a single base aggregation). The shifted
    # CTE re-aggregates separately, so a second occurrence outside
    # base is expected and correct.
    base_body = _cte_body(n, "base")
    assert base_body.count("SUM(orders.amount)") == 1
    # Self-join CTE chain (count definitions, not references).
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    sjoin_defs = [c for c in names if c.startswith("sjoin_")]
    assert len(shifted_defs) == 1, f"expected one shifted_ CTE; got {names}"
    assert len(sjoin_defs) == 1, f"expected one sjoin_ CTE; got {names}"
    assert "LEFT JOIN" in _cte_body(n, sjoin_defs[0])
    # Subtraction arithmetic appears (as either ``a - b`` or
    # ``"orders.amount_sum" - "orders.<hidden>"``).
    assert " - " in n
    # Public alias surfaces in the outer projection.
    assert '"orders.delta"' in _outermost_select(n)


def test_change_pct_emits_self_join_and_division() -> None:
    """``change_pct(amount:sum)`` desugars to
    ``(amount:sum - time_shift(amount:sum, periods=-1)) /
    time_shift(amount:sum, periods=-1)``. Renderer materialises one
    shifted/sjoin pair (time_shift slot identity preserved across the
    numerator and the denominator) and a final arithmetic step doing
    the division.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "change_pct(amount:sum)", "name": "delta_pct"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Exactly one SUM(orders.amount) in the BASE CTE.
    assert _cte_body(n, "base").count("SUM(orders.amount)") == 1
    # One shifted/sjoin CTE pair only — the numerator and denominator
    # share the same time_shift slot.
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    sjoin_defs = [c for c in names if c.startswith("sjoin_")]
    assert len(shifted_defs) == 1, f"expected one shifted_ CTE; got {names}"
    assert len(sjoin_defs) == 1, f"expected one sjoin_ CTE; got {names}"
    # Division operator appears in the arithmetic step.
    assert " / " in n
    # User alias surfaces.
    assert '"orders.delta_pct"' in _outermost_select(n)


def test_time_shift_auto_joins_on_query_dimensions() -> None:
    """Legacy invariant (``_generate_with_computed:1559``): the sjoin
    CTE joins on EVERY query dimension automatically, not just
    columns explicitly listed in ``partition_by``. Without this,
    ``time_shift(amount:sum, periods=-1)`` with ``status`` in
    ``dimensions`` would join row-by-row only on time — broadcasting
    the prior-period total across all status values. The shifted
    CTE must group by status; the sjoin must equality-join on it too.
    """
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "prev"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    sjoin_defs = [c for c in names if c.startswith("sjoin_")]
    assert len(shifted_defs) == 1
    assert len(sjoin_defs) == 1
    # Shifted CTE must group by status (and project it).
    shifted_body = _cte_body(n, shifted_defs[0])
    assert 'orders.status AS "orders.status"' in shifted_body, (
        f"shifted CTE missing status projection; got: {shifted_body!r}"
    )
    assert "GROUP BY" in shifted_body
    gb = shifted_body.split("GROUP BY", 1)[1]
    assert "orders.status" in gb
    # Sjoin CTE must equality-join on status as well as time.
    sjoin_body = _cte_body(n, sjoin_defs[0])
    assert f'base."orders.status" = {shifted_defs[0]}."orders.status"' in sjoin_body, (
        f"sjoin missing status equality; got: {sjoin_body!r}"
    )


def test_change_with_partition_by_threads_to_shifted_cte() -> None:
    """DEV-1450 C6: ``change(measure, partition_by=region)`` threads
    ``partition_by`` to the desugared time_shift. For self-join
    transforms, the partition columns become additional JOIN keys on
    the sjoin CTE so the shifted aggregation aligns per-partition.

    Even when ``region`` is NOT in the query's dimensions, the shifted
    CTE must group by ``region`` so the LEFT JOIN can match on it.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {
                "formula": "change(amount:sum, partition_by=region)",
                "name": "delta",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    sjoin_defs = [c for c in names if c.startswith("sjoin_")]
    assert len(shifted_defs) == 1
    assert len(sjoin_defs) == 1
    # The shifted CTE must SELECT region and GROUP BY region so the
    # equality join on region is well-defined.
    shifted_body = _cte_body(n, shifted_defs[0])
    assert 'orders.region AS "orders.region"' in shifted_body, (
        f"shifted CTE missing region projection; got: {shifted_body!r}"
    )
    assert "GROUP BY" in shifted_body
    group_by = shifted_body.split("GROUP BY", 1)[1]
    assert "orders.region" in group_by, (
        f"GROUP BY missing region; got: {group_by!r}"
    )
    # The sjoin CTE's ON clause references region equality on both sides.
    sjoin_body = _cte_body(n, sjoin_defs[0])
    assert f'base."orders.region" = {shifted_defs[0]}."orders.region"' in sjoin_body, (
        f"sjoin ON clause missing region equality; got: {sjoin_body!r}"
    )


# ---------------------------------------------------------------------------
# consecutive_periods — reset + value staged CTEs
# ---------------------------------------------------------------------------


def test_consecutive_periods_with_boolean_predicate_uses_coalesce() -> None:
    """``consecutive_periods(amount:sum > 0)`` -- the TransformKey
    input is an ``ArithmeticKey`` with a comparison op (boolean
    semantics). The reset/value CTEs use ``COALESCE(<predicate>, FALSE)``
    as the CASE WHEN test (Postgres rejects non-boolean WHEN; the
    boolean-form keeps the SQL portable).

    Pins that the renderer detects boolean-shaped TransformKey inputs
    and uses the COALESCE form instead of the numeric ``IS NOT NULL AND
    <> 0`` form.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {
                "formula": "consecutive_periods(amount:sum > 0)",
                "name": "streak",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Two CP CTEs: reset + value.
    assert " cp_reset" in n.lower() or "_cp_reset_" in n.lower()
    assert " cp_value" in n.lower()
    # Boolean form via COALESCE(..., FALSE).
    upper = n.upper()
    assert "COALESCE(" in upper
    assert ", FALSE)" in upper or ",FALSE)" in upper
    # Two window-function layers (SUM(CASE WHEN ...) OVER (...)).
    # First is the reset (assigns a group id), second is the value
    # (counts within the group).
    assert "SUM(CASE WHEN" in upper
    # User alias surfaces.
    assert '"orders.streak"' in n


def test_consecutive_periods_with_numeric_predicate_uses_is_not_null_form() -> None:
    """``consecutive_periods(amount:sum)`` (no comparison) -- input is
    the AggregateKey itself. The CASE WHEN predicate uses ``<measure>
    IS NOT NULL AND <measure> <> 0`` (numeric truthiness).
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {
                "formula": "consecutive_periods(amount:sum)",
                "name": "streak",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    upper = norm_sql(sql).upper()
    # Numeric form: IS NOT NULL AND <> 0.
    assert "IS NOT NULL" in upper
    assert "<> 0" in upper
    # No COALESCE FALSE branch.
    assert "COALESCE(" not in upper or ", FALSE)" not in upper and ",FALSE)" not in upper


def test_consecutive_periods_inner_aggregate_materialised_in_base() -> None:
    """When the user doesn't project the inner ``amount:sum`` (only
    ``consecutive_periods`` is declared), the AggregateKey is still
    materialised in the base CTE as a hidden slot so the cp_reset CTE
    can reference it. Pins that the planner's hidden-slot pass reaches
    consecutive_periods inputs and that the renderer collects them
    into the base CTE projection.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {
                "formula": "consecutive_periods(amount:sum > 0)",
                "name": "streak",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # SUM(orders.amount) appears in the base CTE even though
    # ``amount:sum`` is not in the public projection.
    base_body = _cte_body(n, "base")
    assert "SUM(orders.amount)" in base_body
    # The outermost SELECT projects ONLY streak (and any TD), not
    # the hidden amount_sum slot.
    outermost = _outermost_select(n)
    assert '"orders.streak"' in outermost
    assert '"orders.amount_sum"' not in outermost


def test_consecutive_periods_auto_partitions_by_query_dimensions() -> None:
    """Legacy: ``partition_aliases = [d.alias for d in dimensions]`` —
    the reset / value window CTEs auto-partition the streak by every
    query dimension so streaks are computed per-group, not globally.

    Pins that a renderer that ignores query dimensions (computing one
    global streak) does not pass.
    """
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {
                "formula": "consecutive_periods(amount:sum > 0)",
                "name": "streak",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Find the cp_reset CTE; its window OVER clause must PARTITION BY
    # the dimension alias.
    names = _cte_names(n)
    reset_defs = [c for c in names if "cp_reset" in c]
    assert reset_defs, f"expected a cp_reset_ CTE; got {names}"
    reset_body = _cte_body(n, reset_defs[0])
    # PARTITION BY mentions the status dimension alias.
    assert 'PARTITION BY "orders.status"' in reset_body, (
        f"cp_reset CTE missing PARTITION BY status; got: {reset_body!r}"
    )


# ---------------------------------------------------------------------------
# date_range invariant (7b.3c) — inner CTE reads raw data
# ---------------------------------------------------------------------------


def test_time_shift_with_date_range_inner_cte_omits_date_filter() -> None:
    """7b.3c invariant: ``TimeDimension.date_range`` becomes a
    ``BetweenKey`` row-phase filter. For self-join transforms the inner
    shifted CTE reads raw data (no date_range filter applied) so the
    earliest visible bucket can have a non-NULL shifted value
    (otherwise the shift would always be NULL at the left edge of the
    requested range).
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-12-31"],
            ),
        ],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "prev"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # base CTE has the date_range BETWEEN; shifted CTE does not.
    assert "BETWEEN" in _cte_body(n, "base").upper()
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    assert len(shifted_defs) == 1
    shifted_body = _cte_body(n, shifted_defs[0])
    assert "BETWEEN" not in shifted_body.upper(), (
        f"shifted CTE must not contain BETWEEN (date_range filter); got: {shifted_body!r}"
    )


def test_change_with_date_range_inner_cte_omits_date_filter() -> None:
    """Same invariant for the planner-desugared change form: inner
    shifted CTE reads raw rows so the earliest visible period in
    ``date_range`` still has a valid ``change`` (otherwise the shift
    would always be NULL at the start of the requested range).
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-12-31"],
            ),
        ],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "change(amount:sum)", "name": "delta"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    assert "BETWEEN" in _cte_body(n, "base").upper()
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    assert len(shifted_defs) == 1
    assert "BETWEEN" not in _cte_body(n, shifted_defs[0]).upper()


def test_consecutive_periods_with_date_range_does_not_re_apply_date_filter() -> None:
    """7b.3c invariant for consecutive_periods. The cp_reset / cp_value
    streak CTEs read from the previous CTE chain (base) and MUST NOT
    re-apply the ``date_range`` filter as their own WHERE — otherwise
    the streak would be doubly filtered. They inherit the row
    population from their FROM clause (which is the base CTE).

    The textual assertion is that no ``WHERE … BETWEEN`` clause
    appears inside the cp_reset / cp_value CTEs. The OVER clause's
    window frame (``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW``)
    is a distinct construct and IS expected in the SQL — the test
    pinpoints the WHERE form via ``WHERE ... BETWEEN`` substring.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-12-31"],
            ),
        ],
        measures=[
            {"formula": "amount:sum"},
            {
                "formula": "consecutive_periods(amount:sum > 0)",
                "name": "streak",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # base CTE has the date_range BETWEEN.
    assert "BETWEEN" in _cte_body(n, "base").upper()
    names = _cte_names(n)
    reset_defs = [c for c in names if "cp_reset" in c]
    value_defs = [c for c in names if "cp_value" in c]
    assert reset_defs and value_defs, f"expected cp_reset / cp_value CTEs; got {names}"
    # The streak CTEs must NOT have their own WHERE filter (no
    # ``date_range`` re-application). They inherit population from
    # base via FROM.
    for cte_name in [reset_defs[0], value_defs[0]]:
        body = _cte_body(n, cte_name).upper()
        # No standalone WHERE clause in the streak CTEs (only window
        # frames; ``ROWS BETWEEN ... AND ...`` does not constitute a
        # WHERE filter).
        assert " WHERE " not in body, (
            f"{cte_name} should not have its own WHERE clause "
            f"(date_range must apply only at base or outer); got: "
            f"{body!r}"
        )


def test_non_date_range_row_filter_flows_through_to_inner_cte() -> None:
    """A non-date_range row-phase filter (e.g. ``status = 'active'``)
    must apply to BOTH the base CTE and the shifted CTE so the shifted
    aggregation runs over the same row population. Only the BetweenKey
    date_range filter is excluded from the inner CTE — other ROW
    filters propagate.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "prev"},
        ],
        filters=["status == 'active'"],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    assert len(shifted_defs) == 1
    shifted_body = _cte_body(n, shifted_defs[0])
    assert "status" in shifted_body and "active" in shifted_body, (
        f"status='active' row filter must flow to shifted CTE; got: {shifted_body!r}"
    )


# ---------------------------------------------------------------------------
# DEV-1450 C13 — duplicate public aliases on one shared self-join slot
# ---------------------------------------------------------------------------


def test_dev1450_c13_two_declared_time_shift_aliases_share_one_slot() -> None:
    """Two measures with the same TransformKey structural identity but
    different ``name``s intern to ONE slot internally — but the
    projection must surface BOTH aliases (DEV-1450 C13).

    Legacy rejects this scenario; the typed pipeline accepts it. The
    sjoin CTE projects the shifted measure under BOTH user aliases so
    the outer SELECT can carry both.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "a"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "b"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Only ONE shifted + ONE sjoin CTE (shared slot identity).
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    sjoin_defs = [c for c in names if c.startswith("sjoin_")]
    assert len(shifted_defs) == 1, f"expected one shifted_ CTE; got {names}"
    assert len(sjoin_defs) == 1, f"expected one sjoin_ CTE; got {names}"
    # Both aliases surface in the outermost SELECT.
    outermost = _outermost_select(n)
    assert '"orders.a"' in outermost
    assert '"orders.b"' in outermost


# ---------------------------------------------------------------------------
# DEV-1446 — one slot per distinct aggregate even when reused in filter
# ---------------------------------------------------------------------------


def test_dev1446_change_in_filter_shares_one_time_shift_slot() -> None:
    """DEV-1446 acceptance: the renamed measure
    ``{"formula": "amount:sum", "name": "revenue"}`` plus a filter
    ``["change(amount:sum) > 0"]`` interns exactly ONE AggregateKey
    slot (the renamed revenue), exactly ONE time_shift slot (the
    filter's inner change desugar), and the emitted SQL has ONE
    ``SUM(orders.amount)`` in the base CTE.

    The structural-key contract (P2) means ``amount:sum`` inside the
    filter's ``change`` shares identity with the projected
    ``revenue``; the renderer must not duplicate the base aggregation.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[{"formula": "amount:sum", "name": "revenue"}],
        filters=["change(amount:sum) > 0"],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Exactly one SUM(orders.amount) in the BASE CTE (DEV-1446: shared
    # AggregateKey identity ensures one base aggregation).
    assert _cte_body(n, "base").count("SUM(orders.amount)") == 1
    # Exactly one shifted CTE and one sjoin CTE (count definitions, not
    # references — the JOIN reference would double-count).
    names = _cte_names(n)
    shifted_defs = [c for c in names if c.startswith("shifted_")]
    sjoin_defs = [c for c in names if c.startswith("sjoin_")]
    assert len(shifted_defs) == 1, f"expected one shifted_ CTE; got {names}"
    assert len(sjoin_defs) == 1, f"expected one sjoin_ CTE; got {names}"
    # POST-filter wrap applies the ``change > 0`` predicate AFTER the
    # CTE chain (filter references a transform-phase slot).
    assert " _filtered" in n
    # Outermost SELECT has revenue (and the TD). It does NOT include
    # the hidden change slot or the hidden amount_sum slot.
    outermost = _outermost_select(n)
    assert '"orders.revenue"' in outermost
    # The hidden time_shift slot's canonical alias must NOT surface in
    # the outer SELECT.
    assert "_time_shift_inner" not in outermost


# ---------------------------------------------------------------------------
# Composite-input rejection (carry-over from 7b.10)
# ---------------------------------------------------------------------------


def test_time_shift_with_composite_input_raises() -> None:
    """``time_shift(amount:sum / qty:sum, periods=-1)`` -- input is an
    ArithmeticKey, not a slottable leaf. The shifted CTE needs an
    inner expression layer to materialise the ratio before shifting,
    which is out of 7b.11 scope. Reject explicitly to prevent silent
    wrong SQL.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "qty:sum"},
            {
                "formula": "time_shift(amount:sum / qty:sum, periods=-1)",
                "name": "shifted_ratio",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    with pytest.raises(
        NotImplementedError,
        match=r"composite-input transforms",
    ):
        generate_from_planned(planned, bundle=_bundle(), dialect="postgres")


def test_consecutive_periods_with_composite_numeric_input_raises() -> None:
    """``consecutive_periods(amount:sum - qty:sum)`` -- the input is an
    ArithmeticKey that's NOT a comparison (numeric subtraction). The
    CASE WHEN predicate can't determine truthiness without an inner
    expression layer; reject explicitly.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "qty:sum"},
            {
                "formula": "consecutive_periods(amount:sum - qty:sum)",
                "name": "streak",
            },
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    with pytest.raises(
        NotImplementedError,
        match=r"composite-input transforms",
    ):
        generate_from_planned(planned, bundle=_bundle(), dialect="postgres")


# ---------------------------------------------------------------------------
# time_shift requires a time dimension
# ---------------------------------------------------------------------------


def test_time_shift_without_time_dimension_raises() -> None:
    """Mirror the legacy 'requires an unambiguous time dimension' error
    for time_shift — the planner's ``_attach_time_keys`` patches the
    time_key, and the post-patch check raises when no resolvable TD
    exists. 7b.10 already pins this for cumsum/lag/lead; pinned here
    for self-join transforms to confirm the same invariant.
    """
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "prev"},
        ],
    )
    with pytest.raises(
        ValueError, match=r"requires an unambiguous time dimension",
    ):
        plan_query(query=query, bundle=_bundle())


# ---------------------------------------------------------------------------
# Mixed window + self-join transform composition
# ---------------------------------------------------------------------------


def test_window_transform_plus_time_shift_compose_in_one_query() -> None:
    """``cumsum(amount:sum)`` + ``time_shift(amount:sum, periods=-1)``
    in the same query. Both transforms reference the same base
    aggregate slot. Renderer must emit:
    * base CTE with SUM(orders.amount) once
    * window-transform step CTE for cumsum
    * shifted + sjoin CTEs for time_shift
    * outer projection lists both aliases
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "cumsum(amount:sum)", "name": "running"},
            {"formula": "time_shift(amount:sum, periods=-1)", "name": "prev"},
        ],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # Exactly one SUM(orders.amount) in the BASE CTE.
    assert _cte_body(n, "base").count("SUM(orders.amount)") == 1
    # Both transform outputs surface.
    assert '"orders.running"' in n
    assert '"orders.prev"' in n
    # Self-join CTE present.
    names = _cte_names(n)
    assert any(c.startswith("shifted_") for c in names)
    assert any(c.startswith("sjoin_") for c in names)
    # Window OVER for cumsum present somewhere in the chain.
    assert "OVER" in n.upper()


# ---------------------------------------------------------------------------
# Sanity — POST-phase filter referencing change result
# ---------------------------------------------------------------------------


def test_filter_on_change_result_renders_post_filter_wrap() -> None:
    """``change(amount:sum)`` declared as a measure (``name="delta"``)
    plus a filter referencing the same change expression. The filter
    classifies as POST-phase; renderer emits a ``_filtered`` wrap that
    applies the predicate against the outer projection.

    Filter uses the colon-form (``change(amount:sum)``) rather than the
    user alias (``delta``) — alias-in-filter resolution is a 7b.15
    item (DEV-1443/1446 cross-cutting acceptance); using the colon
    form here keeps this slice independent of that fix.
    """
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[_td_month()],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "change(amount:sum)", "name": "delta"},
        ],
        filters=["change(amount:sum) > 0"],
    )
    planned = plan_query(query=query, bundle=_bundle())
    sql = generate_from_planned(planned, bundle=_bundle(), dialect="postgres")
    n = norm_sql(sql)
    # POST filter wrap.
    assert " _filtered" in n
    # The change measure surfaces under its user-supplied alias.
    assert '"orders.delta"' in n
