"""Shared fixtures for DEV-1837 — computed dimension × transform-measure
coexistence (stage 1a).

Reuses the DEV-1739/1824 models, dataset, and execution harness verbatim
(``tests/_dev1824_fixtures.py``) and adds the hand-computed oracles for the
dimension-family × measure-family matrix.

Dimension families (every family leads with ``region``)
-------------------------------------------------------
D-col   ``region``                                          (plain column)
D-expr  ``region, lower(city) AS lc``                       (scalar expression)
D-band  ``region, BAND35 AS band``                          (banded row attach)
D-bare  ``region, amount:sum(partition_by=city) AS ct``     (bare row attach)
D-rank  ``region, rank(amount:sum(partition_by=region)) AS rr``  (transform root)

Group grains (m = ``amount:sum``, from the DEV-1739 rows)
---------------------------------------------------------
band per row (city total > 35): rows 1,2 (CityA 30)→0; row 3 (CityB 40)→1;
row 4 (city NULL 30)→0; rows 5,6 (CityC 50)→1; row 7 (CityD 60)→1.
ct per row = the same city totals (30/40/30/50/60); rr = region-total rank
(North 100→1, NULL 60→2, South 50→3).

Monthly series per group (the transform ordering axis):
  D-col   N: Jan 30, Feb 70 · S: Jan 25, Mar 25 · NULL: Mar 60
  D-expr  (N,citya) Jan 30 · (N,cityb) Feb 40 · (N,NULL) Feb 30 ·
          (S,cityc) Jan 25, Mar 25 · (NULL,cityd) Mar 60
  D-band  (N,0) Jan 30, Feb 30 · (N,1) Feb 40 · (S,1) Jan 25, Mar 25 ·
          (NULL,1) Mar 60
  D-bare  = D-band with keys 30/40/50/60 for 0/40-ct/1-ct mapping
  D-rank  = D-col with rr appended (rr is region-functional)

Transform oracles (Option A grain: every projected dimension, minus time
buckets and combined placeholders; ordering axis = month):
* ``time_shift(…, -1)`` is CALENDAR-shifted: a Jan→Mar gap yields NULL for Mar.
* ``lag``/``lead`` step over EXISTING buckets: (S,·) Mar sees Jan's 25.
* ``change`` / ``change_pct`` inherit time_shift's calendar semantics.
* ``cumsum`` accumulates within the full dimension grain.
* ``consecutive_periods(amount:sum > 28)`` counts existing buckets, resetting
  on a failed predicate (25 fails, 30/40/60/70 pass).
* ``rank(amount:sum)`` (measure context) ranks the whole result DESC, RANK()
  ties; no time dimension in those cells.

The fixed M-part × temporal-transform shape (``partitioned-aggregates`` delta):
dims [region] + month, ``rt = amount:sum(partition_by=region)`` attaches the
region totals (N 100 / S 50 / NULL 60) to every month row.
"""

from __future__ import annotations

from tests._dev1824_fixtures import (  # noqa: F401 — re-exported fixture surface
    BAND35,
    BAND35_OF,
    CITY_TOTAL,
    ColumnRef,
    GRAND_TOTAL,
    ModelMeasure,
    REGION_LAST,
    REGION_TOTAL,
    SlayerQuery,
    TRAILING_90D_REGION,
    TimeDimension,
    TimeGranularity,
    dev1824_models,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)

dev1837_models = dev1824_models

RANK_DIM = "rank(amount:sum(partition_by=region))"
BARE_DIM = "amount:sum(partition_by=city)"
CP_PRED = "consecutive_periods(amount:sum > 28)"

#: Dimension-family query dimensions, in projection order.
DIM_FAMILY_DIMS = {
    "col": ["region"],
    "expr": ["region", {"expression": "lower(city)", "name": "lc"}],
    "band": ["region", {"expression": BAND35, "name": "band"}],
    "bare": ["region", {"expression": BARE_DIM, "name": "ct"}],
    "rank": ["region", {"expression": RANK_DIM, "name": "rr"}],
}

#: Transform-measure formulas, keyed by op (the M-transform family).
TRANSFORM_FORMULAS = {
    "time_shift": "time_shift(amount:sum, -1)",
    "lag": "lag(amount:sum)",
    "lead": "lead(amount:sum)",
    "change": "change(amount:sum)",
    "change_pct": "change_pct(amount:sum)",
    "cumsum": "cumsum(amount:sum)",
    "consecutive_periods": CP_PRED,
    "rank": "rank(amount:sum)",
}
#: Transform ops that need the month time dimension (rank is timeless).
TD_TRANSFORM_OPS = frozenset(TRANSFORM_FORMULAS) - {"rank"}

#: ``amount:sum`` per group, WITHOUT the time dimension.
GROUP_M = {
    "col": {("North",): 100.0, ("South",): 50.0, (None,): 60.0},
    "expr": {
        ("North", "citya"): 30.0, ("North", "cityb"): 40.0, ("North", None): 30.0,
        ("South", "cityc"): 50.0, (None, "cityd"): 60.0,
    },
    "band": {("North", 0): 60.0, ("North", 1): 40.0, ("South", 1): 50.0, (None, 1): 60.0},
    "bare": {
        ("North", 30.0): 60.0, ("North", 40.0): 40.0,
        ("South", 50.0): 50.0, (None, 60.0): 60.0,
    },
    "rank": {("North", 1): 100.0, ("South", 3): 50.0, (None, 2): 60.0},
}

#: ``amount:sum`` per group WITH the month time dimension appended to the key.
GROUP_M_MONTH = {
    "col": {
        ("North", "2024-01"): 30.0, ("North", "2024-02"): 70.0,
        ("South", "2024-01"): 25.0, ("South", "2024-03"): 25.0,
        (None, "2024-03"): 60.0,
    },
    "expr": {
        ("North", "citya", "2024-01"): 30.0, ("North", "cityb", "2024-02"): 40.0,
        ("North", None, "2024-02"): 30.0, ("South", "cityc", "2024-01"): 25.0,
        ("South", "cityc", "2024-03"): 25.0, (None, "cityd", "2024-03"): 60.0,
    },
    "band": {
        ("North", 0, "2024-01"): 30.0, ("North", 0, "2024-02"): 30.0,
        ("North", 1, "2024-02"): 40.0, ("South", 1, "2024-01"): 25.0,
        ("South", 1, "2024-03"): 25.0, (None, 1, "2024-03"): 60.0,
    },
    "bare": {
        ("North", 30.0, "2024-01"): 30.0, ("North", 30.0, "2024-02"): 30.0,
        ("North", 40.0, "2024-02"): 40.0, ("South", 50.0, "2024-01"): 25.0,
        ("South", 50.0, "2024-03"): 25.0, (None, 60.0, "2024-03"): 60.0,
    },
    "rank": {
        ("North", 1, "2024-01"): 30.0, ("North", 1, "2024-02"): 70.0,
        ("South", 3, "2024-01"): 25.0, ("South", 3, "2024-03"): 25.0,
        (None, 2, "2024-03"): 60.0,
    },
}

_THIRD = 4.0 / 3.0

#: Non-NULL transform values per (family, op); every group key absent here is
#: expected NULL. Keys match GROUP_M_MONTH (GROUP_M for the timeless ``rank``).
TRANSFORM_X = {
    ("col", "time_shift"): {("North", "2024-02"): 30.0},
    ("col", "lag"): {("North", "2024-02"): 30.0, ("South", "2024-03"): 25.0},
    ("col", "lead"): {("North", "2024-01"): 70.0, ("South", "2024-01"): 25.0},
    ("col", "change"): {("North", "2024-02"): 40.0},
    ("col", "change_pct"): {("North", "2024-02"): _THIRD},
    ("col", "cumsum"): {
        ("North", "2024-01"): 30.0, ("North", "2024-02"): 100.0,
        ("South", "2024-01"): 25.0, ("South", "2024-03"): 50.0,
        (None, "2024-03"): 60.0,
    },
    ("col", "consecutive_periods"): {
        ("North", "2024-01"): 1, ("North", "2024-02"): 2,
        ("South", "2024-01"): 0, ("South", "2024-03"): 0, (None, "2024-03"): 1,
    },
    ("col", "rank"): {("North",): 1, (None,): 2, ("South",): 3},

    ("expr", "time_shift"): {},
    ("expr", "lag"): {("South", "cityc", "2024-03"): 25.0},
    ("expr", "lead"): {("South", "cityc", "2024-01"): 25.0},
    ("expr", "change"): {},
    ("expr", "change_pct"): {},
    ("expr", "cumsum"): {
        ("North", "citya", "2024-01"): 30.0, ("North", "cityb", "2024-02"): 40.0,
        ("North", None, "2024-02"): 30.0, ("South", "cityc", "2024-01"): 25.0,
        ("South", "cityc", "2024-03"): 50.0, (None, "cityd", "2024-03"): 60.0,
    },
    ("expr", "consecutive_periods"): {
        ("North", "citya", "2024-01"): 1, ("North", "cityb", "2024-02"): 1,
        ("North", None, "2024-02"): 1, ("South", "cityc", "2024-01"): 0,
        ("South", "cityc", "2024-03"): 0, (None, "cityd", "2024-03"): 1,
    },
    ("expr", "rank"): {
        ("North", "citya"): 4, ("North", "cityb"): 3, ("North", None): 4,
        ("South", "cityc"): 2, (None, "cityd"): 1,
    },

    ("band", "time_shift"): {("North", 0, "2024-02"): 30.0},
    ("band", "lag"): {("North", 0, "2024-02"): 30.0, ("South", 1, "2024-03"): 25.0},
    ("band", "lead"): {("North", 0, "2024-01"): 30.0, ("South", 1, "2024-01"): 25.0},
    ("band", "change"): {("North", 0, "2024-02"): 0.0},
    ("band", "change_pct"): {("North", 0, "2024-02"): 0.0},
    ("band", "cumsum"): {
        ("North", 0, "2024-01"): 30.0, ("North", 0, "2024-02"): 60.0,
        ("North", 1, "2024-02"): 40.0, ("South", 1, "2024-01"): 25.0,
        ("South", 1, "2024-03"): 50.0, (None, 1, "2024-03"): 60.0,
    },
    ("band", "consecutive_periods"): {
        ("North", 0, "2024-01"): 1, ("North", 0, "2024-02"): 2,
        ("North", 1, "2024-02"): 1, ("South", 1, "2024-01"): 0,
        ("South", 1, "2024-03"): 0, (None, 1, "2024-03"): 1,
    },
    ("band", "rank"): {("North", 0): 1, (None, 1): 1, ("South", 1): 3, ("North", 1): 4},

    ("bare", "time_shift"): {("North", 30.0, "2024-02"): 30.0},
    ("bare", "lag"): {("North", 30.0, "2024-02"): 30.0, ("South", 50.0, "2024-03"): 25.0},
    ("bare", "lead"): {("North", 30.0, "2024-01"): 30.0, ("South", 50.0, "2024-01"): 25.0},
    ("bare", "change"): {("North", 30.0, "2024-02"): 0.0},
    ("bare", "change_pct"): {("North", 30.0, "2024-02"): 0.0},
    ("bare", "cumsum"): {
        ("North", 30.0, "2024-01"): 30.0, ("North", 30.0, "2024-02"): 60.0,
        ("North", 40.0, "2024-02"): 40.0, ("South", 50.0, "2024-01"): 25.0,
        ("South", 50.0, "2024-03"): 50.0, (None, 60.0, "2024-03"): 60.0,
    },
    ("bare", "consecutive_periods"): {
        ("North", 30.0, "2024-01"): 1, ("North", 30.0, "2024-02"): 2,
        ("North", 40.0, "2024-02"): 1, ("South", 50.0, "2024-01"): 0,
        ("South", 50.0, "2024-03"): 0, (None, 60.0, "2024-03"): 1,
    },
    ("bare", "rank"): {
        ("North", 30.0): 1, (None, 60.0): 1, ("South", 50.0): 3, ("North", 40.0): 4,
    },

    ("rank", "time_shift"): {("North", 1, "2024-02"): 30.0},
    ("rank", "lag"): {("North", 1, "2024-02"): 30.0, ("South", 3, "2024-03"): 25.0},
    ("rank", "lead"): {("North", 1, "2024-01"): 70.0, ("South", 3, "2024-01"): 25.0},
    ("rank", "change"): {("North", 1, "2024-02"): 40.0},
    ("rank", "change_pct"): {("North", 1, "2024-02"): _THIRD},
    ("rank", "cumsum"): {
        ("North", 1, "2024-01"): 30.0, ("North", 1, "2024-02"): 100.0,
        ("South", 3, "2024-01"): 25.0, ("South", 3, "2024-03"): 50.0,
        (None, 2, "2024-03"): 60.0,
    },
    ("rank", "consecutive_periods"): {
        ("North", 1, "2024-01"): 1, ("North", 1, "2024-02"): 2,
        ("South", 3, "2024-01"): 0, ("South", 3, "2024-03"): 0,
        (None, 2, "2024-03"): 1,
    },
    ("rank", "rank"): {("North", 1): 1, (None, 2): 2, ("South", 3): 3},
}

#: Bare ``amount:sum(window='1y')`` at the D-col grain — running region totals.
COL_WM = {
    ("North", "2024-01"): 30.0, ("North", "2024-02"): 100.0,
    ("South", "2024-01"): 25.0, ("South", "2024-03"): 50.0, (None, "2024-03"): 60.0,
}
#: ``customers.spend:sum`` attaches the grand customer total to every row.
CM_TOTAL = 350.0


def with_nulls(keys, nonnull: dict) -> dict:
    """Expand a sparse non-NULL oracle over the full group-key universe."""
    return {k: nonnull.get(k) for k in keys}


def dim_key(row: dict, *, family: str, with_month: bool):
    """The oracle group key of one result row for a dimension family."""
    key: tuple = (row["orders.region"],)
    if family == "expr":
        key += (row["orders.lc"],)
    elif family == "band":
        key += (int(row["orders.band"]),)
    elif family == "bare":
        key += (float(row["orders.ct"]),)
    elif family == "rank":
        key += (int(row["orders.rr"]),)
    if with_month:
        key += (month_key(row["orders.ordered_at"]),)
    return key


__all__ = [
    "BAND35", "BAND35_OF", "BARE_DIM", "CITY_TOTAL", "CM_TOTAL", "COL_WM",
    "CP_PRED", "ColumnRef", "DIM_FAMILY_DIMS", "GRAND_TOTAL", "GROUP_M",
    "GROUP_M_MONTH", "ModelMeasure", "RANK_DIM", "REGION_LAST", "REGION_TOTAL",
    "SlayerQuery", "TD_TRANSFORM_OPS", "TRAILING_90D_REGION", "TRANSFORM_FORMULAS",
    "TRANSFORM_X", "TimeDimension", "TimeGranularity", "dev1837_models", "dim_key",
    "gen", "make_exec_engine", "month_key", "month_td", "q", "rows_by", "with_nulls",
]
