"""Pandas ground-truth oracle for the engine A/B audit.

Oracle specs are explicit ``{fn, args}`` instructions carried by corpus
entries — deliberately decoupled from the engine's query language so the
oracle can arbitrate when the two engine versions disagree.

Scope: aggs, group-bys, filters, left joins, limit/order, month/year
bucketing, cumsum/change post-ops. Percentile, week/quarter bucketing and
time_shift are implementation-defined and stay engine-vs-engine only.
"""

import importlib.util
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def load_seed_module():
    """Load tests/perf/seed.py by path (tests.perf is a package; we are not in it)."""
    path = Path(__file__).parent.parent / "seed.py"
    spec = importlib.util.spec_from_file_location("slayer_perf_seed", path)
    assert spec is not None and spec.loader is not None, path
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def frames_from_tables(tables: dict[str, list[dict]]) -> dict[str, pd.DataFrame]:
    return {name: pd.DataFrame(rows) for name, rows in tables.items()}


def frames_from_dataset(dataset: Any) -> dict[str, pd.DataFrame]:
    """Build frames from a tests/perf/seed.py Dataset, mirroring seed_database columns."""
    return {
        "regions": pd.DataFrame([{"id": r.id, "name": r.name} for r in dataset.regions]),
        "shops": pd.DataFrame([
            {"id": s.id, "name": s.name, "region_id": s.region_id,
             "avg_cost": s.avg_cost, "avg_frequency": s.avg_frequency, "size": s.size}
            for s in dataset.shops
        ]),
        "customers": pd.DataFrame([
            {"id": c.id, "name": c.name, "segment": c.segment,
             "primary_shop_id": c.shop_ids[0]}
            for c in dataset.customers
        ]),
        "orders": pd.DataFrame([
            {"id": o.id, "customer_id": o.customer_id, "shop_id": o.shop_id,
             "category": o.category, "cost": o.cost, "created_at": o.created_at,
             "completed_at": o.completed_at, "cancelled_at": o.cancelled_at}
            for o in dataset.orders
        ]),
    }


# ---------------------------------------------------------------------------
# The one aggregation evaluator
# ---------------------------------------------------------------------------

# full-date form: comparable to engine-truncated timestamps via ISO coercion
_BUCKET_FORMATS = {"month": "%Y-%m-01", "year": "%Y-01-01"}


def _apply_joins(df: pd.DataFrame, joins: list[dict], frames: dict) -> pd.DataFrame:
    for join in joins:
        right = frames[join["table"]].copy()
        right.columns = [f"{join['table']}.{c}" for c in right.columns]
        right_key = f"{join['table']}.{join['right_on']}"
        # SQL semantics: NULL keys never match (pandas merge would pair NaNs)
        right = right[right[right_key].notna()]
        left_key = df[join["left_on"]]
        matched = df[left_key.notna()].merge(
            right, how="left", left_on=join["left_on"], right_on=right_key,
        )
        unmatched = df[left_key.isna()].reindex(columns=matched.columns)
        df = pd.concat([matched, unmatched], ignore_index=True)
    return df


def _series_for(df: pd.DataFrame, grouped, col: str):
    return grouped[col] if grouped is not None else df[col]


def _compute_agg(df: pd.DataFrame, grouped, agg: dict, col: str | None = None):
    op = agg["op"]
    if op == "count_star":
        return grouped.size() if grouped is not None else len(df)
    series = _series_for(df, grouped, col or agg["col"])
    if op == "sum":
        return series.sum(min_count=1)
    if op == "avg":
        return series.mean()
    if op == "min":
        return series.min()
    if op == "max":
        return series.max()
    if op == "count":
        return series.count()
    if op == "count_distinct":
        return series.nunique()
    raise ValueError(f"unsupported oracle agg op: {op}")


def _agg_name(agg: dict) -> str:
    return "_count" if agg["op"] == "count_star" else f"{agg['col']}_{agg['op']}"


def _to_native(value: Any) -> Any:
    if value is None or (isinstance(value, float) and value != value):
        return None
    if value is pd.NaT or (isinstance(value, pd.Timestamp) and pd.isna(value)):
        return None
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _agg_fn(frames: dict, args: dict) -> list[list]:
    df = frames[args.get("table", "orders")].copy()
    df = _apply_joins(df, args.get("joins", []), frames)

    bucket = args.get("time_bucket")
    if bucket:
        col = bucket["col"]
        fmt = _BUCKET_FORMATS[bucket["gran"]]
        df[f"{col}_{bucket['gran']}"] = pd.to_datetime(df[col]).dt.strftime(fmt)

    if args.get("filter"):
        df = df.query(args["filter"], engine="python")

    groupby = args.get("groupby", [])
    aggs = args.get("aggs", [])
    # Per-aggregate `where`: a filtered measure (Column.filter / CASE-WHEN), scoped
    # to that one aggregate — NOT a query-level filter. Mask the input to NaN where
    # the predicate is false but keep every group, so an all-non-matching group
    # aggregates to NULL (matching the engine's LEFT-JOIN-back on an isolated CTE)
    # instead of vanishing the way a WHERE-then-GROUP would.
    masked: dict[int, str] = {}
    for idx, agg in enumerate(aggs):
        where = agg.get("where")
        if not where:
            continue
        if agg["op"] == "count_star":
            raise ValueError("oracle: `where` on count_star is unsupported")
        tmp = f"__where_{idx}"
        df[tmp] = df[agg["col"]].where(df.eval(where, engine="python"))
        masked[idx] = tmp
    if not aggs:
        result = df[groupby].copy()
        if args.get("distinct", True):
            result = result.drop_duplicates()
    elif groupby:
        grouped = df.groupby(groupby, dropna=False, sort=False)
        pieces = {_agg_name(a): _compute_agg(df, grouped, a, masked.get(i))
                  for i, a in enumerate(aggs)}
        result = pd.DataFrame(pieces).reset_index()
    else:
        result = pd.DataFrame([{_agg_name(a): _compute_agg(df, None, a, masked.get(i))
                                for i, a in enumerate(aggs)}])

    if args.get("having"):
        result = result.query(args["having"], engine="python")

    for order_col, direction in args.get("order_by", []):
        result = result.sort_values(order_col, ascending=direction == "asc", kind="stable")

    for post in args.get("post", []):
        source = result[post["on"]]
        if post["op"] == "cumsum":
            result[f"{post['on']}_cumsum"] = source.cumsum()
        elif post["op"] == "change":
            # period-aware, matching the engine: current minus the PREVIOUS
            # CALENDAR period's value — None across gap periods, not row-diff
            if not bucket:
                raise ValueError("oracle change post-op requires a time_bucket")
            period_col = f"{bucket['col']}_{bucket['gran']}"
            offset = (pd.DateOffset(months=1) if bucket["gran"] == "month"
                      else pd.DateOffset(years=1))
            prev_keys = (pd.to_datetime(result[period_col]) - offset).dt.strftime(
                _BUCKET_FORMATS[bucket["gran"]])
            by_period = dict(zip(result[period_col], source))
            result[f"{post['on']}_change"] = [
                cur - by_period[prev]
                if prev in by_period and pd.notna(cur) and pd.notna(by_period[prev])
                else None
                for prev, cur in zip(prev_keys, source)
            ]
        else:
            raise ValueError(f"unsupported oracle post op: {post['op']}")

    offset = args.get("offset", 0)
    limit = args.get("limit")
    if offset or limit is not None:
        end = None if limit is None else offset + limit
        result = result.iloc[offset:end]

    columns = groupby + [_agg_name(a) for a in aggs] + [
        f"{p['on']}_{p['op']}" for p in args.get("post", [])
    ]
    return [[_to_native(v) for v in row] for row in result[columns].itertuples(index=False)]


ORACLE_FNS = {"agg": _agg_fn}


def expected(spec: dict, frames: dict) -> list[list]:
    fn = ORACLE_FNS[spec["fn"]]
    return fn(frames, spec.get("args", {}))
