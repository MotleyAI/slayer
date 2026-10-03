"""Shared fixtures for rank-family ordering direction and NULL-input semantics over the sales graph."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Any, Callable, Dict, List, Optional, Tuple

import sqlglot
from pydantic import BaseModel
from sqlglot import exp
from sqlglot.expressions.core import Expression

from tests._dev1847_fixtures import _SALES_ROWS_WIDE

RANK_FNS = (exp.Rank, exp.DenseRank, exp.PercentRank, exp.Ntile)
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

#: sum(amount) by region; Void's two rows are all-NULL.
REGION_TOTALS = {"North": 90.0, "South": 140.0, "East": 180.0, "Gap": 20.0, "Void": None}
RANK_ASC = {"Gap": 1, "North": 2, "South": 3, "East": 4, "Void": None}
RANK_DESC = {"East": 1, "South": 2, "North": 3, "Gap": 4, "Void": None}
NTILE2 = {"Gap": 1, "North": 1, "South": 2, "East": 2, "Void": None}
PERCENT_RANK = {"Gap": 0.0, "North": 1 / 3, "South": 2 / 3, "East": 1.0, "Void": None}
#: rank(min(city), direction='asc') by region: Alpha, Alpha, Delta, Kappa, Xi.
MIN_CITY_RANK_ASC = {"North": 1, "South": 1, "East": 3, "Gap": 4, "Void": 5}
#: rank(sum(amount), partition_by=region, direction='asc') over [region, city].
CITY_RANK_ASC = {
    ("East", "Delta"): 1, ("East", "Epsilon"): 1, ("East", "Zeta"): 3,
    ("North", "Alpha"): 1, ("North", "Beta"): 2,
    ("South", "Alpha"): 1, ("South", "Gamma"): 2,
    ("Gap", "Kappa"): 1, ("Gap", None): 2,
    ("Void", "Xi"): None,
}
#: dense_rank(sum(amount), partition_by=region, direction='desc') over [region, city].
CITY_DENSE_DESC = {
    ("East", "Zeta"): 1, ("East", "Delta"): 2, ("East", "Epsilon"): 2,
    ("North", "Beta"): 1, ("North", "Alpha"): 2,
    ("South", "Gamma"): 1, ("South", "Alpha"): 2,
    ("Gap", None): 1, ("Gap", "Kappa"): 2,
    ("Void", "Xi"): None,
}
#: rank(city, partition_by=region, direction='asc') over [region, city].
CITY_NAME_RANK_ASC = {
    ("East", "Delta"): 1, ("East", "Epsilon"): 2, ("East", "Zeta"): 3,
    ("North", "Alpha"): 1, ("North", "Beta"): 2,
    ("South", "Alpha"): 1, ("South", "Gamma"): 2,
    ("Gap", "Kappa"): 1, ("Gap", None): None,
    ("Void", "Xi"): 1,
}


def cell_totals(key: Callable[[tuple], Tuple]) -> Dict[Tuple, Optional[float]]:
    """sum(amount) per ``key(row)``; an all-NULL cell is NULL."""
    acc: Dict[Tuple, Optional[float]] = {}
    for row in _SALES_ROWS_WIDE:
        k, a = key(row), row[4]
        prev = acc.get(k)
        acc[k] = prev if a is None else (prev or 0.0) + a
    return acc


def rank_within(values: Mapping[Tuple, Any], *, descending: bool, dense: bool = False) -> Dict[Tuple, Optional[int]]:
    """Competition (or dense) rank within ``k[0]``; a NULL value ranks NULL."""
    groups: Dict = defaultdict(dict)
    for k, v in values.items():
        groups[k[0]][k] = v
    out: Dict[Tuple, Optional[int]] = {}
    for cells in groups.values():
        nonnull = [v for v in cells.values() if v is not None]
        for k, v in cells.items():
            if v is None:
                out[k] = None
                continue
            ahead = [x for x in nonnull if (x > v if descending else x < v)]
            out[k] = 1 + (len(set(ahead)) if dense else len(ahead))
    return out


class RankWindow(BaseModel):
    """The ordering-relevant shape of one emitted rank-family window."""

    fn: str
    order_sql: str
    descending: bool
    partition_sql: List[str]
    null_flag: bool
    null_guarded: bool


def _is_null_test(node: Expression, target: str, dialect: str) -> bool:
    return isinstance(node, exp.Is) and isinstance(node.expression, exp.Null) and (
        node.this.sql(dialect=dialect) == target)


def _is_null_flag(node: Expression, target: str, dialect: str) -> bool:
    """``CASE WHEN <target> IS NULL THEN 1 ELSE 0 END``."""
    if not isinstance(node, exp.Case) or len(node.args.get("ifs") or []) != 1:
        return False
    [branch] = node.args["ifs"]
    default = node.args.get("default")
    return (_is_null_test(branch.this, target, dialect)
            and branch.args.get("true") is not None and branch.args["true"].sql() == "1"
            and default is not None and default.sql() == "0")


def _is_null_guard(window: exp.Window, target: str, dialect: str) -> bool:
    """The window is the ELSE of ``CASE WHEN <target> IS NULL THEN NULL ELSE <window> END``."""
    case = window.parent
    if not isinstance(case, exp.Case) or case.args.get("default") is not window:
        return False
    ifs = case.args.get("ifs") or []
    return (len(ifs) == 1 and _is_null_test(ifs[0].this, target, dialect)
            and isinstance(ifs[0].args.get("true"), exp.Null))


def rank_windows(sql: str, *, dialect: str) -> List[RankWindow]:
    """Every rank-family window in ``sql``, in document order."""
    out: List[RankWindow] = []
    for window in sqlglot.parse_one(sql, read=dialect).find_all(exp.Window):
        if not isinstance(window.this, RANK_FNS):
            continue
        [ordered] = window.args["order"].expressions
        target = ordered.this.sql(dialect=dialect)
        parts = window.args.get("partition_by") or []
        flags = [p for p in parts if _is_null_flag(p, target, dialect)]
        out.append(RankWindow(
            fn=type(window.this).__name__.upper(),
            order_sql=target,
            descending=bool(ordered.args.get("desc")),
            partition_sql=[p.sql(dialect=dialect) for p in parts if p not in flags],
            null_flag=len(flags) == 1,
            null_guarded=_is_null_guard(window, target, dialect),
        ))
    return out
