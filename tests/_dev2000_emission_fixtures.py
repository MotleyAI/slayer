"""Rendering and AST helpers for the time-zone emission tests: the conversion primitives each dialect uses."""

from __future__ import annotations

from typing import Any, Optional

import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.core.query import SlayerQuery

from tests._dev2000_fixtures import FISCAL_WEEK, NY, PinnedClock, instant, marks_dataset, orders_dataset
from tests._engine_helpers import _engine_generate

TIER1 = ("sqlite", "duckdb", "postgres", "mysql", "clickhouse", "tsql", "trino", "snowflake", "bigquery")
TIER2 = ("redshift", "databricks")
#: Conversion primitives above one read of a naive (source zone UTC) and an instant field (D15).
NAIVE_DEPTH = {"sqlite": 1, "duckdb": 2, "postgres": 2, "mysql": 1, "clickhouse": 1, "tsql": 2, "trino": 2,
               "snowflake": 1, "bigquery": 2}
INSTANT_DEPTH = {d: 1 for d in TIER1}
_NAMED = frozenset({"totimezone", "slayer_convert_tz", "with_timezone", "convert_tz", "convert_timezone"})
_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
DS_FIELDS = {"time_zone": "UTC", "granularities": [FISCAL_WEEK]}


def zoned_query(zone: Optional[str] = NY, **fields: Any) -> SlayerQuery:
    body: dict[str, Any] = {"population": "orders", **fields}
    if zone is not None:
        body["time_zone"] = zone
    return SlayerQuery.model_validate(body)


async def render(dialect: str, query: SlayerQuery, *, now: str = "2025-06-01 12:00:00") -> str:
    """``query`` rendered for ``dialect`` over ``orders`` / ``marks`` in datasource ``shop`` (``time_zone: UTC``)."""
    return await _engine_generate(
        query=query, dataset=orders_dataset(), extra_datasets=[marks_dataset()], dialect=dialect,
        clock=PinnedClock(instant(now)), datasource_fields=DS_FIELDS,
    )


def parse(sql: str, dialect: str) -> Expr:
    return sqlglot.parse_one(sql, read=dialect)


def is_conversion(node: Expr) -> bool:
    """A zone-conversion primitive: ``AT TIME ZONE``, ``CONVERT_TZ`` / ``CONVERT_TIMEZONE``, zoned ``DATETIME`` /
    ``TIMESTAMP`` (BigQuery), ``toTimeZone``, ``with_timezone``, ``slayer_convert_tz``."""
    if isinstance(node, (exp.AtTimeZone, exp.ConvertTimezone)):
        return True
    if isinstance(node, exp.Datetime):
        return node.args.get("expression") is not None
    if isinstance(node, exp.Timestamp):
        return node.args.get("zone") is not None
    return isinstance(node, exp.Anonymous) and str(node.this).lower() in _NAMED


def conversions_above(node: Expr, *, stop: Optional[Expr] = None) -> int:
    """How many conversion primitives wrap ``node`` (up to ``stop``, exclusive)."""
    count, cur = 0, node.parent
    while cur is not None and cur is not stop:
        count += is_conversion(cur)
        cur = cur.parent
    return count


def _relation(column: exp.Column) -> Optional[Expr]:
    """The FROM / JOIN relation ``column``'s qualifier names, in the nearest enclosing SELECT binding it."""
    select = column.find_ancestor(exp.Select)
    while select is not None:
        for holder in [select.args.get("from_"), *(select.args.get("joins") or [])]:
            relation = holder.this if holder is not None else None
            if relation is not None and relation.alias_or_name == column.table:
                return relation
        select = select.find_ancestor(exp.Select)
    return None


def reads(tree: Expr, name: str) -> list[exp.Column]:
    """Reads of a dataset table's column ``name``: a column of a CTE or derived table carries an earlier read."""
    ctes = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    out = []
    for column in tree.find_all(exp.Column):
        relation = _relation(column) if column.name.lower() == name else None
        if isinstance(relation, exp.Table) and relation.name not in ctes:
            out.append(column)
    return out


def comparisons(tree: Expr) -> list[exp.Binary]:
    return [n for n in tree.walk() if isinstance(n, _COMPARISONS)]


def literal_contexts(tree: Expr, prefix: str, *, dialect: str) -> list[str]:
    """The rendered parent of every string literal starting with ``prefix``."""
    return [
        (lit.parent or lit).sql(dialect=dialect) for lit in tree.find_all(exp.Literal)
        if lit.is_string and str(lit.this).startswith(prefix)
    ]


def converted_sides(tree: Expr, name: str) -> list[tuple[Expr, Expr]]:
    """``(converted side, other side)`` of each comparison one side of which converts a read of ``name``."""
    out = []
    for cmp in comparisons(tree):
        for side, other in ((cmp.left, cmp.right), (cmp.right, cmp.left)):
            if any(conversions_above(c, stop=cmp) for c in reads(side, name)):
                out.append((side, other))
    return out


def raw_sides(tree: Expr, name: str) -> list[tuple[Expr, Expr]]:
    """``(raw side, other side)`` of each comparison one side of which reads ``name`` unconverted."""
    out = []
    for cmp in comparisons(tree):
        for side, other in ((cmp.left, cmp.right), (cmp.right, cmp.left)):
            found = reads(side, name)
            if found and not any(conversions_above(c, stop=cmp) for c in found) and not reads(other, name):
                out.append((side, other))
    return out
