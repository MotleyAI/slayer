"""Repair of time literals in stored queries written by SLayer 0.10.x: zone offsets and slashed dates."""

import re
from typing import Any, cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.time_points import is_time_point_shape

_OFFSET_RE = re.compile(r"(?:Z|[+-]\d{2}:\d{2})$")
_SLASHED_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})(?=$|[ T])")


def normalize_legacy_time_literal(text: str) -> str:
    """``text`` with a zone offset dropped (keeping wall-clock time) and a slashed date in ISO form, when that makes it a time point; else ``text``."""
    if is_time_point_shape(text):
        return text
    candidate = _SLASHED_RE.sub(r"\1-\2-\3", _OFFSET_RE.sub("", text))
    return candidate if is_time_point_shape(candidate) else text


def repair_date_ranges(data: dict) -> dict:
    """Repair every time dimension's stored ``date_range`` in a raw query dict."""
    if isinstance(data.get("time_dimensions"), list):
        data["time_dimensions"] = [_repair_time_dimension(td) for td in data["time_dimensions"]]
    return data


def _repair_time_dimension(td: Any) -> Any:
    if not isinstance(td, dict) or not isinstance(td.get("date_range"), list):
        return td
    bounds = td["date_range"]
    if len(bounds) != 2:
        # 0.10.x filtered only on a [lower, upper] pair, so a single bound filtered nothing either.
        return {k: v for k, v in td.items() if k != "date_range"}
    return {**td, "date_range": [normalize_legacy_time_literal(b) if isinstance(b, str) else b for b in bounds]}


_COMPARISONS = (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE)


def _legacy_literal(node: Any) -> bool:
    return isinstance(node, exp.Literal) and node.is_string and normalize_legacy_time_literal(node.this) != node.this


def legacy_literal_sites(filter_text: str) -> list[tuple[exp.Literal, exp.Column]] | None:
    """Each legacy time literal ``filter_text`` directly compares with a column, or ``None`` when there is none."""
    if "'" not in filter_text:
        return None
    try:
        tree = cast(Expression, sqlglot.parse_one(filter_text))
    except (SqlglotError, ValueError):
        return None
    sites: list[tuple[exp.Literal, exp.Column]] = []
    for node in tree.find_all(*_COMPARISONS):
        for literal, other in ((node.left, node.right), (node.right, node.left)):
            if isinstance(literal, exp.Literal) and _legacy_literal(literal) and isinstance(other, exp.Column):
                sites.append((literal, other))
    for node in tree.find_all(exp.In):
        items = node.expressions
        if isinstance(node.this, exp.Column) and items and all(isinstance(i, exp.Literal) and i.is_string for i in items):
            sites.extend((i, node.this) for i in items if _legacy_literal(i))
    return sites or None


def repaired_filter(text: str, literals: list[exp.Literal]) -> str:
    """``text`` with each of ``literals`` (parsed from it) normalised in place; every other character kept."""
    spans: list[tuple[int, int, exp.Literal]] = []
    for lit in literals:
        start, end = lit.meta.get("start"), lit.meta.get("end")
        if not isinstance(start, int) or not isinstance(end, int):
            return text
        spans.append((start, end, lit))
    for start, end, lit in sorted(spans, key=lambda s: s[0], reverse=True):
        text = text[:start] + exp.Literal.string(normalize_legacy_time_literal(lit.this)).sql() + text[end + 1:]
    return text
