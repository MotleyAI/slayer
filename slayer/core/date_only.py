"""Date-only comparisons in Mode-B text: a bare ``'YYYY-MM-DD'`` compared by ``<=``, ``>``, ``=`` or ``!=`` (``BETWEEN``
included) is its midnight, where it once was its whole day; each such comparison and its whole-day spelling."""

from __future__ import annotations

import re
from collections import Counter
from typing import List, Optional, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.syntax import Cmp, DottedRef, Literal, Ref, StringLiteralSpan, parse_template, string_literal_spans, walk_parsed
from slayer.core.time_points import is_date_only, next_day

#: Operand-left comparison ops whose date-only reading changed, each with its whole-day op and literal shift.
_WHOLE_DAY = {"<=": ("<", True), ">": (">=", True), "==": ("in", False), "=": ("in", False), "!=": ("not in", False)}
_MIRROR = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "==": "==", "=": "=", "!=": "!=", "<>": "!="}
_NORMAL = {"=": "==", "==": "==", "<>": "!=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}

_IDENT = r"[A-Za-z_]\w*"
_PATH = rf"(?<![\w.\"]){_IDENT}(?:\s*\.\s*{_IDENT})*"
_OP = r"<=|>=|<>|!=|==|=|<|>"
_OPERAND_LEFT = re.compile(rf"({_PATH})(\s*)({_OP})(\s*)$")
_LITERAL_LEFT = re.compile(rf"(\s*)({_OP})(\s*)({_PATH})(?![\w.(])")
_BETWEEN = re.compile(rf"({_PATH})\s+(?:(not)\s+)?between\s+$", re.IGNORECASE)
_AND = re.compile(r"\s+and\s+", re.IGNORECASE)
_LEADING = re.compile(r"(?:^|[(,]|\b(?:and|or|not|when|where|then|else|case))$", re.IGNORECASE)
_TRAILING = re.compile(r"^(?:$|[),]|(?:and|or|then|else|end|when)\b)", re.IGNORECASE)


class DateOnlySite(BaseModel):
    """A changed date-only comparison at ``text[start:end]`` of the column ``operand`` (``parts``) with ``date``, and its
    whole-day ``twin``."""

    model_config = ConfigDict(frozen=True)

    start: int
    end: int
    parts: Tuple[str, ...]
    operand: str
    date: str
    twin: str


class DateOnlyScan(BaseModel):
    """A text's changed date-only comparisons: those located on a column (``sites``) and how many others there are."""

    model_config = ConfigDict(frozen=True)

    sites: Tuple[DateOnlySite, ...] = ()
    unlocated: int = 0


def _whole_day(op: str, date: str) -> Tuple[str, str]:
    """The operand-left ``(op, literal)`` ``op 'date'`` meant as a whole day; past the last representable day, the
    day's last microsecond bounds it."""
    new_op, shifted = _WHOLE_DAY[op]
    if not shifted:
        return new_op, date
    try:
        return new_op, next_day(date)
    except OverflowError:
        return op, f"{date.strip()} 23:59:59.999999"


def whole_day_spelling(*, operand: str, op: str, date: str) -> str:
    """The comparison ``operand <op> 'date'`` meant when a bare date was its whole day."""
    new_op, literal = _whole_day(op, date)
    return f"{operand} {new_op} '{literal}'"


def scan_date_only(text: str) -> DateOnlyScan:
    """The changed date-only comparisons ``text`` holds, read by the one Mode-B reader; none when it does not parse."""
    literals = [s for s in string_literal_spans(text) if is_date_only(s.value)]
    if not literals:
        return DateOnlyScan()
    try:
        parsed = parse_template(text)
    except Exception:  # noqa: BLE001 — unparseable stored text keeps its spelling
        return DateOnlyScan()
    pending = Counter(_candidates(parsed))
    sites: List[DateOnlySite] = []
    for key, site in _located(text):
        if pending[key] > 0:
            pending[key] -= 1
            sites.append(site)
    return DateOnlyScan(sites=tuple(sites), unlocated=sum(pending.values()))


def normal_op(op: str) -> str:
    """``op`` in the scanner's spelling (``=`` is ``==``, ``<>`` is ``!=``)."""
    return _NORMAL.get(op, op)


def date_comparisons(text: str) -> List[Tuple[Optional[Tuple[str, ...]], str, str]]:
    """``(operand column parts or None, operand-left op, date)`` per changed comparison in ``text``; none when it does
    not parse."""
    try:
        return _candidates(parse_template(text))
    except Exception:  # noqa: BLE001 — unparseable text has no comparisons to report
        return []


def _candidates(parsed: object) -> List[Tuple[Optional[Tuple[str, ...]], str, str]]:
    """``(operand column parts or None, operand-left op, date)`` per changed comparison in the tree."""
    out = []
    for node in walk_parsed(parsed):
        if not isinstance(node, Cmp) or node.op not in _NORMAL:
            continue
        for literal, operand, op in ((node.right, node.left, node.op), (node.left, node.right, _MIRROR[node.op])):
            if _date_literal(literal) and not isinstance(operand, Literal) and op in _WHOLE_DAY:
                assert isinstance(literal, Literal) and isinstance(literal.value, str)
                out.append((_parts(operand), _NORMAL[op], literal.value))
    return out


def _date_literal(node: object) -> bool:
    return isinstance(node, Literal) and isinstance(node.value, str) and is_date_only(node.value)


def _parts(node: object) -> Optional[Tuple[str, ...]]:
    if isinstance(node, Ref):
        return (node.name,)
    if isinstance(node, DottedRef):
        return tuple(node.parts)
    return None


def _path_parts(path: str) -> Tuple[str, ...]:
    return tuple(p.strip() for p in path.split("."))


def _bounded(text: str, *, start: int, end: int) -> bool:
    """Whether ``text[start:end]`` stands alone as a predicate (no operator binds tighter on either side)."""
    return bool(_LEADING.search(text[:start].rstrip())) and bool(_TRAILING.match(text[end:].lstrip()))


def _located(text: str):
    """``(candidate key, site)`` per changed comparison laid out as ``col <op> 'D'``, ``'D' <op> col`` or
    ``col [NOT] BETWEEN 'A' AND 'D'``."""
    spans = string_literal_spans(text)
    consumed: set[int] = set()
    for i in range(len(spans) - 1):
        found = _between(text, low=spans[i], high=spans[i + 1])
        if found is not None:
            consumed.update((i, i + 1))
            yield found
    for i, lit in enumerate(spans):
        if i in consumed or not is_date_only(lit.value):
            continue
        found = _operand_left(text, lit) or _literal_left(text, lit)
        if found is not None:
            yield found


def _between(text: str, *, low: StringLiteralSpan, high: StringLiteralSpan):
    m = _BETWEEN.search(text, 0, low.start)
    if m is None or not _AND.fullmatch(text, low.end, high.start) or not is_date_only(high.value):
        return None
    if not _bounded(text, start=m.start(), end=high.end):
        return None
    operand, low_text = m.group(1), text[low.start:low.end]
    if m.group(2):
        above, upper = _whole_day(">", high.value)
        twin = f"({operand} < {low_text} or {operand} {above} '{upper}')"
    else:
        below, upper = _whole_day("<=", high.value)
        twin = f"({operand} >= {low_text} and {operand} {below} '{upper}')"
    parts = _path_parts(operand)
    site = DateOnlySite(start=m.start(), end=high.end, parts=parts, operand=operand, date=high.value, twin=twin)
    return (parts, "<=", high.value), site


def _operand_left(text: str, lit: StringLiteralSpan):
    m = _OPERAND_LEFT.search(text, 0, lit.start)
    if m is None or _NORMAL[m.group(3)] not in _WHOLE_DAY or not _bounded(text, start=m.start(), end=lit.end):
        return None
    operand, op = m.group(1), _NORMAL[m.group(3)]
    new_op, value = _whole_day(op, lit.value)
    pad = " " if new_op[0].isalpha() else ""
    twin = f"{operand}{m.group(2) or pad}{new_op}{m.group(4) or pad}'{value}'"
    parts = _path_parts(operand)
    site = DateOnlySite(start=m.start(), end=lit.end, parts=parts, operand=operand, date=lit.value, twin=twin)
    return (parts, op, lit.value), site


def _literal_left(text: str, lit: StringLiteralSpan):
    m = _LITERAL_LEFT.match(text, lit.end)
    if m is None:
        return None
    op = _NORMAL[_MIRROR[m.group(2)]]
    if op not in _WHOLE_DAY or not _bounded(text, start=lit.start, end=m.end()):
        return None
    operand = m.group(4)
    new_op, value = _whole_day(op, lit.value)
    if _WHOLE_DAY[op][1]:
        twin = f"'{value}'{m.group(1)}{_MIRROR[new_op]}{m.group(3)}{operand}"
    else:
        twin = f"{operand} {new_op} '{value}'"
    parts = _path_parts(operand)
    site = DateOnlySite(start=lit.start, end=m.end(), parts=parts, operand=operand, date=lit.value, twin=twin)
    return (parts, op, lit.value), site
