"""Translate Cube curly references to SLayer SQL (Mode A) / DSL (Mode B).

Cube `sql`/`filter` strings use `{CUBE}`, `{member}`, `{cube.member}` — single
braces, distinct from Jinja's `{{ }}` / `{% %}` (which are detected and skipped
upstream). See DEV-1608 §3.
"""

import re
from collections.abc import Collection, Mapping

from pydantic import BaseModel, Field

from slayer.core.refs import IDENTIFIER_RE

_JINJA_RE = re.compile(r"\{\{|\{%|%\}|\}\}")
_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")  # SQL string literal, doubled-quote aware
_REF_RE = re.compile(r"\{([^{}]+)\}")
_MARKER_PREFIX = "__slayer_member_"
_PHYSICAL_PREFIX = "__slayer_physical_"
_IDENT_RE = re.compile(r'"(?:[^"]|"")+"|[A-Za-z_]\w*')

# Operand forms accepted inside a join ON clause.
_OPERAND_BRACE_DOT = re.compile(r"^\{([A-Za-z_]\w*)\}\.(\w+)$")  # {CUBE}.col
_OPERAND_BRACED = re.compile(r"^\{([^{}]+)\}$")                  # {cube.col}
# `\bAND\b` (no surrounding `\s+` quantifiers) avoids the polynomial-backtracking
# shape Sonar S5852 flags; operands are whitespace-stripped after the split.
_AND_SPLIT = re.compile(r"\bAND\b", re.IGNORECASE)


def contains_jinja(text: str) -> bool:
    """True if ``text`` contains Jinja markers (`{{ }}` or `{% %}`).

    Cube's own `{CUBE}` / `{cube.member}` single-brace refs are NOT Jinja.
    """
    return bool(_JINJA_RE.search(text))


def translate_cube_refs(text: str, *, mode: str, cube: str | None = None) -> str:
    """Translate Cube curly refs in ``text`` (string literals are left intact).

    - ``{CUBE}.col`` → ``col`` (SLayer auto-qualifies bare names)
    - bare ``{CUBE}`` → the cube name (table reference)
    - ``{member}`` → ``member`` (same-cube sibling)
    - ``{cube.member}`` / ``{a.b.c}`` → ``cube.member`` / ``a.b.c`` (dotted)

    ``mode`` (``"sql"``/``"dsl"``) documents the intended target layer for the
    caller; it is validated but does not change the syntactic rewrite.
    """
    if mode not in ("sql", "dsl"):
        raise ValueError(f"mode must be 'sql' or 'dsl', got {mode!r}")
    literal_spans = [m.span() for m in _LITERAL_RE.finditer(text)]

    def _in_literal(pos: int) -> bool:
        return any(s <= pos < e for s, e in literal_spans)

    out: list[str] = []
    last = 0
    for m in _REF_RE.finditer(text):
        if m.start() < last or _in_literal(m.start()):
            continue
        out.append(text[last:m.start()])
        next_char = text[m.end()] if m.end() < len(text) else ""
        replacement, extra = _resolve_ref(m.group(1).strip(), next_char, cube)
        out.append(replacement)
        last = m.end() + extra
    out.append(text[last:])
    return "".join(out)


def _resolve_ref(inner: str, next_char: str, cube: str | None) -> tuple[str, int]:
    """Resolve one ``{ref}`` to ``(replacement, extra_chars_consumed)``."""
    if inner == "CUBE":
        if next_char == ".":
            return "", 1  # `{CUBE}.col` → drop the `{CUBE}` and the following dot
        return cube or "", 0
    return inner, 0  # `{member}` / `{a.b}` → inner verbatim


class ForeignRef(BaseModel):
    """A joined cube's physical column, as written."""

    cube: str
    column: str  # identifier SQL, quoting kept


class CubeSql(BaseModel):
    """Cube SQL with physical references bare and each member reference as a marker identifier."""

    text: str
    members: dict[str, str] = Field(default_factory=dict)  # marker -> source member name
    foreign: dict[str, ForeignRef] = Field(default_factory=dict)  # marker -> another cube's physical column
    unknown: list[str] = Field(default_factory=list)  # referenced members the cube does not declare

    def resolved(self, emitted: Mapping[str, str]) -> str:
        """``text`` with each marker replaced by its member's emitted name."""
        out = self.text
        for marker, member in self.members.items():
            out = out.replace(marker, emitted.get(member, member))
        return out


def member_marker(index: int) -> str:
    return f"{_MARKER_PREFIX}{index}__"


def classify_cube_refs(text: str, *, cube: str, members: Collection[str], start: int = 0) -> CubeSql:
    """Translate ``text``'s Cube refs: ``{CUBE}.x`` / ``{<cube>}.x`` → physical ``x``; ``{other}.x`` → a marker for
    ``other``'s physical ``x``; ``{m}`` / ``{CUBE.m}`` / ``{<cube>.m}`` → a member marker; ``{other.m}`` →
    ``other.m``; ``{CUBE}`` → the cube name. Markers are numbered from ``start``."""
    literal_spans = [m.span() for m in _LITERAL_RE.finditer(text)]
    out: list[str] = []
    found = CubeSql(text="")
    last = 0
    for m in _REF_RE.finditer(text):
        if m.start() < last or any(s <= m.start() < e for s, e in literal_spans):
            continue
        out.append(text[last:m.start()])
        replacement, last = _classify(m, text=text, cube=cube, members=members, found=found, start=start)
        out.append(replacement)
    out.append(text[last:])
    found.text = "".join(out)
    return found


def _classify(m: re.Match[str], *, text: str, cube: str, members: Collection[str], found: CubeSql,
              start: int) -> tuple[str, int]:
    """One brace ref's replacement and the position after it; markers are recorded on ``found``."""
    parts = m.group(1).strip().split(".")
    end = m.end()
    index = start + len(found.members) + len(found.foreign)
    if len(parts) == 1 and text[end:end + 1] == ".":
        if parts[0] in ("CUBE", cube):
            return "", end + 1  # `{CUBE}.col` → `col`
        if (col := _IDENT_RE.match(text, end + 1)) is not None:
            marker = f"{_PHYSICAL_PREFIX}{index}__"
            found.foreign[marker] = ForeignRef(cube=parts[0], column=col.group(0))
            return marker, col.end()
    if parts == ["CUBE"]:
        return cube, end
    if len(parts) == 1 or (len(parts) == 2 and parts[0] in ("CUBE", cube)):
        if parts[-1] not in members:
            found.unknown.append(parts[-1])
        marker = member_marker(index)
        found.members[marker] = parts[-1]
        return marker, end
    return m.group(1).strip(), end


class JoinSide(BaseModel):
    """One side of a join key: a physical column of its cube, or one of its members."""

    name: str
    physical: bool


def _operand_ref(operand: str) -> tuple[str, JoinSide] | None:
    """One side of an equality as ``(qualifier, side)``: ``{cube}.col`` physical, ``{cube.member}`` a member.

    ``None`` for anything else (function calls, arithmetic, literals) — those can't go in ``join_pairs``.
    """
    operand = operand.strip()
    m = _OPERAND_BRACE_DOT.match(operand)
    if m:
        return m.group(1), JoinSide(name=m.group(2), physical=True)
    m = _OPERAND_BRACED.match(operand)
    if m:
        parts = m.group(1).strip().split(".")
        if len(parts) >= 2:
            return parts[0], JoinSide(name=".".join(parts[1:]), physical=False)
    return None


def parse_join_on(on_sql: str, *, source_cube: str, target_cube: str) -> list[list[str]] | None:
    """A Cube join ON clause as ``[[source column, target column], ...]``; ``None`` for a non-equality ON."""
    keys = parse_join_keys(on_sql, source_cube=source_cube, target_cube=target_cube)
    return None if keys is None else [[src.name, tgt.name] for src, tgt in keys]


def parse_join_keys(on_sql: str, *, source_cube: str, target_cube: str) -> list[tuple[JoinSide, JoinSide]] | None:
    """A Cube join ON clause (an equality or AND-conjunction of equalities) as ``(source, target)`` key sides;
    ``None`` for any other ON."""
    pairs: list[tuple[JoinSide, JoinSide]] = []
    for part in _AND_SPLIT.split(on_sql.strip()):
        pair = _equality_pair(part, source_cube=source_cube, target_cube=target_cube)
        if pair is None:
            return None
        pairs.append(pair)
    return pairs or None


def _equality_pair(part: str, *, source_cube: str, target_cube: str) -> tuple[JoinSide, JoinSide] | None:
    """One ``A = B`` equality as ``(source, target)`` sides, or ``None``."""
    if part.count("=") != 1 or any(op in part for op in ("<", ">", "!")):
        return None
    left, right = part.split("=")
    lhs = _operand_ref(left)
    rhs = _operand_ref(right)
    if lhs is None or rhs is None:
        return None
    src = tgt = None
    for qualifier, side in (lhs, rhs):
        if qualifier in ("CUBE", source_cube):
            src = side
        elif qualifier == target_cube:
            tgt = side
    if src is None or tgt is None:
        return None
    return src, tgt


def is_bare_identifier(sql: str) -> bool:
    """True if ``sql`` is a single bare column identifier (usable in join_pairs)."""
    return bool(IDENTIFIER_RE.match(sql.strip()))
