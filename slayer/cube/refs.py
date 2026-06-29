"""Translate Cube curly references to SLayer SQL (Mode A) / DSL (Mode B).

Cube `sql`/`filter` strings use `{CUBE}`, `{member}`, `{cube.member}` — single
braces, distinct from Jinja's `{{ }}` / `{% %}` (which are detected and skipped
upstream). See DEV-1608 §3.
"""

import re

_JINJA_RE = re.compile(r"\{\{|\{%|%\}|\}\}")
_LITERAL_RE = re.compile(r"'[^']*'")
_REF_RE = re.compile(r"\{([^{}]+)\}")

# Operand forms accepted inside a join ON clause.
_OPERAND_BRACE_DOT = re.compile(r"^\{([A-Za-z_]\w*)\}\.(\w+)$")  # {CUBE}.col
_OPERAND_BRACED = re.compile(r"^\{([^{}]+)\}$")                  # {cube.col}
_AND_SPLIT = re.compile(r"\s+AND\s+", re.IGNORECASE)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_]\w*$")


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

    ``mode`` (``"sql"``/``"dsl"``) does not change the syntactic rewrite — it
    only documents the intended target layer for the caller.
    """
    literal_spans = [m.span() for m in _LITERAL_RE.finditer(text)]

    def _in_literal(pos: int) -> bool:
        return any(s <= pos < e for s, e in literal_spans)

    out: list[str] = []
    last = 0
    for m in _REF_RE.finditer(text):
        if m.start() < last or _in_literal(m.start()):
            continue
        inner = m.group(1).strip()
        out.append(text[last:m.start()])
        if inner == "CUBE":
            # `{CUBE}.col` → drop the `{CUBE}` AND the following dot.
            if m.end() < len(text) and text[m.end()] == ".":
                last = m.end() + 1
            else:
                out.append(cube or "")
                last = m.end()
            continue
        out.append(inner)  # `{member}` / `{a.b}` → inner verbatim
        last = m.end()
    out.append(text[last:])
    return "".join(out)


def _operand_ref(operand: str) -> tuple[str, str] | None:
    """Parse one side of an equality into ``(qualifier, column)``.

    Returns ``None`` for anything that isn't a bare Cube column reference
    (function calls, arithmetic, literals) — those can't go in ``join_pairs``.
    """
    operand = operand.strip()
    m = _OPERAND_BRACE_DOT.match(operand)
    if m:
        return m.group(1), m.group(2)
    m = _OPERAND_BRACED.match(operand)
    if m:
        parts = m.group(1).strip().split(".")
        if len(parts) >= 2:
            return parts[0], ".".join(parts[1:])
    return None


def parse_join_on(on_sql: str, *, source_cube: str, target_cube: str) -> list[list[str]] | None:
    """Parse a Cube join ON clause into SLayer ``join_pairs``.

    Returns ``[[src_col, tgt_col], ...]`` for an equality (or AND-conjunction of
    equalities); ``None`` for any non-equality / non-column ON. The column names
    are the *member* names as written — the converter resolves them to physical
    columns (and drops the join if a member's sql is non-trivial).
    """
    pairs: list[list[str]] = []
    for part in _AND_SPLIT.split(on_sql.strip()):
        if part.count("=") != 1 or any(op in part for op in ("<", ">", "!")):
            return None
        left, right = part.split("=")
        lhs = _operand_ref(left)
        rhs = _operand_ref(right)
        if lhs is None or rhs is None:
            return None
        src_col = tgt_col = None
        for qualifier, col in (lhs, rhs):
            if qualifier in ("CUBE", source_cube):
                src_col = col
            elif qualifier == target_cube:
                tgt_col = col
        if src_col is None or tgt_col is None:
            return None
        pairs.append([src_col, tgt_col])
    return pairs or None


def is_bare_identifier(sql: str) -> bool:
    """True if ``sql`` is a single bare column identifier (usable in join_pairs)."""
    return bool(_IDENTIFIER_RE.match(sql.strip()))
