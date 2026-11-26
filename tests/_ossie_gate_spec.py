"""Reads Ossie's expression spec for the Layer 1 completeness check; fails closed on any line it cannot classify."""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from slayer.core.keys import SCALAR_SPECS
from tests._ossie_gate import corpus_root

_HEADING = re.compile(r"^(#{1,6}) (.+?)\s*$")
_CODE = re.compile(r"`([^`]+)`")
_CALLEE = re.compile(r"^([A-Z_][A-Z0-9_]*)\(")
_FENCE = "```"
MARKS = ("(REQUIRED)", "(RECOMMENDED)")
#: Unmarked sections whose listed forms the table covers too.
UNMARKED_FORMS = frozenset({"Syntax"})
#: Sections whose list items list date parts or cast targets.
_PART_LISTS = ("Alternative Extraction", "CAST")

#: Every prose line of a REQUIRED or RECOMMENDED section, as the vendored spec words it.
KNOWN_PROSE = frozenset({
    "Where `p` is a value between 0 and 1 (e.g., 0.5 for median, 0.75 for 75th percentile).",
    "Approximate functions trade exact accuracy for significantly better performance on large datasets. They use "
    "probabilistic algorithms (sketches) that are efficiently mergeable, making them well-suited for distributed "
    "computation.",
    "Engine support for approximate aggregates varies. For a non-normative mapping, see "
    "[extensions/function_mappings.md](../extensions/function_mappings.md#approximate-aggregates).",
    "SUM / COUNT aggregation functions support `DISTINCT.`",
    "All aggregations except `AGG` must support filtered aggregation:",
    "Every aggregate function except `AGG` must support a postfix `FILTER (WHERE <predicate>)` modifier:",
    "In this version, `AGG` takes no `FILTER` clause (see [Measure Evaluation: `AGG`](#measure-evaluation-agg)).",
    "The clause has the semantics of the SQL:2003 `<filter clause>` (optional feature T612): the aggregate considers "
    "only the rows for which `<predicate>` succeeds. `FILTER` is applied to the aggregate's input, not as a query "
    "`WHERE`, so it never removes output groups. A converter or engine MUST NOT rewrite an intrinsic `FILTER` "
    "predicate into a query-level `WHERE` clause when doing so would change group membership.",
    "`FILTER (WHERE ...)` is a modifier on an aggregate expression. It is not the standalone `WHERE` clause listed "
    "under [Not Supported in Expressions](#not-supported-in-expressions). The `<predicate>` must reference only "
    "fields of the same dataset as the aggregate's arguments.",
    "An engine MAY evaluate `FILTER (WHERE ...)` by rewriting it to an equivalent expression, but the result MUST be "
    "identical to the result that the `<filter clause>` semantics define.",
    "*Note (non-normative):* For an aggregate function that ignores `NULL` inputs, `FILTER (WHERE ...)` is "
    "equivalent to a `CASE` expression with no `ELSE`:",
    "A non-matching row contributes `NULL`, which these functions ignore. An `ELSE 0` would count as a real row and "
    "would corrupt `AVG`, `MIN`, `MAX`, and `COUNT`. The identity does not hold for an aggregate function that does "
    "not ignore `NULL` inputs, such as `ARRAY_AGG`.",
    "Supported date parts for `EXTRACT` and `DATE_PART`:",
    "Construct DATE, TIME, and TIMESTAMP values using ANSI typed literals or `CAST`.",
    "ISO-8601 strings (`YYYY-MM-DD`, `YYYY-MM-DD HH:MI:SS`, `HH:MI:SS`) require no format",
    "model and behave identically across engines, making them the portable default.",
    "Pattern wildcards for `LIKE`:",
    "- `%` \\- Match any sequence of characters",
    "- `_` \\- Match any single character",
    "All standard aggregation functions can be used as window functions:",
    "Supported target types:",
})
#: Every line of a REQUIRED or RECOMMENDED section's code blocks, as the vendored spec writes it.
KNOWN_CODE = frozenset({
    "-- Approximate distinct count (much faster than COUNT(DISTINCT) on large data)",
    "APPROX_COUNT_DISTINCT(customer_id)", "-- Approximate median", "APPROX_PERCENTILE(amount, 0.5)",
    "-- Approximate 95th percentile", "APPROX_PERCENTILE(response_time, 0.95)",
    "-- DISTINCT modifier", "SUM(DISTINCT amount)", "COUNT(DISTINCT customer_id)",
    "-- Filtered aggregation via CASE", "SUM(CASE WHEN status = 'completed' THEN amount END)",
    "COUNT(CASE WHEN status = 'completed' THEN 1 END)", "-- Filtered aggregation via FILTER (WHERE ...)",
    "SUM(amount) FILTER (WHERE status = 'completed')", "COUNT(*) FILTER (WHERE status = 'completed')",
    "<aggregate_function>(<args>) FILTER (WHERE <predicate>)",
    "SUM(amount) FILTER (WHERE status = 'completed')                  --> "
    "SUM(CASE WHEN status = 'completed' THEN amount END)",
    "COUNT(*) FILTER (WHERE status = 'completed')                     --> "
    "COUNT(CASE WHEN status = 'completed' THEN 1 END)",
    "COUNT(DISTINCT customer_id) FILTER (WHERE status = 'completed')  --> "
    "COUNT(DISTINCT CASE WHEN status = 'completed' THEN customer_id END)",
    "-- EXTRACT function (SQL standard)", "EXTRACT(YEAR FROM date_expr)", "EXTRACT(MONTH FROM date_expr)",
    "EXTRACT(DAY FROM date_expr)", "-- DATE_PART function (common alternative)", "DATE_PART('year', date_expr)",
    "DATE_PART('month', date_expr)", "DATE_PART('day', date_expr)",
    "-- Examples", "DATE_TRUNC('month', order_date)    -- First day of month",
    "DATE_TRUNC('quarter', order_date)  -- First day of quarter",
    "DATE_TRUNC('week', order_date)     -- First day of week (Monday)",
    "-- Add/subtract intervals", "DATEADD(day, 7, order_date)         -- Add 7 days",
    "DATEADD(month, -1, order_date)      -- Subtract 1 month", "DATEADD(year, 1, order_date)        -- Add 1 year",
    "-- Calculate differences", "DATEDIFF(day, start_date, end_date)    -- Days between dates",
    "DATEDIFF(month, start_date, end_date)  -- Months between dates",
    "DATEDIFF(year, start_date, end_date)   -- Years between dates",
    "-- Searched CASE", "CASE", "WHEN condition1 THEN result1", "WHEN condition2 THEN result2",
    "ELSE default_result", "END", "-- Simple CASE", "CASE expression", "WHEN value1 THEN result1",
    "WHEN value2 THEN result2",
    "-- Running total", "SUM(amount) OVER (ORDER BY order_date)", "-- Running average",
    "AVG(amount) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)", "-- Partition totals",
    "SUM(amount) OVER (PARTITION BY region)", "-- Percent of total", "amount / SUM(amount) OVER () * 100",
    "CAST(expression AS target_type)", "TRY_CAST(expression AS target_type)  -- Returns NULL on failure",
})


class Section(BaseModel):
    model_config = ConfigDict(frozen=True)

    heading: str
    lines: tuple[str, ...]

    @property
    def marked(self) -> bool:
        return any(mark in self.heading for mark in MARKS)

    @property
    def name(self) -> str:
        out = self.heading
        for mark in MARKS:
            out = out.replace(mark, "")
        return out.strip().upper()


def spec_path(root: Path | None = None) -> Path:
    return (root or corpus_root()) / "core-spec" / "expression_language.md"


def spec_text(root: Path | None = None) -> str:
    return spec_path(root).read_text(encoding="utf-8")


def _plain_heading(text: str) -> str:
    return text.replace("\\", "").replace("`", "").strip()


def spec_sections(text: str) -> list[Section]:
    """Every heading at any depth with the lines up to the next heading."""
    out: list[Section] = []
    heading: str | None = None
    body: list[str] = []
    fenced = False
    for line in text.splitlines():
        fenced ^= line.strip().startswith(_FENCE)
        match = None if fenced or line.strip().startswith(_FENCE) else _HEADING.match(line)
        if match is None:
            body.append(line)
            continue
        if heading is not None:
            out.append(Section(heading=heading, lines=tuple(body)))
        heading, body = _plain_heading(match.group(2)), []
    if heading is not None:
        out.append(Section(heading=heading, lines=tuple(body)))
    return out


def checked_sections(text: str) -> list[Section]:
    """The REQUIRED and RECOMMENDED sections."""
    return [s for s in spec_sections(text) if s.marked]


def _table_cells(lines: Iterable[str]) -> Iterator[list[str]]:
    rows = [line for line in lines if line.startswith("|")]
    for row in rows[2:]:  # header and alignment rows
        yield [c.strip() for c in re.split(r"(?<!\\)\|", row)[1:-1]]


def _spans(cell: str) -> list[str]:
    return [s.replace("\\|", "|").upper() for s in _CODE.findall(cell)]


def _bullet_spans(lines: Iterable[str]) -> list[str]:
    return [s for line in lines if line.startswith("- `") for s in _spans(line.split("\\-")[0])]


def spec_constructs(section: Section) -> set[str]:
    """The section's table names (else its Syntax column), listed parts, types or frame clauses, else code comments,
    else its heading."""
    out = {n for cells in _table_cells(section.lines) for n in (_spans(cells[0]) or _spans(cells[1]))}
    if section.heading.startswith("Alternative Extraction"):
        out |= {f"{fn}({part})" for part in _bullet_spans(section.lines) for fn in ("EXTRACT", "DATE_PART")}
    elif section.heading.startswith("Date Truncation"):
        parts = next(line for line in section.lines if line.startswith("Supported parts:"))
        out |= {f"DATE_TRUNC({p.strip(chr(39))})" for p in _spans(parts)}
    elif section.heading.startswith("CAST"):
        out |= {f"CAST AS {t}" for t in _bullet_spans(section.lines)}
    elif section.heading == "Syntax":
        # Every listed frame clause, whatever its keyword; before the list, only the NULL placements.
        frames = next((i for i, line in enumerate(section.lines) if line.strip() == "Frame clause options:"),
                      len(section.lines))
        out |= set(_bullet_spans(section.lines[frames + 1:]))
        out |= {s for s in _bullet_spans(section.lines[:frames]) if s.startswith("NULLS ")}
    if not out:
        out = {line.strip()[3:].upper() for line in section.lines if line.strip().startswith("-- ")}
    return out or {section.name}


def unclassified_lines(section: Section) -> list[str]:
    """Lines of a REQUIRED or RECOMMENDED section that are no table row, known code, construct list or known
    prose."""
    out: list[str] = []
    fenced = False
    lists_parts = section.heading.startswith(_PART_LISTS)
    for raw in section.lines:
        line = raw.strip()
        if line.startswith(_FENCE):
            fenced = not fenced
            continue
        if fenced:
            if line and line not in KNOWN_CODE:
                out.append(line)
            continue
        if not line or line == "---" or line.startswith("|") or line in KNOWN_PROSE:
            continue
        if line.startswith("Supported parts:") and section.heading.startswith("Date Truncation"):
            continue
        if lists_parts and line.startswith("- `"):
            continue
        out.append(line)
    return out


def completeness_problems(text: str, *, forms: dict[str, set[str]], prose_sections: frozenset[str]) -> list[str]:
    """Why ``forms`` (section → row forms) does not cover the spec ``text`` exactly, both ways."""
    sections = spec_sections(text)
    headings = {s.heading for s in sections}
    checked = [s for s in sections if s.marked or s.heading in UNMARKED_FORMS]
    problems = [f"unclassified line in {s.heading!r}: {line}" for s in checked if s.marked
                for line in unclassified_lines(s)]
    problems += [f"section without rows: {s.heading}" for s in checked if s.heading not in forms]
    problems += [f"rows for an unknown section: {h}" for h in sorted(set(forms) - headings - prose_sections)]
    problems += [f"rows for an EXPERIMENTAL section: {h}" for h in sorted(forms) if "(EXPERIMENTAL)" in h]
    for section in checked:
        rows = forms.get(section.heading, set())
        expected = spec_constructs(section)
        problems += [f"{section.heading}: no row for {c}" for c in sorted(expected - rows)]
        problems += [f"{section.heading}: row names no spec construct: {c}" for c in sorted(rows - expected)]
    return problems


def spec_syntaxes(name: str, text: str) -> list[str]:
    """``name``'s Syntax column in a REQUIRED or RECOMMENDED table, else its calls in the spec's code blocks."""
    out = [syntax for s in checked_sections(text) for cells in _table_cells(s.lines)
           if name.upper() in _spans(cells[0]) for syntax in _spans(cells[1])]
    return out or [line.strip().upper() for line in text.splitlines()
                   if line.strip().upper().startswith(f"{name.upper()}(")]


def argument_counts(syntax: str) -> tuple[int, bool] | None:
    """A call syntax's argument count and whether it ends in ``...``; ``None`` when it is no call."""
    if _CALLEE.match(syntax) is None:
        return None
    depth, quoted, parts, current = 0, False, [], ""
    for ch in syntax[syntax.index("(") + 1:]:
        quoted ^= ch == "'"
        depth += 0 if quoted else (ch == "(") - (ch == ")")
        if depth < 0:
            break
        if ch == "," and depth == 0 and not quoted:
            parts.append(current.strip())
            current = ""
        else:
            current += ch
    parts = [p for p in (*parts, current.strip()) if p]
    return (len(parts) - 1, True) if parts and parts[-1] == "..." else (len(parts), False)


def expected_ossie_counts(name: str, text: str) -> frozenset[int]:
    """The spec's counts SLayer accepts; a variadic one (``f(a, b, ...)`` with no SLayer maximum) stores its least
    count, every larger count being Ossie's too."""
    spec = SCALAR_SPECS[name]
    low, high = spec.min_args, spec.max_args
    out: set[int] = set()
    for counted in filter(None, map(argument_counts, spec_syntaxes(name, text))):
        count, variadic = counted
        if not variadic:
            out |= {count} if low <= count <= (count if high is None else high) else set()
        elif high is None:
            out.add(max(count, low))
        else:
            out |= set(range(max(count, low), high + 1))
    return frozenset(out)
