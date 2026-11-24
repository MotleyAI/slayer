"""Every REQUIRED construct of the vendored Ossie expression spec is admitted with Ossie's value or a tracked gap."""

from __future__ import annotations

import datetime
import re
from collections.abc import AsyncGenerator, Iterator
from decimal import Decimal
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from slayer.core.errors import SlayerError
from slayer.core.keys import SCALAR_SPECS
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2215_fixtures import BACKENDS, dev2215_engine, q
from tests._dev2217_fixtures import strict, strict_error
from tests._dev2446_required_manifest import GAP, PROSE, ROWS, Row
from tests._dev2446_strict_fixtures import counting_engine
from tests._dev2243_fixtures import CORPUS

SPEC = CORPUS / "core-spec" / "expression_language.md"
_HEADING = re.compile(r"^#{2,4} (.+?)\s*$")
_CODE = re.compile(r"`([^`]+)`")
_CALLEE = re.compile(r"^([A-Z_][A-Z0-9_]*)\(")


class Section(BaseModel):
    model_config = ConfigDict(frozen=True)

    heading: str
    lines: tuple[str, ...]


def _plain_heading(text: str) -> str:
    return text.replace("\\", "").replace("`", "").strip()


def spec_sections() -> list[Section]:
    out: list[Section] = []
    heading: str | None = None
    body: list[str] = []
    for line in SPEC.read_text(encoding="utf-8").splitlines():
        match = _HEADING.match(line)
        if match is None:
            body.append(line)
            continue
        if heading is not None:
            out.append(Section(heading=heading, lines=tuple(body)))
        heading, body = _plain_heading(match.group(1)), []
    if heading is not None:
        out.append(Section(heading=heading, lines=tuple(body)))
    return out


REQUIRED = [s for s in spec_sections() if "(REQUIRED)" in s.heading]


def _table_cells(lines: tuple[str, ...]) -> Iterator[list[str]]:
    rows = [line for line in lines if line.startswith("|")]
    for row in rows[2:]:  # header and alignment rows
        yield [c.strip() for c in re.split(r"(?<!\\)\|", row)[1:-1]]


def _spans(cell: str) -> list[str]:
    return [s.replace("\\|", "|").upper() for s in _CODE.findall(cell)]


def _bullet_spans(lines: tuple[str, ...]) -> list[str]:
    return [s for line in lines if line.startswith("- `") for s in _spans(line.split("\\-")[0])]


def spec_constructs(section: Section) -> set[str]:
    """The section's table names (else its Syntax column), listed parts or types, else code comments, else heading."""
    out = {n for cells in _table_cells(section.lines) for n in (_spans(cells[0]) or _spans(cells[1]))}
    if section.heading.startswith("Alternative Extraction"):
        out |= {f"{fn}({part})" for part in _bullet_spans(section.lines) for fn in ("EXTRACT", "DATE_PART")}
    elif section.heading.startswith("Date Truncation"):
        parts = next(line for line in section.lines if line.startswith("Supported parts:"))
        out |= {f"DATE_TRUNC({p.strip(chr(39))})" for p in _spans(parts)}
    elif section.heading.startswith("CAST"):
        out |= {f"CAST AS {t}" for t in _bullet_spans(section.lines)}
    if not out:
        out = {line.strip()[3:].upper() for line in section.lines if line.strip().startswith("-- ")}
    return out or {section.heading.replace("(REQUIRED)", "").strip().upper()}


def spec_syntaxes(name: str) -> list[str]:
    """``name``'s Syntax column in a REQUIRED or RECOMMENDED table, else its calls in the spec's code blocks."""
    out = [syntax for s in spec_sections() if "REQUIRED)" in s.heading or "RECOMMENDED)" in s.heading
           for cells in _table_cells(s.lines) if name.upper() in _spans(cells[0])
           for syntax in _spans(cells[1])]
    return out or [line.strip().upper() for line in SPEC.read_text(encoding="utf-8").splitlines()
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


def expected_ossie_counts(name: str) -> frozenset[int]:
    """The spec's counts SLayer accepts; a variadic one (``f(a, b, ...)`` with no SLayer maximum) stores its least
    count, every larger count being Ossie's too."""
    spec = SCALAR_SPECS[name]
    low, high = spec.min_args, spec.max_args
    out: set[int] = set()
    for counted in filter(None, map(argument_counts, spec_syntaxes(name))):
        count, variadic = counted
        if not variadic:
            out |= {count} if low <= count <= (count if high is None else high) else set()
        elif high is None:
            out.add(max(count, low))
        else:
            out |= set(range(max(count, low), high + 1))
    return frozenset(out)


def _row_id(row: Row) -> str:
    return f"{row.form}|{row.expression}"


ADMITTED_SCALARS = [r for r in ROWS if r.gap is None and r.kind == "scalar"]
ADMITTED_AGGREGATES = [r for r in ROWS if r.gap is None and r.kind == "aggregate"]
RESERVED = [r for r in ROWS if r.kind == "reserved"]


def test_every_required_heading_has_rows() -> None:
    sections = {r.section for r in ROWS}
    assert {s.heading for s in REQUIRED} - sections == set()
    assert sections - {s.heading for s in spec_sections()} - PROSE == set()


@pytest.mark.parametrize("section", REQUIRED, ids=lambda s: s.heading)
def test_every_required_construct_has_a_row(section: Section) -> None:
    rows = {r.form for r in ROWS if r.section == section.heading}
    assert spec_constructs(section) - rows == set()
    assert rows - spec_constructs(section) == set()


def test_gaps_name_their_tracking_issue() -> None:
    assert {r.gap for r in ROWS} == {None, GAP}


@pytest.mark.parametrize("name", sorted(SCALAR_SPECS))
def test_ossie_arg_counts_match_the_spec_syntax(name: str) -> None:
    assert SCALAR_SPECS[name].ossie_arg_counts == expected_ossie_counts(name)


def _plain(value: Any) -> Any:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, datetime.date):
        return str(value)
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return value


def assert_ossie_value(got: Any, want: Any) -> None:
    got, want = _plain(got), _plain(want)
    if isinstance(want, float) and isinstance(got, float):
        assert got == pytest.approx(want)
    else:
        assert got == want


@pytest.fixture(scope="module", params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as e:
        yield e


@pytest.fixture(scope="module")
async def strict_engine() -> AsyncGenerator[SlayerQueryEngine]:
    async with counting_engine("duckdb") as e:
        yield e


@pytest.mark.parametrize("row", ADMITTED_SCALARS, ids=_row_id)
async def test_admitted_scalar_has_ossies_value(engine: SlayerQueryEngine, row: Row) -> None:
    response = await engine.execute(q(dimensions=[{"field": row.expression, "alias": "v"}], filter="orders.id = 3"))
    assert len(response.data) == 1, response.data
    assert_ossie_value(response.data[0]["v"], row.value)


@pytest.mark.parametrize("row", ADMITTED_AGGREGATES, ids=_row_id)
async def test_admitted_aggregate_has_ossies_value(engine: SlayerQueryEngine, row: Row) -> None:
    response = await engine.execute(q(metrics=[{"metric": row.expression, "alias": "v"}]))
    assert len(response.data) == 1, response.data
    assert_ossie_value(response.data[0]["v"], row.value)


@pytest.mark.parametrize("row", RESERVED, ids=_row_id)
async def test_a_reserved_construct_is_refused(engine: SlayerQueryEngine, row: Row) -> None:
    query = q(dimensions=[{"field": row.expression, "alias": "v"}], filter="orders.id = 3")
    with pytest.raises(SlayerError):
        await engine.execute(query)


def _strict_code(row: Row) -> str | None:
    """``None``: the strict filter answers."""
    if row.kind == "window":
        return "E_WINDOW_IN_WHERE"
    if row.gap is not None or row.kind == "reserved":
        return "E_INVALID_QUERY"
    return "E_AGGREGATE_IN_WHERE" if row.kind == "aggregate" else None


@pytest.mark.parametrize("row", ROWS, ids=_row_id)
async def test_strict_judgement(strict_engine: SlayerQueryEngine, row: Row) -> None:
    query = {"metrics": ["orders.n"], "filter": f"orders.id = 3 AND ({row.expression}) IS NOT NULL"}
    code = _strict_code(row)
    if code is not None:
        await strict_error(strict_engine, query, code)
        return
    response = await strict(strict_engine, query)
    assert response.data == [{"n": 0 if row.value is None else 1}]
