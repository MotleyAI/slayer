"""The Layer 1 table covers every REQUIRED and RECOMMENDED construct of Ossie's expression spec, both ways, and the
strict filter admits exactly the constructs that answer. Execution per engine is ``tests/test_ossie_gate.py``."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import AsyncGenerator

import pytest

from slayer.core.keys import SCALAR_SPECS
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import strict, strict_error
from tests._dev2446_required_manifest import FAMILIES, PROSE, REQUIRED_REFUSALS, ROWS, Row
from tests._dev2446_strict_fixtures import counting_engine
from tests._ossie_gate import TIER1, declaration_problems
from tests._ossie_gate_spec import (
    Section, checked_sections, completeness_problems, expected_ossie_counts, spec_constructs, spec_sections,
    spec_text,
)

SPEC = spec_text()


def row_forms() -> dict[str, set[str]]:
    out: dict[str, set[str]] = defaultdict(set)
    for row in ROWS:
        out[row.section].add(row.form)
    return dict(out)


def problems(text: str) -> list[str]:
    return completeness_problems(text, forms=row_forms(), prose_sections=PROSE)


def test_the_table_covers_the_spec_both_ways() -> None:
    assert problems(SPEC) == []


@pytest.mark.parametrize("section", checked_sections(SPEC), ids=lambda s: s.heading)
def test_every_checked_construct_has_a_row(section: Section) -> None:
    rows = row_forms().get(section.heading, set())
    assert spec_constructs(section) - rows == set()
    assert rows - spec_constructs(section) == set()


def test_recommended_sections_are_checked() -> None:
    trig = next(s for s in spec_sections(SPEC) if s.heading == "Trigonometric (RECOMMENDED)")
    assert spec_constructs(trig) == {"SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2", "RADIANS", "DEGREES", "PI"}
    assert {"SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2", "RADIANS", "DEGREES", "PI"} <= row_forms()[trig.heading]


def test_frame_clauses_are_checked() -> None:
    syntax = next(s for s in spec_sections(SPEC) if s.heading == "Syntax")
    assert {"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", "ROWS BETWEEN N PRECEDING AND N FOLLOWING",
            "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"} <= spec_constructs(syntax)


def test_a_new_table_row_fails() -> None:
    mutated = SPEC.replace("| `LEAST` | `LEAST(x, y, ...)` | Minimum of arguments |",
                           "| `LEAST` | `LEAST(x, y, ...)` | Minimum of arguments |\n"
                           "| `MIDDLE` | `MIDDLE(x, y, z)` | Middle of three |")
    assert mutated != SPEC
    assert any("MIDDLE" in p for p in problems(mutated))


def test_a_new_heading_at_any_depth_fails() -> None:
    mutated = SPEC.replace("### Trigonometric (RECOMMENDED)",
                           "##### Bitwise (REQUIRED)\n\n| Function | Syntax | Description |\n| :---- | :---- | :---- |\n"
                           "| `BIT_AND` | `BIT_AND(x, y)` | Bitwise and |\n\n### Trigonometric (RECOMMENDED)")
    assert mutated != SPEC
    assert "section without rows: Bitwise (REQUIRED)" in problems(mutated)


def test_a_new_list_item_fails() -> None:
    mutated = SPEC.replace("- `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`",
                           "- `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`\n- `NANOSECOND`")
    assert mutated != SPEC
    assert any("EXTRACT(NANOSECOND)" in p for p in problems(mutated))


def test_an_unrecognised_line_fails_closed() -> None:
    mutated = SPEC.replace("Pattern wildcards for `LIKE`:", "Pattern wildcards for `LIKE`:\n\nAlso `[abc]` sets.")
    assert mutated != SPEC
    assert any("Also `[abc]` sets." in p for p in problems(mutated))


def test_a_new_code_line_fails_closed() -> None:
    mutated = SPEC.replace("SUM(DISTINCT amount)\n", "SUM(DISTINCT amount)\nAVG(DISTINCT amount)\n")
    assert mutated != SPEC
    assert any("AVG(DISTINCT amount)" in p for p in problems(mutated))


def test_an_experimental_section_has_no_rows() -> None:
    assert not any("(EXPERIMENTAL)" in section for section in row_forms())


def test_gaps_name_a_family_issue() -> None:
    issues = {s.issue for r in ROWS for s in r.states if s.state == "gap"}
    assert issues <= FAMILIES


def test_a_required_construct_is_refused_only_without_a_translation() -> None:
    refusals = {(r.form, e) for r in ROWS if r.required for e in TIER1 if r.state(e).state == "refused"}
    assert refusals == REQUIRED_REFUSALS


def test_every_row_has_one_state_per_engine() -> None:
    declared = {r.id: r.states for r in ROWS}
    assert len(declared) == len(ROWS), "duplicate row ids"
    assert declaration_problems(declared, declared) == []
    assert all(r.state(e) for r in ROWS for e in TIER1)


@pytest.mark.parametrize("name", sorted(SCALAR_SPECS))
def test_ossie_arg_counts_match_the_spec_syntax(name: str) -> None:
    assert SCALAR_SPECS[name].ossie_arg_counts == expected_ossie_counts(name, SPEC)


def test_a_new_argument_form_changes_the_counts() -> None:
    mutated = SPEC.replace("| `LOG` | `LOG(base, x)` |", "| `LOG` | `LOG(base, x, y)` |")
    assert mutated != SPEC
    assert expected_ossie_counts("log", mutated) != SCALAR_SPECS["log"].ossie_arg_counts


@pytest.fixture(scope="module")
async def strict_engine() -> AsyncGenerator[SlayerQueryEngine]:
    async with counting_engine("duckdb") as e:
        yield e


def _strict_code(row: Row) -> str | None:
    """``None``: the strict filter answers. It refuses a construct with a gap anywhere or refused everywhere; one
    database's missing translation is that database's refusal."""
    if row.kind == "window":
        return "E_WINDOW_IN_WHERE"
    gap = any(s.state == "gap" for s in row.states)
    if row.kind == "reserved" or gap or all(row.state(e).state == "refused" for e in TIER1):
        return "E_INVALID_QUERY"
    return "E_AGGREGATE_IN_WHERE" if row.kind == "aggregate" else None


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r.id)
async def test_strict_judgement(strict_engine: SlayerQueryEngine, row: Row) -> None:
    query = {"metrics": ["orders.n"], "filter": f"orders.id = 3 AND ({row.expression}) IS NOT NULL"}
    code = _strict_code(row)
    if code is not None:
        await strict_error(strict_engine, query, code)
        return
    response = await strict(strict_engine, query)
    assert response.data == [{"n": 0 if row.value is None else 1}]
