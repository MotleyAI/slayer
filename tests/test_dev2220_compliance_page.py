"""Spec: compatibility/ossie-compliance › The compliance page states what the gate proves.

Its lists, per-engine Layer 1 counts and claims equal the gate's declarations, and it cites the vendored revision.
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import yaml

from tests._dev2220_compliance_gaps import GAPS, REVISION
from tests._dev2446_required_manifest import ROWS
from tests._ossie_gate import DISPLAY, TIER1, VENDORED
from tests._ossie_gate_suite import case_state, cases

PAGE = Path(__file__).resolve().parent.parent / "docs" / "interfaces" / "ossie-compliance.md"
CAVEAT = "Expressions inside a query are subject to the listed Layer 1 gaps."
#: A listed row: a backticked construct or case first, its status last.
_LISTED = re.compile(r"^\|\s*`([^`]+)`\s*\|.*\|\s*(planned|refused)\s*\|\s*$")
_COUNTS = re.compile(r"^\|\s*([^|]+?)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*$")


def page_text() -> str:
    return PAGE.read_text(encoding="utf-8")


def listed(text: str, status: str) -> set[str]:
    return {m.group(1) for line in text.splitlines() if (m := _LISTED.match(line)) and m.group(2) == status}


def page_counts(text: str) -> dict[str, tuple[int, int, int]]:
    """Database → (pass, refused, gap) from the Layer 1 counts table."""
    names = set(DISPLAY.values())
    return {m.group(1): (int(m.group(2)), int(m.group(3)), int(m.group(4)))
            for line in text.splitlines() if (m := _COUNTS.match(line)) and m.group(1) in names}


def summary_row(text: str, layer: str) -> list[str]:
    """The summary table's cells for ``layer`` (``1.`` / ``3.``)."""
    row = next(line for line in text.splitlines() if line.startswith(f"| {layer} "))
    return [c.strip() for c in row.strip().strip("|").split("|")]


def declared_counts(engine: str) -> tuple[int, int, int]:
    states = Counter(r.state(engine).state for r in ROWS)
    return states["pass"], states["refused"], states["gap"]


def full_layer1() -> bool:
    return all(r.state(e).state == "pass" for r in ROWS if r.required for e in TIER1)


def test_the_page_lists_exactly_the_gaps() -> None:
    gaps = {r.form for r in ROWS for s in r.states if s.state == "gap"} | {g.case for g in GAPS}
    page = listed(page_text(), "planned")
    assert sorted(page - gaps) == [], "page rows without a declared gap"
    assert sorted(gaps - page) == [], "declared gaps without a page row"


def test_the_page_lists_exactly_the_refusals() -> None:
    refusals = {r.form for r in ROWS for s in r.states if s.state == "refused"}
    assert listed(page_text(), "refused") == refusals


def test_the_page_counts_each_engines_layer1_cells() -> None:
    assert page_counts(page_text()) == {DISPLAY[e]: declared_counts(e) for e in TIER1}


def test_layer3_is_claimed_on_every_tier1_database() -> None:
    """The Layer 3 claim names every database."""
    assert all(case_state(c.name, e).state == "pass" for c in cases() for e in TIER1)
    certified = {name.strip() for name in summary_row(page_text(), "3.")[2].split(",")}
    assert certified == set(DISPLAY.values())
    assert CAVEAT in page_text()


def test_the_full_layer1_claim_follows_the_gaps() -> None:
    """Full Layer 1 is claimed if and only if no REQUIRED cell is a gap or a refusal."""
    claim = summary_row(page_text(), "1.")[1].lower()
    assert ("full" in claim) == full_layer1()


def test_page_rows_are_read() -> None:
    text = ("| `NVL` | `NVL(a, b)` | planned |\n| `RADIANS` | `RADIANS(180)` | refused |\n| `x` | supported |\n"
            "| case | prose | planned |\n| SQLite | 3 | 1 | 2 |\n| Other | 1 | 1 | 1 |")
    assert listed(text, "planned") == {"NVL"}
    assert listed(text, "refused") == {"RADIANS"}
    assert page_counts(text) == {"SQLite": (3, 1, 2)}


def test_the_page_and_the_cases_cite_the_vendored_revision() -> None:
    commit = yaml.safe_load((VENDORED / "provenance.yaml").read_text(encoding="utf-8"))["commit"]
    assert commit.startswith(REVISION), "re-vendored: review the compliance claims and gap cases, then bump REVISION"
    assert REVISION in page_text()
