"""The Ossie compliance page lists exactly the pinned gaps and cites the vendored Ossie revision."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from tests._dev2220_compliance_gaps import GAPS, REVISION
from tests._dev2243_fixtures import CORPUS
from tests._dev2446_required_manifest import ROWS

PAGE = Path(__file__).resolve().parent.parent / "docs" / "interfaces" / "ossie-compliance.md"
#: A gap row: a backticked case id first, the fixing issue last.
_GAP_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|.*\|\s*(DEV-\d+)\s*\|\s*$")


def page_gaps(text: str) -> set[tuple[str, str]]:
    return {(m.group(1), m.group(2)) for line in text.splitlines() if (m := _GAP_ROW.match(line))}


def pinned_gaps() -> set[tuple[str, str]]:
    layer1 = {(r.form, r.gap) for r in ROWS if r.gap is not None}
    return layer1 | {(g.case, g.issue) for g in GAPS}


def test_the_page_lists_exactly_the_pinned_gaps() -> None:
    listed = page_gaps(PAGE.read_text(encoding="utf-8"))
    pinned = pinned_gaps()
    assert sorted(listed - pinned) == [], "page rows without a pinned case"
    assert sorted(pinned - listed) == [], "pinned cases without a page row"


def test_page_rows_are_read() -> None:
    text = "| `NVL` | `NVL(a, b)` | DEV-2282 |\n| `x` | no issue |\n| case | prose | DEV-1 |"
    assert page_gaps(text) == {("NVL", "DEV-2282")}


def test_the_page_and_the_cases_cite_the_vendored_revision() -> None:
    commit = yaml.safe_load((CORPUS / "provenance.yaml").read_text(encoding="utf-8"))["commit"]
    assert commit.startswith(REVISION), "re-vendored: review the compliance claims and gap cases, then bump REVISION"
    assert REVISION in PAGE.read_text(encoding="utf-8")
