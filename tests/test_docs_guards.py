"""Docs-site guards: every page is in the nav, no issue numbers, every Mode-B example parses."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

from slayer.core.syntax import parse_expr

from tests._doc_examples import SKIP_MARKER, extract_markdown, extract_yaml_file, mode_b_values

_ROOT = Path(__file__).resolve().parents[1]
_DOCS = _ROOT / "docs"
_HELP = _ROOT / "slayer" / "memories" / "help_content"

#: Pages deliberately outside the nav.
NAV_EXEMPT = frozenset({
    "CLAUDE.md",
    "api_gaps.md",
    "getting-started/flight-sql.md",  # DEV-2103 adds it to the nav
})
#: Issue numbers DEV-2172 removes from the SQL API page; delete this baseline in the merge that brings it in.
ISSUE_NUMBER_BASELINE = frozenset({
    ("interfaces/pg-facade.md", token)
    for token in ("DEV-1562", "DEV-1565", "DEV-1566", "DEV-1567", "DEV-1568", "DEV-1569", "DEV-1572")
})
_ISSUE_NUMBER = re.compile(rb"\bDEV-\d+\b", re.IGNORECASE)


def _rel(path: Path) -> str:
    return path.relative_to(_DOCS).as_posix()


# --------------------------------------------------------------------------- #
# Nav coverage
# --------------------------------------------------------------------------- #
def _nav_paths(node: Any) -> set[str]:
    if isinstance(node, str):
        return {node}
    values = node.values() if isinstance(node, dict) else node
    return set().union(*(_nav_paths(v) for v in values))


def _nav() -> set[str]:
    return _nav_paths(tomllib.loads((_ROOT / "zensical.toml").read_text())["project"]["nav"])


def test_every_docs_page_is_in_the_nav() -> None:
    pages = {_rel(p) for p in _DOCS.rglob("*") if p.suffix in (".md", ".ipynb")}
    assert sorted(pages - _nav() - NAV_EXEMPT) == []


def test_every_nav_entry_exists() -> None:
    assert sorted(p for p in _nav() if not (_DOCS / p).is_file()) == []


def test_nav_exemptions_exist_and_stay_out_of_the_nav() -> None:
    assert all((_DOCS / p).is_file() for p in NAV_EXEMPT)
    assert NAV_EXEMPT & _nav() == set()


# --------------------------------------------------------------------------- #
# No issue numbers
# --------------------------------------------------------------------------- #
def test_no_issue_numbers_in_docs() -> None:
    found = {
        (_rel(path), token.decode().upper())
        for path in _DOCS.rglob("*")
        if path.is_file() and _rel(path) != "CLAUDE.md"
        for token in _ISSUE_NUMBER.findall(path.read_bytes())
    }
    assert found == ISSUE_NUMBER_BASELINE


# --------------------------------------------------------------------------- #
# Every Mode-B example parses
# --------------------------------------------------------------------------- #
def _doc_sources() -> list[Path]:
    yamls = [p for p in (_DOCS / "examples").rglob("*") if p.suffix in (".yaml", ".yml")]
    return sorted(_DOCS.rglob("*.md")) + sorted(_HELP.glob("*.md")) + sorted(yamls)


def test_every_doc_example_parses() -> None:
    problems: list[str] = []
    for path in _doc_sources():
        rel = path.relative_to(_ROOT).as_posix()
        reader = extract_markdown if path.suffix == ".md" else extract_yaml_file
        extraction = reader(path.read_text(), path=rel)
        problems += [f"{m.path}:{m.line}: unreadable block: {m.error}" for m in extraction.malformed]
        for snippet in extraction.snippets:
            try:
                parse_expr(snippet.text)
            except Exception as exc:  # noqa: BLE001 — every parse failure is a finding
                problems.append(f"{snippet.path}:{snippet.line}: {snippet.text!r}: {exc}")
    assert problems == []


def test_the_guard_reads_a_meaningful_number_of_examples() -> None:
    count = sum(
        len((extract_markdown if p.suffix == ".md" else extract_yaml_file)(p.read_text(), path=str(p)).snippets)
        for p in _doc_sources()
    )
    assert count >= 250


# --------------------------------------------------------------------------- #
# Extractor fixtures
# --------------------------------------------------------------------------- #
def _texts(markdown: str) -> list[str]:
    extraction = extract_markdown(markdown, path="page.md")
    assert extraction.malformed == []
    return [s.text for s in extraction.snippets]


def test_mixed_sql_and_query_fences() -> None:
    page = (
        "Run this:\n\n```sql\nSELECT sum(amount) FROM orders WHERE status = 'paid'\n```\n\n"
        '```json\n{"source_model": "orders", "measures": ["sum(amount)"], "filters": ["status = \'paid\'"]}\n```\n'
        "\n```bash\nslayer query '{\"measures\": [\"count(*)\"]}'\n```\n"
    )
    assert _texts(page) == ["sum(amount)", "status = 'paid'"]


def test_multiline_lists_fragments_json_lines_and_comments() -> None:
    page = (
        "```json\n"
        '"measures": [\n'
        '  "count(*)",\n'
        '  {"formula": "sum(amount) / count(*)", "name": "aov"}\n'
        "]\n"
        "```\n\n"
        "```json\n"
        "// two queries\n"
        '{"source_model": "orders", "dimensions": ["status"]}  // by status\n'
        '{"source_model": "orders", "time_dimensions": ["month(order_date)"]}\n'
        "```\n"
    )
    assert _texts(page) == ["count(*)", "sum(amount) / count(*)", "status", "month(order_date)"]


def test_every_query_field_is_read() -> None:
    query = {
        "source_model": "orders",
        "measures": ["count(*)", {"formula": "sum(amount)", "name": "total"}, {"name": "revenue"}],
        "dimensions": ["status", {"expression": "upper(status)", "name": "s"}, {"name": "status", "label": "S"}],
        "time_dimensions": ["month(order_date)", {"dimension": "order_date", "granularity": "week"}],
        "filters": ["amount > 10"],
        "order": [{"column": "sum(amount)", "direction": "desc"}],
        "variables": {"v": "x"},
    }
    assert mode_b_values(query) == [
        "count(*)", "sum(amount)", "status", "upper(status)", "month(order_date)", "order_date",
        "amount > 10", "sum(amount)",
    ]


def test_stage_lists_and_refinements() -> None:
    stages = [
        {"name": "per_customer", "source_model": "orders", "dimensions": ["customers.name"],
         "measures": [{"formula": "sum(amount)", "name": "total"}]},
        {"source_model": "per_customer", "measures": ["avg(total)"], "filters": ["total > 100"]},
    ]
    call = {"query": "monthly_revenue", "refine": {"dimensions": ["region"], "filters": ["region = 'EU'"]}}
    assert mode_b_values(stages) == ["customers.name", "sum(amount)", "avg(total)", "total > 100"]
    assert mode_b_values(call) == ["region", "region = 'EU'"]


def test_models_keep_only_measure_formulas() -> None:
    model = {
        "name": "orders", "sql_table": "orders", "data_source": "db",
        "filters": ["deleted_at IS NULL"],
        "columns": [{"name": "net", "sql": "amount - tax", "filter": "status = 'paid'", "type": "DOUBLE"}],
        "measures": [{"name": "aov", "formula": "sum(amount) / count(*)"}],
        "aggregations": [{"name": "wavg", "formula": "SUM({value} * {weight}) / SUM({weight})"}],
        "source_queries": [{"source_model": "raw", "filters": ["amount > 0"]}],
    }
    assert mode_b_values(model) == ["sum(amount) / count(*)", "amount > 0"]


def test_inline_extension_and_standalone_objects() -> None:
    query = {"source_model": {"source_name": "orders", "columns": [{"name": "big", "sql": "amount > 100"}]},
             "measures": ["count(*)"]}
    assert mode_b_values(query) == ["count(*)"]
    assert mode_b_values({"formula": "cumsum(sum(revenue))"}) == ["cumsum(sum(revenue))"]
    assert mode_b_values({"dimension": "created_at", "granularity": "month", "date_range": "2025"}) == ["created_at"]
    assert mode_b_values({"name": "wavg", "formula": "SUM({value})", "params": [{"name": "w", "sql": "1"}]}) == []
    assert mode_b_values({"orders.status": "paid", "orders._count": 3}) == []


def test_yaml_fences_and_files() -> None:
    page = "```yaml\nsource_model: orders\nmeasures:\n  - sum(amount)\nfilters:\n  - amount > 5\n```\n"
    assert _texts(page) == ["sum(amount)", "amount > 5"]
    model = "name: orders\nsql_table: orders\nmeasures:\n  - name: total\n    formula: sum(amount)\n"
    assert [s.text for s in extract_yaml_file(model, path="m.yaml").snippets] == ["sum(amount)"]


def test_malformed_block_is_reported_with_its_line() -> None:
    page = "Intro\n\n```json\n{\"measures\": [\n  \"count(*)\",\n  ...\n]}\n```\n"
    extraction = extract_markdown(page, path="page.md")
    assert [(m.path, m.line) for m in extraction.malformed] == [("page.md", 4)]
    assert extraction.snippets == []


def test_snippet_line_points_at_its_value() -> None:
    page = "# T\n\n```json\n{\n  \"source_model\": \"orders\",\n  \"filters\": [\"amount > 1\"]\n}\n```\n"
    assert [(s.line, s.text) for s in extract_markdown(page, path="p.md").snippets] == [(6, "amount > 1")]


def test_repeated_and_overlapping_values_get_their_own_lines() -> None:
    page = (
        "```json\n"
        '{"source_model": "orders",\n'
        ' "dimensions": ["status_name"],\n'
        ' "measures": ["count(*)"],\n'
        ' "filters": ["status"],\n'
        ' "order": [{"column": "count(*)"}]}\n'
        "```\n\n"
        "```yaml\nname: status\nsql_table: t\nmeasures:\n  - name: n\n    formula: status\n```\n"
    )
    assert [(s.line, s.text) for s in extract_markdown(page, path="p.md").snippets] == [
        (3, "status_name"), (4, "count(*)"), (5, "status"), (6, "count(*)"), (14, "status"),
    ]


def test_prose_and_untagged_fences_are_ignored() -> None:
    page = (
        "Use `sum(amount) / count(*)` or \"measures\": [\"bad((\"] in prose.\n\n"
        "```\n{\"measures\": [\"bad((\"]}\n```\n\n"
        "```text\n{\"filters\": [\"bad((\"]}\n```\n"
    )
    assert _texts(page) == []


def test_skip_marker_drops_a_rejected_example() -> None:
    page = (
        f"Rejected:\n\n{SKIP_MARKER}\n\n"
        "```json\n{\"filters\": [\"row_number() over (...)\"]}\n```\n\n"
        "```json\n{\"filters\": [\"amount > 0\"]}\n```\n"
    )
    assert _texts(page) == ["amount > 0"]
