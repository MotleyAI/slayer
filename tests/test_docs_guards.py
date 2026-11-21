"""Docs-site guards: nav coverage, no issue numbers, every Mode-B example parses, links and redirects resolve."""

from __future__ import annotations

import os
import re
import subprocess
import tomllib
from functools import cache
from pathlib import Path
from typing import Any

import pytest

from slayer.core.syntax import parse_expr

from tests._doc_mode_b_examples import SKIP_MARKER, extract_markdown, extract_yaml_file, mode_b_values

_ROOT = Path(__file__).resolve().parents[1]
_DOCS = _ROOT / "docs"
_HELP = _ROOT / "slayer" / "memories" / "help_content"

#: Pages deliberately outside the nav.
NAV_EXEMPT = frozenset({
    "CLAUDE.md",
    "api_gaps.md",
    "getting-started/flight-sql.md",  # DEV-2103 adds it to the nav
})
_ISSUE_NUMBER = re.compile(rb"\bDEV-\d+\b", re.IGNORECASE)
_FENCE = re.compile(r"^\s{0,3}(`{3,}|~{3,})")
_INLINE_CODE = re.compile(r"(`+).*?\1")
_DESTINATION = r"(?:<([^>\n]+)>|((?:[^()\s]|\([^()\s]*\))+))"
_INLINE_LINK = re.compile(r"!?\[[^\]]*\]\(\s*" + _DESTINATION + r"(?:\s+\"[^\"]*\")?\s*\)")
_REFERENCE_LINK = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*" + _DESTINATION)
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
#: Pages moved off old-word paths: old page -> new page, or new folder prefix (ending ``/``).
MOVED_PAGES = {
    "concepts/models.md": "concepts/datasets.md",
    "concepts/formulas.md": "concepts/expressions.md",
    "examples/05_joins/joins.md": "examples/05_relationships/",
    "examples/05_joined_measures/joined_measures.md": "examples/05_joined_metrics/",
    "examples/16_list_columns/list_columns.md": "examples/16_list_fields/",
}
#: A git hook's repository-location variables would point ``git ls-files`` away from ``_ROOT``.
_GIT_LOCATION_VARS = frozenset({"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR"})


def _rel(path: Path) -> str:
    return path.relative_to(_DOCS).as_posix()


@cache
def _docs_files() -> tuple[Path, ...]:
    """Files under ``docs/`` that git does not ignore (generated notebook artefacts are skipped)."""
    env = {k: v for k, v in os.environ.items() if k not in _GIT_LOCATION_VARS}
    listed = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "docs/"],
        cwd=_ROOT, env=env, capture_output=True, text=True, check=True,
    ).stdout
    return tuple(sorted(p for p in (_ROOT / name for name in listed.split("\0") if name) if p.is_file()))


# --------------------------------------------------------------------------- #
# Nav coverage
# --------------------------------------------------------------------------- #
def _nav_paths(node: Any) -> set[str]:
    if isinstance(node, str):
        return {node}
    values = node.values() if isinstance(node, dict) else node
    return set().union(*(_nav_paths(v) for v in values))


def _zensical() -> dict[str, Any]:
    return tomllib.loads((_ROOT / "zensical.toml").read_text())


def _nav() -> set[str]:
    return _nav_paths(_zensical()["project"]["nav"])


def test_every_docs_page_is_in_the_nav() -> None:
    pages = {_rel(p) for p in _docs_files() if p.suffix in (".md", ".ipynb")}
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
        for path in _docs_files()
        if _rel(path) != "CLAUDE.md"
        for token in _ISSUE_NUMBER.findall(path.read_bytes())
    }
    assert found == set()


# --------------------------------------------------------------------------- #
# Relative links and redirects resolve
# --------------------------------------------------------------------------- #
def _relative_links(markdown: str) -> list[tuple[int, str]]:
    """``(line, target without anchor)`` per relative link outside code; external URLs and bare anchors skipped."""
    links: list[tuple[int, str]] = []
    fence: str | None = None
    for lineno, line in enumerate(markdown.splitlines(), 1):
        marker = _FENCE.match(line)
        if fence is None and marker:
            fence = marker.group(1)
            continue
        if fence is not None:
            closing = marker is not None and marker.group(1)[0] == fence[0] and len(marker.group(1)) >= len(fence)
            if closing and not line.strip().lstrip(fence[0]):
                fence = None
            continue
        prose = _INLINE_CODE.sub("", line)
        matches = [*_INLINE_LINK.finditer(prose), *_REFERENCE_LINK.finditer(prose)]
        for target in (m.group(1) or m.group(2) for m in matches):
            path = target.split("#", 1)[0]
            if path and not _SCHEME.match(target):
                links.append((lineno, path))
    return links


def _docs_markdown() -> list[Path]:
    return [p for p in _docs_files() if p.suffix == ".md"]


def test_every_relative_docs_link_resolves() -> None:
    broken = [
        f"{_rel(page)}:{lineno}: {target}"
        for page in _docs_markdown()
        for lineno, target in _relative_links(page.read_text())
        if not (page.parent / target).is_file()
    ]
    assert broken == []


def test_the_link_guard_reads_a_meaningful_number_of_links() -> None:
    assert sum(len(_relative_links(p.read_text())) for p in _docs_markdown()) >= 250


def _redirect_maps() -> dict[str, str]:
    return _zensical()["project"].get("plugins", {}).get("redirects", {}).get("redirect_maps", {})


def test_every_redirect_source_is_gone_and_its_destination_exists() -> None:
    redirects = _redirect_maps()
    assert sorted(src for src in redirects if (_DOCS / src).exists()) == []
    assert sorted(dst for dst in redirects.values() if not (_DOCS / dst).is_file()) == []


@pytest.mark.parametrize(("old", "new"), sorted(MOVED_PAGES.items()))
def test_every_moved_page_has_a_redirect(old: str, new: str) -> None:
    destination = _redirect_maps().get(old)
    assert destination is not None
    assert destination.startswith(new) if new.endswith("/") else destination == new


def test_link_extraction() -> None:
    page = (
        "See [datasets](../concepts/datasets.md#fields), ![logo](img/a.png \"Logo\") and [x](<b c.md>).\n"
        "[ref]: ./other.md\n"
        "External [site](https://docs.motley.ai/x.md), [mail](mailto:a@b.c), [anchor](#top).\n"
        "Code `[no](skip.md)` and ``[no](`skip`.md)`` are not links; [p](guide_(new).md) is.\n"
        "````md\n[fenced](skip.md)\n```\n[still fenced](skip.md)\n````\n"
        "~~~\n[tilde](skip.md)\n~~~\n"
        "[after](after.md)\n"
    )
    assert _relative_links(page) == [
        (1, "../concepts/datasets.md"), (1, "img/a.png"), (1, "b c.md"), (2, "./other.md"),
        (4, "guide_(new).md"), (13, "after.md"),
    ]


# --------------------------------------------------------------------------- #
# Every Mode-B example parses
# --------------------------------------------------------------------------- #
def _doc_sources() -> list[Path]:
    examples = _DOCS / "examples"
    yamls = [p for p in _docs_files() if p.suffix in (".yaml", ".yml") and p.is_relative_to(examples)]
    return [p for p in _docs_files() if p.suffix == ".md"] + sorted(_HELP.glob("*.md")) + yamls


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
        '```json\n{"population": "orders", "metrics": ["sum(amount)"], "filter": ["status = \'paid\'"]}\n```\n'
        "\n```bash\nslayer query '{\"metrics\": [\"count(*)\"]}'\n```\n"
    )
    assert _texts(page) == ["sum(amount)", "status = 'paid'"]


def test_multiline_lists_fragments_json_lines_and_comments() -> None:
    page = (
        "```json\n"
        '"metrics": [\n'
        '  "count(*)",\n'
        '  {"metric": "sum(amount) / count(*)", "alias": "aov"}\n'
        "]\n"
        "```\n\n"
        "```json\n"
        "// two queries\n"
        '{"population": "orders", "dimensions": ["status"]}  // by status\n'
        '{"population": "orders", "time_dimensions": ["month(order_date)"]}\n'
        "```\n"
    )
    assert _texts(page) == ["count(*)", "sum(amount) / count(*)", "status", "month(order_date)"]


def test_every_query_field_is_read() -> None:
    query = {
        "population": "orders",
        "metrics": ["count(orders.*)", {"metric": "sum(orders.amount)", "alias": "total"}, {"name": "revenue"}],
        "dimensions": ["orders.status", {"field": "upper(orders.status)", "alias": "s"}, {"field": "orders.status", "label": "S"}],
        "time_dimensions": ["month(orders.order_date)", {"field": "orders.order_date", "granularity": "week"}],
        "filter": ["orders.amount > 10"],
        "order": [{"field": "sum(orders.amount)", "direction": "desc"}],
        "variables": {"v": "x"},
    }
    assert mode_b_values(query) == [
        "count(orders.*)", "sum(orders.amount)", "orders.status", "upper(orders.status)", "orders.status",
        "month(orders.order_date)", "orders.order_date", "orders.amount > 10", "sum(orders.amount)",
    ]


def test_time_dimension_column_alias_is_read() -> None:
    query = {"time_dimensions": [{"field": "bad((", "granularity": "month"}]}
    assert mode_b_values(query) == ["bad(("]


def test_stage_lists_and_refinements() -> None:
    stages = [
        {"name": "per_customer", "population": "orders", "dimensions": ["customers.name"],
         "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}]},
        {"population": "per_customer", "metrics": ["avg(per_customer.total)"], "filter": ["per_customer.total > 100"]},
    ]
    call = {"query": "monthly_revenue", "refine": {"dimensions": ["region"], "filter": ["region = 'EU'"]}}
    assert mode_b_values(stages) == [
        "customers.name", "sum(orders.amount)", "avg(per_customer.total)", "per_customer.total > 100"]
    assert mode_b_values(call) == ["region", "region = 'EU'"]


def test_datasets_keep_only_metric_expressions() -> None:
    dataset = {
        "name": "orders", "sql_table": "orders", "data_source": "db",
        "filters": ["deleted_at IS NULL"],
        "fields": [{"name": "net", "expression": "amount - tax", "filter": "status = 'paid'", "datatype": "Float"}],
        "metrics": [{"name": "aov", "expression": "sum(amount) / count(*)"}],
        "aggregations": [{"name": "wavg", "expression": "SUM({value} * {weight}) / SUM({weight})"}],
        "source_queries": [{"population": "raw", "filter": ["raw.amount > 0"]}],
    }
    assert mode_b_values(dataset) == ["sum(amount) / count(*)", "raw.amount > 0"]


def test_inline_extension_and_standalone_objects() -> None:
    query = {"population": "orders",
             "extensions": [{"dataset": "orders", "fields": [{"name": "big", "expression": "amount > 100"}]}],
             "metrics": ["count(orders.*)"]}
    assert mode_b_values(query) == ["count(orders.*)"]
    assert mode_b_values({"expression": "cumsum(sum(revenue))"}) == ["cumsum(sum(revenue))"]
    assert mode_b_values({"field": "created_at", "granularity": "month", "date_range": "2025"}) == ["created_at"]
    assert mode_b_values({"name": "wavg", "expression": "SUM({value})", "params": [{"name": "w", "sql": "1"}]}) == []
    assert mode_b_values({"orders.status": "paid", "orders._count": 3}) == []


def test_yaml_fences_and_files() -> None:
    page = "```yaml\npopulation: orders\nmetrics:\n  - sum(amount)\nfilter:\n  - amount > 5\n```\n"
    assert _texts(page) == ["sum(amount)", "amount > 5"]
    dataset = "name: orders\nsql_table: orders\nmetrics:\n  - name: total\n    expression: sum(amount)\n"
    assert [s.text for s in extract_yaml_file(dataset, path="m.yaml").snippets] == ["sum(amount)"]


def test_malformed_block_is_reported_with_its_line() -> None:
    page = "Intro\n\n```json\n{\"metrics\": [\n  \"count(*)\",\n  ...\n]}\n```\n"
    extraction = extract_markdown(page, path="page.md")
    assert [(m.path, m.line) for m in extraction.malformed] == [("page.md", 4)]
    assert extraction.snippets == []


def test_snippet_line_points_at_its_value() -> None:
    page = "# T\n\n```json\n{\n  \"population\": \"orders\",\n  \"filter\": [\"amount > 1\"]\n}\n```\n"
    assert [(s.line, s.text) for s in extract_markdown(page, path="p.md").snippets] == [(6, "amount > 1")]


def test_repeated_and_overlapping_values_get_their_own_lines() -> None:
    page = (
        "```json\n"
        '{"population": "orders",\n'
        ' "dimensions": ["status_name"],\n'
        ' "metrics": ["count(*)"],\n'
        ' "filter": ["status"],\n'
        ' "order": [{"field": "count(*)"}]}\n'
        "```\n\n"
        "```yaml\nname: status\nsql_table: t\nmetrics:\n  - name: n\n    expression: status\n```\n"
    )
    assert [(s.line, s.text) for s in extract_markdown(page, path="p.md").snippets] == [
        (3, "status_name"), (4, "count(*)"), (5, "status"), (6, "count(*)"), (14, "status"),
    ]


def test_prose_and_untagged_fences_are_ignored() -> None:
    page = (
        "Use `sum(amount) / count(*)` or \"metrics\": [\"bad((\"] in prose.\n\n"
        "```\n{\"metrics\": [\"bad((\"]}\n```\n\n"
        "```text\n{\"filter\": [\"bad((\"]}\n```\n"
    )
    assert _texts(page) == []


def test_skip_marker_drops_a_rejected_example() -> None:
    page = (
        f"Rejected:\n\n{SKIP_MARKER}\n\n"
        "```json\n{\"filter\": [\"row_number() over (...)\"]}\n```\n\n"
        "```json\n{\"filter\": [\"amount > 0\"]}\n```\n"
    )
    assert _texts(page) == ["amount > 0"]
