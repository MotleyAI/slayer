"""Docs-site guards: nav, no issue numbers, every Mode-B example parses, links, anchors and redirects resolve."""

from __future__ import annotations

import html
import json
import os
import re
import subprocess
import tomllib
import unicodedata
from functools import cache
from pathlib import Path
from typing import Any
from urllib.parse import unquote

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
_REFERENCE_LINK = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*" + _DESTINATION, re.M)
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
#: Pages moved off old-word paths: old page -> new page, or new folder prefix (ending ``/``).
MOVED_PAGES = {
    "concepts/models.md": "concepts/datasets.md",
    "concepts/formulas.md": "concepts/expressions.md",
    "examples/05_joins/joins.md": "examples/05_relationships/",
    "examples/05_joined_measures/joined_measures.md": "examples/05_joined_metrics/",
    "examples/16_list_columns/list_columns.md": "examples/16_list_fields/",
}
#: Sections split out of ``concepts/queries.md``: old slug -> new ``page.md#slug`` (relative to ``concepts/``).
MOVED_ANCHORS = {
    "filters": "filters.md#filters",
    "comparison-operators": "filters.md#comparison-operators",
    "boolean-logic": "filters.md#boolean-logic",
    "field-vs-metric-filters": "filters.md#field-vs-metric-filters",
    "scalar-functions-in-filters": "filters.md#scalar-functions-in-filters",
    "filtering-on-computed-metrics": "filters.md#filtering-on-computed-metrics",
    "filters-and-auto-joins": "filters.md#filters-and-auto-joins",
    "list-fields": "filters.md#list-fields",
    "window-functions-in-filters": "filters.md#window-functions-in-filters",
    "variables": "filters.md#variables",
    "variables-passed-as-a-runtime-kwarg": "filters.md#variables-passed-as-a-runtime-kwarg",
    "cross-dataset-metrics": "cross-dataset-queries.md#cross-dataset-metrics",
    "multi-hop-dimensions": "cross-dataset-queries.md#multi-hop-dimensions",
    "choosing-a-root-dataset": "cross-dataset-queries.md#choosing-a-root-dataset",
    "forcing-a-root-with-root_hint": "cross-dataset-queries.md#forcing-a-root-with-root_hint",
    "query-lists": "saved-and-multistage-queries.md#query-lists",
    "run-a-saved-query-by-name": "saved-and-multistage-queries.md#run-a-saved-query-by-name",
    "refining-a-saved-query": "saved-and-multistage-queries.md#refining-a-saved-query",
}
_HEADING = re.compile(r"^ {0,3}#{1,6}[ \t]+(.*?)(?:[ \t]+#+)?[ \t]*$")
_EXPLICIT_ID = re.compile(r"<[A-Za-z][^<>]*?\bid=[\"']([^\"']+)[\"']")
_LINK_TEXT = re.compile(r"!?\[([^\]]*)\](?:\([^)]*\)|\[[^\]]*\])")
_HTML_TAG = re.compile(r"</?[A-Za-z][^<>]*>")
_UNDERSCORE_EMPHASIS = re.compile(r"(?<!\w)_{1,2}(\S(?:.*?\S)?)_{1,2}(?!\w)")
_ID_COUNT = re.compile(r"^(.*)_([0-9]+)$")
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
def _nav_entries(node: Any) -> list[str]:
    if isinstance(node, str):
        return [node]
    values = node.values() if isinstance(node, dict) else node
    return [path for v in values for path in _nav_entries(v)]


def _nav_paths(node: Any) -> set[str]:
    return set(_nav_entries(node))


def _zensical() -> dict[str, Any]:
    return tomllib.loads((_ROOT / "zensical.toml").read_text())


def _nav() -> set[str]:
    return _nav_paths(_zensical()["project"]["nav"])


def test_every_docs_page_is_in_the_nav() -> None:
    pages = {_rel(p) for p in _docs_files() if p.suffix in (".md", ".ipynb")}
    assert sorted(pages - _nav() - NAV_EXEMPT) == []


def test_every_nav_entry_exists() -> None:
    assert sorted(p for p in _nav() if not (_DOCS / p).is_file()) == []


def test_no_page_is_listed_twice_in_the_nav() -> None:
    entries = _nav_entries(_zensical()["project"]["nav"])
    assert sorted({p for p in entries if entries.count(p) > 1}) == []


def test_nav_exemptions_exist_and_stay_out_of_the_nav() -> None:
    assert all((_DOCS / p).is_file() for p in NAV_EXEMPT)
    assert NAV_EXEMPT & _nav() == set()


#: Top-level nav sections, each with exactly the pages it holds (order not asserted).
NAV_SECTIONS: dict[str, set[str]] = {
    "Home": {"index.md"},
    "Get started": {
        "getting-started/index.md", "getting-started/mcp.md", "getting-started/cli.md",
        "getting-started/rest-api.md", "getting-started/python.md",
    },
    "Connect your data": {
        "configuration/datasources.md", "database-support.md", "configuration/storage.md",
        "configuration/sql-clients.md", "interfaces/flight-sql.md", "interfaces/pg-facade.md",
    },
    "Model your data": {
        "concepts/datasets.md", "concepts/semantic-models.md", "concepts/ontologies.md", "concepts/ingestion.md",
        "concepts/schema-drift.md", "examples/schema-drift.md",
        "dbt/slayer_vs_dbt.md", "dbt/dbt_import.md",
        "examples/11_dbt_metricflow/dbt_metricflow.md", "examples/11_dbt_metricflow/dbt_metricflow_nb.ipynb",
        "cube/cube_import.md",
        "examples/17_cube_import/cube_import.md", "examples/17_cube_import/cube_import_nb.ipynb",
        "osi/osi_import.md", "osi/osi_export.md",
        "examples/13_osi_import/osi_import.md", "examples/13_osi_import/osi_import_nb.ipynb",
        "examples/13_osi_import/osi_import_agent_nb.ipynb",
    },
    "Query your data": {
        "concepts/queries.md", "concepts/expressions.md", "concepts/time.md", "concepts/filters.md",
        "concepts/cross-dataset-queries.md", "concepts/saved-and-multistage-queries.md", "concepts/references.md",
    },
    "Agent context": {"concepts/memories.md", "concepts/search.md"},
    "Operate": {
        "concepts/row-level-security.md", "concepts/query-cache.md", "reference/opentelemetry.md",
        "reference/usage-reporting.md", "upgrading/2.0.md",
    },
    "Tutorials": {
        "examples/01_dynamic/dynamic.md",
        "examples/02_sql_vs_dsl/sql_vs_dsl.md", "examples/02_sql_vs_dsl/sql_vs_dsl_nb.ipynb",
        "examples/03_auto_ingest/auto_ingest.md", "examples/03_auto_ingest/auto_ingest_nb.ipynb",
        "examples/04_time/time.md", "examples/04_time/time_nb.ipynb",
        "examples/05_relationships/relationships.md", "examples/05_relationships/relationships_nb.ipynb",
        "examples/05_joined_metrics/joined_metrics.md", "examples/05_joined_metrics/joined_metrics_nb.ipynb",
        "examples/06_multistage_queries/multistage_queries.md",
        "examples/06_multistage_queries/multistage_queries_nb.ipynb",
        "examples/07_aggregations/aggregations.md", "examples/07_aggregations/aggregations_nb.ipynb",
        "examples/08_mcp_introspect/mcp_introspect_nb.ipynb",
        "examples/09_lightning_talk/lightning_talk.md", "examples/09_lightning_talk/lightning_talk_nb.ipynb",
        "examples/10_row_level_security/row_level_security_nb.ipynb",
        "examples/12_query_cache/query_cache_nb.ipynb",
        "examples/14_variable_substitution/variable_substitution.md",
        "examples/14_variable_substitution/variable_substitution_nb.ipynb",
        "examples/15_duckdb/duckdb.md", "examples/15_duckdb/duckdb_python_nb.ipynb",
        "examples/15_duckdb/duckdb_cli_nb.ipynb",
        "examples/16_list_fields/list_fields.md", "examples/16_list_fields/list_fields_nb.ipynb",
    },
    "Reference": {
        "reference/mcp.md", "reference/rest-api.md", "reference/python-client.md", "reference/cli.md",
        "concepts/terminology.md", "interfaces/ossie-layer-3.md", "interfaces/ossie-compliance.md",
    },
    "Contributing": {"development.md"},
}


def _nav_sections() -> list[tuple[str, Any]]:
    return [next(iter(entry.items())) for entry in _zensical()["project"]["nav"]]


def test_nav_top_level_sections() -> None:
    assert sorted(title for title, _ in _nav_sections()) == sorted(NAV_SECTIONS)


@pytest.mark.parametrize("section", list(NAV_SECTIONS))
def test_nav_section_holds_exactly_its_pages(section: str) -> None:
    assert _nav_paths(dict(_nav_sections()).get(section, [])) == NAV_SECTIONS[section]


def test_home_page_links_to_the_upgrade_guide() -> None:
    assert "upgrading/2.0.md" in {target for _, target in _relative_links((_DOCS / "index.md").read_text())}


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
def _prose_lines(markdown: str) -> list[tuple[int, str]]:
    """``(line number, line)`` for every line outside fenced code."""
    lines: list[tuple[int, str]] = []
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
        lines.append((lineno, line))
    return lines


def _paragraphs(markdown: str) -> list[tuple[int, str]]:
    """``(first line, text)`` per run of adjacent non-blank prose lines, so link text may wrap."""
    paragraphs: list[tuple[int, list[str]]] = []
    previous = -1
    for lineno, line in _prose_lines(markdown):
        if not line.strip():
            previous = -1
            continue
        if lineno == previous + 1:
            paragraphs[-1][1].append(_INLINE_CODE.sub("", line))
        else:
            paragraphs.append((lineno, [_INLINE_CODE.sub("", line)]))
        previous = lineno
    return [(start, "\n".join(lines)) for start, lines in paragraphs]


def _link_targets(markdown: str) -> list[tuple[int, str]]:
    """``(line, target)`` per non-external link outside code, bare anchors included."""
    links: list[tuple[int, str]] = []
    for start, prose in _paragraphs(markdown):
        matches = sorted([*_INLINE_LINK.finditer(prose), *_REFERENCE_LINK.finditer(prose)], key=lambda m: m.start())
        links += [
            (start + prose.count("\n", 0, m.start()), t)
            for m in matches
            if not _SCHEME.match(t := m.group(1) or m.group(2))
        ]
    return links


def _relative_links(markdown: str) -> list[tuple[int, str]]:
    """``(line, target without anchor)`` per relative link outside code; external URLs and bare anchors skipped."""
    return [(lineno, path) for lineno, target in _link_targets(markdown) if (path := target.split("#", 1)[0])]


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


def test_wrapped_link_extraction() -> None:
    page = "Intro\nsee [the time\nzones](time.md#time-zones) and [a\n\nb](skip.md).\n[ref]: ./r.md\n"
    assert _link_targets(page) == [(2, "time.md#time-zones"), (6, "./r.md")]


# --------------------------------------------------------------------------- #
# Anchors resolve (Python-Markdown default ``toc`` slugs, as Zensical renders them)
# --------------------------------------------------------------------------- #
_QUERIES = _DOCS / "concepts" / "queries.md"
#: Anchored links in notebook Markdown cells (20 measured).
NOTEBOOK_MIN = 15


def _slugify(text: str) -> str:
    value = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    return re.sub(r"[-\s]+", "-", value)


def _unique(slug: str, used: set[str]) -> str:
    while slug in used or not slug:
        count = _ID_COUNT.match(slug)
        slug = f"{count.group(1)}_{int(count.group(2)) + 1}" if count else f"{slug}_1"
    used.add(slug)
    return slug


def _heading_text(raw: str) -> str:
    """Rendered heading text: code spans kept verbatim, link / HTML / emphasis markup dropped, entities decoded."""
    pieces = re.split(r"(`+)(.*?)\1", raw)
    text = ""
    for i in range(0, len(pieces), 3):
        prose = _UNDERSCORE_EMPHASIS.sub(r"\1", _HTML_TAG.sub("", _LINK_TEXT.sub(r"\1", pieces[i])))
        text += html.unescape(prose) + (pieces[i + 2].strip() if i + 2 < len(pieces) else "")
    return text


def _heading_slugs(markdown: str) -> set[str]:
    used: set[str] = set()
    for _, line in _prose_lines(markdown):
        if heading := _HEADING.match(line):
            _unique(_slugify(_heading_text(heading.group(1))), used)
    return used


def _explicit_ids(markdown: str) -> set[str]:
    return {m.group(1) for _, line in _prose_lines(markdown) for m in _EXPLICIT_ID.finditer(_INLINE_CODE.sub("", line))}


def _notebook_markdown(text: str) -> list[tuple[int, str]]:
    """``(cell index, source)`` per Markdown cell."""
    cells = json.loads(text)["cells"]
    return [(i, "".join(c["source"])) for i, c in enumerate(cells) if c["cell_type"] == "markdown"]


def _page_text(path: Path) -> str:
    text = path.read_text()
    return "\n\n".join(src for _, src in _notebook_markdown(text)) if path.suffix == ".ipynb" else text


@cache
def _page_anchors(path: Path) -> frozenset[str]:
    if not path.is_file():
        return frozenset()
    text = _page_text(path)
    return frozenset(_heading_slugs(text) | _explicit_ids(text))


def _where(path: Path) -> str:
    return _rel(path) if path.is_relative_to(_DOCS) else path.name


def _anchored_links(source: Path) -> list[tuple[str, str, Path, str]]:
    """``(location, raw target, target page, anchor)`` per relative or same-page link carrying an anchor."""
    text = source.read_text()
    chunks = (
        [(f"{_where(source)}:cell{i}", src) for i, src in _notebook_markdown(text)]
        if source.suffix == ".ipynb" else [(_where(source), text)]
    )
    links: list[tuple[str, str, Path, str]] = []
    for where, chunk in chunks:
        for lineno, target in _link_targets(chunk):
            path, _, anchor = target.partition("#")
            if anchor:
                page = (source.parent / path).resolve() if path else source.resolve()
                links.append((f"{where}:{lineno}", target, page, unquote(anchor)))
    return links


def _broken_anchors(sources: list[Path]) -> list[str]:
    return [
        f"{where}: {target}"
        for source in sources
        for where, target, page, anchor in _anchored_links(source)
        if page.suffix in (".md", ".ipynb") and anchor not in _page_anchors(page)
    ]


def _anchor_sources() -> list[Path]:
    return [p for p in _docs_files() if p.suffix in (".md", ".ipynb")]


def test_every_docs_anchor_resolves() -> None:
    assert _broken_anchors(_anchor_sources()) == []


def test_the_anchor_guard_reads_a_meaningful_number_of_links() -> None:
    assert sum(len(_anchored_links(p)) for p in _anchor_sources()) >= 250


def test_the_anchor_guard_reads_notebook_cells() -> None:
    assert sum(len(_anchored_links(p)) for p in _anchor_sources() if p.suffix == ".ipynb") >= NOTEBOOK_MIN


def test_heading_slugs_and_explicit_ids() -> None:
    page = (
        "# Queries\n"
        "## Ordering by something you don't project\n"
        "### Forcing a root with `root_hint`\n"
        "### Share of parent (`partition_by=`)\n"
        "### `CAST(<column> AS <type>)` in projection\n"
        "## Cubes → datasets ##\n"
        "## [Linked](x.md) <b>bold</b> and _em_ AT&amp;T\n"
        "## Examples\n## Examples\n## Examples\n"
        "```bash\n# not a heading\n```\n"
        "    # indented code, not a heading\n"
        '<a id="old-slug"></a> and `<a id="in-code"></a>`\n'
    )
    assert _heading_slugs(page) == {
        "queries", "ordering-by-something-you-dont-project", "forcing-a-root-with-root_hint",
        "share-of-parent-partition_by", "castcolumn-as-type-in-projection", "cubes-datasets",
        "linked-bold-and-em-att", "examples", "examples_1", "examples_2",
    }
    assert _explicit_ids(page) == {"old-slug"}


def test_notebook_markdown_cells_are_checked(tmp_path: Path) -> None:
    (tmp_path / "page.md").write_text("# Real heading\n")
    cells = [
        {"cell_type": "markdown", "source": ["## Local\n", "See [ok](page.md#real-heading) and [self](#local).\n"]},
        {"cell_type": "code", "source": ["# [code](page.md#ignored)\n"]},
        {"cell_type": "markdown", "source": "A [bad](page.md#missing) link.\n"},
    ]
    notebook = tmp_path / "nb.ipynb"
    notebook.write_text(json.dumps({"cells": cells}))
    assert [(where, target) for where, target, _, _ in _anchored_links(notebook)] == [
        ("nb.ipynb:cell0:2", "page.md#real-heading"), ("nb.ipynb:cell0:2", "#local"),
        ("nb.ipynb:cell2:1", "page.md#missing"),
    ]
    assert _broken_anchors([notebook]) == ["nb.ipynb:cell2:1: page.md#missing"]


@pytest.mark.parametrize("slug", sorted(MOVED_ANCHORS))
def test_moved_anchor_is_a_hidden_id_in_queries_md(slug: str) -> None:
    text = _QUERIES.read_text()
    assert slug in _explicit_ids(text)
    assert slug not in _heading_slugs(text)


@pytest.mark.parametrize(("slug", "new"), sorted(MOVED_ANCHORS.items()))
def test_moved_anchor_hidden_id_links_to_its_new_home(slug: str, new: str) -> None:
    lines = [line for _, line in _prose_lines(_QUERIES.read_text()) if slug in _explicit_ids(line)]
    assert lines
    assert all(new in {target for _, target in _link_targets(line)} for line in lines)


@pytest.mark.parametrize(("slug", "new"), sorted(MOVED_ANCHORS.items()))
def test_moved_anchor_new_home_is_a_heading(slug: str, new: str) -> None:
    page, _, anchor = new.partition("#")
    target = _QUERIES.parent / page
    assert anchor == slug
    assert target.is_file()
    assert anchor in _heading_slugs(target.read_text())


def test_no_docs_page_links_to_a_moved_queries_anchor() -> None:
    queries = _QUERIES.resolve()
    stale = [
        f"{where}: {target}"
        for source in _docs_markdown()
        for where, target, page, anchor in _anchored_links(source)
        if page == queries and anchor in MOVED_ANCHORS
    ]
    assert stale == []


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
