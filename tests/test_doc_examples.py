"""Every query example in the docs runs on the demo, or is marked as illustrative."""

from __future__ import annotations

import json
import subprocess
import sys
from typing import Any

import pytest

from slayer.storage.yaml_storage import YAMLStorage
from tests import _doc_examples
from tests._doc_examples import (
    IN_SCOPE_GLOBS,
    REPO_ROOT,
    DocQuery,
    DocReference,
    in_scope_pages,
    load_page,
    page_failures,
    parse_page,
    query_failure,
    reference_failure,
)

COUNT = {"source_model": "orders", "measures": ["count(*)"]}
MARKER = '!!! note "Illustrative schema"'
NOT_A_QUERY = "<!-- doc-check: not-a-query -->"
HYPOTHETICAL = '{"source_model": "orders", "measures": ["sum(website_visits)"]}'


def _md(*lines: str) -> str:
    return "\n".join(lines) + "\n"


def _found(markdown: str) -> list[tuple[int, Any]]:
    page = parse_page(path="page.md", text=markdown)
    return [(q.line, q.query) for b in page.blocks for q in b.queries]


# ---------------------------------------------------------------------------
# Extraction: every wrapper yields its query, with the line of its source_model
# ---------------------------------------------------------------------------


class TestExtraction:
    def test_json_block(self) -> None:
        md = _md("# T", "", "```json", "{", '  "source_model": "orders",', '  "measures": ["count(*)"]', "}", "```")
        assert _found(md) == [(5, COUNT)]

    def test_json_stage_list(self) -> None:
        md = _md(
            "```json",
            "[",
            '  {"name": "s", "source_model": "orders", "measures": ["count(*)"], "dimensions": ["store_id"]},',
            '  {"source_model": "s", "measures": ["max(_count)"]}',
            "]",
            "```",
        )
        assert _found(md) == [(3, [
            {"name": "s", "source_model": "orders", "measures": ["count(*)"], "dimensions": ["store_id"]},
            {"source_model": "s", "measures": ["max(_count)"]},
        ])]

    def test_multiline_slayer_query(self) -> None:
        md = _md("```bash", "# Count orders", "slayer query '{", '  "source_model": "orders",',
                 '  "measures": ["count(*)"]', "}'", "```")
        assert _found(md) == [(4, COUNT)]

    def test_multiline_curl_with_escaped_quotes(self) -> None:
        md = _md(
            "```bash",
            "curl -X POST http://localhost:5143/query \\",
            '  -H "Content-Type: application/json" \\',
            "  -d '{",
            '    "source_model": "orders",',
            '    "measures": ["count(*)"],',
            """    "filters": ["status = '\\''completed'\\''"]""",
            "  }'",
            "```",
        )
        assert _found(md) == [(5, {**COUNT, "filters": ["status = 'completed'"]})]

    def test_slayer_query_call(self) -> None:
        md = _md(
            "```python",
            "result = engine.execute_sync(query=SlayerQuery(",
            '    source_model="orders",',
            '    measures=["count(*)"],',
            "))",
            "```",
        )
        assert _found(md) == [(3, COUNT)]

    def test_python_dict_and_list_literals(self) -> None:
        md = _md(
            "```python",
            "query = {",
            '    "source_model": "orders",',
            '    "measures": ["count(*)"],',
            "}",
            "client.query_sync([",
            '    {"name": "s", "source_model": "orders", "measures": ["count(*)"]},',
            '    {"source_model": "s", "measures": ["max(_count)"]},',
            "])",
            "```",
        )
        assert _found(md) == [
            (3, COUNT),
            (7, [{"name": "s", **COUNT}, {"source_model": "s", "measures": ["max(_count)"]}]),
        ]

    def test_mcp_query_call(self) -> None:
        md = _md(
            "```",
            "1. list_datasources()",
            '2. query(query={"source_model": "orders", "measures": ["count(*)"], "limit": 10})',
            "```",
        )
        assert _found(md) == [(3, {**COUNT, "limit": 10})]

    def test_json_stringify_with_quoted_keys(self) -> None:
        md = _md(
            "```javascript",
            'const res = await fetch("http://localhost:5143/query", {',
            '  method: "POST",',
            '  headers: {"Content-Type": "application/json"},',
            "  body: JSON.stringify({",
            '    "source_model": "orders",',
            '    "measures": ["count(*)"],',
            "  }),",
            "});",
            "```",
        )
        assert _found(md) == [(6, COUNT)]

    def test_several_queries_in_one_block(self) -> None:
        md = _md(
            "```bash",
            """slayer query '{"source_model": "orders", "measures": ["count(*)"]}'""",
            "",
            """slayer query '{"source_model": "stores", "measures": ["count(*)"]}' --format json""",
            "```",
        )
        assert _found(md) == [(2, COUNT), (4, {"source_model": "stores", "measures": ["count(*)"]})]

    def test_placeholders_inside_strings(self) -> None:
        query = {
            "source_model": {"name": "m", "data_source": "jaffle_shop", "sql": "SELECT 1 WHERE {? x IN ({stores}) ?}"},
            "measures": ["count(*)"],
            "filters": ["order_total >= {floor}"],
        }
        md = _md("```text", f"query = {json.dumps(query)}", "```")
        assert _found(md) == [(2, query)]

    def test_python_literal_in_json_block_is_not_extracted(self) -> None:
        md = _md("```json", "{'source_model': 'orders', 'measures': ['count(*)']}", "```")
        assert _found(md) == []

    def test_run_by_name_and_file_forms_yield_nothing(self) -> None:
        md = _md(
            "```bash",
            "slayer query @query.json",
            """slayer query monthly_revenue --refine '{"dimensions": ["region"]}'""",
            "```",
            "",
            "```python",
            'client.query_sync("rev_by_region", refine={"dimensions": ["status"]})',
            "```",
        )
        assert _found(md) == []

    def test_demo_references_are_collected(self) -> None:
        md = _md(
            "Inspect `jaffle_shop.orders.order_total` or see the file at ~/demo/jaffle_shop.duckdb.",
            "```bash",
            "slayer memory save --entities jaffle_shop.orders.order_total,jaffle_shop.stores.name",
            "```",
        )
        page = parse_page(path="page.md", text=md)
        assert page.references == [
            DocReference(line=1, reference="jaffle_shop.orders.order_total"),
            DocReference(line=3, reference="jaffle_shop.orders.order_total"),
            DocReference(line=3, reference="jaffle_shop.stores.name"),
        ]


# ---------------------------------------------------------------------------
# Markers and the coverage guard (demo/doc-examples scenarios)
# ---------------------------------------------------------------------------


def _failures(storage: YAMLStorage, *lines: str) -> list[str]:
    return page_failures(page=parse_page(path="page.md", text=_md(*lines)), storage=storage)


class TestMarkersAndGuard:
    def test_unmarked_failing_query_fails_naming_page_and_line(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(doc_demo_storage, "Text.", "", "```json", HYPOTHETICAL, "```")
        assert len(failures) == 1
        assert failures[0].startswith("page.md:4: ")

    def test_marked_query_is_exempt_and_next_block_is_checked(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(
            doc_demo_storage,
            MARKER,
            "    This example uses a hypothetical `website_visits` column.",
            "",
            "```json",
            HYPOTHETICAL,
            "```",
            "",
            "```json",
            HYPOTHETICAL,
            "```",
        )
        assert [f.split(": ")[0] for f in failures] == ["page.md:9"]

    def test_marker_without_body_applies(self, doc_demo_storage: YAMLStorage) -> None:
        assert _failures(doc_demo_storage, MARKER, "", "```json", HYPOTHETICAL, "```") == []

    def test_marker_followed_by_prose_does_not_apply(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(
            doc_demo_storage, MARKER, "    Hypothetical.", "", "Some prose.", "", "```json", HYPOTHETICAL, "```",
        )
        assert [f.split(": ")[0] for f in failures] == ["page.md:7"]

    def test_query_returning_no_rows_fails(self, doc_demo_storage: YAMLStorage) -> None:
        query = '{"source_model": "orders", "measures": ["count(*)"], "dimensions": ["store_id"], "filters": ["order_total < 0"]}'
        failures = _failures(doc_demo_storage, "```json", query, "```")
        assert failures == ["page.md:2: returned no rows"]

    def test_unextractable_source_model_block_fails_unless_marked(self, doc_demo_storage: YAMLStorage) -> None:
        block = ["```javascript", "JSON.stringify({source_model: 'orders', measures: ['count(*)']})", "```"]
        failures = _failures(doc_demo_storage, *block)
        assert len(failures) == 1
        assert failures[0].startswith("page.md:1: ")
        assert _failures(doc_demo_storage, MARKER, "", *block) == []

    def test_unextractable_query_beside_an_extracted_one_fails(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(
            doc_demo_storage,
            "```javascript",
            'JSON.stringify({"source_model": "orders", "measures": ["count(*)"]})',
            "JSON.stringify({source_model: 'orders', measures: ['count(*)']})",
            "```",
        )
        assert failures == ["page.md:1: code block mentions source_model outside any extracted query"]

    def test_not_a_query_comment_exempts_block_from_guard(self, doc_demo_storage: YAMLStorage) -> None:
        block = ["```", "SlayerQuery (source_model, measures, dimensions, filters)", "```"]
        assert len(_failures(doc_demo_storage, *block)) == 1
        assert _failures(doc_demo_storage, NOT_A_QUERY, "", *block) == []

    def test_not_a_query_comment_with_indented_content_between_does_not_apply(
        self, doc_demo_storage: YAMLStorage,
    ) -> None:
        block = ["```", "SlayerQuery (source_model, measures, dimensions, filters)", "```"]
        failures = _failures(doc_demo_storage, NOT_A_QUERY, "    Indented prose.", "", *block)
        assert [f.split(": ")[0] for f in failures] == ["page.md:4"]

    def test_not_a_query_comment_does_not_exempt_queries(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(doc_demo_storage, NOT_A_QUERY, "```json", HYPOTHETICAL, "```")
        assert [f.split(": ")[0] for f in failures] == ["page.md:3"]

    def test_datasource_word_does_not_trip_the_guard(self, doc_demo_storage: YAMLStorage) -> None:
        assert _failures(doc_demo_storage, "```", "ingest_datasource_models(datasource_name='mydb')", "```") == []

    def test_query_check_closes_its_engine(
        self, doc_demo_storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        closed: list[bool] = []

        class Recorder(_doc_examples.SlayerQueryEngine):
            def close(self) -> None:
                closed.append(True)
                super().close()

        monkeypatch.setattr(_doc_examples, "SlayerQueryEngine", Recorder)
        assert query_failure(storage=doc_demo_storage, query=COUNT) is None
        assert query_failure(storage=doc_demo_storage, query=json.loads(HYPOTHETICAL)) is not None
        assert closed == [True, True]

    def test_dangling_demo_reference_fails(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(doc_demo_storage, "Run `slayer inspect jaffle_shop.orders.order_id`.")
        assert len(failures) == 1
        assert failures[0].startswith("page.md:1: jaffle_shop.orders.order_id: ")

    def test_resolving_demo_reference_passes(self, doc_demo_storage: YAMLStorage) -> None:
        assert _failures(doc_demo_storage, "Run `slayer inspect jaffle_shop.orders.order_total`.") == []


# ---------------------------------------------------------------------------
# Image alt text: checked with the code-block rules
# ---------------------------------------------------------------------------

COUNT_JSON = json.dumps(COUNT)
STORES_COUNT = {"source_model": "stores", "measures": ["count(*)"]}


def _checked(markdown: str) -> list[tuple[int, Any]]:
    return [(q.line, q.query) for q in parse_page(path="page.md", text=markdown).checked_queries()]


class TestAltTextExtraction:
    def test_inline_image(self) -> None:
        assert _checked(_md("# T", "", f"![Chart of {COUNT_JSON}](images/x.png)")) == [(3, COUNT)]

    def test_inline_image_with_title(self) -> None:
        assert _checked(_md(f'![Chart of {COUNT_JSON}](images/x.png "Title")')) == [(1, COUNT)]

    def test_reference_style_image(self) -> None:
        md = _md(f"![Chart of {COUNT_JSON}][chart]", "", "[chart]: images/x.png")
        assert _checked(md) == [(1, COUNT)]

    def test_shortcut_reference_image(self, doc_demo_storage: YAMLStorage) -> None:
        md = _md('![Chart of {"source_model": "orders"}]', "", '[chart of {"source_model":  "orders"}]: x.png')
        assert _checked(md) == [(1, {"source_model": "orders"})]
        lines = ("![Each query names a source_model]", "", "  [EACH query names a source_model]: x.png")
        assert len(_failures(doc_demo_storage, *lines)) == 1

    def test_bracketed_text_without_a_definition_is_not_an_image(self, doc_demo_storage: YAMLStorage) -> None:
        assert _failures(doc_demo_storage, "![Each query names a source_model]", "", "[other]: x.png") == []

    def test_nested_brackets_in_alt_text(self) -> None:
        assert _checked(_md(f"![A [boxed] chart of {COUNT_JSON}](images/x.png)")) == [(1, COUNT)]

    def test_multiline_alt_text_names_the_source_model_line(self) -> None:
        md = _md("![Before the query", '{"source_model": "orders",', ' "measures": ["count(*)"]}', "after](x.png)")
        assert _checked(md) == [(2, COUNT)]

    def test_escaped_bracket_does_not_end_alt_text(self) -> None:
        assert _checked(_md(f"![A note \\] then {COUNT_JSON}](images/x.png)")) == [(1, COUNT)]

    def test_several_images(self) -> None:
        md = _md(
            f"![First {COUNT_JSON}](a.png) and ![second {json.dumps(STORES_COUNT)}](b.png)",
            "",
            f"![Third {COUNT_JSON}](c.png)",
        )
        assert _checked(md) == [(1, COUNT), (1, STORES_COUNT), (3, COUNT)]

    def test_non_query_alt_text_yields_nothing(self) -> None:
        assert _checked(_md("![The SLayer architecture diagram](images/arch.png)")) == []

    def test_ordinary_link_is_not_scanned(self, doc_demo_storage: YAMLStorage) -> None:
        lines = (f"See [the query {HYPOTHETICAL}](page.md) and [source_model docs](ref.md).",)
        assert _checked(_md(*lines)) == []
        assert _failures(doc_demo_storage, *lines) == []

    def test_html_img_alt_is_not_scanned(self, doc_demo_storage: YAMLStorage) -> None:
        lines = (f"<img src=\"x.png\" alt='{HYPOTHETICAL}'>",)
        assert _checked(_md(*lines)) == []
        assert _failures(doc_demo_storage, *lines) == []

    @pytest.mark.parametrize(
        "lines",
        [
            pytest.param((f"Write `![Chart of {HYPOTHETICAL}](x.png)` for an image.",), id="inline-code"),
            pytest.param(("Write ``![a `source_model` chart](x.png)`` for an image.",), id="double-backtick-code"),
            pytest.param(("Write `![Before", f"{HYPOTHETICAL}](x.png)` for an image.",), id="multiline-inline-code"),
            pytest.param((f"<!-- ![Chart of {HYPOTHETICAL}](x.png) -->",), id="html-comment"),
            pytest.param(("<!--", "![Each query names a source_model](x.png)", "-->"), id="multiline-html-comment"),
        ],
    )
    def test_image_syntax_in_inline_code_or_comment_is_not_scanned(
        self, doc_demo_storage: YAMLStorage, lines: tuple[str, ...],
    ) -> None:
        assert _checked(_md(*lines)) == []
        assert _failures(doc_demo_storage, *lines) == []

    def test_unclosed_backtick_does_not_hide_an_image_after_a_blank_line(self) -> None:
        md = _md("A stray ` here.", "", f"![Chart of {COUNT_JSON}](x.png) and `x`")
        assert _checked(md) == [(3, COUNT)]

    @pytest.mark.timeout(20)
    def test_many_unclosed_openers_scan_in_near_linear_time(self) -> None:
        runs = " ".join("`" * (k % 7 + 1) for k in range(60_000))
        md = _md("<!-- " * 60_000, runs, f"![Chart of {COUNT_JSON}](x.png)")
        assert _checked(md) == [(3, COUNT)]

    def test_inline_code_inside_alt_text_is_still_checked(self) -> None:
        assert _checked(_md(f"![Chart of `{COUNT_JSON}`](x.png)")) == [(1, COUNT)]

    def test_inline_code_before_image_keeps_it_not_standalone(self, doc_demo_storage: YAMLStorage) -> None:
        assert _failures(doc_demo_storage, NOT_A_QUERY, "`x` ![Each query names a source_model](x.png)") != []

    def test_image_after_inline_code_keeps_its_line(self) -> None:
        md = _md("Use `code` here.", "", f"![Chart of {COUNT_JSON}](x.png) and `![not]({HYPOTHETICAL})`")
        assert _checked(md) == [(3, COUNT)]


class TestAltTextChecks:
    def test_unmarked_failing_query_fails_naming_page_and_source_model_line(
        self, doc_demo_storage: YAMLStorage,
    ) -> None:
        failures = _failures(doc_demo_storage, "Text.", "", "![Before", f"{HYPOTHETICAL}](x.png)")
        assert len(failures) == 1
        assert failures[0].startswith("page.md:4: ")

    def test_alt_text_mentioning_source_model_without_query_fails(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(doc_demo_storage, "Text.", "", "![Each query names a source_model](x.png)")
        assert len(failures) == 1
        assert failures[0].startswith("page.md:3: ")

    def test_not_a_query_comment_exempts_image_from_guard(self, doc_demo_storage: YAMLStorage) -> None:
        assert _failures(doc_demo_storage, NOT_A_QUERY, "![Each query names a source_model](x.png)") == []

    def test_marked_image_is_exempt_and_next_image_is_checked(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(
            doc_demo_storage,
            MARKER,
            "    This chart uses a hypothetical `website_visits` column.",
            "",
            f"![Chart of {HYPOTHETICAL}](x.png)",
            "",
            f"![Chart of {HYPOTHETICAL}](y.png)",
        )
        assert [f.split(": ")[0] for f in failures] == ["page.md:6"]

    def test_marked_image_without_query_passes_the_guard(self, doc_demo_storage: YAMLStorage) -> None:
        assert _failures(doc_demo_storage, MARKER, "", "![Each query names a source_model](x.png)") == []


def test_readme_yields_its_headline_alt_text_query() -> None:
    headline = {
        "source_model": "orders",
        "measures": ["sum(order_total)", "change_pct(sum(order_total))"],
        "dimensions": ["stores.name", "date_trunc('month', ordered_at)"],
    }
    assert headline in [q.query for q in load_page("README.md").checked_queries()]


def test_shared_extractor_never_imports_slayer() -> None:
    probe = (
        "import sys; from scripts import doc_examples; "
        "assert doc_examples.load_page('README.md').checked_queries(); "
        "sys.exit(any(m == 'slayer' or m.startswith('slayer.') for m in sys.modules))"
    )
    result = subprocess.run([sys.executable, "-c", probe], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# The in-scope pages
# ---------------------------------------------------------------------------

_PAGES = [load_page(path) for path in in_scope_pages()]

_QUERIES = [
    pytest.param(page.path, q, id=f"{page.path}:{q.line}")
    for page in _PAGES
    for q in page.checked_queries()
]

_REFERENCES = [
    pytest.param(page.path, r, id=f"{page.path}:{r.line}:{r.reference}")
    for page in _PAGES
    for r in page.references
]


def test_every_in_scope_glob_matches_a_page() -> None:
    for pattern in IN_SCOPE_GLOBS:
        assert list(REPO_ROOT.glob(pattern)), pattern


@pytest.mark.parametrize(("path", "doc_query"), _QUERIES)
def test_documented_query_returns_rows(doc_demo_storage: YAMLStorage, path: str, doc_query: DocQuery) -> None:
    reason = query_failure(storage=doc_demo_storage, query=doc_query.query)
    assert reason is None, f"{path}:{doc_query.line}: {reason}"


def test_every_source_model_block_yields_a_query_or_is_marked() -> None:
    unextracted = [f"{page.path}:{b.line}" for page in _PAGES for b in page.unextracted_blocks()]
    assert unextracted == []


@pytest.mark.parametrize(("path", "ref"), _REFERENCES)
def test_documented_demo_reference_resolves(doc_demo_storage: YAMLStorage, path: str, ref: DocReference) -> None:
    reason = reference_failure(storage=doc_demo_storage, reference=ref.reference)
    assert reason is None, f"{path}:{ref.line}: {ref.reference}: {reason}"
