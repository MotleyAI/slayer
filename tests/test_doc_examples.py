"""Every query example in the docs runs on the demo, or is marked as illustrative."""

from __future__ import annotations

import json
from typing import Any

import pytest

from slayer.storage.yaml_storage import YAMLStorage
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

    def test_dangling_demo_reference_fails(self, doc_demo_storage: YAMLStorage) -> None:
        failures = _failures(doc_demo_storage, "Run `slayer inspect jaffle_shop.orders.order_id`.")
        assert len(failures) == 1
        assert failures[0].startswith("page.md:1: jaffle_shop.orders.order_id: ")

    def test_resolving_demo_reference_passes(self, doc_demo_storage: YAMLStorage) -> None:
        assert _failures(doc_demo_storage, "Run `slayer inspect jaffle_shop.orders.order_total`.") == []


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
