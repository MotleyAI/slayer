## 1. Tests (pr-tests stage)

- [x] 1.1 Identity test (extend `tests/test_mcp_server_runtime.py::TestServerIdentity`): title, description, website URL; two icons, light first; each `src` strictly parsed (`data:image/png;base64,` prefix, valid base64, PNG signature, IHDR 180×160), MIME `image/png`, sizes `["180x160"]`, themes `light`/`dark`; dark payload byte-equal to `docs/SLayer.png`. Verify: fails before implementation.
- [x] 1.2 Pinned tool table test: `tools/list` name set == the spec table's 28 names; each tool's title and four hints equal its row, none null. Verify: fails before implementation.
- [x] 1.3 Registration guard test: registering through the `tool()` wrapper without `title` or `hints` fails the server build. Verify: fails before implementation.
- [x] 1.4 Output schema test: every tool's output schema is an object whose only property is the required string `result`. Verify: passes today (pins existing behaviour).
- [x] 1.5 Closed-world test: seed a datasource, table-backed and query-backed datasets, a memory, a priority list (plus an ontology and mapping where the ontology extra allows); build the server with a `client_factory` that records and raises; call each closed-world tool with valid arguments; assert the factory was never invoked and no call failed for want of a client.
- [x] 1.6 Read-only test: same seed; for each read-only tool (point and collection forms of `inspect`; `query` and `describe_datasource` against a working in-process SQLite/DuckDB datasource) snapshot the stored definitions (datasources, datasets minus sampled values, semantic models, ontologies, mappings, memories, priority) before and after; assert equal.

## 2. Server metadata

- [x] 2.1 Generate `slayer/mcp/icons/slayer-light.png` (alpha of `docs/SLayer.png`, RGB `#091d31`) with a throwaway Pillow run (`uvx --from pillow`); copy `docs/SLayer.png` to `slayer/mcp/icons/slayer-dark.png`. Verify: both 180×160 PNGs; dark byte-equal to `docs/SLayer.png`.
- [x] 2.2 Extend `_import_mcp_sdk` to also return `Icon` and `ToolAnnotations` (no top-level mcp import); pass `title`, `description`, `website_url` and the two data-URI icons (light first) to the server constructor. Verify: test 1.1 passes; existing SDK-range ImportError tests still pass.

## 3. Tool metadata

- [x] 3.1 Make `title` and `hints` required keyword arguments of the local `tool()` wrapper; one helper builds `ToolAnnotations` with all four hints set. Verify: test 1.3 passes.
- [x] 3.2 Give every `@tool(...)` its title and hints per the spec table. Verify: test 1.2 passes.
- [x] 3.3 Description corrections (design decision 16) in the `create_dataset`, `ingest_datasource`, `delete_datasource`, `delete_dataset` and `edit_datasource` docstrings, within the 2048-char budget. Verify: `tests/test_mcp_surface_budget.py` passes.
- [x] 3.4 Verify tests 1.4, 1.5 and 1.6 pass; if one fails, the implementation contradicts its spec row — stop and raise it, never adjust the table on your own.

## 4. Docs

- [x] 4.1 `docs/reference/mcp.md`: add "Server metadata and tool annotations" (server fields, the four criteria, the per-tool hint table); correct the `delete_datasource` row (cascade) and the `create_datasource` / `ingest_datasource` rows (re-type exception to "never overwrites"). Verify: `tests/test_doc_examples.py` passes; no new page, so `zensical.toml` nav unchanged.

## 5. Gates

- [x] 5.1 Run the local test scope, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline) and the enforcement bundle (`la-arch-check`, `npx -y likec4@1.47.0 validate architecture`). Verify: all green.
