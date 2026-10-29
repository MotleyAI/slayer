Item ids (A1–A14, the B bullets, C, D) refer to the DEV-2173 issue body; design.md holds the decisions.

## 1. Tests (pr-tests stage)

- [x] 1.1 Nav-coverage guard test: every `.md` / `.ipynb` under `docs/` is in the `zensical.toml` nav. Allowlist: `docs/CLAUDE.md`, `docs/api_gaps.md`, and `docs/getting-started/flight-sql.md` with a forward pointer to DEV-2103. Verify: red today on the orphans.
- [x] 1.2 Issue-number guard test: no `DEV-<n>` in any file under `docs/` (all file types) except `docs/CLAUDE.md`, plus the exact baseline of the (path, token) pairs in `docs/interfaces/pg-facade.md` today, with a forward pointer to DEV-2172. Verify: red today.
- [x] 1.3 Doc-example parse guard: the schema-aware extractor and parse test described in design.md (Structural guards §3), with extractor fixture tests. Verify: the fixture tests pass and the guard runs green or red on real findings only.
- [x] 1.4 Idiom execution tests: one executable test per new bullet and scenario in `specs/mcp/tool-surface/spec.md`, each with concrete rows and expected output. The new ones: year-over-year `time_shift`, `ntile` order, `FILTER`, granularity calls, the fixed-grain computed dimension, time points, regex braces, null-aware tests, list filters, quoted / list / empty-list variables. Bullets already covered by `tests/test_mcp_surface_budget.py` / `tests/_mcp_idiom_fixtures.py` are left as they are. Verify: behaviour that exists passes; any red result is investigated before implementation.
- [x] 1.5 Marker tests: add the new guidance markers to `MARKER_OWNERS` (`FILTER (WHERE` → `measures`; `IS DISTINCT FROM` and `contains_any` → `filters`; the whole-`IN` binding marker → the `SlayerQuery.variables` field; the per-query-expression marker → `source_model`). Add a check that no query text advises inline-extension `columns` for a per-query column. Verify: red until 4.x lands.
- [x] 1.6 Retarget `tests/test_help_seed.py:472` / `:477` to `docs/reference/cli.md` / `docs/reference/mcp.md` (same assertions), and drop `interfaces/mcp.md` from `_MCP_DOCS` in `tests/test_docs_dev_1560.py`. Verify: red until 3.x folds the help sections in.
- [x] 1.7 Escape-regime test cleanup:
  - delete the python-specific tests (`tests/test_models.py::TestSubstituteVariables`, `tests/test_query_optional_blocks.py:131`, the invalid-regime check);
  - drop `escape=` from the regime-agnostic tests, running them with `backslash_escapes=False`;
  - drop every `escape="sql"` argument;
  - confirm SQL-only contract coverage (mandatory `backslash_escapes`, optional blocks, empty-list error, per-dialect escaping) and add what's missing.

  Verify: red only on the removed parameter.
- [x] 1.8 Update the `match=` strings at `tests/test_mode_b_placeholder_parse.py:71` and `tests/test_mode_b_variables_construction.py:64` to the new brace-error stem. Verify: red until 6.2.

## 2. Docs: wrong statements (A)

- [ ] 2.1 A1–A4, A7, A8: ranking order (`models.md:297`), the scalar allowlist (`formulas.md:490`), top-level measure wrappers (`formulas.md:506`), `collect` (`07_queries.md:40`, the `models.md` / `03_aggregations.md` catalogues, the empty-input rule), THEN / ELSE values (`formulas.md:545-547`). Verify: each cited line reads correctly against the code.
- [ ] 2.2 A5, A13: row-level expressions, not only columns, and to-one hops allowed (`time.md`, `references.md`, `formulas.md:33`, `03_aggregations.md:3-4`), including the temporal operands `CAST(… AS DATE)` and typed literals. Verify by reading the cited lines.
- [ ] 2.3 A6: variables in every Mode-B surface (`references.md:28`, `reference/mcp.md:105`); the deleted escaping regime (`models.md:515`); empty lists per the design's A6 decision (`queries.md:477-488`); the variables example page. Verify by reading the cited lines.
- [ ] 2.4 A9–A12: canonical column-type spellings (MCP `create_model` / `edit_model` text, `models.md:79`), the `db_type` description (`models.py:189-196`), lists not usable as group keys / aggregation inputs (`models.md:69`, `terminology.md:9`, `ingestion.md:44`, `help_content/01_models.md`), the per-query expression form (`06_joins.md:45-47`, `02_workflow.md:45-46`). Verify by reading the cited lines.
- [ ] 2.5 A14: the precise `==` sentence in `help_content/03_aggregations.md`. Verify by reading it.

## 3. Docs: missing rules (B) and site hygiene (C)

- [ ] 3.1 B / grammar (DEV-2101 bullets): FILTER, literal-only binding (booleans and numeric strings included), `IS [NOT] TRUE|FALSE`, operators, casts, interval and date literals, reject hints, identifiers and errors, the `reference/mcp.md:117-121` tables. Each at the place the issue names. Verify by reading.
- [ ] 3.2 B / row-level expressions (DEV-2119 bullets): the TimeDimension rules (`queries.md:175-196`) and ranking (`formulas.md:61-67`, `:444`, `:458-460`). Verify by reading.
- [ ] 3.3 B / portable library (DEV-2120 bullets):
  - JSON inputs and the `db_type` fields-table row;
  - literal string matching;
  - `json_value`, regex, coercion, braces;
  - the smaller gaps;
  - one `help_content` line each for `TRY_CAST`, `::`, `FILTER (WHERE …)`, `ILIKE`.

  Verify by reading.
- [ ] 3.4 B / lists (DEV-2137 bullets):
  - the Field descriptions for `Column.type` and `element_type`;
  - ingestion type mapping;
  - restrictions, plus a row in the `02_workflow.md` error table;
  - composition;
  - `inspect` / `models_summary` rendering;
  - profiling;
  - the smaller rules.

  Verify by reading.
- [ ] 3.5 C / orphans: fold the listed unique content into `reference/{cli,mcp,rest-api}.md`, then delete `docs/interfaces/{cli,mcp,python-client,rest-api}.md`. Verify: 1.6 passes and `grep` finds no inbound link to a deleted page.
- [ ] 3.6 C / reference gaps: add the 4 MCP tools, 3 REST routes and 5 CLI commands listed in design.md, each checked against `slayer/mcp/server.py`, `slayer/api/server.py` and `slayer/cli.py`. Verify by reading against the code.
- [ ] 3.7 C / nav: add notebook 08 under Tutorials as "MCP Introspection", after Aggregations. Verify: 1.1 passes.
- [ ] 3.8 C / issue numbers: remove every `DEV-<n>` from `docs/` outside the sibling-owned pages (full inventory across all file types first). Verify: 1.2 passes.
- [ ] 3.9 C / DEV-1768 drift items: case-sensitivity claim, `order[].column` conflict, `*:count` → `_count`. Each is verified and fixed where it remains. Verify by reading.

## 4. MCP query schema

- [ ] 4.1 Add the one-clause guidance to `measures`, `filters`, `SlayerQuery.variables` and `source_model` in `slayer/core/query.py`, replacing the inline-extension-column advice. Verify: 1.5 and the existing one-owner / sole-owner tests pass.
- [ ] 4.2 Measure the `query` input schema, set `QUERY_SCHEMA_BUDGET` to that size plus ~10%, and update its comment. Verify: `test_query_schema_within_size_budget` passes.

## 5. Other artefacts

- [ ] 5.1 `examples/comparisons/matrix.yaml` Q22 → `partial` with the right note; regenerate `matrix.json` with `export_matrix.py`. Verify: `matrix.json` diff shows only Q22.
- [ ] 5.2 Notebook 02, cells 10–12 and 14: rewrite to the direct day-of-week form, then `jupyter nbconvert --to notebook --execute --inplace`. Verify: the outputs are saved and the cells run clean.
- [ ] 5.3 Notebook 08: move the narrative from `inspect_model` to `inspect`, then re-execute in place. Verify: the outputs are saved and the cells run clean.

## 6. Dead code (D)

- [ ] 6.1 Remove the `escape="python"` regime and the `escape` parameter from `slayer/core/query.py`; drop the argument at `slayer/ir/variables.py:186`. Verify: 1.7 passes.
- [ ] 6.2 Reword the brace error at `slayer/core/syntax.py:259`. Verify: 1.8 passes.
- [ ] 6.3 Fix the stale comments and their non-forward issue / PR refs (`query.py:206-207`, `slayer/sql/window_detect.py:1-8`, `tests/test_sql_boolean_literal_filters.py:1-8`), and delete `_column_summary` (`slayer/mcp/server.py:470`). Verify: `ruff` and the unit suite pass.

## 7. Verification

- [ ] 7.1 Full unit suite (`poetry run pytest -m "not integration"`), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors), `poetry run la-arch-check`, `openspec validate dev-2173-docs-and-agent-facing-text-catch-up-with-the-sql-grammar-row --strict`, and the docs build (`uvx zensical build`). All green.
