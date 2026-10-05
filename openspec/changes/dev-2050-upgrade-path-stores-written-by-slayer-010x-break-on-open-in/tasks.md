## 1. Tests first (pr-tests stage)

Every regression test uses raw dicts/YAML in the exact 0.10.x shape (see the Linear issue DEV-2050 for the shapes), never current-code constructors, and MUST fail on `main` before implementation. At plan time, 0.10.2 behaviour was reproduced with `uv venv <dir>` + `uv pip install --python <dir> motley-slayer==0.10.2` (served from the local uv cache in the sandbox). The generator then runs under that venv's python and drives 0.10.2's own `YAMLStorage` / `SQLiteStorage` / `SlayerQueryEngine` / `Memory` / `slayer import-cube`.

- [x] 1.1 Upgrade corpus: `tests/fixtures/upgrade/v0_10_2/generate.py` builds, with real `motley-slayer==0.10.2` in a throwaway `uv` venv:
  - a DuckDB data file;
  - a YAML store and a SQLite store, each containing:
    - an ingested model;
    - an `import-cube` of a project whose FK is not a dimension, with an undeclared target key too;
    - a multi-stage query-backed model whose stage-2 order is `rank(rev:sum)`;
    - an aggregation `SUM({value}) * '{n}'`, which works on SQLite;
    - `date_range` with `Z` / `+02:00`, slashed, `[]`, 1-element and 3-element values;
    - stored filters with legacy literals;
    - memories with queries;
  - `expected.json` holding 0.10.2's results for every recorded query.

  Commit the generated artefacts. `tests/test_upgrade_corpus_v0_10_2.py` opens a temp copy of each store at HEAD:
  - with the datasource present: everything loads, every model re-saves, re-ingest succeeds, and every query matches `expected.json`;
  - with the datasource entry removed: everything loads, and no connection is attempted.

  Verify: fails on main.
- [x] 1.2 `models/stored-upgrade` — refinement-gate scenarios:
  - v10 with no datasource entry loads and is written back at v14;
  - v10 with an unreachable Postgres (port 1) loads, with a spy proving refinement and engine creation are never called;
  - v5 with no datasource keeps the "unavailable for type refinement" error;
  - v7 is still refined.

  Verify: the v10 cases fail on main.
- [x] 1.3 `models/join-keys` — every new or modified scenario:
  - source-side hidden column, with the 0.10.2 rows, cast-free `ON` golden SQL on DuckDB/SQLite, write-back, a second load that leaves file bytes unchanged, and re-save passing;
  - target-side hidden column on v10 and v13 targets;
  - a dotted key still fails while its siblings load;
  - a SQLite corrupt-JSON sibling: the others load, exactly one warning.

  Postgres no-cast check in an integration test. Existing canonicalisation tests stay green. Verify: the new ones fail on main.
- [x] 1.4 `models/stored-upgrade` — rank scenarios:
  - a 0.10.2 query-backed model with a `raw_formula` rank order, executed on the YAML and SQLite backends, ranking descending;
  - a 0.10.2 memory query (YAML `.md` and SQLite);
  - a v13 model with a v5 source query and un-rewritten `raw_formula` comes out at model v14 / query v6, repaired;
  - a v3 memory with a v5 query comes out at memory v4 / query v6, repaired.

  Verify: fail on main.
- [x] 1.5 `models/stored-upgrade` — `date_range` scenarios:
  - offset bounds return the 0.10.2 rows on DuckDB (+ Postgres integration);
  - slashed bounds;
  - `[]`, 1-element and 3-element ranges give unfiltered rows;
  - memory offset bound (YAML + SQLite);
  - a version-less client query with an offset bound or `[]` is still rejected.

  Verify: the stored ones fail on main.
- [x] 1.6 `models/stored-upgrade` — filter-literal scenarios:
  - `ordered_at >= '…Z'` runs;
  - reversed operand order, a two-bound `>= … and <= …` range, an all-literal `IN`, and a dotted joined TIMESTAMP are all repaired;
  - `status = '2024/01/01'` (TEXT) and a mixed `IN` stay verbatim, asserted on the loaded query's filter text.

  Verify: the repaired ones fail on main.
- [x] 1.7 `models/document-isolation` — every scenario:
  - a typed `StoredDocumentLoadError` naming the document, with `__cause__`, for a validation failure and for corrupt YAML/JSON;
  - one unloadable model (expression join key): every other model re-saves, re-ingest succeeds, and the report names the bad model once;
  - one invalid memory: listing returns the rest, and `get_memory` on the bad id raises (YAML + SQLite);
  - exactly one warning per document per operation, including a multi-table re-ingest (`catch_warnings(record=True)` + `simplefilter("always")`);
  - `validate_models` marks the bad model invalid;
  - population inference and memory entity resolution fail closed with the bad model's error.

  Verify: fail on main.
- [x] 1.8 `aggregations/formula-templates` — every scenario:
  - SQLite `* '{n}'` and DuckDB `* CAST('{n}' AS DOUBLE)` return twice the sum;
  - the model re-saves without "never referenced";
  - `placeholder_names` includes `n` on every dialect;
  - `'{{n}}'` gives the literal `{n}`;
  - braces in a quoted identifier or comment stay inert;
  - `N'{n}'`, `E'{n}'` and dollar-quoted strings raise;
  - kwarg `n` set to each dialect's spelling of the `a'b\c` literal produces golden SQL on duckdb, postgres, mysql, clickhouse and bigquery;
  - `'{value}'` and a column default for `'{w}'` raise at save;
  - a column kwarg for a quoted `n` raises at binding;
  - unquoted `{n}` is unchanged.

  Reverse `tests/test_dev1934_formula_agg.py::test_string_literal_placeholder_is_inert` into the `'{value}'`-raises test (approved in plan). Verify: fail on main.
- [x] 1.9 Codex review of the tests against this change's specs. Fix the valid findings.

## 2. Typed load boundary and isolation helper

- [x] 2.1 Add `StoredDocumentLoadError(SlayerError, ValueError)` to the core errors module. Raise it from the single-document load boundary (`get_model`, `get_memory`, the memory row loaders) for decode, migration, refinement, validation and malformed-version failures, with the message `<ds>.<name>: <cause>` and `raise … from`. Tests asserting an inner class on a load now assert it on `__cause__`. Verify: 1.7's boundary tests pass, and the full unit suite stays green.
- [x] 2.2 Add `UnloadableDocumentWarning` (`kind="unloadable_document"`) plus its carrier `UserWarning` to `slayer/core/warnings.py`, joined to `AnySlayerWarning`, with the cause's CR/LF sanitised. Verify: unit test of the payload and carrier wording.
- [x] 2.3 Add the shared helper, which returns `(models, failures)` and catches only `StoredDocumentLoadError`, plus its memory twin. Add the per-operation failures accumulator (passed by keyword, warns once per document). Verify: 1.7's warn-once test passes.
- [x] 2.4 Move every enumeration site onto the helper with skip-and-warn, copying the warnings onto the response where one exists:
  - `_find_edge_named` / `_load_join_peers` (one load per save; terse trade-off comment);
  - ingestion `_stored_sanitized_identity_map`, `_scoped_models_for_validation` and the per-model loop (report entries blame the bad model);
  - `builtin_models(detailed=True)`, `models_summary`;
  - search `build_graph` / `_collect_model_subtree_canonicals` / `_collect_index_corpus` / `_lookup_bare_datasource_canonical`;
  - REST `GET /models`, MCP `inspect_model` and `edit_datasource`;
  - CLI `models list` and `refresh-samples`, profiling;
  - Flight and PG facade catalogs, demo startup;
  - `load_visible_models`, `_preload_join_targets`, `_collect_all_models`;
  - the memory listings (`list_memories`, the YAML `save_memory(id=…)` scan, `strip_dangling_entities_from_memories`, inspect learnings, search).

  Verify: 1.7 passes, and `grep` finds no remaining hand-rolled `get_model` loop over a datasource.
- [x] 2.5 Reimplement `validate_models` on the helper so it reports failures, and `_all_models_in_datasource` on the helper with a re-raise, covering population inference, bare-name scoping, `recommend_root_model`, detection scope and resolver. Verify: 1.7's validate and fail-closed tests pass.

## 3. Refinement gate and version steps

- [x] 3.1 Set `_LIVE_REFINEMENT_BELOW_VERSION = 8`, with a comment stating the rule (refinement repairs only docs written before ingest refined both kinds). Verify: 1.2 passes.
- [x] 3.2 `_rewrite_order` also rewrites a dict item's string `raw_formula`. Add stored-only steps: SlayerModel 13→14 (stamp `source_queries` + re-run the order rewrite), SlayerQuery 5→6 (re-run the rank rewrite + the `date_range` repair from 4.1), Memory 3→4 (stamp `query`). Bump `CURRENT_VERSIONS` to 14 / 6 / 4. Update tests pinning the current version numbers. Verify: 1.4 passes.

## 4. Legacy time literals

- [x] 4.1 Shared normaliser: strip the offset keeping wall-clock time; slashed → ISO; otherwise unchanged. Add the `date_range` repair (drop when length ≠ 2, terse comment on the 1-element case) to query 5→6. Verify: 1.5 passes.
- [ ] 4.2 Storage-side filter-literal repair:
  - sqlglot parse; direct-comparison operands in every layout; columns resolved through raw dicts, including dotted joins;
  - cheap skip for filters with no quoted literal;
  - wired into `_migrate_and_refine_on_load` for `source_queries`, and into the memory load path gated on raw memory version < 4.

  Verify: 1.6 passes.

## 5. Undeclared join keys

- [x] 5.1 Source-side hidden-column repair after canonicalisation: valid names only; type from the peer's raw target column when it is a valid `DataType`. Verify: 1.3's source cases pass.
- [x] 5.2 Target-side repair: the migrating table- or SQL-backed document scans raw siblings for incoming joins and appends its own hidden columns. Unreadable siblings are skipped; query-backed docs are untouched. Verify: 1.3's target and corrupt-sibling cases pass.

## 6. Quoted aggregation placeholders

- [x] 6.1 `SqlTemplate`:
  - collect `{name}` inside ordinary string-literal tokens, honouring `{{` / `}}`;
  - include them in `placeholder_names`;
  - raise on a placeholder inside a non-ordinary literal kind;
  - render by rebuilding `exp.Literal.string` with the literal values spliced in.

  Add one literal classifier shared by render and `check_aggregation_definition` (save-time default check). Terse comment on why non-literal bindings fail. Verify: 1.8 passes.
- [x] 6.2 Docs: one sentence in the aggregation-formula section of `docs/concepts/models.md` (quoted placeholders take a literal parameter's value; literal braces are `{{ }}`). Verify: the page builds and `zensical.toml` is unchanged.

## 7. Verification

- [ ] 7.1 Full unit suite `poetry run pytest -m "not integration"` is green.
- [ ] 7.2 Integration with the CI invocation, including the Postgres cases for join keys and `date_range`, is green.
- [ ] 7.3 `poetry run ruff check slayer/ tests/` is clean; `poetry run basedpyright` shows no baseline growth; `la-arch-check` passes.
- [ ] 7.4 Upgrade-corpus test green. Record in the PR description that the generator was run once against `motley-slayer==0.10.2`.
