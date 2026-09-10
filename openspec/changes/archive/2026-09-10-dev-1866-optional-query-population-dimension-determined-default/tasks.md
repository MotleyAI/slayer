# Tasks — optional query population

## 1. Tests first (spec-tests stage; all fail before implementation)

- [x] 1.1 Fixtures: `tests/_dev1866_fixtures.py` — a datasource with a unique-minimal
  topology (orders→customers→regions, to-one declared), a tie topology, an
  unknown-cardinality hop, a two-datasource name collision, and SQLite/DuckDB
  seeders; verify import + smoke test.
- [x] 1.2 `tests/test_dev1866_inference.py` — selection unit tests: canonical
  customers.region+orders.total pin, filter-as-hidden-dimension pull, measure-typed
  and saved-measure filters excluded, computed-dimension aggregate-boundary rule,
  model-level filters excluded, variable-masking rule, raw-row mode, dedup of items.
- [x] 1.3 `tests/test_dev1866_fail_closed.py` — tie, no viable candidate, empty
  determination set, ambiguous hop, not-provably-to-one hop, datasource zero/multiple
  candidates, sibling-anchored stage refs; each asserts `PopulationInferenceError`
  payload (reason kind, candidates) and stable message prefix.
- [x] 1.4 `tests/test_dev1866_execution.py` (SQLite + DuckDB, executed values) —
  rootless query equals explicit twin on sql/data/columns/attributes/warnings with
  the expected metadata difference; NULL-attachment canonical rows; raw-row rows.
- [x] 1.5 `tests/test_law_population_invariance.py` — law-harness style: same query
  ± a measure ⇒ identical inferred population and row set.
- [x] 1.6 `tests/test_dev1866_reporting.py` — population/population_inferred across
  execute, dry_run, explain, cache miss/hit/refresh, client decode, explicit-model
  and inline-model/extension queries, run-by-name.
- [x] 1.7 `tests/test_dev1866_recommend_alignment.py` — entity-type classification
  (saved measure attachment), dimension-only objective, root_hint feasible /
  infeasible / malformed, coverage criterion, parity with engine inference.
- [x] 1.8 Surface tests: REST rootless body accepted end to end
  (`tests/test_api_server.py` additions) and MCP tool optional param
  (`tests/test_mcp_*` additions); SlayerQuery-without-source_model validation and
  round-trip in `tests/test_migrations.py` additions.

## 2. Core + engine implementation

- [x] 2.1 `slayer/core/query.py`: `source_model` optional default None (validators
  and `strip_source_model_prefix` no-op on None); verify 1.8 serialization tests.
- [x] 2.2 `slayer/core/errors.py`: `PopulationInferenceError` with structured
  payload + stable prefix; verify 1.3 error-shape assertions.
- [x] 2.3 `slayer/engine/population.py`: item extraction (design §3), datasource
  scoping with `prefer_data_source` pin, per-candidate binding probe with
  `provably_to_one` hop check (design §2), hop-sum selection (design §4); verify
  1.2/1.3 pass.
- [x] 2.4 `slayer/engine/query_engine.py`: pre-bundle inference pass for main +
  named stages, storage-consult comment update; verify 1.4/1.5 pass.
- [x] 2.5 Reporting plumbing through `_Prepared` and every `SlayerResponse`
  construction site + client model fields; verify 1.6 passes.

## 3. Recommendation + surfaces

- [x] 3.1 `recommend_root_model` on the shared core: entity-type classification,
  attachments in `item_paths`, root_hint/coverage updates; verify 1.7 passes.
- [x] 3.2 REST (`slayer/api/server.py`) and MCP (`slayer/mcp/server.py`) accept
  omitted `source_model`; docstrings updated; verify 1.8 passes.

## 4. Docs + architecture + hygiene

- [x] 4.1 `docs/concepts/queries.md`: source_model row optional, default rule +
  rootless example, recommend repositioned as explain surface; verify docs grep
  finds no "required" claim for source_model.
- [x] 4.2 `.claude/skills/slayer-query.md` (+ overview if it claims mandatory
  source_model) and one concise CLAUDE.md conventions line; verify by grep.
- [x] 4.3 `architecture/semantics.arc42.md` axiom 12 tag → `[enforced:
  test:tests/test_law_population_invariance.py]`; verify
  `poetry run python tools/arch_check.py` passes.
- [x] 4.4 Full gate: `poetry run pytest -m "not integration"`, ruff, lint-imports,
  arch_check, basedpyright, `openspec validate --strict`; all green.
