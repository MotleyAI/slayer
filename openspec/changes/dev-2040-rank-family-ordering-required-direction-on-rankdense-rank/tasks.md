## 1. Tests first (pr-tests stage)

Expected values for the NULL-inner cases come from raw-row oracles over the `sales`
fixture (Void's total is NULL). The oracle used at plan time reproduces the corpus's
old `810 / 43` and gives `810 / 33` under NULL→NULL.

- [x] 1.1 Direction syntax and binding tests: every scenario of "Rank-family ordering
  direction" (asc/desc/synonyms/non-numeric/partition_by/distinct keys/missing/invalid/
  forbidden/ntile-percent_rank ascending), executed on SQLite and DuckDB. Verify: they
  fail against the current code.
- [x] 1.2 Importer-parity tests: identical kwargs through `core/formula.py`
  `parse_formula` and the binder raise the same `TransformArgumentError`. Verify: they fail
  now.
- [x] 1.3 NULL-semantics tests: every scenario of "Rank-family NULL inputs rank NULL",
  covering all four functions with mixed-NULL, all-NULL-partition and partitioned
  inputs on SQLite and DuckDB, plus T-SQL / postgres emission. Verify: they fail now.
- [x] 1.4 Golden emission tests for the rank family on postgres, sqlite, duckdb, tsql and
  bigquery, recorded after implementation. Verify: the golden files exist and are pinned.
- [x] 1.5 Naming tests: the "Rank direction spelled as its bare value" scenario. Verify:
  they fail now.
- [x] 1.6 Migration tests: every scenario of "Stored rank calls without a direction load
  as descending":
  - stored v12 model with write-back to v13;
  - `source_queries` across all Mode-B fields, nested calls and colon syntax;
  - unversioned model with an unversioned nested query, and an inline-query
    `source_model`;
  - YAML and SQLite memories, unversioned and with an unversioned `query`;
  - fresh payloads and current-version payloads still error;
  - an explicit old version is filled in;
  - untouched cases (string literal, `.rank(`, explicit direction, `ntile`/`percent_rank`,
    `Column.sql`);
  - idempotence;
  - an untokenisable formula still loads.
  Plus unit tests of the stored-only gate in `migrate()`. Verify: they fail now.
- [x] 1.7 Mechanical fill of the ~98 existing test files that spell bare `rank(` /
  `dense_rank(`: add `direction='desc'`. Approved test-logic changes, each limited to
  what the plan dictates:
  - expected values where an inner is NULL become NULL (e.g. the Void cell; `_dev1946` /
    `_dev1919` oracles; the global `810 / 43` → `810 / 33`);
  - unnamed rank result keys gain `_desc` / `_asc`;
  - `ntile` / `percent_rank` expected values flip to ascending.
  List every file whose expected values changed in the pr-tests handoff. Verify:
  `poetry run pytest -m "not integration"` shows only intended failures.
- [x] 1.8 Codex-review the tests against the specs.

## 2. Core rule and error

- [x] 2.1 Add `TransformArgumentError(QueryTypeError)` to `slayer/core/errors.py`.
  Verify: the error tests from 1.1 can import it.
- [x] 2.2 Move the direction synonym table out of `core/query.py` into a core module that
  `OrderItem` and the new rule both import, and add the core direction validator
  (required / forbidden / literal / normalise / raise). Verify: `OrderItem` tests still
  pass.
- [x] 2.3 `core/formula.py`: route `_parse_transform_kwargs` through the validator, with
  `direction` admitted for `rank` / `dense_rank`. Verify: 1.2 passes.

## 3. Binding, naming, emission

- [x] 3.1 `engine/binding.py`: call the validator in `_bind_transform_params`, store
  `("direction", ...)` in `TransformKey.kwargs`, and move the other transform-kwarg
  `ValueError`s onto `TransformArgumentError`. Verify: 1.1 binding/error scenarios pass.
- [x] 3.2 Render `direction` as its bare value in the canonical formula text used for
  derived keys. Verify: 1.5 passes.
- [x] 3.3 `sql/generator.py`: emit `ORDER BY v ASC|DESC` per `direction` for `rank` /
  `dense_rank` and `ASC` for `ntile` / `percent_rank`, all wrapped in the NULL→NULL shape
  (design decision 5) as sqlglot AST. Verify: 1.1 and 1.3 pass on SQLite and DuckDB, and
  the T-SQL emission test passes.
- [x] 3.4 Record the golden baselines and re-bless any existing golden SQL that changed
  only by the CASE wrapper, `ASC` or the direction. Review each re-blessed diff. Verify: 1.4
  and the golden suites pass.

## 4. Lazy migration

- [x] 4.1 `storage/migrations.py`: add the `stored_only` registration flag and its gate in
  `migrate()`. Verify: the gate unit tests from 1.6 pass.
- [x] 4.2 Add the token-level rewrite function in `storage` (stdlib `tokenize` only).
  Verify: the rewrite edge-case tests pass.
- [x] 4.3 Register the stored-only steps `SlayerModel` 12→13, `SlayerQuery` 4→5 and
  `Memory` 2→3, including the nested-query stamping (design decision 2), and bump
  `CURRENT_VERSIONS`. Verify: the 1.6 model/query/memory scenarios pass.
- [x] 4.4 Stamp `version: 1` on unversioned stored dicts in `_migrate_and_refine_on_load`
  and the YAML / SQLite memory load sites. Verify: the unversioned-legacy scenarios pass.

## 5. Agent-facing text, docs, examples

- [x] 5.1 Update the rank-family line of the `query` tool description in
  `slayer/mcp/server.py`, the suggestion strings in `slayer/sql/window_detect.py` and
  `slayer/core/errors.py`, and `slayer/memories/help_content` if it spells a rank call.
  Verify: grep finds no bare `rank(` / `dense_rank(` in `slayer/` outside migrations and
  tests.
- [x] 5.2 Update the docs: `docs/concepts/formulas.md` (function table and rank section:
  direction, ascending `ntile` / `percent_rank`, NULL→NULL, `_asc` / `_desc` keys),
  `queries.md`, `models.md`, `references.md`, `docs/database-support.md`,
  `docs/dbt/dbt_import.md`, `docs/osi/osi_import.md`, and the `01_dynamic`,
  `05_joined_measures`, `07_aggregations` and `15_duckdb` example pages. Verify: grep
  finds no bare rank call in `docs/`.
- [x] 5.3 Update `examples/` (`embedded`, `clickhouse`, `snowflake`, `verify_common.py`,
  `comparisons/matrix.yaml` + `probes.yaml`). Verify: grep is clean and the matrix /
  probe checks in the unit suite pass.
- [x] 5.4 Re-execute every edited notebook in place
  (`jupyter nbconvert --to notebook --execute --inplace`). Verify: committed outputs are
  fresh and the notebook suite passes.
- [x] 5.5 Write the release-notes entry in an untracked `RELEASE_NOTES_*.md`: required
  direction, the `ntile` / `percent_rank` flip, NULL→NULL, renamed unnamed rank keys, lazy
  migration. Never stage it.

## 6. Gates

- [ ] 6.1 `poetry run pytest -m "not integration"` is fully green.
- [ ] 6.2 The integration suite with the CI invocation from CLAUDE.md is green
  (Postgres locally).
- [ ] 6.3 `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new
  errors vs the baseline) are clean.
- [ ] 6.4 `uvx --no-build --from living-architecture==0.2.1 la-arch-check` is clean.
- [ ] 6.5 `openspec validate dev-2040-rank-family-ordering-required-direction-on-rankdense-rank --strict`
  passes.
