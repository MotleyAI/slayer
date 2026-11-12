## 1. Tests first (pr-tests)

- [x] 1.1 Find every existing test pinning a removed behaviour. List each file and the change, ask the owner's approval to align it, then align it:
  - inline `source_model` acceptance (e.g. `tests/test_dev1922_source_model_union.py`, `tests/test_dev1966_inline_qb_ban.py`);
  - extension-column semantics;
  - textual Mode-A `{var}` substitution and identifier-position placeholders;
  - "parameterized unparseable source is skipped" in save validation;
  - dialect-SQL string aggregation arguments (the backslash-escaping quoted-argument case);
  - the profiling extension query.

  Search with `grep -rlE '"columns"|columns=|source_model=SlayerModel|"source_model": \{' tests`, the LSP references of `ModelExtension.columns` and `substitute_variables`, and `tests/*variable*`, `tests/*save_time*`, `tests/*formula_template*`.
- [x] 1.2 Mechanically respell the existing tests that use an extension column or an inline model only as scaffolding (~61 and ~16 files). An extension column becomes an expression in its position, or a column on a test-local stored model; an inline model becomes a stored model. No logic change. Verify `grep` finds the removed forms only in refusal and migration tests.
- [x] 1.3 `tests/test_dev2021_queries_carry_no_mode_a.py` (the semantics §4 P2.3 tag's test), on SQLite and DuckDB over a `secrets` table: each route of `queries/extensions` › Queries carry no Mode A is refused before SQL executes; each Mode-B spelling runs with hand-computed values. Include the legacy `source_model` extension with `columns`, a `SlayerModel` instance, REST 422 and the MCP query tool.
- [x] 1.4 `tests/test_dev2021_mode_a_variables.py`: every scenario of `queries/variables` › Mode-A surfaces bind variables as literals, across `Column.sql`, `Column.filter`, model `sql` and model `filters`:
  - literal positions, a numeric string, a boolean, an `IN` list, `O'Brien`;
  - refusal for an identifier, a quoted identifier, a function or type name, a comment, part of an `IN` list, and a string in a bare slot, at save and at query time;
  - `{? ?}` blocks with and without the variable, escaped braces;
  - the Cube `FILTER_PARAMS` fixtures still executing;
  - the dialect families (SQLite, DuckDB, plus Postgres via `pytest-postgresql`).
- [x] 1.5 `tests/test_dev2021_string_parameters.py`: `aggregations/formula-templates` scenarios. `weight='id * 2'` equals the unquoted form; `'length(sqlite_version())'` and the subquery fail with the typed parameter error; the Mode-B quoted literal `'a''b\c'`; `window='90d'` unchanged; a definition default still Mode A.
- [x] 1.6 `tests/test_dev2021_stored_conversion.py`: every scenario of `models/stored-upgrade` › Stored extension columns are inlined, over YAML and SQLite stores:
  - per use site: dimension, time dimension, measure name, filter, order, partition and ranking keys, `main_time_dimension`, extension measure, chained columns;
  - `filter`, sql-less, declared-type `CAST` (INT, DOUBLE, DATE, TIMESTAMP, BOOLEAN; TEXT and UNKNOWN bare);
  - qualification through a named edge and an alias;
  - every unconvertible case (inline source, non-Mode-B text, `ARRAY`, hidden sql-less column, shadowing, cycle, `join_pairs` on either side, a non-expression position) failing to load with stored bytes unchanged and other documents loading;
  - write-back for query-backed models and saved queries; memories converted on each load without write-back;
  - the warnings; recorded rows and result keys preserved.
- [x] 1.7 `tests/test_dev2021_column_sql_statement.py`: `models/column-definitions` › Column.sql is one expression.
- [x] 1.8 Profiling and usage-reporting tests: min/max over physical, derived, renamed, quoted and array columns on SQLite and DuckDB; the `inline_source` flag on extensions.
- [ ] 1.9 Run `poetry run pytest -m "not integration"`: the new tests fail for the expected reasons and nothing else regresses from the respelling.

## 2. Query payload: Mode B only

- [ ] 2.1 `slayer/core/query.py`:
  - `ModelExtension` drops `columns`, and a typed error rejects them (this also covers the legacy form);
  - `SourceSpec` becomes `str`, and an object without `source_name` (dict or `SlayerModel`) is refused with the inline-model error;
  - the field descriptions of `source_model` and `extensions` are updated.

  Verify: tests 1.3 pass and basedpyright reports every leftover `SlayerModel` source branch.
- [ ] 2.2 Delete every inline-model and extension-column branch, each verified by its consumer's tests:
  - `slayer/engine/{extensions,bundle_builder,stage_ordering,model_reads,plan,profiling}.py`;
  - `slayer/ir/{variables,source_bundle}.py`;
  - `slayer/live_schema/schema_drift.py`, `slayer/memories/resolver.py`, `slayer/inspect/model_render.py`;
  - `slayer/usage_reporting/features`, `slayer/osi` export, `slayer/storage` dict walkers, `slayer/api/server.py`, `slayer/mcp/server.py`.
- [ ] 2.3 `slayer/engine/param_binding.py`: a query-side string argument naming a placeholder is parsed by `slayer.core.syntax` and bound as its unquoted spelling; definition defaults keep the Mode-A path. Verify tests 1.5.
- [ ] 2.4 Profiling: non-array columns use `min(col)` / `max(col)`, arrays keep the element expansion. Fix the SQLite temporal typed render if the tests expose it. Verify tests 1.8.

## 3. Mode-A variables as literals

- [ ] 3.1 Replace textual Mode-A substitution with a structural binder:
  - `{? ?}` blocks first;
  - sentinel placeholders, parsed in the model's dialect;
  - replace each sentinel node with a literal node, by the Mode-B rules;
  - refuse other positions with the literal-values error.

  It goes through the one Mode-A door (python.sql P4). Verify tests 1.4.
- [ ] 3.2 Run the position check at model save (`validate_for_save`, importers included) and at query binding, and rework save validation's parameterized-source parse (literal placeholders, no unparseable skip). Verify tests 1.4 and the save-validation tests.
- [ ] 3.3 `Column.sql` one-expression check at construction. Verify tests 1.7.

## 4. Stored conversion

- [ ] 4.1 `slayer/storage/migrations.py`:
  - `SlayerQuery` v13 → v14 (stamp only);
  - bump `SlayerModel` and `Memory`;
  - a stored-only marker so fresh versioned input is never converted.

  Verify the registry tests.
- [ ] 4.2 Model-aware conversion in the storage load pipeline (`slayer/storage/base.py`), beside `_repair_source_query_filters`. Bind each stored query with the static query scope builder, with the extension columns present. Inline in dependency order with key-preserving names and canonical-path qualification. Raise the typed unconvertible error inside `stored_document_boundary`. Warn once per converted column. Write back models and saved queries; memories without write-back. Verify tests 1.6 and the 0.10.2 / 1.8.2 corpus tests.

## 5. Architecture and docs

- [ ] 5.1 Apply the approved `semantics.arc42.md` §4 P2 sub-clause 2.3 verbatim (design.md decision 6). Run the enforcement bundle: `la-arch-check`, `npx -y likec4@1.47.0 validate architecture`, `poetry run basedpyright`.
- [ ] 5.2 Docs:
  - `docs/concepts/queries.md`, including the "Trusted input" note and every statement describing unquoted `{var}` as raw substitution, plus `docs/reference/mcp.md`, `docs/getting-started/mcp.md`, `docs/concepts/ingestion.md`;
  - the help memories under `slayer/memories/help_content`;
  - examples 02, 06, 08, 13 and 14 (14 moves from an inline model to a stored model), re-executing every touched notebook with `jupyter nbconvert --to notebook --execute --inplace`.

  Verify with `grep -rnE 'inline model|"columns": \[|source_model": \{' docs slayer/memories`.
- [ ] 5.3 Run `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors against the baseline). All green.

## 6. After DEV-2102 archives and before this change archives

- [ ] 6.1 Merge `origin/main` once DEV-2102 has archived. Re-run `openspec validate --strict`. Rebase this change's MODIFIED texts on the archived requirements if DEV-2102 changed them in review (needs the owner's OK of the exact edit, per the frozen-plan rule).
- [x] 6.2 `openspec/specs/queries/extensions/spec.md`'s `## Purpose` (written by DEV-2102) still says "columns, measures and joins". A delta cannot modify Purpose, so ask the owner to approve the one-word removal as a direct spec edit in this PR.
- [ ] 6.3 After merge: ask the owner to close DEV-2044 as absorbed by this change.
