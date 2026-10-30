## 1. Tests (pr-tests)

- [x] 1.1 Write `tests/test_dev2171_importers_preserve_meaning.py`: one Cube, one dbt and one OSI fixture over a DuckDB `orders` table holding every ambiguous form in the specs (collision in each surface, self-named transforms, filtered helpers, same-cube dotted refs, raw SQL calc measures, list columns, OSI array functions); assert executed values and refusal report entries; verify it fails at HEAD for the reasons the specs name
- [x] 1.2 Unit tests per spec scenario beside the existing importer tests (resolver, Cube reference forms / calc measures / join keys / FILTER_PARAMS bodies / views, dbt `data_type` table and datasource use, typing order incl. offline and unreachable, list-aggregation refusals, OSI mappings and literals); verify each fails at HEAD
- [x] 1.3 Cover the Codex gap list: optional and required FILTER_PARAMS bodies, quoted / case-folded identifiers and case-only ambiguity, composite and renamed join keys, root vs nested CTE / subquery scope, query-backed (sql-mode) sources, helper dedup by (operand, filter, type), current-spec vs legacy OSI overlays, facade name collisions and transitive helpers, unreachable datasource, probed lists with known vs unknown element types
- [x] 1.4 Test that `validate_models` proposes no deletion for an imported Cube and dbt model with a measure over a list column (datasource reachable)
- [x] 1.5 List every existing test that pins a behaviour this change reverses and get the user's consent before changing any of them
- [x] 1.6 Split the collision forms out to DEV-2175 (user-approved): strict xfails pointing at DEV-2175, interim collision-drop tests per importer, acceptance fixtures split into value-checked and dropped-with-report parts, the OSI shadowing test asserting the drop

## 2. Shared package

- [x] 2.1 Create `slayer/importers/` and apply the approved `architecture/model.c4` claim verbatim; verify `poetry run la-arch-check` passes
- [x] 2.2 Physical-reference resolver (design: Resolver pipeline steps 3, 5, 6) over a model's final column set; verify the resolver unit tests pass
- [x] 2.3 Move the aggregate translator from `slayer/osi/expression.py` into `slayer/importers/` with a per-importer operand policy (Cube: Mode-A helper for non-bare operands, FILTER → helper filter; OSI: Mode-B inline as today); verify the existing OSI expression and metric tests still pass
- [x] 2.4 Column-typing helper (design: Typing order) incl. dbt `data_type` parsing in the datasource dialect and the offline report entry with its reason; verify the typing unit tests pass
- [x] 2.5 `measure_dependency_problems` gains keyword-only column names and a join-path resolver and checks aggregation inputs; verify existing callers and the new tests pass

## 3. Cube

- [x] 3.1 Reserve source member names and build the source → emitted name map before translating any SQL; brace classification per the cube-import spec; verify the reference-form tests pass
- [x] 3.2 Route dimensions, CASE predicates, aggregate measures, measure filters and segments through the resolver; filtered helpers named `<measure>_col`; a construct reading a physical name a different member holds, and a self-named member, is dropped with a report; verify the collision-drop and filtered-helper tests pass
- [x] 3.3 FILTER_PARAMS: resolve each `body_template` separately with sentinels kept opaque; verify the FILTER_PARAMS tests and the existing `tests/test_cube_*filter_params*.py` pass
- [x] 3.4 Calculated measures through the shared translator; drop untranslatable ones with a report; verify the calc-measure tests pass
- [x] 3.5 Two-sided join-key resolution phase; a masked key drops the join with a report; verify the join-key tests pass
- [x] 3.6 View facades carry hidden dependency columns transitively; verify the view tests pass
- [x] 3.7 `import-cube` resolves `--datasource` and types columns via the helper; verify the CLI typing tests pass
- [x] 3.8 Resolve `{other}.x` as the joined cube's physical column (spec "Cube reference forms"; design step 2/5)

## 4. dbt

- [x] 4.1 Route dimension, measure (incl. `sum_boolean`) and entity `expr`s through the resolver; a construct reading a physical name a different member holds, and a self-named member, is dropped with a report; verify the collision-drop tests pass
- [x] 4.2 Read column `data_type` for semantic models' underlying dbt models regardless of `--include-hidden-models`; map via the typing helper; verify the `data_type` tests pass
- [x] 4.3 `import-dbt` resolves `--datasource` and connects independent of `--include-hidden-models`; verify the CLI typing test passes

## 5. OSI

- [x] 5.1 Route field expressions (current-spec and legacy overlay) through the resolver; a construct reading a physical name a different member holds, and a self-named member, is dropped with a report; verify the collision-drop tests and the existing OSI suites pass
- [x] 5.2 Refuse list aggregation inputs other than `COUNT`; verify the refusal tests pass
- [x] 5.3 Map `ARRAY_AGG(DISTINCT)`, `ARRAY_CONTAINS` / `= ANY`, `ARRAY_LENGTH` / `CARDINALITY` in metrics (Mode B) and in field expressions (rendered through `SqlDialect` list hooks); verify the mapping tests pass on DuckDB and the emission tests for Postgres, Snowflake and BigQuery
- [x] 5.4 Remove `_SAFE_STRING`; emit `''`, keep backslash; verify the literal test passes

## 6. Docs, arc42, gates

- [x] 6.1 One sentence per change in `docs/cube/cube_import.md`, `docs/dbt/dbt_import.md`, `docs/osi/osi_import.md` (including the NULL conventions of the OSI mappings); verify the pages build in `zensical.toml` nav
- [x] 6.2 Flip principle 21's tag with the approved wording; verify `poetry run la-arch-check` passes
- [x] 6.3 Relate DEV-1363 to DEV-2171 and comment on DEV-1363 that this PR closes it
- [x] 6.4 Run `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors) and `poetry run la-arch-check`; all pass
- [x] 6.5 Comment on DEV-2175 listing the xfails it lifts
