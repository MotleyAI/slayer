## 1. Tests first (pr-tests stage)

- [x] 1.1 Write `tests/test_list_set_equality.py` covering every scenario of the `models/list-columns`, `queries/list-functions` and `queries/expression-grammar` deltas, executed on DuckDB and Postgres (pytest-postgresql, unit) with the seven-contractor fixture (Ann…Gil); verify each fails before implementation
- [x] 1.2 Add multi-stage cases (two and three stages: a grouped list projected raw, regrouped, compared, `count_distinct`-ed, `length`-ed and used as `partition_by=`), producer / join-back cases (local, cross-model, nested producer, window transform, `time_shift`, partitioned aggregate with a list grain member, NULL and reordered / duplicated lists), and raw-vs-grouped cases over the same list expression including `CASE` / `COALESCE` list expressions; verify they fail
- [x] 1.3 Add emission tests for every native-list dialect over every GROUP BY site, comparison, `IN`, `count_distinct`, window `PARTITION BY` and grain join-back, asserting BigQuery's `TO_JSON_STRING` key with `ANY_VALUE` display and Postgres's canonical array cast on `VARCHAR[]` / `TEXT[]` / integer widths / floats in both operand orders; verify they fail
- [x] 1.4 Add a key-kind exhaustiveness test that every kind classification (`KIND_POLICY`, traversal, reroot / prepend, type, phase and stage-schema inference) handles `ListSetKey`; verify it fails
- [x] 1.5 Add warning tests: a grouped list dimension and `count_distinct` over a list each emit one `WholeListWarning` (deduplicated per operation and key), surfaced as a Python warning, on `SlayerResponse.warnings`, in REST JSON and in the MCP rendering; none in raw-rows queries
- [x] 1.6 Add facade tests over the Postgres wire and Arrow Flight: `SELECT id, skills, row_count FROM contractors`, `SELECT skills, COUNT(*) … GROUP BY skills`, ordering by a scalar with a grouped list, ordering by the list by name / alias / ordinal (refused), and `SELECT skills_count_distinct FROM contractors` with the catalog listing it
- [x] 1.7 Add save / re-ingest / catalog round-trip tests for `allowed_aggregations: [count, count_distinct]` and the re-typing scenarios
- [x] 1.8 Extend the library-matrix `length` oracle (`tests/_dev2120_fixtures.py`) to distinct elements and add `check_lists` cases for grouping, comparison, `IN`, `count_distinct` and the ClickHouse / BigQuery NULL-as-`[]` rule, so the live ClickHouse, Trino, Snowflake and BigQuery suites run them
- [x] 1.9 With the user's approval per file, align the existing tests that pin the old behaviour: `tests/test_dev2172_list_dimensions.py`, `tests/test_dev2137_list_columns.py`, `tests/test_dev2137_list_functions.py` (`length` 3 → 2, sum 5 → 4), `tests/_dev2120_fixtures.py`, `tests/_dev1871_raise_ledger.py`, and the `test_dataset_source_table_returns_rows` column count in `tests/integration/test_metabase_e2e.py`

## 2. Core

- [x] 2.1 Add `ListSetKey` to `slayer/core/keys.py` with `children()` / `map_children()`, display, `KIND_POLICY` and every fail-closed dispatch; verify 1.4 passes
- [x] 2.2 Add `WholeListWarning` + `SlayerWholeListWarning` to `slayer/core/warnings.py`; verify the payload serialises on `SlayerResponse.warnings`
- [x] 2.3 Set `DEFAULT_AGGREGATIONS_BY_TYPE[ARRAY]` to `{count, count_distinct}` in `slayer/core/enums.py`, update the save-time rule in `slayer/core/models.py` and re-ingest re-typing in `slayer/engine/ingestion.py` together; verify 1.7 passes
- [x] 2.4 Extend `slayer/core/syntax.py` so an `IN` right-hand side may be all array literals and/or placeholders, mixed array / scalar items failing to parse; `bind_variables` writes a list variable as an array literal where it is one operand and leaves a whole-`IN` placeholder unwritten, the query / measure copy carrying its effective variables; add `parse_bound_expr` turning that placeholder into a `BoundVariable(name, value)`; verify the grammar scenarios parse / fail

## 3. Engine

- [x] 3.1 Add the one idempotent canonicalisation pass (after binding and coercion, before slot / grain construction) rewriting grouped list dimensions, list comparison / `IN` operands, `count_distinct` sources, `partition_by=` members and transform partition keys to `ListSetKey`, returning the updated bound query; raw-rows projections keep the list; verify interning tests in 1.2 pass
- [x] 3.2 Turn `check_list_dimension`, `_check_list_comparison`, `_check_list_in` and `_check_list_aggregation` into validators of the new rules (list-vs-list allowed with family check; list-vs-scalar, ordering ops and `BETWEEN` refused; `count_distinct` allowed; opaque lists refused in every set-reading position and in `length`); update `tests/_dev1871_raise_ledger.py` rows; verify the refusal scenarios pass
- [x] 3.3 Bind `IN` per the left side's type through `parse_bound_expr` at every binder parse site: `_bind_in` binds the left side first; a list left side takes a `BoundVariable` as one list operand (`L IN ({wanted})` ≡ `L = {wanted}`, `[]` binds), a scalar left side expands it into the whole right-hand side (an empty one failing with the variable's name), a written array literal never expands; verify the variable scenarios pass
- [x] 3.4 Emit `WholeListWarning` from the checker at grouped list dimensions and `count_distinct` over lists, deduplicated per (operation, key); verify 1.5 passes
- [x] 3.5 Keep `ARRAY<element>` on grouped list outputs in `StageSchema`; verify the multi-stage cases pass

## 4. SQL generation

- [x] 4.1 Add `list_set` and `list_key` hooks to `slayer/sql/dialects/base.py` (both refuse) and implement them on Postgres (canonical family array type, input cast before unnesting, both CASE branches typed alike), DuckDB, Snowflake, ClickHouse, Trino and BigQuery (`TO_JSON_STRING` key; NULL reads as `[]`); add them to `_LIST_HOOKS` in `tests/test_dev2169_dialect_library_support.py`; verify the base-refusal and emission tests pass
- [x] 4.2 Render `length` over a list as the size of `list_set`, on every native-list dialect; verify the library matrix passes
- [x] 4.3 Render `ListSetKey` through `list_set` in the one ValueKey renderer; render list comparisons, `IN` and `count_distinct` over `list_key(list_set(·))` with both operands coerced to the family's canonical array type
- [x] 4.4 Make every GROUP BY site in `slayer/sql/generator.py` consume a structured `(display, group_key)` dimension, projecting `ANY_VALUE(display)` where they differ; `dimension_values` reads the display; verify BigQuery emission over every site
- [x] 4.5 Make window `PARTITION BY` and grain join-backs read `list_key` from the slot's ARRAY type at the consuming site, `build_null_safe_eq` staying generic; verify the join-back and transform cases pass

## 5. Facade and agent surface

- [x] 5.1 Remove list-specific grouping refusals from `slayer/facade/translator.py` (`_validate_group_by`, `_reject_lossy_cast_in_implicit_grouping`) and confirm list ordering stays refused; verify 1.6 passes
- [x] 5.2 Confirm the catalog advertises `<list>_count_distinct` and keeps refusing other list metrics; verify the honest-catalog test (`tests/test_dev2172_honest_catalogs.py`) passes
- [x] 5.3 Update list-column rendering in `inspect` / `search` / `models_summary` to say a list groups and compares as a set, and the help topic `slayer/memories/help_content/01_models.md`; verify the rendering scenario passes

## 6. Architecture, docs, verification

- [x] 6.1 Apply the approved Axiom 3.2 wording verbatim to `architecture/semantics.arc42.md` (design.md decision 3) with `[enforced: test:tests/test_list_set_equality.py]`; run `poetry run la-arch-check`
- [x] 6.2 Update `docs/examples/16_list_columns/list_columns.md` (grouping and comparing lists, `length` meaning) and re-execute `list_columns_nb.ipynb` with `jupyter nbconvert --to notebook --execute --inplace`; update `docs/concepts/queries.md` (List columns), `docs/concepts/references.md` (Lists), `docs/interfaces/pg-facade.md:138` and `docs/interfaces/flight-sql.md:115`
- [x] 6.3 Run `poetry run pytest -m "not integration"`, the CI integration invocation, the live ClickHouse / Trino / Snowflake suites, and `poetry run pytest -m metabase_e2e tests/integration/test_metabase_e2e.py`; all pass
- [x] 6.4 Run `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors vs the baseline); both clean
