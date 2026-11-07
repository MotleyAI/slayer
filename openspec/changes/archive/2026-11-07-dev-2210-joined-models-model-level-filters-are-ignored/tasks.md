## 1. Tests (pr-tests stage; all fail before implementation except the controls)

- [x] 1.1 Extend `tests/test_dev2210_join_target_model_filters.py` (staged repro: semi-join, anti-join, to-one dimension / null filter, producer control) with every scenario of requirement "Model filters define the model's dataset" — null-extended pushdown, INNER edge, SQL-backed and query-backed targets, list-child parent, recursive filter read, filtered-root regression, isolated / windowed / shifted / date-range / spine evaluations on a filtered root — executed on SQLite and DuckDB (Postgres via pytest-postgresql where the helper supports it); verify they fail for the right reason
- [x] 1.2 Add the association scenarios (unproven-hop `count(*)` and join-target row counts, mixed own/joined filter NULL reading, inlined-vs-association lowering soundness via a generator force flag) and verify they fail
- [x] 1.3 Add filter-cycle tests: save refused at the engine and the storage door (direct, through a derived column), stored cycle fails the query, reverse edges without filter recursion accepted; verify they fail
- [x] 1.4 Add validation tests: save refused for a filter on an own measure and on a joined windowed column; a join target's stored invalid filter fails a query rooted elsewhere with `ModelFilterError`; verify they fail
- [x] 1.5 Add RLS tests (column and join rulesets, base table and a table only a filter introduces, `read_models` accounting) and Tier-1 emission tests (ClickHouse, BigQuery, SQL Server, Trino, MySQL, Snowflake) for the derived-table, inlined and `EXISTS` shapes; verify they fail
- [x] 1.6 Add `tests/test_law_relation_door.py` (AST scan of `slayer/sql/`: `_to_table` / `_embed_model_sql` only in `_backing_relation`, `_backing_relation` only in `_emit_relation`, model `.filters` only in the door plus the variable-surface collector); verify it fails on today's code

## 2. The relation door

- [x] 2.1 Split `_backing_relation` out of `_emit_relation` (spine, stage-read guard, splice checks, list-child refusal) with the model stamp on the outermost relation; verify the existing suite is green
- [x] 2.2 Make `_emit_relation` emit the filtered dataset: allocator-minted inner alias, filters entered through the Mode-A door in a frame rooted at the model, proven-to-one entries inlined as joins via the door, every other entry as a correlated `EXISTS` over the spine shape, recursion-stack backstop raising `ModelFilterCycleError`; verify 1.1, 1.2, 1.5 and 1.6 pass
- [x] 2.3 Delete the WHERE path: `ModeAFilter`, `PlannedQuery.mode_a_filters`, its `_lower_positions` entry, the root-only lift in `_emit_planned`, and the `mode_a_filters` ids in `_plan_empty_base_grain` / `_plan_src_row_filters`, replaced by the root-has-filters fact folded into `host_gated`; verify the isolated / windowed / shifted / date-range / spine scenarios pass

## 3. Validation and cycles

- [x] 3.1 Add `ModelFilterCycleError(SlayerError, ValueError)` to `slayer/core/errors.py` carrying the ordered model chain; verify by the cycle tests
- [x] 3.2 Extend `validate_model_filter` to judge each reference against its owning model (qualified joined references, derived-column closures) via the reference-closure machinery; keep its checker raise site; verify 1.4 and the raise-ledger tests pass
- [x] 3.3 Add the filter-dataset dependency walk (every model on every path of the filters' dependency closure) with cycle detection; verify 1.3 passes
- [x] 3.4 Query time: validate and cycle-check the filters of every model in the query's read set (`models_read_by` + transitive closure) in the checker; verify the query-time validation and stored-cycle tests pass
- [x] 3.5 Save time: run filter validation and the cycle check (cycles through the saved model only) in the storage door beside `validate_derived_columns` and in the engine save path; verify both-door tests pass

## 4. Architecture and docs

- [x] 4.1 Apply the approved wordings verbatim (design.md decisions 15–17): `architecture/semantics.arc42.md` Axiom 3.3, `architecture/python.sql.arc42.md` P16, `architecture/system.arc42.md` P18; verify `poetry run la-arch-check` passes
- [x] 4.2 Docs, one sentence each: `docs/concepts/models.md` › Model filters (applied wherever the model's rows are read, before any join; a joined row with no passing match reads as NULL), `docs/concepts/terminology.md` › Model filter, `docs/database-support.md` (ClickHouse 25.4+ for a model filter across an unproven or to-many hop); verify the docs build links nothing new

## 5. Gates

- [x] 5.1 Re-bless golden SQL changed by the root derived-table shape, checking each diff only changes the relation shape; verify `poetry run pytest -m "not integration"` is green
- [x] 5.2 Run `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline) and `poetry run la-arch-check`; verify all clean
