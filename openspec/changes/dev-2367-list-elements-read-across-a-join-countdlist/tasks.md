## 1. Tests (pr-tests stage)

- [x] 1.1 Create `tests/test_dev2367_list_elements_across_joins.py` (unit suite: in-process DuckDB via `tests/_dev2137_fixtures.py` `list_engine` / `all_datasets` / `contractors_dataset`) with one test per `queries/list-children` delta scenario: elements per assignment (alone and with `contractors.team`), saved `n_skills` / `n_go`, `collect` per assignment, from `contractors` by `assignments.id`, keyless parent, positions vs model filter and two FILTER aggregates in one query; verify each fails on the current code for the stated reason (binder error / ScopeLeakError), not a fixture error
- [x] 1.2 Add the identical-keyless-parents-across-a-join scenario with a small fixture (a keyless list dataset with two identical rows `['go']` plus a joined dataset reaching both from one row); verify it fails today
- [x] 1.3 Add one test per `queries/semantics` ADDED scenario: 230 from both populations (plain and by team: A 130, B 100), custom-aggregation default `w` 230 from both, `sum(rate * assignments.id)` 460 from both, undetermined inputs refused identically, redundant-`partition_by` + home-changing default; verify the population-dependent ones fail today
- [x] 1.4 Add the producer-kind matrix (D10): FILTER, ranked / `first` / `last`, windowed, attached-constituent and explicit-`partition_by` aggregations homed below the root, run from `contractors` and from `assignments`, asserting equal values or the same typed error class; where both refuse today the test pins equal refusals
- [x] 1.5 Add per-dialect rendering tests for the position (Postgres, DuckDB, BigQuery, Snowflake, ClickHouse, Trino): no self-join of `contractors`, one element expansion carrying its position (ClickHouse: one `ARRAY JOIN` of `(position, value)` tuples); execute the elements-per-assignment query on DuckDB and on Postgres via `pytest-postgresql` (unit, not `integration`), over lists with NULL elements and repeated values
- [x] 1.6 Apply the approved arc42 enforcement tags (design D13, verbatim) once the test file exists; verify `poetry run la-arch-check` passes

## 2. List-child identity (failure 1)

- [x] 2.1 Dialects: add `positioned` to `list_relation` in `slayer/sql/dialects/base.py` and every override (Postgres/Trino `WITH ORDINALITY`, BigQuery `WITH OFFSET`, Snowflake FLATTEN `INDEX`, DuckDB `ROW_NUMBER() OVER ()` inside the element subquery, ClickHouse one tuple array then one `ARRAY JOIN`); position numbered after NULL removal; never with `distinct`; verify 1.5 rendering tests pass
- [x] 2.2 Planner: `_association_entity` (`slayer/engine/compile/stages.py`) gives a list-child home identity keys = parent identity keys over the child→parent back hop (parent PK, else the parent augmented by `with_row_identity`) + the reserved element position; move `_AssociationEntity.entity_fields` / `host_grain` to value keys (design D3); return the augmented parent (D4)
- [x] 2.3 Generator: `_build_from_and_joins` (`slayer/sql/generator.py`) emits the augmented parent through `_emit_relation` and requests the position from `list_relation` only for a list-child association root; verify 1.1, 1.2 pass and no golden changes

## 3. Route by the resolved home (failure 2)

- [x] 3.1 Elaborator: classify each aggregate's home as `host` / `physical_target(path)` / `operand_dataset` / `unresolved` (design D6), produced where `home_path_for` / `resolve_aggregate_homes` run; verify by unit tests on the classification of the 1.3/1.4 shapes
- [x] 3.2 Replace the anchor-based test with one target-homed predicate in `is_cross_dataset_agg` (`slayer/core/keys.py`), `crossing_local_root_predicate` (`slayer/engine/cardinality/relationship_safety.py`), `combined_kind` (`slayer/engine/elaborate_env.py`), the routing helpers of `slayer/engine/compile/discovery.py` (`discover_roots` receives the classification) and all callers; verify 1.3 and 1.4 pass
- [x] 3.3 Carry the home through every compiler substitution that rebuilds an `AggregateKey` (`_strip_redundant_partitions` and any other), a missing home raising an internal error, never falling back to the anchor (D7); verify the redundant-`partition_by` test passes
- [x] 3.4 Any existing test that pins a refusal or value this change alters: STOP and ask the user for approval to align it (name each file and change) before editing

## 4. Docs, gates and hand-off

- [x] 4.1 Add the approved sentence to `docs/concepts/queries.md` (design D14)
- [x] 4.2 Run the local test scope (`la-affected-tests --base main` plus list / association / home / cross-model suites), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`; all green
- [ ] 4.3 After the PR is opened, comment on DEV-2277: its strict xfails `test_list_element_saved_metric` and `test_custom_aggregation_default_reads_like_from_its_home[list-element]` (tests/test_dev2277_stored_definitions.py) XPASS once this lands — whichever of the two branches merges second removes them
