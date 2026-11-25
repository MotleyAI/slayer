## 1. Tests first (pr-tests)

- [x] 1.1 Write `tests/test_dev2432_windowed_columns.py` with the probe fixture (`orders` 7 rows, `customers` 3 rows, `slots` / `slot_src` for keys) on SQLite and DuckDB, one executed test per scenario of `specs/models/field-expressions/spec.md` › A windowed field is a column of its dataset, each asserting the spec's hand-oracle values; verify every test fails on the pre-change code (invalid SQL, a refusal, or a wrong value).
- [x] 1.2 In the same file, add explicit tests for: a three-level window chain and a cyclic windowed-field chain refused by `DatasetFieldCycleError`; a windowed key on the source side, the target side and both sides, plus a correlated semi-join (filter across the fanning reverse); a masked windowed field as dimension, aggregation source, query filter and order key; `OVER (` inside a string literal and a window inside a scalar subquery classified correctly; the session-policy scenario on the policy-enabled engine path; verify each fails on the pre-change code where the behaviour is new.
- [x] 1.3 Add the portable-refusal tests (`specs/models/field-expressions/spec.md` › A portable windowed field is refused at both save doors): engine door, storage door with validation on, and the query-time backstop for a dataset stored unvalidated; verify the storage-door test fails today.
- [x] 1.4 Add the model-filter tests of `specs/models/model-filters/spec.md` (own-dataset cycle direct and through a derived column at both doors, error names `ranked`; cycle through a joined windowed column's definition; stored own windowed filter fails the query; joined windowed filter accepted at both doors and evaluated); verify the new ones fail today.
- [x] 1.5 Align the approved existing tests listed in design.md decision 17 (and nothing else without approval); verify they fail on the pre-change code exactly where the plan changes behaviour.

## 2. Classification and binding

- [x] 2.1 Classify a resolved field as windowed when its runnable dialect entry's sqlglot parse contains an `exp.Window` node, once at resolution (`engine/definitions.py`), carried on the resolved field; verify the string-literal and scalar-subquery classification tests pass.
- [x] 2.2 Bind a windowed-field reference as a column of its dataset's materialised relation, never as an inline expansion; remove `binding._reject_windowed_field_sql`; verify the query-filter scenario and the aligned `test_sql_generator.py` / `test_reference_semantics.py` tests pass.
- [x] 2.3 Make `sql/field_expansion` stop at a windowed field (the materialised column), keeping the field `filter` mask as the consumer's `CASE WHEN` around the column; verify the derived-over-windowed and masked-field tests pass.

## 3. Plan and emission

- [x] 3.1 Order a dataset's windowed fields into dependency layers at the relation door, and once the statement is built drop each materialised column no reference reads (and any layer left empty); verify by the window-over-windowed and three-level-chain tests, plus a dry-run test that a query reading no windowed field emits no window.
- [x] 3.2 In `_emit_relation`, emit one AST-built materialisation subquery per layer after `_filtered_dataset` and before `_numbered_relation`, with aliases minted by `sql/naming.py` and to-one joins of a window's definition inside its layer, each joined relation through the same door; verify the dimension, aggregation-source, FILTER, partition-key, ranking-key, order-key, time-dimension, join-target, to-one-joined, filtered-dataset and SQL-backed tests pass.
- [x] 3.3 Type `SemiJoinHop`'s computed join keys (physical / computed / materialised windowed) and render the materialised column in every `_JoinOps` consumer; verify the relationship-key and semi-join tests pass.
- [x] 3.4 Verify the session-policy test passes (policy rewrite beneath the window) and the scope-closure validator stays green.

## 4. Model filters and save doors

- [x] 4.1 In `engine/field_dependency` make a windowed-column reference add its owner to the filter dependency path, name the windowed column in the own-dataset cycle message, and delete the windowed branch of `elaborate_env.validate_dataset_filter`; verify the model-filter tests and `test_dev1450fix_model_filter_derived.py` pass at both doors and at query time.
- [x] 4.2 Refuse a portable windowed field at the storage save door with the engine door's `IllegalWindowInFilterError`; verify the portable-refusal tests and the aligned `test_binding.py` test pass.

## 5. Harness, docs, gates

- [x] 5.1 Apply the approved Axiom 1 sentence and tag (design.md decision 5) to `architecture/semantics.arc42.md` verbatim; verify `la-arch-check` passes.
- [x] 5.2 Add one sentence on windowed fields to the field-definition page under `docs/concepts/` (raw-`OVER` refusals in `docs/concepts/references.md` stay); verify the page is in `zensical.toml` nav.
- [x] 5.3 Update the module docstrings that describe the removed refusals (`sql/window_detect.py`, `engine/field_dependency.py`) to the new behaviour; verify `ruff check slayer/ tests/` and `la-typecheck` pass.
- [x] 5.4 Run the local test scope (`la-affected-tests --base main` plus the aligned and new files), `npx -y likec4@1.47.0 validate architecture`, and fix until green.
