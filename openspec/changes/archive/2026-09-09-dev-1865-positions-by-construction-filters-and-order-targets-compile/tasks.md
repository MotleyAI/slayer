## 1. Typing pass (engine)

- [x] 1.1 Implement resolve-then-type for filter conjuncts (split on top-level AND; resolve against existing bindings incl. computed-dim attached slots; field = aggregate-free + projected-field-legal, else measure = declared-measure-legal, else typing error naming both failures) and verify with unit tests over the positions-spec typing scenarios
- [x] 1.2 Route order targets (after MIN/MAX desugar) through the same pass, retiring the ad-hoc unsupported-ORDER-BY `ValueError`; verify the untypeable-order-target test gets the typing error
- [x] 1.3 Make the mixed-grain OR, raw-rows measure-filter, and DEV-1825 shapes flow through typing (error, error, legal respectively) and verify with the error-message and executed-value tests

## 2. Plan representation

- [x] 2.1 Add `masks: list[MaskEntry]` (slot id, typing, stratum) to `PlannedQuery`; compile each conjunct to a hidden `ValueSlot`; verify plan-level tests see one mask per conjunct with stable slot ids
- [x] 2.2 Add the dedicated Mode-A model-filter text carrier and verify model-filter goldens are byte-identical
- [x] 2.3 Retire `FilterPhase`/`filters_by_phase`/`outer_where_filter_ids`/`combined_filter_indices` and `conjunct_scope`/`classify_regroup_filter`/`_split_partitioned_filter_conjuncts`/`_partitioned_conjunct_scope`; verify no references remain (LSP) and the suite passes
- [x] 2.4 Migrate `filter_reachability.py` + semi-join planning inputs to stratum-0 mask entries; verify with the plan-level dev1840-fixture test that `semi_join_filters` and dispositions are unchanged
- [x] 2.5 Drop `OrderEntry.scope`; update `test_dev1747_order_entry.py` and siblings mechanically (approved); verify the dev1747 suites pass

## 3. Lowering (sql)

- [x] 3.1 Add the sectioned lowering stage in `generator.py` mapping masks to today's placements (base WHERE / re-aggregation point / HAVING / outer WHERE / outer wrapper) and verify every existing golden suite passes unmodified
- [x] 3.2 Implement the materialize-the-boolean fallback for unlowered shapes and verify with a forced-fallback unit test that results match the lowered form
- [x] 3.3 Relocate `_classify_order_scope` into lowering, feeding `resolve_order_term` unchanged; verify order goldens byte-identical
- [x] 3.4 Generalize the `order_only_local_ids` trim to mask slots and verify hidden columns never appear in results (projection tests)

## 4. Newly-legal shapes

- [x] 4.1 Retire the DEV-1824 guard (`stage_planner.py:404-409`); verify cross-model partition_by filter executed-value tests pass on SQLite + DuckDB
- [x] 4.2 Retire the DEV-1825 guard (`regroup_planner.py:352-358`); verify both mixed-predicate executed-value tests (row mask and cell mask arms)

## 5. Tests

- [x] 5.1 New golden suite `test_dev1865_golden_sql.py` + `tests/golden/dev1865_sql_baseline.json` for newly-legal shapes across the dialect matrix; verify baselines committed and green
- [x] 5.2 Value-parity tests: structural (mask slot `ValueKey` equals the measure's) and executed twin-query law incl. NULL-drops-row; verify on SQLite + DuckDB
- [x] 5.3 Stratification boundary tests: stratum-0 reaches producers; attached-ref mask not inherited into producers; measure mask value-preserving (incl. cross-model + windowed); verify executed values
- [x] 5.4 Typing-error tests: mixed-grain OR names both failures; raw-rows measure filter; untypeable order target
- [x] 5.5 Order gap coverage: duplicate opposite-direction targets (MIN+MAX), NULL ordering, hidden-slot trimming; verify executed values

## 6. Docs and closeout

- [x] 6.1 Update `docs/concepts/queries.md` (typing rule, two newly-legal shapes, new error); update stale `docs/architecture/planning.md` / `sql-generation.md` references iff the files still exist; verify docs grep clean for retired names
- [x] 6.2 Run the full non-integration suite, `ruff`, and the architecture enforcement bundle (`lint-imports`, `arch_check.py`, likec4 validate, basedpyright vs baseline); verify all green
