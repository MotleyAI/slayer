## Context

See proposal.md › Why. Mechanism, verified by probe on `main` at `f1f032cc3` (SQLite and DuckDB, `tests/_dev1739_fixtures.py`):

1. `_desugar_regroups` (`slayer/engine/compile/stages.py`) first replaces every re-aggregation root by its `__regroup__` placeholder in every position (`_substitute_reaggregations`).
2. `substitute_in_bound_filter` (`compile/regroup.py`) recomputes the filter's phase from the substituted key. The placeholder is a ROW-phase `FieldKey`, so the measure-typed filter `max(…) > 45` becomes the ROW-phase `__regroup__0__max > 45`.
3. The cross-dataset producer builds its base filters from every non-row-restriction filter and keeps those with `bf.phase == Phase.ROW` (`_home_filters`, the `product_anchors` argument, `_producer_row_conjuncts`). It therefore inherits the filter as a row conjunct and re-roots it onto `orders`, whose scope has no such column.

Structural cause: two rules decide which filters a producer inherits. Local regroup and re-aggregation producers (`_regroup_inherited_filters`) and the host population disposal (`dispose_population_filters`) use the checker's mask typing (`ConjunctTyping`: FIELD, stratum 0); the cross-dataset and association producers use the recomputed phase. They agree only until a substitution lowers a phase. A plain partitioned aggregate in the same filter works because its phase stays AGGREGATE at that point.

Principles that apply: semantics Axiom 14 (a measure-typed filter masks cells after all values are computed; stratum-0 field filters fix the rows every aggregation runs over), Law 3 (sibling-measure independence), Axiom 9 (closure); engine P6 (placement derives from stage and mask typing, never from key shape) and P9 (the checker owns typing; the compiler consults it); spec `queries/positions` › Filter conjuncts stratify. Core P4 (no reserved name bending a kind) is the deeper placeholder-encoding issue, owned by DEV-2017 and untouched here.

## Goals / Non-Goals

**Goals:**
- One selection of the population's field masks, read by every population-inheriting producer, so no filter a substitution rewrote can reach a producer it does not define.
- Every shape in the tracker issue returns the control's rows with the sibling's own values on SQLite and DuckDB.

**Non-Goals:**
- DEV-2375 (a computed dimension's re-aggregation placeholder re-rooted into a cross-dataset producer's grain, missed by `_attaches_carrying`): a different mechanism, not closed here; the finding is recorded on DEV-2375.
- DEV-2017 (placeholders as their own key kind).
- Shifted producers' `_frame_free_filters` (field masks of any stratum minus frame bounds): unchanged.
- No spec, arc42, model or docs change.

## Decisions

1. **One producer-inheritance rule keyed on the checker's typing.** (default) Options weighed: (a) one selection of field-typed, stratum-0 filters per the checker, read by every producer; (b) fold in DEV-2017 so the placeholder no longer lowers phase. Took (a): it makes the bug class (a non-population filter reaching a producer) impossible whatever the key encoding, and is grounded in engine P6/P9 and the stratification spec; (b) alone, as a pure representation change, would still leave a phase-keyed path.
2. **The selection reads the checker's ORIGINAL typing by filter index.** (codex) Eligibility is never re-derived from a `BoundFilter` that substitution rewrote; substitution may only rewrite the key of an already-selected filter.
3. **A typed carrier replaces the two overlapping lists.** (codex) The synthesis context's `base_filters_with_text` and `population_filters` / `n_population_date` become one list of Pydantic entries carrying at least the original filter index, the (possibly rewritten) `BoundFilter`, its text and its date-range flag, with date-range entries kept as an ordered prefix so producers still receive `n_date_range`. The phase checks on the cross-dataset path that this makes redundant retire.
4. **Shifted producers keep their rule.** (default) `_frame_free_filters` inherits field masks of any stratum; nothing shows it broken (the `time_shift` siblings pass), so it is out of scope.
5. **`skip_specs: true`.** (default) `queries/positions` › Filter conjuncts stratify and Measure mask prunes cells only already specify the behaviour.
6. **DEV-2375 out of scope; finding posted on it.** (user) Probe `dim2375` still fails with the fix; the re-rooting mechanism was recorded as a comment on DEV-2375.
7. **Descriptive test names, no issue numbers.** (default) Per the no-issue-numbers-in-code rule.
8. **Test oracle: drop only the predicate under test.** (codex) Each case's control is the same query with `count(orders.*)` as the only metric (row oracle), and the sibling's expected values come from the same query without the measure-typed predicate under test but keeping every other filter; hand values back both.
9. **Tests prove the typing they exercise.** (codex) The stratum>0 field-typed case asserts the checker types its conjunct FIELD with stratum > 0; the stratum-0 row-filter case asserts FIELD with stratum 0.
10. **"No population" means no explicit `population` field.** (codex) Same dimensions and the same relationship-reading sibling; executed values asserted.

## Risks / Trade-offs

- [A stratum-0 FIELD conjunct with a non-ROW phase would now be inherited where the phase check used to drop it] → stratum 0 is aggregate-free by construction; if any test shows otherwise, stop and raise it rather than re-adding a phase check.
- [Removing the redundant phase checks changes `dispose_population_filters`'s input selection] → its filter set is already FIELD/stratum-0; the targeted suites (population pushdown, stratification, row supply, dual-phase, time spine) plus CI's full suite pin the behaviour.
