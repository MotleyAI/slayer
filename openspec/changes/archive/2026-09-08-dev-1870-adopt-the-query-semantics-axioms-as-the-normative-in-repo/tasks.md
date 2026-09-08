# Tasks

## 1. arch_check tag vocabulary (TDD)

- [x] 1.1 Add failing tests in `tests/test_arch_check.py`: malformed `[target:]`/
  `[review]`/`[enforced:]` variants flagged; `[target: X]` ids must match `DEV-\d+`;
  a numbered principle item with no status tag is flagged; mixed tags on one item
  are legal; orphan `architecture/*.arc42.md` (not system, node `arc42:`, or
  `cross_cutting_arc42:`) flagged both directions — verify tests fail
  *(done incl. Codex-review round: 9 fail on missing findings, legality/acceptance
  pins pass; new-finding prefixes: tag vocabulary + principle coverage under
  `enforced-tags:`, orphans + `cross_cutting_arc42` back-direction under
  `arc42-exists:`; no new CHECK_IDS; malformed tags flagged in prose too and never
  count as coverage; adjacent-item boundary pinned)*
- [x] 1.2 Implement the `_check_enforced_tags`/`_check_arc42` extensions and
  `cross_cutting_arc42:` index support in `tools/arch_check.py` — verify 1.1 tests
  pass and `poetry run python tools/arch_check.py` stays green pre-dissolution

## 2. Spec scenario coverage (verify-then-fill)

- [x] 2.1 Map every `queries/semantics` + `models/join-cardinality` delta scenario to
  an existing executed-value test and record the mapping here — verified 2026-09-08
  by running the exact pins (38 passed) and every named file (544 passed, 2 skipped):
  - Attributable partitions, unattributable broadcasts →
    `test_dev1836_producer_execution.py::TestBroadcast::test_mixed_grain_broadcasts_only_the_unsafe_member`
  - Join fan-out never multiplies →
    `test_dev1836_producer_execution.py::TestTargetRootedExactness::test_joined_sum_is_not_multiplied_by_fan_out`
  - One row per dimension combination: aggregating clause →
    `test_dev1836_producer_execution.py::TestTargetRootedExactness::test_joined_sum_is_not_multiplied_by_fan_out`
    (exact combination set; `rows_by` asserts no duplicate group rows); measure-less
    default + raw-row carve-out →
    `test_distinct_dimension_values.py::TestEndToEndExecution::{test_flag_true_dedupes,test_flag_false_returns_raw_rows}`
  - Different-grain arithmetic broadcasts to the union →
    `test_dev1739_execution.py::TestCoarserRepeatAndShare::{test_region_total_repeats_across_cities,test_per_cell_share_is_city_over_region}`
    (per-cell coarser-operand repetition; per-cell ratio at the union grain — the
    latter added by this change)
  - Coarser consumption is a typed error →
    `test_dev1824_partitioned_execution.py::TestRowCombinedCoexistence::test_order_by_raw_finer_grain_raises_clean_error`
    (asserts a clean `ValueError`, not a `SlayerError` subclass)
  - Cross-path filter restricts by association →
    `test_dev1840_execution.py::TestReverseHopPushdown::test_pushdown_restricts_population_not_cardinality`
  - A restriction is never silently ignored →
    `test_dev1840_strict_metadata.py::TestExcludedFiltersKeepTheWarning::test_mixed_disjunction_warns`
    + `TestStrictNarrows::test_strict_still_errors_on_a_mixed_disjunction`
  - Adding a measure changes nothing else →
    `test_dev1837_dimension_measure_matrix.py::TestCardinalityNeutrality::test_adding_measure_is_cardinality_neutral`
    (3×3 dim-family × measure-family sweep) +
    `test_dev1836_producer_execution.py::TestTargetRootedExactness::test_adding_cm_measure_is_cardinality_neutral`
    (cross-model) +
    `test_dev1739_execution.py::TestCardinalityAndSiblingInvariance::test_adding_partition_measure_changes_nothing_else`
    (partitioned)
  - Measure-typed filter masks without altering values →
    `test_dev1839_measure_execution.py::TestConformingSurfaces::test_filter_over_mixed_arithmetic`
  - A query equals its single-measure splits →
    `test_dev1824_partitioned_execution.py::TestCoexistenceAcrossFamilies` (solo-equality helper)
  - Implicit broadcast warns, explicit grain does not →
    `test_dev1836_broadcast_strict.py::TestBroadcastMetadata::{test_broadcast_reported_per_metric_and_dimension,test_explicit_partition_broadcast_never_warns}`
  - A chain of proven hops determines →
    `test_dev1836_producer_execution.py::TestTargetRootedExactness::test_safe_two_hop_dim_keeps_exact_values`
  - One unproven hop breaks determination →
    `test_dev1836_producer_execution.py::TestBroadcast::test_unproven_hop_dim_broadcasts`
- [x] 2.2 Write a new executed-value test for any scenario 2.1 finds unpinned —
  one gap found (per-cell ratio at the union grain was only pinned in aggregate):
  added `test_dev1739_execution.py::TestCoarserRepeatAndShare::test_per_cell_share_is_city_over_region`

## 3. The algebra and arc42 files

- [x] 3.1 Write `architecture/semantics.arc42.md`: all 14 axioms + 6 laws, one line
  each, per-clause tags per design decisions 3–4; true items link their spec
  requirement — verify `arch_check` green (tag vocabulary + orphan checks)
- [x] 3.2 Write `architecture/engine.arc42.md` (~8 principles) and
  `architecture/core.arc42.md` (~5 principles), all `[review]`, per design decision 6;
  add `arc42:` entries for `engine`/`core` and the `cross_cutting_arc42:` list to
  `architecture/index.yaml` — verify `arch_check` green
- [x] 3.3 Extend `architecture/sql.arc42.md` with P10–P12 (P12 tagged
  `[enforced: test:tests/test_dev1837_dimension_measure_matrix.py]`), update its §2
  refs; rewrite `system.arc42.md` principle 8 as pointer+invariant, add the three-tag
  vocabulary to §4, update §2/§6 stale refs — verify `arch_check` green

## 4. Dissolution and deletions

- [x] 4.1 Delete all 14 `docs/architecture/*.md` files and the root `specs/` folder
  (4 files) per the design ledger — verify `git status` shows only expected deletions
  and `grep -rn "docs/architecture" --include="*.md"` outside archives returns only
  historical/openspec-archive hits
- [x] 4.2 Remove the Architecture nav section from `zensical.toml`; update root
  `CLAUDE.md` documentation-requirements (behaviour → openspec/specs, principles →
  architecture/); fix the `tests/test_projection_trim.py:979` comment ref — verify
  grep for `architecture/` in `zensical.toml` is empty and the full non-integration
  suite passes

## 5. Gates and follow-through

- [x] 5.1 Run the enforcement bundle — `poetry run lint-imports`,
  `poetry run python tools/arch_check.py`, `npx -y likec4@1.47.0 validate
  architecture`, `poetry run basedpyright` — verify all green (no new errors vs
  baseline)
- [x] 5.2 Run `openspec validate dev-1870-adopt-the-query-semantics-axioms-as-the-normative-in-repo
  --strict`, `poetry run pytest -m "not integration"`, and
  `poetry run ruff check slayer/ tests/` — verify all green
- [x] 5.3 Post the DEV-1841 pointer comment (dice–slice + mode-axis `[target:]`
  entries in `semantics.arc42.md`, flip obligation) — verify comment visible on the
  issue *(posted 2026-09-08)*; include the dissolution ledger in the PR description
  *(done — PR #371)*
