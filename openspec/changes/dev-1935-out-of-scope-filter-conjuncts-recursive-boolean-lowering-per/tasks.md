## 1. Tests first (spec-tests stage)

- [x] 1.1 `tests/test_dev1935_boolean_lowering.py` + `tests/_dev1935_fixtures.py`: executed-value oracles on
      SQLite + DuckDB (460/475/370/55/320/420, six raw rows, partitioned gold 190 / silver 230 / bronze 40,
      producer-side DEV-1840 gold 160 / silver 230 / bronze 40, association ok 270 / new 250 and ok 270 /
      new 100, materialised cells {1,3,5,7,9,10}, two-spellings gold 60 / silver 80, event-less null-extension
      LEFT 190 / INNER 100 / rejecting 100), parametrised over the three modes where the spec says "any mode";
      assert the `semi_join_pushed` entries, no dropped-filter warning, no Python warning. RED confirmed on
      main (fail-closed raise / ScopeLeakError / defect value).
- [x] 1.2 Same file: structural pins — determinism (generate twice, byte-identical); two disjoint branches →
      two EXISTS; `null_extended` False on every hop of the existing OK / bad_pop fixture pushes; the EXISTS
      never appears as an operand of OR (`OR EXISTS` absent); spine only for a non-rejecting predicate
      (saving leg → hop null_extended True); declared-INNER descendant stays False (event-less INNER variant);
      the population's dropped arm impossible (OR-mix population pushes, never drops). Deferred within 1.2:
      the forward-vs-reverse *executed* declared-INNER symmetry beyond the event-less mini-graph — the mini-
      graph covers the reverse-descendant case; a forward-INNER-descendant executed pin is optional and can be
      added at implement if the null_extended structural pin proves insufficient.
- [x] 1.3 Same file (`TestNullRejectionAnalysis`): null-rejection rules pinned via the observable
      `SemiJoinHop.null_extended` flag on the fanning `region_events` hop — comparison/IN/BETWEEN → UNKNOWN
      (False), `is null` → TRUE (True), `is not null` → FALSE (False), AND-of-UNKNOWN → False, NOT UNKNOWN →
      False, scalar call → DEPENDS (True), OR with a root-local leg → DEPENDS (True). Chosen over importing an
      unnamed internal analyser fn, which would over-constrain the implementation; TimeTrunc/StarKey/Mode-A
      fragment rows from the design's rule list are NOT yet covered here — add at implement once the analyser
      exists and its fragment/TimeTrunc handling is concrete (they are hard to isolate through the public plan
      without dedicated fixtures). RED on main (AttributeError: no `null_extended`, or the mixed-OR raise).
- [x] 1.4 `tests/test_dev1935_golden_sql.py` + `tests/golden/dev1935_sql_baseline.json` (8 cases × 7 Tier-1
      dialects): mixed OR (structural / gold-null-extended / negation), `is null`, multi-branch OR, cross-branch
      atom, partially materialised host base, partitioned producer. Baseline generated recording CURRENT
      behaviour (the 5 mixed/multi-branch cases record raises; is_null / materialised / producer record
      current SQL). `test_new_shapes_generate_sql` is the RED tripwire (flip cases must become SQL, not a
      raise). At IMPLEMENT: the SQL for all 8 cases moves → add each to ALLOWED_DELTAS with a reason,
      `SLAYER_UPDATE_GOLDEN=1`, empty the manifest.
- [~] 1.5 Consented flips — DONE (RED-verified, left UNSTAGED for spec-implement's commit gate):
      `test_dev1840_disposition.py::TestBooleanTotalPushdown` (4, dropped→pushed, plan-level),
      `test_dev1909_population_pushdown.py::TestOutOfScopeNowPushed` (5, fail-closed→pushed/460/6-rows),
      `test_dev1841_error_mode.py` (1, error→pushed gold 160), `test_dev1841_association_filters.py` (1,
      dropped→pushed ok 270 / new 250), `test_dev1836_broadcast_strict.py` (1, error→pushed gold 160),
      `test_dev1840_strict_metadata.py` (3, error/warn→pushed gold 160 / no-warning), the
      `check_population_filter_in_pushdown_scope` ledger row removed from `tests/_dev1871_raise_ledger.py`.
      DEFERRED to spec-implement (verify against actual output — dataset-specific values / internal routing &
      dedup structure; flipping blind risks false-greens; consent already recorded in design decision 11):
      * `test_dev1747_reroot_filter_routing.py` (8): `FILTER_MIXED_OR` flips dropped-and-warned → pushed on the
        customers producer. Flip `test_mixed_or_filter_is_dropped_and_warned`, `test_mixed_filters_route_
        independently`, `TestClassifiedExactlyOnce::test_the_classifier_receives_the_structural_summary`
        (keep "filter reached the classifier", flip disposition to `semi_join_filters`),
        `TestExcludedWarns::{test_excluded_filter_produces_a_warning_on_the_plan, test_warning_carries_the_
        original_filter_text, test_warning_carries_a_reason, test_exactly_one_warning_per_filter_per_execute,
        test_warnings_as_errors_mode_surfaces_the_drop}`, and `TestFilteredLocalDispatchAccounting::{test_an_
        excluded_filter_still_narrows_the_host, test_the_excluded_filter_warns_exactly_once}` → no dropped
        warning, semi-join present, host regions still `[REGION_A_LOW, REGION_A_HIGH]`, producer `cs` under the
        semi-join population. `TestUnreachableDimensionsStillDrop` STAYS (genuinely unreachable dimension).
        Some warning-metadata tests may warrant removal rather than flip — confirm with the run.
      * The three dropped-filter dedup files → re-point to `semi_join_pushed` entry dedup (one entry per
        (location, measure, text), never one per consumer): `test_dev1745_warning_contract.py`,
        `test_dev1836_warning_collector.py`, `test_dev1838_interning.py`.
      * `test_dev1840_golden_sql.py`: `excluded/mixed_or` in `dev1840_sql_baseline.json` re-blesses to the
        spine-shaped EXISTS via ALLOWED_DELTAS with a recorded reason (SQL only moves once the lowering lands).
      OPEN QUESTION (Codex review finding 2) — spec scenario "Genuinely unreachable filter keeps the
      established behavior" (cross-model-aggregates) says a genuinely unreachable filter is *dropped-and-warned
      in lenient modes*. Empirically, in the connected dev1900/1840 graph a reference with no join path (e.g.
      `promos.discount`) is HARD-refused in EVERY mode (`UnresolvableDimensionJoinError` at dimension routing),
      never a lenient dropped-filter warning; and there is no host-reachable/producer-unreachable topology
      (customers reaches everything orders reaches via the reverse hop), so the producer `UNREACHABLE_NO_PATH`
      -> dropped-warning arm (stages.py:1125/1287) appears UNREACHABLE for connected graphs once mixed-OR /
      multi-branch (its former triggers) push. `TestGenuinelyUnreachableFilterRefused` pins the actual behaviour
      (refused every mode). spec-implement must reconcile: either (a) the dropped-filter-warning path is dead for
      connected graphs post-DEV-1935 and the spec scenario should say "refused" (update the delta), or (b) a
      bespoke disconnected-producer fixture is needed to exercise the warning — decide against the implementation.

## Codex review of the tests (spec-tests Step 2) — folded in
- Finding 1 (HIGH): materialised oracle corrected to `{None,1,3,5,7,9,10}` (c7's LEFT-extended South row survives
  via the region leg); main already emits this inline, so it is a same-row-binding + null-extension anchor, not a
  RED. **The `queries/semantics` delta scenario "A materialised branch binds to the grouped row" states the cells
  as `1,3,5,7,9,10` and omits the null-order cell — normative-doc correction pending (see below).**
- Finding 3: dev1909 `test_producer_pushes_not_drops` strengthened to assert the restricted values + no dropped
  warning in every mode (was tier-set only → false-green for broadcast/associate).
- Findings 4 & 5: the decision-7 (`EXISTS` not under `OR`) and two-spellings (one `orders` relation) pins now use
  the sqlglot AST, not text matching.
- Finding 2: handled as the OPEN QUESTION above.

NORMATIVE-DOC FIX DONE: the `specs/queries/semantics/spec.md` scenario "A materialised branch binds to the
grouped row inside a multi-branch conjunct" now reads cells `{null, 1, 3, 5, 7, 9, 10}` — the orderless South
customer's null-order cell (its LEFT-extended row satisfies the region leg) is included; the test pins the same set.
      Intended-red set (this stage): all of `tests/test_dev1935_boolean_lowering.py` except
      `TestEventLessNullExtension::test_rejecting_predicate_excludes_event_less` and
      `TestGroupingAndDeterminism::test_two_disjoint_branches_yield_two_exists` (byte-identity/invariant anchors
      that pass now AND after); `test_dev1935_golden_sql.py::test_new_shapes_generate_sql`; and the eleven flips
      listed as DONE above; plus the ledger-parity test that now sees the un-ledgered checker raise.

## 2. Planner (spec-implement stage)

- [ ] 2.1 `slayer/ir/planned.py`: `SemiJoinHop.null_extended: bool = False`; verify existing goldens
      unchanged
- [ ] 2.2 `stages.py`: canonical hop tokens in `_forward_hops` / `_reverse_hops` / `_remap_ref_path`
      (edge name, else target model); verify the two-spellings test and the golden corpus
- [ ] 2.3 `stages.py`: delete `_reject_mixed_or_not` and the single-first-hop block in
      `_conjunct_push_plan`; verify the DEV-1840 disposition tests flip to pushed
- [ ] 2.4 `stages.py`: union-find grouping in `_semi_join_groups_from_pushes` with first-appearance
      order; verify the grouping and determinism pins
- [ ] 2.5 `stages.py`: the null-rejection analysis (design decision 5) setting `null_extended` per hop
      after grouping; verify 1.3 and `null_extended` false on every existing fixture push
- [ ] 2.6 `stages.py`: `PopulationFilters` two-way with the asserted dropped arm; delete the residue
      check, `drop_excluded`, `dropped_warnings`; per-branch materialisation in `host_split` (reduced push
      for a partially materialised conjunct); verify the DEV-1909 residue tests and the materialised-branch
      scenario
- [ ] 2.7 `slayer/engine/elaborate_env.py`: delete `check_population_filter_in_pushdown_scope`; verify
      the ledger and `tests/test_dev1871_raise_parity.py` pass
- [ ] 2.8 `stages.py` association arm: confirm no change needed beyond 2.3; verify the association
      scenarios (ok 270 / new 250, ok 270 / new 100)

## 3. Renderer

- [ ] 3.1 `slayer/sql/generator.py::_build_semi_join_exists`: CROSS-joined further first-level hops with
      WHERE correlations; per-hop INNER/LEFT for deeper hops; verify byte-identity of every existing golden
- [ ] 3.2 Same builder: the spine shape (`FROM (SELECT 1 AS one) AS <alias>`, correlations in ON,
      allocator-reserved alias, scope-closure validator updated); verify the dev1935 goldens on every
      Tier-1 dialect and `assert_scope_closed`
- [ ] 3.3 Refs on a materialised branch render against the outer alias; verify the materialised-branch
      scenario SQL joins `orders` once, outside the EXISTS

## 4. Docs, harness, gates

- [ ] 4.1 `docs/concepts/queries.md` (one sentence: product rule, `is null` idiom; drop the OR/NOT
      exclusion) and `docs/database-support.md` (one sentence: MySQL 8.0.20+, BigQuery); verify with
      `grep -n "OR\`/\`NOT" docs/` finding nothing stale
- [ ] 4.2 `architecture/semantics.arc42.md` tags (Axiom 3, Axiom 14, Law 5, Law 6) — present the exact
      diff and apply only on explicit approval; verify `poetry run python tools/arch_check.py`
- [ ] 4.3 Full gates: `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`,
      `poetry run basedpyright` (no new errors), `poetry run python tools/arch_check.py`,
      `npx -y likec4@1.47.0 validate architecture`; then the CI integration invocation
