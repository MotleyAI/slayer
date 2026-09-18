## 1. Fixtures and failing tests (spec-tests stage) — DONE 2026-09-18

- [x] 1.1 `tests/_dev1900_fixtures.py`: executed-value oracles added — structural 420 / derived 120 (present), partitioned gold 190 / silver 230, windowed April 420 (+ month series), `*:count` gold 3 / silver 2, raw rows 5, two-branch 280, nested inner gold 190 / silver 230, first/last per tier, producer-only 82, out-of-scope tiers {bronze, gold, silver}, association-arm new 100 / ok 150. Re-exports `pushed_filter_infos` + `month_key`.
- [x] 1.2 `tests/test_dev1909_population_pushdown.py` (SQLite + DuckDB): every DEV-1909 scenario of the two spec deltas. 48 RED (feature-missing) + 20 green regression guards (to-one, same-row, dims-only, producer-only 82 + zero rows, association arm). Empirically confirmed each RED fails via the guard raise or the fan-defect value (partitioned 290, windowed 520, raw 6, nested 290), never a setup error.
- [x] 1.3 Structural boundary tests: `TestBoundaryAssociationArmUntouched` (association producer keeps 100/150, no population entry — observable proxy for "no population semi_join_filters"); nested producer body carries the EXISTS with no `orders` join (`test_nested_producer_inherits_restriction` inspects the dry SQL).
- [x] 1.4 `tests/test_dev1909_golden_sql.py` + `tests/golden/dev1909_sql_baseline.json`: nine cases × seven Tier-1 dialects, blessed pre-implementation (fanning-inline → recorded raise; producer/dims-only → fanning SQL; to-one → plain SQL). Harness green pre-implementation (69 passed).
- [x] 1.5 Re-points (all RED pre-implementation): removed the 4 DEV-1909 `strict` xfails in `test_dev1747_reroot_filter_routing.py`, the 1 in `test_dev1840_execution.py`, and the 1 in `test_dev1748_first_last_matrix.py` (oracle flipped multiplied 215 → association `FAN_FIRST + FAN_LAST` = 144); removed the `reroot/unreachable_filter::` carve-out in `test_dev1747_golden_sql.py` and added its pending ALLOWED_DELTAS; moved `fanning/pop_filter_derived` out of `FAIL_CLOSED` in `test_dev1900_golden_sql.py` and added its pending ALLOWED_DELTAS; re-pointed `test_dev1900_population_guard.py`'s structural/derived raise cases to 420 / 120 and its aggregate-only-in-filter case to `{gold, silver}`, keeping the unanalyzable (now via the new checker), to-one, dims-only and producer-only cases.
- [x] 1.6 `tests/_dev1871_raise_ledger.py`: the two `check_population_filter_no_fanout` rows replaced by `check_population_filter_in_pushdown_scope` and `check_filter_dependencies_analyzable` (message text is the contract implementation must match byte-for-byte). Raise-parity RED until the checkers land.
- [x] 1.7 Test-impact audit (design decision 11) — the full non-integration suite is **76 failed / 18987 passed / 100 skipped / 2 xfailed**; every failure is an intended DEV-1909 RED, confined to the eight touched/new files below; no collateral breakage (dev1840/1841/1853/1910/1836/1847 suites all still green, so every unchanged customers-rooted fanning-filter and dropped-filter assertion stays green).
  - NEW: `test_dev1909_population_pushdown.py` (48 RED), `test_dev1909_golden_sql.py` + `golden/dev1909_sql_baseline.json` (green pre-impl).
  - RE-POINTED (RED): `test_dev1900_population_guard.py` (18), `test_dev1747_reroot_filter_routing.py` (4), `test_dev1840_execution.py` (2), `test_dev1748_first_last_matrix.py` (1), `test_dev1747_golden_sql.py` (1), `test_dev1900_golden_sql.py` (1), `test_dev1871_raise_parity.py` (1).
  - FIXTURES (no assertions): `_dev1900_fixtures.py` (oracles), `_dev1748_fixtures.py` (`FAN_RUSH_ASSOC_SUM`), `_dev1871_raise_ledger.py` (2-for-2 swap).
  - Scenario coverage: every `#### Scenario` of the two MODIFIED requirements has a covering test — the DEV-1909-specific ones in the new module, the pre-existing (unchanged) ones in the dev1840/1841/1853/1910 suites.
  - PENDING for spec-implement: re-bless `dev1747`/`dev1900` baselines (their ALLOWED_DELTAS are staged) once the SQL flips; the axiom-14 enforced-tag edit (needs Egor's per-change OK).

## 2. IR

- [ ] 2.1 `slayer/ir/planned.py`: `EmptyBaseGrainPlan.host_gated` (decision 7). No `SemiJoinFilter.root_relation`, no `PreboundQuery.semi_join_filters` — population groups ride the existing `PlannedQuery.semi_join_filters`. Verify: `arch_check` + `basedpyright` clean (baseline unchanged).

## 3. Checker

- [ ] 3.1 `slayer/engine/elaborate_env.py`: `check_population_filter_in_pushdown_scope(filter_text, reason)` and `check_filter_dependencies_analyzable(filter_text, column)` added; `check_population_filter_no_fanout` deleted; `first_unanalyzable_filter_column` added to `reference_closure.py` if not already present. Verify: 1.6 raise-parity green.

## 4. Population disposition

- [ ] 4.1 `slayer/engine/compile/stages.py`: `PopulationFilters` + `dispose_population_filters` (decision 1), `producer_view` / `host_split` by grain paths (decision 2); `_conjunct_disposition` raises the analyzability error on a `None` closure. Verify: 1.2 structural / derived / two-branch green.
- [ ] 4.2 Host base consumer in `compile_prebound`: `host_split` drops the pushed conjuncts from the masks (combined filters preserved), copies groups to the plan, residue check (decision 5 — fail closed for out-of-scope when a plain aggregate is inline OR raw-row mode), backstop assertion (decision 6); `_assert_population_filters_no_fanout` deleted. Verify: 1.2 same-branch, raw-row, residue, unanalyzable green.
- [ ] 4.3 Producer consumers: thread `PopulationFilters` through `compile_synthesized` as a compile-context parameter (decision 3), so local regroups, `_synthesize_wrap_attach`, the broadcast-local (host-rooted) cross-model synthesis, and nested host-rooted producers consume it and set `semi_join_filters` on their own final `PlannedQuery`; excluded conjuncts land on `dropped_filter_warnings`. The association arm and target-rooted producers are excluded by kernel (decision 4) and keep their dev-1910/1840 routing. Verify: 1.2/1.3 partitioned/windowed/first-last/nested green; association 100/150 and DEV-1840/1841/1853 unchanged.
- [ ] 4.4 Empty-base gating in `_plan_empty_base_grain` + renderer (decision 7). Verify: 1.2 producer-only 82 / zero-rows green.

## 5. Renderer

- [ ] 5.1 `slayer/sql/generator.py`: the empty-base placeholder branch of `_render_with_combined_attaches` builds the host FROM + `LIMIT 1` and applies the semi-join EXISTS conditions when masks OR `host_gated`. Verify: 1.4 goldens re-blessed with EXISTS and no fanning join; `assert_scope_closed` green.

## 6. Reporting

- [ ] 6.1 `slayer/core/warnings.py`: `SemiJoinPushedWarningPayload.measure: Optional[str] = None` + branched human message; `slayer/engine/query_engine.py`: top-level plan `semi_join_filters` emit `(location, None, text)` entries. Verify: 1.2 warning assertions green.

## 7. Docs, architecture, gates

- [ ] 7.1 `docs/concepts/queries.md`: guard sentence replaced (Filters and Auto-Joins); `warnings` table cell notes `measure` is `null` for a population push. Nav unchanged.
- [ ] 7.2 `architecture/semantics.arc42.md` axiom 14 gains `[enforced: test:tests/test_dev1909_population_pushdown.py]` (per-change approval, exact diff shown before applying). `arch_check` green.
- [ ] 7.3 Full non-integration suite green, ruff clean, conventions gate green, basedpyright baseline unchanged, `arch_check` + LikeC4 green. Any shifting test outside 1.5/1.7 stops for a ruling.
- [ ] 7.4 Codex pass on the working tree (standing rule); resolve findings; re-review clean.
