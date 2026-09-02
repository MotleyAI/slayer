# Tasks — DEV-1838 stage 4

TDD protocol: group 0 then 1 land as failing tests + ledger BEFORE implementation
(flow steps 4–5); groups 2–7 are the apply phase, one green revertible stage each.
Divergence classes per design D10; class (c) is empty by contract.

## 0. Baseline, ledger, dispositions

- [x] 0.1 Re-baseline against then-current main + the DEV-1836 branch state; re-run `openspec validate dev-1838-stage-4-node-discipline-fold-the-root-select-residue-and --strict`; re-check the three MODIFIED/REMOVED delta blocks against the post-1836 corpus (verify by validate green + block-for-block diff)
- [x] 0.2 Write the suite disposition table into `divergences.md` and execute it throughout: RETIRE AFTER PORTING — test_dev1750_guard_ownership, test_filtered_local_isolation, test_cross_model_planner_wiring, test_cross_model_planner, test_dev1746_isolation_classifier (may_inline seam pin ports to join_safety), test_dev1748_ranked_plan (value pins port), test_dev1450fix_reroot_strategy, test_dev1747_reroot_filter_routing + test_dev1745_reachability (routing semantics port onto producer filter inheritance where not already in test_dev1836_filter_inheritance), test_dev1747_prebound_planner; KEEP WITH IMPORT SWAPS — test_dev1744_naming_allocator, test_planned (drop CMA model tests only); KEEP — test_dev1748_first_last_matrix, test_dev1748_golden_sql, test_dev1732_frame_bound_filters, test_dev1836_ranked_windowed, warning collector/contract suites (reconciled, never weakened); AUDIT — test_dev1783_pr286_g1 (verify: table present, every ported pin proven failing-without/passing-with before its source suite is deleted)
- [x] 0.3 Enumerate the class-(d) ledger per input role (crossed predicate / argument / host-grain source) with today's executed values on `tests/_dev1838_fixtures.py`, and the class-(b) batch list per stage, in `divergences.md` (verify: ledger reviewed and approved by the author before group 2 starts)
- [x] 0.4 Author `tests/_dev1838_fixtures.py` + failing test suites for every delta scenario in specs/ (one covering test minimum per scenario) + the D2 fusion snapshot set + D3 interning negative pins + D6 warning pins + D8 invariant pin (verify: new tests fail for feature-missing reasons only; full existing suite still green)

## 1. Stage 1 — producer interning

- [x] 1.1 Compute the D3 structural identity on `RegroupAttachPlan` at construction; thread a per-query registry through `plan_query` recursion so a producer needed by several scopes is one shared plan object (verify: registry unit tests incl. negative pins pass)
- [x] 1.2 Thread a generation-wide rendered-producer map through `_render_producer_split` (like the allocator) so one identity renders one CTE; consumers keep per-scope join coordinates (verify: band×wm golden collapses to a single `_cm_amount_sum_partition_by_city` across dialects; two-depth and differing-coordinates tests pass)
- [x] 1.3 Verify stage: full non-integration suite + SQLite/DuckDB integration green; executed values unchanged; warning pins (D6) green; class-(b) batch re-blessed (dev1835 `lift/band_x_wm` + dev1839 `lift/dual_role`, all dialects; executed-value companions green on SQLite+DuckDB)

## 2. Stage 2 — family unification

- [x] 2.0 Inventory every field of WindowedAggregatePlan / RankedAggregatePlan / CrossModelAggregatePlan and record its destination (kernel field / producer body / deletion-with-pin) in `divergences.md` (verify: table complete before 2.1)
- [x] 2.1 Add the typed producer kernel (plain | ranked | trailing-window) to `RegroupAttachPlan` per design D4 (verify: kernel unit tests pass)
- [x] 2.2 Migrate plain windowed measures onto trailing-window-kernel producers; delete the `_wm_` renderer arm + `WindowedAggregatePlan` + the `:5378` guard (verify: windowed executed-value suites green incl. frame-bound rewrites, boundary inclusivity, month durations, hidden order-only targets, multiple time dimensions on SQLite+DuckDB; class-(b) batch re-blessed)
- [x] 2.3 Migrate host-route first/last onto ranked-kernel producers; delete `build_host_ranked_plan`/`build_target_ranked_plan`, `RankedAggregatePlan`, the ranked renderer arms (verify: ranked suites green with set-wise tie comparison incl. DuckDB null/tie cases; class-(b) batch re-blessed) — NOTE: `build_host_ranked_plan` + `RankedAggregatePlan` + the full `_rk_` arm re-sequenced into 2.4, where the `disable_host_rooted_isolation` sub-plan context that still exercises them dissolves (see divergences.md stage-2.3 entry)
- [x] 2.4 Migrate host-rooted cross-model, `grain="host"` wraps, and filtered-local onto host-rooted producers under per-role safety (design D5) (verify: ported behavior pins green; class-(d) errors match the approved ledger; provably-safe-hop shapes value-identical) — includes the re-sequenced 2.3 ranked deletions; see divergences.md stage-2.3/2.4 entries for the reconcile record and the D5 source carve-out
- [x] 2.5 Land the D8 invariant pin, then delete `classify_isolation`, `IsolationKind`, `isolation.py`, `cross_model_planner.py`, `CrossModelAggregatePlan`, `_render_cross_model_cte`, `_render_rerooted_cross_model_cte`, `_guard_target_grain_time_shift`; relocate `may_inline_crossing_inputs` to `join_safety.py`; update response_meta/warnings/query_engine plan walks (verify: grep-zero for deleted symbols in slayer/; full suite green — executed outcomes in the ledger's stage-2.5 entry)
- [x] 2.6 Verify stage: full non-integration + SQLite/DuckDB integration green; disposition table fully executed for retired suites (14706 passed, only the 10 stage-4 TDD failures remain; integration 561 passed / 51 skipped excl. live-Metabase; ruff + openspec --strict clean)

## 3. Stage 3 — node fold

- [x] 3.1 Introduce the renderer Node model and rebuild the pipeline base → aggregate → combined → steps → post with D2 fusion; fold the plain and cross-model paths into it (verify: fusion snapshot tests green; fast-path goldens byte-identical)
- [x] 3.2 Merge the two Kahn chains into one driver with the canonical batch order (design D7); delete `_render_cross_model_transform_chain`; keep the deadlock backstop (verify: transform suites green; loser-order goldens re-blessed class (b))
- [x] 3.3 Delete the collapsed-ranked/collapsed-windowed pre-dispatch arms — collapse now falls out of fusion (verify: former collapsed shapes' goldens byte-identical or in the approved class-(b) batch; `assert_scope_closed` + single-flat-WITH pins green)
- [x] 3.4 Verify stage: full non-integration + SQLite/DuckDB integration green

## 4. Stage 4 — CTE-body lifts

- [x] 4.1 Remove the three CTE-body guards (`:1685/:1721/:1734`); nested attaches hoist per the existing D2 mechanism (verify: the specs' nested-attach scenarios execute on SQLite + DuckDB; cardinality-neutral pins green; no-placeholder-leak green)
- [x] 4.2 Rework `test_dev1837_guards.py::TestCteBodyArms` from raise-pins to the positive execution contract (verify: suite green; no-residue grep scenario passes)

## 5. Stage 5 — sweep

- [x] 5.1 Meta-test: no coexistence `NotImplementedError` remains in `slayer/sql/` (expressiveness fail-closed errors excluded by explicit list) (verify: meta-test green)
- [x] 5.2 Docs: `docs/architecture/composable-attach.md` stage-4 section + roadmap tick; skills touched only if error surfaces changed (verify: docs updated; zensical nav untouched — no new pages)
- [x] 5.3 Perf A/B re-run (`--retime`) + append a DEV-1838 section to `tests/perf/compare/RESULTS.md` (verify: no perf flags, or flagged shapes triaged with the author)
- [x] 5.4 Finalize `divergences.md`; close DEV-1799 as subsumed (comment linking design D7); ruff clean (verify: `poetry run ruff check slayer/ tests/` clean)
