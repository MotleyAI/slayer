# DEV-1838 — divergence ledger (design D10)

Classes per DEV-1836 D10, carried forward: (a) byte-identical, (b) SQL-shape
only, (c) deliberate value changes — **EMPTY by contract** (any executed-value
change discovered is a bug or an unapproved flip: stop and ask), (d) new
errors, enumerated per input role. Today's values measured on
`tests/_dev1838_fixtures.py`, SQLite (probe scripts, task 0.3).

## Task 0.2 — suite disposition table

| Suite | Disposition |
|---|---|
| `test_dev1750_guard_ownership` | RETIRE AFTER PORTING — pins `CrossModelAggregatePlan.cte_root_model` ownership of `time_shift` inners; the host-rooted/regroup routing behavior ports to kernel-producer assertions in the 1838 suites |
| `test_filtered_local_isolation` | RETIRE AFTER PORTING — pure plan-structure pins on the filtered-local `CrossModelAggregatePlan`; behavior (values + SQL shape) ports to `test_dev1838_role_safety` (first executed-value coverage this path has ever had) |
| `test_cross_model_planner_wiring` | RETIRE AFTER PORTING — `filter_referenced_slot_ids` unit pins move beside their function's new home; the plan-shape pins (local ⇒ no plan, cross-model ⇒ target-rooted producer, distinct keys ⇒ distinct producers, host-local filters dropped+warned, target model filters propagate) port onto producer assertions |
| `test_cross_model_planner` | RETIRE AFTER PORTING — same porting rule as wiring |
| `test_dev1746_isolation_classifier` | RETIRE — classifier-mechanism pins die with `classify_isolation`; the `may_inline_crossing_inputs` seam pin ports to `join_safety` |
| `test_dev1748_ranked_plan` | RETIRE AFTER PORTING — `RankedAggregatePlan` mechanism pins die with the class; executed-value pins port to the ranked-kernel suites |
| `test_dev1450fix_reroot_strategy` | RETIRE AFTER PORTING — re-rooted CMA strategy pins; routing semantics port onto producer filter inheritance |
| `test_dev1747_reroot_filter_routing` + `test_dev1745_reachability` | RETIRE AFTER PORTING — routing semantics port onto producer filter inheritance where not already in `test_dev1836_filter_inheritance` |
| `test_dev1747_prebound_planner` | RETIRE AFTER PORTING — prebound-vs-text parity re-pins against the unified plan shape |
| `test_dev1744_naming_allocator` | KEEP WITH IMPORT SWAPS |
| `test_planned` | KEEP WITH IMPORT SWAPS — drop the CMA model tests only |
| `test_dev1748_first_last_matrix` | KEEP — executed ground truth for ranked kernels (set-wise ties) |
| `test_dev1748_golden_sql` | KEEP — class-(b) re-bless in the stage-2.3 batch |
| `test_dev1732_frame_bound_filters` | KEEP — frame-bound rewrites are kernel fields after 2.2 |
| `test_dev1836_ranked_windowed` | KEEP |
| warning collector / contract suites | KEEP — reconciled, never weakened (D6) |
| `test_dev1783_pr286_g1` | AUDIT — every ported pin proven failing-without/passing-with before its source suite is deleted |

## Class (d) — new errors per input role (today's silently-computed values)

Role: **crossed predicate** — a `Column.filter` crossing an unproven hop on a
host-rooted producer (plain / ranked / windowed kernels alike):

| Shape | Today (SQLite) | Pinned (new) |
|---|---|---|
| `alpha_amount:sum` by `status` (filter over the unproven `customers → segments` hop) | ok=40, new=40 — every c1/c3 order double-counted through the duplicate `'a'` segment row (true 20/20) | hard error naming the hop + remedy |
| `rush_amount:sum` by `status` (filter over the unproven 1:N `orders → tags` hop) | ok=70 — order 3 counted twice through its two rush tags (true 40) | hard error |
| `alpha_amount:last` by `status` | ok=5, new=20 — ranks over the fanned join | hard error |
| `rush_amount:last` by `status` | ok=30, new=NULL | hard error |
| `rush_amount:sum(window='90d')` | (ok, Feb)=70 — 10 + 30×2 (true 40) | hard error |

Role: **crossed argument** — an aggregation param / ranking arg crossing an
unproven hop:

| Shape | Today (SQLite) | Pinned (new) |
|---|---|---|
| `amount:tscaled_sum` by `status` (param `tags.factor` over the unproven 1:N) | ok=135, new=8 — SUM over the fanned join | hard error naming the hop + remedy |
| `amount:last(customers.segments.updated_at)` (ranking arg over the unproven hop) | ok ∈ {5, 10} — the max segment date ties across the fanned rows (D9 set-wise; observed 10 on SQLite); new=20 | hard error |

Role: **host-grain source** — an aggregate explicitly evaluated at host grain
over a joined source. **Stays LEGAL** (D5 carve-out); value-preservation pins,
not errors:

| Shape | Today = pinned |
|---|---|
| ORDER BY `tags.factor` asc/desc (wrap over the to-many hop) | asc → [new, ok] (MIN 0.2 < 0.5); desc → [ok, new] (MAX 3.0 > 0.2) |
| ORDER BY `customers.tier` asc (proven), `customers.segments.label` desc (unproven source path) | [new, ok]; [ok, new] (MAX Beta > Alpha) |

Stays-legal pins over PROVEN hops (unchanged values, same suites):
`gold_amount:sum` 10/20 · `gold_amount:last` 10/20 · `amount:wscaled_sum`
110/40 · `amount:last(customers.signup_at)` 5/40.

## Class (b) — SQL-shape re-bless batches per stage

- **Stage 1 (interning):** `dev1835_sql_baseline` `lift/band_x_wm` (all
  dialects) — the `_cm_amount_sum_partition_by_city_2` twin collapses; any
  other golden carrying a `_N`-suffixed duplicate producer.
- **Stage 2.2 (windowed kernel):** windowed cases of `dev1824` / `dev1835` /
  `dev1837` / `dev1839` baselines if kernel emission moves naming/shape.
  **Outcome: byte-identical — no re-bless.** The kernel emission reproduces
  the legacy field bundle exactly (probe-verified: the full `_wm_` arm was
  unreachable before deletion, unit + sqlite/duckdb integration green).
- **Stage 2.3 (ranked kernel):** `dev1748_first_last_baseline`,
  `dev1835` `mig/bare_rk`.
  **Outcome: byte-identical — no re-bless.** Kernel-driven ranked emission
  reproduces the legacy plan bundle exactly. Deletion re-sequencing (probe
  evidence): `build_target_ranked_plan` + the RANKED_TARGET classifier arm
  were probe-dead (0 hits, full suite + sqlite/duckdb integration) and are
  deleted, the arm replaced by a D8 raise; the legacy ranked-collapse
  dispatch + `_collapses_to_ranked_cte` were probe-dead post-kernel and are
  deleted. `RankedAggregatePlan` + `build_host_ranked_plan` + the full
  `_rk_` renderer arm survive to task 2.4: the
  `disable_host_rooted_isolation=True, enable_producer_regroups=False`
  sub-plan context (no desugar) still builds/renders them — pinned by
  `test_sql_generator::TestDev1709WidenedIsolationShapes` — and that context
  is exactly what 2.4/2.5 dissolves. Ranking-key precedence + plan-node pins
  ported from `test_dev1748_ranked_plan` to `test_dev1838_kernels`
  (suite retires with the class in 2.4).
  **Ranked deletion completed inside 2.4:** the DEV-1709 recursion-guard pins
  ported OFF the disabled context (kernel form, `test_sql_generator`),
  unblocking the full deletion — `RankedAggregatePlan`,
  `build_host_ranked_plan`, `_host_grain`, the classifier RANKED_HOST
  plan-construction arm (→ skip: the kernel owns the emission), the full
  `_rk_` renderer arm + join/projection/order-env sites, the rerooted-ranked
  residual guard (its `ARM_REROOTED_RANKED` message leaves the sources — that
  stage-4 no-residue sweep case flips green early), and
  `PlannedQuery.ranked_aggregate_plans`. A fail-closed raise in the plain
  aggregate spec builder replaces the structural guarantee (D8).
  `test_dev1748_ranked_plan` RETIRED per dispositions (plan-node/precedence
  pins → `test_dev1838_kernels`; emission → `dev1748_golden_sql`; values →
  `test_dev1748_first_last_matrix`).
- **Stage 2.4 (host-rooted unification):** `dev1745` / `dev1747` / `dev1750`
  baselines; `test_sql_generator::TestIsolatedFilteredMeasureCTEs` shape
  asserts move to the producer form.
  **In-flight outcomes:**
  - `dev1750_sql_baseline` re-blessed (5 cases × 5 dialects): the three
    wscaled shapes to the producer form (executed values pinned unchanged in
    `test_dev1750_execution`); `b/liscaled_one_to_many` to the class-(d)
    raise record; `change/of_wscaled` to the LIFTED composing form.
  - **Lift (spec-consistent, closure axiom):** `change`/`change_pct` over a
    crossing-fragment inner now composes with correct values (was
    `RenderContextMissingFacilityError`); value pins in
    `test_dev1750_execution` (13.0 = 48−35, −2.0 = 46−48 hand-checks).
  - Class-(d) reconciles by the approved crossed-argument role:
    `test_dev1527_agg_kwargs` (li_weight over 1:N `line_items`) and
    `test_dev1750_execution` liscaled shapes flip to fail-closed pins.
  - **join_safety fix:** `provably_to_one` now matches uniqueness through a
    bare-identifier `Column.sql` rename (join pairs naming the RAW column vs
    a PK declared on its renamed model column); unit pins added in
    `test_dev1836_safety_predicate`. Widens proofs only.
  - **Production plan-walk fix:** `get_column_types` maps producer-answered
    measures (crossing / windowed / ranked) through attach substitutions —
    also closes the pre-existing gap for bare windowed/ranked probes.
  - Mixed AGGREGATE filter routing: each conjunct now routes to its OWN
    scope (local operand → `_base` HAVING; isolated operand → outer WHERE) —
    value-identical (1:1 base-group ↔ output-row), pinned in the reconciled
    `TestIsolatedFilteredMeasureCTEs` shape asserts.
  - `test_filtered_local_isolation` ported IN PLACE to the producer form
    (deviation from the RETIRE disposition: the trigger-matrix breadth —
    derived sources, kwargs, template fragments, composite leaves — has no
    other home; retiring would lose discovery-breadth coverage).
  - **D5 role clarification (source carve-out):** a crossing SOURCE
    (`Column.sql` reading through a join) is NOT gated — it consumes the
    target's values per-match (the DEV-1709 F1 semantics; the moral twin of
    a cross-model aggregate), generalizing the ledger's host-grain-source
    carve-out. It still TRIGGERS isolation (its own producer). Only filter
    references and arguments (args / kwargs / aggregation params) fail
    closed, exactly as the spec delta words it. Pinned by the recovered
    `TestDev1709SiblingProtection` integration values (`li_qty:sum` 10.0,
    filtered 5.0, ranked sibling 30.0); the crossed-kwarg integration pin
    (`weighted_avg(weight=li_qty)`) reconciled to the approved class-(d)
    error like its dev1527/dev1750 twins. The delta spec's headline sentence
    was amended accordingly ("source expression columns" removed from the
    gated list; the source carve-out spelled out) — flagged here for the
    author: the LEDGER (which never enumerated crossing-source errors) is
    treated as the approved contract over the delta's original headline.
- **Stage 2.5 (legacy deletions) — executed outcomes:** all class (a): goldens
  byte-identical, no re-bless.
  - DELETED end to end: `CrossModelAggregatePlan` + the `PlannedQuery` field,
    the generator's cm_ctes build loop, `_render_rerooted_cross_model_cte`,
    `_render_cross_model_cte`, `_guard_target_grain_time_shift`, the orphaned
    routed-filter helper family + `_cm_plan_identity`,
    `assert_no_unrooted_cross_model_plans` (checker + call; the transition pin
    retired in the same commit, the getattr-guarded sweep stays),
    `_plan_outer_where_filters` (vacuous — outer-WHERE ids now come only from
    the D7 combined-conjunct routing), `isolation.py`,
    `cross_model_planner.py`, query_engine's `_walk_cross_model_plans`.
    `_render_with_cross_model_plans` renamed `_render_with_combined_attaches`.
    Schema-drift attribution now collects producer root models off the attach
    walk instead of CMA target models.
  - `may_inline_crossing_inputs` relocated to `join_safety` AND wired into
    `_crossing_local_root_predicate` (behavior-preserving: hardcoded False;
    gives the DEV-1688 seam its one production call site). Seam pins ported to
    `test_dev1836_safety_predicate::TestMayInlineSeam`; the load-bearing pin
    proves flipping it keeps a crossing-input aggregate inline.
  - Suite dispositions executed: RETIRED `test_cross_model_planner`,
    `test_cross_model_planner_wiring`, `test_dev1450fix_reroot_strategy`,
    `test_dev1747_prebound_planner`, `test_dev1746_isolation_classifier`
    (may_inline pins → safety_predicate; the DEV-1783 item-6 crossing-paths
    union pin → `test_dev1838_kernels`; the ScopeFrame separation pin already
    lives in `test_scope`). `test_dev1744_naming_allocator` import-swapped
    (`cte_schema` profile kept — frozen legacy matrix still pins it;
    `TestCrossModelDedupIdentity` retired, equivalents in
    `test_dev1838_interning`). `test_dev1783_pr286_g1`'s
    `TestHostRootedRoutesLeaveWhereIdsEmpty` retired with its mechanism
    (per-conjunct routing pinned by the reconciled
    `TestIsolatedFilteredMeasureCTEs`). `test_planned` dropped the CMA model
    tests only.
  - Deviation (same rationale as filtered_local): `test_dev1750_guard_ownership`,
    `test_dev1747_reroot_filter_routing`, `test_dev1745_reachability` KEPT,
    ported in place rather than retired — the guard-ownership split, the
    inheritance-warning battery (spy retargeted to
    `_cross_model_inherited_filters`; no-bare-except pin retargeted to the
    producer inheritance path), and the key-kind reachability scans have no
    other home. `TestStructuralRouting` → `TestProducerInheritanceRouting`
    (the classify_host_filter PROPAGATE optimizations for prefix-of-target and
    crossing-derived references do not port: the DEV-1836 D3 attributable
    doctrine drops them with a warning, pinned behaviorally in
    `test_dev1836_filter_inheritance`).
  - `test_dev1747_order_entry` recursion-guard pin rewritten to the real
    observable (the wrap producer holds its key INLINE — no nested attach);
    the old "no CMA under the disabled flag" assert was vacuously true.
  - `test_dev1769_routed_filter_path_validation`: the direct-call layer
    (`TestRerootRoutedLeafDirect`) retired with the `_reroot_routed_leaf`
    seam — filters re-root at plan time now; the E2E layer (route-in /
    drop-and-warn) survives unchanged as the coverage.
- **Stage 3 (node fold / one Kahn driver):** the loser batch-order's transform
  goldens (`dev1824` / `dev1835` / `dev1837` / `dev1839` transform cases);
  fast-path plain cases MUST stay byte-identical (the `dev1838` fusion
  snapshots are the tripwire, class (a)).
  **Outcome: all class (a) — zero re-bless.** D7 measurement over the full
  golden corpus (898 keys, both orders run end to end): window-batch-first is
  byte-identical everywhere; temporal-first moved `dev1747`
  `chain/local_multi_step` on all 5 dialects. Canonical order = window batch,
  then temporal ops; the one driver (`_run_transform_chain`) holds it and the
  deadlock backstop. `_render_cross_model_transform_chain` deleted (the
  combined path now enters the shared `_render_steps_and_post` tail);
  the collapsed-ranked/collapsed-windowed pre-dispatch arms deleted (the
  kernel dispatch is the pipeline's aggregate phase,
  `_render_kernel_producer_body`, its grain base fused in as a subquery per
  D2 — byte-identical). DEV-1799 subsumed as designed.
- **Stage 4 (CTE-body lifts):** multi-stage statements flatten from a nested
  stage-internal WITH to one flat chain (today `exp.With` count = 2 on a
  producer-carrying stage; becomes 1). No golden pins these today.
  **Outcome: zero golden churn** (the flattened/leaking shapes had no
  goldens; all 898 golden keys byte-identical). Mechanics: (1) the wrap-attach
  synthesis un-desugars its grain (`row_attaches` reverse substitution) so the
  sub-plan re-desugars a banded grain into a nested ROW attach that interns
  onto the outer band producer — the placeholder leak dies at the plan level;
  the wrap producer plans with `enable_producer_regroups` when its grain
  carries a desugarable key (same rule as the cross-model synthesis); (2)
  `generate_planned_stages` opens ONE generation (`install_generation`: shared
  allocator + rendered-producer map, stage relation names reserved) and splits
  each non-final stage via `_split_statement_ctes` (the extracted D2 hoist),
  so stage internals hoist to the statement's top level; (3) the two CTE-body
  guard raises deleted and the vestigial `as_hoistable_producer` flag removed
  end to end; `TestCteBodyArms` reworked to the positive hoistable-body
  contract (task 4.2 authorization). Full suite green: 14718 passed, zero
  failures; the task-5.1 meta-test (no coexistence `NotImplementedError` in
  `slayer/sql/`, expressiveness allowlist explicit) landed green.

Every class-(b) re-bless runs its executed-value companion suite on SQLite +
DuckDB in the same commit.

## Today's broken shapes the lifts fix (not new errors — recorded brokenness)

- **Placeholder leak, invalid SQL:** a band (row-attach) grain member inside a
  filtered-local or host-grain-wrap sub-plan leaks
  `orders.__regroup__0__amount_sum_partition_by_city` into the `_cm_` CTE —
  SQLite errors "no such column" (surfaced as SchemaDriftError). Shapes: band +
  `gold_amount:sum`; band + ORDER BY `customers.tier`. Post-1838: execute with
  the oracles in `_dev1838_fixtures.py` (`BAND_GOLD`, `BAND_TIER_ORDER`).
- **Bind-time leak:** `amount:last(partition_by=city)` as a dimension +
  `gold_amount:sum` — ValueError "Column '__regroup__0__amount_last_…' not
  found". Post-1838: executes (`LASTDIM_GOLD`).
- **Nested WITH in multi-stage:** producer-carrying non-final stages render a
  WITH inside the stage CTE (2 `exp.With` nodes; invalid on T-SQL). Values are
  correct today (pinned); the flat-WITH half of the scenarios is the lift.

## Found-broken, out of this change's committed scope — flag to the author

- **Dual-role cross-model partitioned WITHOUT the partition key in the query
  grain** (`SPEND_BAND` dim + the same
  `customers.spend:sum(partition_by=customers.tier)` selected, dims =
  `[sband]` only): RuntimeError "Combined regroup attach is missing a host /
  producer grain slot for its join-back" — with or without filters, today.
  With `customers.tier` IN the grain the same dual-role shape executes,
  already shares one producer, and is pinned green
  (`test_same_aggregate_in_both_roles_shares_one_producer`,
  `test_shared_producer_dropped_filter_warns_once`). RESOLVED disposition
  (review round 1): verified the keyless-grain variant does NOT fall out of
  stage-1 interning — it still raises the same RuntimeError. Filed DEV-1850
  (hidden host key slot for the combined join-back); excluded from both spec
  deltas (`partitioned-aggregates` coexistence, `cross-model-aggregates`
  composition) and pinned by
  `test_dual_role_without_partition_key_in_grain_unsupported`.

## Task 2.0 — legacy plan-class field inventory (destinations)

`WindowedAggregatePlan` → trailing-window kernel / producer body:

| Field | Destination |
|---|---|
| `aggregate_slot_id` | producer body — the attach substitution's producer answer slot |
| `agg` | producer body — the answer slot's `AggregateKey.agg` |
| `window_raw`, `window_parts`, `window_granularity` | kernel: `window_raw` / `window_parts` / `window_granularity` |
| `window_time_dimension_slot_id` | kernel: `bucket_slot_id` (the producer's synthesized active-TD grain slot) |
| `dimension_slot_ids`, `other_time_dimension_slot_ids`, `grain_slot_ids` | producer body — the producer grain (row slots + attach `join_pairs`) |
| `where_filter_ids` | kernel: `src_where_filter_ids` (inherited into `_src`, frame bounds excluded) |
| `src_filter_rewrites` | kernel: `src_filter_rewrites` (DEV-1732 residuals) |
| `public_alias`, `hidden` | consumer slot (producer output named via `public_alias_by_agg`) |

`RankedAggregatePlan` → ranked kernel / producer body:

| Field | Destination |
|---|---|
| `aggregate_slot_id` | producer body — substitution answer slot |
| `agg` | kernel: `agg` (`first` ranks asc, `last` desc) |
| `root_model` | `RegroupAttachPlan.producer_root_model` |
| `datasource` | producer body (bundle) |
| `target_path`, `join_chain` | producer body — the producer roots at the target; its own join plan replaces the chain (DELETION-WITH-PIN: `test_dev1748_first_last_matrix` executed values) |
| `ranking_time_key` | kernel: `ranking_time_key` (plan-time resolved, producer-scope coordinates) |
| `grain` (`RankedGrainMember`) | attach `join_pairs` — host key ↔ producer grain slot; the partition IS the producer grain (single source, D8 join-covers-grain assert kept) |
| `where_filter_ids`, `having_filter_ids`, `applied_filter_ids` | producer body filter inheritance + `outer_where_filter_ids` routing |
| `target_model_filters` | producer body — root model's own `filters` |
| `dropped_filter_warnings` | attach `dropped_filter_warnings` (D6 event carriage) |
| `public_alias`, `hidden` | consumer slot |

`CrossModelAggregatePlan` → producer body / attach fields / deletion:

| Field | Destination |
|---|---|
| `aggregate_slot_id` | producer body — substitution answer slot |
| `target_model` | `producer_root_model` |
| `datasource` | producer body (bundle) |
| `join_chain` | producer body join plan |
| `join_back_pairs`, `shared_grain_slots` | attach `join_pairs` |
| `cte_stage_schema` | `producer_plan.stage_schema` |
| `applied/where/having_filter_ids` | producer body filter inheritance + `outer_where_filter_ids` |
| `target_model_filters` | producer body — root model filters |
| `dropped_filter_warnings` | attach `dropped_filter_warnings` |
| `hidden`, `public_alias` | consumer slot |
| `rerooted_plan`, `rerooted_grain_pairs`, `rerooted_agg_slot_id` | DELETION-WITH-PIN — target-rooted producers subsume re-rooting (DEV-1836); pins ported per task 0.2 dispositions |
| `cte_root_model` | `producer_root_model` (host-rooted producers root at the host) |

## Codex review of the tests — triage record (flow step 5)

Accepted (folded into the suite): dual-role cross-model green pin + the
shared-producer-warns-once pin on the tier-in-grain shape (T1/T2 — the
keyless variant stays flagged above); D8 sweep getattr-guarded so it outlives
the CMA deletion, and the synthetic-bad-plan pin documented as a transition
pin retiring with task 2.5 (T3); ranking-column cross-producer negative added
(T4); unsafe-input asserts strengthened with the input's name
(`alpha_amount` / `rush_amount` / `updated_at`) alongside hop + remedy (T5);
producer counts switched from name-substring matching to total top-level
`_cm_` counts — name-scheme-agnostic beyond the D3-pinned prefix (T6); the
two step-4 API contracts recorded in design D3/D8 (T7); the
`amount:last(customers.segments.updated_at)` ledger row rewritten as the D9
set `{5, 10}` (T8).

Rejected, with rationale:
- "require a dedicated safety exception type" (T5 half) — `(SlayerError,
  ValueError, NotImplementedError)` is the repo's established fail-closed
  contract (the DEV-1836 unsafe-input suite pins the same tuple); narrowing
  before implementation invites false failures without adding safety.
- "negative pin for two windowed producers differing only in frame-bound
  rewrites" (T4 half) — not constructible: rewrites derive from query-level
  filters, which apply to every scope alike (probe: both city producer
  bodies stay byte-identical under a mixed frame-bound filter; the rewrite
  lives in the windowed ``_src`` reader). Kernel-param divergence is covered
  by the duration/measure-filter negatives; filter-context divergence by the
  D3 identity unit test.
- "count structurally matching CTE bodies instead of the `_cm_` prefix"
  (T6 half) — the uniform `_cm_` naming is itself corpus-pinned (DEV-1835
  D3, "producers render under the uniform `_cm_` naming"), so prefix-level
  counting is spec-backed; body-structure matching would re-implement the
  golden harness inside unit asserts.
