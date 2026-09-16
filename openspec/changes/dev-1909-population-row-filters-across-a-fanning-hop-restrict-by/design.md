# DEV-1909 design — one population disposition

## Context

See proposal.md — Why. Substrate facts that shape the design (verified on the DEV-1900 fixture
graph, SQLite, 2026-09-16):

- `_conjunct_disposition` / `_conjunct_push_plan` (`compile/stages.py`) already dispose a ROW
  conjunct three ways relative to any root; `_synthesize_association_producer` already calls them
  with the host as root (`target_path=()`), and `_synthesize_cross_model_producer` does the same
  for a local aggregate routed through the mode axis. `_regroup_inherited_filters` (partitioned,
  windowed, first/last, host-grain wrap) inherits every stratum-0 field mask verbatim.
- The renderer applies `PlannedQuery.semi_join_filters` on every base and producer body
  (`_generate_from_planned_impl`, `_render_with_combined_attaches`, the window `_src`, the ranked
  and association bodies, the windowed grain base) EXCEPT the empty-base placeholder branch of
  `_render_with_combined_attaches`, which reads only `EmptyBaseGrainPlan.host_filter_ids` and
  emits a bare `SELECT 1` when that list is empty.
- `_plan_src_row_filters` subtracts date-range masks and `frame_bound_columns` from a trailing
  window's `_src`; semi-join conditions are applied to `_src` unconditionally.
- DEV-1841 routing sends every plain local aggregate to a producer whenever a projected dimension
  or time dimension is unattributable from the host, so a fanning projected path and an inline
  plain aggregate never coexist.
- `SemiJoinFilter` conjuncts and hops are in one root's coordinates; the renderer correlates the
  first hop against `planned_query.source_relation`.
- `_collect_semi_join_pushed_warnings` walks producer attaches only; the payload's `measure` is
  required.

## Goals / Non-Goals

**Goals:** one disposition of the population's row filters, consumed by the host base and every
producer rooted at the population; the population restricted by association in every query
shape; same-row binding wherever a consumer's grain materialises the branch; the interim guard
retired; fail-closed residues; goldens re-blessed where the host base fanned.

**Non-Goals:** recursive boolean lowering of OR-mixed conjuncts (per-branch EXISTS under the
original OR); Mode-A `SlayerModel.filters` / column `filter=` fragments crossing fanning hops; a
target-rooted producer nested inside a host-rooted producer still disposes only the parent's
inline conjuncts relative to its own root (pre-existing); DEV-1908 / DEV-1910.

## Decisions

1. **One `PopulationFilters` object (engine.compile).** `dispose_population_filters(prebound,
   filter_typings, scope, bundle)` runs once in `compile_prebound` at the top level (the guard's
   trigger condition today), before producer synthesis. Per population conjunct — a top-level AND
   conjunct of a ROW-phase, FIELD-typed, stratum-0 filter, date-range bounds included — it
   records `(key, text, typing, disposition ∈ {inline, semi_join, excluded}, fanning_paths,
   group, warning)`, using `_conjunct_disposition` with the host as root. Consumers derive views:
   `view(grain_paths)` returns the masks to apply inline (with aligned texts/typings, a split
   multi-conjunct string contributing its kept conjuncts in place with the parent's typing and
   `None` text, date-range bounds first), the `SemiJoinFilter` groups to attach, and the excluded
   warnings. It replaces `_regroup_inherited_filters`, the association producer's own
   disposition loop, and `_assert_population_filters_no_fanout`. *Alternative* — fix each site —
   rejected: three sites that must agree by hand is the bug class.
2. **Per-consumer same-row rule.** A consumer applies a `semi_join` conjunct inline iff its own
   grain materialises every fanning path of that conjunct (each fanning path is a prefix of, or
   equal to, a closure path of one of its grain keys). Host base: projected dimensions and time
   dimensions. Producer: its partition keys plus the window time key; the host-grain wrap adds
   its source path (position parity). Partitioned-by-a-to-one-key and broadcast producers have
   no fanning grain and take the EXISTS; association producers and fanning-axis window producers
   keep same-branch conjuncts inline. *Alternative* — spine rule for the host base only,
   producers always EXISTS — rejected: it reproduces the association producer's 250/250 defect
   and would push a fanning-axis date bound into a window's `_src`.
3. **Groups live on the prebound, rooted.** `PreboundQuery.semi_join_filters` and
   `SemiJoinFilter.root_relation`; `compile_prebound` copies the groups onto the plan and asserts
   every group's root equals the plan's `source_relation`. Population groups are passed only at
   host-rooted construction sites (`_regroup_producer_prebound`, the wrap, the association and
   broadcast-local syntheses); target-rooted producers keep building their own groups from the
   original conjuncts via `_cross_model_inherited_filters` and set them on their prebound. A
   nested host-rooted producer inherits its parent's groups by construction; the two post-hoc
   `model_copy(update={"semi_join_filters": …})` sites go. *Alternative* — post-hoc copy on the
   plan — rejected: nested producers miss it.
4. **Residues.** `excluded` (out of pushdown scope): stays a mask on the host base (applied
   through the join, numerically right whenever no plain aggregate is inline), dropped from every
   producer onto the attach's `dropped_filter_warnings` (error mode errors through the existing
   collector), and `check_population_filter_in_pushdown_scope(filter_text, reason)` raises when
   `_has_inline_population_aggregate` holds — that predicate survives only as this trigger.
   Closure `None`: `check_filter_dependencies_analyzable(filter_text, column)` raises from
   `_conjunct_disposition` for host and producers alike. *Alternative* — drop the conjunct from
   the host base too — rejected: it would unfilter today's correct dims-only results.
5. **Backstop.** `compile_prebound` asserts that a spine-covered fanning mask never coexists
   with an inline plain aggregate (decision 2's premise, pinned).
6. **Empty-base spine.** `_plan_empty_base_grain` treats non-empty population groups as host
   gating (`EmptyBaseGrainPlan.host_gated`); the renderer's placeholder branch builds the host
   FROM, applies WHERE and the EXISTS conditions, and `LIMIT 1` whenever masks or groups exist.
7. **Reporting.** `SemiJoinPushedWarningPayload.measure: Optional[str] = None`; the collector
   emits one entry per `(location, None, text)` from each top-level plan's groups; producer
   entries unchanged; dedup identity unchanged. ClickHouse gating already reads top-level plans.
8. **Checker and ledger.** Two checker rules replace `check_population_filter_no_fanout`; the
   ledger swaps its two rows for two; `guards` baseline unchanged (neither is a
   `NotImplementedError`).
9. **Normative edit (approved).** `architecture/semantics.arc42.md` axiom 14 gains
   `[enforced: test:tests/test_dev1909_population_pushdown.py]`; the verbatim diff is shown
   before it is applied.
10. **Test-impact protocol.** Repository-wide audit of customers-rooted queries filtering across
    `orders` / `order_tags` and of every dropped-filter assertion; each affected test is
    classified unchanged / re-blessed / newly carrying `semi_join_pushed`; ambiguous
    correlation keeps failing closed; any other shifting test stops for a ruling.

## Risks / Trade-offs

- [Golden churn on dims-only / producer-only shapes with fanning host filters] → intended
  (uniform push); ALLOWED_DELTAS carry reasons; values pinned by executed oracles.
- [A same-row case misclassified as EXISTS] → the per-consumer rule is pinned by the
  same-branch, association and fanning-axis window scenarios; decision 5's assertion fails closed
  on the reverse mistake.
- [Host-coordinate groups attached to a target-rooted plan] → `root_relation` assertion at
  compile; nesting tests inspect both SQL bodies.
- [ClickHouse < 25.4 now fails closed for host pushes] → already the producer rule; error names
  the version requirement.
- [EXISTS plan cost on large fanning tables] → accepted; correctness first.
- [Pushed conjunct's display text names the whole multi-conjunct string] → same as producers
  today.

## Migration Plan

Pure planner / renderer change; no stored-artifact migration. PR base is the DEV-1900 branch
until it merges, then main; merge only. Rollback = revert the PR.
