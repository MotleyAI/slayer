# DEV-1909 design — one population disposition (re-planned against current main)

## Context

Re-planned 2026-09-18 against current main (HEAD `9e3ba6b9`, the dev-1832 merge). The earlier
implementation was discarded — it no longer merged with dev-1832 — and the plan approved
2026-09-16 is adjusted here to the code state that has since landed (dev-1832 cross-model
expression aggregation, dev-1910 home-rooted association, dev-1471 cross-stage time dimensions,
dev-1922 `source_model` union). Substrate facts, verified empirically on the DEV-1900 fixture
graph (SQLite + DuckDB):

- The DEV-1900 interim guard still stands: `check_population_filter_no_fanout`
  (`elaborate_env.py`), fired by `_assert_population_filters_no_fanout` (`stages.py`), raises
  for both oracles (population `spend:sum` over `orders.status='ok'`, hop `orders`; and
  `amount:sum` over the derived `customers.regions.bad_pop>0`, hop `region_events`). The base
  `queries/semantics` requirement carries an explicit *"until … (DEV-1909)"* forward-pointer to
  this change.
- The derived-`bad_pop` filter is a **population** restriction, not an aggregate input:
  `bad_pop` appears only in the filter and `amount:sum` is a plain local aggregate, so dev-1832's
  "Unsafe aggregate inputs fail closed" checks never see it. Its target stays 120 by association.
- Host-rooted regroup producers still fan **silently**: `_regroup_inherited_filters`
  (`stages.py`) inherits every stratum-0 FIELD mask verbatim into each producer's own FROM/WHERE,
  so `sum(spend, partition_by=tier)` returns gold 290 (target 190) and `sum(spend, window='1y')`
  returns April 520 (target 420); `_has_inline_population_aggregate` excludes partitioned/windowed
  aggregates, so the guard never fires there. Raw-row mode returns 6 join-multiplied rows (target 5).
- The association case is **already correct** (dev-1910): `_synthesize_association_producer` is
  gone; a local aggregate over an unattributable grain takes the association arm of
  `_synthesize_cross_model_producer` (`_association_arm` / `_association_inline_filters`), home-roots,
  and inlines the reachable conjunct with per-entity dedup, emitting a `semi_join_pushed` entry.
  Associate `spend:sum` by `orders.status` filtered `orders.amount in (20,30)` already returns
  100 / 150. **This change does not touch the association arm.**
- The semi-join IR is `SemiJoinFilter` on `PlannedQuery.semi_join_filters` (`ir/planned.py`); there
  is no `root_relation` field and `PreboundQuery` carries no semi-join groups. Target-rooted
  producers attach their groups after recursive compilation (`_synthesize_cross_model_producer`);
  `_synthesize_wrap_attach` recursively compiles via `compile_synthesized`.
- `SemiJoinPushedWarningPayload.measure` is required (`str`); `_collect_semi_join_pushed_warnings`
  walks producer attaches only.

## Goals / Non-Goals

**Goals:** one disposition of the population's ROW filters, computed once at the host root and
consumed by the host base query and every **host-rooted regroup** producer (partitioned, windowed,
first/last, host-grain wrap, broadcast-local); the population restricted by association in every
query shape (oracles 1–5); same-row binding wherever a consumer's grain materialises the branch;
the interim guard retired; fail-closed residues; goldens re-blessed where the host base fanned.

**Non-Goals:** the association arm and target-rooted producers (dev-1910 / dev-1840 own them);
recursive boolean lowering of out-of-scope conjuncts (`OR`/`NOT` mixing local and cross-path refs,
or several branches) — deferred to **DEV-1935**, which replaces this change's fail-closed/drop
residue with per-branch `EXISTS`; Mode-A `SlayerModel.filters` / column `filter=` fragments crossing
fanning hops; a target-rooted producer nested inside a host-rooted producer disposing only the
parent's inline conjuncts (pre-existing); window association under `associate` for local and
cross-model windowed aggregates (**DEV-1914**, decision 12); DEV-1908 / DEV-1911.

## Decisions

1. **One `PopulationFilters` object (engine.compile).** `dispose_population_filters(prebound,
   filter_typings, scope, bundle)` runs once in `compile_prebound` at the top level (the guard's
   trigger point today), before producer synthesis. Per population conjunct — a top-level AND
   conjunct of a ROW-phase, FIELD-typed, stratum-0 filter, date-range bounds included — it records
   `(key, text, typing, disposition ∈ {inline, semi_join, excluded}, fanning_paths, group, warning)`
   via `_conjunct_disposition` with the host as root. Consumers derive views: `view(grain_paths)`
   returns the masks to apply inline (with aligned texts/typings, date-range bounds first), the
   `SemiJoinFilter` groups to attach, and the excluded warnings. It replaces
   `_regroup_inherited_filters`'s treatment of fanning conjuncts and `_assert_population_filters_no_fanout`.
   *Alternative* — patch the host base and the regroup path independently — rejected: two sites that
   must agree by hand is the bug class (system arc42 §3, structural-fix bias).

2. **Per-consumer same-row rule (Axiom 3 Association + Law 5 Dice–slice).** A consumer applies a
   `semi_join` conjunct inline iff its own grain materialises every fanning path of that conjunct
   (each fanning path is a prefix of, or equal to, a closure path of one of its grain keys); else it
   takes the `EXISTS`. Host base: projected dimensions and time dimensions — a dims-only or
   producer-only base binds a same-branch conjunct to the dimension's row; DEV-1841 routes a fanning
   projected dimension to a producer, so a host-base inline aggregate never coexists with a fanning
   projected path and always takes the `EXISTS`. Producer: its partition keys plus the window time
   key — both attributable from the population by construction (the partition-key rule; decision 12
   for the window axis), so a host-rooted producer never materialises a fanning path and always
   takes the `EXISTS`. One generic rule serves both consumers.

3. **Groups attach on the plan; the disposition threads through compilation.** The `EXISTS` groups
   live on `PlannedQuery.semi_join_filters` (the dev-1840/1910 field), not the prebound; there is no
   `root_relation`. `dispose_population_filters` runs once at the top, and the resulting
   `PopulationFilters` is threaded — as a compile-context parameter — through `compile_synthesized`
   into every host-rooted producer build, so a **nested** host-rooted producer receives it during its
   own recursive compilation and stores its own groups on its own final `PlannedQuery`. Each
   consumer drops the pushed conjuncts from its inherited verbatim masks. *Alternative* — post-hoc
   `model_copy(update={"semi_join_filters": …})` on top-level plans — rejected (Codex): a nested
   host-rooted producer is compiled before the caller can post-attach, so it would miss the restriction.

4. **The association / target-rooted boundary is by producer kernel, not by root.** A host-rooted
   producer (`target_path == ()`) can still be an association producer (oracle 6), so the exclusion
   is keyed on the association arm / kernel, never on "root == host": the association arm keeps its
   dev-1910 routing and receives **no** population `semi_join_filters`; a target-rooted producer keeps
   its dev-1840 metric-root disposition. *Alternative* — exclude by `root == host` — rejected (Codex):
   it would double-apply population groups onto host-rooted association producers.

5. **Residues.** `excluded` (out of pushdown scope — `OR`/`NOT` mixing local and cross-path refs, or
   several branches): stays a mask on the host base, dropped from every producer onto the attach's
   `dropped_filter_warnings` (error mode errors through the existing collector), and
   `check_population_filter_in_pushdown_scope(filter_text, reason)` raises whenever such a conjunct
   **would multiply** the population — a plain aggregate is inline over the population **or** raw-row
   mode is on (`distinct_dimension_values=false`) — so the raw-row grain guarantee holds. Closure
   `None`: `check_filter_dependencies_analyzable(filter_text, column)` raises from `_conjunct_disposition`
   for host and producers alike. The proper association handling of these conjuncts is **DEV-1935**.
   *Alternative* — leave an out-of-scope conjunct inline in raw-row mode — rejected (Codex): it
   multiplies raw rows, breaking the grain guarantee.

6. **Backstop.** `compile_prebound` asserts that a spine-covered fanning mask never coexists with an
   inline plain aggregate (decision 2's premise, pinned).

7. **Empty-base spine.** `_plan_empty_base_grain` treats non-empty population groups as host gating
   (`EmptyBaseGrainPlan.host_gated`); the renderer's placeholder branch builds the host FROM, applies
   WHERE and the `EXISTS` conditions, and `LIMIT 1` whenever masks or groups exist — so a producer-only
   query over a population nobody passes yields zero rows.

8. **Reporting.** `SemiJoinPushedWarningPayload.measure: Optional[str] = None`; the collector emits one
   entry per `(location, None, text)` from each top-level plan's groups; producer entries unchanged;
   dedup identity unchanged. ClickHouse gating already reads top-level plans.

9. **Checker and ledger.** Two checker rules replace `check_population_filter_no_fanout`
   (`check_population_filter_in_pushdown_scope`, `check_filter_dependencies_analyzable`); the ledger
   swaps its two rows for two; `guards` baseline unchanged (neither is a `NotImplementedError`).

10. **Normative edit (needs per-change approval).** `architecture/semantics.arc42.md` axiom 14 gains
    `[enforced: test:tests/test_dev1909_population_pushdown.py]`; the verbatim one-line diff is shown
    before it is applied.

11. **Test-impact protocol.** Repository-wide audit of customers-rooted queries filtering across
    `orders` / `order_tags` / `regions.region_events` and of every dropped-filter assertion; each
    affected test is classified unchanged / re-blessed / newly carrying `semi_join_pushed`; ambiguous
    correlation keeps failing closed; any other shifting test stops for a ruling. `test_dev1900_population_guard.py`'s
    raise cases are re-pointed to executed-value oracles; the unanalyzable/out-of-scope cases stay raises
    via the new checkers.

12. **A windowed aggregate's time axis must be attributable from its home (ruling 2026-09-18).**
    `sum(spend, window='1y')` by `orders.ordered_at` from `customers` is not a population-filter case:
    the time bucket is a grain member of the windowed aggregate (Axiom 2.3) that its home does not
    determine (Axiom 7), so it resolves per the mode axis (Axiom 8) — broadcast has no denotation for a
    window (no value exists at the axis-less grain), associate is the distinct-entity dedup, error
    refuses. Today the host-rooted producer multiplies the root-local value once per matching order in
    every mode, unwarned, with or without a filter (300/600/700/810 unfiltered): an Axiom 4 violation
    the population push cannot fix. The cross-model spelling of the same node already fails closed
    (`check_windowed_cross_model_time_axis`; `queries/cross-model-aggregates` › Explicit grain and
    window), associate mode refuses `window=`, and an explicit fanning partition key fails in every
    mode; spelling-invariance (Axioms 2.5/2.7) gives the host-rooted spelling the same rule. So: the
    checker rule is generalised to every windowed aggregate (name and message drop "cross-model") and
    fired from the host-rooted windowed regroup path, mode-invariant; the DEV-1909 windowed scenario
    becomes the local-axis window (`customers.signup_at`, same oracle series 100/250/310/420, verified
    on current code) plus a fail-closed scenario. Window association — frame membership by association,
    same-row filter binding, per-entity dedup inside the frame, under `associate`, both spellings — is
    **DEV-1914** (oracles recorded there). *Alternatives rejected*: dedup inside the window in every
    mode (association without the mode opt-in: in one query the plain sum would broadcast while the
    window associates, against Axiom 8 and the cross-model rule); dedup only when a population conjunct
    is inlined (keeps the unfiltered multiply; one shape, two behaviours).

## Risks / Trade-offs

- [Golden churn on dims-only / producer-only shapes with fanning host filters] → intended (uniform
  push); ALLOWED_DELTAS carry reasons; values pinned by executed oracles.
- [A same-row case misclassified as `EXISTS`] → the per-consumer rule is pinned by the association,
  same-branch and local-axis window scenarios; decision 6's assertion fails closed on the reverse mistake.
- [A windowed query that returned numbers now errors] → intended (decision 12): the numbers were
  join-multiplied in every mode; the error names the time dimension and the remedy.
- [Population groups leak onto a host-rooted association producer] → decision 4's kernel-keyed boundary;
  a structural test asserts the association producer receives no population groups.
- [Nested host-rooted producer misses the disposition] → decision 3's compile-context threading; a
  nested-producer test inspects both bodies for the `EXISTS`.
- [ClickHouse < 25.4 now fails closed for host pushes] → already the producer rule; error names the
  version requirement.
- [EXISTS plan cost on large fanning tables] → accepted; correctness first.

## Migration Plan

Pure planner / renderer change; no stored-artifact migration. PR base is main; merge only, never
rebase. When DEV-1911 lands (`join_safety.py` / `bind_inputs.py` — no file overlap), re-merge main.
Rollback = revert the PR.
