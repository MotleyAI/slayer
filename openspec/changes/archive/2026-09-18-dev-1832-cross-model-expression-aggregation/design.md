# DEV-1832 design — one source anchor, one home, one filter meaning

## Context

See proposal.md — Why. Substrate facts verified on the branch (stacked on the
DEV-1900 head `18ec88ff`):

- The only code standing between the three rejections and a compiling query is the
  rejection itself: `binding._bind_expression_agg_source` (dotted leaves),
  `binding._reject_filtered_expression_operands`, and the transform arm of
  `syntax._validated_agg_source`. The closure walk (`reference_closure.key_closure`),
  re-rooting (`join_safety.reroot_from_root`), join registration
  (`ScopeFrame.resolve`) and input safety already traverse expression leaves.
- About fifteen sites answer "where does this source live" from
  `getattr(source, "path", ())` / `key_host_path(agg.source)`: `is_cross_model_agg`,
  `is_local_partitioned_agg`, `is_local_combined_regroup_ref`,
  `crossing_local_root_predicate`, `_home_path`, `_default_home_candidate_paths`, the
  `locus="host"` branch of `_synthesize_cross_model_producer`, `_resolve_agg_owner`,
  `_resolve_column_filter_key`, `_column_filter_closure`, `_leaf_closure`,
  `grain_member_attributable`'s aggregate arm, `assert_partition_key_attributable`,
  `_key_display`, `compile/staging.py`, the generator's `_resolve_agg_inputs_via_scope`
  and `_resolve_fragment_kwargs`, `canonical_aggregate_alias`, the star branch of
  `render/value_expr._render_builtin_aggregate`. An expression source has no `path`,
  so each answers "local": with the binder check removed, `sum(customers.spend + 0)`
  would home at `orders` while `customers.spend:sum` homes at `customers`.
- `Aggregate.home` in the elaborated environment is the query root for every
  aggregate (`elaborate_env._aggregate_terms`); the real home is computed at producer
  synthesis (`compile/stages._home_path`). `compile_prebound` types a synthesized
  sub-plan locally when `filter_typings is None` (six call sites), so nested producers
  carry no environment today.
- `Column.filter` is applied by `AggregateKey.column_filter_key`, the binder
  resolver, two closure arms, two generator join passes, five `_wrap_filter` sites, a
  `count(*)` special case, and the ranked picked-value CASE. Parameters are masked by
  the source's filter; a filtered parameter is not masked; dimension / filter / order
  positions read the raw value. The binder makes a column derived only when
  `Column.sql` is set and differs from the name; `is_trivial_base` and
  `column_dependency._column_dependencies` reason about `Column.sql` alone.
- Constituent classifiers (`operand_aggregates`, `source_row_leaves`,
  `attached_inputs`, `is_reaggregation_key`, `is_row_attach_root`,
  `attached_operand_keys`, `walk_consumer_keys`) recognise only `AggregateKey`; a
  transform's inner aggregate is found instead of the transform.
  `regroup_root_grain(TransformKey)` unions the inner partition grains;
  `_effective_root_grain` adds the active bucket for a windowed inner. The carrier
  registry mints placeholders for any `ValueKey`; `_regroup_producer_prebound`
  accepts transform roots as answers; `_answers_need_nested_regroups` is true for
  transform answers.
- The guard ratchet allowlists `^Cross-model operand inside an aggregated expression
  is not supported\.$` with no matching raise (stale). `SqlExprKey`'s only producer
  besides `sql_expr.parse_sql_expr` is the filter resolver.

## Goals / Non-Goals

**Goals:** one accessor for "where a source lives"; the home resolved once, in
elaboration, and consumed by the compiler; one meaning for `Column.filter` with no
per-site machinery; transforms as first-class constituents through the existing
carrier / row-attach / transform-root producers; the three v1 refusals gone; goldens
byte-identical except the enumerated behaviour changes.

**Non-Goals:** routing disposition (which producer kind) in elaboration and typed
`first`/`last` dispatch (DEV-1903); inlining crossing inputs (DEV-1688 seam,
`may_inline_crossing_inputs` stays `False`); reverse-hop cancellation for defaults
(DEV-1908); windowed constituents in pure re-aggregation beyond what the bucket-aware
grain helper yields (probe, see D4).

## Decisions

1. **One source-anchor accessor, swept everywhere.** `source_anchor_path(source)` and
   `source_leaf_paths(source)` in `core/keys.py`: a column / star source yields its
   own path, a literal-only source `()`, an expression the longest common prefix of
   its leaves' paths (leaves = the row-level column/star leaves; attached constituents
   are opaque). Every site in Context bullet 2 reads the accessor; an AST test in
   `tests/test_dev1832_anchor.py` fails on ANY `getattr(<expr>.source, "path"…)` /
   `<expr>.source.path` read in `slayer/engine` and `slayer/sql` outside the accessor
   (Codex F4: enumeration is the first task; the guard is not "new reads only").
   Alternative — special-case expression sources at the sites the examples hit —
   rejected: fail-open at every unpatched site (core P3, system P13).
2. **Home resolved in elaboration; the term carries it.** `Aggregate` gains
   `home_path: Tuple[str, ...]` (relative to the environment's host); `ModelDataset`
   is unchanged (Codex F5: a path on the dataset conflates identity with routing and
   goes stale on re-rooting). A new `engine/home.py` holds the S2 rule — the generalised
   `_home_path` seeded from every source leaf path, parameters, defaults and every
   attached constituent's grain members (Axiom 2.3: explicit `partition_by=`, else
   the query dimensions; a transform's union grain, bucket-aware), via closures —
   imported by `elaborate.py`, which resolves each aggregate's home and
   passes the mapping into `build_environment`; `elaborate_env` stays engine-import-free
   (it must not import `join_safety`, which imports it). `_synthesize_cross_model_producer`
   reads `env.terms[agg].home_path` for `target_path`; `_home_path` and
   `_default_home_candidate_paths` leave the compiler. No-home = anchor, and the
   existing input-safety checker raises. Alternative — generalise `_home_path` in place
   — rejected by the user (the decision belongs to the typed elaborator).
3. **Every synthesized sub-plan is elaborated** (Codex F1). One helper
   `compile_synthesized(prebound, *, source_model, bundle, scope, …)` = `elaborate_query(prebound=…)`
   + `compile_query`; the six `compile_prebound` call sites in `compile/stages.py`
   (wrap attach, cross-model, association, re-aggregation outer, carrier, local
   partitioned) use it; `compile_prebound` requires `env` and its local-typing branch
   is deleted. Sub-plans resolve homes relative to their own root. Alternative —
   thread a home resolver into recursive contexts — rejected: two typing doors.
4. **Transform constituents through existing producers; every grain explicit.** The
   classifiers in Context bullet 5 treat `AggregateKey | TransformKey` as constituents,
   stopping at either; `binding._source_is_reaggregation` / `syntax._is_mixed_agg_source`
   count a `TransformCall` as attached; `AggCall.source` and `_AggregateSource` admit a
   transform so a bare `sum(cumsum(…))` parses and binds. A transform constituent is a
   typed dataset (Axiom 2.3) at the union of its inner aggregates' grains, evaluated by
   the transform-root regroup producer (computed-dimension path) and consumed by the
   carrier (pure) or the row attach (mixed) unchanged; `constituent_grain(c, …)` =
   `effective_root_grain` (bucket-aware) for both kinds (Codex F3) and drives the
   carrier's union grain, the home rule and `_root_grain`. Sub-decisions (Egor,
   2026-09-17):
   - **4a Explicit axis, no implicit rewrite.** A time-ordered constituent takes its
     axis from `partition_by=` exactly as in dimension position —
     `sum(cumsum(amount:sum(partition_by=[region, ordered_at])) - 1)`, where
     `ordered_at` maps to the query's bucket (the DEV-1824 spelling) — and a grain
     lacking the axis fails with the time-axis error. Top-level measure-position
     transforms keep evaluating at the query grain over the attached value
     (`queries/partitioned-aggregates` › Partitioned aggregates nested inside
     transforms; DEV-1868 executed pins): no behaviour change. Alternative — join the
     query's active bucket onto every grained local inner of a time transform at bind
     time (handover-5 "E6") — rejected: it silently changes a specified legal spelling
     (`cumsum(amount:sum(partition_by=region))` 60/120/180 → 10/30/60), cannot apply
     to cross-model inners (the host bucket is not attributable from the target, so
     DEV-1868's broadcast reading would survive only there), and makes one spelling
     mean different things in dimension and measure position.
   - **4b Normalisation.** Inside a transform constituent in measure / filter / order
     position, an ungrained, non-windowed, local inner aggregate is explicitly grained
     at the query grain — the dimensions plus the time-dimension buckets — at bind
     time, BEFORE partition-key validation so the synthesized keys pass the same
     attributability and resolution checks as a user-written `partition_by=` (Codex:
     a query dimension the inner's home does not determine fails closed with the
     partition-key error rather than becoming an unchecked explicit grain)
     (`core/keys.normalize_transform_constituents`, one `_map_bound_keys` pass that
     skips dimension-position measures). One canonical
     representation: the union grain is always the union of explicit grains, so a
     mixed operand `rank(a:sum(partition_by=[region, ordered_at]) - b:sum)` types at
     `(region, month)` with `b` computed at the query grain and broadcast, never
     re-evaluated per region. A constituent with no inner aggregate (`rank(region)`)
     types at the query grain (`effective_root_grain`, transform arm). All-ungrained is
     the degenerate identity plus warning, exactly as `sum(sum(amount))`. Dimension
     position is untouched — its inners must already be explicit (Axiom 9 residue).
   - **4c Collapsing transforms.** `first` / `last` reduce along the axis: the
     dataset's grain is the operand grain minus the axis (Axiom 11.3b). Lowered at bind
     time into an exact second-order pick: a top-level collapsing constituent `t` becomes
     `max(t, partition_by=<operand grain − axis>)` (`core/keys.lower_collapsing_constituents`,
     `AXIS_COLLAPSING_TRANSFORMS` in `core/enums.py`), so the carrier, attributability
     and the mode axis see the collapsed grain while the transform is still evaluated
     with its axis inside the nested producer — no new producer arm. `max` is exact:
     the value is constant along the axis within a partition. Preserving ops
     (`cumsum`, `lag`, `lead`, `time_shift`, `change`, `change_pct`,
     `consecutive_periods`) keep the operand grain. A nested collapsing transform
     inside a constituent's input (`cumsum(last(x))`) evaluates within the enclosing
     producer as today; only the constituent boundary collapses. Alternative — a
     dedicated collapse arm in `_build_carrier_attach` — held as the fallback only if
     the nested re-aggregation-constituent path cannot render (probe first); the shape
     ships in this change either way. Alternative — treat `first`/`last` as
     axis-preserving — rejected: ragged partitions over-count and the axis broadcast
     goes unwarned.
   - **4d Checkers.** `check_dimension_temporal_axis` also walks every transform
     reachable inside a measure-source constituent — nested ones included, as the
     dimension arm does (Codex: a nested time transform must not evade Axiom 11.5) —
     with a position-neutral message (ledger row and the `dev1839` golden raises
     re-recorded); `_attach_time_keys` and `_time_search_children`
     descend into aggregate sources, args and kwargs so a constituent's transform gets
     the query's bucket and the no-time-dimension error reaches it; the row-leaf
     checker already walks sources.
   - **4e Deferred shapes, fail-closed.** A windowed inner under a transform
     constituent (`sum(rank(amount:sum(window='90d', partition_by=region)))`) hits the
     windowed time-dimension error (probe-verified); a cross-model grained inner under a
     transform constituent is unprobed. Each is pinned as a typed error (never wrong
     numbers) — or, if it executes correctly, as executed values — and any pinned
     error is deferred to a follow-up issue with a detailed comment naming it as an
     Axiom 9 closure gap (Codex: a well-typed shape refused for an implementation
     reason is a closure violation, tolerated only as an explicitly tracked deferral).
5. **Filter desugar is structural, not textual** (Codex F2). `Column.sql` semantics and
   `is_trivial_base` are unchanged. A new `Column.needs_expansion` (non-trivial value
   or filter set) drives the binder's `ColumnSqlKey` decision and
   `reference_closure._derived_column`. The wrapper `CASE WHEN (<filter>) THEN (<value>) END`
   is applied at the three expansion seams AFTER the value expands — the reference-site
   expander (`column_expansion._process_reference_site`), the scope's `ColumnSqlKey`
   anchor (`sql/scope.py`), and the generator's aggregate-source expansion — the filter
   expanded at the same owner path through the same door. `column_dependency` adds the
   filter's references as edges (the value's self-reference is not one; a filter naming
   its own column is a cycle); `fragment_closure` covers both texts. Deleted: everything
   in Context bullet 4 except the seams above, plus `AggregateKey.column_filter_key`,
   `AggRenderSpec.filter_sql`, `compute_column_filter_join_paths`, the compiler's
   identity tuples, `render/value_expr`'s filter guard; `SqlExprKey` and
   `parse_sql_expr` retired (`canonical_sql_text` keeps returning text). Alternative —
   compose the definition as text — rejected: the wrapped value's self-name is a false
   cycle for every filtered physical column.
6. **Accounting.** The binder raises and the parse arm are not ledger rows; the stale
   ratchet entry is removed; `guards.baseline` unchanged (no `NotImplementedError`
   added or removed). Naming needs no rule: `expression_source_leaf` already collapses
   dots.
7. **Normative edits proposed, not applied**: `enforced:` tags on semantics axiom 2
   (home test, applied 2026-09-16) and axiom 6 (transform-constituent test); the
   Axiom 11 rewrite as sub-rules 11.1 operand grain / 11.2 timeless / 11.3 time-ordered
   with 11.3a preserving and 11.3b collapsing / 11.4 position / 11.5 recursion (draft
   approved by Egor 2026-09-17), with the Axiom 2.3 transform clause re-pointed at
   Axiom 11's result grain; the exact diff is presented for approval at implement
   time. No `index.yaml` change: `models/` is already a cross-cutting spec group.

## Risks / Trade-offs

- [Sweep misses a source-path read] → the AST guard turns red on any remaining read;
  enumeration is task 1.
- [Elaborating every sub-plan changes typing of a nested producer] → sub-plans were
  typed by the same `type_and_split_filters` with the same flags; `elaborate_query`
  reproduces that call; goldens byte-identical or the divergence ledger stops the work.
- [Windowed inner under a transform constituent has no carrier arm] → D4e: typed error
  pinned, deferral documented on a fresh issue.
- [The collapsing desugar nests a re-aggregation constituent inside the carrier (four
  producer levels)] → D4c probe-first; the reagg discovery already runs inside
  producer sub-plans (`local_discovery` is true whenever producer regroups are
  enabled) and the carrier resolves a placeholder-substituted constituent by
  projection position; a failure falls back to a dedicated collapse step, never to
  the preserving treatment and never to a deferral.
- [Filter desugar moves parenthesisation of single-column SQL] → allowed-delta
  re-blessing with executed values pinned unchanged; the three semantic changes are
  enumerated and each pinned by a fail-without-fix test.
- [A model's `Column.sql` references a filtered column and previously read the raw
  value] → intended (pure sugar); documented in `models.md`.
- [Home resolution needs join safety inside elaboration] → lives in `engine/home.py`
  imported by `elaborate.py` (already imports `join_safety`); `elaborate_env` untouched
  by engine imports, so no cycle.

## Migration Plan

Pure planner / renderer change; no stored-artifact migration. Behaviour changes are
enumerated in the proposal and pinned by tests. Rollback = revert the PR. The branch
is stacked on DEV-1900: the PR targets that branch until #398 merges, then main;
integrate forward by merging, never rebasing.
