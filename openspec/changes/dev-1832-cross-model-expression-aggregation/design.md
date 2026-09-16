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
   `compile_synthesized(prebound, *, query, bundle, scope, …)` = `elaborate_query(prebound=…)`
   + `compile_query`; the six `compile_prebound` call sites in `compile/stages.py`
   (wrap attach, cross-model, association, re-aggregation outer, carrier, local
   partitioned) use it; `compile_prebound` requires `env` and its local-typing branch
   is deleted. Sub-plans resolve homes relative to their own root. Alternative —
   thread a home resolver into recursive contexts — rejected: two typing doors.
4. **Transform constituents through existing producers.** The classifiers in Context
   bullet 5 treat `AggregateKey | TransformKey` as constituents, stopping at either;
   `binding._source_is_reaggregation` / `syntax._is_mixed_agg_source` count a
   `TransformCall` as attached. `constituent_grain(c, …)` = `_effective_root_grain`
   (bucket-aware) for both kinds (Codex F3); the carrier's union grain, prebound
   `main_time_key` / `window_td_key`, join pairs and degeneracy test use it. A transform
   constituent is evaluated by the transform-root regroup producer (computed-dimension
   path) and consumed by the carrier (pure) or the row attach (mixed) unchanged. The
   row-leaf checker (`_first_row_leaf`) and `check_dimension_temporal_axis` also walk
   transforms inside aggregation sources; the axis message drops "inside a computed
   dimension" (ledger row and golden raises updated). **Probe-first hard stop:** if the
   carrier cannot express a windowed-inner constituent's bucket grain, the test pins
   the typed error and the shape is deferred to a fresh issue with a detailed comment.
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
   (home test) and axiom 6 (transform-constituent test); the exact diff is presented
   for approval at implement time. No `index.yaml` change: `models/` is already a
   cross-cutting spec group.

## Risks / Trade-offs

- [Sweep misses a source-path read] → the AST guard turns red on any remaining read;
  enumeration is task 1.
- [Elaborating every sub-plan changes typing of a nested producer] → sub-plans were
  typed by the same `type_and_split_filters` with the same flags; `elaborate_query`
  reproduces that call; goldens byte-identical or the divergence ledger stops the work.
- [Windowed inner under a transform constituent has no carrier arm] → D4 probe-first
  hard stop, typed error pinned, deferral documented on a fresh issue.
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
