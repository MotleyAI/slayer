# DEV-1859 design — the row-grain branch of the grain-union rule

## Context

See proposal.md for motivation. Legs A and B landed on the branch in the first
pass; the second pass (2026-09-15, after merging DEV-1892) re-cuts them where
they were band-aids and adds leg C. Substrate facts (verified against the
branch at the DEV-1892 merge, `9c143bf3`):

- `AggregateKey.children()` spans source, args, kwargs AND partition keys; the
  combined-consumer walk descends all of them (skipping only the partition-key
  subtree, DEV-1847 shape B). The leg-A classifier inspected the source only, so
  an attached (aggregate-valued) parameter beside a row-level or mixed source
  was discovered as a COMBINED consumer and escaped its scope at render (probe:
  placeholder-escape ValueError).
- `grain_determines`' AggregateKey arm tests partition-key MEMBERSHIP; for
  `sum(amount, partition_by=customers.regions.name)` against the entity grain
  `{status, customers.id}` it returns False while the key itself is determined.
- `ScopeFrame.resolve` already renders an expression source (arithmetic /
  scalar-call / literal keys via the row-expression renderer with the scope's
  own leaf anchor), resolving attached placeholders, expanding derived columns
  and registering joins; the kwarg pass calls it on every expression source for
  join registration and discards the result. `_render_expression_source_sql`
  rendered the same tree again through a private resolver — where leg A bolted
  on `attached_columns`. Its cross-model guard is dead: `binding.py` rejects the
  shape.
- A literal is a legal aggregation source (`_AGG_SOURCE_KINDS`).
- DEV-1892's association producer compiles with nested discovery off; its
  picked-parameter keys are built before compile.

## Goals / Non-Goals

**Goals:** axiom 6's last leg true by construction on the existing row-attach
machinery, for source constituents AND parameters; the mixed shape structurally
unable to reach the DEV-1847 carrier; one classifier consumers cannot
mis-apply; one renderer for expression sources; the non-shift transform
miscompile closed by a consumer-grain-aware type rule.

**Non-Goals:** any change to pure re-aggregation semantics or goldens;
`first`/`last` over expressions; the shift family's regimes; transform-in-
dimension rules (DEV-1868); `source_queries` (DEV-1878). Deferred to DEV-1903:
one discovery walk, collapsing the producer flags, merging the two transform
checkers, typed first/last dispatch.

## Decisions — first pass (retained)

1. Classify mixed sources before re-aggregation discovery; a mixed root's inner
   aggregates row-attach (the computed-dim mechanism) and the root stays an
   inline expression aggregate; pure roots keep the 1847 path bit-for-bit.
2. Pure-re-aggregation checkers scope to pure roots; modifier legality is
   decided by the operand's grain type, never by construction (axiom 9).
3. Leg B is consumer-grain-aware: reject only row leaves that are not projected
   grain keys; `rank(weight)` with `weight` projected stays legal.
4. No new warnings: the broadcast onto rows is the standard coarser→finer
   coercion; per-row weighting is the shape's meaning.
5. Architecture bundle in this PR: the axiom-6 tag flips to `[enforced: …]`
   (exact one-line diff presented first — normative harness); no `index.yaml`
   change.
6. `first`/`last` over a mixed source dispatch to the aggregation path (the
   existing "not supported over an expression" rule); a pure attached first
   arg keeps the transform routing. Parse-level; DEV-1903 moves it to typing.
7. Computed-dimension leaves under non-shift transforms: expression-equality
   only; the generator's transform-layer readiness checks the whole input key
   against the slot map first.

## Decisions — second pass

8. **One classifier over the aggregation's full input set** (`core/keys.py`).
   `attached_inputs(k)`: the deduped top-level aggregates across source, args
   and kwargs. The SOURCE alone decides the dataset: attached iff the source has
   attached constituents and no row leaf; otherwise row grain — a literal-only
   source included, since `sum(1)` still evaluates per row of the home dataset.
   `is_reaggregation_key` = attached source (pure; nothing to subtract).
   `is_row_attach_root` = not an attached source AND attached inputs present.
   `is_mixed_source_key` is deleted. `attached_operand_keys` (was
   `reaggregation_operand_keys`) = every aggregate nested in the inputs of ANY
   root with attached inputs — the lenient partition-key set bind time needs
   for row-attached constituents. Alternative rejected: extending
   `is_mixed_source_key` to kwargs and the removal block to parameters — three
   overlapping predicates and add-then-remove would remain.
9. **Discovery is opaque below a root's inputs, never below its partition
   keys.** `walk_consumer_keys` yields every key without entering the source,
   args or kwargs of a root that owns attached inputs, and DOES walk its
   partition keys (an attach-carrying computed dimension in `partition_by=`
   still needs the outer attach the grain join is built on — Codex high
   finding). Every root-discovery walk uses it: the combined-consumer walk and
   its order / filter variants, the dimension root walks, the bare-combined
   walk, the local-broadcast scan. A root that is itself a combined consumer is
   recorded and not descended; its sub-plan owns its inputs. The two
   `_discover_*_roots` functions collapse into one walk parameterised by
   predicate.
10. **The regroup block acts on an explicit disposition.** After all
    discoveries, each row-attach root is `inline` or `own-producer` (present in
    any of the four root lists — the sub-plan case where a root at exactly its
    producer's grain is filtered to inline is deliberate: its inputs row-attach
    there). The block row-attaches the attached inputs of `inline` roots only
    (local → row_aggs, cross-model → cm_row) and adds them to the measure
    substitution map; the list-subtraction steps are deleted because nothing
    upstream discovers those inners any more. DEV-1903 item 1 replaces the
    inference with a walk that yields the disposition.
11. **One nested-discovery content predicate.** `_answers_need_nested_regroups`
    = any answer that is a transform or owns attached inputs; used at the
    carrier, the regroup producer, the re-aggregation outer producer, the
    dimension wrap, and the association producer (replacing its hard-coded
    off). Each site's grain-key clause and the `(not windowed)` clause are
    untouched, so no existing nesting decision changes.
12. **Determination is recursive; ungrained parameters type at the query
    grain.** `grain_determines`' AggregateKey arm: each partition key
    determined — a grain member, a to-one-reached column, or an aggregate-valued
    key whose own grain is determined (correct semantics, pinned, not
    accidental); an expression-valued partition key stays conservative (exact
    membership only). At the two `check_parameter_determined` sites an
    ungrained parameter is checked with its grain normalised to the producer's
    projected dimensions — what the carrier already does for constituents — the
    original key staying the pipeline identity. Rejecting the ungrained form
    would refuse a well-typed term (axiom 9).
13. **Attached parameters are direct aggregate keys.** Keyword / positional
    parameters bind to `AggregateKey`; a definition default is Mode-A text and
    cannot carry an attached value until DEV-1901 types defaults — so mapping
    `PickedParam.key` through the compiled producer plan's substitutions (the
    pattern the re-aggregation outer producer already uses for entity keys)
    covers every attached parameter that can exist. The association producer
    turns nested discovery on per decision 11; the level-1 `MAX(<placeholder>)`
    resolves through the scope's attached columns. No generator change: the
    kwarg resolver and the picked-parameter renderer already go through the
    scope.
14. **Default-mode twin hard stop.** The parameter's producer must stay rooted
    at its own home and attach into the target-rooted producer. Probe first at
    test-writing: if the target-rooted sub-plan compiles it through the
    existing path, executed-value tests; if it needs a new kernel arm, pin the
    typed error it raises and post the deferral on a fresh issue. Error mode is
    the mode's own refusal of the unattributable dimension.
15. **Expression sources render once, through the scope.**
    `_render_expression_source_sql(scope=…)` returns `scope.resolve(source)`
    serialised; the private resolver and the dead guard go; `attached_columns`
    leaves all five signatures; `scope` is REQUIRED for expression-source
    rendering — `_composite_agg_builder` and `_filter_agg_builder` gain it, the
    time-shift leaf path passes its `shifted_scope` — no fallback that silently
    loses join registration (Codex). A derived-column expansion divergence, if
    any, follows the golden ledger protocol.
16. **One row-leaf walker for both transform checkers.** `_first_row_leaf(key,
    *, exempt)`: aggregates opaque, a transform descended through its input
    only, exempt keys legal at any node; the shift family passes an empty
    exempt set, the non-shift checker the projected grain keys. Messages
    unchanged (ledger-pinned).
17. **Spec reconciliation with the unarchived DEV-1892 change.** The
    expression-aggregation MODIFIED requirement is the full post-1892 text
    minus both rejections; the ungrained-parameter rule is an ADDED requirement
    (1892's parameter requirement is not in the corpus until PR #394 archives).
    This change archives only after DEV-1892's archive has landed on main.
18. **c4 oracle.** The NULL-region cell is the LEFT-join group with no region
    name: c4's order and the orphan order together; c4's weight is that total.

## Risks / Trade-offs

- [Opacity changes discovery order in a way an existing golden notices] →
  divergence ledger; the `(not windowed)` and grain-key nesting clauses are
  untouched to keep the blast radius to the new shapes.
- [The default-mode twin exposes a re-rooting gap] → decision 14's hard stop.
- [Recursive determination accepts a shape 1892 rejected] → every 1892 residue
  pin stays (the "grained outside the operand" case is still undetermined);
  positive/negative tests per partition-key kind.
- [Scope-rendered expression sources diverge on derived columns crossing a
  join] → SQL assertion: the join emitted once with the allocator's alias.
- [A windowed constituent inside a row-attach root takes the row-phase windowed
  kernel for the first time] → executed test on the DEV-1846 fixtures.
- [Leg-B derived-set drift as transform ops are added] → the rejection matrix
  parametrizes over the derived set itself.

## Migration Plan

Pure feature lift plus one new type rule. Previously erroring mixed sources and
attached parameters on row-level sources now execute; previously miscompiling
non-shift row-leaf transforms now raise the typed error. Golden divergences
follow the ledger protocol. Rollback = revert the PR. Three existing pins
re-point with maintainer consent (recorded in tasks.md).

## Open Questions

None.
