## Context

Every ranked `first` / `last` renders through a producer kernel (`RankedProducerKernel`
or `TrailingWindowProducerKernel`), whose ordering key comes from one resolver
(`resolve_ranking_time_key`). Its default candidates are read from three places, each
the producer's own grain:

- local regroup, shifted and windowed producers: the producer plan's row slots
  (`_producer_row_keys` / `_ranking_row_keys`);
- the cross-model producer: `requested` — the explicit `partition_by=` keys when given,
  else the query dimensions — filtered to its attributable members
  (`ranking_candidates`), resolved once in a pre-check and again in the kernel.

So `partition_by=`, `window=` and nesting decide which query dimensions the default
can see (proposal.md › Why; probes on `main` reproduce US 40, the no-default failure,
the cross-model failure, the window-axis key and the nested US 35).

Principles that apply: semantics Axiom 2.4 (the ordering key is determined by the home
and never widens it — judged by closure, unchanged), Axiom 2.5's intent (an enclosing
aggregation's spelling never changes the node), Axiom 2.8 (fail closed); engine P10
(dependencies are closures) and P4 (interning — the kernel is already part of
`regroup_producer_identity`, so equal producers with different keys never merge). No
arc42 edit.

## Goals / Non-Goals

**Goals:**
- One source of default-key candidates for every ranked producer: the host query's
  dimensions, carried into every producer in that producer's coordinates.
- Delete the producer-grain candidate plumbing.

**Non-Goals:**
- Changing the precedence, the bucket exclusion, the resolution error or the
  ranked-input safety errors.
- Merging producers whose explicit and defaulted keys coincide.

## Decisions

1. **The query's dimensions are one planning input, threaded like `mode_policy`.**
   `PreboundQuery` carries the ordered ranking candidates — the query's dimension keys
   then time-dimension keys, in projection order, every declared one (not the
   functionally pruned grain). Binding sets them from the query; every synthesized
   producer prebound (`_regroup_producer_prebound`, required argument) receives them
   in its own coordinates; `_rewrite_regrouped_prebound` copies them. Kernels resolve
   from the producer prebound's candidates only. Rejected: passing host row keys into
   each kernel site — three sites agreeing by hand is the shape that produced the bug.
   (default)
2. **Nested producers read the user query's dimensions**, inherited through the
   enclosing producer's prebound, never the enclosing aggregation's grain: otherwise
   the outer `partition_by=` spelling changes the inner value. (default)
3. **Every ranked producer**: plain, `partition_by=`, `window=` (the window axis no
   longer outranks a temporal query dimension, as `trailing-window` already specifies)
   and `time_shift`. (default)
4. **Local home: every candidate**; a chosen key crossing an unproven or fanning hop
   fails closed through the existing local ranked-input check — the unpartitioned
   behaviour, so the spelling no longer matters. (default)
5. **Target-rooted home: re-root, skip the undetermined.** Each candidate is
   home-routed and re-rooted into the home's coordinates; a candidate the home does
   not determine — judged by the one predicate that decides grain membership
   (shared-key re-root, else `grain_member_attributable`), factored out of the
   safe-grain loop and shared — is skipped, order preserved; then the home's
   `default_time_dimension`. The pre-check keeps running ahead of the grain and axis
   decisions, reading the same re-rooted list. (default)
6. **All declared dimensions, not the pruned grain**: the key must not depend on
   whether a redundant dimension is listed. (default)
7. **Release note**: one entry in `docs/upgrading/2.0.md` beside the other `first` /
   `last` entries. (default)
8. **Spec home**: the existing `aggregations/functional-form` and
   `queries/cross-model-aggregates` capabilities. (default)
9. **Codex coverage folded in** (codex): a cross-model test skipping an earlier
   undetermined candidate in favour of a later one; a cross-model computed candidate;
   a two-stage query whose stages have different temporal dimensions; an executed
   `time_shift` test; candidate-order tests (a non-temporal dimension before a
   temporal computed one; a bucket skipped for its source).
10. **A column read only by a filter is no candidate**: the precedence lists the query's
    dimensions, never its filters. (user)

## Risks / Trade-offs

- [A `PreboundQuery` construction site forgets the candidates and silently falls back]
  → the producer builder takes them as a required argument; the stage and nested
  scenarios fail if any copy is lost.
- [Re-rooting a computed candidate into the home misses a routed leaf] → the
  cross-model computed scenario and the shared attributability predicate.
- [Result change for existing partitioned / windowed / nested / cross-model queries]
  → documented in the 2.0 upgrade guide; an explicit ranking key keeps the old value.
