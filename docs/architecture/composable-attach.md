# Composable attach (the regroup primitive)

A `partition_by=` aggregate is computed at a grain finer or coarser than the
query, then attached back to the query rows. SLayer compiles every such shape
through one mechanism — the **regroup primitive** — rather than a family of
special cases:

1. **Producer** — a synthesized sub-plan that computes the aggregate at its own
   declared grain (a `PlannedQuery`, so it can itself carry rolling windows,
   rankings, or transform steps).
2. **Attach** — the producer renders as a CTE and LEFT JOINs back onto the query
   on the producer's complete grain, null-safe (`IS NOT DISTINCT FROM`). A
   keyless producer (`partition_by=[]`) attaches as a single-row CROSS JOIN.
3. **Substitution** — each consumed partitioned aggregate is replaced, by
   structural key identity (never text), with a reserved-leaf placeholder
   (`__regroup__<n>__<seed>`) that resolves to the producer's output column at
   render time.

This generalizes the DEV-1739 partitioned-aggregate join-back onto a recursive
node: a producer is a full plan, so the three steps compose across the supported
local shapes (windowed, first/last, transform-nested, filtered). A *nested*
attach — an aggregate over an attached value, or `partition_by=` on a computed
dimension — is not yet expressible and fails closed (DEV-1824).

## Two attach phases

- **Row attach** — a partitioned aggregate inside a *computed dimension* joins
  into `_base` **before** aggregation, so the query can group by a value derived
  from it (banding, ranking partitions). The placeholder resolves through the
  base scope's `regroup_env`.
- **Combined attach** — a partitioned *measure*, composite, order target, or
  filter reference joins at the *combined SELECT* **after** aggregation — the
  position the DEV-1739 `CrossModelAggregatePlan` occupied. The placeholder
  resolves to the `_cm_` producer column.

Both phases coexist in one query, in one flat `WITH` chain.

## Context grain (measure ⇔ dimension symmetry)

Any measure-legal expression is legal as a computed dimension provided it is
**grain-self-contained**: every aggregate carries an explicit `partition_by=`
(cross-model sources are legal since stage 3 — they compile through a
target-rooted producer, below), and every transform wraps such a grained
aggregate. A transform then evaluates at the grain of its *containing context*:

- As a **measure**, `rank(revenue:sum(partition_by=region))` ranks the result
  rows at the query grain (over the attached, broadcast region total).
- As a **dimension**, the same expression ranks the *regions* — the transform is
  compiled *inside the producer*, at the producer grain, because the query would
  otherwise have to group by a value that depends on its own grouping.

## Grain-union broadcasting

Within an expression tree, **every node's output grain is the union of its
children's grains**, and each combination point broadcasts every operand to that
union: an aggregate contributes its own `partition_by` set (`partition_by=[]` is
the empty grain), a transform the union of its input's grains, and
arithmetic/scalar/CASE the union of its operands'. Combining aggregates at
different grains is therefore never, by itself, an error — the union is the one
grain at which every operand is defined without further aggregation. Consuming at
a finer grain broadcasts further (as a measure, to the query grain); a coarser
consumer stays illegal.

The producer for a mixed-grain transform-in-dimension is synthesized at that
union grain **recursively**: aggregates *at* the union grain compile inline (a
plain grouped aggregate), while each strict-subset operand — a coarser aggregate,
or a nested transform at its own grain (e.g. an inner `cumsum` over months within
a region) — becomes a nested **combined attach** inside the producer, its own
producer built by the same rule. Discovery inside a producer excludes roots at
exactly the producer's grain, so grains strictly decrease and the recursion is
bounded. The producer's internal `WITH` (the nested attaches plus its own
transform steps) hoists into the one flat chain (see [The CTE-hoist](#the-cte-hoist)).

One case still fails closed: a time-ordered transform (`cumsum`, `lag`, …) whose
evaluation grain lacks its time-ordering key — which would duplicate result rows,
so the author is told to include the time key in `partition_by`. (A mixed-grain
transform whose inner aggregate is windowed or `first`/`last` no longer fails
here — DEV-1835 lifts it onto the union grain, folding in the synthesized time
bucket.)

## Producer synthesis

- **Windowed** (`window=` + `partition_by=`) — the producer synthesizes the
  consumer's active time dimension as a real `TimeTruncKey` in its grain (its
  declared main time key), evaluates the rolling window per bucket, and includes
  the time bucket verbatim in the attach keys.
- **Ranked** (`first` / `last` + `partition_by=`) — collapses to a single ranked
  CTE ordering on the model's resolved ranking time column; a temporal partition
  key never hijacks the ordering.
- **Transform-at-producer-grain** (a transform in a dimension) — the transform
  is a producer measure, rendered as a step CTE over the grained aggregate.

## Target-rooted producers (stage 3)

A **cross-model aggregate** — one whose source names a joined model
(`customers.revenue:sum` from `orders`) — compiles through the same primitive,
with the producer's `FROM` **rooted at the aggregate's own model** (the join
target), not the consumer's. Rooting at the target is what makes the value
join-fan-proof: the aggregate runs over the target's rows once, never through a
1:N host join that would multiply them.

- **Safe grain vs broadcast.** The producer computes at the subset of the
  requested grain **attributable from its root** — a grain key is attributable
  when its path re-roots onto the target's own join graph over hops that are
  *provably to-one* (a solo/complete primary key on the far side, a declared
  `one_to_one`/`many_to_one` cardinality, or a mirrored reverse edge). Every
  other requested dimension is **broadcast**: the value repeats across it, and
  the response carries a `broadcast` warning (fields: `measure`, `location` —
  the pipeline stage, and `dimensions`, each with its `reason`). An *explicit*
  `partition_by=` key that is unattributable is a hard error instead (the
  author asked for a grain the engine cannot prove safe).
- **Filter inheritance.** Each ROW-phase conjunct of the query's filters whose
  references are all attributable from the root inherits into the producer
  (re-rooted to target coordinates); an unreachable/unsafe conjunct is excluded
  from that producer and warned (`unreachable_filter_dropped` — the host base still applies
  it to local measures). An AGGREGATE-phase predicate over the cross-model value
  applies at the outer SELECT's WHERE on the attached column, restricting rows
  uniformly with local aggregate filters.
- **Unsafe inputs are hard errors.** A source expression, aggregation argument,
  ranking key, or `Column.filter` that crosses an unproven hop from the root
  raises with the offending hop and the remedy (declare join cardinality or a
  covering unique key) — never a silently fanned value.
- **Strict mode.** `"strict": true` on the query turns every implicit broadcast
  and dropped producer filter into an error, for callers that need exactness or
  nothing.

Inside the producer the aggregate keeps its **canonical alias** in target
coordinates (`"customers.revenue_sum"`); the consumer's public name lands on the
outer projection through the placeholder substitution, and the final result keys
are unchanged (`"orders.customers.revenue_sum"`).

## Step-layer grain rule (stage 1a)

By the time the transform-chain step CTEs render, a computed dimension is an
ordinary dimension slot. One shared rule defines every transform's auto-grain —
the window-family `PARTITION BY`, the `time_shift` shifted-CTE re-aggregation
grain, and the `consecutive_periods` grouping: **every projected dimension
slot** (plain and derived columns, computed dimensions, bare row-attach
placeholder dimensions), **excluding time buckets** (the transform's ordering
axis) **and combined-attach placeholder slots** (an attached measure value must
never widen a grain). Placeholder roles are read structurally from the attach
plans, never from leaf text. The shifted CTE re-aggregates the source with the
row producers joined in, so a computed dimension in its grain — and a
row-lowered predicate over it — resolves to the producer column, keeping the
shifted row population at parity with `base`.

## Filter placement

A filter's top-level `AND` conjuncts route **independently**, decided
pre-substitution (substitution lowers a predicate's phase to ROW, so placement
cannot be re-derived afterward). Each conjunct resolves to the earliest scope
where **all** its operands are available:

- a raw non-dimension column → base row (WHERE) only;
- a plain aggregate → base-grouped (HAVING) or combined;
- a partitioned-aggregate placeholder → combined (outer WHERE) only;
- a dimension → base row or combined.

The conjunct routes to the earliest scope in the intersection; a predicate whose
operands share no scope (e.g. a partitioned aggregate OR-ed with a raw column)
raises a clear "split the filter" error rather than an internal one.

## The CTE-hoist

A producer that itself needs intermediate relations (a rolling window, a ranking,
transform steps) renders its own internal `WITH`. To keep one flat chain, that
`WITH` is **hoisted**: nested renders share the parent's `AliasAllocator`, so
`_cm_` / `step` names are globally unique by construction; the
literal `_base` / `base` names are reserved in the allocator and renamed
per-producer. Several complex producers therefore coexist in one query with no
name collisions on any dialect.

## Invariants

- **Cardinality neutrality** — attaching a partitioned aggregate never changes
  the row count or any other column's value. The join is on the producer's
  complete grain (or provably single-row when keyless).
- **No placeholder leakage** — reserved `__regroup__` prefixes never appear in
  public schemas, response metadata, or emitted SQL; a real column using the
  prefix is rejected at plan time while a regroup is active.
- **Fail closed** — an unrouted shape (an aggregate over an attached value, a
  no-common-scope filter, an unsafe explicit partition key) raises a clear
  error, never wrong numbers; the post-discovery total-routing invariant (D7)
  catches any cross-model or partitioned leaf discovery failed to dispose.

## Roadmap

This is **stage 1** of unifying SLayer's isolation families onto one mechanism.
The end state is a closure axiom: **any grain-legal dimension composes with any
legal measure**. Every fail-closed coexistence guard is a temporary migration
artifact with an issue attached, never a permanent boundary.

- **Stage 1 (DEV-1824)** — the full local `partition_by` surface on the
  generalized primitive: windowed / first-last / transform-nested / filtered
  aggregates, row+combined coexistence, the measure ⇔ dimension symmetry, and the
  CTE-hoist.
- **Stage 1a (DEV-1837)** — computed dimensions coexist with
  transform-chain measures (`time_shift`, `change`, `cumsum`,
  rank-of-a-measure) in both render chains, under the shared step-layer grain
  rule above. Transform measures are *steps over the query-grain result*, not
  attaches, so no family migration dissolves this pair; it lifts directly.
  Also: the per-conjunct filter `AND` split generalizes to row-attach
  references, and a partitioned measure combined with a temporal transform no
  longer leaks its placeholder into the shifted CTE. The dimension-family ×
  measure-family compatibility matrix becomes the migration's tracked
  definition of done.
- **Stage 1b (DEV-1839, this change)** — grain-union broadcasting: a
  different-grain arithmetic nested inside a transform-in-dimension unions the
  grains and broadcasts recursively (strict-subset operands become nested
  combined attaches inside the union-grain producer); bare composite arithmetic
  over two-plus partitioned aggregates lifts as a measure. Windowed/`first`-`last`
  mixed grains defer to stage 2 (lifted there); a time-ordered transform whose
  grain lacks its time axis fails closed instead of duplicating rows.
- **Stage 2 (DEV-1835, this change)** — migrate the local `_wm_` (windowed) and
  `_rk_` (ranked) renderer arms onto the primitive and delete them, unifying every
  local family under `_cm_` producer naming; a general cross-phase attach-dedup
  pass subsumes duplicate producers (a computed-dim grain key functionally
  determined by the rest of the grain is pruned, so it needs no twin producer);
  the DEV-1504 G4–G7 / windowed-ranked coexistence / `time_shift`-over-ranked
  guards dissolve; and the stage-1b windowed/`first`-`last` mixed-grain union
  broadcast lifts — a windowed inner contributes the query's synthesized time
  bucket, `first`/`last` is timeless.
- **Stage 3 (DEV-1836, this change)** — migrate the cross-model `_cm_` family
  (plain, re-rooted, ranked, windowed, partitioned) onto target-rooted
  producers; cross-model sources become legal in the composed shapes, in
  dimension expressions, and in `window=`; intermediate-hop and
  band×cross-model guards fall (broadcast instead); the D7 total-routing
  invariant lands. `classify_isolation` survives only for the host-rooted
  routes (crossing `Column.filter` inputs, host-grain wraps, filtered-local) —
  its retirement moves to stage 4.
- **Stage 4 (DEV-1838)** — node discipline: the query renders as a chain of
  nodes (base → aggregate → combined → steps → post), each consuming only the
  previous node's schema; the CTE-body deferrals lift via the CTE-hoist;
  `classify_isolation` + the legacy cross-model dispatch retire; exit
  criterion — the guard list is empty and the matrix has no xfails.
