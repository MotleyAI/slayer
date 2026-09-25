## Context

See proposal.md › Why. The discovery walk (`slayer/engine/compile/discovery.py`) already
emits one disposition per (root, phase) occurrence. Three downstream sites disagree with it:

1. `_plan_regroups` groups re-aggregation roots in `_group_reaggregation_roots` with a
   "row wins" precedence → one row-phase attach; a combined consumer then reads the base-FROM
   placeholder (HAVING leak, unresolved ORDER BY hidden slot). Plain / cross-model / transform
   roots go through `_group_routed_roots`, which keeps both phases (one interned producer, one
   placeholder, two attaches).
2. `dimension_transform_roots` (and so `position_classes` and `_Walker.dimension`) test
   `is_grained_aggregate`, which excludes re-aggregations by construction → `rank(R)` in a
   dimension is not a transform root; `R` is row-attached alone and the rank window lands in
   the base SELECT (`MaterialisationStageError`).
3. The "partition key must be a query dimension" rule raises in `bind_inputs`
   (`_validate_partition_keys` via `check_partition_key_resolves`) before filters are typed
   (`combined_partitioned_consumers` is called with no `measure_typed`), and re-aggregation
   outer keys have a second, position-blind copy (`check_reaggregation_partition_key_is_query_dim`
   in `_synthesize_reaggregation_producer`).

A throwaway probe (collect all phases per re-aggregation root + one attach per phase; the
type-based dimension predicate; skipping a covered re-aggregation in `_Walker.dimension`)
fixed shapes 1, 2 and ORDER BY R with correct values and one producer CTE joined twice, and
broke 8 tests — among them `tests/test_dev1903_discovery.py::TestTransformOverReaggregationDimension`
(`Regroup attach join keys do not match the producer's grouping grain`), which D4 must keep green.

Normative harnesses: semantics Axiom 9 (inspect types only), Axiom 13 (positions), engine P4
(interning; gains the D1 clause), P6 (phase/stage), P9 (type errors raise in the checker).

## Goals / Non-Goals

**Goals:** one (root, phase) grouping for every producer root; one type-based classifier
shared by the checker and discovery; one post-typing partition-key rule; collision-free
synthesized names.

**Non-Goals:** error-message key rendering (DEV-1973); the specified row-scope semantics of a
field-typed filter on a computed dimension's own aggregate (`queries/positions`) is unchanged.

## Decisions

**D1 — one grouping, per-occurrence metadata.** Delete `_group_reaggregation_roots` and
`_ReaggregationRoots`; re-aggregation becomes a routing of `_group_routed_roots` with row and
combined buckets. Each (root, phase) gets its own `RegroupAttachPlan`; both share one
placeholder (`RegroupPlaceholderRegistry.placeholder_for(root)`) and one producer via
`producer_registry` interning. Public alias and declared type are carried per occurrence
(each attach takes its own occurrence's metadata; a row-only root keeps its dimension's) — no
phase precedence. Invariant (asserted by a plan-level test): at each consumer phase the
placeholder resolves to exactly one attach — dimensions read the ROW attach; measure, filter
and order slots read the COMBINED attach, never the ROW one.
*Alternative rejected:* a set of phases inside `_group_reaggregation_roots` — keeps two
grouping paths that must agree by hand.

**D2 — type-based classification and subtree ownership.** One predicate — "explicitly grained
aggregate" = any `AggregateKey` with `partition_keys` (re-aggregations included) — feeds
`dimension_transform_roots` / `position_classes` (checker) and `_Walker.dimension`
(discovery). A dimension transform root owns everything opaque beneath it: a covered
re-aggregation and its constituents are synthesized only inside the transform's producer,
never emitted as independent dispositions (asserted by a discovery test listing the
dispositions of a `rank(R)` dimension). Grounding: Axiom 9.

**D3 — one partition-key rule, post-typing, in the checker.** `bind_inputs` keeps resolution
and rewrite (time-dimension source column → bucket), the ambiguity error and attributability.
"Must be a query dimension" becomes a checker pass that runs on the rebuilt prebound and the
aligned typings returned by `type_and_split_filters` (so split AND-conjuncts are judged
individually), over consumer positions: combined consumers only (measures, measure-typed
filter conjuncts and order targets, composites / transforms in those positions), plain,
cross-model and re-aggregation outer keys alike. `check_reaggregation_partition_key_is_query_dim`
and its compiler call are deleted; the error location names the real consumer. Grounding:
engine P9, live `queries/partitioned-aggregates` › Combined-consumer partition keys.
*Alternative rejected:* typing filters inside `bind_inputs` — duplicates the split/typing pass
and breaks the `prebound=` entry path.

**D4 — row-phase re-aggregation synthesis at the declared grain.** A row-phase
re-aggregation attach is synthesized with its own declared outer grain as the projected
context (the DEV-1928 constituent path in `_plan_regroups`), whether or not that grain lies
within the query dimensions. Three grains are kept distinct: the semantic result grain (the
declared `partition_by=`), the producer's projected unique grain, and the consumer-side join
coordinates; the latter two MUST be equal (plus the active time bucket for a windowed /
time-ordered consumer) — `_assert_attach_covers_producer_grain` stays the backstop.

**D5 — injective synthesized grain names.** `_regroup_grain_name` falls back to the literal
`"grain"` for any non-column key, so two expression-valued grain keys in one producer collide
(`DuplicateMeasureNameError … 'grain'`). New rule: column-like keys keep their path/leaf
spelling; any other key uses its declared dimension name when the producer carries one
(`grain_name_by_key`), else a deterministic key-derived identifier — never encounter order
(goldens and interning stay stable). Synthesized-producer assembly asserts unique names before
elaboration. Implementation confirms the collision site first.

**D6 — order targets.** An order target structurally equal to a dimension-borne transform or
re-aggregation gets its combined attach through D1; no hidden slot is left unresolved at plan
time. Ordering by the dimension's name keeps sorting by its banded output.

## Risks / Trade-offs

- [Existing SQL goldens change where a measure read a grouped row placeholder] → re-bless;
  values must be unchanged (value tests stay green).
- [D4 changes the synthesis context of dimension re-aggregations already green today] → the
  dev1847 / 1903 / 1928 / 1942 suites are the regression net; test-logic edits need user
  consent.
- [NULL rank position differs between dialects] → rank values asserted per dialect; a
  divergence is a stop-and-ask, not a silent oracle change.
