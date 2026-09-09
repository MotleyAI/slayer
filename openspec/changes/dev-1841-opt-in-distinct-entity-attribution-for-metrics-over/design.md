# DEV-1841 design — mode axis + association producer

## Context

See proposal.md for motivation. Substrate facts that shape the design:

- Attribution classification and the broadcast split live in
  `stage_planner._synthesize_cross_model_producer`; `strict` is enforced post-planning
  in `query_engine._raise_on_strict_events` off collected warning payloads.
- Local aggregates never enter that machinery: sliced by a fanning dimension they emit
  a naive SUM over the fanned join (verified: duplicate same-status orders double a
  customer's spend, silently). The dev-1853 pin of that shape held only on
  duplicate-free data.
- Producers are typed `ProducerKernel` variants (`plain` / `ranked` /
  `trailing-window`) attached via `RegroupAttachPlan`; filter routing into producers
  (inline / semi-join EXISTS / drop+warn) is `_cross_model_inherited_filters` +
  `_conjunct_disposition`, with the correlation path from `core/join_walker.py`.
- Nested producers are planned recursively through `StrictQueryCarrier`, which rejects
  undeclared query fields.
- The guard ratchet (`tests/test_law_guard_ratchet.py`) pins `NotImplementedError`
  deferral sites to `guards.baseline: 8`, only ever lowered.

## Goals / Non-Goals

**Goals:** one classification path for every (aggregate, unattributable dimension)
pair; broadcast defaults byte-identical for cross-model shapes; association exact by
construction (dedup by entity key, no arity metadata).

**Non-Goals:** per-aggregate mode override (dropped by decision — a later addition
would reuse the query field's name and values as an aggregation kwarg); associate
support for `window=`/`first`/`last` (typed error now); population inference changes
(DEV-1866); the dimension-free cross-model total's population (unchanged).

## Decisions

1. **Mode as a field, threaded typed.** `to_many_handling` on `SlayerQuery`
   (`Literal`, default `"broadcast"`), copied onto `PreboundQuery` and propagated
   explicitly through `StrictQueryCarrier` and every recursive `plan_query` call so
   nested producers resolve under the same mode. No context variables (engine P3).
   The `error` gate replaces `_raise_on_strict_events`, keyed off the mode instead of
   the retired bool.
2. **Association is a new kernel, host-rooted.** `ProducerKernel` gains an
   `association` variant carrying the root's unique-key columns and the level-2
   aggregate spec. Level 1 groups by (grain × entity key) and retains one picked
   column per aggregate input expression (`MAX` — constant per entity because every
   input is root-determined under the unchanged unsafe-inputs rule); `*:count` retains
   no value column (level 2 `COUNT(*)` counts entities). Level 2 is an ordinary
   aggregate over the level-1 rows grouped by grain — which is why the whole plain
   scalar family (percentile included) costs one kernel. Emission is plain nested
   GROUP BY inside the existing flat-WITH pipeline (sql P10/P11) — no correlated
   subqueries, so no ClickHouse version gate; portability claims are about this
   producer shape, backed by golden coverage on every dialect emission path.
3. **Local aggregates join the same classification.** The disposition machinery
   (single-disposition requirement) classifies every aggregate reference — local and
   cross-model, in every consumer role — against the query grain from its root. A
   local aggregate with unattributable grain dimensions routes exactly like a
   cross-model one (its path is empty; its root is the host): safe-grain producer +
   broadcast, association kernel, or error. Alternative considered: keep locals on the
   naive inline path and special-case a dedup — rejected; it would preserve two code
   paths and the axiom-4 hole in the non-measure roles.
4. **Filter routing is reused verbatim, root = host.** The association producer
   inherits the standard producer filter routing (inline iff attributable from host;
   EXISTS semi-join iff unsafe-but-reachable; drop+warn else). Alternative considered
   and rejected after review: inlining pushed predicates as plain WHERE via joined
   branches — not equivalence-preserving for NULL-sensitive predicates and sibling
   fan-out branches.
5. **Typed errors, not deferrals.** The unsupported associate combinations
   (`window=`/`first`/`last`, missing unique key) raise `SlayerError` subclasses with
   remedies — specified behavior, outside the `NotImplementedError` ratchet; baseline
   stays 8.
6. **Warnings.** Broadcast payload gains an unconditional hint field; new `associated`
   payload kind (surfaced twice, suppressed for explicit `partition_by=`); new
   response-only informational kind for semi-join-pushed conjuncts (every mode, no
   Python warning, never an error); broadcast reason distinguishes fanning from
   unreachable. All in the existing discriminated union (core P5).
7. **Strict retirement.** Field removed (`extra="forbid"` already rejects strays); a
   `model_validator(mode="before")` intercepts `strict` specifically to name the
   replacement. Storage migration `SlayerQuery` 3→4 rewrites stored payloads. REST
   `QueryRequest` and the MCP `query` tool swap the parameter — the only mirrors.
8. **Architecture bundle in this PR.** Axiom 8 reworded (override clause dropped) and
   flipped to enforced test ids; law 5 flipped to the dice–slice harness law test;
   `index.yaml` `queries.touches` gains `storage`.

## Risks / Trade-offs

- [Local-shape flip breaks consumers relying on multiplied values] → divergence-ledger
  entry per affected golden/test; the dev-1853 pinned test is rewritten on
  duplicate-carrying data asserting both modes (approved); values were wrong, not
  merely different.
- [Association producer cost: dedup over host×entity pairs] → plain GROUP BY the
  optimizer handles; no worse than the fanned join it replaces; documented as opt-in.
- [Cells non-additive under associate confuses sums over cells] → the `associated`
  warning names the overlapping dimensions explicitly.
- [Mode not reaching a nested planning site silently defaults to broadcast] →
  propagation is explicit and a nested-context test matrix (computed dimensions,
  composites, filters, order) pins each site.

## Migration Plan

Storage migration is automatic on load (v3→v4). API callers sending `strict` get a
typed error naming the replacement — no silent window. Rollback = revert the PR;
stored v4 queries load under v3 code only via the standard forward-version tolerance.
