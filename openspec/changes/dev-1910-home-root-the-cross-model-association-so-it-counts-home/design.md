## Context

See proposal.md › Why. Today `_synthesize_cross_model_producer` (target-rooted: root at
the home, compute at the fan-out-safe grain, broadcast the rest) hands associate-mode
resolution to `_synthesize_association_producer`, which roots at the HOST, LEFT JOINs to the
home, dedups per home entity with the two-level `association` kernel, and copies the filter
disposition, prebound, compile and attach code. Its host rooting is exact only when every home
entity has a population row. Naively re-rooting that function at the home recursed: the
rerooted aggregate (source path `()`) is re-classified by `_local_broadcasts` /
`crossing_local_root_predicate` as a local aggregate over an unattributable dimension and
re-enters producer synthesis. Spikes on the current branch established: a customers-rooted
association body renders a fanning reverse-hop grain (`orders.status`) and a nested
customers-side attached parameter; the same body with a host-side attached parameter
(`sum(orders.amount, partition_by=regions.name)`) is re-routed to a host-rooted producer whose
input safety refuses `orders.amount`; the local association with a same-branch filter is
decoupled today (390 vs the orders-rooted oracle 140); `walk` resolves a two-hop reverse path.
The reverse hop from the home to the host is a single token (`_back_token`), which cannot
express a two-hop home.

## Goals / Non-Goals

**Goals:** one producer synthesis with two arms; association rooted at the home; the
approved semantics (home-path association, back-hop presence, same-row coupling); no new
error vocabulary; DEV-1841/1847/1859/1892/1900 suites green with values unchanged wherever
every home entity has a population row.

**Non-Goals:** the broadcast twin of the attached-parameter shape (DEV-1906, its strict xfail
keeps xfailing); population row filters across a fanning hop on the host base query
(DEV-1909); the customers-rooted local root with an attached parameter reading orders columns
(the host-root twin of DEV-1906, refused by the crossing-local-root routing — recorded on
DEV-1906); ranked / windowed association (DEV-1914).

## Decisions

1. **One synthesis, two arms, one tail.** `_synthesize_cross_model_producer` keeps its
   prologue (home path, root, alias, requested grain, `safe_pairs` vs `unattributable`);
   `mode == "associate" and unattributable` selects the association arm, else the plain arm;
   both feed one shared tail (filter disposition, `_regroup_producer_prebound` at the root,
   `compile_prebound(disable_host_rooted_isolation=True, enable_producer_regroups=…)`,
   attached-parameter substitution, answer slot, `join_pairs` via `host_by_rerooted`,
   attach-cover assert, `RegroupAttachPlan`). `_synthesize_association_producer` is deleted.
   Alternative — keep two functions and mirror the safe-pairs mapping (the issue's wording):
   rejected, it preserves the duplication the bug came from.
2. **Association arm.** Eligibility (`check_association_windowed_ranked`,
   `check_association_root_unique_key`); input safety with attached inputs stripped (as today);
   parameter typing against the entity grain in HOST coordinates
   (`ColumnKey(path=target_path, leaf=col)`, unchanged) and a second, root-coordinate entity
   key list (`ColumnKey(path=(), leaf=col)`) for the kernel — the two are never conflated;
   picked parameters rerooted via `reroot_from_root`, `anchor_path` = source path relative to
   the home; `agg_rooted = reroot_from_root(agg).model_copy(update={"locus": "host"})` so the
   level-1 body compiles it inline at its full (fanning) grain; every unattributable dimension
   rerooted into the producer grain (`assoc_pairs`), joined back on the host key exactly like
   `safe_pairs`; kernel `AssociationProducerKernel(entity_keys, picked_params, present_keys)`.
   `producer_root_model` = the home; `associated_measure` / `associated_dimensions` only for an
   implicit grain, as today.
3. **Presence guard on the back hop only.** `present_keys` = the host-side join columns of the
   last hop of the reverse path (`ColumnKey(path=back_path, leaf=col)`), populated iff at least
   one association dimension's rerooted key has a leaf under the back path; rendered in level 1
   as `NOT (<col> IS NULL)` for EVERY listed column (all-components rule — a composite equality
   join matches only when every column matches). Not applied to forward paths from the home
   (a region with no events keeps its NULL `bad_pop`, which the population computes
   identically) and not when home == host (the population's own LEFT JOIN puts the orderless
   customer in the NULL cell; `test_order_context` pins that row). Codex proposed a witness for
   every fanning association path; rejected — the asymmetry exists only where the population
   never has the row.
4. **Reachable conjuncts inline; entry kept.** For each ROW conjunct the arm calls
   `_conjunct_disposition` with root = home: inline → keep; pushed (the push plan validated
   eligibility) → inline `reroot_from_root(cj)` as a bound filter instead of a `SemiJoinFilter`;
   dropped → drop + warn. `semi_join_filters` is empty for association producers, so the
   conjunct texts ride `RegroupAttachPlan.association_restricted_filter_texts` and
   `_attach_semi_join_texts` yields them too — the response entry (`kind: "semi_join_pushed"`,
   the public contract) is unchanged. Rationale: the dedup makes an inline fanning join
   observationally identical to EXISTS on every branch, and only inline gives the same-row
   coupling with the dimension the dice–slice law and the existing 140 oracle require.
   Alternative — inline only conjuncts sharing a first hop with an association dimension,
   semi-join the rest: rejected, two rules for one observable semantics.
5. **Reverse path.** `_back_token` becomes `_back_path(root_model, host_name, target_path,
   models_by_name) -> Tuple[str, ...]`: walk the target path forward from the host
   (`models_by_name` is host-inclusive since DEV-1900); per hop the reverse token is the edge
   name when declared (direction-agnostic, the DEV-1853 disambiguator) else the hop's source
   model name; reversed; fallback `(host_name,)` when the forward walk fails (today's
   behaviour). `attributable_from_root`, `_reroot_leaf_via_host` and `broadcast_reason` use
   `(*back, *hp)`. Ambiguity on a reverse hop raises the ambiguous-hop error at walk time, as
   the spec requires in every mode.
6. **Host-locus wraps are never re-routed.** `crossing_local_root_predicate` excludes
   `k.locus == "host"` (as `_local_broadcasts` already does): the level-1 aggregate is inline by
   construction, so the crossing closure of its attached parameter no longer drags it onto a
   host-rooted producer; the parameter, cross-model from the home, synthesizes its own
   target-rooted producer and row-attaches on the rerooted partition keys (the shape spike S1e
   rendered for a home-side parameter).
7. **No new raise sites.** A dimension the home cannot reach at all can only arise from an
   ambiguous reverse hop, which already raises; no ledger row, no guard-ratchet change.
8. **Nested regroup discovery unified.** `enable_producer_regroups` = windowed bucket, or an
   attach-owning answer, or a computed / local-partitioned grain key — for both arms (the
   association arm previously omitted the grain-key clause).

## Risks / Trade-offs

- [The DEV-1859 associate headline (attached host-side parameter) depends on decision 6 and the
  cross-model nested attach rendering inside the level-1 body] → the spec-tests stage pins its
  plan shape (nested producer rooted at orders inside the customers-rooted body); the existing
  exec tests are the oracle.
- [Reverse-path generalization touches every producer mode] → unit tests for exact rerooted
  paths / reasons (unnamed two-hop, named edge, mixed, ambiguity); every non-association golden
  stays byte-identical.
- [SQL shape of every association producer changes] → goldens re-blessed through the
  ALLOWED_DELTAS manifest; executed values are the oracle.
- [Local association with a same-branch filter changes from 390 to 140] → approved; pinned by
  a new executed test, no existing test pins 390.
- [ClickHouse semi-join version gate] → association producers no longer emit EXISTS, so the
  gate simply does not trigger for them; other producers unchanged.

## Migration Plan

No data or API migration. Goldens re-blessed for association cases only.
