# Design

## Root cause

`assert_partition_key_attributable` (`slayer/engine/join_safety.py`) opens with:

```python
hp = key_host_path(pk)
if not hp:
    return  # a local column — no join to cross
```

`key_host_path` reads the key's **own** `.path`. For a derived host column such as
`bad_pop = pop + region_events.value`, that path is empty, so the guard concludes "local,
nothing to cross" and returns — even though the fanning `regions → region_events` hop is
hidden inside the column's definition. The multiplying window partition is then emitted
silently. The guard judges spelling, not the DEV-1900 **dependency closure** (a
reference's own path plus every path its derived definition crosses, recursively —
engine.arc42 principle 10).

## Decision: judge the path-less key's closure from the HOST, mode-invariantly

Replace the blanket early-return with a closure-attributability check rooted at the
**host**:

```python
if not hp:
    attributable = key_attributable_from_root(
        key=pk, target_path=(), root_model=host_m,
        models_by_name=bundle.models_by_name, bundle=bundle,
        host_model=host_m, host_name=None,
    )
    reason = None if attributable else key_broadcast_reason(
        key=pk, target_path=(), root_model=host_m,
        models_by_name=bundle.models_by_name, bundle=bundle,
        host_model=host_m, host_name=None,
    )
    check_partition_key_attributable(
        label=label, pk=pk, attributable=attributable, reason=reason,
    )
    return
```

- **From the host, not the aggregate's root.** The naive fix (judge from the aggregate's
  root) rejects the legitimate cross-model `partition_by=status`: `status` is safe from
  the host (`orders`) but unattributable from the cross-model root (`customers`), so a
  from-root check raises it and breaks the association kernel (Egor's "broke 15 tests").
  Judging from the **host** cleanly separates the two: `status`'s closure is local to the
  host → passes; `bad_pop`'s closure fans from the host → fails. The cross-model root
  concern for a host-safe key stays where it already lives — the mode-aware planner
  (`check_cross_model_partition_keys_attributable`).

- **Mode-invariant (fails closed under `associate` too).** A key that fans from its own
  host is not single-valued at the host grain, so no mode can count it inline. This is an
  **input-safety** failure — the codebase already treats input safety as mode-invariant
  (`test_dev1841_association_errors::TestUnsafeInputInAssociate`) — distinct from Axiom
  8's mode-aware *dimension-attribution* concern (a safe dimension unattributable only
  from a further root). So the check does not take a `mode` argument. Empirically this
  breaks nothing: a spike of exactly this check ran the full unit suite green (18671
  passed, 0 failures), with `partition_by=status` still associating and the repro raising
  in all three modes naming `region_events`.

- **Single chokepoint.** The check lives at bind, inside `_validate_partition_keys`
  (`slayer/engine/bind_inputs.py`), reached via `rewrite_rank_partition_keys` mapped
  post-order over every declared measure, filter, and order spec. It therefore covers
  every position (measure, filter, order, computed dimension, re-aggregation inner,
  windowed) and both `AggregateKey` and `TransformKey` partition keys, with no
  per-position wiring. `assert_partition_key_attributable` is called unconditionally
  before the `lenient` producer-grain relaxation, so lenient keys are still safety-checked.

## Rejected alternative: mode-aware, planner-side (P1)

Threading `to_many_handling` into the check and adding a symmetric local-partition guard
in the planner (mirroring `check_cross_model_partition_keys_attributable`) would fail
closed under `broadcast`/`error` only and leave `associate` at its current silently-wrong
value. Rejected: it preserves a silent wrong result, needs mode plumbing plus a second
planner-side check, and leaves the path-less and path-bearing spellings inconsistent
under `associate`. The mode-invariant safety framing is the cleaner end-state (Egor
chose P2).

## Diagnostic: name the hop for both spellings

Today the path-**bearing** branch computes its reason with `broadcast_reason(host_path=hp,
...)`, using the key's own path, which yields "unreachable from the aggregate's root (no
join path from it)" even when the fan is provable. Switch both branches to the
closure-aware `key_broadcast_reason`, so the message names `region_events`. The
message-template shape (interned in `tests/_dev1871_raise_ledger.py` with a `…` reason
wildcard) is unchanged; only the interpolated reason improves. Sub-cases covered by
tests: a resolvable fanning closure names the hop; an unanalysable derived closure stays
deterministic and does not falsely name a hop. An unreachable or ambiguous partition_by
path never reaches this guard — dimension resolution rejects it first
(`UnresolvableDimensionJoinError` / `AmbiguousJoinPathError`), failing closed
deterministically upstream without inventing a hop (tests confirm the guard's hop is not
named there).

## StageSchema scope

`assert_partition_key_attributable` returns early when `scope` is not a `ModelScope`. A
`StageSchema` binds against the flat outputs of a prior stage, which carry no join graph
and no `Column.sql` join-crossing definitions — a fanning closure cannot exist there — so
this is not-applicable, not fail-open. A staged smoke test is added if such a shape is
constructible; otherwise the reasoning is recorded here and at the call site.

## Normative harness: Axiom 8 clarification (needs explicit approval of the exact diff)

`architecture/semantics.arc42.md` Axiom 8 currently reads, in its last clause:

> ; an explicit partition_by naming an unattributable dimension is an error outside
> associate mode. [enforced: test:tests/test_dev1841_association_errors.py]

Proposed replacement (drawing the safety-vs-attribution line this change relies on):

> ; an explicit partition_by naming a dimension unattributable only from a further
> (cross-model) root is an error outside associate mode
> [enforced: test:tests/test_dev1841_association_errors.py], but one whose own
> dependency closure (engine P10) crosses a fanning hop from its host is a
> mode-invariant input-safety error — raised in every mode, associate included, since it
> can never be counted without multiplying rows (Axiom 2.8).
> [enforced: test:tests/test_dev1911_fanning_partition_key.py]

This is a normative harness edit and is made only after Egor approves this exact diff.

## Deferred

Making the local partitioned aggregate *associate correctly* under `associate` (route it
through the association kernel the cross-model path already uses) is out of scope and
filed as DEV-1932 Case 1; Case 1b tracks whether the path-bearing spelling should
likewise associate rather than fail closed. This change only fails both closed.
