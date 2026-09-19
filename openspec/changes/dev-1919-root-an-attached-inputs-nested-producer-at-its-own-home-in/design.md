## Context

See proposal.md — Why. `_synthesize_cross_model_producer` (`slayer/engine/compile/stages.py`)
has two arms. The association arm (DEV-1910) re-anchors the aggregate into the home's
coordinates with `reroot_from_root` (`slayer/engine/join_safety.py`): a host-side leaf
inside an attached input becomes a reverse-hop reference (`amount` → `orders.amount` in a
`customers`-rooted body), so nested regroup discovery sees a cross-model aggregate whose
own home is `orders` and compiles its producer there (Axiom 2.5); it masks attached
inputs with a literal before `_assert_cross_model_inputs_safe` (Axiom 2.3 opacity). The
broadcast/error arm uses the prefix-strip `reroot_value_key` when the home equals the
source anchor, leaving host-side leaves in coordinates the body cannot resolve, and runs
input safety unmasked; `_first_unattributable_attached_leaf` +
`check_attached_inputs_attributable` refuse what those coordinates cannot compile. The
same residue hits a mixed-source constituent under a different message, because the
unmasked closure descends into it.

Probe evidence (throwaway patch, 2026-09-19, reverted): with the reverse-hop reroot, the
mask and the guard deleted, the headline by `status` under broadcast gives 33780/407 on
both cells with the warning; by `customers.tier` every mode agrees (gold 63.75, silver
138.33, bronze 40); the mixed twin gives 33780 / (15300, 16600, 1880); the full unit
suite shows only the expected pin flips, an alias-only golden diff (dev1859
`param/associate`: the nested partition key now spelled in producer coordinates) and one
message-precedence change (below).

## Goals / Non-Goals

**Goals:**
- One rooting law: every arm re-anchors the aggregate the same way; every attached input's
  producer is rooted at its own home in every mode.
- Attached inputs opaque to the enclosing aggregation's input safety, by construction.
- The guard and its ledger row gone; guards baseline unchanged.

**Non-Goals:**
- Merging the two input-safety messages (hop-only vs argument-naming) into one.
- Changing how a nested producer inherits population filters (it follows the enclosing
  producer's routing exactly as under associate today).
- Any new attach kernel.

## Decisions

- **D1 One rooting law.** `agg_rooted = reroot_from_root(...)` is computed once above both
  arms. The association arm sets `locus="host"` always; the broadcast arm sets it only when
  `target_path != source_anchor_path(agg.source)` (the source-beyond-the-home case). Kernel,
  filter routing and warnings stay per arm. Alternative rejected: patching the broadcast
  arm's `else` branch only — leaves the reroot/mask/safety triple duplicated and drift-prone.
- **D2 Opacity lives in the safety helper.** `_assert_cross_model_inputs_safe` masks
  `attached_inputs(agg_rooted)` with `LiteralKey(1)` via `substitute_value_keys` internally,
  in every mode; the association arm's private masking block is removed. Each arm keeps its
  single safety call at its current position so the precedence of eligibility /
  explicit-partition-key errors versus input-safety errors is unchanged. The attached
  input's own inputs are judged by its own producer synthesis (recursion), including
  analyzability (fail closed) and the DEV-1911 fanning-partition-key rule at bind.
- **D2b One home-determination rule for attached parameters.** An explicitly grained
  attached (aggregate-valued) parameter is well-defined only when the home determines
  every member of its resolved grain (Axiom 2.3); the association arm already refuses
  this shape, and with the guard gone the plain path must too. One predicate serves
  both arms: each grain member attributable from the home over provably to-one hops,
  judged closure-aware as the `safe_pairs` loop judges a dimension, recursing into an
  aggregate-valued key's own grain; an expression-valued key is never determined; an
  ungrained parameter types at the query grain and stays determined (DEV-1859 decision
  12, unchanged). It runs at the top of the shared tail, after each arm's own
  eligibility and input-safety checks, so arm-internal error precedence is unchanged
  and the determination error fires in every mode and for any dimension.
  Column-valued and definition-default parameters keep their existing treatment in
  each arm; a source constituent's grain enters home resolution (2.2) and needs no
  second check. The predicate consumes the parameter's resolved grain, so a transform
  parameter (DEV-1903, result grain) and a windowed one (DEV-1915, window bucket) slot
  in without a second rule. Rejected alternative (Egor, 2026-09-19): extend Axiom
  2.4's column-valued clause to attached parameters so the home widens to `orders`
  and the query counts per order — a home-rule change beyond this brief.
- **D3 Guard deleted.** `_first_unattributable_attached_leaf`, `check_attached_inputs_attributable`,
  the import, and the `tests/_dev1871_raise_ledger.py` row. `_constituent_alias` and
  `broadcast_reason` stay (other users).
- **D4 Associate trigger narrows** to `mode == "associate" and bool(unattributable)`;
  `associated_measure = None if explicit else alias`. An attached-input aggregate with only
  attributable dimensions takes the plain path in every mode, so the association eligibility
  checks (root unique key, no `window=`/`first`/`last`) apply only when association is
  needed; values are identical because there is no fan-out to dedup.
- **D8 Attached parameters live in kwargs only** (spec-review, Egor 2026-09-19). The
  binder folds positionals onto declared names and refuses a positional value on an
  aggregation declaring none (ranked `first`/`last`: exactly one column ranking key), so after
  bind an attached parameter can only be a kwarg and the D2b rule is total by
  construction. Rejected: iterating `args` in `_check_attached_params_determined` — a
  second hand-kept channel, and the stray spelling stays a functional-form violation.
- **D5 Message precedence.** Honest coordinates make a host column used as a target ranking
  key (`customers.spend:last(ordered_at)`) trip the closure hop check before the
  argument-leaf check. In `_assert_cross_model_inputs_safe` the argument-leaf check now runs
  first and returns `(leaf, reason)` with the reason from `key_broadcast_reason`; the hop
  check runs only when no argument leaf failed. `check_cross_model_inputs_safe`'s leaf arm
  becomes "ranks/reads by L, which is not attributable from R (crosses a fanning or unproven
  join hop to H); declare join cardinality or a covering unique key on the target." The
  dev1838 ranking-key test and the DEV-1900 derived-kwarg tests both pass unchanged (the new
  leaf message is a superset of what each asserts); the dev1900 golden
  `fanning/cross_model_kwarg` is re-blessed. Alternatives rejected: keep hop-first and weaken
  the dev1838 assertion (loses the column name); merge both arms into one message (touches
  the source-crossing wording and its goldens for a side issue). The raise ledger collapses
  the reason as an interpolation (`(…)`) since it may also be the unreachable-no-path reason;
  the hop wording is pinned by the executed D5 tests, not the ledger.
- **D6 No new kernel.** With the grain determined by the home there is no fan-out; the plain
  null-safe grain attach (sql P10/P12) suffices. Nested compile, interning and the grain join
  are untouched.
- **D7 DEV-1906** (the older duplicate) is canceled; its references in tests are removed here.

## Spec-tests probe (throwaway patch D1–D5, 2026-09-19, reverted)

- D4 pin: stripping the customers PK makes `orders → customers` unproven, so the
  parameter's own key fails at bind; the pin uses a keyless customers with the hop
  declared many-to-one (`tests/_dev1919_fixtures.keyless_declared_models`).
- `amount:weighted_avg(weight=sum(customers.spend, partition_by=status))` is the
  mode-aware case (Axiom 8, enforced by `test_dev1911`); the mode-invariant sibling
  keys the parameter by `customers.regions.bad_pop`.
- Transform parameters fail at bind → deferred to DEV-1903; the windowed parameter
  fails at the sum/avg allowlist → deferred to DEV-1915; the windowed constituent
  form executes (10000 / 25000 / 28080 / 33780) and is pinned instead.
- D2b: without the check, the undetermined shape executed over the fanned
  `customers LEFT JOIN orders` rows (gold 182825/2425).
- Everything else as planned: headline / mixed / recursive values, plain-kernel plan
  shape, D5 messages (`bad_pop:last(ordered_at)` names the hop `customers`).

## Risks / Trade-offs

- [The dev1859 `param/associate` golden changes] → alias-only (nested partition key in
  producer coordinates); re-blessed through the allowed-delta manifest with the diff
  reviewed.
- [A Codex-folded scenario (recursive nesting, ranked-transform parameter, windowed outer)
  trips an unrelated gap] → probe first in spec-tests; an unrelated gap gets its own issue,
  never a narrowed scenario or a silent deferral.
- [Precedence change surfaces in another error pin] → the full unit suite under the probe
  showed exactly one (dev1838), covered by D5.
- [`check_cross_model_inputs_safe` signature change] → one caller; the raise-parity ledger
  row is updated in the same commit.
