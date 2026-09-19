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
- **D3 Guard deleted.** `_first_unattributable_attached_leaf`, `check_attached_inputs_attributable`,
  the import, and the `tests/_dev1871_raise_ledger.py` row. `_constituent_alias` and
  `broadcast_reason` stay (other users).
- **D4 Associate trigger narrows** to `mode == "associate" and bool(unattributable)`;
  `associated_measure = None if explicit else alias`. An attached-input aggregate with only
  attributable dimensions takes the plain path in every mode, so the association eligibility
  checks (root unique key, no `window=`/`first`/`last`) apply only when association is
  needed; values are identical because there is no fan-out to dedup.
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
  the source-crossing wording and its goldens for a side issue).
- **D6 No new kernel.** With the grain determined by the home there is no fan-out; the plain
  null-safe grain attach (sql P10/P12) suffices. Nested compile, interning and the grain join
  are untouched.
- **D7 DEV-1906** (the older duplicate) is canceled; its references in tests are removed here.

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
