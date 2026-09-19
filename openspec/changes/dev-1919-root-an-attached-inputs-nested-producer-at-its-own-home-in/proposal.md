## Why

An attached input — an aggregate or grained transform appearing as an aggregation's
parameter or as a source constituent — is opaque and has its own home (semantics
Axioms 2.3, 2.5, 9), yet the broadcast/error arm of the cross-model producer
synthesis walks the input's interior and refuses the query when a leaf is not
attributable from the OUTER aggregation's home. The headline
`customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
rooted at `orders` executes under `associate` but is refused under the default mode
and under `error`, a "not supported inside" refusal of a well-typed term — a closure
violation kept as an implementation residue. The association arm already roots the
input's producer at its own home; the lift makes every arm do so.

## What Changes

- One rooting law in `_synthesize_cross_model_producer`: the aggregate is re-anchored
  into the home's coordinates once, above both arms, by the reverse-hop-aware reroot
  the association arm already uses; each attached input's producer is then compiled
  at the input's own home and attached onto the outer home's rows by its grain,
  null-safely, in every mode. The arms differ only in locus, kernel, filter routing
  and warnings.
- Attached inputs are opaque to the outer input-safety check: the masking the
  association arm did privately moves into the shared safety helper.
- The guard (`_first_unattributable_attached_leaf`, `check_attached_inputs_attributable`)
  and its raise-ledger row are deleted.
- The association arm is entered only when some dimension is unattributable; an
  attached-input aggregate with only attributable dimensions takes the plain path in
  every mode, so association eligibility (root unique key, no `window=`/`first`/`last`)
  applies only when association is needed.
- Input-safety message precedence: an explicit column argument crossing a fanning or
  unproven hop is reported first, naming both the column and the hop; the closure hop
  message covers the rest.
- Pinned refusals flip to executed values; DEV-1906 (duplicate) is canceled and its
  references removed.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/partitioned-aggregates`: "Attached parameters on row-level sources" — the
  parameter is attached per home row in every mode; the default-mode twin executes.
- `aggregations/expression-aggregation`: "Expression source typing" — an attached input
  (source constituent or parameter) is compiled at its own home in every mode; the
  broadcast/error attributability restriction is dropped.
- `queries/attribution-modes`: "Association eligibility and input handling" — eligibility
  applies only when some dimension is unattributable.
- `queries/cross-model-aggregates`: "Unsafe aggregate inputs fail closed" — an explicit
  column argument crossing a hop is reported naming both the column and the hop, ahead
  of the closure hop message.

## Impact

- `slayer/engine/compile/stages.py` (`_synthesize_cross_model_producer`,
  `_association_arm`, `_assert_cross_model_inputs_safe`, `_first_unattributable_arg_leaf`),
  `slayer/engine/elaborate_env.py` (`check_attached_inputs_attributable` deleted,
  `check_cross_model_inputs_safe` leaf arm).
- Tests: pins in `test_dev1859_attached_param_exec.py`, `test_dev1892_residue.py`,
  `test_dev1832_cross_model_exec.py`, `_dev1871_raise_ledger.py`; goldens dev1859
  (`param/associate` re-blessed, `param/broadcast` + `mixed/cross_model_constituent`
  added) and dev1900 (`fanning/cross_model_kwarg` re-blessed); new
  `tests/test_dev1919_home_rooted_attached_inputs.py`.
- Docs: one sentence in `docs/concepts/queries.md`; arc42: an `enforced:` tag on
  Axiom 9 (approved edit, applied when the test exists).
- Guards baseline and legacy-arrow baseline unchanged; no new axiom.
