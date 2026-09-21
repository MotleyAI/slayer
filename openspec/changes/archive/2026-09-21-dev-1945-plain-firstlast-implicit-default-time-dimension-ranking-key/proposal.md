## Why

A `first`/`last` aggregation's ranking key is an input of the aggregation (semantics
Axiom 2.4: the ordering key must be determined by the home) and every input-safety
predicate consumes an input's dependency closure (engine P10). Today only an EXPLICIT
ranking argument is judged, and by raw path rather than by closure; the key resolved
implicitly by the ranked planner — temporal row dimension → time dimension's raw column
→ the model's `default_time_dimension` — is never judged at all. A model whose
`default_time_dimension` names a derived column across an unproven hop is accepted at
save time with a warning (the DEV-1930 backstop contract), and `amount:last` with no
time dimension then ranks over the fanned join silently, host-rooted and target-rooted
alike; a fanning temporal dimension or time dimension used as the implicit key ranks
inside the fan too, while its explicit spelling is refused.

## What Changes

- The ranking key of a ranked aggregation — explicit argument or implicitly resolved,
  whichever candidate wins — is judged by its dependency closure from the producer's
  root, for host-rooted, target-rooted and windowed producers alike; a key crossing a
  hop that is not provably many-to-one fails closed with the existing input-safety
  messages, naming the column and the hop.
- One closure predicate serves both judgement sites: the pre-compile explicit-argument
  role (which keeps the spec's argument-before-source precedence and now names a
  derived argument instead of falling through to the hop-only message) and the
  post-compile resolved key inside the two kernel builders.
- The model-default key is typed by the binder's rule (a derived or filtered column
  expands), so its definition is visible to the closure.
- The ranked planner's dead target-rooted branch (`target_path` re-anchoring and its
  "not resolvable" refusal) is deleted with its one unit test; a target-rooted producer
  resolves on its re-rooted aggregate exactly like a host-rooted one.
- No new checker function, no new raise site, no new deferral site; the DEV-1871
  ledger, the guards baseline and the legacy-arrow baseline are unchanged.
- **BREAKING** for two silently-wrong shapes: a fanning temporal dimension or time
  dimension serving as the implicit ranking key now fails closed (its explicit spelling
  already did).
- DEV-1729 (the same gap under the pre-DEV-1900 "isolate" framing) is marked a
  duplicate of DEV-1945; its pinned test uses a proven hop and stays green.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/cross-model-aggregates`: "Unsafe aggregate inputs fail closed" — the ranking
  key of a ranked aggregation, explicit or implicitly resolved, is an input judged by
  closure from the producer's root; a derived explicit ranking argument is named as the
  argument.
- `models/column-definitions`: "Unproven references are accepted with a save-time
  warning and a query-time backstop" — the backstop also refuses such a column when it
  ranks a `first`/`last` as the model's `default_time_dimension`.

## Impact

- `slayer/engine/ranked_planner.py` (`resolve_ranking_time_key`: typed default, dead
  branch removed), `slayer/engine/compile/stages.py` (`_assert_local_producer_inputs_safe`
  explicit-argument role, `_ranked_kernel`, `_trailing_window_kernel`, one shared
  ranking-key crossing helper and one checked-resolution helper).
- Tests: new `tests/_dev1945_fixtures.py` + `tests/test_dev1945_ranking_key_safety.py`;
  `tests/test_dev1476_first_last_explicit_time.py` loses the dead-branch unit test. No
  goldens (the errors are dialect-independent).
- Docs: one sentence in `docs/concepts/models.md`; arc42: an `enforced:` tag on
  semantics Axiom 2 (approved edit, applied when the test exists).
