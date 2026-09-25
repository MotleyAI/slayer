## Why

DEV-1871 centralized the scattered shape guards into one type checker (`slayer/engine/elaborate_env.py`) under strict parity, so the checker still raises the inherited mishmash — bare `ValueError`, one `NotImplementedError`, bare `SlayerError`, and typed classes with unstructured messages. Core arc42 P4 wants intentional failures typed and stably formatted; the centralized raise surface makes the unification one bounded change.

## What Changes

- New `QueryTypeError(SlayerError)` base in `slayer/core/errors.py` (keyword-only constructor, never raised directly) that always renders via `_format_error_message` and exposes `.summary` / `.location` / `.scope` / `.suggestion`.
- Thirteen concrete families beneath it (`TimeAxisError`, `WindowDurationError`, `PartitionKeyError`, `UnsafeJoinInputError`, `UnanalyzableDependencyError`, `TransformInputError`, `ComputedDimensionError`, `AssociationError`, `ParameterGrainError`, `ReaggregationError`, `NameCollisionError`, `DimensionTypeError`, `ModelFilterError`); the existing `PositionTypingError`, `DistinctDimensionValuesError`, `TimeDimensionColumnError`, `MeasureNameCollidesWithColumnError`, `CanonicalAliasShadowsColumnError`, `DuplicateMeasureNameError` re-parented into the family.
- Every checker raise converted; each message split into `location` (the subject), `summary` (the defect) and `suggestion` (the remedy).
- **BREAKING** (message text): every checker type-error message changes to the `ClassName: summary` + `at` / `suggestion:` layout.
- **BREAKING** (type): the time-axis violation raises `TimeAxisError` instead of `NotImplementedError` — REST answers 400 with the message (was a bare 500); Flight SQL reports invalid-argument (was unimplemented).
- `parse_window_duration` raises `WindowDurationError` for every malformed duration, including non-string input.
- MCP resolution-error rendering no longer doubles the class-name prefix.
- The raise-ledger parity test ratchets the checker: every checker raise must be a concrete `QueryTypeError` subclass.

## Capabilities

### New Capabilities
- `queries/type-errors`: the typed, stably-formatted error family every query type error raised by the checker belongs to.

### Modified Capabilities
<!-- none: existing specs say "typed error" or "ValueError", both still true -->

## Impact

- Code: `slayer/core/errors.py`, `slayer/engine/elaborate_env.py`, `slayer/core/window_duration.py`, `slayer/core/query.py`, `slayer/mcp/server.py`; Flight error mapping only if the new test shows the implicit path fails.
- Tests: raise ledger + parity test, guard ratchet, ~110 golden raise entries re-blessed, a handful of message-pinning tests.
- Architecture: core arc42 P4 and engine arc42 P9 wording (approved).
- Docs: one sentence in `docs/concepts/queries.md`.
- Follow-up: DEV-1969 (remaining engine/core raises, plain-message `SlayerError`s).
