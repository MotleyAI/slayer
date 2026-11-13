## Why

SLayer 2.0 makes a valid Ossie query or model valid SLayer with Ossie's meaning, and 2.0 is the only release allowed to change the meaning of input SLayer already accepts. One such difference remains in the expression language: SLayer reads a date-only string compared with a timestamp (`created_at <= '2024-04-10'`) as the whole day, while Ossie, like plain SQL, reads it as midnight. Two smaller gaps sit beside it: SQLite casts to timestamp types collapse to the year, and Ossie's numeric rules (true division, `ROUND` ties away from zero) hold today but are not pinned. Everything additive from the original DEV-2280 scope (`OVER`, DISTINCT aggregates, `WITHIN GROUP`, the missing functions, quoted identifiers) moved to DEV-2282 for 2.x.

## What Changes

- **BREAKING** A date-only string `'YYYY-MM-DD'` compared with a temporal operand (`=`, `!=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `NOT BETWEEN`, either order) is the instant at its midnight, in every position and on every dialect. The whole day stays spellable as single-string `in '…'` / `not in '…'` and as `date_range`. Other period literals and relative tokens keep their period meaning. A value bound through a variable follows the same rule.
- A query or save comparing a date-only literal with a TIMESTAMP operand by `<=`, `>`, `=`, `!=`, `BETWEEN` or `NOT BETWEEN` gets one informational structured warning naming the whole-day spelling, marked for removal in 3.0.
- Stored queries, memories and models written before this release keep their results: a load-path migration with model context rewrites each affected comparison to its whole-day equivalent.
- The removal-tripwire marker mechanism gains a marker for informational warnings, which emits no `FutureWarning`.
- Ossie export writes a comparison against an instant as standard SQL instead of dropping the measure.
- SQLite renders casts to timestamp types through its timestamp normaliser and refuses a `TIME` cast (and any timestamp modifier it cannot keep) at plan time.
- Integer division and two-argument `ROUND` ties are pinned on every Tier-1 dialect.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/time-points`: a date-only literal in a comparison is an instant; frame-bound rules follow; the fresh-input warning.
- `models/stored-upgrade`: stored date-only comparisons are upgraded to keep their whole-day meaning.
- `compatibility/removal-tripwire`: the shared marker covers informational warnings scheduled for removal.
- `importers/osi-export`: instant comparisons export as standard SQL.
- `importers/osi-import`: an imported metric's date-only predicate keeps Ossie's meaning.
- `queries/casts`: SQLite casts to timestamp and time types.
- `queries/scalar-functions`: division and `ROUND` tie semantics.

## Impact

- `slayer/core/time_points.py` (classification), binding of time-point comparisons, frame-bound detection, structured warnings.
- `slayer/storage/` load path, migration registry and Mode-B text walkers; stored versions SlayerModel, SlayerQuery and Memory.
- `slayer/core/deprecation.py` and `tests/test_law_removal_tripwire.py`.
- `slayer/osi/exporter.py`; `slayer/sql/dialects/sqlite.py` cast rendering.
- Docs: `docs/concepts/time.md`, agent help in `slayer/memories/help_content`, `docs/dbt/dbt_import.md`, `docs/osi/osi_import.md`, and the 2.0 migration guide `docs/upgrading/2.0.md` (arriving with PR #590).
