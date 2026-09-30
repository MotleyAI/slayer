## Why

Callers must compute every date bound themselves: there are no relative ranges ("last month"), no period literals (`'2025-Q1'`), no one-sided `date_range`, and `gran(col)` is rejected inside filters (DEV-1999, absorbing DEV-1886; semantic-layer comparison DEV-1998 row Q20). Worse, today's bounds are wrong and dialect-dependent on TIMESTAMP columns: `date_range: ['2024-01-01', '2024-12-31']` drops Dec-31 rows (all of them on SQLite, all after midnight on DuckDB), and `whole_periods_only` emits `col <= '<yesterday>'`, dropping complete periods, and only when no filter string textually mentions the column.

## What Changes

- **Time points.** A string literal compared with a temporal operand, or used as a `date_range` element, is a *time point*: an **instant** (ISO date-time with a time part — plain comparison) or a **period** `[start, next_start)` — period literals `YYYY`, `YYYY-Qn`, `YYYY-MM`, `YYYY-Www`, `YYYY-MM-DD`, or relative tokens (`today`, `this|last|next <unit>`, `last N <units>`, `N <units> ago`, `<unit> to date`, …) resolved in Python from an injectable engine clock read once per execution (SQL-side `now()` / `current_date()` keep reading the database clock).
- **Period comparison semantics in Mode-B filters** (`x >= P`, `x > P`, `x < P`, `x <= P`, `x = P`, `x != P`, and new single-string `x in P` / `x not in P`) lowered to half-open relational bounds. **BREAKING**: a date-only string compared with a DATE/TIMESTAMP column now means the whole day (`ts <= '2024-12-31'` includes Dec 31 10:00; `ts > '2024-01-01'` excludes all of Jan 1; `ts = '2024-01-01'` matches the whole day).
- **`date_range`** accepts a single period (string or one-element list) and one-sided ranges (`null` bound); a date-only upper bound covers its whole day. **BREAKING**: `[]`, 3+ elements and `[null, null]` are typed errors (the `MALFORMED_DATE_RANGE` warning is removed); results on TIMESTAMP columns change as above.
- **`gran(col)`** binds as a row-level time-bucket expression in every position (filters, aggregation sources, computed-dimension sub-expressions); its comparison with a literal lowers to an exact raw-column bound.
- **One temporal typing, one ISO grammar, shared with the date functions**: a temporal operand is anything `core.keys.temporal_type` types DATE/TIMESTAMP (columns, `min`/`max`/`first`/`last`, date functions, clock calls, conditionals) or a direct `gran(col)`; date-position ISO literals accept the time-point instant grammar.
- **`whole_periods_only`** is rebuilt as typed bounds: frame bounds on each time dimension's column snap to the earlier bucket boundary, the upper bound clamped to now, so every returned bucket is complete and the current bucket is excluded. **BREAKING**: results change (previously dropped complete periods; now snaps).
- Lowered time bounds stay **frame bounds**: they never clip trailing windows or `time_shift`; a period `=` / `in` is a frame bound.
- The **SQL facade** converts date-only literals in the time bounds it emits to instants, so translated SQL keeps its SQL meaning.
- Typed errors (fail closed) for unparseable time strings and zone offsets against temporal operands, relative tokens against non-temporal operands (the date functions' `DateOperandTypeError`), and sub-day points against DATE operands.
- New concept page `docs/concepts/time.md` as the single temporal reference.

## Capabilities

### New Capabilities
- `queries/time-points`: time points (instants, period literals, relative tokens), the clock, temporal operands and their typing errors, the comparison lowering, `gran(col)` as an expression in every position, frame-bound status of lowered bounds, SQLite timestamp-storage independence, and `whole_periods_only` snapping.

### Modified Capabilities
- `queries/date-range`: `date_range` elements are time points; single-period and one-sided ranges; null-bound error and wrong-length warning requirements replaced by typed shape errors.
- `queries/time-dimensions`: a granularity call inside a filter is now recognised (the "not recognised" scenario and the filter sentence in the order-key requirement are replaced).
- `facade/time-filter-translation`: date-only literals in facade-emitted time bounds are converted to instants.
- `queries/date-functions`: date-position ISO literals use the time-point instant grammar; a string literal compared with a temporal operand is a time point.

## Impact

- `slayer/core`: new pure `time_points.py`; `TimeDimension.date_range` type; `SlayerQuery.snap_to_whole_periods` deleted; `keys.py` gains `TimePointCmpKey` (transient, resolved before planning), loses `BetweenKey` and `parse_iso_temporal` (moved into `time_points.py`, the one ISO parser); resolved bounds are `LiteralKey(date|datetime)`, which `time_bounds.py` accepts.
- `slayer/engine`: parser (scalar-string `in`), binder (`gran(col)` → `TimeTruncKey`, time-point comparisons → `TimePointCmpKey`), one checker-owned resolution pass in `bind_inputs` over `core.keys.temporal_type` (errors, lowering), after `check_date_operands` and before time-key attachment, typed `whole_periods_only`, clock on the engine and `now` on `ResolvedSourceBundle`; `normalization.py` loses `MALFORMED_DATE_RANGE`.
- `slayer/sql`: `TimeTruncKey` in the row-expression renderer; comparisons against a temporal literal render through one dialect hook (BigQuery and SQLite overrides).
- `slayer/facade/translator.py`: AST-level instant-ization.
- Surfaces: REST/MCP `date_range` schema; MCP `query` tool docs.
- Goldens `dev1745`, `dev1747`, `dev1958` re-blessed; docs across `docs/concepts`, `docs/reference`, `docs/interfaces`, `docs/examples/04_time`.
