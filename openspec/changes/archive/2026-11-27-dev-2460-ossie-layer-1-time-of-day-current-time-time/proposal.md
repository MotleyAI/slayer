## Why

Ossie Layer 1 requires `CURRENT_TIME`, the `TIME '…'` literal and `CAST(… AS TIME)`. SLayer has no time-of-day type: the literal is refused at parse time, `CURRENT_TIME` is unknown, the cast gives an opaque value or is refused, a `Time` field is opaque, and a database `TIME` column ingests as `DateTime` or `String`.

## What Changes

- A time-of-day type: `Time` becomes an ordered, operable type (dimension, comparison, ordering, the ordered aggregations, conditionals, `hour` / `minute` / `second`), never a time axis and never zone-converted.
- The `TIME '…'` literal, `current_time()` (also bare `CURRENT_TIME` and `CURRENT_TIME()`), and `CAST` / `::` / `TRY_CAST` to `TIME` yield a time of day.
- Every Tier-1 database answers: natively, as ISO text on SQLite, and on ClickHouse from server 25.6 (with a setting SLayer sends on 25.6–25.11). Older ClickHouse servers get a typed refusal.
- A result cell of a time-of-day column is always a time of day; a value that is not one fails the query with a typed error.
- Ingestion and drift type database time-of-day columns as `Time`.
- Strict Ossie mode accepts the three constructs.
- The Ossie compliance gate measures ClickHouse on server 25.12.
- **BREAKING**: bare `current_time` is a keyword; a field of that name must be written quoted.
- **BREAKING**: a `Time` field, and a cast to `TIME`, return a time of day in place of the driver's raw value; typed outputs carry a time type in place of text.
- **BREAKING**: a re-ingest re-types a stored `DateTime` or `String` field over a database time-of-day column to `Time`.
- **BREAKING**: a stored custom granularity named `current_time` is renamed `current_time_granularity` on upgrade.

## Capabilities

### New Capabilities
- `queries/time-of-day`: the time-of-day type, its three constructs, what it can do, its per-database behaviour, its results and its ingestion.

### Modified Capabilities
- `queries/casts`: a cast to `TIME` types as a time of day; SQLite no longer refuses it.
- `queries/expression-grammar`: the `TIME '…'` literal is no longer a rejected form.
- `queries/date-functions`: `date_part` takes a time-of-day operand for `hour`, `minute` and `second`.
- `compatibility/ossie-vocabulary`: `Time` no longer behaves as the opaque type; ingestion assigns it.

## Impact

- Code: `slayer/core` (type kinds, grammar, keys, strict-mode tables, result codec), `slayer/engine` (binding, typing, result normalisation, server-profile use), `slayer/sql` (dialects, rendering, reflection), `slayer/database` (result reading), `slayer/flight`, `slayer/pg_facade`, `slayer/facade`, `slayer/live_schema`, `slayer/storage` (rename gate).
- Tests: the required-constructs manifest and the compliance-gap list lose their time-of-day gaps; the Ossie gate's ClickHouse container moves to 25.12.
- Docs: datasets, references, time, expressions, database support, Ossie compliance, Ossie Layer 3, the 2.0 upgrade guide, MCP help topics.
- Spec attachment: `queries/time-of-day` sits under the existing cross-cutting `queries` group (`architecture/index.yaml`); no arc42 or model edit.
