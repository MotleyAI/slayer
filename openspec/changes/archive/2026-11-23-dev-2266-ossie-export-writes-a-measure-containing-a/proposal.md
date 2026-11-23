## Why

The Ossie exporter refuses every measure whose formula contains a time bucket (`count_distinct(date_trunc('month', ordered_at))`): the metric travels only in SLayer's own `SLAYER` extension, so no other Ossie tool sees it.

## What Changes

- A time bucket inside a measure exports in the metric's Ossie SQL: `DATE_TRUNC('<UNIT>', CAST(x AS TIMESTAMP))` for every built-in unit, wrapped in `CAST(… AS DATE)` for a day-or-coarser bucket of a DATE, so it keeps its logical type on any engine.
- `week_sunday` exports as portable Ossie SQL through `DATEADD`; `date_add` in a measure now exports as Ossie `DATEADD` instead of `INTERVAL` arithmetic.
- A measure containing a custom-granularity bucket exports as a datasource-dialect entry only when SLayer reads that rendering back and it calls none of SLayer's registered functions; otherwise it stays SLayer-only with a report entry (every built-in dialect's rendering is refused today).
- The exporter binds measures with the datasource's custom granularities (today such measures fail to bind).
- Mode B's `date_part` accepts a bare part word (`DATE_PART(MONTH, x)`), as `EXTRACT` does, so SLayer reads its own exported extraction.
- `date_diff` is out of scope (DEV-2282, which settles Ossie `DATEDIFF`'s meaning).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `importers/osi-export`: measures containing time buckets export as Ossie SQL; the datasource-dialect-only rule covers custom-granularity buckets.
- `queries/date-functions`: `date_part` accepts a bare part word.

## Impact

- `slayer/sql/render/value_expr.py` (alias environment states its slotted key kinds), new `slayer/sql/dialects/ossie.py` (the Ossie document dialect), `slayer/sql/dialects/sqlite.py` (one registry of SLayer-registered functions), `slayer/engine/binding.py` (optional granularities), `slayer/osi/exporter.py`, `slayer/core/syntax.py`.
- `architecture/python.sql.arc42.md` principle 2 admits the Ossie document dialect (approved wording).
- Docs: `docs/osi/` export page.
