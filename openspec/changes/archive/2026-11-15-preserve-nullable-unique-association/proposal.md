## Why

A standalone SQL `UNIQUE` constraint can allow multiple NULL values. Association uses that column as the home-row identity, and the generated presence check drops real rows whose identity is NULL. Sums and counts can understate the rows that reach a group. See [#597](https://github.com/MotleyAI/slayer/issues/597).

## What Changes

- Number the association home rows before the join when the model has no primary key, including models with standalone unique columns.
- Verify grouped sums and counts through a fanning join on SQLite and DuckDB, both with default association and with explicit `associate`.

## Capabilities

### Modified Capabilities

- `queries/attribution-modes`: extend per-query row identity to association homes with standalone unique columns.

## Impact

The change is in association planning. The model and stored query formats are unaffected. An associated home with only standalone unique columns now incurs the same row-number operation as a keyless home.
