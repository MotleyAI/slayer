## Why

On the 1.8.x line, an associated aggregate keys its home rows on the primary key or, failing that, the first standalone `unique` column. A SQL `UNIQUE` column can hold several NULLs, and the association producer drops every row whose key is NULL, so an explicitly associated sum or count silently loses real rows ([#597](https://github.com/MotleyAI/slayer/issues/597)). `main` fixes this in [#598](https://github.com/MotleyAI/slayer/pull/598) by keying association on the primary key only and falling back to a per-query row identity, which 1.x lacks.

## What Changes

- Association keys a home on its primary key; a home without one is told apart by a per-query row identity (a row number over its filtered rows, before any join), as on `main`.
- A standalone `unique` column no longer serves as the association key; it still proves join cardinality.
- A root without a primary or unique key now associates instead of failing with a typed error.
- Association parameter checks read the numbered home, so a weighted average still uses each home row's own weight.
- Every dialect renders the row numbering, ordering by a constant where `ROW_NUMBER()` rejects an empty window.

## Capabilities

### New Capabilities

### Modified Capabilities

- `queries/attribution-modes`: association no longer requires a key; it deduplicates by the primary key, else by a per-query row identity.
- `queries/list-children`: association across a non-list to-many hop deduplicates by the primary key, else by the per-query row identity.

## Impact

Association planning (`slayer/engine/compile/stages.py`, `slayer/engine/elaborate_env.py`), SQL generation (`slayer/sql/generator.py`, `slayer/sql/naming.py`, `slayer/sql/dialects/`), `docs/concepts/queries.md`. Model and stored-query formats are unchanged. An associated home without a primary key incurs a row-number window. Ships as 1.8.5 from the `1.x` branch.
