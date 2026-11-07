## Why

Columns whose values are lists of scalars (BigQuery `REPEATED` fields, Postgres / DuckDB / ClickHouse / Trino / Snowflake arrays) are ingested as `TEXT`, losing the element type. On BigQuery the `TEXT` defaults offer `GROUP BY`, `=`, `count_distinct`, `min` and `max`, which the database refuses. Mode B has no list functions. So an agent cannot ask "contractors who have skill X", "with all of X and Y", "how many contractors per skill" or "which skills does this team have" in one ordinary query.

## What Changes

- New `ARRAY` data type with `Column.element_type`. Ingestion maps every native list type to it with the declared element type; for Snowflake's untyped `ARRAY`, the element type is inferred from the data at ingest.
- List columns are not dimensions, are not comparable, and aggregate only with `count`; every refusal names its remedy.
- One rule everywhere: a list's elements are its non-NULL elements.
- Mode-B list functions: `contains(L, x)` and `length(L)` are overloaded on a list first argument; new `contains_any(L, M)` and `contains_all(L, M)`. A new array literal `['a', 'b']`; `[]` is typed by context.
- Element access: a list column `L` is a keyless one-to-many child `L` with column `L.value` (semantics Axiom 3.2). Filters restrict the parent by EXISTS, `L.value` can be a dimension, and element aggregates never fan out parent measures. `associate` works across a list hop without a key.
- New built-in aggregation `collect(x)`: the distinct, sorted list of non-NULL values per cell.
- List results are JSON lists over REST and MCP, and JSON text in the BI facades, whose catalogs keep list-typed models.
- `inspect`, `search` and `models_summary` show a list column's element samples and its operations.
- Re-ingest re-types a stored `TEXT` column whose live column is a list (system principle 14 exception). Schema drift reports such a column as a re-type finding, never as a column to delete.
- SQLite, MySQL and SQL Server reject every list construct at plan time.
- Comparison matrix: new row Q27 "List (array) columns" with probes on all four engines.

## Capabilities

### New Capabilities
- `models/list-columns`: the list type, element types, ingestion per dialect, eligibility, the NULL-element rule, re-typing on re-ingest, and the inspect / search rendering.
- `queries/list-functions`: `contains`, `contains_any`, `contains_all` and `length` over lists, the array literal, typing, and dialect support.
- `queries/list-children`: element access through `L.value` — filters, dimensions, aggregates, association, composition and name collisions.
- `aggregations/collect`: the `collect` aggregation.
- `facade/list-values`: list values in the pg-wire, Flight and JDBC facades.

### Modified Capabilities
- `models/schema-drift`: a stored `TEXT` column whose live column is a list is a re-type finding, never a deletion.
- `models/sample-profiling`: list columns are profiled over their elements.

## Impact

- **Code:**
  - `slayer/core/enums.py`, `slayer/core/models.py`, `slayer/core/keys.py`, `slayer/core/syntax.py`, `slayer/core/scope.py` and `slayer/core/join_walker.py`;
  - `slayer/engine/ingestion.py`, `introspect_utils.py`, `binding.py`, `bind_inputs.py`, `elaborate_env.py`, `key_metadata.py`, `compile/stages.py`, `profiling.py` and `schema_drift.py`;
  - `slayer/ir/planned.py`;
  - `slayer/sql/generator.py`, `slayer/sql/render/*` and `slayer/sql/dialects/*`;
  - `slayer/storage/base.py` (save-time collisions);
  - `slayer/inspect/*`, `slayer/search/render.py`, `slayer/mcp/server.py`, `slayer/core/query.py` (field descriptions), `slayer/facade/*`, `slayer/pg_facade/types.py` and `slayer/flight/types.py`.
- **Docs:** `docs/concepts/models.md`, `references.md`, `formulas.md`, `queries.md`, `docs/database-support.md`, and `slayer/memories/help_content/*`.
- **Comparison:** `examples/comparisons/dataset.sql`, `probes.yaml`, `matrix.yaml`, `matrix.json`, and each engine's model files.
- **Architecture:** `semantics.arc42.md` Axiom 3.2 (added, `[target: DEV-2137]`, flipped to `[enforced]` in this change) and `system.arc42.md` principle 14 (exception added). No c4 change expected.
- **Behaviour change for stored models:** none until re-ingest. After a re-ingest, list columns that were `TEXT` stop being groupable and comparable.
