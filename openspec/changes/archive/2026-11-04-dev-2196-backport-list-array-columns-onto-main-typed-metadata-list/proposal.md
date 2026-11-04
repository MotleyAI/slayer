## Why

List (array) columns shipped on the Mode-B grammar stack, which needs more time before it reaches `main`. Meanwhile `main` maps every native array to `TEXT` and offers agents queries some databases refuse (BigQuery rejects `GROUP BY`, `=`, `COUNT(DISTINCT)`, `MIN` and `MAX` on a `REPEATED` field). This change ports the list-column feature onto `main` now, matching the stack's behaviour wherever it does not depend on stack-only machinery, and keeping its names so the later stack merge resolves as same-content.

## What Changes

- `DataType.ARRAY` with an `element_type`; ingestion maps every native list type (Postgres, BigQuery, DuckDB, ClickHouse, Trino, Snowflake incl. inferred element types) instead of falling back to `TEXT`.
- List columns are not dimensions, comparison operands or general aggregation inputs; `count` is their only aggregation.
- List functions `contains`, `contains_any`, `contains_all` and list `length`; bracketed array literals; list-valued `{variables}` in a list argument.
- `contains` is list-only on `main`: a non-list first argument is a typed error pointing at `LIKE`.
- Element types compare within a type family; a numeric string literal against a number list is read as the number; any other mismatch is a typed error (no column coercion on `main`).
- Element access: a list column is a keyless one-to-many child (`L.value`, `count(L.*)`), usable in filters, dimensions and aggregates without fan-out, in association, through to-one joins and in later query stages.
- `collect` built-in aggregation returning the distinct sorted list; its empty value is the empty list.
- Profiling and sampling read a list's elements.
- A stored non-list column over a live list is a report-only re-type finding; re-ingest re-types it in place keeping only `count`.
- Facades return lists as JSON text; REST/MCP JSON returns lists as JSON arrays.
- Databases without native lists refuse every list construct at plan time.
- A model's custom aggregation may take any scalar or list function name and shadows that function inside its model, with a structured warning; a formula-backed `collect` stays the model's own; a stored datasource granularity named like a reserved library name is renamed on load with every reference rewritten. A stored model that loads on 1.6.0 loads unchanged.
- arc42: semantics Axiom 3.2 (List children) and Axiom 4 (empty list for `collect`), system P14 (re-type exception), engine P5 (stage element access).

## Capabilities

### New Capabilities
- `models/list-columns`: the `ARRAY` type and element type, native-list ingestion, Snowflake element inference, non-NULL elements, list-column restrictions, backend refusals, re-typing on re-ingest, agent rendering.
- `queries/list-functions`: `contains`, `contains_any`, `contains_all`, list `length`, array literals and list variables, element-type matching.
- `queries/list-children`: element access through the list child, element filters, element dimensions, keyless association, no fan-out, composition, hop-name collisions, in-row emission.
- `aggregations/collect`: the `collect` aggregation, its empty value, scalar-only inputs, list results, backend support, and a model's own `collect` formula.
- `facade/list-values`: list columns in facade catalogs and list values as JSON text over the wire facades.

### Modified Capabilities
- `models/sample-profiling`: list columns are profiled over their elements.
- `models/schema-drift`: a non-list column over a live list is a report-only re-type finding; a declared list is not drift.
- `queries/cross-model-aggregates`: before ClickHouse 25.8, a semi-join pushdown that also reads a list's elements fails naming 25.8.
- `models/aggregation-names`: a model aggregation may take a library function name and shadows it in its model, announced every time; transform and date-syntax names stay reserved.
- `models/stored-upgrade`: reserved-named stored granularities are renamed with every reference; library growth never invalidates a stored artefact.

## Impact

- Code: `slayer/core/` (enums, models, keys, join_walker, scope, query, format, warnings, errors), `slayer/engine/` (syntax, binding, bind_inputs, elaborate_env, key_metadata, compile/stages, profiling, ingestion, schema_drift, query_engine), `slayer/ir/`, `slayer/sql/` (client, generator, scope_check, render/aggregates, render/value_expr, render/row_expr, dialects incl. new `_clickhouse_cells.py`, reflection), `slayer/storage/` (base, both backends, new `granularity_rename.py`), facades (`facade/datatypes.py`, `pg_facade/types.py`, `flight/`), `slayer/mcp/` and help topics.
- No model, query or memory version bump.
- Docs: concepts (models, formulas, queries, references, ingestion, terminology), database support, pg-facade, flight-sql, MCP reference.
- Architecture: `semantics.arc42.md` Axioms 3.2 and 4, `system.arc42.md` P14, `python.engine.arc42.md` P5 (approved wording); no `.c4` change.
