# Workflow

## Discovery

1. `inspect(entity_type="model")` — every model grouped by datasource, one line each.
2. `inspect(reference="<model>", entity_type="model")` — columns, measures, joins,
   sample rows (`num_rows`).
3. `search(question="...")` — find models, columns, and saved learnings by meaning.

## Building a query

Start with one measure and a tiny `limit`; add dimensions, then filters, then
transforms (they need a time dimension), checking row counts at each step. Debug
with `show_sql=true`; preview without executing via `dry_run=true`.

When a one-off concept is missing, extend inline (`source_model` as a
ModelExtension) rather than editing the stored model; persist reusable shapes with
`create_model` (`query=` for multi-stage results). A named stage shadows a stored
model of the same name.

## Error decoder

| Fragment | Check |
|---|---|
| "not found" on a measure/column | `inspect` the model — spelling, or on a joined model? |
| "not allowed on" | the column's `allowed_aggregations` whitelist |
| "Unresolvable" / "ambiguous" path | missing join edge, or write the full path |
| "Time dimension required" | add `time_dimensions` or set `main_time_dimension` |
| database connection errors | `describe_datasource(name=...)` runs a live check |

## Connecting a database

Fast: `create_datasource(..., auto_ingest=true)` → `models_summary`. Cautious:
`create_datasource(..., auto_ingest=false)` → `describe_datasource` (verify + list
tables) → `ingest_datasource_models` → `models_summary`.
