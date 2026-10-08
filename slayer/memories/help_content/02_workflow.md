# Workflow

## Discovery

1. `inspect(entity_type="model")` — every model grouped by datasource, one line each.
2. `inspect(reference="<model>", entity_type="model")` — columns, measures, joins,
   sample rows (`num_rows`).
3. `search(question="...")` — find models, columns, and saved learnings by meaning.

## Method

Decompose the question into blocks first: every qualifier, projected column, filter,
grouping, unit, rounding, and ordering hint is one block, and each must map to a named
column, measure, filter or dimension. Never drop a qualifier because no entity matched:
search for it, else encode it as an expression or an inline extension column; reference
already-encoded quantities by name rather than re-deriving them. Pin explicitly rather
than guessing: which aggregation ("typical" is not automatically avg vs median), the
grouping column and raw vs standardized labels, each aggregate's scope (all rows vs a
filtered subset), sort column + direction + tie-break, NULL handling, units and
rounding, exact numeric constants. "How many" is a scalar `count(*)`; "which / list /
show" is the rows. Project exactly the columns the question names.

Answer with one query whenever the query language can express it (shares, ranks,
windows, period-over-period, joined aggregates, nesting) instead of combining several
results by hand or in code.

## Filter literals

Build every `==` / `in` / `like` condition on a text column from that column's
sampled values (inspect it with `compact=false`), never a guessed spelling; samples are a top-N snapshot,
so verify a needed literal that is absent (e.g. a distinct-values query) rather than
assume.
Compare case- and whitespace-insensitively in the condition only, never on a projected,
grouped or join-key column. Apply only the transformations (TRIM / ROUND / CAST /
dedup) the question or a governing definition requires.

## Building and verifying

Start with one measure and a small `limit`; add dimensions, then conditions, then
transforms, checking row counts at each step. Debug with `show_sql=true`; preview the
SQL with `dry_run=true`.

Run the exact final query and read the result: row count plausible; no dimension-only
GROUP BY when you wanted per-record rows (`distinct_dimension_values: false`); sort
column and direction as asked; each aggregate's scope right; NULL behaviour intended;
string values in the expected casing. On a wrong result change one variable at a time:
two changes per attempt make the outcome uninterpretable.

When a one-off concept is missing, extend inline (`source_model` as an extension)
rather than editing the stored model; persist reusable shapes with `create_model`
(`query=` for multi-stage results).

## Error decoder

| Fragment | Check |
|---|---|
| "not found" on a measure/column | `inspect` the model — spelling, or on a joined model? |
| "not allowed on" | the column's `allowed_aggregations` whitelist |
| "Unresolvable" / "ambiguous" path | missing join edge, or write the full path |
| "Time dimension required" | add `time_dimensions` or set `main_time_dimension` |
| "matches a source column" | rename the measure as the suggestion says |
| database connection errors | `describe_datasource(name=...)` runs a live check |

## Connecting a database

Fast: `create_datasource(..., auto_ingest=true)` → `models_summary`. Cautious:
`create_datasource(..., auto_ingest=false)` → `describe_datasource` (verify + list
tables) → `ingest_datasource_models` → `models_summary`.
