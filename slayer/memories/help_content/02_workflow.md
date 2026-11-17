# Workflow

## Discovery

1. `inspect(entity_type="dataset")` — every dataset grouped by datasource, one line each.
2. `inspect(reference="<dataset>", entity_type="dataset")` — fields, metrics, relationships,
   sample rows (`num_rows`).
3. `search(question="...")` — find datasets, fields, and saved learnings by meaning.

## Method

Decompose the question into blocks first: every qualifier, projected field, filter,
grouping, unit, rounding, and ordering hint is one block, and each must map to a named
field, metric, filter or dimension. Never drop a qualifier because no entity matched:
search for it, else encode it as an expression where it is used; reference
already-encoded quantities by name rather than re-deriving them. Pin explicitly rather
than guessing: which aggregation ("typical" is not automatically avg vs median), the
grouping field and raw vs standardized labels, each aggregate's scope (all rows vs a
filtered subset), sort field + direction + tie-break, NULL handling, units and
rounding, exact numeric constants. "How many" is a scalar `count(*)`; "which / list /
show" is the rows.

## Filter literals

Build every `=` / `in` / `like` condition on a text field from that field's
sampled values (inspect it with `compact=false`), never a guessed spelling; samples are a top-N snapshot,
so verify a needed literal that is absent (e.g. a distinct-values query) rather than
assume.
Compare case- and whitespace-insensitively (`trim(name) ILIKE 'al%'`) in the condition only, never on a projected,
grouped or join-key field. Apply only the transformations (TRIM / ROUND / CAST /
dedup) the question or a governing definition requires.

## Building and verifying

Start with one metric and a small `limit`; add dimensions, then conditions, then
transforms, checking row counts at each step. Debug with `show_sql=true`; preview the
SQL with `dry_run=true`.

Run the exact final query and read the result: row count plausible; no dimension-only
GROUP BY when you wanted per-record rows (`distinct_dimension_values: false`); sort
field and direction as asked; each aggregate's scope right; NULL behaviour intended;
string values in the expected casing. On a wrong result change one variable at a time:
two changes per attempt make the outcome uninterpretable.

When a one-off concept is missing, write it as an expression in the position that uses
it rather than editing the stored dataset; persist reusable shapes with `create_dataset`
(`query=` for multi-stage results).

## Error decoder

| Fragment | Check |
|---|---|
| "not found" on a metric/field | `inspect` the dataset — spelling, or on a joined dataset? |
| "not allowed on" | the field's `allowed_aggregations` whitelist |
| "Unresolvable" / "ambiguous" path | missing relationship, or write the full path |
| "Time dimension required" | add `time_dimensions` or set `main_time_dimension` |
| "matches a source field" | rename the metric as the suggestion says |
| "is a list" | group, compare or aggregate its elements (`tags.value`), or test `contains(tags, x)` |
| database connection errors | `describe_datasource(name=...)` runs a live check |

## Connecting a database

Fast: `create_datasource(..., auto_ingest=true)` → `datasets_summary`. Cautious:
`create_datasource(..., auto_ingest=false)` → `describe_datasource` (verify + list
tables) → `ingest_datasource` → `datasets_summary`.
