# Workflow

How to chain the MCP tools (or CLI commands) for common tasks. Complements the
tool-by-tool documentation, which covers what each one does in isolation.

## Discovery — "what data is here?"

```text
1. list_datasources()                       # pick a datasource
2. models_summary(datasource_name="mydb")   # brief list of its models
3. inspect_model(model_name="orders")       # dimensions, measures, sample rows, SQL
```

`models_summary` gives one line per model with just names + descriptions of
its fields and the list of joined models — pick the right one without the
weight of a full `inspect_model` call.

`inspect_model` with `num_rows` returns live sample data — helpful for guessing
what values a column actually holds before writing a filter.

## Building a query

1. Start small — one field, no dims, tiny `limit`. Confirm the model works.
2. Add dimensions one at a time. Check row counts match what you expect.
3. Add filters. Measure-based filters route to HAVING automatically.
4. Add transforms last (`cumsum`, `change`, `time_shift`) — they need a time
   dimension.
5. If a result looks wrong, pass `show_sql=true` to see the generated SQL.
6. To preview without executing, use `dry_run=true`. For DB plans, `explain=true`.

## Connecting a new database

Two paths.

**Fast — auto-ingest:**

```text
1. create_datasource(name="mydb", type="postgres", ..., auto_ingest=true)
2. models_summary(datasource_name="mydb")              # see what ingestion produced
```

**Cautious — inspect first:**

```text
1. create_datasource(..., auto_ingest=false)
2. describe_datasource(name="mydb", schema_name="public")  # verify connection + list schemas + list tables (all in one call)
3. ingest_datasource_models(datasource_name="mydb", schema_name="public")
4. models_summary(datasource_name="mydb")
```

## Iterating on a model

- Missing a measure? `edit_model` with a `measures` upsert. Example spec:
  `{"name": "margin", "sql": "revenue - cost"}`.
- One-off concept for a single query? Use `ModelExtension` inside
  `source_model` instead of editing the model — see `help(topic='extending')`.
- Multi-stage result you'd like to reuse? `create_model` with a `query`
  parameter persists the computed shape as a new model.

## Common error decoder

| Error message fragment | What to check |
|------------------------|--------------|
| "Measure X not found" | `inspect_model` — spelled right, or on a joined model? |
| "Aggregation Y not allowed on measure X" | `allowed_aggregations` whitelist — see `help(topic='aggregations')`. |
| "Unresolvable dot path" | Missing `joins` entry or a typo in the target_model. |
| "Time dimension required" | Transform needs a time dim — set `time_dimensions` or `main_time_dimension`. |
| "Datasource 'X' not found" | `list_datasources`. |
| Database connection errors | `describe_datasource(name=...)` runs a test query and surfaces the error. |

## When to reach for help()

- Unfamiliar colon/aggregation/transform output in a tool arg doc →
  `help(topic='aggregations')` or `help(topic='transforms')`.
- Wondering why a filter didn't do what you expected → `help(topic='filters')`.
- Need to compose queries or bucket an aggregate → `help(topic='extending')`.

## See also

- `help(topic='queries')` — the anatomy of a single query.
- `help(topic='extending')` — multi-stage queries and inline model extension.
