# Workflow

How to chain the MCP tools (or CLI commands) for common tasks. Complements the
tool-by-tool documentation, which covers what each one does in isolation.

## Discovery — "what data is here?"

```text
1. list_datasources()                                        # pick a datasource
2. models_summary(datasource_name="mydb")                    # brief list of its models
3. inspect(reference="orders", entity_type="model")          # columns, measures, sample rows, SQL
```

`models_summary` gives one line per model with just names + descriptions of
its columns and measures and the list of joined models — pick the right one without the
weight of a full `inspect(entity_type="model")` call.

`inspect(reference="<model>", entity_type="model")` with `num_rows` returns live sample
data — helpful for guessing what values a column actually holds before writing a filter.

## Building a query

1. Start small — one field, no dims, tiny `limit`. Confirm the model works.
2. Add dimensions one at a time. Check row counts match what you expect.
3. Add filters. Measure-based filters route to HAVING automatically.
4. Add transforms last (`cumsum`, `change`, `time_shift`) — they need a time
   dimension.
5. If a result looks wrong, pass `show_sql=true` to see the generated SQL.
6. To preview without executing, pass `dry_run=true` as an MCP tool kwarg (or `engine.execute(query, dry_run=True)` in Python). For DB plans, `explain=true` works the same way. As of v3, these are execution kwargs — not fields on the query body itself.

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

- Missing a row-level field? `edit_model` with a `columns` upsert.
  Example: `columns=[{"name": "margin", "sql": "revenue - cost", "type": "number"}]`.
- Missing a saved aggregated formula? `edit_model` with a `measures` upsert.
  Example: `measures=[{"name": "avg_margin", "formula": "margin:sum / *:count"}]`.
- One-off concept for a single query? Use `ModelExtension` inside
  `source_model` instead of editing the model — see `memory:help.extending`.
- Multi-stage result you'd like to reuse? `create_model` with a `query`
  parameter persists the computed shape as a new model.

## Common error decoder

| Error message fragment | What to check |
|------------------------|--------------|
| "Measure X not found" | `inspect(reference="<model>", entity_type="model")` — spelled right, or on a joined model? |
| "Aggregation Y not allowed on measure X" | `allowed_aggregations` whitelist — see `memory:help.aggregations`. |
| "Unresolvable dot path" | Missing `joins` entry or a typo in the target_model. |
| "Time dimension required" | Transform needs a time dim — set `time_dimensions` or `main_time_dimension`. |
| "Datasource 'X' not found" | `list_datasources`. |
| Database connection errors | `describe_datasource(name=...)` runs a test query and surfaces the error. |

## When to reach for the concept topics

Each topic below is a help memory — read it with
`inspect(reference="memory:help.<topic>", entity_type="memory")`.

- Unfamiliar colon/aggregation/transform output in a tool arg doc →
  `memory:help.aggregations` or `memory:help.transforms`.
- Wondering why a filter didn't do what you expected → `memory:help.filters`.
- Need to compose queries or bucket an aggregate → `memory:help.extending`.

## See also

- `memory:help.queries` — the anatomy of a single query.
- `memory:help.extending` — multi-stage queries and inline model extension.
