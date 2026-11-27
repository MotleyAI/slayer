# Saved and Multistage Queries

## Query lists

Pass a list of queries to `execute()`. Earlier queries are named sub-queries, the last is the main query. Named queries can be referenced by `population` name or joined via `relationships`:

```json
[
  {
    "name": "monthly",
    "population": "orders",
    "metrics": ["count(orders.*)", "sum(orders.amount)"],
    "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]
  },
  {
    "population": "monthly",
    "metrics": ["count(monthly.*)"]
  }
]
```

This counts how many months exist in the monthly summary. The main query references `"monthly"` by name — a stage name overrides a same-named dataset only within its own query list, never inside stored definitions such as a dataset's stored relationships.

You can also relate named queries to datasets:

```json
[
  {
    "name": "customer_scores",
    "population": "customers",
    "dimensions": ["customers.id"],
    "metrics": ["avg(customers.score)"]
  },
  {
    "population": "orders",
    "extensions": [{"dataset": "orders", "relationships": [{"to": "customer_scores", "from_columns": ["customer_id"], "to_columns": ["id"]}]}],
    "metrics": ["count(orders.*)", "avg(customer_scores.score_avg)"],
    "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]
  }
]
```

The main query's `extensions` entry adds a relationship from `orders` to the named sub-query. Queries can also be saved as permanent datasets — see [Query-backed datasets](datasets.md#query-backed-datasets). `population` names a stored dataset or stage; an inline dataset is rejected — write a query-backed dataset's queries as named stages of the list instead.

Sibling stages can also reference each other — any non-final stage may use a *prior* named stage as `population`, as `relationships.to`, or in a reference such as `max(z_ev.n)`, so a query list forms a DAG, not just a chain; only the siblings a stage names enter its relationship graph. For example, two parallel rollups feeding a single final stage:

```json
[
  {
    "name": "customer_scores",
    "population": "customers",
    "dimensions": ["customers.id"],
    "metrics": ["avg(customers.score)"]
  },
  {
    "name": "tagged_orders",
    "population": "orders",
    "extensions": [{"dataset": "orders", "relationships": [{"to": "customer_scores", "from_columns": ["customer_id"], "to_columns": ["id"]}]}],
    "dimensions": ["customer_scores.score_avg"],
    "metrics": ["count(orders.*)"]
  },
  {
    "population": "tagged_orders",
    "metrics": ["max(tagged_orders._count)"]
  }
]
```

A grouped stage is unique on its dimensions, so a relationship onto a sibling stage that covers all of them (here `customer_scores` on `id`) is provably many-to-one and keeps exact per-dimension values. A joined stage's columns keep their flat names: a stage `s` with the dimension `customers.name` is read as `s.customers__name`, and the dotted `s.customers.name` is refused with that flat name as its suggestion.

**Stages inherit edges on pass-through keys.** A stage keeps every relationship of a dataset it reads, in either direction, whose key fields it outputs unchanged (a plain dimension, renamed by an alias or not; never a time bucket, a cast, a computed expression or a metric), so a later stage reaches the related dataset without declaring the relationship again:

```json
[
  {"name": "per_customer", "population": "orders", "dimensions": ["customer_id"], "metrics": [{"metric": "sum(amount)", "alias": "revenue"}]},
  {"population": "per_customer", "dimensions": ["customers.region"], "metrics": [{"metric": "sum(revenue)", "alias": "total"}]}
]
```

An inherited edge keeps its name, its target and (for a stored relationship) its join type; its stage side is "one" exactly when the stage's grain is covered by the key, and its target side keeps the source edge's arity, so `per_customer` above is one-to-one with `customers`. An aggregate over a dataset reached through a stage that filters, limits or reads a filtered population counts only the rows tied to a stage row, as through a filtered stored dataset. A relationship the consuming query declares on the stage wins over an inherited one: an identical one is applied once, a same-named one on other keys replaces it, and differently named edges on the same keys stay parallel.

**Order doesn't matter for runtime lists.** Stages can be submitted in any order — the engine auto-sorts them topologically so every stage appears after the siblings it references (a reference whose first segment names a relationship of the stage's population reads that relationship, not a same-named sibling). The **last entry** of the input list is always the entry point / DAG root (its result is what's returned); only the non-final entries are reordered. Cycles and self-references are rejected with a clear error naming the offending stages. A non-final stage may not reference the root (the root must be the dependency sink). Stages that aren't reachable from the root are accepted as utility sub-queries — they're silently dropped from the emitted SQL.

`SlayerDataset.source_queries` (stored, YAML-defined) follows the same rules: its stages are ordered by their references, so a stage may read one listed after it.

**Surface coverage.** Query lists work via every surface:

- Python SDK: `engine.execute(query=[...])` and `SlayerClient.query`/`query_sync`/`sql`/`sql_sync`/`explain`/`explain_sync`/`query_df` all accept `SlayerQuery | dict | list[SlayerQuery | dict] | str` (str = run-by-name).
- CLI: `slayer query @file.json` — accepts both a single object and a top-level list.
- MCP: the `query` tool with `query=[...]` (a list of stage objects); the same tool also accepts a dataset name or a single query object.
- REST: `POST /query` with body `{"queries": [...], "variables": {...}, "dry_run": ..., "explain": ...}` (the single-query body shape is also still accepted).

## Run a saved query by name

If a dataset is **query-backed** (created via `create_dataset_from_query` or saved with `source_queries`), you can run its stored backing query directly:

```python
await engine.execute("monthly_revenue", variables={"region": "EU"})
```

This loads the dataset, runs its `source_queries` stages with the merged variables, and returns the final-stage result. Calling `execute(str)` on a non-query-backed dataset raises a clear error directing the user to wrap it in a `SlayerQuery` instead.

REST equivalent: `POST /query` with `{"name": "<dataset>", "variables": {...}}`. Run-by-name also accepts `dry_run`, `explain` and `refine`; query-defining fields (`population`, `metrics`, `dimensions`, `filter`, `time_dimensions`, `order`, `limit`, `offset`) are not allowed next to `name` — put them in `refine`.

CLI equivalent: `slayer query <dataset_name> [--refine JSON|@file] [--variables k=v ...] [--dry-run] [--explain]` — when the positional argument doesn't look like JSON (doesn't start with `{` or `[`) and isn't a `@file` reference, it's interpreted as a dataset name.

MCP equivalent: `query(query="<dataset>", refine={...}, variables={...}, dry_run=True/False, explain=True/False)` — a bare dataset-name string is run-by-name execution (a non-query-backed name raises the same error as `execute(str)`).

### Refining a saved query

`refine` merges extra clauses into the saved query's **final stage**, which then runs exactly as if you had written that merged stage by hand:

```python
await engine.execute("monthly_revenue", refine={"dimensions": ["orders.region"]})  # extra group-by
await engine.execute("monthly_revenue", refine={"filter": "orders.amount >= 50"})  # ANDed with the saved filter
await engine.execute("monthly_revenue", refine={"order": [{"field": "revenue", "direction": "desc"}], "limit": 1})
```

- `dimensions` and `metrics` are appended; an entry identical to a saved one (`orders.region`, or its pre-2.0 spelling `region`) collapses, and a same-named entry that differs raises `RefinementConflictError`.
- `time_dimensions` merge per (field, granularity): a refinement may add a `date_range` or `label` the saved one lacks, but a different value conflicts — narrow a saved window with a filter on the time field instead.
- `filter` conditions are appended to the saved ones, skipping any the saved filter already has (the merged filter is then a list); `order`, `limit`, `offset`, `main_time_dimension`, `whole_periods_only`, `distinct_dimension_values`, `to_many_handling` and `time_zone` replace the saved value (`{"limit": null}` clears a saved limit, `{"time_zone": null}` a saved zone; a null `to_many_handling` keeps the saved mode).
- `population`, `name` and `variables` are not refinable; `refine` with a query object or list is an error.

For a multi-stage saved query only the final stage is refined, so it can use only what the earlier stages project: refining `[{"name": "per_customer", ...}, {"population": "per_customer", "metrics": [{"metric": "avg(per_customer.revenue)", "alias": "avg_revenue"}]}]` with `{"dimensions": ["per_customer.region"]}` averages per region, provided `per_customer` projects `region`.

Adding a second granularity of a saved time field (`{"time_dimensions": ["date_trunc('year', orders.ordered_at)"]}` on a monthly query) renames the saved key `orders.ordered_at` to `orders.ordered_at.month`, as in any query with two granularities. A saved query's population is fixed when it is saved (see [Population](queries.md#population)), so a refinement never changes it.
