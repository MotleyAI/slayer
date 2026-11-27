# SLayer — Semantic Layer for AI Agents

<p align="center">
  <img src="images/slayer-hero.png" alt="SLayer — AI agent operating a semantic layer" width="700">
</p>

A lightweight, open-source semantic layer by [MotleyAI](https://github.com/motleyai). Agents describe what data they want — metrics, dimensions, filters — and SLayer generates the SQL.

[GitHub](https://github.com/MotleyAI/slayer) | [PyPI](https://pypi.org/project/motley-slayer/)

**Upgrading from 1.x?** See [Upgrading to 2.0](upgrading/2.0.md) for every breaking change and how to migrate.

## Why?

When AI agents write raw SQL, they can get joins wrong, and produce metrics that drift between queries. 

Existing semantic layers (Cube, dbt semantic layer) were built for dashboards — heavy infrastructure, slow model refresh cycles, and not enough flexibility for ad-hoc agent queries.

SLayer is different: datasets are editable at runtime, aggregation is chosen at query time, and there's no build step.

## What it looks like

Given the demo's `orders` [dataset](concepts/datasets.md), with an `order_total` field and a relationship to `stores` and on to `regions`:

```json
{
  "population": "orders",
  "metrics": [
    "sum(orders.order_total)",
    {"metric": "change_pct(sum(orders.order_total))", "alias": "mom_growth"},
    {"metric": "sum(orders.order_total) / time_shift(sum(orders.order_total), -1, 'year') - 1", "alias": "yoy_growth"},
    "last(stores.name, stores.opened_at)"
  ],
  "dimensions": ["stores.regions.name"],
  "time_dimensions": [{
    "field": "orders.ordered_at",
    "granularity": "month",
    "date_range": "last 12 months"
  }],
  "filter": ["orders.status = 'completed'", "change(sum(orders.order_total)) > 0"],
  "order": [{"field": "orders.order_total_sum", "direction": "desc"}]
}
```

One query, and SLayer handles:

- **`sum(orders.order_total)`** — aggregation is chosen at query time, not baked into the field definition. The same `order_total` field works with `sum`, `avg`, `median`, `weighted_avg`, or [any custom aggregation](examples/07_aggregations/aggregations.md). Add `partition_by=` for a coarser grain repeated across rows — `sum(orders.order_total) / sum(orders.order_total, partition_by=stores.regions.name)` is share-of-parent.
- **`change_pct(sum(orders.order_total))`** — month-over-month growth as a [transform](examples/04_time/time.md). SLayer generates the necessary window query. Other built-in transforms: `cumsum`, `change`, `time_shift`, `rank` / `percent_rank` / `dense_rank` / `ntile`, `lag`, `lead` — all nestable (`"change(cumsum(sum(orders.order_total)))"` works).
- **`sum(orders.order_total) / time_shift(sum(orders.order_total), -1, 'year') - 1`** — arithmetic on aggregated metrics. `time_shift` runs a separate time-shifted sub-query and joins it back by all dimensions; dividing by it gives year-over-year growth. Standard operator precedence applies.
- **`last(stores.name, stores.opened_at)`** — a metric from a [joined dataset](examples/05_joined_metrics/joined_metrics.md), resolved by walking the [relationship graph](examples/05_relationships/relationships.md). `last` is an aggregation that picks the latest record's value — `stores.opened_at` tells it which field defines "latest", so this is the region's newest store.
- **`stores.regions.name`** — a multi-hop dimension: SLayer traces `orders → stores → regions` and builds the joins automatically.
- **`change(sum(orders.order_total)) > 0`** — filtering on a computed transform. SLayer computes the transform first as a hidden field, then applies the filter on the outer query.

## What SLayer does

- **[Auto-ingestion](concepts/ingestion.md)** — Point it at a database, it introspects the schema, detects foreign keys, and generates datasets with relationships. No manual YAML needed to get started ([tutorial](examples/03_auto_ingest/auto_ingest.md)). Re-run the same idempotent pass on every server boot with `slayer serve --ingest-on-startup` / `slayer mcp --ingest-on-startup`.
- **Aggregation at query time** — Metrics are expressions, not pre-baked aggregates. `"sum(orders.order_total)"`, `"median(orders.order_total)"`, `"weighted_avg(orders.order_total, weight=orders.subtotal)"`. Built-in and [custom aggregations](examples/07_aggregations/aggregations.md) with parameters.
- **Composable transforms** — `cumsum`, `change`, `change_pct`, `time_shift`, `rank` / `percent_rank` / `dense_rank` / `ntile`, `lag`, `lead` — all nestable: `"change(cumsum(sum(orders.order_total)))"` just works ([tutorial](examples/04_time/time.md)).
- **Cross-dataset metrics** — Query metrics from [joined datasets](examples/05_joined_metrics/joined_metrics.md) with dot syntax: `"avg(stores.tax_rate)"`. Relationships are auto-resolved by walking the dataset graph ([tutorial](examples/05_relationships/relationships.md)).
- **[Multistage queries](examples/06_multistage_queries/multistage_queries.md)** — Use one query as the source for another, or save any query as a permanent dataset.
- **Runtime dataset editing** — Add metrics, dimensions, and relationships through any interface. No rebuild, no restart.
- **[Memories + semantic search](concepts/search.md)** — Persist free-form learnings tagged with canonical entities (`<datasource>.<dataset>.<field>`) and retrieve them alongside dataset / field discovery hits via a single `search` call. Three retrieval channels (BM25 over memory tags + Tantivy full-text + optional dense embeddings) are RRF-fused into one flat ranked list. Optional graph pre-filter via `cypher_filter` ([memories concept](concepts/memories.md)).
- **Broad database support** — Integration-tested against Postgres, MySQL, ClickHouse, DuckDB, and SQLite. Others via sqlglot.

## Get started
- **[MCP](getting-started/mcp.md)** — for AI agents (Claude Code, Cursor, etc.)
- **[CLI](getting-started/cli.md)** — query from the terminal, manage datasets and datasources
- **[REST API](getting-started/rest-api.md)** — build apps in any language
- **[Python SDK](getting-started/python.md)** — embed SLayer directly, no server needed

## Under the hood

<!-- doc-check: not-a-query -->
```
Agent --> MCP / REST API / Python SDK
              |
         SlayerQuery (population, metrics, dimensions, filter)
              |
         SlayerQueryEngine (resolves dataset definitions from storage)
              |
         PlannedQuery (typed value keys, slots, relationship paths, phases)
              |
         SQLGenerator (sqlglot AST --> dialect-aware SQL)
              |
         SlayerSQLClient (SQLAlchemy --> database)
              |
         SlayerResponse (data, columns, sql)
```

**SlayerQuery** is what the user sends — names and references, no SQL. **PlannedQuery** is the engine-internal form: every metric and dimension is a typed *value key* interned into a slot, carrying its resolved expression, aggregation, relationship path, and phase. New datasource adapters only need to translate PlannedQuery.

Full concept docs: [Datasets](concepts/datasets.md) | [Queries](concepts/queries.md) | [Expressions](concepts/expressions.md)
