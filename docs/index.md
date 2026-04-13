# SLayer — Semantic Layer for AI Agents

<p align="center">
  <img src="images/slayer-hero.png" alt="SLayer — AI agent operating a semantic layer" width="700">
</p>

A lightweight, open-source semantic layer by [MotleyAI](https://github.com/motleyai). Agents describe what data they want — measures, dimensions, filters — and SLayer generates the SQL.

[GitHub](https://github.com/MotleyAI/slayer) | [PyPI](https://pypi.org/project/motley-slayer/)

## Why?

When AI agents write raw SQL, they hallucinate column names, get joins wrong, and produce metrics that drift between queries. Existing semantic layers (Cube, dbt metrics) were built for dashboards — heavy infrastructure, slow model refresh cycles, and not enough flexibility for ad-hoc agent queries.

SLayer is different: models are editable at runtime, aggregation is chosen at query time, and there's no build step.

## What it looks like

Given an `orders` [model](concepts/models.md) with a `revenue` measure and joins to `customers` and `regions`:

```json
{
  "source_model": "orders",
  "fields": [
    "revenue:sum",
    {"formula": "change_pct(revenue:sum)", "name": "mom_growth"},
    {"formula": "revenue:sum / time_shift(revenue:sum, -1, 'year') - 1", "name": "yoy_growth"},
    "customers.score:last(changed_at)"
  ],
  "dimensions": ["customers.regions.name"],
  "time_dimensions": [{
    "dimension": "created_at",
    "granularity": "month",
    "date_range": ["2025-01-01", "2025-12-31"]
  }],
  "filters": ["status = 'completed'", "change(revenue:sum) > 0"],
  "order": [{"name": "revenue_sum", "direction": "desc"}]
}
```

One query, and SLayer handles:

- **`revenue:sum`** — aggregation is chosen at query time, not baked into the measure definition. The same `revenue` measure works with `sum`, `avg`, `median`, `weighted_avg`, or [any custom aggregation](examples/07_aggregations/aggregations.md).
- **`change_pct(revenue:sum)`** — month-over-month growth as a [transform](examples/04_time/time.md). SLayer generates the necessary window query. Other built-in transforms: `cumsum`, `change`, `time_shift`, `rank`, `lag`, `lead` — all nestable (`"change(cumsum(revenue:sum))"` works).
- **`revenue:sum / time_shift(revenue:sum, -1, 'year') - 1`** — arithmetic on aggregated measures. `time_shift` runs a separate time-shifted sub-query and joins it back by all dimensions; dividing by it gives year-over-year growth. Standard operator precedence applies.
- **`customers.score:last(changed_at)`** — a measure from a [joined model](examples/05_joined_measures/joined_measures.md), resolved by walking the [join graph](examples/05_joins/joins.md). `last` is an aggregation that picks the latest record's value — `changed_at` tells it which column defines "latest."
- **`customers.regions.name`** — a multi-hop dimension: SLayer traces `orders → customers → regions` and builds the joins automatically.
- **`change(revenue:sum) > 0`** — filtering on a computed transform. SLayer computes the transform first as a hidden field, then applies the filter on the outer query.

## What SLayer does

- **[Auto-ingestion](concepts/ingestion.md)** — Point it at a database, it introspects the schema, detects foreign keys, and generates models with joins. No manual YAML needed to get started ([tutorial](examples/03_auto_ingest/auto_ingest.md)).
- **Aggregation at query time** — Measures are expressions, not pre-baked aggregates. `"revenue:sum"`, `"revenue:median"`, `"price:weighted_avg(weight=quantity)"`. Built-in and [custom aggregations](examples/07_aggregations/aggregations.md) with parameters.
- **Composable transforms** — `cumsum`, `change`, `change_pct`, `time_shift`, `rank`, `lag`, `lead` — all nestable: `"change(cumsum(revenue:sum))"` just works ([tutorial](examples/04_time/time.md)).
- **Cross-model measures** — Query measures from [joined models](examples/05_joined_measures/joined_measures.md) with dot syntax: `"customers.score:avg"`. Joins are auto-resolved by walking the model graph ([tutorial](examples/05_joins/joins.md)).
- **[Multistage queries](examples/06_multistage_queries/multistage_queries.md)** — Use one query as the source for another, or save any query as a permanent model.
- **Runtime model editing** — Add measures, dimensions, and joins through any interface. No rebuild, no restart.
- **Broad database support** — Integration-tested against Postgres, MySQL, ClickHouse, DuckDB, and SQLite. Others via sqlglot.

## Get started
- **[MCP](getting-started/mcp.md)** — for AI agents (Claude Code, Cursor, etc.)
- **[CLI](getting-started/cli.md)** — query from the terminal, manage models and datasources
- **[REST API](getting-started/rest-api.md)** — build apps in any language
- **[Python SDK](getting-started/python.md)** — embed SLayer directly, no server needed

## Under the hood

```
Agent --> MCP / REST API / Python SDK
              |
         SlayerQuery (model, fields, dimensions, filters)
              |
         SlayerQueryEngine (resolves model definitions from storage)
              |
         EnrichedQuery (resolved SQL expressions, model metadata)
              |
         SQLGenerator (sqlglot AST --> dialect-aware SQL)
              |
         SlayerSQLClient (SQLAlchemy --> database)
              |
         SlayerResponse (data, columns, sql)
```

**SlayerQuery** is what the user sends — names and references, no SQL. **EnrichedQuery** is the engine-internal form where every measure and dimension carries its resolved SQL, aggregation, and model context. New datasource adapters only need to translate EnrichedQuery.

Full concept docs: [Models](concepts/models.md) | [Queries](concepts/queries.md) | [Formulas](concepts/formulas.md)
