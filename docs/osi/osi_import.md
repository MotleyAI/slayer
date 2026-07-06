# Importing OSI (Open Semantic Interchange) Configs

SLayer can import [Open Semantic Interchange](https://github.com/open-semantic-interchange/OSI) (OSI) semantic-model configs and convert them into SLayer models. OSI is a vendor-neutral YAML/JSON standard for semantic models, datasets, relationships, and metrics.

## Quick Start

```bash
slayer import-osi ./osi_configs --datasource my_postgres --storage ./slayer_data
```

This reads every `.yaml`/`.yml`/`.json` file in the path (a single file or a directory), converts each OSI dataset into a SLayer model, and saves the models to storage. A **reachable datasource is required** — column data types come from live table introspection (OSI carries no column types), so the importer connects to `--datasource` and overlays OSI's semantic metadata on top.

Spec versions `1.0`, `0.1.0`, `0.1.1`, and `0.2.0.dev0` are all accepted (they are structurally identical); an unknown version is warned about but still parsed.

## What Gets Converted

| OSI construct | SLayer target |
|---|---|
| `semantic_model[].datasets[]` | one `SlayerModel` each (name = dataset name) |
| dataset `source` (`db.schema.table`) | `sql_table` + `schema`; a query source → `sql` mode |
| dataset `fields[]` | `Column`s (real types from introspection) |
| field `expression` (bare) | overlaid onto the introspected column |
| field `expression` (derived, e.g. `UPPER(x)`) | a derived `Column` with `sql` set |
| field `dimension.is_time` | column typed temporal; sets `default_time_dimension` |
| dataset `primary_key` | `Column.primary_key = true` |
| `relationships[]` (`from` → `to`) | a LEFT `ModelJoin` on the `from` model |
| `metrics[]` (raw SQL aggregation) | a `ModelMeasure` formula |
| `ai_context` (instructions + synonyms) | entity `description` + `meta["osi_ai_context"]` |
| `unique_keys` / `custom_extensions` | model/column `meta` |

### Metrics

An OSI metric holds a raw SQL aggregation expression. SLayer parses it into colon-syntax formulas:

- `SUM(amount)` → `amount:sum`, `COUNT(*)` → `*:count`, `COUNT(DISTINCT id)` → `id:count_distinct`
- arithmetic + constants + scalar functions pass through: `SUM(a) / NULLIF(COUNT(*), 0)` → `a:sum / nullif(*:count, 0)`
- a non-bare aggregate operand is materialized as a hidden derived column: `SUM(quantity * amount)` → a hidden column `quantity * amount` plus `<col>:sum`
- `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY x)` → `x:percentile(p=0.9)` (`0.5` → `x:median`)

A metric that references columns from more than one dataset is attached to an **anchor** model — the model that reaches every referenced dataset over the relationship-derived joins (chosen via the same logic as [`recommend_root_model`](../concepts/queries.md#choosing-a-root-model)). Cross-dataset columns are emitted as join-qualified dotted refs (`customers.regions.population:sum`).

## Dialect Selection

OSI expressions are multi-dialect. `--dialect` (default `ANSI_SQL`) picks which one to read; when it is absent, the importer falls back to another SQL-compatible dialect (`SNOWFLAKE`, `DATABRICKS`). An expression available only in a non-SQL dialect (`MDX`, `MAQL`, `TABLEAU`) is clean-failed to the report.

## Clean-Fail Report

Anything that cannot be expressed exactly is reported (never silently dropped) and the raw construct is preserved in `meta`. Examples: a metric with a `CASE`/window expression, a relationship with mismatched key lengths or an unknown target, a dataset whose table cannot be introspected, an illegal name (containing `.`/`:`), a metric whose referenced datasets are not connected by any join path, or an orphan `COUNT(*)` (a column-less metric in a semantic model with no unique fact table, so its grain is ambiguous). The CLI prints a grouped report and a `models / unconverted / dropped` tally at the end.

A query source is introspected live (a `LIMIT 0` / cursor-metadata probe) for real column types, the same way table sources are — no connection-less heuristics.
