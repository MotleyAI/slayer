# Importing dbt Semantic Layer Definitions

SLayer can import dbt Semantic Layer definitions (semantic models and metrics) and convert them to SLayer models. This document describes the conversion process, output format, and limitations.

For a comparison of SLayer vs dbt expressiveness, see [SLayer vs dbt](slayer_vs_dbt.md).

## Quick Start

```bash
slayer import-dbt ./my_dbt_project --datasource my_postgres --storage ./slayer_data
```

This reads all `.yaml`/`.yml` files in the project directory, extracts `semantic_models` and `metrics`, and writes SLayer model YAML files to the storage directory.

## What Gets Converted

### Semantic Models (1:1 mapping)

Each dbt semantic model becomes one SLayer model:

| dbt field | SLayer field |
|-----------|-------------|
| `name` | `name` |
| `model: ref('x')` | `sql_table: x` |
| `description` | `description` |
| `defaults.agg_time_dimension` | `default_time_dimension` |

### Dimensions

- `categorical` → `type: string`
- `time` → `type: time`
- `expr` → `sql` (omitted if same as name)
- `label` and `description` preserved directly

### Entities → Primary Keys + Joins

dbt entities are converted to two things:
1. **Primary/unique entities** → a dimension with `primary_key: true`
2. **Foreign entities** → a `ModelJoin` to the model that owns the matching primary entity

The converter builds an entity registry by scanning all models, then resolves foreign entity references to explicit SLayer joins. For example:

```yaml
# dbt: orders model has foreign entity customer_id
# dbt: customers model has primary entity customer_id (expr: id)

# SLayer output: orders model gets a join
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
```

### Measures — Aggregation at Query Time

dbt bakes aggregation into each measure (`agg: sum`). SLayer separates them — measures are row-level expressions, aggregation is specified at query time.

The converter stores the dbt aggregation in `allowed_aggregations`:

```yaml
# dbt: {name: revenue, agg: sum, expr: amount}
# SLayer: {name: revenue, sql: amount, allowed_aggregations: [sum]}
```

Use `--no-strict-aggregations` to allow all aggregation types on imported measures.

### Measure Consolidation

When multiple dbt measures share the same SQL expression but differ only in aggregation type, they are consolidated into a single SLayer measure with multiple `allowed_aggregations`. The original name:aggregation pairs are listed in the description:

```yaml
# dbt: {name: revenue_sum, agg: sum, expr: amount} + {name: revenue_avg, agg: average, expr: amount}
# SLayer:
- name: amount
  sql: amount
  allowed_aggregations: [sum, avg]
  description: "dbt measures: revenue_sum (sum), revenue_avg (average)"
```

### Metrics

dbt metrics are handled differently depending on their type:

#### Simple metrics (with filter)

Converted to a **filtered measure** on the base model. The dbt Jinja filter syntax is converted to a SLayer filter string:

```yaml
# dbt metric: loss_payment_amount (filter: has_loss_payment = 1)
# SLayer measure added to the claim_amount model:
- name: loss_payment_amount
  sql: claim_amount
  filter: "has_loss_payment = 1"
  allowed_aggregations: [sum]
```

At query time, `loss_payment_amount:sum` generates:
```sql
SUM(CASE WHEN has_loss_payment = 1 THEN claim_amount END)
```

#### Simple metrics (without filter)

Nothing to add — the underlying measure is already directly queryable in SLayer.

#### Derived, ratio, and cumulative metrics → `queries.yaml`

These don't map to model-level constructs. They are converted to SlayerQuery definitions and written to a separate `queries.yaml` file:

- **Derived**: `{"formula": "metric_a:sum + metric_b:sum", "name": "combined"}`
- **Ratio**: `{"formula": "numerator:sum / denominator:sum", "name": "ratio"}`
- **Cumulative (unbounded)**: `{"formula": "cumsum(measure:agg)", "name": "running_total"}`

These can be executed via the SLayer API/MCP or used as templates for building queries.

#### Unsupported metric types

- **Cumulative with window or grain_to_date**: warning emitted (SLayer's `cumsum` is unbounded)
- **Conversion metrics**: warning emitted (entity-based sequential event tracking not supported)

## Filter Syntax Conversion

dbt uses Jinja templates for filter references:

```jinja
{{ Dimension('claim_amount__has_loss_payment') }} = 1
{{ TimeDimension('metric_time', 'day') }} >= '2024-01-01'
{{ Entity('customer_id') }} IS NOT NULL
```

The converter resolves these to plain SLayer filter strings:

- `Dimension('entity__dim')` → `dim` (if entity is the model's own primary) or `target_model.dim` (if entity is foreign)
- `TimeDimension('name', 'grain')` → `name` (granularity is query-time in SLayer)
- `Entity('name')` → the entity's SQL expression column name

## Output Files

The converter produces:
1. **Model YAML files** in `models/` — one per dbt semantic model
2. **`queries.yaml`** — SlayerQuery definitions for derived/ratio/cumulative metrics (if any)
3. **Console report** — summary of models imported, queries generated, and warnings

## Regular dbt Models (Hidden Import)

By default, `import-dbt` ingests only dbt models that are wrapped by a `semantic_model`. Every other dbt model — staging tables, marts that never got a semantic layer, raw sources materialized as models — stays invisible to SLayer.

Pass `--include-hidden-models` to change that. SLayer will use dbt's own parser to enumerate every regular model in the project, skip the ones already represented by a `semantic_model`, introspect the materialized tables via SQL, and register each one as a **hidden** SLayer model (`hidden: true`).

Hidden models are queryable by name via the REST API, MCP, and SQL engine but are excluded from discovery surfaces (`slayer models list`, MCP `datasource_summary`, and the hidden dimensions/measures of `GET /models/{name}`). Agents looking for what's available see only the curated semantic layer; agents that already know a table's name can still reach it.

### Prerequisites

Install the optional `dbt` extra so SLayer can invoke `dbt parse` and read `target/manifest.json`:

```bash
pip install 'motley-slayer[dbt]'
# or, with Poetry:
poetry install -E dbt
```

The datasource passed to `--datasource` must be able to open a live connection — SQL introspection reads actual column types from the warehouse.

### Usage

```bash
slayer import-dbt ./my_dbt_project \
  --datasource my_warehouse \
  --include-hidden-models
```

Each hidden model is printed with a `[hidden]` marker. The final line summarises how many are visible vs hidden.

### Metadata Carried Over

When the dbt manifest supplies column-level documentation, it is overlaid onto the introspected dimensions/measures:

- Model `description` → SlayerModel `description`
- Column `description` → matching `Dimension.description` or `Measure.description` (only fills blanks — introspected values are not overwritten)

Columns without a dbt description fall back to whatever SQL introspection produced.

### Failure Semantics

Hidden-model import is deliberately best-effort:

- **dbt-core not installed**: logged once, the regular-model pass is skipped entirely, semantic-model import still runs.
- **Table not materialized yet / connection error**: one warning per failed model, SLayer keeps going for the rest.
- **Name collision**: if a regular model shares a name with a semantic model, the regular one is skipped — the visible semantic model wins.

### Toggling Hidden Later

The `hidden` flag lives on each SlayerModel, Dimension, and Measure. You can flip it with the MCP `edit_model` tool:

```json
{"model_name": "raw_events", "hidden": false}
```

This lets you promote a silently imported table to first-class visibility once you have decided it belongs in the semantic layer.

## Limitations

- **Non-additive dimensions** (`non_additive_dimension`): not converted. Use `balance:last(time_col)` for snapshot measures, or multi-stage queries for complex patterns.
- **Rolling-window cumulative**: SLayer's `cumsum()` is unbounded; trailing windows are not supported.
- **Grain-to-date cumulative**: not supported.
- **Conversion metrics**: not supported.
- **Per-measure `agg_time_dimension`**: SLayer has one `default_time_dimension` per model. Specify at query time.
- **dbt `ref()` resolution**: the converter extracts the model name from `ref('name')` but does not resolve the full dbt DAG. The `sql_table` is set to the bare model name — the actual table/view name in the database may differ if dbt uses custom schemas or aliases.

## CLI Reference

```text
slayer import-dbt <dbt_project_path> [options]

Arguments:
  dbt_project_path          Path to dbt project root or models directory

Options:
  --datasource NAME         SLayer datasource name for imported models (required)
  --storage PATH            Storage directory for output (default: ./slayer_data)
  --no-strict-aggregations  Allow all aggregation types (don't restrict to dbt's defined agg)
  --include-hidden-models   Also import regular dbt models (not wrapped by a
                            semantic_model) as hidden SLayer models via SQL
                            introspection. Requires the `dbt` extra.
```
