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

### Measures — Column + ModelMeasure Split

dbt bakes aggregation into each measure (`agg: sum`). SLayer separates them — a row-level expression lives on a `Column`, and the aggregation is named on a `ModelMeasure` formula.

Each unique SQL expression among the dbt measures of a semantic model becomes one SLayer `Column`; each dbt measure becomes one `ModelMeasure` whose formula references that column with the colon aggregation:

```yaml
# dbt: {name: revenue, agg: sum, expr: amount}
# SLayer:
columns:
  - name: amount
    type: number
    format: {type: float}
measures:
  - name: revenue
    formula: amount:sum
```

When the dbt expression is a SQL fragment rather than a bare identifier (e.g. `amount * quantity`), the Column is named `<first_dbt_measure_name>_col`:

```yaml
# dbt: {name: line_total, agg: sum, expr: amount * quantity}
columns:
  - name: line_total_col
    sql: amount * quantity
    type: number
    format: {type: float}
measures:
  - name: line_total
    formula: line_total_col:sum
```

If the natural Column name would collide with a `ModelMeasure` name on the same model, the Column is suffixed with `_col`. The dbt measure's `label` and `description` are written verbatim onto the `ModelMeasure` only — never onto the underlying `Column`.

### Measure Consolidation

When multiple dbt measures share the same SQL expression but differ in aggregation, they collapse into a single SLayer Column; each dbt measure still becomes its own ModelMeasure:

```yaml
# dbt: {name: revenue_sum, agg: sum, expr: amount} + {name: revenue_avg, agg: average, expr: amount}
columns:
  - name: amount
    type: number
    format: {type: float}
measures:
  - name: revenue_sum
    formula: amount:sum
  - name: revenue_avg
    formula: amount:avg
```

### Metrics

dbt metrics fold into `ModelMeasure` formulas on their source semantic model. No separate query file is produced.

#### Simple metrics (with filter)

Converted to a `Column` carrying the filter (with no `allowed_aggregations` whitelist) plus a `ModelMeasure` referencing it:

```yaml
# dbt metric: loss_payment_amount (filter: has_loss_payment = 1)
columns:
  - name: loss_payment_amount_col
    sql: claim_amount
    type: number
    format: {type: float}
    filter: "has_loss_payment = 1"
measures:
  - name: loss_payment_amount
    formula: loss_payment_amount_col:sum
```

At query time, `loss_payment_amount` generates:

```sql
SUM(CASE WHEN has_loss_payment = 1 THEN claim_amount END)
```

#### Simple metrics (without filter)

Nothing to add — the underlying measure is already directly queryable.

#### Derived / ratio / cumulative metrics → `ModelMeasure`

All three fold into a `ModelMeasure` on the source semantic model. Inputs are referenced by **bare ModelMeasure name**, so the formula parser resolves them locally:

- **Derived**: `formula: "metric_a + metric_b"`
- **Ratio**: `formula: "numerator / denominator"`
- **Cumulative (unbounded)**: `formula: "cumsum(measure_name)"`

#### Unconverted metrics

Some dbt metrics cannot be expressed as a `ModelMeasure`. They are reported in `ConversionResult.unconverted_metrics` and printed with an `UNCONVERTED` tag. Categories:

- **Cumulative with window or grain_to_date**: SLayer's `cumsum` is unbounded.
- **Conversion metrics**: entity-based sequential event tracking is not supported.
- **Transform-name shadowing**: a dbt measure or metric named after a SLayer transform (`cumsum`, `lag`, `lead`, `change`, `change_pct`, `time_shift`, `rank`, `first`, `last`) is rejected — using it bare in a formula would shadow the transform.

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

## Output

The converter produces:
1. **Model YAML files** (or rows in SQLite storage) — one per dbt semantic model. Every metric folds into a `ModelMeasure` on its source model.
2. **Console report** — summary of models imported, unconverted metrics, and warnings.

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
  --include-hidden-models   Also import regular dbt models (not wrapped by a
                            semantic_model) as hidden SLayer models via SQL
                            introspection. Requires the `dbt` extra.
```

## Hard Failures

The converter raises `DbtConversionError` (and aborts) when a dbt semantic model defines both a dimension and a measure with the same name. SLayer columns and named measures share a single namespace per model, so the names must be disjoint — rename one side in the dbt project.
