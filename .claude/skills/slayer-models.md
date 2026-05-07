---
description: How to create and manage SLayer models and datasources. Use when defining models, dimensions, measures, or datasource configs.
---

# Model Management in SLayer

## Creating a Model (YAML)

```yaml
name: orders
sql_table: public.orders         # one of: sql_table, sql, or source_queries
data_source: my_postgres

# v2: a single `columns` list replaces v1's separate `dimensions` and `measures`.
# Whether a column is used as a group-by dimension or as a measure source is
# decided per query.
columns:
  - name: id
    sql: "id"
    type: number
    primary_key: true
  - name: status
    sql: "status"
    type: string
  - name: created_at
    sql: "created_at"
    type: time
  - name: amount
    sql: "amount"
    type: number
  - name: quantity
    sql: "quantity"
    type: number

default_time_dimension: created_at  # Optional: used by time-dependent formulas

# `measures` is a library of saved named formulas (not row-level columns).
# Each entry has the same shape as inline `SlayerQuery.measures`.
measures:
  - name: revenue
    formula: "amount:sum"
  - name: aov
    formula: "amount:sum / *:count"
```

Aggregation is specified at query time with **colon syntax**: `"amount:sum"`, `"amount:avg"`, `"*:count"`. A bare-name reference like `{"formula": "aov"}` resolves to the saved `ModelMeasure` formula on the model. Built-in aggregations: `sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `first`, `last`, `weighted_avg`, `median`, `percentile`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`. The two-column ones (`corr`, `covar_samp`, `covar_pop`) take the second column as a named param: `price:corr(other=quantity)`.

## Data Types

**Column types**: `string`, `number`, `boolean`, `time` (timestamp), `date`

## Joins

Models can declare LEFT JOIN relationships to other models:

```yaml
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
```

Enables cross-model measures (`customers.score:avg`), multi-hop dimensions (`customers.regions.name`), and transforms on joined measures (`cumsum(customers.score:avg)`). Auto-ingestion creates one direct join per FK on the source table. Multi-hop paths (e.g. `orders → customers → regions`) are resolved at query time by walking each intermediate model's own joins. Diamond joins (same table via different paths) are supported — each path gets a unique `__`-delimited alias (e.g., `customers__regions` vs `warehouses__regions`).

**Derived-on-derived chaining.** A `Column.sql` may reference another *derived* column — local same-model or via the join graph (single-dot `B.col` or `__`-delimited `B__C.col` path). The engine recursively inlines those references at query time, so you can write `A.ratio = "A.bar / B.foo_normalized"` even when `B.foo_normalized.sql = "foo_raw / 100.0"`. No need to inline derivations at every consumer site. Cycles raise a clear `ValueError` at enrichment time.

## Model Filters

Models can have always-applied WHERE filters: `filters: ["deleted_at IS NULL"]`. Only WHERE conditions on underlying table columns.

## Window functions in `Column.sql`

A column's `sql` may contain a window function (e.g. `row_number() over (order by mass desc)`). The column behaves like any other column when SELECTed; when used in a query `filters` entry, SLayer auto-promotes the predicate to a post-aggregation outer `WHERE`. Use the `Column.sql`-with-window pattern for non-standard window expressions; for top-N filtering prefer the inline `rank(<measure>) <= N` transform — dialect-portable and simpler. Raw `OVER (...)` SQL inside a `ModelMeasure.formula` is rejected at construction time with an actionable error.

## Source modes

A SlayerModel has exactly one source mode (mutually exclusive):
- `sql_table`: physical table.
- `sql`: explicit SQL subquery.
- `source_queries`: list of `SlayerQuery` stages — the model is **query-backed**.

## Query-backed models

`create_model_from_query(query, name, variables=None)` saves a query (or list of stages) as a query-backed model. It populates `model.source_queries`, optional `model.query_variables` defaults, and caches `model.columns` + `model.backing_query_sql` from a save-time dry-run (unresolved `{var}` placeholders default to `'0'`).

Saved query-backed models support two access patterns:
- **Run by name**: `engine.execute("monthly_revenue", variables={...})` runs the stored backing query.
- **Use as source_model**: `{"source_model": "monthly_revenue", ...}` treats the saved result as a model in another query.

Variable precedence (highest first): runtime kwarg > stage `.variables` > outer query `.variables` > `model.query_variables`.

You **cannot** supply `columns` or `backing_query_sql` when saving a query-backed model — they're engine-managed cache; the save path rejects them. Caches refresh **only on save paths**: `engine.save_model()` and `create_model_from_query(save=True)`. `engine.execute()` never writes to storage — even on stale or empty caches.

## SQL Expressions

- Use **bare column names** (e.g., `"amount"`) in dimension/measure SQL — SLayer qualifies them automatically
- For complex expressions, use the model name as table prefix (e.g., `"orders.amount * orders.quantity"`)
- **SQLite**: `json_extract(col, '$.path')` is preserved as the function-call form (not rewritten to `col -> '$.path'`, which would return the JSON-quoted form and silently break `CASE WHEN` / equality matches against bare-string literals). Use `->>` directly if you specifically want the SQLite scalar operator.

## Datasource Config

```yaml
name: my_postgres
type: postgres
host: ${DB_HOST}
port: 5432
database: ${DB_NAME}
username: ${DB_USER}       # "user" is also accepted
password: ${DB_PASSWORD}
```

`${VAR}` references are resolved from environment variables at read time.

## Auto-Ingestion

Connect to a DB and generate models automatically:

```python
from slayer.engine.ingestion import ingest_datasource
models = ingest_datasource(datasource=ds, schema="public")
```

Generates:
- One `Column` per non-joined database column (with `type` inferred). PK columns get `primary_key=True`. A column literally named `count` is renamed to `count_col` to avoid clashing with `*:count`.
- `*:count` is always available without an explicit definition; aggregation is picked per query via colon syntax (e.g., `amount:sum`).
- **Dynamic joins**: detects FK relationships and emits explicit join metadata (LEFT JOINs built at query time).
- FK columns are excluded from joinable models; ID-like columns (`*_id`, `*_key`) are usable as group-by columns only via the `primary_key` flag.

## MCP Incremental Editing

Via MCP, agents edit models through the unified `edit_model` tool:
- `edit_model(model_name="orders", description="Core orders table")`
- `edit_model(model_name="orders", columns=[{"name": "region", "sql": "region", "type": "string"}])` — upserts columns by name
- `edit_model(model_name="orders", measures=[{"name": "margin", "formula": "(amount - cost):sum"}])` — upserts named ModelMeasure formulas
- `edit_model(model_name="orders", delete_columns=["legacy_field"])`
- `edit_model(model_name="orders", delete_measures=["margin"])`

For query-backed models, `columns` and `backing_query_sql` are **engine-managed cache** — `edit_model` rejects user-supplied `columns` on a query-backed save with a clear error. Edit `source_queries` or `query_variables` instead.

## Storage Backends

- `YAMLStorage(base_dir="./data")` — models as YAML files in `data/models/`, datasources in `data/datasources/`
- `SQLiteStorage(db_path="./slayer.db")` — everything in a single SQLite file
- Both implement `StorageBackend` protocol: `save_model()`, `get_model()`, `list_models()`, `delete_model()`, same for datasources
- Use `resolve_storage("path")` factory for auto-detection (directory → YAML, .db → SQLite, URI schemes for custom backends)
