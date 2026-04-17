---
description: How to create and manage SLayer models and datasources. Use when defining models, dimensions, measures, or datasource configs.
---

# Model Management in SLayer

## Creating a Model (YAML)

```yaml
name: orders
sql_table: public.orders         # or sql: "SELECT * FROM ..."
data_source: my_postgres

dimensions:
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

default_time_dimension: created_at  # Optional: used by time-dependent formulas

measures:
  - name: revenue
    sql: "amount"          # Row-level expression — aggregation chosen at query time
  - name: quantity
    sql: "quantity"
```

Measures are **row-level expressions** — no aggregation type in the definition. Aggregation is specified at query time with colon syntax: `"revenue:sum"`, `"revenue:avg"`, `"*:count"`.

## Data Types

**Dimension types**: `string`, `number`, `boolean`, `time` (timestamp), `date`

## Joins

Models can declare LEFT JOIN relationships to other models:

```yaml
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
```

Enables cross-model measures (`customers.score:avg`), multi-hop dimensions (`customers.regions.name`), and transforms on joined measures (`cumsum(customers.score:avg)`). Auto-ingestion creates one direct join per FK on the source table. Multi-hop paths (e.g. `orders → customers → regions`) are resolved at query time by walking each intermediate model's own joins. Diamond joins (same table via different paths) are supported — each path gets a unique `__`-delimited alias (e.g., `customers__regions` vs `warehouses__regions`).

## Model Filters

Models can have always-applied WHERE filters: `filters: ["deleted_at IS NULL"]`. Only WHERE conditions on underlying table columns.

## Creating Models from Queries

`create_model_from_query(query, name)` saves a query's SQL as a permanent model with auto-introspected dimensions and measures.

## SQL Expressions

- Use **bare column names** (e.g., `"amount"`) in dimension/measure SQL — SLayer qualifies them automatically
- For complex expressions, use the model name as table prefix (e.g., `"orders.amount * orders.quantity"`)

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
- Dimensions for all columns
- One measure per non-ID column (e.g., `{name: "amount", sql: "amount"}`) — aggregation chosen at query time
- `*:count` is always available without a measure definition
- **Dynamic joins**: detects FK relationships, creates models with explicit join metadata (LEFT JOINs built at query time)
- FK columns are excluded; ID-like columns (`*_id`, `*_key`) are dimensions only

## MCP Incremental Editing

Via MCP, agents can edit models incrementally:
- `update_model(model_name="orders", description="Core orders table")`
- `add_measures(model_name="orders", measures=[{"name": "margin", "sql": "amount - cost"}])`
- `add_dimensions(model_name="orders", dimensions=[{"name": "region", "sql": "region", "type": "string"}])`
- `delete_measures_dimensions(model_name="orders", names=["margin"])`

## Storage Backends

- `YAMLStorage(base_dir="./data")` — models as YAML files in `data/models/`, datasources in `data/datasources/`
- `SQLiteStorage(db_path="./slayer.db")` — everything in a single SQLite file
- Both implement `StorageBackend` protocol: `save_model()`, `get_model()`, `list_models()`, `delete_model()`, same for datasources
- Use `resolve_storage("path")` factory for auto-detection (directory → YAML, .db → SQLite, URI schemes for custom backends)
