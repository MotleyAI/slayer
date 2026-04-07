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

default_time_dimension: created_at  # Optional: used by time-dependent formulas when no time_dimensions in query

measures:
  - name: count
    type: count                  # COUNT(*), no sql needed
  - name: revenue_sum
    sql: "amount"
    type: sum
  - name: revenue_avg
    sql: "amount"
    type: avg
```

## Data Types

**Dimension types**: `string`, `number`, `boolean`, `time` (timestamp), `date`

**Measure aggregation types**: `count`, `count_distinct`, `sum`, `avg`, `min`, `max`, `last` (most recent time bucket's value — for snapshot metrics like balances)

## Joins

Models can declare LEFT JOIN relationships to other models:

```yaml
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
```

Enables cross-model measures (`customers.avg_score`), multi-hop dimensions (`customers.regions.name`), and transforms on joined measures (`cumsum(customers.avg_score)`). Auto-generated from FKs during ingestion. Joins are auto-resolved transitively by walking the join graph. Diamond joins (same table via different paths) are supported — each path gets a unique `__`-delimited alias (e.g., `customers__regions` vs `warehouses__regions`).

## Model Filters

Models can have always-applied WHERE filters: `filters: ["deleted_at is None"]`. Only WHERE conditions on underlying table columns.

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

## Auto-Ingestion with Rollup Joins

Connect to a DB and generate denormalized models automatically:

```python
from slayer.engine.ingestion import ingest_datasource
models = ingest_datasource(datasource=ds, schema="public")
```

Generates:
- Dimensions for all columns
- `count` measure, `{col}_sum` and `{col}_avg` for numeric non-ID columns
- **Dynamic joins**: detects FK relationships, creates models with explicit join metadata (LEFT JOINs built at query time)
- Joined dimensions use full-path dotted naming (`customers.name`, `customers.regions.name`)
- FK columns are excluded; ID-like columns (`*_id`, `*_key`) skip sum/avg measures
- Count-distinct measures for each referenced table's PK (`customers.count`)

## MCP Incremental Editing

Via MCP, agents can edit models incrementally:
- `update_model(model_name="orders", description="Core orders table")` — update metadata without replacing the full definition
- `add_measures(model_name="orders", measures=[{"name": "total", "sql": "amount", "type": "sum"}])`
- `add_dimensions(model_name="orders", dimensions=[{"name": "region", "sql": "region", "type": "string"}])`
- `delete_measures_dimensions(model_name="orders", names=["total"])`

## Storage Backends

- `YAMLStorage(base_dir="./data")` — models as YAML files in `data/models/`, datasources in `data/datasources/`
- `SQLiteStorage(db_path="./slayer.db")` — everything in a single SQLite file
- Both implement `StorageBackend` protocol: `save_model()`, `get_model()`, `list_models()`, `delete_model()`, same for datasources
