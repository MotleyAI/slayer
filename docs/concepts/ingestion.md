# Auto-Ingestion

SLayer can introspect a database schema and automatically generate models with dimensions, measures, and **rollup-style denormalized joins**.

## How It Works

Ingestion runs in three steps:

### Step 1: FK Graph Analysis

SLayer introspects foreign key constraints and builds a directed dependency graph:

```
orders ──FK──→ customers ──FK──→ regions
```

The graph is validated to be acyclic (a `RollupGraphError` is raised if cycles are detected). For each table, SLayer computes the **transitive closure** — all tables reachable via FK chains.

### Step 2: Build Rollup SQL

For tables with FK references, SLayer generates a denormalized SQL query with LEFT JOINs:

```sql
SELECT
    public.orders.id AS id,
    public.orders.amount AS amount,
    public.orders.customer_id AS customer_id,
    public.customers.id AS customers__id,
    public.customers.name AS customers__name,
    public.regions.id AS regions__id,
    public.regions.name AS regions__name
FROM public.orders
LEFT JOIN public.customers ON public.orders.customer_id = public.customers.id
LEFT JOIN public.regions ON public.customers.region_id = public.regions.id
```

Tables with no FK references use their plain table name instead.

### Step 3: Introspect & Generate Model

SLayer introspects the column types from the rollup query (or plain table) and generates a model:

- **Dimensions** for every column (type inferred from SQL type)
- **`count` measure** (always)
- **`{col}_sum` and `{col}_avg` measures** for numeric columns that aren't IDs
- **Count-distinct measures**: `customers__count`, `regions__count` for each referenced table's PK

ID-like columns (`id`, `*_id`, `*_key`, `*_pk`, `*_fk`) are excluded from sum/avg generation since aggregating IDs is rarely meaningful. FK columns from referenced tables are excluded from rollup dimensions to avoid redundancy.

The same `_columns_to_model()` function handles both rollup and non-rollup tables — the only difference is whether the model uses `sql` (rollup query) or `sql_table` (plain table).

## Usage

### CLI

```bash
slayer ingest --datasource my_postgres --schema public --models-dir ./slayer_data
```

### Python

```python
from slayer.engine.ingestion import ingest_datasource

models = ingest_datasource(
    datasource=ds,
    schema="public",
    include_tables=["orders", "customers"],  # Optional filter
    exclude_tables=["migrations"],            # Optional exclusion
)
```

### MCP

```
create_datasource(name="mydb", type="postgres", ...)
ingest_datasource_models(datasource_name="mydb", schema_name="public")
```

### REST API

```bash
curl -X POST http://localhost:5143/ingest \
  -H "Content-Type: application/json" \
  -d '{"datasource": "my_postgres", "schema_name": "public"}'
```

## Querying Rolled-Up Models

After ingestion, you can query rolled-up dimensions directly:

```json
{
  "model": "orders",
  "fields": [{"formula": "count"}, {"formula": "amount_sum"}],
  "dimensions": [{"name": "customers__name"}]
}
```

Or transitively rolled-up dimensions:

```json
{
  "model": "orders",
  "fields": [{"formula": "count"}],
  "dimensions": [{"name": "regions__name"}]
}
```

## Cycle Handling

If the FK graph contains cycles (e.g., `A → B → A`), ingestion logs a warning and falls back to simple models without rollup joins.
