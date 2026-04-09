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

For tables with FK references, SLayer creates explicit join metadata and dotted dimensions. For example, `orders → customers → regions` produces:

- **Joins**: `orders → customers` (on `customer_id = id`), `customers → regions` (on `customers.region_id = id`)
- **Dotted dimensions**: `customers.name`, `customers.id`, `customers.regions.name`, `customers.regions.id`
- **Path-based SQL**: Dimension SQL uses `__`-delimited table aliases (e.g., `customers__regions.name`) to disambiguate joined tables

At query time, LEFT JOINs are constructed dynamically from this metadata — no SQL is baked into the model. Each joined table gets a path-based alias (e.g., `LEFT JOIN regions AS customers__regions`).

Tables with no FK references use their plain table name with no joins.

### Step 3: Introspect & Generate Model

SLayer introspects the column types and generates a model:

- **Dimensions** for every column (full-path dotted names for joined columns, e.g., `customers.name`, `customers.regions.name`)
- **`count` measure** (always)
- **Numeric non-ID columns**: `{col}_sum`, `{col}_avg`, `{col}_min`, `{col}_max`, `{col}_distinct`
- **Non-numeric non-ID columns**: `{col}_distinct` (COUNT DISTINCT), `{col}_count` (COUNT non-null)
- **Count-distinct measures**: `customers.count`, `customers.regions.count` for each referenced table's PK

ID-like columns (`id`, `*_id`, `*_key`, `*_pk`, `*_fk`) are excluded from sum/avg generation. FK columns from referenced tables are excluded from dimensions to avoid redundancy.

All models use `sql_table` (the source table) plus `joins` (structured metadata). JOINs are built dynamically at query time.

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
  "source_model": "orders",
  "fields": ["count", "amount_sum"],
  "dimensions": ["customers.name"]
}
```

Or transitively joined dimensions (using full path):

```json
{
  "source_model": "orders",
  "fields": ["count"],
  "dimensions": ["customers.regions.name"]
}
```

## Diamond Joins

When the same table is reachable via multiple FK paths (e.g., `orders → customers → regions` AND `orders → warehouses → regions`), ingestion creates separate joins for each path. Each path gets a unique alias:

- `customers.regions.name` → SQL alias `customers__regions`
- `warehouses.regions.name` → SQL alias `warehouses__regions`

This avoids table alias collisions and allows querying both paths simultaneously:

```json
{
  "source_model": "orders",
  "dimensions": [
    "customers.regions.name",
    "warehouses.regions.name"
  ],
  "fields": ["count"]
}
```

## Cycle Handling

If the FK graph contains cycles (e.g., `A → B → A`), ingestion logs a warning and falls back to simple models without rollup joins.
