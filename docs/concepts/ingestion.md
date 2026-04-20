# Auto-Ingestion

SLayer can introspect a database schema and automatically generate models with dimensions, measures, and **direct FK-based joins**.

SLayer has three ingestion paths:

1. **Auto-ingest** (this page) — introspect a live database and generate visible models with direct FK-based joins.
2. **dbt semantic layer import** — convert `semantic_models` and `metrics` from a dbt project into visible SLayer models. See [dbt Import](../dbt/dbt_import.md).
3. **Hidden dbt-model import** — the `--include-hidden-models` variant of `import-dbt` adds every regular dbt model that isn't wrapped by a `semantic_model` as a **hidden** SlayerModel built via SQL introspection. Hidden models stay out of discovery/listing endpoints but remain queryable by name. See [Regular dbt Models (Hidden Import)](../dbt/dbt_import.md#regular-dbt-models-hidden-import).

## How It Works

Ingestion runs in three steps:

### Step 1: FK Graph Analysis

SLayer introspects foreign key constraints and builds a directed dependency graph:

```
orders ──FK──→ customers ──FK──→ regions
```

The graph is validated to be acyclic (a `RollupGraphError` is raised if cycles are detected). For each table, SLayer computes the **transitive closure** — all tables reachable via FK chains — to determine which columns to introspect for dotted dimensions. The transitive closure is used only for column discovery, not for generating joins (see Step 2).

### Step 2: Build Direct Joins

Each model gets one join entry per foreign key **on its own table** — never multi-hop joins. For example, given `orders → customers → regions`:

- The `orders` model gets a single join: `orders → customers` (on `customer_id = id`)
- The `customers` model gets a single join: `customers → regions` (on `region_id = id`)
- The `orders` model does **not** get a baked-in `orders → regions` join

Each join stores the source/target column pair from the table's own FK. Multi-hop paths (e.g., `customers.regions.name` queried from `orders`) are resolved at query time by walking each intermediate model's joins.

- **Dotted dimensions**: `customers.name`, `customers.id`, `customers.regions.name`, `customers.regions.id` (the transitive closure from Step 1 tells ingestion which columns to create)
- **Path-based SQL**: At query time, dimension SQL uses `__`-delimited table aliases (e.g., `customers__regions.name`) to disambiguate joined tables. Each joined table gets a path-based alias (e.g., `LEFT JOIN regions AS customers__regions`).

Tables with no FK references use their plain table name with no joins.

### Step 3: Introspect & Generate Model

SLayer introspects the column types and generates a model:

- **Dimensions** for every column (full-path dotted names for joined columns, e.g., `customers.name`, `customers.regions.name`)
- **One measure per non-ID column** (e.g., `{name: "amount", sql: "amount"}`) — aggregation is specified at query time via colon syntax (`amount:sum`, `amount:avg`, etc.)
- **`*:count`** is always available — no explicit count measure is needed
- **Count-distinct measures**: `customers.*:count_distinct`, `customers.regions.*:count_distinct` for each referenced table's PK

ID-like columns (`id`, `*_id`, `*_key`, `*_pk`, `*_fk`) are excluded from sum/avg generation. FK columns from referenced tables are excluded from dimensions to avoid redundancy.

All models use `sql_table` (the source table) plus `joins` (direct FK joins only, storing source/target column pairs). Multi-hop JOINs are resolved dynamically at query time by walking the join graph.

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
  "fields": ["*:count", "amount:sum"],
  "dimensions": ["customers.name"]
}
```

Or multi-hop dimensions (resolved at query time by walking each model's joins):

```json
{
  "source_model": "orders",
  "fields": ["*:count"],
  "dimensions": ["customers.regions.name"]
}
```

## Diamond Joins

When the same table is reachable via multiple FK paths (e.g., `orders → customers → regions` AND `orders → warehouses → regions`), each model only stores its own direct joins. The multi-hop paths are resolved at query time by walking intermediate models' joins. Each path gets a unique alias:

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
  "fields": ["*:count"]
}
```

## Cycle Handling

If the FK graph contains cycles (e.g., `A → B → A`), ingestion logs a warning and falls back to simple models without rollup joins.
