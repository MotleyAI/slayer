# Auto-Ingestion

SLayer can introspect a database schema and automatically generate models with a unified `columns` list and **direct FK-based joins**. Aggregations are picked at query time with colon syntax (`amount:sum`, `*:count`); the model itself doesn't carry pre-baked aggregates.

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

The graph is validated to be acyclic (a `RollupGraphError` is raised if cycles are detected). For each table, SLayer computes the **transitive closure** — all tables reachable via FK chains — to determine which columns to introspect for dotted references (e.g. `customers.regions.name`). The transitive closure is used only for column discovery, not for generating joins (see Step 2).

### Step 2: Build Direct Joins

Each model gets one join entry per foreign key **on its own table** — never multi-hop joins. For example, given `orders → customers → regions`:

- The `orders` model gets a single join: `orders → customers` (on `customer_id = id`)
- The `customers` model gets a single join: `customers → regions` (on `region_id = id`)
- The `orders` model does **not** get a baked-in `orders → regions` join

Each join stores the source/target column pair from the table's own FK. Multi-hop paths (e.g., `customers.regions.name` queried from `orders`) are resolved at query time by walking each intermediate model's joins.

- **Dotted column references**: `customers.name`, `customers.id`, `customers.regions.name`, `customers.regions.id` are reachable from `orders` via the join graph at query time — they live as columns on the target models, not on `orders`.
- **Path-based SQL**: At query time, column SQL uses `__`-delimited table aliases (e.g., `customers__regions.name`) to disambiguate joined tables. Each joined table gets a path-based alias (e.g., `LEFT JOIN regions AS customers__regions`).

Tables with no FK references use their plain table name with no joins.

### Step 3: Introspect & Generate Model

SLayer introspects each table's column types and generates a model:

- **One `Column`** per non-joined column on the source table — name, `type` inferred from the database (`string` / `number` / `boolean` / `time` / `date`), `primary_key=True` for PKs. Whether each column is used as a group-by dimension or as an aggregation source is decided per query.
- **A column literally named `count`** is renamed to `count_col` to avoid clashing with the always-available `*:count`.
- **No auto-generated `measures`** — `SlayerModel.measures` is the named-formula library and stays empty after ingestion. You can add named formulas later via the API/MCP if you want bare-name shortcuts (`{"formula": "aov"}`).
- **`*:count`** is always available without any model definition.
- The `allowed_aggregations` whitelist is left at the default for the column's data type. PK columns are restricted to `count`/`count_distinct` automatically.

FK columns from referenced tables are excluded from the source model to avoid redundancy — they're reachable via the join graph as `customers.id` etc.

All models use `sql_table` (the source table) plus `joins` (direct FK joins only, storing source/target column pairs). Multi-hop JOINs are resolved dynamically at query time by walking the join graph.

## Usage

### CLI

```bash
slayer ingest --datasource my_postgres --schema public --storage ./slayer_data
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
  "measures": ["*:count", "amount:sum"],
  "dimensions": ["customers.name"]
}
```

Or multi-hop dimensions (resolved at query time by walking each model's joins):

```json
{
  "source_model": "orders",
  "measures": ["*:count"],
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
  "measures": ["*:count"]
}
```

## Cycle Handling

If the FK graph contains cycles (e.g., `A → B → A`), ingestion logs a warning and falls back to simple models without rollup joins.
