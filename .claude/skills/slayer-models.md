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

Aggregation is specified at query time with **colon syntax**: `"amount:sum"`, `"amount:avg"`, `"*:count"`. A bare-name reference like `{"formula": "aov"}` resolves to the saved `ModelMeasure` formula on the model. Built-in aggregations: `sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `count_distinct_approx`, `first`, `last`, `weighted_avg`, `median`, `percentile`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`. `count_distinct_approx` is dialect-aware (native approximate-distinct where available, exact `COUNT(DISTINCT)` fallback otherwise). The two-column ones (`corr`, `covar_samp`, `covar_pop`) take the second column as a named param: `price:corr(other=quantity)`.

## Data Types

**Column types**: `string`, `number`, `boolean`, `time` (timestamp), `date`

## Joins

Models can declare LEFT JOIN relationships to other models:

```yaml
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
    cardinality: many_to_one   # optional; source→target arity
```

Enables cross-model measures (`customers.score:avg`), multi-hop dimensions (`customers.regions.name`), and transforms on joined measures (`cumsum(customers.score:avg)`). Auto-ingestion creates one direct join per FK on the source table (composite FKs stay a single join with multiple `join_pairs`). `cardinality` (`one_to_one` / `one_to_many` / `many_to_one` / `many_to_many`, omit when undetermined) is descriptive metadata, orthogonal to the always-LEFT join type; auto-ingestion fills it structurally, and `slayer validate-models --cardinality [--persist-cardinality]` infers it from the data. See [models.md#join-cardinality](../../docs/concepts/models.md#join-cardinality). Multi-hop paths (e.g. `orders → customers → regions`) are resolved at query time by walking each intermediate model's own joins. Diamond joins (same table via different paths) are supported — each path gets a unique `__`-delimited alias (e.g., `customers__regions` vs `warehouses__regions`).

**Derived-on-derived chaining.** A `Column.sql` may reference another *derived* column — local same-model or via the join graph (single-dot `B.col` or `__`-delimited `B__C.col` path). Same-model refs can be **bare** (`A.ratio = "bar / foo_normalized"`) or **qualified** (`A.ratio = "A.bar / A.foo_normalized"`) — both inline identically. The engine recursively inlines those references at query time, so you can write `A.ratio = "A.bar / B.foo_normalized"` even when `B.foo_normalized.sql = "foo_raw / 100.0"`. No need to inline derivations at every consumer site. Refs inside a nested scope (sub-query, `UNION` branch, CTE, `VALUES`) are left alone — they belong to the inner rowset. Cycles raise `ColumnCycleError` (a subclass of `ValueError`) at `save_model` time, so a cyclic model never reaches a query.

## Model Filters

Models can have always-applied WHERE filters: `filters: ["deleted_at IS NULL"]`. Only WHERE conditions on underlying table columns.

## Window functions in `Column.sql`

A column's `sql` may contain a window function (e.g. `row_number() over (order by mass desc)`); it behaves like any other column when SELECTed. **Filtering directly on such a column from a query is rejected** (DEV-1369) — use the inline `rank(<measure>) <= N` / `dense_rank` / `percent_rank` / `ntile(n=<N>)` transform for top‑N (dialect-portable and simpler), or factor the windowed expression into an earlier stage of a multi-stage `source_queries` model. Raw `OVER (...)` SQL inside a `ModelMeasure.formula` is rejected at construction with an actionable error.

## Source modes

A SlayerModel has exactly one source mode (mutually exclusive):
- `sql_table`: physical table **or view**.
- `sql`: explicit SQL subquery.
- `source_queries`: list of `SlayerQuery` stages — the model is **query-backed**.

`source_kind` (`table` / `view` / `materialized_view` / `null` for unknown)
records which kind of object `sql_table` names. Auto-ingestion sets it and
refreshes it on re-ingest. A view has no primary key and no foreign keys, so a
view-backed model has no primary-key column and no auto-generated joins —
`source_kind: view` is the signal that this is inherent, not missing data.

Model names cannot contain `__` (reserved for join-path aliases), but
`sql_table` can. Ingestion sanitizes only the name: object
`reports__patient__drug` → model `reports_patient_drug`, `sql_table` unchanged.

Auto-ingestion sets `hidden: true` on recognised ELT/migration bookkeeping
tables — prefixes `_dlt_`, `_airbyte_`, plus exact names like
`alembic_version`, `flyway_schema_history`, `databasechangelog`,
`django_migrations`, `schema_migrations`, `_fivetran_audit` — and records
`meta.internal_table` with the tool that matched. Hidden, not skipped: the model
is absent from `models_summary` and search but stays queryable by name and
usable as a join target, so `_dlt_loads` still answers freshness questions.
Hiding happens only at creation, so `edit_model(name, data_source=..., hidden=false)` survives
every later re-ingest. `slayer ingest --surface-internals` ingests newly created
internals visible instead. Every ingest surface reports what it hid — the CLI
and `datasources create --ingest` print a `Hidden (N)` section,
`ingest_datasource_models` returns one, and `POST /ingest` carries
`hidden_internals` in the 200 body.

Auto-ingestion covers only the connection's default schema unless told
otherwise: `--schema a,b` / `schemas=[…]`, or `--all-schemas` (`schemas` and
`all_schemas` on `ingest_datasource_models` / `POST /ingest`; `--schema` and
`--all-schemas` are mutually exclusive → CLI error / 422 / MCP error string).
A *single* requested schema is always emitted verbatim, even when it equals the
connection default (`--schema public` → `sql_table: public.orders`). Only when
*several* schemas are in scope (multiple `--schema` names or `--all-schemas`)
does the default stay bare while non-default schemas are qualified
(`analytics.orders`). A same-named table across schemas resolves to one winner
(exact > sanitized, default > non-default, then lower schema / object name);
columns and PKs are read only from the winner's own schema. A model stored bare
before qualification existed is self-healed to its qualified `sql_table` on
re-ingest when unambiguous. See
[Auto-Ingestion](../../docs/concepts/ingestion.md#schema-scope).

## Query-backed models

`create_model_from_query(query, name, variables=None)` saves a query (or list of stages) as a query-backed model. It populates `model.source_queries`, optional `model.query_variables` defaults, and caches `model.columns` + `model.backing_query_sql` from a save-time dry-run (unresolved `{var}` placeholders default to `'0'`).

Saved query-backed models support two access patterns:
- **Run by name**: `engine.execute("monthly_revenue", variables={...})` runs the stored backing query.
- **Use as source_model**: `{"source_model": "monthly_revenue", ...}` treats the saved result as a model in another query.

Variable precedence (highest first): runtime kwarg > stage `.variables` > outer query `.variables` > `model.query_variables`.

**Variables in model SQL (DEV-1625)**: the same `{var}` mechanism also substitutes into a model's **raw-SQL (Mode A) surfaces** — `SlayerModel.sql`, `SlayerModel.filters`, `Column.sql`, `Column.filter` — for a query's **direct source model** (the primitive for parameterizing hand-written SQL, e.g. Cube `FILTER_PARAMS`). Same precedence and `{{`/`}}` escaping. Contract: **raise-on-missing once any variable is in play** (a `query_variables` default or a caller value); a **fully variable-free execution leaves braces as literals** so raw brace literals like `'{1,2,3}'` survive untouched. String values are Mode-A- and dialect-aware-escaped (write the quotes yourself: `WHERE region = '{region}'`; the quote/backslash escaping follows the datasource dialect so values round-trip on standard AND backslash-escaping backends like MySQL/ClickHouse — DEV-1727; still trusted input, and only *quoted* literals are escaped); a **list** value renders an injection-safe `IN`-list body (`region IN ({regions})` with `{"regions": ["US","CA"]}` → `region IN ('US', 'CA')`; write the parens, elements auto-quoted; empty list raises); `inspect_model` shows the literal `{var}` template. Nested `source_queries` stages, query-backed direct sources, join targets, and cross-model targets are deferred (DEV-1678) — a `{var}` there stays literal and errors on the stray placeholder.

**Optional blocks (DEV-1730)**: Mode-A surfaces also support `{? pred ?}` — the predicate renders (parenthesised) when every inner `{var}` is supplied, else the whole block collapses to `(1=1)`. This is the SLayer form of a Cube `FILTER_PARAMS` optional pushdown (`{? region IN ({regions}) ?}` → `(region IN ('US','CA'))` or `(1=1)`). Put `AND` outside the block and open `WHERE` with `1=1`; a block needs ≥1 var, doesn't nest, collapses even on a zero-variable call, and is Mode-A-only. `inspect` lists a model's placeholders as **required** (bare, no default) vs **optional** (in-block or defaulted), derived from the SQL (`extract_model_variables`). `slayer import-cube` reads Cube `.js` as well as YAML and maps `FILTER_PARAMS` to these forms — requiredness from member `meta.required` (`--ignore-required-meta` forces optional).

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
- One `Column` per non-joined database column (with `type` inferred). PK columns get `primary_key=True`; single-column `UNIQUE` constraints set `unique=True`. A column literally named `count` is renamed to `count_col` to avoid clashing with `*:count`.
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
