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

If the graph is acyclic, SLayer computes the **transitive closure** for each table — all tables reachable via FK chains — to determine which columns to introspect for dotted references (e.g. `customers.regions.name`). The transitive closure is used only for column discovery, not for generating joins (see Step 2). If a cycle is detected, ingestion logs a warning and falls back to simple models without rollup joins (see [Cycle Handling](#cycle-handling) below).

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

### SQLite affinity probing

SQLite's declared column types are affinity hints, not strict constraints: a column declared `INTEGER` can store `INTEGER`, `REAL`, `TEXT`, or `BLOB` values per row. To prevent silent truncation downstream (a column declared `INTEGER` but actually storing `0.99` would cast to `0` and break `AVG`/`SUM` results), SLayer runs an additional value-level probe on SQLite ingestion for every column the inspector reports as `INTEGER`-affinity.

The probe samples up to **`PROBE_SCAN_CAP + 1` rows** (100,001 by default; configurable via `slayer.sql.sqlite_introspect.PROBE_SCAN_CAP`). The `+1` lets the probe detect saturation — if 100,001 rows come back, there's at least one row past the cap, and the probe declines to certify INT. It decides per column:

- **DOUBLE** when any row's storage class is `REAL`, or any integer-storage value fails `ROUND(col) = col`, or every distinct TEXT value coerces to a finite `float()`.
- **TEXT** when any row holds a `BLOB`, or any TEXT value is non-coercible / non-finite, or the distinct-text sample saturates the 1,000-distinct-value cap.
- **INT** when the entire sample is integer-shaped and the sample isn't saturated.
- The SA-derived `INT` is kept (probe returns `None`) when the column is empty, all-NULL, the row sample saturates without enough evidence, or the probe itself errors. The probe logs a `WARNING` in the saturated / error cases.

The probe is **idempotent re-ingest aware**: a persisted `Column.type = INT` is widened to `DOUBLE` / `TEXT` on the next `slayer ingest` if the live storage classes disagree. `Column.format = NumberFormat(INTEGER)` (the auto-ingested default) is flipped to `FLOAT` for `DOUBLE` or cleared for `TEXT`; user-set custom formats (currency, custom precision) are preserved verbatim with an `INFO` log noting the type change. The CLI prints `Updated: <model> (widened: <col>)` so the change is visible.

Non-SQLite datasources (Postgres, MySQL, DuckDB, ClickHouse, SQL Server) skip the probe entirely — their type systems are strict and this class of bug doesn't exist.

Already-persisted v7 SQLite models with the wrong `INT` type are **not** auto-repaired on `storage.get_model()` load (running a full table scan per column on every load would be too expensive). Re-ingest is the auto-heal path: `slayer ingest` or `slayer serve --ingest-on-startup`. The DEV-1361 DOUBLE → INT narrowing on legacy-dict migration is also gated on the probe on SQLite — it only fires when the probe positively certifies INT.

## Usage

### CLI

```bash
slayer ingest --datasource my_postgres --schema public --storage ./slayer_data
```

### Python

```python
import asyncio
from slayer.engine.ingestion import ingest_datasource_idempotent

async def main():
    result = await ingest_datasource_idempotent(
        datasource=ds,
        storage=storage,
        schema="public",
        include_tables=["orders", "customers"],  # Optional filter
        exclude_tables=["migrations"],            # Optional exclusion
    )
    # result.additions  — what was added (new models / columns / joins)
    # result.to_delete  — pending validate_models drift entries
    # result.errors     — per-model failures (best-effort, doesn't abort)
    return result

asyncio.run(main())
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

## Ingesting at Startup

`slayer serve` and `slayer mcp` both accept `--ingest-on-startup`, an
**opt-in** flag that walks every configured datasource and runs the same
idempotent ingestion pass described in [Idempotent
Re-Ingestion](#idempotent-re-ingestion) **before** the port opens / before
stdio JSON-RPC starts. Mirrors the existing `--demo` boot hook, so both
flags compose: `--demo` runs first (creating the Jaffle Shop datasource),
then the startup-ingest pass runs over every datasource including the
freshly-created demo.

Each per-datasource pass refreshes embeddings for the datasource doc,
every visible model + its visible children, **and every memory whose
canonical entities are rooted at the datasource** (DEV-1416). A stale
`embeddings.db` (created without an `OPENAI_API_KEY`, or after a manual
`memories/<id>.md` edit) is therefore repaired by the next
`--ingest-on-startup` with no extra step. Per-memory embed failures
surface as `IngestionError(model_name="memory:<id>", …)` in the
result's `errors` list.

### Defense-in-depth dangling-ref cleanup (DEV-1428)

The same per-datasource memory pass also walks each memory's
`entities` list and probes every reference against storage. Refs
that resolve to a definitive "not found" (deleted model, dropped
column, vanished referenced memory) are stripped from the persisted
list — three layers of defense keep entity tags clean as the
underlying entities come and go:

1. **Cascade-strip at delete (primary)** — `delete_model` /
   `delete_datasource` / `forget_memory` / `edit_model_remove`
   rewrite affected memories synchronously.
2. **Retrieval-time in-memory filter (belt)** — `SearchService`
   filters each memory's `entities` against the live canonical set
   before BM25 ranks, so partially-completed cascades never surface
   stale tags. No write-back.
3. **Ingest-time persisted cleanup (suspenders)** — the per-memory
   walk described here.

A raise during the cleanup-side existence-check (transient infra
failure) is treated as **"ref intact"** — the reference is kept,
never dropped on a maybe.

For memories with an attached `Memory.query` (the inline example
query stored alongside the learning), the cleanup pass attempts to
re-extract entities from the query. On failure the pass emits an
`IngestionError(model_name="memory:<id>", error="attached query has
stale references: ...")` — the query itself is **not** rewritten.
Agents reading the warning can re-save the memory to clean it.

### CLI

```bash
slayer serve --ingest-on-startup
slayer mcp --ingest-on-startup
slayer serve --demo --ingest-on-startup     # demo first, then ingest all DSes
```

### Environment variable

`SLAYER_INGEST_ON_STARTUP=<truthy>` enables the same behaviour. Truthy =
`1`, `true`, `yes` (case-insensitive). Anything else (including unset, `0`,
`false`, empty) is off. An explicit `--ingest-on-startup` wins over the
env var when both are set.

### Programmatic (embedders)

```python
from slayer.api.server import create_app
from slayer.mcp.server import create_mcp_server

app = create_app(storage=storage, ingest_on_startup=True)
mcp = create_mcp_server(storage=storage, ingest_on_startup=True)
```

Same "models are fresh by the time the constructor returns" guarantee the
CLI gets.

### Error semantics

- **One datasource fails** (the ingest call raises): caught, friendly-formatted, accumulated, server starts anyway.
- **Per-table errors inside a single datasource** (`result.errors` non-empty): printed; that datasource still counts as "succeeded" because the call itself returned.
- **`storage.list_datasources()` raises**: propagates — server does not start. Boot should not proceed with broken storage.
- **Zero datasources**: prints `Ingest-on-startup: no datasources configured` and starts normally.

### Drift handling

`to_delete` entries from each per-datasource result are printed via the
standard drift renderer and accumulated into the return value's
`drift_pending` list, but **never auto-applied**. Destructive cleanup
remains gated behind `slayer validate-models --force-clean [--yes]`. See
[Schema Drift](schema-drift.md).

### Output

All boot-ingest output goes to **stderr** for both `slayer serve` and
`slayer mcp` — `mcp` uses stdio JSON-RPC and any byte on stdout would
corrupt the channel. Final line:

```text
Ingest-on-startup: N/M datasources ingested
```

or, when at least one failed:

```text
Ingest-on-startup: N/M datasources ingested (K failed: name1, name2)
```

## Idempotent Re-Ingestion

`slayer ingest` (and the equivalent MCP / REST entry points) is idempotent by default — re-runs are safe. For each in-scope live table:

- **No persisted model with that name** → ingest from scratch via the path above.
- **Existing `sql_table`-mode model** → append new columns and joins from the live schema. Existing columns and joins are **never** mutated — `description`, `label`, `format`, `meta`, and `allowed_aggregations` are preserved verbatim.
- **Existing `sql`-mode or query-backed model with the matching name** → skipped silently; those are user-authored.

After the additive pass, `validate_models` runs against the in-scope models and the result is merged into the response (`IdempotentIngestResult.to_delete`). Type-bucket drift on existing columns surfaces there — apply via `slayer validate-models --force-clean`, then re-ingest to pick up the new live type. See [Schema Drift](schema-drift.md) for the full diff / cascade contract.

### Search side effects

After validation, every ingest also refreshes the search corpus for the touched datasource:

- **Sample values** (`Column.sampled`) — re-profiled for every non-hidden, non-PK column on every table-backed model in the datasource. The cached snapshot is consumed by the tantivy search index and by `inspect_model`. See [Search](search.md#sample-value-cache).
- **Embedding rows** — when the `advanced_search` extra is installed and a usable provider API key is in the environment, the embedding refresh re-runs for the datasource doc plus every visible model + its visible children. `SLAYER_EMBEDDING_MODEL` is an *optional* override of the default (`openai/text-embedding-3-small`); setting it is not required. The SHA256 `content_hash` on each row means re-ingests are cheap when nothing changed. See [Search](search.md#channel-3--dense-embedding-similarity).

Both refreshes are best-effort: per-entity runtime failures land in `IdempotentIngestResult.errors` as friendly strings, never aborting ingestion. When the `advanced_search` extra is not installed or no API key is configured for the active embedding model, the embedding pass is silently skipped — the user-visible signal lives on the next `search` response.

`include_tables` / `exclude_tables` constrain the additive pass plus the `sql_table`-mode subset of validation: a `sql_table`-mode model whose table is excluded is left out of both. `sql`-mode and query-backed models in the same datasource are still passed through `validate_models` regardless of the table filter — they are not tied to a specific table name. Run `validate_models` directly (no `--include`/`--exclude`) to validate only those modes.
