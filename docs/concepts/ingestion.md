# Auto-Ingestion

SLayer can introspect a database schema and automatically generate datasets with a unified `fields` list and **direct FK-based relationships**. Aggregations are picked at query time (`sum(orders.amount)`, `count(orders.*)`); the dataset itself doesn't carry pre-baked aggregates.

SLayer has three ingestion paths:

1. **Auto-ingest** (this page) — introspect a live database and generate visible datasets with direct FK-based relationships.
2. **dbt semantic layer import** — convert `semantic_models` and `metrics` from a dbt project into visible SLayer datasets. See [dbt Import](../dbt/dbt_import.md).
3. **Hidden dbt-model import** — the `--include-hidden-datasets` variant of `import-dbt` adds every regular dbt model that isn't wrapped by a `semantic_model` as a **hidden** SlayerDataset built via SQL introspection. Hidden datasets stay out of discovery/listing endpoints but remain queryable by name. See [Regular dbt Models (Hidden Import)](../dbt/dbt_import.md#regular-dbt-models-hidden-import).

## How It Works

Ingestion runs in three steps:

### Step 1: FK Graph Analysis

SLayer introspects foreign key constraints and builds a directed dependency graph:

```
orders ──FK──→ customers ──FK──→ regions
```

Every foreign key between two ingested tables of the same schema becomes a relationship, whatever cycles the graph contains (see [Cycle Handling](#cycle-handling) below); self-referencing FKs are skipped.

A key that cannot be read is listed under "Lost key reads" in the ingest output (`key_read_failures` in the REST response), and its table is ingested without it; a re-ingest that reads the key fills it, unless a stored field with a key column's name reads something else.

### Step 2: Build Direct Relationships

Each dataset gets one relationship entry per foreign key **on its own table** — never multi-hop relationships. For example, given `orders → customers → regions`:

- The `orders` dataset gets a single relationship: `orders → customers` (on `customer_id = id`)
- The `customers` dataset gets a single relationship: `customers → regions` (on `region_id = id`)
- The `orders` dataset does **not** get a baked-in `orders → regions` relationship

Each relationship stores the source/target column pair from the table's own FK. Multi-hop paths (e.g., `customers.regions.name` queried from `orders`) are resolved at query time by walking each intermediate dataset's relationships.

- **Dotted field references**: `customers.name`, `customers.id`, `customers.regions.name`, `customers.regions.id` are reachable from `orders` via the relationship graph at query time — they live as fields on the target datasets, not on `orders`.
- **Path-based SQL**: You reference joined fields with dotted paths (`customers.regions.name`); at query time SLayer emits them with `__`-delimited internal table aliases to disambiguate joined tables (e.g., `LEFT JOIN regions AS customers__regions`). The `__` alias is a generated-SQL detail, not something you write.

Tables with no FK references use their plain table name with no relationships.

### Step 3: Introspect & Generate Dataset

SLayer introspects each table's column types and generates a dataset:

- **One `DatasetField`** per non-joined column on the source table — name, `datatype` inferred from the database (`String` / `Integer` / `Decimal` / `Float` / `Boolean` / `Date` / `DateTime` / `DateTimeTz` — `Decimal` for an exact numeric and `DateTimeTz` for a column holding instants (a zoned timestamp, BigQuery and MySQL `TIMESTAMP`, ClickHouse `DateTime` / `DateTime64` naming a zone; a re-ingest re-types a stored `DateTime` field over one) — or `Opaque` for a type SLayer can only store and display), `primary_key=True` for PKs. Whether each field is used as a group-by dimension or as an aggregation source is decided per query; a [list field](queries.md#list-fields) has rules of its own.
- **List columns** become `datatype: Array` with their `element_type`: Postgres `T[]`, BigQuery `REPEATED T` / `ARRAY<T>`, DuckDB `T[]` / `LIST`, ClickHouse `Array(T)` (`Nullable` / `LowCardinality` unwrapped), Trino `array(T)` and Snowflake `ARRAY(T)`; a nested list or a list of records gets `element_type: UNKNOWN`, and a Snowflake `ARRAY` without a declared element type has it inferred from the stored elements.
- **`db_type`** keeps the raw database type where `datatype` loses information (`Opaque`, exact `NUMERIC` / `DECIMAL`, JSON, zoned timestamps, lists); a native JSON column is read natively only when it is recorded, so a dataset written by hand without it reads the field as `TEXT`.
- **Column and table comments** stored in the database become `DatasetField.description` and `SlayerDataset.description` (see [Comments and descriptions](#comments-and-descriptions) below).
- **`unique=True`** for columns that alone form a `UNIQUE` constraint or unique index. PK columns are not stamped redundantly — a sole `primary_key` already implies uniqueness. Composite uniqueness is evaluated per key-set during relationship-cardinality inference rather than being flattened onto individual fields.
- **`enforced_keys`** lists the primary key and each unique field when the database enforces them (Postgres, MySQL/MariaDB, SQLite, SQL Server, Oracle, DuckDB), so joins onto those keys are [trusted](datasets.md#trusted-relationships). Partial and expression unique indexes never count. Snowflake, BigQuery, Redshift, Databricks, ClickHouse and Trino record none, since their constraints are informational.
- On DuckDB (1.1 or later), columns and keys (primary, foreign, `UNIQUE`, unique indexes over plain columns) are read from DuckDB's own catalog (`duckdb_columns()`, `duckdb_constraints()`, `duckdb_indexes()`); a key that cannot be read on any database is logged as a warning and the table is ingested without it.
- **A column literally named `count`** is renamed to `count_col` to avoid clashing with the always-available `count(<dataset>.*)`.
- **No auto-generated `metrics`** — `SlayerDataset.metrics` is the named-expression library and stays empty after ingestion. You can add named expressions later via the API/MCP if you want named shortcuts (`"orders.aov"`).
- **`count(<dataset>.*)`** is always available without any dataset definition.
- The `allowed_aggregations` whitelist is left at the default for the field's data type. A sole PK field is restricted to `count`/`count_distinct`/`count_distinct_approx`/`min`/`max`/`collect` automatically.

FK columns from referenced tables are excluded from the source dataset to avoid redundancy — they're reachable via the relationship graph as `customers.id` etc.

All datasets use `sql_table` (the source table) plus `relationships` (direct FK relationships only, storing source/target column pairs). Multi-hop JOINs are resolved dynamically at query time by walking the relationship graph. A **composite** foreign key becomes a single relationship carrying all of its column pairs, not one relationship per column.

Each FK relationship also gets a structural [`cardinality`](datasets.md#relationship-cardinality) guess from the key constraints alone (no data is read): `many_to_one` by default, upgrading to `one_to_one` when the source key is itself unique. A side counts as unique only when some PK/unique key-set is a subset of the join key — if `(a)` is unique then `(a, b)` is too, but a constraint on `(a, b)` does not make `(a)` unique. When the target key cannot be *verified* unique from its constraints, cardinality is left unset rather than guessed. To infer it from the data instead, run `slayer validate-datasets --cardinality`.

### Comments and descriptions

Textual metadata stored in the database is imported during ingestion, so
agents see the same documentation the DBA wrote:

- **Column comments** → `DatasetField.description`
- **Table comments** → `SlayerDataset.description`
- **BigQuery dataset description** → the datasource's `description`
  (BigQuery only — no cross-database API exists for schema-level comments)

The import is strictly **fill-if-empty**: a comment lands only where the
existing `description` is empty. Hand-written descriptions are never
overwritten, on first ingest or any re-ingest — consistent with the
additive-only re-ingestion contract. To re-import a comment, clear the
description and re-run `slayer ingest`. Imported descriptions feed the
search corpus like any other description, so the next embedding refresh
picks them up.

Per-database support:

| Database | Column comments | Table comments |
|---|---|---|
| BigQuery | ✓ (field descriptions) | ✓ (table description) |
| Postgres, MySQL, SQL Server, Snowflake, ClickHouse | ✓ | ✓ |
| DuckDB | ✓ (`COMMENT ON`) | ✓ |
| SQLite | — (no such feature) | — |

Comment fetching is best-effort: a database or driver that cannot surface
comments simply yields datasets without descriptions, never a failed ingest.

### SQLite affinity probing

SQLite's declared column types are affinity hints, not strict constraints: a column declared `INTEGER` can store `INTEGER`, `REAL`, `TEXT`, or `BLOB` values per row. To prevent silent truncation downstream (a column declared `INTEGER` but actually storing `0.99` would cast to `0` and break `AVG`/`SUM` results), SLayer runs an additional value-level probe on SQLite ingestion for every column the inspector reports as `INTEGER`-affinity.

The probe samples up to **`PROBE_SCAN_CAP + 1` rows** (100,001 by default; configurable via `slayer.database.sqlite_introspect.PROBE_SCAN_CAP`). The `+1` lets the probe detect saturation — if 100,001 rows come back, there's at least one row past the cap, and the probe declines to certify INT. It decides per column:

- **Float** when any row's storage class is `REAL`, or any integer-storage value fails `ROUND(col) = col`, or every distinct TEXT value coerces to a finite `float()`.
- **String** when any row holds a `BLOB`, or any TEXT value is non-coercible / non-finite, or the distinct-text sample saturates the 1,000-distinct-value cap.
- **Integer** when the entire sample is integer-shaped and the sample isn't saturated.
- The SA-derived `Integer` is kept (probe returns `None`) when the column is empty, all-NULL, the row sample saturates without enough evidence, or the probe itself errors. The probe logs a `WARNING` in the saturated / error cases.

The probe is **idempotent re-ingest aware**: a persisted `DatasetField.datatype = Integer` is widened to `Float` / `String` on the next `slayer ingest` if the live storage classes disagree. `DatasetField.format = NumberFormat(INTEGER)` (the auto-ingested default) is flipped to `FLOAT` for `DOUBLE` or cleared for `TEXT`; user-set custom formats (currency, custom precision) are preserved verbatim with an `INFO` log noting the type change. The CLI prints `Updated: <dataset> (widened: <col>)` so the change is visible.

Non-SQLite datasources (Postgres, MySQL, DuckDB, ClickHouse, SQL Server) skip the probe entirely — their type systems are strict and this class of bug doesn't exist.

Already-persisted v7 SQLite datasets with the wrong `Integer` type are **not** auto-repaired on `storage.get_dataset()` load (running a full table scan per column on every load would be too expensive). Re-ingest is the auto-heal path: `slayer ingest` or `slayer serve --ingest-on-startup`. The DOUBLE → INT narrowing on legacy-dict migration is also gated on the probe on SQLite — it only fires when the probe positively certifies INT.

## Schema scope

By default `slayer ingest` covers only the connection's **default schema**, and
that schema's tables stay unqualified (`sql_table: orders`). To reach other
schemas, pass their names or ask for all of them:

- `--schema a,b` (or `schemas=["a","b"]`) — ingest the named schemas. A single
  name is emitted **verbatim** into `sql_table` (`--schema public` →
  `public.orders`); across several schemas the default one stays bare and only
  the non-default siblings are prefixed (`analytics.orders`). A name that is not a plain identifier is quoted
  (`sales."order items"`, `"sales.v2".orders`).
- `--all-schemas` — ingest every non-system schema in the datasource's own
  catalog. The default schema stays bare; every other schema is qualified.
- `--schema` and `--all-schemas` are mutually exclusive (the same conflict is
  rejected by the REST `IngestRequest` as a 422 and by the MCP tools as an
  error string).

Object names with `__` are preserved as faithful dataset names — `a__b` and `a_b`
are distinct datasets, and neither is sanitized. A name collision therefore only
happens when two objects share the **same raw name** across different scanned
schemas; exactly one wins the dataset name — the default schema beats a non-default
one, then the lower schema name, then the lower object name — and the losers are
reported under `skipped` (labelled `schema.table` when more than one schema was
scanned). Columns, primary keys, and comments are always read from the winning
table's own schema, never unioned across same-named twins. `sanitize_dataset_name`
survives only as a re-ingest fallback-matching spelling: a stored old-world
sanitized dataset (`a_b`) whose `sql_table` points at the live `a__b` is still
matched to it, so re-ingest updates it in place (stored name wins).

`datasources create --schema X` persists `X` as the datasource's
`schema_name`, so later bare `slayer ingest` runs keep using it; `--all-schemas`
persists no default schema. A dataset that was ingested before qualification
existed (stored bare) is **self-healed** to its qualified `sql_table` on the
next re-ingest when that is unambiguous — but a bare dataset that names a real
default-schema table is never repointed at a non-default twin.

## Usage

### CLI

```bash
# Default schema only (tables stay unqualified)
slayer ingest --datasource my_postgres --storage ./slayer_data

# A specific non-default schema (emitted verbatim: analytics.orders)
slayer ingest --datasource my_postgres --schema analytics

# Several schemas, or every non-system schema
slayer ingest --datasource my_postgres --schema public,analytics
slayer ingest --datasource my_postgres --all-schemas
```

### Python

```python
import asyncio
from slayer.engine.query_engine import SlayerQueryEngine

async def main():
    engine = SlayerQueryEngine(storage=storage)
    try:
        # Reads the database through the engine's client and writes to its storage.
        result = await engine.ingest_idempotent(
            ds,
            schema="public",
            include_tables=["orders", "customers"],  # Optional filter
            exclude_tables=["migrations"],            # Optional exclusion
        )
    finally:
        await engine.aclose()  # release the client's resources on this event loop
    # result.additions  — what was added (new datasets / fields / relationships)
    # result.to_delete  — pending validate_datasets drift entries
    # result.broken_dependents — stored definitions a list re-type broke
    # result.errors     — per-dataset failures (best-effort, doesn't abort)
    return result

asyncio.run(main())
```

### MCP

```
create_datasource(name="mydb", type="postgres", ...)
ingest_datasource(datasource_name="mydb", schema_name="public")
# or, across schemas:
ingest_datasource(datasource_name="mydb", schemas="public,analytics")
ingest_datasource(datasource_name="mydb", all_schemas=True)
```

### REST API

```bash
curl -X POST http://localhost:5143/ingest \
  -H "Content-Type: application/json" \
  -d '{"datasource": "my_postgres", "schema_name": "public"}'
# or: {"datasource": "my_postgres", "schemas": ["public", "analytics"]}
# or: {"datasource": "my_postgres", "all_schemas": true}
```

## Querying Rolled-Up Datasets

After ingestion, you can query rolled-up dimensions directly:

```json
{
  "population": "orders",
  "metrics": ["count(orders.*)", "sum(orders.amount)"],
  "dimensions": ["customers.name"]
}
```

Or multi-hop dimensions (resolved at query time by walking each dataset's relationships):

```json
{
  "population": "orders",
  "metrics": ["count(orders.*)"],
  "dimensions": ["customers.regions.name"]
}
```

## Diamond Relationships

When the same table is reachable via multiple FK paths (e.g., `orders → customers → regions` AND `orders → warehouses → regions`), each dataset only stores its own direct relationships. The multi-hop paths are resolved at query time by walking intermediate datasets' relationships. Each path gets a unique alias:

- `customers.regions.name` → SQL alias `customers__regions`
- `warehouses.regions.name` → SQL alias `warehouses__regions`

This avoids table alias collisions and allows querying both paths simultaneously:

```json
{
  "population": "orders",
  "dimensions": [
    "customers.regions.name",
    "warehouses.regions.name"
  ],
  "metrics": ["count(orders.*)"]
}
```

## Cycle Handling

FK cycles (e.g., `A → B → A`) are ingested as-is, and when two or more relationships connect the same pair of datasets (a 2-cycle, or `billing_address_id` and `shipping_address_id` both referencing `addresses`) ingestion names each unnamed one after its FK column with a trailing `_id`/`_fk` removed (`billing_address`, `shipping_address`) — falling back to `<dataset>_<stem>`, then a numeric suffix, on a name clash, and never renaming a name you set — so you address them as `orders.billing_address.city`.

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
every visible dataset + its visible children, **and every memory whose
canonical entities are rooted at the datasource**. A stale
`embeddings.db` (created without an `OPENAI_API_KEY`, or after a manual
`memories/<id>.md` edit) is therefore repaired by the next
`--ingest-on-startup` with no extra step. Per-memory embed failures
surface as `IngestionError(dataset_name="memory:<id>", …)` in the
result's `errors` list.

### Defense-in-depth dangling-ref cleanup

The same per-datasource memory pass also walks each memory's
`entities` list and probes every reference against storage. Refs
that resolve to a definitive "not found" (deleted dataset, dropped
field, vanished referenced memory) are stripped from the persisted
list — three layers of defense keep entity tags clean as the
underlying entities come and go:

1. **Cascade-strip at delete (primary)** — `delete_dataset` /
   `delete_datasource` / `forget_memory` / `edit_dataset_remove`
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
`IngestionError(dataset_name="memory:<id>", error="attached query has
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

Same "datasets are fresh by the time the constructor returns" guarantee the
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
remains gated behind `slayer validate-datasets --force-clean [--yes]`. See
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

`slayer ingest` (and every other ingesting entry point: `slayer datasources create --ingest`, MCP `ingest_datasource` and `create_datasource`, REST `/ingest`, the startup ingest) is idempotent by default — re-runs are safe. For each in-scope live table:

- **No persisted dataset with that name** → ingest from scratch via the path above.
- **Existing `sql_table`-mode dataset** → append new fields and relationships from the live schema. Existing user metadata is **never** overwritten — `description`, `label`, `format`, `meta`, and `allowed_aggregations` are preserved verbatim. The only in-place updates are strictly additive gap-fills: a relationship's `cardinality` is set only when it is currently unset (a value you chose is never replaced), a field's `unique` is only ever turned on, never off, a dataset with no primary key takes the live one (a key you set is never changed), a missing enforced key is added (one is never removed; schema drift does that), and an **empty** `description` (field and dataset level) is filled from the live DB comment. Filling any of these is enough to trigger a save, so a re-ingest that adds no fields or relationships still persists newly-discovered metadata. Filled descriptions are reported per dataset (`DatasetAddition.described_fields`, `dataset_described`; the CLI prints `+descriptions: …`). Other metadata fills are reported as `metadata_changed` (`+key metadata`).
- **Existing `sql`-mode or query-backed dataset with the matching name** → skipped by the merge, since those are user-authored; the only change re-ingestion makes to an `sql`-mode dataset is the list re-type below.

With the default YAML storage, two live tables whose quoted names differ only by letter case (`"Orders"` vs `orders`) cannot both be persisted — dataset names collide as filenames on macOS / Windows, so the save is rejected (`IdCollisionError`). The first table wins; the second surfaces as a per-dataset entry in `IdempotentIngestResult.errors` (or a per-dataset message on the CLI / MCP paths) without aborting the rest of the ingest. SQLite storage persists both.

After the additive pass, `validate_datasets` runs against the in-scope datasets and the result is merged into the response (`IdempotentIngestResult.to_delete`). Type-bucket drift on existing fields surfaces there — apply via `slayer validate-datasets --force-clean`, then re-ingest to pick up the new live type; the one exception is a non-list field whose live column is now a list, which re-ingestion re-types in place (keeping only `count` allowed) — in a stored `sql`-mode dataset too, whose field is matched to its query's result column. See [Schema Drift](schema-drift.md) for the full diff / cascade contract.

### Search side effects

After validation, every ingest also refreshes the search corpus for the touched datasource:

- **Sample values** (`DatasetField.sampled`) — **not** re-profiled at ingest. A per-column full-table scan would dominate ingest wall-clock on wide datasources, so samples are populated lazily on the first `inspect` of a field (or explicitly via `slayer datasources refresh-samples`). See [Search](search.md#sample-value-cache).
- **Embedding rows** — when the `advanced_search` extra is installed and a usable provider API key is in the environment, the embedding refresh re-runs for the datasource doc plus every visible dataset + its visible children. `SLAYER_EMBEDDING_MODEL` is an *optional* override of the default (`openai/text-embedding-3-small`); setting it is not required. The SHA256 `content_hash` on each row means re-ingests are cheap when nothing changed. See [Search](search.md#channel-3-dense-embedding-similarity).

Both refreshes are best-effort: per-entity runtime failures land in `IdempotentIngestResult.errors` as friendly strings, never aborting ingestion. When the `advanced_search` extra is not installed or no API key is configured for the active embedding model, the embedding pass is silently skipped — the user-visible signal lives on the next `search` response.

`include_tables` / `exclude_tables` constrain the additive pass plus the `sql_table`-mode subset of validation: a `sql_table`-mode dataset whose table is excluded is left out of both. `sql`-mode and query-backed datasets in the same datasource are still passed through `validate_datasets` regardless of the table filter — they are not tied to a specific table name. Run `validate_datasets` directly (no `--include`/`--exclude`) to validate only those modes.
