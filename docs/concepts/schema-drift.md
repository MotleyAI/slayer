# Schema drift

When a live database schema changes — a column is dropped, a type bucket
flips, a table goes away — persisted SLayer datasets stop being valid. Until
the change is reconciled, queries against the affected datasets fail with
raw DBAPI errors that don't tell you which dataset is broken or what to fix.

SLayer surfaces drift as a first-class concept across three behaviours:

1. **`validate_datasets`** — read-only diff that returns the minimal list of
   *deletes* needed to make persisted datasets valid against the live state.
2. **Idempotent re-ingestion** — additive only; never overwrites
   user-customised entries. Returns a combined report of what was added
   plus what `validate_datasets` says still needs deleting.
3. **`SchemaDriftError`** — query-time wrap that checks for drift when a
   query fails and surfaces the structured drift payload instead of the
   raw DBAPI message. It checks only the datasets the failed statement read,
   reuses live-schema facts for up to 60 seconds, and its `datasets` lists
   the drifted datasets. It wraps the failure only when a drift entry touches
   what the statement reads — a whole-dataset drop, a field it reads (also
   through a derived field or as a join key), a relationship it crosses, a metric
   or aggregation it uses, any dataset filter, or a re-type finding on a
   field it reads; any other drift leaves the original database error.

Use `validate_datasets` to inspect drift; use `slayer validate-datasets
--force-clean` (CLI only) to apply the deletes.

## How drift is detected

Drift is computed per source mode:

* **`sql_table`** — open the datasource via SQLAlchemy `Inspector`, read
  live columns, types, primary keys, and foreign keys. Compare against
  `dataset.fields` and `dataset.relationships`. Live types are mapped through SLayer's
  coarse buckets (`number` / `string` / `boolean` / `temporal`):
  `INTEGER` and `FLOAT` collapse to `number`, `DATE` and `TIMESTAMP`
  collapse to `temporal`. A bucket change on a field that still exists is a
  [re-type finding](#re-type-findings), never a delete — switching `BIGINT` ↔
  `FLOAT` is *not* drift, but `NUMBER` ↔ `STRING` is. On SQLite, which stores
  dates as text, a `DATE` or `TIMESTAMP` field over a live `TEXT` column is not drift.
* **`sql`** — trial-execute `SELECT * FROM (<dataset.sql>) AS _sd_validate
  WHERE 1=0`. Successful execution exposes cursor metadata (column names
  and types), which goes through the same field-level diff. A failed
  trial-execute means the SQL itself is broken — emit a whole-dataset drop.
  A field whose stored type conflicts with its result column's (e.g. one an
  older SLayer probe typed) is a re-type finding.
* **`source_queries`** (query-backed) — never re-introspected directly.
  Treated as a *cascade target*: when validation against the underlying
  datasource produces a field or dataset drop, any query-backed dataset that
  transitively references the dropped thing gets a whole-dataset drop.

A dataset whose listed table cannot be read, or whose datasource cannot be
connected to, gets no verdict rather than a drop.

## Cascade rules

A drop on `M.X` cascades through:

1. **Derived fields on M.** Any `DatasetField.sql` on `M` that references `X`
   (via sqlglot AST walk) is dropped, transitively across chains of
   derived fields.
2. **Metrics on M.** Any `DatasetMetric.expression` on `M` referencing `X`
   or any other dropped metric on `M` is dropped.
3. **Relationships.** Any relationship on `M` with `X` as a local FK field drops; any
   relationship from `K` whose `to == M` and whose `join_pairs` target `X`
   drops.
4. **Filters.** Dataset-level filter strings on `M` referencing `X` are
   moved to `remove_filters`.
5. **Cross-dataset derived references.** Derived fields / metrics /
   filters on any dataset in the same datasource that resolve through the
   relationship graph to dropped `M.X` cascade-drop on the referencing side.
6. **Query-backed source_queries.** Any query-backed dataset whose stages
   transitively reference dropped `M.X` (or whole-dropped `M`) is
   whole-dropped.
7. **PK drops do NOT cascade.** A primary-key field drop only emits its
   own field-drop entry; it never expands into derived fields,
   metrics, or filters.

Cascade walking stays strictly within the parent datasource. Relationships and
references that resolve into another datasource are skipped silently.

## Re-type findings

A field whose live column exists with a conflicting type is reported as `{finding: "retype", dataset_name, data_source,
field, stored_type, live_type, live_db_type, remedy_kind, remedy, summary}` — never deleted, never applied by
`--force-clean`, printed by the CLI as a `RE-TYPE:` line — whose `remedy_kind` is `reingest` for a live list on a
table-backed dataset or a SQLite field the affinity probe widened, and otherwise `set_type` (set the field's datatype to
`live_type`), with `remedy` and `summary` its human-readable text.

A stored non-list field whose live column is a native list is one: re-ingesting re-types it to the list in place,
keeping only `count` among its `allowed_aggregations`. An `sql`-mode dataset is treated the same way, its result columns typed from the
live schema of the tables its query reads (else from the type the database
reports for the result column), so a list the query returns is found even on
BigQuery, whose driver reports a repeated result column by its element type.

## Output shape

`validate_datasets` returns a `List[ToDeleteEntry]`, where each entry is
either:

* `EditDatasetDelete` — `{tool: "edit_dataset", dataset_name, data_source,
  remove: {fields, metrics, aggregations, relationships, relationship_edges}, remove_filters,
  reasons}`. Replays directly as an `edit_dataset` call; `relationships` lists each
  dropped relationship by its target (its edge name when several relationships share that
  target) and `relationship_edges` addresses each exactly as `{to, name, join_pairs}`.
* `WholeDatasetDelete` — `{tool: "delete_dataset", dataset_name, data_source,
  reasons}`. Replays directly as a `delete_dataset` call.

If a single dataset receives both kinds, the `WholeDatasetDelete` preempts
and only the whole-dataset drop is emitted (the field-level deletes would
be no-ops once the dataset is gone).

## Surfaces

* **Engine.** `await engine.validate_datasets(data_source=...)` — read-only.
  `await engine.apply_drift_deletes(deletes)` — destructive; returns
  `ApplyDriftResult` (applied / errors / residual).
* **MCP.** `validate_datasets(data_source: Optional[str] = None)` —
  read-only, returns JSON. No apply path is exposed via MCP.
* **REST.** `POST /validate-datasets` — read-only. Query-time failures
  attributed to drift surface as **HTTP 422** with body
  `{error: "schema_drift", datasets, to_delete, original}`.
* **CLI.** `slayer validate-datasets [--datasource X] [--dataset M]
  [--format text|json] [--force-clean] [--yes] [--cardinality]`. Without `--force-clean`,
  prints the diff and exits 0. `--force-clean` prompts (or skips with
  `--yes`), applies via `apply_drift_deletes`, and exits non-zero on
  per-entry errors or non-empty residual drift. `--dataset` scopes both the
  report and the applied deletes. Unlike the engine method, an unscoped CLI
  run validates each datasource explicitly and exits 1 if any of them fails,
  rather than dropping it from the result. `--cardinality` adds an opt-in
  [relationship-arity profiling](models.md#relationship-cardinality) pass, which runs after
  any `--force-clean` apply. See the
  [CLI reference](../reference/cli.md#slayer-validate-datasets).

`--force-clean` is intentionally CLI-only — destructive auto-application
must be opt-in at the human-typed layer.

## Idempotent re-ingestion

`slayer ingest --datasource <name>` (and the equivalent MCP / REST
endpoints) is idempotent by default — re-runs are safe. For each
in-scope live table:

* No persisted dataset with that name → ingest from scratch.
* Existing `sql_table`-mode dataset → append new fields and relationships;
  *never* overwrite description, label, format, meta, or
  `allowed_aggregations` on existing entries.
* Existing `sql`-mode or `source_queries`-mode dataset with the matching
  name → skipped silently.
* A stored non-list field whose live column is a native list → re-typed
  to that list in place (description, label and meta kept, only `count`
  kept among its aggregations, `format` and granularity dropped); until
  then `validate_datasets` reports it as a `RE-TYPE` finding instead of a
  deletion, and a query failure it explains raises `SchemaDriftError`
  carrying that finding. Each stored metric, derived field, dataset filter
  or saved query the re-type breaks is listed in
  `IdempotentIngestResult.broken_dependents` and warned, and the re-type is
  saved regardless.

After the additive pass, `validate_datasets` runs against the in-scope
datasets and the result is merged into `IdempotentIngestResult.to_delete`.
`include_tables` / `exclude_tables` constrain *both* the additive pass
and the validator — excluded tables are not touched in either direction.

## SQLite affinity-probe drift

On SQLite, `validate_datasets` additionally runs the value-level affinity
probe (see [Ingestion → SQLite affinity probing](ingestion.md#sqlite-affinity-probing))
against every persisted base field with `datatype = INT`. When the probe
disagrees with the persisted type — the live storage class is `REAL`,
non-coercible `TEXT`, `BLOB`, or otherwise non-integral numeric evidence —
`validate_datasets` reports a re-type finding with the probe's verdict as
`live_type` and the re-ingest remedy, deleting nothing.

Probe failures and saturated samples produce no drift entry (a `WARNING`
is logged instead). Non-SQLite datasources skip the probe entirely.

## FK-introspection limitations

Some dialects (ClickHouse, BigQuery, Snowflake) don't expose foreign-key
metadata through the SQLAlchemy `Inspector`. On those backends, relationships
are still validated by name (the relationship target must exist as a dataset in
the datasource), but the additive ingestion pass cannot infer new relationships
from FK relationships — define relationships manually via `edit_dataset`.

## Related

* [Datasets](models.md) — `SlayerDataset` source modes, fields, metrics.
* [Ingestion](ingestion.md) — auto-ingestion details, FK rollups.
