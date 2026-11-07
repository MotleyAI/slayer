# Schema drift

When a live database schema changes — a column is dropped, a type bucket
flips, a table goes away — persisted SLayer models stop being valid. Until
the change is reconciled, queries against the affected models fail with
raw DBAPI errors that don't tell you which model is broken or what to fix.

SLayer surfaces drift as a first-class concept across three behaviours:

1. **`validate_models`** — read-only diff that returns the minimal list of
   *deletes* needed to make persisted models valid against the live state.
2. **Idempotent re-ingestion** — additive only; never overwrites
   user-customised entries. Returns a combined report of what was added
   plus what `validate_models` says still needs deleting.
3. **`SchemaDriftError`** — query-time wrap that checks for drift when a
   query fails and surfaces the structured drift payload instead of the
   raw DBAPI message. It checks only the models the failed statement read,
   reuses live-schema facts for up to 60 seconds, and its `models` lists
   the drifted models.

Use `validate_models` to inspect drift; use `slayer validate-models
--force-clean` (CLI only) to apply the deletes.

## How drift is detected

Drift is computed per source mode:

* **`sql_table`** — open the datasource via SQLAlchemy `Inspector`, read
  live columns, types, primary keys, and foreign keys. Compare against
  `model.columns` and `model.joins`. Live types are mapped through SLayer's
  coarse buckets (`number` / `string` / `boolean` / `temporal`):
  `INTEGER` and `FLOAT` collapse to `number`, `DATE` and `TIMESTAMP`
  collapse to `temporal`. A bucket change on a column that still exists is a
  [re-type finding](#re-type-findings), never a delete — switching `BIGINT` ↔
  `FLOAT` is *not* drift, but `NUMBER` ↔ `STRING` is.
* **`sql`** — trial-execute `SELECT * FROM (<model.sql>) AS _sd_validate
  WHERE 1=0`. Successful execution exposes cursor metadata (column names
  and types), which goes through the same column-level diff. A failed
  trial-execute means the SQL itself is broken — emit a whole-model drop.
  A column whose stored type conflicts with its result column's (e.g. one an
  older SLayer probe typed) is a re-type finding.
* **`source_queries`** (query-backed) — never re-introspected directly.
  Treated as a *cascade target*: when validation against the underlying
  datasource produces a column or model drop, any query-backed model that
  transitively references the dropped thing gets a whole-model drop.

A model whose listed table cannot be read, or whose datasource cannot be
connected to, gets no verdict rather than a drop.

## Cascade rules

A drop on `M.X` cascades through:

1. **Derived columns on M.** Any `Column.sql` on `M` that references `X`
   (via sqlglot AST walk) is dropped, transitively across chains of
   derived columns.
2. **Measures on M.** Any `ModelMeasure.formula` on `M` referencing `X`
   or any other dropped measure on `M` is dropped.
3. **Joins.** Any join on `M` with `X` as a local FK column drops; any
   join from `K` whose `target_model == M` and `foreign_column == X`
   drops.
4. **Filters.** Model-level filter strings on `M` referencing `X` are
   moved to `remove_filters`.
5. **Cross-model derived references.** Derived columns / measures /
   filters on any model in the same datasource that resolve through the
   join graph to dropped `M.X` cascade-drop on the referencing side.
6. **Query-backed source_queries.** Any query-backed model whose stages
   transitively reference dropped `M.X` (or whole-dropped `M`) is
   whole-dropped.
7. **PK drops do NOT cascade.** A primary-key column drop only emits its
   own `drop_column` entry; it never expands into derived columns,
   measures, or filters.

Cascade walking stays strictly within the parent datasource. Joins and
references that resolve into another datasource are skipped silently.

## Re-type findings

A column whose live column exists with a conflicting type is reported as `{finding: "retype", model_name, data_source,
column, stored_type, live_type, live_db_type, remedy_kind, remedy, summary}` — never deleted, never applied by
`--force-clean`, printed by the CLI as a `RE-TYPE:` line — whose `remedy_kind` is `reingest` for a live list on a
table-backed model or a SQLite column the affinity probe widened, and otherwise `set_type` (set the column's type to
`live_type`), with `remedy` and `summary` its human-readable text.

## Output shape

`validate_models` returns a `List[ToDeleteEntry]`, where each entry is
either:

* `EditModelDelete` — `{tool: "edit_model", model_name, data_source,
  remove: {columns, measures, aggregations, joins, join_edges}, remove_filters,
  reasons}`. Replays directly as an `edit_model` call; `joins` lists each
  dropped join by its target (its edge name when several joins share that
  target) and `join_edges` addresses each exactly as `{target_model, name, join_pairs}`.
* `WholeModelDelete` — `{tool: "delete_model", model_name, data_source,
  reasons}`. Replays directly as a `delete_model` call.

If a single model receives both kinds, the `WholeModelDelete` preempts
and only the whole-model drop is emitted (the column-level deletes would
be no-ops once the model is gone).

## Surfaces

* **Engine.** `await engine.validate_models(data_source=...)` — read-only.
  `await engine.apply_drift_deletes(deletes)` — destructive; returns
  `ApplyDriftResult` (applied / errors / residual).
* **MCP.** `validate_models(data_source: Optional[str] = None)` —
  read-only, returns JSON. No apply path is exposed via MCP.
* **REST.** `POST /validate-models` — read-only. Query-time failures
  attributed to drift surface as **HTTP 422** with body
  `{error: "schema_drift", models, to_delete, original}`.
* **CLI.** `slayer validate-models [--datasource X] [--model M]
  [--format text|json] [--force-clean] [--yes] [--cardinality]`. Without `--force-clean`,
  prints the diff and exits 0. `--force-clean` prompts (or skips with
  `--yes`), applies via `apply_drift_deletes`, and exits non-zero on
  per-entry errors or non-empty residual drift. `--model` scopes both the
  report and the applied deletes. Unlike the engine method, an unscoped CLI
  run validates each datasource explicitly and exits 1 if any of them fails,
  rather than dropping it from the result. `--cardinality` adds an opt-in
  [join-arity profiling](models.md#join-cardinality) pass, which runs after
  any `--force-clean` apply. See the
  [CLI reference](../reference/cli.md#slayer-validate-models).

`--force-clean` is intentionally CLI-only — destructive auto-application
must be opt-in at the human-typed layer.

## Idempotent re-ingestion

`slayer ingest --datasource <name>` (and the equivalent MCP / REST
endpoints) is idempotent by default — re-runs are safe. For each
in-scope live table:

* No persisted model with that name → ingest from scratch.
* Existing `sql_table`-mode model → append new columns and joins;
  *never* overwrite description, label, format, meta, or
  `allowed_aggregations` on existing entries.
* Existing `sql`-mode or `source_queries`-mode model with the matching
  name → skipped silently.
* A stored non-list column whose live column is a native list → re-typed
  to that list in place (description, label and meta kept, only `count`
  kept among its aggregations, `format` and granularity dropped); until
  then `validate_models` reports it as a `RE-TYPE` finding instead of a
  deletion, and a query failure it explains raises `SchemaDriftError`
  carrying that finding. Each stored measure, derived column, model filter
  or saved query the re-type breaks is listed in
  `IdempotentIngestResult.broken_dependents` and warned, and the re-type is
  saved regardless.

After the additive pass, `validate_models` runs against the in-scope
models and the result is merged into `IdempotentIngestResult.to_delete`.
`include_tables` / `exclude_tables` constrain *both* the additive pass
and the validator — excluded tables are not touched in either direction.

## SQLite affinity-probe drift

On SQLite, `validate_models` additionally runs the value-level affinity
probe (see [Ingestion → SQLite affinity probing](ingestion.md#sqlite-affinity-probing))
against every persisted base column with `type = INT`. When the probe
disagrees with the persisted type — the live storage class is `REAL`,
non-coercible `TEXT`, `BLOB`, or otherwise non-integral numeric evidence —
`validate_models` reports a re-type finding with the probe's verdict as
`live_type` and the re-ingest remedy, deleting nothing.

Probe failures and saturated samples produce no drift entry (a `WARNING`
is logged instead). Non-SQLite datasources skip the probe entirely.

## FK-introspection limitations

Some dialects (ClickHouse, BigQuery, Snowflake) don't expose foreign-key
metadata through the SQLAlchemy `Inspector`. On those backends, joins
are still validated by name (the join target must exist as a model in
the datasource), but the additive ingestion pass cannot infer new joins
from FK relationships — define joins manually via `edit_model`.

## Related

* [Models](models.md) — `SlayerModel` source modes, columns, measures.
* [Ingestion](ingestion.md) — auto-ingestion details, FK rollups.
