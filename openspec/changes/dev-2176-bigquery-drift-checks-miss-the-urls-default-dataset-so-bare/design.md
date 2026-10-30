## Context

See proposal.md (Why). The connection's default schema is a per-dialect quirk, yet it is
answered outside `slayer/sql/dialects/` (contra `python.sql` arc42 P2, "Dialect quirks live
only in `dialects/`"), by two paths that must agree by hand:

- `schema_scope._default_schema_name`: `inspector.default_schema_name`, else a hard-coded
  `SELECT current_schema()`. It feeds `resolve_ingest_scope` (ingest scope, `validate_models`,
  query-time attribution via `schema_drift._live_schema_refs`) and `default_schema_ref`
  (`_default_schema_membership`, `introspect_utils`, `_live_schema_refs`'s
  enumeration-failure fallback).
- `ingestion.py` FK grouping reads `getattr(inspector, "default_schema_name", None)` itself.

Driver survey (installed sources): Postgres and DuckDB report `current_schema()`, SQLite
`main`, MySQL `DATABASE()`, SQL Server `SCHEMA_NAME()`, Snowflake the session
`current_schema`, ClickHouse `currentDatabase()`, Trino the URL schema — all through
SQLAlchemy's `default_schema_name`. BigQuery alone keeps its default (the URL dataset) in
`dialect.dataset_id` and reports none; `BigqueryDialect.reflect_schema_description` already
reads `dataset_id` for itself.

Reproduced on DuckDB with the default forced to `None` and a bare model `contractors`:
1 schema → `[]` either way; 2 schemas + default known → `[]`; 2 schemas + default hidden →
`[WholeModelDelete]`.

## Goals / Non-Goals

**Goals:**
- One owner for "the connection's default schema": a `SqlDialect` hook every lookup uses.
- Bare BigQuery models resolve in drift however many datasets the project holds.
- The BigQuery CI job is stable under overlapping runs.

**Non-Goals:**
- No new fail-closed rule for a bare name when the default is genuinely unknown (e.g. a
  BigQuery URL without a dataset): the database cannot resolve that bare name either, so the
  delete verdict is accurate.
- No change to how drift handles a schema that disappears between enumeration and listing
  (it still fails closed with `IntrospectionUnavailable`); CI serialization removes that race
  for the shared project instead.

## Decisions

1. **A dialect hook owns the default schema** (default). `SqlDialect` gains a hook taking
   the SQLAlchemy engine and inspector and returning the connection's default schema or
   `None`. Base: the inspector's `default_schema_name` when it is a non-empty `str` (a mock's
   attribute is ignored), else the `SELECT current_schema()` probe — moved verbatim from
   `schema_scope` so Tier-2 drivers (Spark/Hive) keep it. `BigqueryDialect` overrides it to
   return the SQLAlchemy dialect's `dataset_id` (non-empty `str` only) and runs no SQL.
   `reflect_schema_description` reuses the hook. Alternative rejected: a BigQuery branch in
   `schema_scope` (a dialect quirk outside `dialects/`).
2. **`schema_scope._default_schema_name` delegates to the hook** (default), looked up with
   `dialect_for_sa_backend(<engine>.dialect.name)`; an undeclared backend gets the base hook.
   One path then feeds every caller listed in Context.
3. **The FK cross-schema check uses the same resolver** (default) instead of reading the
   inspector itself. No live behaviour change (BigQuery reflects no FKs); it removes the
   second code path.
4. **Touched-file compliance** (default): `schema_scope._CATALOG_QUALIFYING_DIALECTS =
   {"duckdb"}` becomes a `SqlDialect` attribute (True on `DuckdbDialect`), read through the
   registry wherever the name set was consulted; `schema_drift`'s
   `datasource.type != "sqlite"` gate becomes the existing
   `dialect_for_ds_type(...).probes_integer_affinity`; the `(DEV-1758)` reference leaves the
   `schema_scope` module docstring. Splitting these out was rejected (rejected (codex): the
   touched-file rule requires them here); registry tests pin them instead (codex).
5. **CI serialization** (user). The `bigquery-example` job joins one repo-wide concurrency
   group with `cancel-in-progress: false` and `queue: max`, so overlapping runs queue (up to
   100 pending) and none is cancelled. It is the only job using the shared GCP project.
6. **Vanished-schema re-check not adopted** (user). Re-reading the schema list after a
   listing failure would not be a guarantee — BigQuery's dataset listing can lag a
   deletion — so the race is removed in CI by serialization (Decision 5) instead.
7. **Live test owns its second dataset** (default). The new integration test creates (and
   tears down) a uniquely-named second dataset rather than relying on another run's, ingests
   the fixture dataset with no `schema` (bare `sql_table`s) and asserts `validate_models`
   reports no delete.
8. **Unit tests build BigQuery engines offline** (codex). `create_engine("bigquery://p/ds")`
   builds a client and needs ADC; tests use `bigquery://p/ds?user_supplied_client=true`,
   which sets `dataset_id` without credentials or network.
9. **Hook precedence is pinned** (codex): base — a valid inspector value wins and the probe
   runs only when it is empty; BigQuery — returns `dataset_id`, runs no SQL, and returns
   `None` when the URL names no dataset. No new whitespace rejection (would be new behaviour).
10. **Same-named objects across schemas** (codex): a scenario and test pin that a bare name
    resolves to the default schema's object while the other schema's twin is reachable only
    qualified.
11. **Enumeration-failure fallback** (codex): a regression test covers
    `_live_schema_refs`'s fallback (which calls `default_schema_ref` separately).
12. **Docs** (default): one sentence in `docs/configuration/datasources.md`.
13. **Re-ingest healing** (default): a bare-stored BigQuery model re-ingested with an
    explicit `--schema` naming the URL dataset is healed to the qualified `sql_table` — the
    default-schema-twin rule other dialects already follow; previously skipped as
    "kept unqualified".

## Risks / Trade-offs

- [The base probe stays text SQL in `dialects/base.py`] → kept verbatim to avoid regressing
  untestable Tier-2 drivers; BigQuery no longer runs it.
- [A schema deleted mid-drift-pass still refuses the run] → out of scope (Decision 6); CI
  serialization covers the shared project.
- [Serialized BigQuery CI lengthens wall-clock for overlapping PRs] → accepted; the job is
  short and `queue: max` never cancels.
- [`validate_models` on BigQuery enumerates every dataset in the project] → unchanged
  behaviour; the live test tolerates unrelated datasets because bare names now resolve only
  in the URL dataset.
