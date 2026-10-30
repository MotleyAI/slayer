## Why

On BigQuery, SLayer never learns the connection's default dataset (the `<dataset>` in
`bigquery://<project>/<dataset>`): `sqlalchemy-bigquery` keeps it in `dialect.dataset_id`
instead of reporting it as `default_schema_name`, and the `SELECT current_schema()` fallback
is not BigQuery SQL. Once the project holds a second dataset, drift checks stop resolving
bare-named models: `validate_models` reports every ingested model as a `WholeModelDelete`
(`--force-clean` would delete them), and query-time drift attribution blames a whole-model
delete instead of the real cause. It also makes the BigQuery CI job fail whenever two runs
overlap in the shared GCP project.

## What Changes

- The connection's default schema is resolved by one `SqlDialect` hook; the base keeps
  today's behaviour (inspector default, else the `current_schema()` probe) and
  `BigqueryDialect` returns the URL dataset. Every default-schema lookup (ingest scope,
  drift, the FK cross-schema check, the dataset description) goes through it.
- Dialect-name checks in the touched files move into dialect attributes: DuckDB's
  catalog-qualified schema tokens, SQLite's integer-affinity probe gate in drift.
- The BigQuery CI job is serialized across runs (a repo-wide concurrency group that
  queues, never cancels), and gains a live drift test with a second dataset present.
- Docs: the URL dataset is BigQuery's default; bare table names resolve there.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/schema-drift`: adds the requirement that a bare `sql_table` resolves in the
  connection's default schema, however many schemas the database holds.

## Impact

- Code: `slayer/sql/dialects/base.py`, `bigquery.py`, `duckdb.py`;
  `slayer/engine/schema_scope.py`, `schema_drift.py`, `ingestion.py`.
- CI: `.github/workflows/ci.yml` (`bigquery-example` concurrency).
- Tests: unit tests (no network) for the hook, scope resolution and drift;
  `tests/integration/test_integration_bigquery.py` live drift test.
- Docs: `docs/configuration/datasources.md`.
- Behaviour: a bare-stored BigQuery model re-ingested with an explicit `--schema` naming
  the URL dataset is now healed to the qualified `sql_table` (the default-schema twin rule
  other dialects already follow).
