## 1. Tests (pr-tests stage; each must fail before the fix unless noted)

- [x] 1.1 Hook unit tests: base hook returns a valid inspector `default_schema_name` without probing, runs the `SELECT current_schema()` probe only when that value is empty/non-str; `BigqueryDialect` hook returns `dataset_id` from an offline engine (`bigquery://p/ds_a?user_supplied_client=true`), runs no SQL, and returns `None` for `bigquery://p?user_supplied_client=true`; `reflect_schema_description` resolves the dataset via the hook — verify `pytest` fails without the fix
- [x] 1.2 Scope unit test: `resolve_ingest_scope(all_schemas=True)` over the offline BigQuery engine + a stub inspector (`default_schema_name=None`, schemas `ds_a`, `ds_b`) marks `ds_a` `is_default`; `default_schema_ref` returns `ds_a` — verify fails without the fix
- [x] 1.3 Drift unit tests (one per spec scenario of `models/schema-drift`): `validate_datasource` with the live connection opening the offline BigQuery engine + stub inspector, listing and introspection stubbed — bare model in `ds_a` gets no `WholeModelDelete`; dropped column → edit entry; bare name only in `ds_b` → `WholeModelDelete`; same-named `contractors` in both → bare compares to `ds_a`'s object, `ds_b.contractors` to `ds_b`'s; query-time attribution (`DriftSnapshot.live_tables`) resolves the bare model; schema-list failure fallback (`get_schema_names` raises) still resolves the bare model — verify each fails without the fix, except the fallback, which the driver already resolves to the URL dataset (a regression guard)
- [x] 1.4 Registry tests: DuckDB is the only catalog-qualifying dialect; SQLite the only `probes_integer_affinity` dialect (these pin the compliance refactor; they may pass before it)
- [x] 1.5 Live test in `tests/integration/test_integration_bigquery.py`: create (and tear down) a uniquely-named second dataset, ingest the fixture dataset with no `schema` (assert `sql_table`s are bare), assert `validate_models` reports no `WholeModelDelete` (only the edit for a column injected to prove a verdict ran) — CI only (needs `GCP_PROJECT_ID` + ADC)

## 2. Dialect hook

- [x] 2.1 Add the default-schema hook to `SqlDialect` in `slayer/sql/dialects/base.py` (inspector value, else the moved `current_schema()` probe) — verify 1.1 base cases pass
- [x] 2.2 Override it in `BigqueryDialect` (`dataset_id`, no SQL) and make `reflect_schema_description` use it — verify 1.1 BigQuery cases pass
- [x] 2.3 Add the catalog-qualifying attribute to `SqlDialect` (True on `DuckdbDialect`) — verify 1.4 passes

## 3. Engine wiring

- [x] 3.1 `schema_scope._default_schema_name` delegates to the hook via `dialect_for_sa_backend`; replace `_CATALOG_QUALIFYING_DIALECTS` with the dialect attribute; drop the `(DEV-1758)` docstring reference — verify 1.2 and 1.3 pass and `tests/test_schema_scope.py`, `tests/test_ingestion_schema_qualification.py` stay green
- [x] 3.2 Route the FK cross-schema check in `ingestion.py` through the same resolver — verify the FK ingestion tests stay green
- [x] 3.3 Replace `schema_drift`'s `datasource.type != "sqlite"` gate with `probes_integer_affinity` — verify the SQLite drift-probe tests stay green

## 4. CI and docs

- [ ] 4.1 Add a job-level `concurrency` (one repo-wide group, `cancel-in-progress: false`, `queue: max`) to `bigquery-example` in `.github/workflows/ci.yml` — verify the YAML parses and the PR's BigQuery job runs green
- [x] 4.2 One sentence in `docs/configuration/datasources.md`: the URL dataset is BigQuery's default and bare table names resolve there

## 5. Gates

- [x] 5.1 Full unit suite `poetry run pytest -m "not integration"`, the CI integration invocation, `ruff check slayer/ tests/`, `basedpyright` (no new errors), `la-arch-check` — all green
