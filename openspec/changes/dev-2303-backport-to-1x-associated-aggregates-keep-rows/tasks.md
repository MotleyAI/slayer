## 1. Tests (pr-tests; all fail before implementation)

- [x] 1.1 Port `main`'s `tests/_dev2216_fixtures.py` subset (keyless and keyed Ossie models, `ossie_engine`, `query`/`measure`/`cells`/`kinds` helpers, expected-value constants) to `1.x`, adapted to `1.x`'s engine helpers; verify it imports and the keyed models execute
- [x] 1.2 `tests/test_dev2216_keyless_association.py`, explicit `to_many_handling: "associate"` on SQLite and DuckDB: keyless home matches the keyed model (`sum`, `wsum`, `weighted_avg`); duplicate rows count separately while fan-out copies count once; joined filter with fan-out; filtered keyless home numbers its filtered rows; a `__regroup__row_id` column does not shadow the row identity; no column can take `ROW_IDENTITY_COLUMN`; attributable keyless home takes the plain path (same values as broadcast, no association warning); verify each fails on `1.x` before implementation where behaviour changes
- [x] 1.3 Same file: Postgres execution of the duplicate-row case via `pytest-postgresql` (unit, no integration marker)
- [x] 1.4 Same file: every one of the 13 dialects renders `ROW_NUMBER` for a keyless-home association, with `ORDER BY` on tsql/snowflake/databricks/spark/oracle; a PK home's SQL has no `ROW_NUMBER`; a unique-only home's SQL is numbered
- [x] 1.5 Same file: #598's matrix — reflected nullable `UNIQUE source_ref` on `sales`, `tags` with a duplicated `shared` row; nullable vs all-non-NULL × rooted at `sales` (local) vs `tags` (cross-model); `sum(amount)` = shared 80 / other 90, `count(sales.*)` = 3 / 1, `weighted_avg(amount, weight=weight)` = 30 / 90; SQLite and DuckDB; `associated` warning present
- [x] 1.6 Same file: a home with a primary key and a nullable standalone `unique` column — SQL not numbered; executed values keyed on the PK keep every NULL-unique row
- [x] 1.7 Align existing tests (approved in plan): `tests/test_dev1841_association_errors.py::test_root_without_unique_key_fails` becomes `test_root_without_unique_key_associates` asserting executed values; `tests/test_dev1894_query_type_errors.py` "association" case triggers on `sum(customers.spend, window='90d')` with a `customers.signup_at` month time dimension over `dev1840_models()` (as on `main`); `tests/_dev1871_raise_ledger.py` drops the `check_association_root_unique_key` row

## 2. Implementation (pr-implement)

- [x] 2.0 Re-read MotleyAI/slayer#598's final diff (`gh pr diff 598`); if it changed since 2026-11-16, STOP and present the plan delta before coding
- [x] 2.1 `slayer/sql/naming.py`: `ROW_IDENTITY_COLUMN = "__slayer_row_id"`, `with_row_identity`, `is_row_identity` (from `main`); verify 1.2's reserved-name test passes
- [x] 2.2 `slayer/sql/dialects/`: `SqlDialect.row_number()` + `row_number_needs_order` (base), flag on Snowflake, Oracle, Databricks, Spark; T-SQL override with `ORDER BY (SELECT NULL)`; verify 1.4 passes
- [x] 2.3 `slayer/sql/generator.py`: `_producer_render_bundle` numbers the root when the `AssociationProducerKernel`'s `entity_keys` include the row identity; the relation door wraps a model carrying the row-identity column in `_numbered_relation` (built from the given alias) after filters and `_stamp_model`
- [x] 2.4 `slayer/engine/compile/stages.py`: `_association_arm` keys on PK columns, else `with_row_identity(root)`; list-only association stays key-free; row-valued parameter checks read the numbered home (local home → numbered root); `_check_attached_params_determined` grain display shows the PK else "`<model>` rows"; verify 1.2, 1.5, 1.6 pass
- [x] 2.5 `slayer/engine/elaborate_env.py`: delete `check_association_root_unique_key` and its import; verify 1.7 passes
- [x] 2.6 Re-bless any SQL golden changed by numbering a unique-only home, showing the diff

## 3. Docs, release, gates

- [x] 3.1 `docs/concepts/queries.md:792`: eligibility no longer needs a key; one sentence that a home without a primary key is told apart row by row, including one with a nullable unique column
- [x] 3.2 Bump `pyproject.toml` version to 1.8.5
- [x] 3.3 Run the local test scope, `ruff check`, `basedpyright` (no new errors vs baseline), `la-arch-check`, `openspec validate dev-2303-backport-to-1x-associated-aggregates-keep-rows --strict`; all green
- [ ] 3.4 Open the PR against base `1.x` (not `main`) after the user's go-ahead
