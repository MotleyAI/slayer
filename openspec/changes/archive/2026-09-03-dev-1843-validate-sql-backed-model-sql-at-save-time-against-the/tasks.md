# Tasks

## 1. Tests first (TDD)

- [x] 1.1 Add `build_sql_model_trial_query` unit tests — wraps SQL in the
  `_sd_validate WHERE 1=0` probe and strips a single trailing `;`/whitespace;
  verify by asserting the produced text.
- [x] 1.2 Add `_is_unreachable_db_error` classification tests — TRUE for
  `DisconnectionError`, `InterfaceError`-wrapped connect failures, and messages
  "could not connect" / "connection refused" / "getaddrinfo" / "unable to open
  database file" / "connection to server at" / "login timeout", plus cloud
  type-names (`ServiceUnavailable`, `GatewayTimeout`, `TooManyRequests`,
  `DeadlineExceeded`, transport `ConnectionError`); FALSE for "no such table" /
  "syntax error" / "permission denied for table" / `BadRequest` / `Forbidden` /
  plain `ValueError`. Verify each asserts the expected bool.
- [x] 1.3 Add `engine.validate_sql_model_source` / `engine.save_model` tests over a
  seeded SQLite datasource — valid sql persists; unknown-column / syntax-error /
  unknown-table raise `ModelSqlValidationError` and do not persist; trailing-`;`
  valid sql persists. Verify via `storage.get_model` after each save.
- [x] 1.4 Add skip/gate tests — `sql_table` model and query-backed model do not
  trial-execute (monkeypatched client not called); parameterized `model.sql`
  skips (client not called) and persists; a `{var}` only in a `Column.sql` does
  NOT skip an invalid static `model.sql` (still rejects). Verify by asserting
  call/no-call and persistence.
- [x] 1.5 Add warn-and-save tests — datasource-not-configured; monkeypatched
  transient (`DisconnectionError`); monkeypatched unreachable ("could not
  connect"); monkeypatched auth ("password authentication failed") each persist
  the model. Reachable permission-denied ("permission denied for table") rejects.
- [x] 1.6 Add MCP door tests (`test_mcp_server.py`) — `create_model` sql-mode:
  valid → "created" + persisted, invalid → error string + not persisted.
  `edit_model` sql-mode: edit to invalid → error + original row intact, edit to
  valid → success. Verify via `_call` result text and `storage.get_model`.
- [x] 1.7 Add REST door tests — `POST /models` with invalid sql-mode SQL returns
  400 and the model is not persisted; valid SQL is retrievable afterward;
  `PUT /models/{name}` to invalid SQL returns 400 and leaves the original intact.
- [x] 1.8 Run the new tests and confirm they FAIL for the right reason
  (feature missing), not setup errors.

### Step-5 (Codex review of tests) coverage additions

- [x] 1.9 Prove reuse of the shared builder: capture the SQL handed to
  `get_column_types` in `validate_sql_model_source` and assert it equals
  `build_sql_model_trial_query(model.sql)`; add a schema-drift test that
  `_live_columns_for_sql_model` passes the same wrapper.
- [x] 1.10 Harden the error test — assert `ds.type` appears, a datasource
  password sentinel does NOT (no `repr(ds)` leak), and `ModelSqlValidationError`
  subclasses both `SlayerError` and `ValueError`.
- [x] 1.11 Pin classifier separation — `_is_unreachable_db_error` TRUE / 
  `_is_transient_db_error` FALSE for "unable to open database file"; the reverse
  for "database is locked" / "deadlock". Plus boundaries: bare (non-connect)
  `InterfaceError` and "400"/"403" messages are NOT unreachable.
- [x] 1.12 Assert the logging contract — parameterized skip logs INFO (naming the
  model), and each warn-and-save case logs a WARNING (naming the model); use a
  transient-only signal so the transient case is unambiguous.
- [x] 1.13 Add a `{? {var} ?}` optional-block model-SQL skip test (blocked
  placeholders also skip, not just bare `{var}`).

## 2. Shared helpers (`slayer/sql/client.py`)

- [x] 2.1 Add `build_sql_model_trial_query(inner_sql)` (trailing-`;`/whitespace
  strip + `SELECT * FROM (<inner>) AS _sd_validate WHERE 1=0`); verify 1.1 passes.
- [x] 2.2 Add `_is_unreachable_db_error(exc)` with `_UNREACHABLE_DB_ERROR_SIGNALS`
  and `_UNREACHABLE_DB_ERROR_TYPE_NAMES`, walking the orig/cause/context chain;
  keep it separate from `_is_transient_db_error`. Verify 1.2 passes.

## 3. Error type (`slayer/core/errors.py`)

- [x] 3.1 Add `ModelSqlValidationError(SlayerError, ValueError)` with a docstring
  matching the module's `(SlayerError, ValueError)` pattern.

## 4. Engine validator (`slayer/engine/query_engine.py`)

- [x] 4.1 Add top-level imports: `extract_variable_refs` (core.query),
  `build_sql_model_trial_query` / `_is_transient_db_error` / `_is_auth_failure` /
  `_is_unreachable_db_error` (sql.client), `ModelSqlValidationError` (core.errors).
- [x] 4.2 Implement `async validate_sql_model_source(self, model)` — gate to
  raw-sql (`model.sql` set, no `sql_table`, no `source_queries`); skip+INFO when
  `extract_variable_refs(model.sql)` is non-empty; warn+return when datasource is
  unset/unconfigured; else `_client_for(ds)` + `build_sql_model_trial_query` +
  `get_column_types`; classify exceptions (transient/auth/unreachable → warn;
  else raise `ModelSqlValidationError` using `ds.type` + `model.data_source` name
  only). Verify 1.3–1.5 pass.
- [x] 4.3 Call `await self.validate_sql_model_source(model)` in `save_model`
  immediately before `storage.save_model(model)`.

## 5. MCP doors (`slayer/mcp/server.py`)

- [x] 5.1 Reroute `create_model` sql-branch through `engine.save_model` (keep the
  `existed` created/replaced verb; `except`→ error string / `_friendly_db_error`).
  Verify 1.6 create tests pass.
- [x] 5.2 Reroute `edit_model` non-query branch through `engine.save_model` (unify
  with the query-backed branch; `except`→ "Validation error: {exc}"); confirm the
  post-save atomic-move guard still uses `saved_model.data_source`. Verify 1.6
  edit tests pass.

## 6. Dedup schema drift (`slayer/engine/schema_drift.py`)

- [x] 6.1 Replace the inline wrapper in `_live_columns_for_sql_model` with
  `build_sql_model_trial_query(model.sql)`; add the import. Verify existing
  `test_validate_models.py` still passes (no behavior change).

## 7. Full suite + lint

- [x] 7.1 Run `poetry run pytest -m "not integration"` and fix any failures.
- [x] 7.2 Run `poetry run ruff check slayer/ tests/` and fix any issues.
