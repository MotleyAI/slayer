## Why

A model created with a raw `sql` source (not `sql_table` or `source_queries`) is
persisted without any check of that SQL — not even a parse. SQL that is invalid
for the datasource's dialect saves silently and only blows up on first query. A
sqlglot parse is insufficient (it is deliberately permissive); only a live
trial-execute against the backend is the real dialect check.

## What Changes

- At save time, a raw-`sql` model's source SQL is trial-executed against the live
  datasource (`SELECT * FROM (<sql>) AS _sd_validate WHERE 1=0`, the same wrapper
  schema drift already uses). A backend rejection of a **reachable** datasource
  rejects the save with a clear error.
- Inconclusive results — datasource unreachable, transient error, auth failure,
  or datasource not configured — **warn-and-save** (log only; the save proceeds).
- Parameterized source SQL (a `model.sql` containing `{var}` / `{? ?}`) is
  **skipped** (logged at INFO), because a sentinel-filled placeholder in
  identifier position would false-reject a valid model.
- The check is wired at the single canonical save door, `engine.save_model`, so
  it covers REST `POST/PUT /models` and CLI `slayer models create`. The two MCP
  doors that bypassed the engine — `create_model` and `edit_model` — are rerouted
  through `engine.save_model`, so they also normalize, validate Mode-A joins, and
  run this SQL check.
- The trial-execute wrapper (with its trailing-`;` strip) is extracted from
  `_live_columns_for_sql_model` into a shared helper and reused by both schema
  drift and save-time validation.
- Ingestion / YAML-load saves (`storage.save_model(..., _validate=False)`) are
  **out of scope** and unchanged.

## Capabilities

### New Capabilities
- `models/save-validation`: save-time validation of a model's source before it is
  persisted, including the live trial-execute of a raw-`sql` model's SQL against
  its datasource dialect and the reject-vs-warn policy for unreachable backends.

### Modified Capabilities
<!-- None: no existing spec under openspec/specs/ describes model save behavior. -->

## Impact

- `slayer/sql/client.py`: new shared `build_sql_model_trial_query()` and a new
  `_is_unreachable_db_error()` classifier (alongside `_is_transient_db_error` /
  `_is_auth_failure`).
- `slayer/core/errors.py`: new `ModelSqlValidationError(SlayerError, ValueError)`.
- `slayer/engine/query_engine.py`: new `validate_sql_model_source()`, called by
  `save_model`.
- `slayer/mcp/server.py`: `create_model` and `edit_model` reroute their non-query
  save through `engine.save_model`.
- `slayer/engine/schema_drift.py`: `_live_columns_for_sql_model` reuses the
  extracted wrapper (no behavior change).
- User-visible: creating/editing a sql-mode model with invalid SQL against a
  reachable datasource now fails (REST 400 / CLI exit-1 / MCP error string)
  instead of persisting.
