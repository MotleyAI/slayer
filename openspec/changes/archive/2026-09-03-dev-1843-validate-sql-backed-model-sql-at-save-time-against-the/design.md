## Context

See proposal.md — Why. The trial-execute wrapper already exists in
`schema_drift._live_columns_for_sql_model` (`SELECT * FROM (<sql>) AS
_sd_validate WHERE 1=0`, trailing-`;` stripped) but collapses every failure to
`None`. `SlayerSQLClient.get_column_types()` additionally re-wraps its input as a
dialect-appropriate `SELECT * FROM (<sql>) AS _types LIMIT 0/1` (T-SQL uses `TOP`).
`slayer/sql/client.py` already carries the error classifiers `_is_transient_db_error`
and `_is_auth_failure`, and is imported at top level by both `query_engine` and
`schema_drift` (which import each other only lazily).

## Goals / Non-Goals

- Goal: reject a raw-`sql` model at save time only when a **reachable** datasource
  gives a definite rejection; never block a valid author on an inconclusive result.
- Non-Goal: validating parameterized (`{var}`) source SQL, `sql_table` models,
  query-backed models, or ingestion/YAML-load saves.
- Non-Goal: surfacing warn-and-save skips to the caller (log-only; no
  response-shape change).

## Decisions

- **Single canonical door.** The check lives in `engine.save_model` (covers REST +
  CLI). Both MCP doors (`create_model`, `edit_model`) reroute their non-query save
  through `engine.save_model` rather than `storage.save_model`. Alternative — a
  standalone validator called at each door — was rejected: rerouting also gives the
  MCP doors the normalization + Mode-A-join validation REST/CLI already have, and
  keeps one validation path. For non-`source_queries` models `save_model` does not
  recompute `data_source`, so `edit_model`'s post-save atomic-move (delete old
  source row on rename/ds-change) is unaffected.

- **Reject vs warn = "did a reachable backend reject it?".** On a trial-execute
  exception: if `_is_transient_db_error` OR `_is_auth_failure` OR the new
  `_is_unreachable_db_error` matches → warn-and-save; otherwise the datasource was
  reached and rejected the statement → raise `ModelSqlValidationError`
  (a `ValueError` subclass, so REST→400 / CLI→exit-1 / MCP→error-string all work).
  This biases toward the product decision "never block a valid author," while a
  genuine syntax / missing-object / permission error on a reachable DB still
  rejects.

- **`_is_unreachable_db_error` is narrow and separate.** Kept out of
  `_is_transient_db_error` so `execute()` retry semantics are untouched. Matches
  `DisconnectionError` plus connection-establishment message signals (`could not
  connect`, `connection refused`, host-resolution, `unable to open database file`,
  `connection to server at`, `login timeout`) and a curated cloud type-name set
  (`ServiceUnavailable`/503, `GatewayTimeout`/504, `TooManyRequests`/429,
  `DeadlineExceeded`, transport `ConnectionError`). Deliberately excludes blanket
  `InterfaceError`, bare `timed out`, `BadRequest`/400, and `Forbidden`/403 — those
  can be real rejections, so they must stay rejectable (Codex MAJOR 2/3, MINOR 4).

- **Placeholder skip keys on `model.sql` alone.** Gate on
  `extract_variable_refs(model.sql)` (`(bare, blocked)`), NOT
  `extract_model_variables(model)` which also scans `Column.sql` / `Column.filter` /
  `model.filters`. A `{var}` in a column expression must not suppress validating a
  static, invalid `model.sql` (Codex MAJOR 1). Skip is logged at INFO.

- **Reuse, don't duplicate.** Extract `build_sql_model_trial_query(inner_sql)` into
  `slayer/sql/client.py` (the trailing-`;` strip + `_sd_validate` wrapper);
  `_live_columns_for_sql_model` calls it. The resulting double-wrap
  (`_sd_validate` inside `get_column_types`' `_types`) is the exact shape schema
  drift already ships across Tier-1 dialects, so it carries no new dialect risk.

- **No secret leakage.** The error message and logs use `model.data_source` (name)
  and `ds.type` only — never `repr(ds)`, which could expose connection config
  (Codex MAJOR 4).

## Risks / Trade-offs

- [A definite-but-unrecognized transient error (novel driver message / type)
  rejects a valid model during an outage] → the classifier errs toward warn via
  three complementary matchers; new signals are cheap to add. Negative tests pin
  the reject side.
- [A reachable backend that returns a connection-shaped message mid-execution
  warns instead of rejecting a truly bad SQL] → signals are connection-establishment
  phrases unlikely on a `WHERE 1=0` probe; acceptable given the product bias.
- [Rerouting `edit_model` through `engine.save_model` changes a delicate path] →
  verified the atomic-move guard uses `saved_model.data_source`, unchanged for
  non-query models; covered by edit-door tests.
- [Every sql-mode save now does a live round-trip] → intended; the whole point of
  the issue. Parameterized and non-sql-mode models skip it.

## Migration Plan

Additive; no data migration. Rollback = revert. Existing persisted models are
untouched (validation runs only on new create/edit through the engine).
