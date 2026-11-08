## Context

See proposal.md — Why. `slayer/sql/client.py` executes rendered SQL at four
sites (`_execute_sql_async`, `_execute_sql_sync`, `_get_column_types_async`,
`_get_column_types_sync`) and its own constant session statements (timeout
SETs, `SET TRANSACTION READ ONLY`) at eleven more, all via `conn.execute(sa.text(…))`.
Rendered SQL never carries bind parameters, so `text()` adds only its regex
hazard. Every other `text()` in the package is SLayer-authored introspection
SQL, some with real bind parameters — untouched.

Verified in the pinned environment (SQLAlchemy 2.0.49): `exec_driver_sql` with
`no_parameters=True` reaches `cursor.execute(statement)` with no parameter
argument on every installed adapter — psycopg2, asyncpg (maps `None` to `()`),
aiomysql, BigQuery DBAPI, Snowflake connector, ClickHouse http/native,
duckdb-engine. SQLite reproduces the production error through `text()` and
succeeds through the driver call.

## Goals / Non-Goals

**Goals:**
- Make the bug class unrepeatable inside the client: one door, `text()` gone.
- Preserve `%` semantics byte-for-byte at the server.

**Non-Goals:**
- The client's inline `db_type` branching for timeout SETs (against sql §3.2,
  dialect quirks live only in `dialects/`, duplicated by hand across the sync
  and async paths) — filed separately, blocked by this change.
- Driver-level placeholder parsing (oracledb `:name`, pyodbc `?`): DBAPI and
  ODBC parsers tokenize SQL and respect string literals, unlike the `text()`
  regex, so the reported shape is safe on every driver; a bare marker outside a
  literal is not valid SQL for those dialects in the first place.

## Decisions

- **D1 — one verbatim door, everything through it.** `_exec_verbatim(conn, sql)`
  and `_exec_verbatim_async(conn, sql)` wrap
  `conn.exec_driver_sql(sql, execution_options={"no_parameters": True})`; all
  fifteen statement executions in the client route through the pair and
  `sa.text` disappears from the module. *Alternatives:* fix the two named sites
  (leaves the type probes broken for the same SQL, four sites agreeing by hand);
  helper for the four rendered sites only (two mechanisms in one function, the
  invariant checkable only by review). Constant SETs move too because the DBAPI
  call is identical and the resulting invariant — no `text(` in the module — is
  grep-simple.
- **D2 — `no_parameters=True` stays on.** Without it SQLAlchemy passes an empty
  parameter set and pyformat drivers (psycopg2) interpolate `%`, so `'%Y-%m'`
  fails. With it the driver receives a single `%` and leaves it alone — the same
  server-side text as today's compile-time doubling plus driver un-doubling.
  BigQuery's SQLAlchemy dialect uses the `named` paramstyle, so `%` was never
  doubled there; its DBAPI un-doubles `%%` with or without parameters — identical
  before and after. Snowflake's dialect switches empty-sequence interpolation off
  for uncompiled statements — also verbatim.
- **D3 — arc42 sql §3 item 13 (approved verbatim):** "**Rendered SQL executes
  verbatim**: `client.py` hands every statement to the DBAPI unchanged through
  one door (`exec_driver_sql`, no parameters); SQLAlchemy `text()` never touches
  rendered SQL — `:name` is never a bind parameter, `%` never a format
  directive. [enforced: test:tests/test_dev1933_verbatim_execution.py]". The
  "unchanged" is exact at SLayer's boundary; a DBAPI's own escape contract is the
  DBAPI's, today and after.
- **D4 — spec placement:** new node-owned group `openspec/specs/sql/execution`;
  `architecture/index.yaml` `sql` node gains `specs: [sql]` in the archive commit
  (arch_check's `spec-mapping` requires the group on disk, which only happens at
  archive). *Alternative:* under the cross-cutting `queries` group — rejected,
  those specs describe query semantics, not the wire, and the owning node had no
  spec group.
- **D5 — guard shape (Codex F6):** the test pins (a) no `text(` call anywhere in
  the client module and (b) no `exec_driver_sql` / connection `execute` call
  outside the helper pair. No call counting — a refactor that adds a third door
  is exactly what should trip it; harmless refactors inside the helpers do not.
- **D6 — coverage (Codex F1/F2/F4/F5):** unit doors on in-memory SQLite plus an
  end-to-end `ModelExtension` regex column through the query engine; Postgres
  integration on real asyncpg (`execute`, `get_column_types`) and real psycopg2
  (`execute_sync`) with `~ '(?i)(?:…)'` and `LIKE '%x%'` plus the end-to-end
  `REGEXP_REPLACE` shape from the issue; one focused case per Tier-1 suite
  (DuckDB, MySQL, ClickHouse, SQL Server, BigQuery, Snowflake) through their
  existing engine fixtures. MySQL/ClickHouse/SQL Server run only in their
  path-gated CI workflows (Docker unavailable locally); BigQuery/Snowflake only
  when CI holds credentials.
- **Rejected (Codex F3):** keeping the constant SETs on `text()` — the SETs hold
  neither `:` nor `%`, the DBAPI call is the same, and every per-dialect case
  emits them live before its query.

## Risks / Trade-offs

- [A driver treats a parameter-less execute differently from an empty-set one]
  → every installed adapter read; Tier-1 suites exercise the live path.
- [Existing mocks assert on `conn.execute`] → the Snowflake timeout tests are
  retargeted to `exec_driver_sql` with identical counts (consented); no other
  test mocks the connection's execute.
- [Docker suites cannot be run before push] → they are path-gated on their test
  files, which this change touches, so they run on the PR.
