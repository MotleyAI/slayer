# Capture findings — `flight-sql-jdbc-driver` 18.3.0

Captured by `tests/flight/capture_dbt_jdbc.py` (now supports two modes:
`--mode stub` against `CaptureFlightServer`, `--mode live` against the
real `RecordingFlightSqlServer` backed by the Jaffle Shop demo).

The checked-in corpus at `capture-latest.jsonl` (58 RPCs) is from a
`--mode live` run — every prepared-statement round-trip is fully filled
in (the original Phase 1.0 stub capture had 39 RPCs and aborted partway
through because the stub returned empty `ActionCreatePreparedStatementResult`s
that the driver refused).

## RPC mix observed

```
10 get_flight_info     — all DatabaseMetaData.* introspection commands
10 do_get              — ditto (one per get_flight_info)
19 do_action           — every Statement.executeQuery + Connection.commit/rollback
 0 GetSqlInfo          — never issued (driver introspected via DatabaseMetaData
                         alone; if we want to advertise capabilities we must
                         do so via well-typed empty/canned responses to the
                         per-command introspection calls)
```

## Major design impacts vs the original spec

### 1. SQL flows through prepared statements, not `CommandStatementQuery`

The original spec's §4.2 table marked
`CommandPreparedStatementQuery` /
`ActionCreatePreparedStatementRequest` /
`ActionClosePreparedStatementRequest` as **stubbed** with
`Unimplemented`. **In practice the upstream JDBC driver issues
EVERY `Statement.executeQuery` — including `SELECT 1`, `BEGIN`,
`SHOW search_path` — via the prepared-statement path**, not via
`CommandStatementQuery`.

Stubbing those as `Unimplemented` would break **all** SQL.

Action: Phase 1 must promote prepared-statement handlers from
"stubbed" to **first-class real handlers**. Spec + Linear updated.

### 2. Stateless ticket design extended to prepared statements

The original spec §6.4 made the Flight `Ticket` carry the original
SQL string for statelessness. For the prepared-statement flow we
extend the same trick: the `prepared_statement_handle` IS the SQL
string (UTF-8 bytes, possibly with a small length-prefix nonce
for uniqueness across concurrent same-SQL prepares). On
`ActionClosePreparedStatementRequest` the server simply ignores
the request body (no per-handle state to evict). On
`get_flight_info(CommandPreparedStatementQuery{handle})` and
`do_get(ticket)` the server decodes the handle, re-runs the
translator pipeline, and either returns canned probe / INFORMATION_SCHEMA
data or executes the SlayerQuery.

Side effect: three translator runs per BI query instead of two
(create-prepared + flight-info + do_get), each doing a fresh
sqlglot parse. The execution path stays at two database round-
trips (`LIMIT 0` on create, full on do_get). Acceptable.

### 3. `CommandGetDbSchemas` is the canonical name, not an alias

The Apache JDBC driver calls `CommandGetDbSchemas` (not the
deprecated `CommandGetSchemas`) for `DatabaseMetaData.getSchemas()`.
The spec's §4.2 calls it an "alias" — it's actually the primary
spelling.

### 4. `GetSqlInfo` is not exercised by DatabaseMetaData introspection

The driver introspects entirely via the per-command catalog RPCs
(`GetCatalogs` / `GetDbSchemas` / `GetTables` / `GetTableTypes` /
`GetColumns` / `GetPrimaryKeys` / `GetExportedKeys` /
`GetImportedKeys` / `GetCrossReference` / `GetTypeInfo`).
`GetSqlInfo` is only fetched on explicit request. Phase 1 still
implements it (cheap; mandatory per Flight SQL spec), but it's
marked `[unobserved]` for documentation purposes.

### 5. `getCrossReference` IS issued

Driver issues `CommandGetCrossReference` even when neither side
of the relationship is named with a specific schema. Phase 1
keeps the stub (empty result with correct schema) — spec
already covers this case.

## Prepared-statement flow — the live-mode refresh

The `--mode live` rerun now fills in what the stub couldn't: the JDBC
driver completes the prepared-statement triplet, and the JSONL trace
records every leg.

Observed flow per `Statement.executeQuery(sql)`:

1. `do_action(CreatePreparedStatement, body=Any{ActionCreatePreparedStatementRequest{query=<sql>}})`
2. The driver decodes the returned `Any{ActionCreatePreparedStatementResult}`
   and reads the `prepared_statement_handle` (= UTF-8 SQL bytes).
3. **The driver skips `get_flight_info(CommandPreparedStatementQuery)`** —
   it goes straight to `do_get` using a `TicketStatementQuery{statement_handle=<sql>}`
   built from the dataset schema in the prepared-statement result. The
   server's `get_flight_info_for_sql` path is never exercised by JDBC in
   this version; only the pyarrow-flight Python client uses it.
4. On `Connection.close()` / `Statement.close()`, the driver issues a
   single `do_action(ClosePreparedStatement, body=Any{ActionClosePreparedStatementRequest})`.
   It is a no-op on the server side (handles are stateless).

That observation is the reason `slayer/flight/server.py`'s
`do_action` accepts the body either Any-wrapped (JDBC) or raw
(pyarrow-flight) via `_parse_action_body`, and the
`handle_create_prepared_statement` response is **always** Any-wrapped
(the Apache JDBC driver requires the wrapping; the pyarrow client
tolerates both shapes).

JDBC `token=X` auth (handshake-based) is the remaining un-implemented
piece — see `tests/integration/test_integration_flight.py::test_auth_positive`
(`xfail(strict=True)` so the future fix auto-promotes to PASSED).

## Probe-query observations

The four probe queries from spec §6.5 (`SELECT 1`,
`SELECT NULL WHERE 1=0`, `SELECT version()`,
`SELECT current_database()`) all went through the prepared-statement
path. Their Phase 1 implementation lives in
`slayer.flight.probe_queries.match(sql) -> Optional[CannedResponse]`
and is hooked into the translator pipeline at the prepared-
statement create step. No probe was issued spontaneously by the
driver during connect/introspection — every probe in the capture
came from our `capture_dbt_jdbc.py` calling `executeQuery`
explicitly. So the whitelist is sized for *user-typed* probes
from interactive clients (DBeaver, etc.); it doesn't need to
expand to a hypothetical "driver-spontaneous" set.

## Auth headers

`metadata = {}` on every captured RPC because the test URL had
no `token=` parameter. A second capture run with
`?token=tok&environmentId=42` would surface the bearer header
shape; deferred until Phase 1's auth handler lands so we can
test against real auth flow.
