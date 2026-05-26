# DEV-1390 resume plan

Session-handover notes for the Flight SQL facade (DEV-1390). The
**authoritative spec** lives in the [Linear issue
description](https://linear.app/motley-ai/issue/DEV-1390); read that
first. This file covers what's been done, what's left, the design
decisions already locked, and how to verify each piece.

---

## 1. Status — 13 of 17 delivery items LANDED

| # | Item | Status | Tests |
|---|---|---|---|
| 0 | Phase 1.0 capture harness (§1.1) | ✅ LANDED | 39 RPCs captured |
| 1 | `slayer/flight/types.py` | ✅ LANDED | 35 |
| 2 | `slayer/flight/catalog.py` | ✅ LANDED | 16 |
| 3 | `slayer/flight/probe_queries.py` | ✅ LANDED | 19 |
| 4 | `slayer/flight/info_schema.py` | ✅ LANDED | 12 |
| 5 | `slayer/flight/translator.py` | ✅ LANDED | 42 |
| 6 | `slayer/flight/auth.py` | ✅ LANDED | 30 |
| 7 | `slayer/flight/handlers.py` (incl. prepared-statement triplet) | ✅ LANDED | 19 |
| 8 | `slayer/flight/server.py` (assembly) | ✅ LANDED | — |
| 9 | `slayer flight-serve` CLI (`slayer/flight/cli.py` + `slayer/cli.py` mount) | ✅ LANDED | — |
| 10 | Live integration tests (JayDeBeAPI + pyarrow-client) | ✅ LANDED | 17 + 21 |
| 11 | Docs (interfaces, getting-started, CLAUDE.md, README.md) | ✅ LANDED | — |
| 12 | Final lint + full test pass + post-handlers capture refresh | ✅ LANDED | — |

**173 unit tests pass.** `poetry run ruff check slayer/flight/ tests/flight/` is clean. A working **smoke test** is captured at the bottom of this file.

### Files created in the previous session (already `git add`-ed; user to commit)

**Production code:**
- `slayer/flight/__init__.py` (empty)
- `slayer/flight/_capture_stub.py`
- `slayer/flight/_flight_sql_pb2.py` (generated from `FlightSql.proto`)
- `slayer/flight/FlightSql.proto` (vendored from Apache Arrow 18.0.0)
- `slayer/flight/auth.py`
- `slayer/flight/catalog.py`
- `slayer/flight/cli.py`
- `slayer/flight/handlers.py`
- `slayer/flight/info_schema.py`
- `slayer/flight/probe_queries.py`
- `slayer/flight/server.py`
- `slayer/flight/translator.py`
- `slayer/flight/types.py`

**Tests + fixtures:**
- `tests/flight/__init__.py`
- `tests/flight/capture_dbt_jdbc.py` (standalone Phase 1.0 capture driver)
- `tests/flight/conftest.py` (`jdbc_jar`, `jaydebeapi_connect`, `capture_stub` fixtures)
- `tests/flight/fixtures/CAPTURE-FINDINGS.md`
- `tests/flight/fixtures/capture-latest.jsonl` (39 RPCs)
- `tests/flight/test_auth.py`
- `tests/flight/test_catalog.py`
- `tests/flight/test_handlers.py`
- `tests/flight/test_info_schema.py`
- `tests/flight/test_probe_queries.py`
- `tests/flight/test_translator.py`
- `tests/flight/test_types.py`

**Modified files (NOT `git add`-ed — user adds at commit time per CLAUDE.md):**
- `.gitignore` (added `tests/.cache/` for the auto-downloaded JDBC JAR)
- `poetry.lock`
- `pyproject.toml` (added `flight` extra with `pyarrow`; added `jaydebeapi` + `jpype1` dev deps; added ruff per-file-ignore for the generated `_flight_sql_pb2.py`)
- `slayer/cli.py` (registered `flight-serve` subparser + dispatch case)

---

## 2. Locked design decisions (no need to re-interview)

These came out of the previous session's `/spec` interview and the
Phase 1.0 capture findings. **Don't re-litigate them** unless real
evidence pushes against one.

1. **Wire-format ground truth** — design is anchored on a real
   wire-capture against the upstream Apache `flight-sql-jdbc-driver`
   v18.3.0, driven from Python via JayDeBeAPI. Capture corpus checked
   in at `tests/flight/fixtures/capture-latest.jsonl` (39 RPCs).
2. **Prepared-statement handlers are first-class real handlers**, not
   stubs (Phase 1.0 finding #1). Every `Statement.executeQuery` from
   the Apache JDBC driver goes through the prepared-statement
   triplet, not `CommandStatementQuery`.
3. **Stateless server** — Flight `Ticket.ticket` and
   `prepared_statement_handle` both carry the **original UTF-8 SQL
   bytes** (wrapped in `TicketStatementQuery` for ticket-shape
   conformance). No per-connection / per-handle state on the server.
   `Close` is a no-op.
4. **Probe-query whitelist** (4 entries): `SELECT 1`,
   `SELECT NULL WHERE 1=0`, `SELECT version()` / `SELECT @@version`,
   `SELECT current_database()`. Applied as step 3 of the translator
   pipeline.
5. **`SELECT *` rejection** on Flight tables with a pointer to
   `SELECT * FROM INFORMATION_SCHEMA.METRICS`. Allowed on
   `INFORMATION_SCHEMA.*`.
6. **`row_count` collision** — synthetic `*:count` metric is renamed
   to `_row_count` if a user-defined column shadows the name (one
   `WARNING` log per affected model at catalog build).
7. **No catalog caching in Phase 1** — every protocol call rebuilds
   `FlightCatalog` from the active `StorageBackend`. Follow-up
   `StorageBackend.serial()` accessor is a Phase 2 issue.
8. **`--demo` + auth interplay** — when `--demo` is set AND `--host`
   isn't explicitly given AND `--token` isn't given, the effective
   `--host` defaults to `127.0.0.1` for the no-token-on-loopback
   fallback. Non-loopback + no-token is a startup-time refusal.
9. **`environmentId`** — logged at INFO on each request, no
   validation.
10. **Concurrency** — no extra locking; pinned by an N=10 concurrent-
    `do_get` integration test (Task 15 below).
11. **Capture only the Apache upstream JDBC** — dbt Labs proprietary
    fork is Phase 2.
12. **Dotted form end-to-end for cross-model names** —
    `customers.regions.name`, not `customers__regions__name`. Catalog,
    `INFORMATION_SCHEMA`, BI-tool projection, WHERE, and SLayer DSL
    all use the same form. No `__` → `.` rewrite step in the
    translator.
13. **Translator result shape** — tagged union of `ProbeResult`,
    `InfoSchemaResult`, `NoOpResult`, `QueryResult` (β option from the
    interview).
14. **Bare-name table resolution** — searches every schema; unique
    match → use, multiple → error naming candidates, zero → "Unknown
    table" (ii option).
15. **GROUP BY policy** — strict-on-extras / lenient-on-omissions (c
    option). User `GROUP BY` items not in the derived dimension set
    error; omissions are silently filled in from the projection.
16. **Protobuf marshalling** — vendor `FlightSql.proto` from Apache
    Arrow 18.0.0 + generate `_flight_sql_pb2.py` (option A from the
    interview). Generated module is 28KB, lives at
    `slayer/flight/_flight_sql_pb2.py`. To regenerate after a future
    Arrow bump:
    ```bash
    cd slayer/flight
    poetry run python -m grpc_tools.protoc -I. --python_out=. FlightSql.proto
    mv FlightSql_pb2.py _flight_sql_pb2.py
    ```
17. **Phase 1 wire schema** — derived from **catalog-declared types**
    via `QueryResult.projection_types`, not from a LIMIT-0 SQL
    execution (the engine's `SlayerResponse.attributes` doesn't yet
    expose per-column Arrow types). LIMIT-0 still runs for engine-
    side query validation. Phase 2 issue tightens this to a real
    LIMIT-0-derived schema.

---

## 3. What's left — Tasks 15, 16, 17

### Task 15 — Live integration tests

Two files, both under `tests/integration/`:

#### 15a. `tests/integration/test_integration_flight.py` (JayDeBeAPI)

Drive the live server through the **Apache `flight-sql-jdbc-driver`
JAR** via JayDeBeAPI — same fixture (`jdbc_jar`,
`jaydebeapi_connect`) as the Phase 1.0 capture harness. Marked
`@pytest.mark.integration`. Tests:

1. **Demo-server fixture.** Use a module-scoped fixture that:
   - Resolves storage in a tmpdir.
   - Calls `_prepare_demo(args, storage)` to ingest the Jaffle Shop dataset.
   - Constructs `SlayerQueryEngine` + `FlightHandlers`.
   - Calls `build_server(host="127.0.0.1", port=0, handlers=handlers, token=None)`.
   - Runs `.serve()` in a background thread.
   - Yields `(host, port)`; teardown calls `.shutdown()` + `.wait()`.

2. **DatabaseMetaData introspection** —
   `meta.getCatalogs()`, `.getSchemas()`, `.getTables(None, None, "%", None)`,
   `.getColumns(None, None, "%", "%")`, `.getPrimaryKeys(None, None, "orders")`,
   `.getExportedKeys(...)`, `.getImportedKeys(...)`, `.getCrossReference(...)`,
   `.getTypeInfo()`. Assert non-empty for catalogs/schemas/tables; assert
   `getPrimaryKeys` returns empty with correct shape; etc.

3. **`INFORMATION_SCHEMA.METRICS` SELECT** — `executeQuery` returns
   rows with the expected columns and at least one `revenue_sum`-like
   metric.

4. **Real metric/dim SELECT (prepared-statement path)** —
   `SELECT row_count FROM orders` returns one row with `row_count > 0`.

5. **Time-grain query** — `SELECT month(ordered_at), row_count FROM orders
   WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31'` returns
   monthly buckets.

6. **Cross-model dim query** — `SELECT row_count, customers.X FROM orders`
   where `X` is a real column on the demo's `customers` model.

7. **`SELECT *` rejection** — `executeQuery("SELECT * FROM orders")`
   surfaces as a `java.sql.SQLException` whose message contains
   `"SELECT * not supported"`.

8. **DML rejection** — `INSERT INTO orders VALUES (1)` raises with
   `"read-only"` in the message.

9. **Four probe queries** — each returns the canned response shape.

10. **Auth subcases:**
    - Positive: server constructed with `token="s3cret"`, JDBC URL
      includes `token=s3cret`. `getCatalogs()` succeeds.
    - Negative: same server but JDBC URL includes `token=wrong`.
      `executeQuery` raises `UNAUTHENTICATED`-flavoured error.

11. **N=10 concurrency subcase** — ten threads each call
    `executeQuery("SELECT row_count FROM orders")`. Every thread's
    result is identical and well-formed.

Skip with a clear message if `shutil.which("java")` is `None` or the
JAR fixture download failed.

**Gotchas to expect:**
- JayDeBeAPI requires Java ≥ 11 on PATH. The CI environment may not have it.
- The JAR is auto-downloaded by the `jdbc_jar` fixture from Maven Central on first run; subsequent runs use `tests/.cache/`.
- `Connection.commit()` / `.rollback()` from JayDeBeAPI translate to gRPC actions but the Apache driver may not actually call them on `do_action`. Verify with a fresh capture if behaviour seems off.

#### 15b. `tests/integration/test_integration_flight_pyarrow_client.py` (Java-free)

Same surface, driven by `pyarrow.flight` Python client. Subset of
15a's tests:

1. Same demo-server fixture (re-usable; share via a `conftest.py`
   under `tests/integration/`).
2. `client.get_flight_info(FlightDescriptor.for_command(packed_protobuf))` +
   `client.do_get(ticket)` for each catalog command (Catalogs / DbSchemas
   / Tables / TableTypes / PrimaryKeys / SqlInfo).
3. Prepared-statement round-trip: `do_action("CreatePreparedStatement", body)`
   → parse `ActionCreatePreparedStatementResult` →
   `get_flight_info(CommandPreparedStatementQuery{handle})` →
   `do_get(info.endpoints[0].ticket)`.
4. Probe queries via the prepared-statement path.
5. Auth +/- using a `client.GenericOptions("authorization", b"Bearer X")`
   middleware option.

Skip-free in CI (no JDK required). This is the always-runs check.

**Reference smoke-test recipe at §5 below — it's already shown to
work end-to-end and is the basis for both integration files.**

### Task 16 — Documentation

Four files:

#### 16a. `docs/interfaces/flight-sql.md`

Protocol reference. Section headings should mirror the Linear-issue
spec sections but written as user-facing docs:
- Connection URL format (`jdbc:arrow-flight-sql://host:port/?...`).
- Authentication: bearer-token via URL `token=` param; loopback fallback.
- TLS: cert/key pair; `useEncryption=false` for plain gRPC.
- Catalog layout: `slayer.<datasource>.<model>`; dotted-form columns.
- SQL subset accepted (single-FROM SELECT, time-grain functions,
  BETWEEN/comparator date filters, ORDER BY / LIMIT / OFFSET).
- Probe-query whitelist (the 4 entries).
- DML/DDL behaviour (read-only error).
- Error taxonomy (gRPC status code mapping).
- The `LIMIT 0` two-round-trip note + the prepared-statement Path B
  flow (handle = SQL bytes).
- Unobserved commands: `CommandStatementQuery`, `GetSqlInfo`,
  `GetXdbcTypeInfo`, `CommandPreparedStatementQuery`,
  `ActionClosePreparedStatementRequest` — marked `[unobserved]`.

#### 16b. `docs/getting-started/flight-sql.md`

Per-tool connect guide. One section per dbt-SL-connector tool with
the exact JDBC URL shape:
- Power BI (via "dbt Semantic Layer" connector)
- Sigma
- Looker
- Tableau (case-sensitive identifiers — call this out)
- DBeaver Community
- Hex

Each section: 4-5 lines max — connector name, paste-in JDBC URL,
"expected to work — Phase 2 hand-test pending" badge.

#### 16c. `CLAUDE.md` — new "Flight SQL" section

Add adjacent to the existing "Async Architecture" section. Bullet-
list summary:
- Port 5144 (next after 5143).
- `slayer flight-serve [--host HOST] [--port PORT] [--storage PATH] [--token T] [--tls-cert C] [--tls-key K] [--demo]`.
- Loopback no-token fallback (and `--demo` host default).
- The `LIMIT 0` two-round-trip story (Path A vs Path B).
- `tests/flight/fixtures/CAPTURE-FINDINGS.md` for the wire-capture story.
- Stateless server: SQL bytes in ticket + handle.
- Test fixtures: `jdbc_jar` (auto-download), `jaydebeapi_connect`,
  `capture_stub`. JayDeBeAPI integration tests skip if Java is absent.
- Catalog dotted-form convention (`customers.regions.name`).

#### 16d. `README.md` — one-line mention

Under whatever interfaces section exists.

### Task 17 — Final lint + full test pass + post-handlers capture refresh

Three steps:

1. **Re-run capture against the real Phase-1 server** (per
   CAPTURE-FINDINGS.md follow-up). Modify
   `tests/flight/capture_dbt_jdbc.py` to optionally point at a live
   `FlightSqlServer` instead of the `_capture_stub`. Re-run, commit
   the refreshed `capture-latest.jsonl` (now with
   `CommandPreparedStatementQuery` + `ActionClosePreparedStatementRequest`
   round-trips filled in). Update `CAPTURE-FINDINGS.md` to mark those
   as "now observed."
2. **Lint:** `poetry run ruff check slayer/ tests/`
3. **Tests:**
   ```bash
   poetry run pytest                                              # unit suite (excludes integration)
   poetry run pytest tests/integration/test_integration_flight*.py -m integration
   ```
4. **Re-sync Linear** with the updated `LANDED` markers in §13 (items
   1-9 + 10 if integration lands; item 11 if docs land; item 12 if
   lint+tests pass). Use the `mcp__linear__save_issue` tool with the
   full description. The previous spec push went via that tool's
   `description` parameter; the previous content is at
   `/tmp/claude/dev1390-updated.md` from the previous session if it
   survives, otherwise re-fetch via `mcp__linear__get_issue` and
   spot-edit.

---

## 4. Open question for next session

**Catalog-declared types vs LIMIT-0 wire types** — Phase 1 currently
ships catalog-declared types as the wire schema (§17 in the
locked-decisions list above). The original spec promised
LIMIT-0-derived. The translator emits the catalog-declared types via
`QueryResult.projection_types`; the handler builds the wire schema
from that. A user with a `ModelMeasure` whose declared type is
incorrect (or unset) will see a wire-type mismatch surface as an
`ArrowTypeError` (we saw exactly this during smoke testing — fixed by
adding `projection_types` to the translator's output).

**This is documented as a Phase 2 follow-up** but worth surfacing
during the docs pass — `INFORMATION_SCHEMA.METRICS.data_type` is the
authoritative type for now; users should set `ModelMeasure.type` for
custom formulas that surface over the facade.

---

## 5. Smoke-test recipe (proven working)

To verify a working state at any point:

```bash
poetry run python <<'PY'
import argparse, threading, time, tempfile
from slayer.cli import _resolve_storage, _prepare_demo
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.flight.handlers import FlightHandlers
from slayer.flight.server import build_server

args = argparse.Namespace(
    storage=tempfile.mkdtemp(prefix="slayer-flight-smoke-"),
    models_dir=None, datasource=None, force=False,
)
storage = _resolve_storage(args)
_prepare_demo(args, storage)
engine = SlayerQueryEngine(storage=storage)
handlers = FlightHandlers(engine=engine, storage=storage)
server = build_server(host="127.0.0.1", port=0, handlers=handlers, token=None)
threading.Thread(target=server.serve, daemon=True).start()
time.sleep(0.2)

import pyarrow.flight as fl
import slayer.flight._flight_sql_pb2 as fsql_pb
from google.protobuf.any_pb2 import Any as PbAny

def pack(msg, suffix):
    a = PbAny()
    a.type_url = f"type.googleapis.com/arrow.flight.protocol.sql.{suffix}"
    a.value = msg.SerializeToString()
    return a.SerializeToString()

client = fl.connect(f"grpc://127.0.0.1:{server.port}")

# Prepared-statement path
cmd = fsql_pb.ActionCreatePreparedStatementRequest()
cmd.query = "SELECT row_count FROM orders"
results = list(client.do_action(fl.Action("CreatePreparedStatement", cmd.SerializeToString())))
# Apache JDBC compatibility requires the response to be Any-wrapped — unwrap.
any_msg = PbAny()
any_msg.ParseFromString(results[0].body.to_pybytes())
resp = fsql_pb.ActionCreatePreparedStatementResult()
resp.ParseFromString(any_msg.value)

q = fsql_pb.CommandPreparedStatementQuery()
q.prepared_statement_handle = resp.prepared_statement_handle
info = client.get_flight_info(fl.FlightDescriptor.for_command(pack(q, "CommandPreparedStatementQuery")))
table = client.do_get(info.endpoints[0].ticket).read_all()
print(f"row_count result: {table.to_pylist()}")
# Expected: [{'row_count': 1181491}]  (or whatever the demo's order count is)

server.shutdown(); server.wait()
PY
```

If this prints `row_count result: [{'row_count': <some int>}]`, the
facade is working end-to-end.

---

## 6. Verification commands

```bash
# Run all flight unit tests
poetry run pytest tests/flight/                                   # expects 173 passed

# Lint
poetry run ruff check slayer/flight/ tests/flight/                # expects clean

# Full non-integration suite (per CLAUDE.md global rule)
poetry run pytest                                                  # everything except @pytest.mark.integration

# Integration suite (once 15 lands)
poetry run pytest tests/integration/test_integration_flight*.py -m integration
```

---

## 7. References

- **Linear issue**: <https://linear.app/motley-ai/issue/DEV-1390> (authoritative spec)
- **Phase 1.0 capture findings**: `tests/flight/fixtures/CAPTURE-FINDINGS.md`
- **Capture corpus**: `tests/flight/fixtures/capture-latest.jsonl` (39 RPCs)
- **Vendored proto**: `slayer/flight/FlightSql.proto` (Apache Arrow 18.0.0)
- **Generated protobuf module**: `slayer/flight/_flight_sql_pb2.py`
- **Parent issue**: DEV-1389 (original Postgres-wire facade — pivoted)
