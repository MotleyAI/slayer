"""Standalone capture driver for the DEV-1390 Flight SQL facade (spec §1.1).

Two modes:

* ``stub`` (default) — boots a ``CaptureFlightServer`` that returns empty
  responses. Used for the initial Phase 1.0 wire capture; the JDBC
  driver bails out partway through the prepared-statement triplet because
  there's no real ``ActionCreatePreparedStatementResult`` to read.
* ``live`` — boots a ``RecordingFlightSqlServer`` (production handlers
  wrapped in a per-RPC logger) backed by the bundled Jaffle Shop demo.
  Used for the Phase 1 refresh capture: every RPC the JDBC driver issues
  is recorded, including the prepared-statement / ticket round-trips that
  the stub couldn't satisfy.

Usage::

    poetry run python tests/flight/capture_dbt_jdbc.py [output_name] [--mode live|stub]

The optional positional arg is the basename (without ``.jsonl``) for the
fixture file. Defaults to ``capture-latest``.

Requires Java >= 11 on PATH and network access to Maven Central on first
run (to download the JAR into ``tests/.cache/``). Live mode additionally
requires the ``duckdb`` extra and ``jafgen`` (the same prerequisites as
``slayer flight-serve --demo``).
"""

from __future__ import annotations

import argparse
import shutil
import threading
import time
import traceback
import urllib.request
from pathlib import Path

from slayer.flight._capture_stub import (
    CaptureFlightServer,
    RecordingFlightSqlServer,
)

JDBC_DRIVER_VERSION = "18.3.0"
JDBC_DRIVER_URL = (
    "https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/"
    f"{JDBC_DRIVER_VERSION}/flight-sql-jdbc-driver-{JDBC_DRIVER_VERSION}.jar"
)
JDBC_DRIVER_CLASS = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"

HERE = Path(__file__).resolve().parent
CACHE_DIR = HERE.parent / ".cache"
FIXTURES_DIR = HERE / "fixtures"


def _ensure_jar() -> Path:
    if shutil.which("java") is None:
        raise SystemExit("Java >= 11 must be on PATH; install a JDK and retry.")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    jar = CACHE_DIR / f"flight-sql-jdbc-driver-{JDBC_DRIVER_VERSION}.jar"
    if not jar.exists():
        print(f"[capture] downloading {JDBC_DRIVER_URL} -> {jar}")
        urllib.request.urlretrieve(JDBC_DRIVER_URL, jar)
    size = jar.stat().st_size
    if size < 1_000_000:
        jar.unlink(missing_ok=True)
        raise SystemExit(f"JAR at {jar} looked corrupted ({size} bytes); re-run to refetch.")
    return jar


def _try(label: str, fn) -> None:
    print(f"[capture] {label}")
    try:
        fn()
    except Exception:
        print(f"[capture]   ! exception during {label}:")
        traceback.print_exc(limit=2)


def _drain_resultset(rs) -> int:
    """Iterate every row of a JDBC ResultSet so the driver issues do_get."""
    cursor_rows = 0
    while rs.next():
        cursor_rows += 1
    return cursor_rows


def _exercise(conn) -> None:
    """Run a representative introspection + statement surface against the connection.

    Every call is wrapped in ``_try`` so a single failure (the capture stub
    returns empty/well-typed responses, which may upset the driver mid-stream)
    doesn't abort earlier calls' logs.
    """
    jconn = conn.jconn  # underlying java.sql.Connection
    meta = jconn.getMetaData()

    _try("DatabaseMetaData.getCatalogs", lambda: _drain_resultset(meta.getCatalogs()))
    _try("DatabaseMetaData.getSchemas",
         lambda: _drain_resultset(meta.getSchemas()))
    _try("DatabaseMetaData.getSchemas(catalog, %)",
         lambda: _drain_resultset(meta.getSchemas("slayer", "%")))
    _try("DatabaseMetaData.getTables",
         lambda: _drain_resultset(meta.getTables(None, None, "%", None)))
    _try("DatabaseMetaData.getTableTypes",
         lambda: _drain_resultset(meta.getTableTypes()))
    _try("DatabaseMetaData.getColumns",
         lambda: _drain_resultset(meta.getColumns(None, None, "%", "%")))
    _try("DatabaseMetaData.getPrimaryKeys",
         lambda: _drain_resultset(meta.getPrimaryKeys(None, None, "orders")))
    _try("DatabaseMetaData.getExportedKeys",
         lambda: _drain_resultset(meta.getExportedKeys(None, None, "orders")))
    _try("DatabaseMetaData.getImportedKeys",
         lambda: _drain_resultset(meta.getImportedKeys(None, None, "orders")))
    _try("DatabaseMetaData.getCrossReference",
         lambda: _drain_resultset(meta.getCrossReference(None, None, "orders", None, None, "customers")))
    _try("DatabaseMetaData.getTypeInfo",
         lambda: _drain_resultset(meta.getTypeInfo()))

    stmt = jconn.createStatement()

    def run_select(sql: str) -> None:
        rs = stmt.executeQuery(sql)
        _drain_resultset(rs)
        rs.close()

    _try("SELECT 1",
         lambda: run_select("SELECT 1"))
    _try("SELECT NULL WHERE 1=0",
         lambda: run_select("SELECT NULL WHERE 1=0"))
    _try("SELECT version()",
         lambda: run_select("SELECT version()"))
    _try("SELECT current_database()",
         lambda: run_select("SELECT current_database()"))

    _try("SELECT * FROM INFORMATION_SCHEMA.METRICS",
         lambda: run_select("SELECT * FROM INFORMATION_SCHEMA.METRICS"))
    _try("SELECT * FROM INFORMATION_SCHEMA.DIMENSIONS",
         lambda: run_select("SELECT * FROM INFORMATION_SCHEMA.DIMENSIONS"))
    _try("SELECT * FROM INFORMATION_SCHEMA.TABLES",
         lambda: run_select("SELECT * FROM INFORMATION_SCHEMA.TABLES"))
    _try("SELECT * FROM INFORMATION_SCHEMA.COLUMNS",
         lambda: run_select("SELECT * FROM INFORMATION_SCHEMA.COLUMNS"))
    _try("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA",
         lambda: run_select("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA"))

    _try("metric+dim SELECT",
         lambda: run_select(
             "SELECT revenue_sum, status FROM orders "
             "WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31' "
             "ORDER BY revenue_sum DESC LIMIT 10"
         ))
    _try("time-grain SELECT",
         lambda: run_select(
             "SELECT month(ordered_at), revenue_sum FROM orders "
             "WHERE ordered_at >= '2024-01-01'"
         ))
    _try("cross-model dim SELECT",
         lambda: run_select(
             "SELECT customers.regions.name, revenue_sum FROM orders"
         ))

    _try("DML rejection (INSERT)",
         lambda: run_select("INSERT INTO orders (id) VALUES (1)"))
    _try("DDL rejection (CREATE)",
         lambda: run_select("CREATE TABLE foo (a INT)"))

    _try("BEGIN", lambda: run_select("BEGIN"))
    _try("COMMIT", lambda: run_select("COMMIT"))
    _try("ROLLBACK", lambda: run_select("ROLLBACK"))
    _try("SET timezone", lambda: run_select("SET TIME ZONE 'UTC'"))
    _try("SHOW search_path", lambda: run_select("SHOW search_path"))

    _try("Connection.commit", lambda: jconn.commit())
    _try("Connection.rollback", lambda: jconn.rollback())

    stmt.close()


def _make_server(*, mode: str, capture_log: Path):
    """Construct the recording server for the chosen mode.

    Returns ``(server, port, teardown)`` — ``teardown`` is invoked from
    the caller's ``finally`` block.
    """
    if mode == "stub":
        server = CaptureFlightServer("grpc://127.0.0.1:0", capture_log)
        return server, server.port, lambda: None

    if mode != "live":
        raise SystemExit(f"unknown capture mode: {mode!r}")

    # Live mode: real Flight SQL server backed by the bundled Jaffle Shop demo.
    import argparse as _argparse
    import tempfile as _tempfile

    from slayer.cli import _prepare_demo, _resolve_storage
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.flight.handlers import FlightHandlers

    args = _argparse.Namespace(
        storage=_tempfile.mkdtemp(prefix="capture-live-"),
        models_dir=None, datasource=None, force=False,
    )
    storage = _resolve_storage(args)
    _prepare_demo(args, storage)
    engine = SlayerQueryEngine(storage=storage)
    handlers = FlightHandlers(engine=engine, storage=storage)
    server = RecordingFlightSqlServer(
        location="grpc://127.0.0.1:0",
        handlers=handlers,
        log_path=capture_log,
    )
    return server, server.port, lambda: None


def main(out_basename: str = "capture-latest", *, mode: str = "stub") -> int:
    jar = _ensure_jar()
    print(f"[capture] using JAR: {jar}")
    print(f"[capture] mode: {mode}")

    capture_log = HERE / "capture-run.jsonl"
    server, port, teardown = _make_server(mode=mode, capture_log=capture_log)
    location = f"grpc://127.0.0.1:{port}"
    print(f"[capture] capture server bound at {location}")

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    time.sleep(0.3)

    # Pre-start the JVM with the ``--add-opens`` flags Arrow needs on Java 17+.
    import jpype

    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            classpath=[str(jar)],
            convertStrings=True,
        )

    import jaydebeapi

    url = f"jdbc:arrow-flight-sql://127.0.0.1:{port}/?useEncryption=false"
    print(f"[capture] connecting via {url}")
    conn = None
    try:
        conn = jaydebeapi.connect(JDBC_DRIVER_CLASS, url, [], str(jar))
        _exercise(conn)
    except Exception:
        print("[capture] driver-level exception (continuing — partial log may still be useful):")
        traceback.print_exc(limit=3)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        server.shutdown()
        server.wait()
        thread.join(timeout=2)
        teardown()

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FIXTURES_DIR / f"{out_basename}.jsonl"
    if capture_log.exists():
        shutil.copy(capture_log, out_path)
        line_count = sum(1 for _ in out_path.open())
        print(f"[capture] wrote {out_path} ({line_count} RPCs)")
        capture_log.unlink(missing_ok=True)
    else:
        print("[capture] no capture log produced — nothing to copy")
        return 1

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "out_basename", nargs="?", default="capture-latest",
        help="basename for the fixture file (default: capture-latest)",
    )
    parser.add_argument(
        "--mode", choices=("stub", "live"), default="stub",
        help="capture against the stub (default) or the real FlightSqlServer",
    )
    args = parser.parse_args()
    raise SystemExit(main(args.out_basename, mode=args.mode))
