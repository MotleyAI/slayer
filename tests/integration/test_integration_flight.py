"""Live integration tests for the Flight SQL facade via JayDeBeAPI (DEV-1390 Task 15a).

Drives the production Flight SQL server through the upstream Apache
``flight-sql-jdbc-driver`` JAR — the same client a Power BI / Sigma /
Looker / dbt-SL deployment uses in production. Skipped automatically
when Java isn't on PATH or the JAR can't be downloaded.

Covers introspection, the four probe queries, semantic-model selects
(prepared-statement path), time grain, cross-model dimensions,
``SELECT *`` rejection, DML rejection, bearer-token auth, and an
N=10 concurrent ``executeQuery`` smoke test.
"""

from __future__ import annotations

import threading
from typing import Any
from collections.abc import Callable

import pytest


pytestmark = pytest.mark.integration


def _exec_query_to_rows(jconn, sql: str):
    """Run ``executeQuery`` and return ``(columns, rows)`` via JDBC."""
    stmt = jconn.createStatement()
    try:
        rs = stmt.executeQuery(sql)
        try:
            md = rs.getMetaData()
            n = md.getColumnCount()
            columns = [str(md.getColumnLabel(i + 1)) for i in range(n)]
            rows = []
            while rs.next():
                rows.append([rs.getObject(i + 1) for i in range(n)])
            return columns, rows
        finally:
            rs.close()
    finally:
        stmt.close()


def _drain_count(rs) -> int:
    """Count rows in a JDBC ResultSet and close it."""
    try:
        n = 0
        while rs.next():
            n += 1
        return n
    finally:
        rs.close()


def _columns_of(rs) -> list[str]:
    """Return the column labels of a JDBC ResultSet's metadata."""
    md = rs.getMetaData()
    return [str(md.getColumnLabel(i + 1)) for i in range(md.getColumnCount())]


# ----- DatabaseMetaData introspection ----------------------------------------


def test_get_catalogs(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    host, port = flight_demo_server
    url = flight_jdbc_url(host=host, port=port)
    conn = jaydebeapi_connect(url)
    try:
        meta = conn.jconn.getMetaData()
        rs = meta.getCatalogs()
        cols = _columns_of(rs)
        catalogs = []
        while rs.next():
            catalogs.append(str(rs.getObject(1)))
        rs.close()
        assert "catalog_name" in [c.lower() for c in cols] or "table_cat" in [c.lower() for c in cols]
        assert "slayer" in catalogs
    finally:
        conn.close()


def test_get_schemas_and_tables(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        meta = conn.jconn.getMetaData()
        # schemas
        rs = meta.getSchemas()
        schemas = []
        while rs.next():
            schemas.append(str(rs.getObject(1)))
        rs.close()
        assert "jaffle_shop" in schemas

        # tables — orders should be there
        rs = meta.getTables(None, None, "%", None)
        tables = []
        while rs.next():
            tables.append((str(rs.getObject(2)), str(rs.getObject(3))))
        rs.close()
        assert ("jaffle_shop", "orders") in tables
        assert ("jaffle_shop", "customers") in tables
    finally:
        conn.close()


def test_get_primary_keys_empty(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    """Primary-keys stub returns no rows (Phase 1 — §5.2 of the spec).

    Note: the Apache JDBC driver collapses an empty pa.Table to a 0-row /
    0-column ResultSet on the JDBC side regardless of the column metadata
    our pa.Schema advertises; we only assert the row count here.
    """
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        meta = conn.jconn.getMetaData()
        rs = meta.getPrimaryKeys(None, None, "orders")
        assert _drain_count(rs) == 0
    finally:
        conn.close()


def test_get_keys_and_cross_reference_empty(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        meta = conn.jconn.getMetaData()
        assert _drain_count(meta.getExportedKeys(None, None, "orders")) == 0
        assert _drain_count(meta.getImportedKeys(None, None, "orders")) == 0
        assert _drain_count(
            meta.getCrossReference(None, None, "orders", None, None, "customers")
        ) == 0
    finally:
        conn.close()


def test_get_type_info_returns_jdbc_shape(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    """Our 2-column ``CommandGetXdbcTypeInfo`` response is reshaped by the
    Apache JDBC driver into the 18-column JDBC ``getTypeInfo`` envelope; we
    only assert the result set has the expected JDBC column metadata.

    Populating the full row content is a Phase 2 issue — Phase 1 marks
    ``getTypeInfo`` as stubbed.
    """
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        meta = conn.jconn.getMetaData()
        rs = meta.getTypeInfo()
        cols = _columns_of(rs)
        rs.close()
        assert "TYPE_NAME" in cols
        assert "DATA_TYPE" in cols
    finally:
        conn.close()


# ----- INFORMATION_SCHEMA + semantic-model SELECTs ---------------------------


def test_select_information_schema_metrics(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        cols, rows = _exec_query_to_rows(
            conn.jconn, "SELECT * FROM INFORMATION_SCHEMA.METRICS"
        )
        lower = [c.lower() for c in cols]
        assert "metric_name" in lower
        assert "table_name" in lower
        # At least one metric should exist on the demo's orders table.
        assert any(
            str(r[lower.index("table_name")]).lower() == "orders" for r in rows
        )
    finally:
        conn.close()


def test_select_row_count_via_prepared_statement(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    """The Apache JDBC driver routes every executeQuery through the prepared-
    statement triplet — `SELECT row_count FROM orders` exercises Path B."""
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        cols, rows = _exec_query_to_rows(conn.jconn, "SELECT row_count FROM orders")
        assert cols == ["row_count"]
        assert len(rows) == 1
        assert int(rows[0][0]) > 0
    finally:
        conn.close()


def test_time_grain_select(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        cols, rows = _exec_query_to_rows(
            conn.jconn,
            "SELECT month(ordered_at) AS m, row_count FROM orders "
            "WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31' "
            "ORDER BY m",
        )
        assert cols == ["m", "row_count"]
        assert 1 <= len(rows) <= 12
        # Each bucket should have a positive count.
        for row in rows:
            assert int(row[1]) > 0
    finally:
        conn.close()


def test_cross_model_dim_select(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    """`customers.name` joins orders→customers via the catalog's dotted form."""
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        cols, rows = _exec_query_to_rows(
            conn.jconn, "SELECT customers.name, row_count FROM orders"
        )
        assert cols == ["customers.name", "row_count"]
        assert len(rows) > 0
        # Every row carries a non-empty customer name + a positive count.
        for row in rows[:5]:
            assert row[0] is not None
            assert int(row[1]) > 0
    finally:
        conn.close()


# ----- error paths -----------------------------------------------------------


def test_select_star_rejected(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        with pytest.raises(Exception) as excinfo:
            _exec_query_to_rows(conn.jconn, "SELECT * FROM orders")
        assert "SELECT *" in str(excinfo.value) or "select *" in str(excinfo.value).lower()
    finally:
        conn.close()


def test_dml_rejected(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        with pytest.raises(Exception) as excinfo:
            _exec_query_to_rows(conn.jconn, "INSERT INTO orders VALUES (1)")
        assert "read-only" in str(excinfo.value).lower()
    finally:
        conn.close()


# ----- probe queries ---------------------------------------------------------


@pytest.mark.parametrize(
    "probe_sql",
    [
        "SELECT 1",
        "SELECT NULL WHERE 1=0",
        "SELECT version()",
        "SELECT current_database()",
    ],
)
def test_probe_queries(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
    probe_sql: str,
) -> None:
    """Each of the four whitelisted probes returns a canned result."""
    host, port = flight_demo_server
    conn = jaydebeapi_connect(flight_jdbc_url(host=host, port=port))
    try:
        cols, rows = _exec_query_to_rows(conn.jconn, probe_sql)
        assert len(cols) >= 1
        if "WHERE 1=0" in probe_sql:
            assert rows == []
        else:
            assert len(rows) == 1
    finally:
        conn.close()


# ----- auth ------------------------------------------------------------------


# JDBC-driver bearer-token auth (URL ``token=X``) requires a server-side
# ``do_handshake`` handler that issues a bearer token: the Apache JDBC
# driver always initiates a handshake before its first real RPC. Our
# current ``BearerTokenMiddlewareFactory`` does header-based validation
# only — sufficient for the pyarrow.flight client (covered in
# ``test_integration_flight_pyarrow_client.py``) but not for JDBC.
# Implementing the handshake handler is a Phase 2 follow-up; the JDBC
# auth surface is xfail-strict until that lands so a future implementation
# flips this to PASSED automatically.


@pytest.mark.xfail(
    strict=True,
    reason=(
        "JDBC token= auth requires server-side do_handshake (Phase 2); "
        "current header-validation middleware is bypassed by the Apache "
        "driver's pre-RPC handshake call. See test file header."
    ),
)
def test_auth_positive(
    flight_demo_server_with_token: tuple[str, int, str],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    host, port, token = flight_demo_server_with_token
    url = flight_jdbc_url(host=host, port=port, token=token)
    conn = jaydebeapi_connect(url)
    try:
        meta = conn.jconn.getMetaData()
        rs = meta.getCatalogs()
        catalogs = []
        while rs.next():
            catalogs.append(str(rs.getObject(1)))
        rs.close()
        assert "slayer" in catalogs
    finally:
        conn.close()


def test_auth_negative_wrong_token(
    flight_demo_server_with_token: tuple[str, int, str],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    """A wrong (or missing) token surfaces as a Flight UNAUTHENTICATED /
    UNIMPLEMENTED error from the JDBC driver, irrespective of the
    Phase 2 handshake gap."""
    host, port, _token = flight_demo_server_with_token
    url = flight_jdbc_url(host=host, port=port, token="wrong")
    with pytest.raises(Exception) as excinfo:
        # The driver may raise on connect or on the first introspection RPC.
        conn = jaydebeapi_connect(url)
        try:
            _exec_query_to_rows(conn.jconn, "SELECT 1")
        finally:
            conn.close()
    msg = str(excinfo.value).lower()
    assert (
        "unauthenticated" in msg
        or "unimplemented" in msg
        or "bearer" in msg
        or "auth" in msg
        or "invalid" in msg
    )


# ----- concurrency -----------------------------------------------------------


def test_n10_concurrent_executequery(
    flight_demo_server: tuple[str, int],
    flight_jdbc_url: Callable[..., str],
    jaydebeapi_connect: Callable[..., Any],
) -> None:
    """Ten threads issue the same SELECT concurrently; results must agree."""
    host, port = flight_demo_server
    url = flight_jdbc_url(host=host, port=port)

    results: list[int] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            conn = jaydebeapi_connect(url)
            try:
                _cols, rows = _exec_query_to_rows(
                    conn.jconn, "SELECT row_count FROM orders"
                )
                with lock:
                    results.append(int(rows[0][0]))
            finally:
                conn.close()
        except BaseException as exc:  # noqa: BLE001 — capture for assert  # NOSONAR(S5754) — capture threading errors for assert
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"concurrent workers raised: {errors!r}"
    assert len(results) == 10
    assert len(set(results)) == 1, f"results disagreed across threads: {results!r}"
    assert results[0] > 0
