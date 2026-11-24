"""The demo loader: CSVs from any directory, cents converted, columns renamed, dates shifted to today."""

from __future__ import annotations

import datetime as dt
import re
from decimal import Decimal
from pathlib import Path
from typing import cast

import duckdb

from slayer.demo import jaffle_shop

CSVS = {
    "customers": "id,name\nc1,Ann\nc2,O'Brien\n",
    "stores": "id,name,opened_at,tax_rate\ns1,Brooklyn,2018-01-01,0.05\n",
    "orders": "id,customer,ordered_at,store_id,subtotal,tax_paid,order_total\n"
              "o1,c1,2018-08-31,s1,1000,50,1050\no2,c2,2018-09-01,s1,250,13,263\n",
}


def test_load_data_from_a_quoted_directory(tmp_path: Path) -> None:
    data_dir = tmp_path / "it's data"
    data_dir.mkdir()
    for name, body in CSVS.items():
        (data_dir / f"raw_{name}.csv").write_text(body, encoding="utf-8")
    conn = duckdb.connect(str(tmp_path / "demo.duckdb"))
    try:
        jaffle_shop.create_schema(conn)
        jaffle_shop.load_data(conn=conn, data_dir=str(data_dir))
        assert conn.execute("SELECT name FROM customers ORDER BY id").fetchall() == [("Ann",), ("O'Brien",)]
        assert conn.execute("SELECT customer_id, order_total FROM orders ORDER BY id").fetchall() == [
            ("c1", Decimal("10.50")), ("c2", Decimal("2.63"))]
        today = dt.date.today()
        assert conn.execute("SELECT MAX(ordered_at), MIN(ordered_at) FROM orders").fetchone() == (
            today, today - dt.timedelta(days=1))
        assert conn.execute("SELECT opened_at FROM stores").fetchone() == (dt.date(2018, 1, 1) + (
            today - dt.date(2018, 9, 1)),)
        assert [r[0] for r in conn.execute("SHOW TABLES").fetchall() if r[0].endswith("_raw")] == []
    finally:
        conn.close()


class _RecordingConnection:
    """Delegates to a DuckDB connection, recording each statement it executes."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self.statements: list[str] = []

    def execute(self, query: str, parameters: object = None) -> duckdb.DuckDBPyConnection:
        self.statements.append(query)
        return self._conn.execute(query) if parameters is None else self._conn.execute(query, parameters)

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)


_VIEW_DDL = re.compile(r"\b(CREATE|DROP)\b[\w\s]*?\bVIEW\b", re.IGNORECASE)


def test_loading_the_demo_creates_no_view(tmp_path: Path, jafgen_dir: str) -> None:
    """Spec: demo/jaffle-shop › Loading the demo creates no view."""
    conn = duckdb.connect(str(tmp_path / "demo.duckdb"))
    try:
        jaffle_shop.create_schema(conn)
        recorder = _RecordingConnection(conn)
        jaffle_shop.load_data(conn=cast(duckdb.DuckDBPyConnection, recorder), data_dir=jafgen_dir)
        assert [s for s in recorder.statements if _VIEW_DDL.search(s)] == []
        for table in ("orders", "stores", "customers"):
            row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
            assert row is not None and row[0] > 0, table
    finally:
        conn.close()
