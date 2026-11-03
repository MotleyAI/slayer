"""The demo loader: CSVs from any directory, cents converted, columns renamed, dates shifted to today."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import duckdb

from slayer.demo import jaffle_shop

CSVS = {
    "customers": "id,name\nc1,Ann\nc2,O'Brien\n",
    "stores": "id,name,opened_at,tax_rate\ns1,Main,2018-01-01,0.05\n",
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
