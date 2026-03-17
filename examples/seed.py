"""Shared seed data for SLayer examples.

Creates a small e-commerce dataset suitable for demonstrating rollup joins,
time dimensions, filters, and cross-model queries.

Usage:
    python seed.py <connection_string>
    python seed.py sqlite:///example.db
    python seed.py postgresql://user:pass@localhost:5432/slayer_demo
"""

import sys
from datetime import date, datetime

import sqlalchemy as sa


DROP_SQL = """
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS regions;
"""

CREATE_SQL_STANDARD = """
CREATE TABLE regions (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    region_id INTEGER REFERENCES regions(id)
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
"""

# ClickHouse uses MergeTree engine, no PRIMARY KEY constraint, no REFERENCES
CREATE_SQL_CLICKHOUSE = """
CREATE TABLE regions (
    id Int32,
    name String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE customers (
    id Int32,
    name String,
    email String,
    region_id Int32
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE products (
    id Int32,
    name String,
    category String,
    price Float64
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE orders (
    id Int32,
    customer_id Int32,
    product_id Int32,
    quantity Int32,
    status String,
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;
"""


def _get_create_sql(connection_string: str) -> str:
    """Return dialect-appropriate CREATE TABLE SQL."""
    if "clickhouse" in connection_string.lower():
        return CREATE_SQL_CLICKHOUSE
    return CREATE_SQL_STANDARD

REGIONS = [
    (1, "North America"),
    (2, "Europe"),
    (3, "Asia Pacific"),
    (4, "Latin America"),
    (5, "Middle East"),
]

CUSTOMERS = [
    (1, "Acme Corp", "acme@example.com", 1),
    (2, "Globex Inc", "globex@example.com", 2),
    (3, "Initech", "initech@example.com", 1),
    (4, "Umbrella Ltd", "umbrella@example.com", 3),
    (5, "Stark Industries", "stark@example.com", 1),
    (6, "Wayne Enterprises", "wayne@example.com", 2),
    (7, "Cyberdyne Systems", "cyberdyne@example.com", 3),
    (8, "Soylent Corp", "soylent@example.com", 4),
    (9, "Tyrell Corp", "tyrell@example.com", 3),
    (10, "Weyland-Yutani", "weyland@example.com", 5),
]

PRODUCTS = [
    (1, "Widget A", "widgets", 29.99),
    (2, "Widget B", "widgets", 49.99),
    (3, "Gadget Pro", "gadgets", 149.99),
    (4, "Gadget Mini", "gadgets", 79.99),
    (5, "Service Plan Basic", "services", 9.99),
    (6, "Service Plan Pro", "services", 29.99),
    (7, "Enterprise License", "licenses", 999.99),
    (8, "Starter Kit", "kits", 199.99),
]

ORDERS = [
    # Jan 2024 — 4 orders (slow start)
    (1, 1, 1, 3, "completed", datetime(2024, 1, 5, 10, 0)),
    (2, 2, 3, 1, "completed", datetime(2024, 1, 12, 14, 30)),
    (3, 3, 5, 10, "completed", datetime(2024, 1, 18, 9, 15)),
    (4, 4, 2, 2, "cancelled", datetime(2024, 1, 28, 11, 45)),
    # Feb 2024 — 5 orders (growing)
    (5, 1, 4, 1, "completed", datetime(2024, 2, 3, 8, 0)),
    (6, 6, 6, 5, "completed", datetime(2024, 2, 10, 13, 0)),
    (7, 7, 1, 4, "pending", datetime(2024, 2, 15, 10, 30)),
    (8, 8, 8, 1, "completed", datetime(2024, 2, 20, 15, 0)),
    (9, 2, 3, 2, "completed", datetime(2024, 2, 25, 9, 0)),
    # Mar 2024 — 6 orders (spring push)
    (10, 3, 2, 3, "completed", datetime(2024, 3, 2, 11, 0)),
    (11, 9, 7, 1, "completed", datetime(2024, 3, 8, 14, 0)),
    (12, 10, 4, 2, "cancelled", datetime(2024, 3, 14, 16, 30)),
    (13, 5, 6, 3, "completed", datetime(2024, 3, 19, 10, 0)),
    (14, 1, 3, 1, "completed", datetime(2024, 3, 25, 12, 0)),
    (15, 6, 1, 2, "completed", datetime(2024, 3, 30, 9, 0)),
    # Apr 2024 — 5 orders
    (16, 4, 1, 5, "completed", datetime(2024, 4, 1, 9, 0)),
    (17, 6, 8, 2, "pending", datetime(2024, 4, 7, 11, 30)),
    (18, 7, 5, 8, "completed", datetime(2024, 4, 12, 14, 0)),
    (19, 2, 2, 1, "completed", datetime(2024, 4, 18, 16, 0)),
    (20, 8, 7, 1, "completed", datetime(2024, 4, 24, 10, 30)),
    # May 2024 — 7 orders (peak season)
    (21, 9, 1, 2, "completed", datetime(2024, 5, 3, 8, 0)),
    (22, 10, 6, 4, "completed", datetime(2024, 5, 9, 13, 0)),
    (23, 3, 4, 1, "cancelled", datetime(2024, 5, 15, 15, 0)),
    (24, 5, 3, 2, "completed", datetime(2024, 5, 20, 11, 0)),
    (25, 1, 8, 1, "completed", datetime(2024, 5, 26, 9, 30)),
    (26, 2, 7, 1, "completed", datetime(2024, 5, 28, 14, 0)),
    (27, 4, 3, 3, "completed", datetime(2024, 5, 31, 16, 0)),
    # Jun 2024 — 4 orders (summer dip)
    (28, 4, 2, 3, "completed", datetime(2024, 6, 2, 10, 0)),
    (29, 6, 7, 1, "completed", datetime(2024, 6, 8, 14, 30)),
    (30, 2, 5, 6, "pending", datetime(2024, 6, 14, 9, 0)),
    (31, 7, 4, 2, "completed", datetime(2024, 6, 20, 16, 0)),
    # Jul 2024 — 3 orders (summer low)
    (32, 1, 1, 2, "completed", datetime(2024, 7, 5, 10, 0)),
    (33, 9, 3, 1, "completed", datetime(2024, 7, 15, 11, 0)),
    (34, 5, 6, 3, "cancelled", datetime(2024, 7, 25, 14, 0)),
    # Aug 2024 — 5 orders (recovery)
    (35, 3, 2, 4, "completed", datetime(2024, 8, 2, 9, 0)),
    (36, 10, 8, 1, "completed", datetime(2024, 8, 10, 13, 0)),
    (37, 6, 4, 2, "completed", datetime(2024, 8, 16, 15, 0)),
    (38, 8, 1, 3, "pending", datetime(2024, 8, 22, 10, 0)),
    (39, 2, 7, 1, "completed", datetime(2024, 8, 28, 16, 0)),
    # Sep 2024 — 6 orders (back to business)
    (40, 1, 3, 2, "completed", datetime(2024, 9, 3, 8, 0)),
    (41, 4, 5, 5, "completed", datetime(2024, 9, 9, 11, 0)),
    (42, 7, 2, 1, "completed", datetime(2024, 9, 14, 14, 0)),
    (43, 9, 6, 3, "completed", datetime(2024, 9, 19, 9, 30)),
    (44, 5, 8, 1, "completed", datetime(2024, 9, 24, 16, 0)),
    (45, 3, 1, 2, "cancelled", datetime(2024, 9, 29, 12, 0)),
    # Oct 2024 — 8 orders (Q4 ramp)
    (46, 1, 7, 1, "completed", datetime(2024, 10, 1, 10, 0)),
    (47, 2, 4, 3, "completed", datetime(2024, 10, 5, 13, 0)),
    (48, 6, 3, 2, "completed", datetime(2024, 10, 10, 9, 0)),
    (49, 8, 2, 4, "completed", datetime(2024, 10, 14, 15, 0)),
    (50, 10, 1, 1, "pending", datetime(2024, 10, 18, 11, 0)),
    (51, 3, 6, 5, "completed", datetime(2024, 10, 22, 14, 0)),
    (52, 5, 8, 2, "completed", datetime(2024, 10, 26, 16, 0)),
    (53, 9, 5, 3, "completed", datetime(2024, 10, 30, 10, 0)),
    # Nov 2024 — 9 orders (peak Q4)
    (54, 1, 3, 3, "completed", datetime(2024, 11, 2, 9, 0)),
    (55, 4, 7, 1, "completed", datetime(2024, 11, 5, 11, 0)),
    (56, 7, 2, 2, "completed", datetime(2024, 11, 8, 14, 0)),
    (57, 2, 8, 1, "completed", datetime(2024, 11, 11, 10, 0)),
    (58, 6, 4, 4, "completed", datetime(2024, 11, 15, 13, 0)),
    (59, 10, 1, 3, "completed", datetime(2024, 11, 18, 16, 0)),
    (60, 8, 6, 2, "pending", datetime(2024, 11, 22, 9, 0)),
    (61, 3, 3, 1, "completed", datetime(2024, 11, 25, 15, 0)),
    (62, 5, 5, 6, "completed", datetime(2024, 11, 28, 12, 0)),
    # Dec 2024 — 6 orders (holiday)
    (63, 1, 7, 2, "completed", datetime(2024, 12, 3, 10, 0)),
    (64, 9, 2, 3, "completed", datetime(2024, 12, 8, 14, 0)),
    (65, 4, 4, 1, "completed", datetime(2024, 12, 12, 11, 0)),
    (66, 6, 8, 2, "completed", datetime(2024, 12, 16, 9, 0)),
    (67, 2, 1, 5, "cancelled", datetime(2024, 12, 20, 16, 0)),
    (68, 7, 3, 1, "completed", datetime(2024, 12, 28, 13, 0)),
]


def seed(connection_string: str) -> None:
    """Create tables and insert seed data."""
    engine = sa.create_engine(connection_string)

    create_sql = _get_create_sql(connection_string)

    with engine.connect() as conn:
        # Drop and recreate tables (safe for re-runs)
        for statement in DROP_SQL.strip().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(sa.text(statement))
        for statement in create_sql.strip().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(sa.text(statement))

        # Insert data
        for r in REGIONS:
            conn.execute(sa.text("INSERT INTO regions VALUES (:id, :name)"), {"id": r[0], "name": r[1]})

        for c in CUSTOMERS:
            conn.execute(
                sa.text("INSERT INTO customers VALUES (:id, :name, :email, :region_id)"),
                {"id": c[0], "name": c[1], "email": c[2], "region_id": c[3]},
            )

        for p in PRODUCTS:
            conn.execute(
                sa.text("INSERT INTO products VALUES (:id, :name, :category, :price)"),
                {"id": p[0], "name": p[1], "category": p[2], "price": p[3]},
            )

        for o in ORDERS:
            conn.execute(
                sa.text("INSERT INTO orders VALUES (:id, :cid, :pid, :qty, :status, :created_at)"),
                {"id": o[0], "cid": o[1], "pid": o[2], "qty": o[3], "status": o[4], "created_at": o[5]},
            )

        conn.commit()

    engine.dispose()
    print(f"Seeded: {len(REGIONS)} regions, {len(CUSTOMERS)} customers, {len(PRODUCTS)} products, {len(ORDERS)} orders")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <connection_string>")
        sys.exit(1)
    seed(sys.argv[1])
