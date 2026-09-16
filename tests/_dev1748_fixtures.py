"""Shared fixtures for the DEV-1748 first/last (§5.8) pinning matrix.

Underscore-prefixed so pytest skips collection. Each seeded group targets one
thing a wrong implementation gets wrong (see per-group comments below); ``fan``'s
1:N ``order_tags`` (order 15 tagged ``rush`` twice) covers fan-out and
``empty_orders`` the empty-grain join-back. Every amount is distinct.
"""

from __future__ import annotations

import sqlite3
from typing import List

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._engine_helpers import make_seeded_sqlite_engine

# The corpus — expectations named, so a test asserts against a name not a digit.

#: ``paid``: two rows, ordered. first != last, and both differ from min/max.
PAID_FIRST = 11.0
PAID_LAST = 13.0

#: ``paid``'s customers' signup order reverses their ``created_at`` order, so a
#: joined derived time arg picks the row ``created_at`` rejects.
PAID_BY_JOINED_SIGNUP = PAID_FIRST

#: ``tie``: identical timestamps. Either value is a legal answer.
TIE_CANDIDATES = (21.0, 22.0)

#: ``nulltime``: one NULL timestamp, one real one.
NULLTIME_NULL_ROW_AMOUNT = 31.0
NULLTIME_DATED_ROW_AMOUNT = 32.0

#: ``nullval``: the newest row's value is NULL; the older one is not.
NULLVAL_OLDER = 41.0

#: ``filt``: newest row (5.0) below the ``big_amount`` threshold, older (61.0) above; filtered ``last`` = 61.0.
FILT_MATCHING = 61.0
FILT_NEWER_NONMATCHING = 5.0

#: ``nomatch``: neither row clears the threshold.
NOMATCH_AMOUNTS = (1.0, 2.0)

#: NULL-status group — the nullable grain member.
NULL_STATUS_FIRST = 51.0
NULL_STATUS_LAST = 52.0

#: ``fan``: order 15 carries four tags, TWO of them ``rush`` — the semi-join matches it once.
FAN_FIRST = 71.0
FAN_LAST = 73.0
FAN_RUSH_DUPLICATES = 2
#: ``sum`` over ``fan`` filtered to ``rush``: order 15 + order 16, never 15 twice.
FAN_RUSH_SUM = FAN_FIRST + FAN_LAST

#: The ``big_amount`` / ``gold_amount`` filter threshold.
BIG_AMOUNT_THRESHOLD = 20.0

#: Customers, ranked by ``signup_at`` — customer 101 signed up last.
CUSTOMER_SPEND_FIRST = 1000.0
CUSTOMER_SPEND_LAST = 250.0

_ORDERS_DDL = """
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    status TEXT,
    created_at TEXT,
    shipped_at TEXT,
    amount REAL
"""


def seed_dev1748_sqlite(db_path: str) -> None:
    """Create + seed the DEV-1748 SQLite corpus at ``db_path``."""
    con = sqlite3.connect(db_path)
    con.executescript(
        f"""
        CREATE TABLE regions (
            id INTEGER PRIMARY KEY,
            name TEXT,
            opened_at TEXT,
            population REAL
        );
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            region_id INTEGER,
            tier TEXT,
            spend REAL,
            signup_at TEXT
        );
        CREATE TABLE orders ({_ORDERS_DDL});
        CREATE TABLE empty_orders ({_ORDERS_DDL});
        CREATE TABLE order_tags (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            name TEXT
        );
        """
    )
    con.executemany(
        "INSERT INTO regions VALUES (?,?,?,?)",
        # Region 2's name is NULL — a joined nullable grain member.
        [(1, "Alpha", "2020-01-01", 100.0), (2, None, "2021-01-01", 200.0)],
    )
    con.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?)",
        [
            (100, 1, "gold", CUSTOMER_SPEND_FIRST, "2024-01-01"),
            (102, 2, "silver", 75.0, "2024-02-01"),
            # Signed up last -> wins ``customers.spend:last``.
            (101, 2, None, CUSTOMER_SPEND_LAST, "2024-03-01"),
        ],
    )
    con.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?,?)",
        [
            # paid — customers INVERTED against created_at order (see PAID_BY_JOINED_SIGNUP).
            (1, 101, "paid", "2024-01-01", "2024-01-02", PAID_FIRST),
            (2, 100, "paid", "2024-03-01", "2024-03-02", PAID_LAST),
            # tie — same timestamp, different values.
            (3, 100, "tie", "2024-02-02", "2024-02-03", TIE_CANDIDATES[0]),
            (4, 100, "tie", "2024-02-02", "2024-02-04", TIE_CANDIDATES[1]),
            # nulltime — a NULL ranking timestamp against a real one.
            (5, 100, "nulltime", None, "2024-01-06", NULLTIME_NULL_ROW_AMOUNT),
            (6, 100, "nulltime", "2024-01-05", None, NULLTIME_DATED_ROW_AMOUNT),
            # nullval — the newest row's VALUE is NULL.
            (7, 100, "nullval", "2024-01-07", "2024-01-08", NULLVAL_OLDER),
            (8, 100, "nullval", "2024-02-07", "2024-02-08", None),
            # NULL status — the nullable grain member.
            (9, 101, None, "2024-01-09", "2024-01-10", NULL_STATUS_FIRST),
            (10, 101, None, "2024-02-09", "2024-02-10", NULL_STATUS_LAST),
            # nomatch — no row clears the big_amount threshold.
            (11, 101, "nomatch", "2024-01-11", "2024-01-12", NOMATCH_AMOUNTS[0]),
            (12, 101, "nomatch", "2024-02-11", "2024-02-12", NOMATCH_AMOUNTS[1]),
            # filt — the NEWEST row does not match; an older one does.
            (13, 102, "filt", "2024-01-13", "2024-01-14", FILT_MATCHING),
            (14, 102, "filt", "2024-02-13", "2024-02-14", FILT_NEWER_NONMATCHING),
            # fan — order 15 carries four tag rows (``rush`` twice).
            (15, 102, "fan", "2024-01-15", "2024-01-16", FAN_FIRST),
            (16, 102, "fan", "2024-03-15", "2024-03-16", FAN_LAST),
        ],
    )
    con.executemany(
        "INSERT INTO order_tags VALUES (?,?,?)",
        [
            (1, 15, "rush"), (2, 15, "gift"), (3, 15, "fragile"),
            # The DUPLICATE match: order 15 is tagged ``rush`` twice.
            (7, 15, "rush"),
            (4, 16, "rush"),
            (5, 1, "rush"), (6, 2, "rush"),
        ],
    )
    con.commit()
    con.close()


# Models.


def dev1748_models() -> List[SlayerModel]:
    """``orders -> customers -> regions`` + 1:N ``order_tags`` + empty twin; host-first (``[0]``)."""
    regions = SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="opened_at", type=DataType.TIMESTAMP),
            Column(name="population", type=DataType.DOUBLE),
        ],
    )
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        default_time_dimension="signup_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
            # A LOCAL derived time column on the target (needs no further join).
            Column(
                name="signup_alias", type=DataType.TIMESTAMP, sql="signup_at",
            ),
            # A derived time column whose sql CROSSES customers -> regions (residual hop).
            Column(
                name="deep_opened", type=DataType.TIMESTAMP,
                sql="regions.opened_at",
            ),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    order_tags = SlayerModel(
        name="order_tags", sql_table="order_tags", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="order_id", type=DataType.INT),
            Column(name="name", type=DataType.TEXT),
        ],
    )
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="shipped_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
            # Filtered on a LOCAL column.
            Column(
                name="big_amount", type=DataType.DOUBLE, sql="amount",
                filter=f"amount > {BIG_AMOUNT_THRESHOLD}",
            ),
            # Filtered on a JOINED column.
            Column(
                name="gold_amount", type=DataType.DOUBLE, sql="amount",
                filter="customers.tier = 'gold'",
            ),
            # Filtered on a DERIVED column (``amount_x2`` is local-derived).
            Column(
                name="doubled_big", type=DataType.DOUBLE, sql="amount",
                filter=f"amount * 2 > {BIG_AMOUNT_THRESHOLD * 2}",
            ),
            # A derived value whose sql CROSSES into regions.
            Column(
                name="cust_region", type=DataType.TEXT,
                sql="customers.regions.name",
            ),
            # A local derived value — the non-crossing control.
            Column(name="amount_x2", type=DataType.DOUBLE, sql="amount * 2"),
            # A local derived TIME column, for an explicit derived time arg.
            Column(
                name="created_alias", type=DataType.TIMESTAMP, sql="created_at",
            ),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="order_tags", join_pairs=[["id", "order_id"]]),
        ],
    )
    empty_orders = SlayerModel(
        name="empty_orders", sql_table="empty_orders", data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="shipped_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
            Column(
                name="big_amount", type=DataType.DOUBLE, sql="amount",
                filter=f"amount > {BIG_AMOUNT_THRESHOLD}",
            ),
        ],
    )
    return [orders, customers, regions, order_tags, empty_orders]


def dev1748_bundle() -> "ResolvedSourceBundle":
    """The corpus as a resolved bundle, for calling ``plan_query`` directly."""
    models = dev1748_models()
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=list(models[1:]),
    )


async def make_sqlite_engine(base_dir: str, db_path: str) -> SlayerQueryEngine:
    """Storage + engine bound to the seeded SQLite file at ``db_path``."""
    return await make_seeded_sqlite_engine(
        base_dir=base_dir, db_path=db_path, models=dev1748_models()
    )


# Result helpers.


def by_group(rows: List[dict], *, key: str, value: str) -> dict:
    """``{group: value}`` from a response's rows, keyed on the raw value (NULL included)."""
    return {row[key]: row[value] for row in rows}
