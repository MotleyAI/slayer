"""Shared fixtures for the first/last (§5.8) pinning matrix.

Underscore-prefixed (like ``tests/_dev1746_fixtures.py`` and
``tests/_dev1747_fixtures.py``) so pytest skips it during collection while
``from tests._dev1748_fixtures import ...`` works.

The corpus exists to make first/last SEMANTICS observable, so every group is
built around one thing a wrong implementation would get wrong:

* ``paid`` — the plain case. ``first`` and ``last`` differ, and neither equals
  ``min`` or ``max`` of the group, so an implementation that quietly aggregated
  instead of ranking fails rather than coincidentally matching.
* ``tie`` — two rows sharing a timestamp with different values. ``ROW_NUMBER``
  breaks that tie arbitrarily, so this group is asserted by MEMBERSHIP and
  documented as nondeterministic (§5.8: no false parity).
* ``nulltime`` — a NULL ranking timestamp competing with a real one. A NULL key
  is never picked, so the dated row wins both ``first`` and ``last``.
* ``nullval`` — the winning row's VALUE is NULL. ``last`` must be NULL, not the
  largest non-NULL value in the group; this is what separates
  ``MAX(CASE WHEN rn = 1 THEN v END)`` from ``MAX(v)``.
* ``filt`` — the newest row does NOT match the metric's ``DatasetField.filter`` and
  an older one does, so a filtered first/last that ranked before filtering
  returns the wrong row (or NULL) instead of the right one.
* ``nomatch`` — NO row matches the filter. The group must survive with a NULL
  metric, which is the case that separates a WHERE inside an isolated CTE
  (rows vanish, LEFT JOIN restores the group) from a broken inner join.
* ``NULL`` status — a NULL GRAIN member, so the null-safe join-back (P-I) has
  something to match.

``order_tags`` is the 1:N fan-out: order 15 carries FOUR tag rows (``rush``
twice, plus ``gift`` and ``fragile``) while orders 1, 2 and 16 carry one each.
The duplicate ``rush`` is what makes multiply-per-match observable — a join
pulled in for one metric changes another's value if it leaks into the wrong
scope.

``empty_orders`` is a structurally identical but EMPTY table. An ungrouped
aggregate over it must still return exactly one row carrying NULL — the
invariant the CROSS-JOIN-for-empty-grain join-back depends on.

Every seeded amount is distinct and no group total is a multiple of another, so
a test reading the wrong column fails rather than accidentally agreeing.
"""

from __future__ import annotations

from typing import List

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import make_seeded_sqlite_engine

# --------------------------------------------------------------------------- #
# The corpus — expectations named, so a test asserts against a name not a digit
# --------------------------------------------------------------------------- #

#: ``paid``: two rows, ordered. first != last, and both differ from min/max.
PAID_FIRST = 11.0
PAID_LAST = 13.0

#: The ``paid`` rows belong to DIFFERENT customers, and their customers' signup
#: order is the REVERSE of their own ``created_at`` order. So ranking ``paid``
#: by a time field reached through the customers relationship picks the row that
#: ranking by ``created_at`` rejects — which is what makes a test of the joined
#: derived time arg able to fail. Without the inversion every row of a group
#: would share one signup timestamp, the ranking would be a tie, and an
#: implementation that ignored the argument entirely would pass.
PAID_BY_JOINED_SIGNUP = PAID_FIRST

#: ``tie``: identical timestamps. Either value is a legal answer.
TIE_CANDIDATES = (21.0, 22.0)

#: ``nulltime``: one NULL timestamp, one real one.
NULLTIME_NULL_ROW_AMOUNT = 31.0
NULLTIME_DATED_ROW_AMOUNT = 32.0

#: ``nullval``: the newest row's value is NULL; the older one is not.
NULLVAL_OLDER = 41.0

#: ``filt``: the newest row (5.0) is below the ``big_amount`` threshold; the
#: older one (61.0) is above it. A correct filtered ``last`` returns 61.0.
FILT_MATCHING = 61.0
FILT_NEWER_NONMATCHING = 5.0

#: ``nomatch``: neither row clears the threshold.
NOMATCH_AMOUNTS = (1.0, 2.0)

#: NULL-status group — the nullable grain member.
NULL_STATUS_FIRST = 51.0
NULL_STATUS_LAST = 52.0

#: ``fan``: order 15 carries four tags, TWO of them ``rush``. The duplicate is
#: what makes 1:N multiply-per-match observable — filtering to ``rush`` matches
#: order 15 twice, so a sibling ``sum(amount)`` doubles its contribution while
#: the ranked pick, which only cares which row is newest, does not move.
FAN_FIRST = 71.0
FAN_LAST = 73.0
FAN_RUSH_DUPLICATES = 2
#: ``sum`` over the ``fan`` group when filtered to ``rush``: order 15 counted
#: twice plus order 16 once.
FAN_RUSH_MULTIPLIED_SUM = FAN_FIRST * FAN_RUSH_DUPLICATES + FAN_LAST
#: A fanning ``rush`` population filter restricts by association, so the
#: sibling sum counts each order once — order 15 and order 16, no duplicate.
FAN_RUSH_ASSOC_SUM = FAN_FIRST + FAN_LAST

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
    """Create + seed the SQLite corpus at ``db_path``."""
    with transaction(db_path) as con:
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
                # Signed up last -> wins ``last(customers.spend)``.
                (101, 2, None, CUSTOMER_SPEND_LAST, "2024-03-01"),
            ],
        )
        con.executemany(
            "INSERT INTO orders VALUES (?,?,?,?,?,?)",
            [
                # paid — the plain ordered pair. The customers are INVERTED against
                # the created_at order (customer 101 signed up LAST, customer 100
                # FIRST), so ranking this group through the customers relationship reverses
                # the winner. See PAID_BY_JOINED_SIGNUP.
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


# --------------------------------------------------------------------------- #
# Datasets
# --------------------------------------------------------------------------- #


def dev1748_datasets() -> List[SlayerDataset]:
    """``orders -> customers -> regions`` plus 1:N ``order_tags`` and an empty
    twin of ``orders``.

    Returned host-first; ``[0]`` is the host and the rest are ``extra_datasets``.
    """
    regions = SlayerDataset(
        name="regions", sql_table="regions", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="opened_at", datatype=DataType.DATETIME),
            DatasetField(name="population", datatype=DataType.FLOAT),
        ],
    )
    customers = SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        default_time_dimension="signup_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="spend", datatype=DataType.FLOAT),
            DatasetField(name="signup_at", datatype=DataType.DATETIME),
            # A LOCAL derived time field on the target — the explicit
            # cross-dataset time arg that needs no further relationship.
            DatasetField(
                name="signup_alias", datatype=DataType.DATETIME, sql="signup_at",
            ),
            # A derived time field whose sql CROSSES customers -> regions.
            # Used as an explicit time arg this is the residual-hop shape.
            DatasetField(
                name="deep_opened", datatype=DataType.DATETIME,
                expression="regions.opened_at",
            ),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )
    order_tags = SlayerDataset(
        name="order_tags", sql_table="order_tags", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="order_id", datatype=DataType.INTEGER),
            DatasetField(name="name", datatype=DataType.STRING),
        ],
    )
    orders = SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(name="shipped_at", datatype=DataType.DATETIME),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            # Filtered on a LOCAL field.
            DatasetField(
                name="big_amount", datatype=DataType.FLOAT, sql="amount",
                filter=f"amount > {BIG_AMOUNT_THRESHOLD}",
            ),
            # Filtered on a JOINED field.
            DatasetField(
                name="gold_amount", datatype=DataType.FLOAT, sql="amount",
                filter="customers.tier = 'gold'",
            ),
            # Filtered on a DERIVED field (``amount_x2`` is local-derived).
            DatasetField(
                name="doubled_big", datatype=DataType.FLOAT, sql="amount",
                filter=f"amount * 2 > {BIG_AMOUNT_THRESHOLD * 2}",
            ),
            # A derived value whose sql CROSSES into regions.
            DatasetField(
                name="cust_region", datatype=DataType.STRING,
                expression="customers.regions.name",
            ),
            # A local derived value — the non-crossing control.
            DatasetField(name="amount_x2", datatype=DataType.FLOAT, expression="amount * 2"),
            # A local derived TIME field, for an explicit derived time arg.
            DatasetField(
                name="created_alias", datatype=DataType.DATETIME, sql="created_at",
            ),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            DatasetRelationship(to="order_tags", from_columns=["id"], to_columns=["order_id"]),
        ],
    )
    empty_orders = SlayerDataset(
        name="empty_orders", sql_table="empty_orders", data_source="test",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(name="shipped_at", datatype=DataType.DATETIME),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(
                name="big_amount", datatype=DataType.FLOAT, sql="amount",
                filter=f"amount > {BIG_AMOUNT_THRESHOLD}",
            ),
        ],
    )
    return [orders, customers, regions, order_tags, empty_orders]


def dev1748_bundle() -> "ResolvedSourceBundle":
    """The corpus as a resolved bundle, for calling ``plan_query`` directly.

    Plan-level tests assert on the plan the planner builds; going through the
    engine would only add a render step they do not care about.
    """
    models = dev1748_datasets()
    return ResolvedSourceBundle(
        dialect="postgres",
        population=models[0], referenced_datasets=list(models[1:]),
    )


async def make_sqlite_engine(base_dir: str, db_path: str) -> SlayerQueryEngine:
    """Storage + engine bound to the seeded SQLite file at ``db_path``."""
    return await make_seeded_sqlite_engine(
        base_dir=base_dir, db_path=db_path, datasets=dev1748_datasets()
    )


# --------------------------------------------------------------------------- #
# Result helpers
# --------------------------------------------------------------------------- #


def by_group(rows: List[dict], *, key: str, value: str) -> dict:
    """``{group: value}`` from a response's rows.

    Keyed on the raw value, NULL included, so a NULL grain member is a real
    entry rather than a missing one — which is the whole point of the
    null-safe join-back.
    """
    return {row[key]: row[value] for row in rows}
