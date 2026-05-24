"""DEV-1450 follow-up #4a — derived-column time dimensions.

A ``TimeDimension`` whose column resolves to a DERIVED column
(``Column.sql`` set) now works everywhere a base-column time dimension
does. The typed pipeline widens ``TimeTruncKey.column`` to
``Union[ColumnKey, ColumnSqlKey]`` and the SQL generator applies
``DATE_TRUNC`` over the EXPANDED derived expression rather than over a
bare identifier.

Two test styles:

* bind-level (``bind_time_dimension`` yields ``TimeTruncKey`` whose
  ``column`` is a ``ColumnSqlKey``);
* engine end-to-end against a seeded SQLite, covering the derived TD as
  a dimension, in ORDER BY, under window (cumsum) and self-join
  (time_shift / change / consecutive_periods) transforms, with
  ``date_range``, as a joined TD, and as a cross-model shared grain.

Before the #4a implementation these all fail because
``bind_time_dimension`` raises ``NotImplementedError`` for a derived TD.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import ColumnSqlKey, Phase, TimeTruncKey
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_time_dimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="prod",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="shipped_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
            # Non-trivial derived temporal column: "effective" date is the
            # shipped date when present, else the created date.
            Column(
                name="effective_at",
                sql="coalesce(shipped_at, created_at)",
                type=DataType.TIMESTAMP,
                label="Effective date",
            ),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="prod",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="signed_up_at", type=DataType.TIMESTAMP),
            # Derived temporal column on the JOIN TARGET (shifts a month
            # forward so it is unambiguously non-trivial).
            Column(
                name="signup_eff",
                sql="datetime(signed_up_at, '+1 month')",
                type=DataType.TIMESTAMP,
            ),
        ],
    )


@pytest.fixture
async def engine() -> AsyncIterator[Tuple[SlayerQueryEngine, str]]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, "
        "revenue REAL, signed_up_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            (1, "NA", 100.0, "2023-01-15 00:00:00"),
            (2, "NA", 50.0, "2023-02-20 00:00:00"),
            (3, "EU", 70.0, "2023-03-10 00:00:00"),
        ],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "created_at TEXT, shipped_at TEXT, amount REAL)"
    )
    # effective_at = coalesce(shipped_at, created_at):
    #   row1 -> 2024-01, row2 -> 2024-02 (shipped overrides created Jan),
    #   row3 -> 2024-02, row4 -> 2024-03, row5 -> 2024-03.
    # By DERIVED month:  Jan=10/1, Feb=12/2, Mar=12/2.
    # By BARE created_at: Jan=15/2, Feb=7/1,  Mar=12/2  (distinguishes).
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            (1, 1, "2024-01-10 00:00:00", None, 10.0),
            (2, 1, "2024-01-20 00:00:00", "2024-02-05 00:00:00", 5.0),
            (3, 2, "2024-02-15 00:00:00", None, 7.0),
            (4, 3, "2024-03-05 00:00:00", None, 3.0),
            (5, 3, "2024-03-25 00:00:00", "2024-03-28 00:00:00", 9.0),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(_customers_model())
    await storage.save_model(_orders_model())
    yield SlayerQueryEngine(storage=storage), db_path


def _bundle_local() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_model(),
        referenced_models=[_customers_model()],
    )


# ---------------------------------------------------------------------------
# bind_time_dimension
# ---------------------------------------------------------------------------


class TestBindDerivedTimeDimension:
    def test_local_derived_td_binds_to_columnsqlkey(self) -> None:
        td = TimeDimension(
            dimension=ColumnRef(name="effective_at"),
            granularity=TimeGranularity.MONTH,
        )
        bound = bind_time_dimension(
            td,
            scope=ModelScope(source_model=_orders_model()),
            bundle=_bundle_local(),
        )
        assert isinstance(bound.value_key, TimeTruncKey)
        assert bound.value_key.column == ColumnSqlKey(
            path=(), model="orders", column_name="effective_at",
        )
        assert bound.value_key.granularity == "month"
        assert bound.phase == Phase.ROW

    def test_joined_derived_td_carries_path(self) -> None:
        td = TimeDimension(
            dimension=ColumnRef(name="customers.signup_eff"),
            granularity=TimeGranularity.MONTH,
        )
        bound = bind_time_dimension(
            td,
            scope=ModelScope(source_model=_orders_model()),
            bundle=_bundle_local(),
        )
        assert isinstance(bound.value_key, TimeTruncKey)
        assert bound.value_key.column == ColumnSqlKey(
            path=("customers",), model="customers", column_name="signup_eff",
        )


# ---------------------------------------------------------------------------
# Engine end-to-end
# ---------------------------------------------------------------------------


def _amounts(resp, key: str):
    return sorted(r[key] for r in resp.data)


async def test_derived_td_dimension_groups_by_derived_expr(engine):
    """A derived TD groups by the EXPANDED expression, not the bare column;
    the result key matches the base-column TD shape (``orders.effective_at``)."""
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="effective_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "amount:sum"}, {"formula": "*:count"}],
        )
    )
    assert "orders.effective_at" in resp.columns
    assert resp.row_count == 3
    # Derived split is {10, 12, 12}; the bare-created_at split would be
    # {7, 12, 15}.
    assert _amounts(resp, "orders.amount_sum") == [10.0, 12.0, 12.0]
    assert _amounts(resp, "orders._count") == [1, 2, 2]


async def test_derived_td_carries_attribute_label(engine):
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="effective_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "amount:sum"}],
        )
    )
    assert resp.attributes.dimensions["orders.effective_at"].label == "Effective date"


async def test_derived_td_order_by(engine):
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="effective_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "amount:sum"}],
            order=[OrderItem(column=ColumnRef(name="effective_at"), direction="asc")],
        )
    )
    # Ascending by derived month: Jan=10, Feb=12, Mar=12.
    assert [r["orders.amount_sum"] for r in resp.data] == [10.0, 12.0, 12.0]


async def test_derived_td_cumsum_over_order(engine):
    """cumsum's OVER(ORDER BY <td>) uses the derived TD's SELECT alias."""
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="effective_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                {"formula": "*:count"},
                {"formula": "cumsum(*:count)", "name": "cs"},
            ],
            order=[OrderItem(column=ColumnRef(name="effective_at"), direction="asc")],
        )
    )
    # Derived counts per month: 1, 2, 2 -> cumsum 1, 3, 5.
    assert [r["orders.cs"] for r in resp.data] == [1, 3, 5]


async def test_derived_td_time_shift(engine):
    """time_shift's self-join CTE shifts the EXPANDED derived expression."""
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="effective_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                {"formula": "amount:sum"},
                {"formula": "time_shift(amount:sum, periods=-1)", "name": "prev"},
            ],
            order=[OrderItem(column=ColumnRef(name="effective_at"), direction="asc")],
        )
    )
    # Derived amounts per month: Jan=10, Feb=12, Mar=12.
    # Prior-period: Jan=None, Feb=10, Mar=12.
    assert [r["orders.prev"] for r in resp.data] == [None, 10.0, 12.0]


async def test_derived_td_change(engine):
    """change desugars to time_shift + arithmetic over the derived TD."""
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="effective_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                {"formula": "amount:sum"},
                {"formula": "change(amount:sum)", "name": "chg"},
            ],
            order=[OrderItem(column=ColumnRef(name="effective_at"), direction="asc")],
        )
    )
    # change = current - prev: Jan=None, Feb=12-10=2, Mar=12-12=0.
    assert [r["orders.chg"] for r in resp.data] == [None, 2.0, 0.0]


async def test_derived_td_consecutive_periods(engine):
    """consecutive_periods reads the materialised derived-TD alias."""
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="effective_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                {"formula": "consecutive_periods(amount:sum > 5)", "name": "streak"},
            ],
            order=[OrderItem(column=ColumnRef(name="effective_at"), direction="asc")],
        )
    )
    # All three derived months have amount>5, so the run grows 1,2,3.
    assert [r["orders.streak"] for r in resp.data] == [1, 2, 3]


async def test_derived_td_date_range(engine):
    """date_range builds ``<expanded sql> BETWEEN start AND end`` — filtering
    on the derived effective date, not the bare created date."""
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="effective_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-02-01 00:00:00", "2024-02-28 00:00:00"],
            )],
            measures=[{"formula": "amount:sum"}, {"formula": "*:count"}],
        )
    )
    # Only the two effective-Feb rows survive (row2 shipped 02-05, row3
    # created 02-15) -> one month bucket, amount 12, count 2. With the bare
    # created_at it would be row3 only -> amount 7, count 1.
    assert resp.row_count == 1
    assert resp.data[0]["orders.amount_sum"] == pytest.approx(12.0)
    assert resp.data[0]["orders._count"] == 2


async def test_joined_derived_td(engine):
    """A derived TD on a JOIN TARGET (``customers.signup_eff``) resolves
    through the join and groups orders by the customer's derived month."""
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers.signup_eff"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "amount:sum"}],
        )
    )
    assert "orders.customers.signup_eff" in resp.columns
    assert resp.row_count == 3
    # cust1 (rows 1,2 -> 15), cust2 (row3 -> 7), cust3 (rows 4,5 -> 12).
    assert _amounts(resp, "orders.amount_sum") == [7.0, 12.0, 15.0]


async def test_cross_model_agg_with_joined_derived_td_shared_grain(engine):
    """A cross-model aggregate (``customers.revenue:sum``) grouped by a
    joined derived TD on the same target shares grain through the derived
    expression."""
    eng, _ = engine
    resp = await eng.execute(
        SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers.signup_eff"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "customers.revenue:sum"}],
        )
    )
    assert "orders.customers.signup_eff" in resp.columns
    assert "orders.customers.revenue_sum" in resp.columns
    # One bucket per distinct customer signup month reachable from orders.
    assert resp.row_count == 3
