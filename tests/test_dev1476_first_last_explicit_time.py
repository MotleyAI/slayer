"""DEV-1476 Stage B sub-package — explicit time arg on first/last.

Acceptance tests for the three residual bugs from the 4-bug package
(bug (a) — ``AggregateKey.args`` schema — landed in Stage A, PR #144):

* (b) ``_build_first_last_base_select`` doesn't honor ``spec.time_column``
  from ``key.args`` — fires when there's NO time dimension on the query.
* (c) Cross-model reroot strips path from kwargs but NOT from ``key.args`` —
  fires for ``customers.amount:last(customers.signup_at)`` cross-model.
* (d-cross) ``_resolve_explicit_time_col`` cross-model ``ColumnSqlKey``
  raises ``NotImplementedError`` — fires for ``customers.amount:last(
  customers.signup_at_alias)`` (derived column).

Each test executes a SQL query end-to-end against a seeded SQLite
database to prove the row-level ordering is correct.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import AggregateKey, ColumnKey, ColumnSqlKey
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.cross_model_planner import _local_agg_formula
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Unit: DEV-1476 bug (c) — reroot strips target prefix from key.args
# (symmetric to the kwarg reroot already tested in
# tests/test_dev1450fix_cross_model_derived.py).
# ---------------------------------------------------------------------------


def test_local_agg_formula_reroots_positional_columnkey_arg() -> None:
    """``customers.amount:last(customers.signup_at)`` rerooted to the
    customers scope: the positional ``ColumnKey`` arg drops its
    ``customers`` prefix and renders as a target-local identifier
    (``signup_at``), NOT as a Pydantic-repr scalar literal.
    """
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=("customers",), leaf="signup_at"),),
    )
    assert _local_agg_formula(key) == "amount:last(signup_at)"


def test_local_agg_formula_reroots_positional_columnsqlkey_arg() -> None:
    """Same as above for ``ColumnSqlKey`` (derived-column variant)."""
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(
            ColumnSqlKey(
                path=("customers",),
                model="customers",
                column_name="signup_at_alias",
            ),
        ),
    )
    assert _local_agg_formula(key) == "amount:last(signup_at_alias)"


def test_local_agg_formula_keeps_residual_path_in_args_for_deeper_hop() -> None:
    """A positional arg one hop past the target keeps its residual path
    (mirrors the kwarg-side behaviour pinned in
    ``test_local_agg_formula_keeps_residual_path_for_deeper_kwarg``).
    """
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=("customers", "regions"), leaf="opened_at"),),
    )
    assert _local_agg_formula(key) == "amount:last(regions.opened_at)"


@pytest.fixture
async def engine_with_seeded_data() -> AsyncIterator[Tuple[SlayerQueryEngine, str]]:
    """Real SQLite database with two orders per status, with strictly
    ordered ``created_at`` so first/last is verifiable.
    """
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE orders ("
        "id INTEGER PRIMARY KEY, status TEXT, amount REAL, "
        "created_at TEXT, customer_id INTEGER)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            (1, "paid", 10.0, "2024-01-01", 1),
            (2, "paid", 20.0, "2024-01-05", 1),    # last(amount,created_at) for paid = 20
            (3, "open", 7.0, "2024-01-02", 2),
            (4, "open", 14.0, "2024-01-08", 3),    # last(amount,created_at) for open = 14
        ],
    )
    cur.execute(
        "CREATE TABLE customers ("
        "id INTEGER PRIMARY KEY, region TEXT, amount REAL, "
        "signup_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            (1, "NA", 100.0, "2023-06-01"),
            (2, "NA", 50.0, "2023-07-01"),    # last(amount,signup_at) for NA = 50
            (3, "EU", 70.0, "2023-08-01"),    # last(amount,signup_at) for EU = 70
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="signup_at", type=DataType.TIMESTAMP),
                # Derived column — used by (d-cross).
                Column(
                    name="signup_at_alias",
                    sql="signup_at",
                    type=DataType.TIMESTAMP,
                ),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
                Column(name="customer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            )],
        )
    )
    yield SlayerQueryEngine(storage=storage), db_path


# ---------------------------------------------------------------------------
# (b) — no time dimension on the query, explicit time arg drives ranking
# ---------------------------------------------------------------------------


async def test_b_no_time_dimension_with_explicit_time_arg(
    engine_with_seeded_data,
) -> None:
    """``amount:last(created_at)`` succeeds with no query-level
    ``time_dimensions``. The explicit positional time arg pre-resolved
    into ``spec.time_column`` drives the ranked subquery's ORDER BY.
    """
    engine, _ = engine_with_seeded_data
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:last(created_at)"}],
    ))
    assert resp.data, resp.sql
    # Two rows: one for paid (last amount = 20), one for open (last amount = 14).
    by_status = {row["orders.status"]: row for row in resp.data}
    last_key = next(
        k for k in by_status["paid"].keys() if "last" in k.lower()
    )
    assert by_status["paid"][last_key] == pytest.approx(20.0)
    assert by_status["open"][last_key] == pytest.approx(14.0)


# ---------------------------------------------------------------------------
# (c) — cross-model bare column, explicit time arg
# ---------------------------------------------------------------------------


async def test_c_cross_model_bare_column_explicit_time_arg(
    engine_with_seeded_data,
) -> None:
    """``customers.amount:last(customers.signup_at)`` resolves cross-model
    with explicit time arg; reroot pass must strip the ``customers.``
    prefix from ``key.args`` symmetrically to kwargs.
    """
    engine, _ = engine_with_seeded_data
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.amount:last(customers.signup_at)"}],
    ))
    assert resp.data, resp.sql
    by_region = {row["orders.customers.region"]: row for row in resp.data}
    # NA last by signup_at: customer 2 signed up 2023-07-01, amount=50.
    # EU last by signup_at: customer 3 signed up 2023-08-01, amount=70.
    last_key = next(
        k for k in by_region["NA"].keys() if "last" in k.lower()
    )
    assert by_region["NA"][last_key] == pytest.approx(50.0)
    assert by_region["EU"][last_key] == pytest.approx(70.0)


# ---------------------------------------------------------------------------
# (d-cross) — cross-model derived time column
# ---------------------------------------------------------------------------


async def test_cross_model_first_last_uses_target_default_time_dimension(
    engine_with_seeded_data,
) -> None:
    """Codex round-2 fix — cross-model ``customers.amount:last`` with NO
    explicit positional time arg falls back to the target model's
    ``default_time_dimension``. Without this fix the rendered SQL
    references ``_last_rn`` against a bare ``FROM customers`` and the
    SQLite execute trips.

    Set ``customers.default_time_dimension="signup_at"`` on the fly via
    a fresh storage so the fallback is exercised.
    """
    import os
    import sqlite3
    import tempfile

    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE orders ("
        "id INTEGER PRIMARY KEY, status TEXT, amount REAL, "
        "created_at TEXT, customer_id INTEGER)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            (1, "paid", 10.0, "2024-01-01", 1),
            (2, "open", 7.0, "2024-01-02", 2),
        ],
    )
    cur.execute(
        "CREATE TABLE customers ("
        "id INTEGER PRIMARY KEY, region TEXT, amount REAL, "
        "signup_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            (1, "NA", 100.0, "2023-06-01"),
            (2, "NA", 50.0, "2023-07-01"),
            (3, "EU", 70.0, "2023-08-01"),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="prod",
        default_time_dimension="signup_at",  # ← the fallback target
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="prod",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="customer_id", type=DataType.INT),
        ],
        joins=[ModelJoin(
            target_model="customers",
            join_pairs=[["customer_id", "id"]],
        )],
    ))
    engine = SlayerQueryEngine(storage=storage)

    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.amount:last"}],  # ← NO explicit time arg
    ))
    assert resp.data, resp.sql


async def test_cross_model_first_last_with_no_time_at_all_raises() -> None:
    """Codex round-2 fix — cross-model first/last with neither an explicit
    positional time arg NOR a ``target_model.default_time_dimension`` must
    raise a clear ValueError, not silently emit broken SQL.
    """
    import os
    import sqlite3
    import tempfile

    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
    cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, amount REAL)")
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="prod",
        # default_time_dimension intentionally unset
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="prod",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(
            target_model="customers",
            join_pairs=[["customer_id", "id"]],
        )],
    ))
    engine = SlayerQueryEngine(storage=storage)

    with pytest.raises(ValueError, match=r"first/last.*ranking time"):
        await engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.amount:last"}],
        ))


async def test_d_cross_cross_model_derived_time_arg(
    engine_with_seeded_data,
) -> None:
    """``customers.amount:last(customers.signup_at_alias)`` where
    ``signup_at_alias`` is a derived column resolves end-to-end.
    Removes the ``NotImplementedError`` guard from
    ``_resolve_explicit_time_col`` for cross-model ``ColumnSqlKey``.
    """
    engine, _ = engine_with_seeded_data
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.DAY,
        )],
        measures=[{
            "formula": "customers.amount:last(customers.signup_at_alias)",
        }],
    ))
    assert resp.data, resp.sql
