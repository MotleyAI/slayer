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
from typing import AsyncIterator

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


def _orders_model(with_amount: bool = False) -> SlayerModel:
    """Standard orders model joined to customers. ``with_amount`` adds the
    ``amount`` / ``created_at`` columns for queries that aggregate orders
    locally; the join-only variant omits them.
    """
    columns = [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="status", type=DataType.TEXT),
    ]
    if with_amount:
        columns += [
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ]
    columns.append(Column(name="customer_id", type=DataType.INT))
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="prod",
        columns=columns,
        joins=[ModelJoin(
            target_model="customers",
            join_pairs=[["customer_id", "id"]],
        )],
    )


async def _engine_from_sql(
    *,
    ddl: list[str],
    inserts: list[tuple[str, list[tuple]]],
    models: list[SlayerModel],
) -> SlayerQueryEngine:
    """Build a ``SlayerQueryEngine`` over a throwaway SQLite file.

    ``ddl`` statements run verbatim; each ``inserts`` entry is an
    ``(sql, rows)`` pair fed to ``executemany``; ``models`` are persisted
    against a ``prod`` SQLite datasource pointing at the seeded file.
    """
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    for stmt in ddl:
        cur.execute(stmt)
    for sql, rows in inserts:
        cur.executemany(sql, rows)
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    for model in models:
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


@pytest.fixture
async def engine_with_seeded_data() -> AsyncIterator[SlayerQueryEngine]:
    """Real SQLite database with two orders per status, with strictly
    ordered ``created_at`` so first/last is verifiable.

    ``last(amount, created_at)``: paid → 20, open → 14.
    ``last(amount, signup_at)``: NA → 50, EU → 70.
    """
    yield await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, status TEXT, amount REAL, "
            "created_at TEXT, customer_id INTEGER)",
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, region TEXT, amount REAL, "
            "signup_at TEXT)",
        ],
        inserts=[
            ("INSERT INTO orders VALUES (?,?,?,?,?)", [
                (1, "paid", 10.0, "2024-01-01", 1),
                (2, "paid", 20.0, "2024-01-05", 1),
                (3, "open", 7.0, "2024-01-02", 2),
                (4, "open", 14.0, "2024-01-08", 3),
            ]),
            ("INSERT INTO customers VALUES (?,?,?,?)", [
                (1, "NA", 100.0, "2023-06-01"),
                (2, "NA", 50.0, "2023-07-01"),
                (3, "EU", 70.0, "2023-08-01"),
            ]),
        ],
        models=[
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
            ),
            _orders_model(with_amount=True),
        ],
    )


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
    engine = engine_with_seeded_data
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
    engine = engine_with_seeded_data
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


async def test_local_first_last_over_derived_column_expands_inner_refs(
    engine_with_seeded_data,
) -> None:
    """Codex round-3 fix — local ``first``/``last`` over a derived
    ``ColumnSqlKey`` aggregate source must qualify the inner bare refs
    in ``Column.sql`` via ``_expand_derived_column_sql``.

    Without bundle threading, the ranked-subquery path bypassed the
    derived-ref expansion, so e.g. ``net_amount:last(created_at)``
    where ``net_amount.sql = "amount * 0.9"`` would render the bare
    ``amount`` inside the CASE expression without qualifying it under
    the source relation.

    We can't easily inspect the rendered SQL here (the ``MAX(CASE WHEN
    _last_rn = 1 ...)`` body wraps the derived sql); the end-to-end
    execute is the strongest pin — if the bare ``amount`` is unqualified
    and the FROM is the ranked subquery's own alias, SQLite will fail
    with "no such column".
    """
    engine = engine_with_seeded_data
    # Add a derived ``net_amount`` column on orders so ``net_amount:last``
    # exercises the ColumnSqlKey aggregate-source path.
    orders = await engine.storage.get_model("orders")
    assert orders is not None
    orders = orders.model_copy(update={
        "columns": list(orders.columns) + [
            Column(
                name="net_amount",
                sql="amount * 0.9",
                type=DataType.DOUBLE,
            ),
        ],
    })
    await engine.storage.save_model(orders)

    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "net_amount:last(created_at)"}],
    ))
    assert resp.data, resp.sql


async def test_cross_model_first_last_uses_target_default_time_dimension() -> None:
    """Codex round-2 fix — cross-model ``customers.amount:last`` with NO
    explicit positional time arg falls back to the target model's
    ``default_time_dimension``. Without this fix the rendered SQL
    references ``_last_rn`` against a bare ``FROM customers`` and the
    SQLite execute trips.

    Set ``customers.default_time_dimension="signup_at"`` on the fly via
    a fresh storage so the fallback is exercised.
    """
    engine = await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, status TEXT, amount REAL, "
            "created_at TEXT, customer_id INTEGER)",
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, region TEXT, amount REAL, "
            "signup_at TEXT)",
        ],
        inserts=[
            ("INSERT INTO orders VALUES (?,?,?,?,?)", [
                (1, "paid", 10.0, "2024-01-01", 1),
                (2, "open", 7.0, "2024-01-02", 2),
            ]),
            ("INSERT INTO customers VALUES (?,?,?,?)", [
                (1, "NA", 100.0, "2023-06-01"),
                (2, "NA", 50.0, "2023-07-01"),
                (3, "EU", 70.0, "2023-08-01"),
            ]),
        ],
        models=[
            SlayerModel(
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
            ),
            _orders_model(with_amount=True),
        ],
    )

    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.amount:last"}],  # ← NO explicit time arg
    ))
    by_region = {row["orders.customers.region"]: row for row in resp.data}
    # Only NA appears because no order references the EU customer.
    assert set(by_region) == {"NA"}, resp.sql
    last_key = next(k for k in by_region["NA"].keys() if "last" in k.lower())
    # NA rows: (100.0 @ 2023-06-01) and (50.0 @ 2023-07-01); ``last`` by
    # signup_at picks 50.0. Without the default_time_dimension fallback,
    # this would NULL out or pick by another (non-deterministic) order.
    assert by_region["NA"][last_key] == pytest.approx(50.0), resp.sql


async def test_cross_model_first_last_with_no_time_at_all_raises() -> None:
    """Codex round-2 fix — cross-model first/last with neither an explicit
    positional time arg NOR a ``target_model.default_time_dimension`` must
    raise a clear ValueError, not silently emit broken SQL.
    """
    engine = await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)",
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, region TEXT, amount REAL)",
        ],
        inserts=[],
        models=[
            SlayerModel(
                name="customers",
                sql_table="customers",
                data_source="prod",
                # default_time_dimension intentionally unset
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="region", type=DataType.TEXT),
                    Column(name="amount", type=DataType.DOUBLE),
                ],
            ),
            SlayerModel(
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
            ),
        ],
    )

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
    engine = engine_with_seeded_data
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


async def test_cross_model_last_with_target_filter_ranks_filtered_rows() -> None:
    """Codex fix — cross-model first/last must compute ``_last_rn`` /
    ``_first_rn`` over the FILTERED row set, not the full target table.

    Without the fix, a target-model filter (``deleted_at IS NULL``)
    applies on the outer CTE after ranking; a soft-deleted row that is
    the most-recent by ``signup_at`` still wins ``_last_rn = 1`` inside
    the subquery, and the outer ``MAX(CASE WHEN _last_rn = 1 ...)`` then
    returns NULL because that winning row is excluded by the WHERE.

    With the fix, the filter is pushed inside the ranked subquery, so
    ``_last_rn = 1`` points at the most-recent non-deleted row.
    """
    engine = await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, status TEXT, customer_id INTEGER)",
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, region TEXT, amount REAL, "
            "signup_at TEXT, deleted_at TEXT)",
        ],
        inserts=[
            ("INSERT INTO orders VALUES (?,?,?)",
             [(1, "paid", 1), (2, "paid", 2)]),
            # NA: id=1 is older and active; id=2 is newer but soft-deleted.
            # With the filter applied BEFORE ranking, last(amount) = 100.0
            # (the active row). With the buggy post-rank filter, the
            # newer-but-deleted row wins _last_rn = 1 and the outer
            # MAX(CASE WHEN _last_rn = 1 ...) is NULL.
            ("INSERT INTO customers VALUES (?,?,?,?,?)", [
                (1, "NA", 100.0, "2023-06-01", None),
                (2, "NA", 999.0, "2023-08-01", "2024-01-01"),
            ]),
        ],
        models=[
            SlayerModel(
                name="customers",
                sql_table="customers",
                data_source="prod",
                default_time_dimension="signup_at",
                filters=["deleted_at IS NULL"],
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="region", type=DataType.TEXT),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="signup_at", type=DataType.TIMESTAMP),
                    Column(name="deleted_at", type=DataType.TIMESTAMP),
                ],
            ),
            _orders_model(),
        ],
    )

    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.amount:last"}],
    ))
    by_region = {row["orders.customers.region"]: row for row in resp.data}
    assert set(by_region) == {"NA"}, resp.sql
    last_key = next(k for k in by_region["NA"].keys() if "last" in k.lower())
    # The newer (999.0) row is deleted; ranking should skip it and surface
    # the older active row's amount (100.0). A NULL here is the regression.
    assert by_region["NA"][last_key] == pytest.approx(100.0), resp.sql


async def test_cross_model_last_over_column_filter_uses_filtered_rank() -> None:
    """Codex round-7 fix — a cross-model ``first``/``last`` over a target
    column carrying a ``Column.filter`` must use the ranked subquery's
    dedicated ``_last_rn_fN`` / ``_match_fN`` columns.

    The ranked subquery skips the bare ``_last_rn`` for filtered specs and
    emits ``_last_rn_f0`` + ``_match_f0`` instead. Before the fix
    ``_render_cross_model_cte`` built the aggregate with the bare
    ``_last_rn`` (the filtered maps were discarded), so SQLite tripped on
    ``no such column: _last_rn``. With the fix the maps are threaded into
    ``_build_agg`` and ``last`` returns the most-recent ACTIVE row.
    """
    engine = await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, status TEXT, customer_id INTEGER)",
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, region TEXT, amount REAL, "
            "signup_at TEXT, status TEXT)",
        ],
        inserts=[
            ("INSERT INTO orders VALUES (?,?,?)", [(1, "paid", 1)]),
            # NA: the newest row (300.0 @ 2023-09-01) is inactive; the
            # newest ACTIVE row is 100.0 @ 2023-06-01. A Column.filter of
            # status='active' means last(active_amount) = 100.0.
            ("INSERT INTO customers VALUES (?,?,?,?,?)", [
                (1, "NA", 100.0, "2023-06-01", "active"),
                (2, "NA", 200.0, "2023-07-01", "inactive"),
                (3, "NA", 300.0, "2023-09-01", "inactive"),
            ]),
        ],
        models=[
            SlayerModel(
                name="customers",
                sql_table="customers",
                data_source="prod",
                default_time_dimension="signup_at",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="region", type=DataType.TEXT),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="signup_at", type=DataType.TIMESTAMP),
                    Column(name="status", type=DataType.TEXT),
                    # Column-level filter: aggregations of active_amount only
                    # see active rows. This sets synth.filter_sql on the
                    # cross-model first/last path.
                    Column(
                        name="active_amount",
                        sql="amount",
                        filter="status = 'active'",
                        type=DataType.DOUBLE,
                    ),
                ],
            ),
            _orders_model(),
        ],
    )

    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.active_amount:last"}],
    ))
    by_region = {row["orders.customers.region"]: row for row in resp.data}
    assert set(by_region) == {"NA"}, resp.sql
    last_key = next(k for k in by_region["NA"].keys() if "last" in k.lower())
    # Newest active row is 100.0 (2023-06-01); the newer 200/300 rows are
    # inactive. The filtered ranking must skip them.
    assert by_region["NA"][last_key] == pytest.approx(100.0), resp.sql


async def test_local_last_over_derived_complex_time_col_qualifies_under_join() -> None:
    """Codex fix — a local ``last`` whose explicit time arg is a derived
    column with a COMPLEX ``Column.sql`` (``date(created_at)``) must qualify
    the inner bare ``created_at`` to the source relation via
    ``_expand_derived_column_sql``.

    The query also groups by a joined dimension (``customers.region``), so
    the ranked subquery's FROM is ``orders LEFT JOIN customers``. Both
    tables carry a ``created_at`` column, so an UNQUALIFIED inner ref in the
    ROW_NUMBER ORDER BY is ambiguous and SQLite raises "ambiguous column
    name: created_at". With the expansion the ref pins to ``orders``.
    """
    engine = await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, amount REAL, "
            "created_at TEXT, customer_id INTEGER)",
            # customers ALSO has a created_at column → the collision.
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, region TEXT, created_at TEXT)",
        ],
        inserts=[
            ("INSERT INTO orders VALUES (?,?,?,?)", [
                (1, 10.0, "2024-01-01", 1),
                (2, 20.0, "2024-01-05", 1),   # newest NA order → last = 20
                (3, 7.0, "2024-01-02", 2),
            ]),
            ("INSERT INTO customers VALUES (?,?,?)", [
                (1, "NA", "2099-01-01"),  # later than any order, to lose if unqualified
                (2, "NA", "2099-02-01"),
            ]),
        ],
        models=[
            SlayerModel(
                name="customers",
                sql_table="customers",
                data_source="prod",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="region", type=DataType.TEXT),
                    Column(name="created_at", type=DataType.TIMESTAMP),
                ],
            ),
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="prod",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="created_at", type=DataType.TIMESTAMP),
                    Column(name="customer_id", type=DataType.INT),
                    # Derived COMPLEX time column — bare ``created_at`` inside
                    # a function call, so it can't be cheaply qualified by
                    # the bare-identifier shortcut.
                    Column(
                        name="created_day",
                        sql="date(created_at)",
                        type=DataType.TIMESTAMP,
                    ),
                ],
                joins=[ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                )],
            ),
        ],
    )

    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],  # pulls the join into the ranked subquery
        measures=[{"formula": "amount:last(created_day)"}],
    ))
    assert resp.data, resp.sql
    by_region = {row["orders.customers.region"]: row for row in resp.data}
    last_key = next(k for k in by_region["NA"].keys() if "last" in k.lower())
    # last(amount) ranked by date(orders.created_at): newest NA order is
    # id=2 @ 2024-01-05 → 20.0. Picking up customers.created_at instead
    # would be an ambiguity error, not a wrong number.
    assert by_region["NA"][last_key] == pytest.approx(20.0), resp.sql
