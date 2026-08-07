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
import re
import sqlite3
import tempfile
from decimal import Decimal
from typing import AsyncIterator
from unittest import mock

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    Phase,
)
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.cross_model_planner import _local_agg_formula
from slayer.engine.planned import PlannedQuery, ValueSlot
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import SQLGenerator
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import ScopeFrame
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


# ===========================================================================
# DEV-1710 Stage 6 — explicit first/last time args resolve through the
# ScopeFrame resolver (Law 1). The join a first/last time arg crosses must be
# discovered as a SIDE EFFECT of resolving it through the host scope — the same
# mechanism dimensions / filters / sources / kwargs already use — replacing the
# bespoke legacy collector branch (``_collect_joined_paths_for_base``'s
# AGGREGATE arm) and the ad-hoc rendering in ``_resolve_explicit_time_col``.
# ===========================================================================


# --------------------------------------------------------------------------- #
# Bundle-only fixtures (no storage): the resolver / sub-pass work off a
# ``ResolvedSourceBundle`` + a planned query, not a live DB.
# --------------------------------------------------------------------------- #
def _u_regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="prod",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="opened_at", type=DataType.TIMESTAMP),
        ],
    )


def _u_customers() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="prod",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="region_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
            Column(name="signup_at_alias", sql="signup_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _u_orders(extra: list[Column] | None = None) -> SlayerModel:
    cols = [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="status", type=DataType.TEXT),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="created_at", type=DataType.TIMESTAMP),
        Column(name="customer_id", type=DataType.INT),
    ]
    cols += extra or []
    return SlayerModel(
        name="orders", sql_table="orders", data_source="prod",
        columns=cols,
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def _u_bundle(orders: SlayerModel) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=orders,
        referenced_models=[orders, _u_customers(), _u_regions()],
    )


def _planned_slots(query: SlayerQuery, bundle: ResolvedSourceBundle):
    """Return ``(base_render_order, slots_by_id)`` the way the generator
    assembles them for a single-stage query — enough to drive the aggregate
    join-discovery pass. For a public first/last measure the slot is already in
    ``planned.projection``, so the projection list is a faithful render order.
    """
    planned = plan_query(query=query, bundle=bundle)
    slots_by_id = {
        s.id: s
        for s in (
            list(planned.row_slots)
            + list(planned.aggregate_slots)
            + list(planned.combined_expression_slots)
        )
    }
    return list(planned.projection), slots_by_id


def _host_scope(
    gen: SQLGenerator, *, source_model: SlayerModel, bundle: ResolvedSourceBundle,
    relation: str | None = None,
) -> ScopeFrame:
    alloc = AliasAllocator()
    rel = relation or source_model.name
    return ScopeFrame(
        scope_id=alloc.next_scope_id(rel),
        root_model=source_model, root_relation=rel,
        bundle=bundle, dialect=gen._dialect, allocator=alloc,
    )


# --------------------------------------------------------------------------- #
# Group 1 — ``_explicit_time_arg_of``: the single arg-selection contract shared
# by the raise-gate, the discovery pass, and the render seam (Codex F1: without
# one helper the gate scanned all args while the renderer stopped at the first,
# so a join could register for an arg the renderer ignored).
# --------------------------------------------------------------------------- #
class TestExplicitTimeArgOf:
    def _gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="postgres")

    def test_returns_first_columnkey(self) -> None:
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(ColumnKey(leaf="created_at"),),
        )
        assert self._gen()._explicit_time_arg_of(key) == ColumnKey(leaf="created_at")

    def test_returns_first_columnsqlkey(self) -> None:
        arg = ColumnSqlKey(path=(), model="orders", column_name="created_at_alias")
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="first", args=(arg,))
        assert self._gen()._explicit_time_arg_of(key) == arg

    def test_returns_path_bearing_arg_as_is(self) -> None:
        # The helper only SELECTS the arg; the caller decides what to do with a
        # residual path (discovery skips it, the render seam raises DEV-1526).
        arg = ColumnKey(path=("customers",), leaf="signup_at")
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="last", args=(arg,))
        assert self._gen()._explicit_time_arg_of(key) == arg

    def test_none_for_non_first_last(self) -> None:
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="sum",
            args=(ColumnKey(leaf="created_at"),),
        )
        assert self._gen()._explicit_time_arg_of(key) is None

    def test_none_for_empty_args(self) -> None:
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="last", args=())
        assert self._gen()._explicit_time_arg_of(key) is None

    def test_none_when_first_arg_is_scalar(self) -> None:
        # F1 degenerate: the first positional arg is a scalar literal, not a
        # time column. Gate + render + discovery must all treat this as "no
        # explicit time arg" (so the gate falls back to the default and the
        # renderer returns None) — proven by them sharing THIS helper.
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(Decimal("5"),),
        )
        assert self._gen()._explicit_time_arg_of(key) is None


# --------------------------------------------------------------------------- #
# Group 2 — the discovery pass in ``_resolve_agg_inputs_via_scope`` registers a
# first/last time arg's crossed joins into the host scope (Law 1). Pre-impl the
# pass has no arm for positional args, so these are red until Stage 6 lands.
# --------------------------------------------------------------------------- #
class TestTimeArgJoinDiscovery:
    def _gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="postgres")

    def _run(self, query: SlayerQuery, orders: SlayerModel) -> ScopeFrame:
        bundle = _u_bundle(orders)
        order, slots = _planned_slots(query, bundle)
        gen = self._gen()
        scope = _host_scope(gen, source_model=orders, bundle=bundle)
        gen._resolve_agg_inputs_via_scope(
            base_render_order=order, slots_by_id=slots, scope=scope,
        )
        return scope

    def test_single_hop_time_arg_registers_join(self) -> None:
        scope = self._run(
            SlayerQuery(
                source_model="orders", dimensions=["status"],
                measures=[{"formula": "amount:last(customers.signup_at)"}],
            ),
            _u_orders(),
        )
        assert ("customers",) in scope.join_paths.as_list()

    def test_multi_hop_time_arg_registers_every_prefix(self) -> None:
        # Codex F3: a two-hop time arg must register BOTH prefixes so
        # ``_build_from_and_joins`` emits ``customers`` and ``customers__regions``.
        scope = self._run(
            SlayerQuery(
                source_model="orders", dimensions=["status"],
                measures=[{"formula": "amount:last(customers.regions.opened_at)"}],
            ),
            _u_orders(),
        )
        assert ("customers",) in scope.join_paths.as_list()
        assert ("customers", "regions") in scope.join_paths.as_list()

    def test_local_derived_crossing_time_arg_registers_join(self) -> None:
        # A LOCAL derived time column whose ``Column.sql`` reaches a joined table
        # registers that join via the resolver's expansion + scan.
        orders = _u_orders(extra=[
            Column(name="cust_signup", sql="customers.signup_at", type=DataType.TIMESTAMP),
        ])
        scope = self._run(
            SlayerQuery(
                source_model="orders", dimensions=["status"],
                measures=[{"formula": "amount:last(cust_signup)"}],
            ),
            orders,
        )
        assert ("customers",) in scope.join_paths.as_list()

    def test_composite_time_arg_registers_join(self) -> None:
        # The pass recurses composite (ArithmeticKey) slots for first/last leaves.
        scope = self._run(
            SlayerQuery(
                source_model="orders", dimensions=["status"],
                measures=[{
                    "formula": "amount:last(customers.signup_at) + 1", "name": "plus1",
                }],
            ),
            _u_orders(),
        )
        assert ("customers",) in scope.join_paths.as_list()

    def test_cross_model_time_arg_not_registered_on_host(self) -> None:
        # A CROSS-MODEL first/last (source path non-empty) is owned by its
        # ``_cm_*`` CTE — the aggregate-input pass skips cross-model aggs
        # entirely, so it must NOT pull the arg's join into the host ``_base``.
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.amount:last(customers.signup_at)"}],
        )
        bundle = _u_bundle(_u_orders())
        order, slots = _planned_slots(query, bundle)
        # Confirm the slot IS a cross-model aggregate (source.path non-empty) —
        # otherwise ``join_paths == []`` could pass because the agg was never a
        # cross-model one, not because the pass correctly skipped it (Codex).
        agg = next(
            s for s in slots.values()
            if isinstance(s.key, AggregateKey) and s.key.agg == "last"
        )
        assert getattr(agg.key.source, "path", ()) == ("customers",), agg.key
        gen = self._gen()
        scope = _host_scope(gen, source_model=_u_orders(), bundle=bundle)
        # Spy on the resolver: a cross-model agg must never reach ``scope.resolve``
        # in this pass (so no join can register), proving the skip is structural.
        seen: list = []
        real = ScopeFrame.resolve

        def _spy(self, ref, *, consumer=None):
            seen.append(ref)
            return real(self, ref, consumer=consumer)

        with mock.patch.object(ScopeFrame, "resolve", _spy):
            gen._resolve_agg_inputs_via_scope(
                base_render_order=order, slots_by_id=slots, scope=scope,
            )
        assert scope.join_paths.as_list() == []
        assert seen == [], f"cross-model arg was resolved: {seen!r}"

    def test_residual_path_bearing_columnsqlkey_skipped_without_raise(self) -> None:
        # PINS SUPERSEDED MACHINERY. ``_resolve_agg_inputs_via_scope``'s
        # first/last sub-pass is production-unreferenced since DEV-1748 — a
        # ranked aggregate never reaches ``base_render_order``, so this walker
        # never sees one. Kept until PR 6 deletes the helper, per P-J, so the
        # deletion is reviewed as a deletion rather than smuggled in here.
        #
        # A path-bearing ColumnSqlKey time arg (a hop PAST the target, the shape
        # the render seam raises DEV-1526 on) must be SKIPPED by discovery — no
        # raise, no bogus registration. Built by hand: this residual shape only
        # arises post-reroot at generation time, never from ``plan_query``.
        gen = self._gen()
        orders = _u_orders()
        bundle = _u_bundle(orders)
        residual = ColumnSqlKey(path=("regions",), model="regions", column_name="opened_day")
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"), agg="last", args=(residual,),
        )
        slot = ValueSlot(
            id="s0", key=key, declared_name="x", public_name="x",
            phase=Phase.AGGREGATE, type=DataType.DOUBLE,
        )
        scope = _host_scope(gen, source_model=orders, bundle=bundle)
        # Spy: the residual arg must be recognised (source is local, so the agg
        # IS walked) yet the path-bearing ColumnSqlKey short-circuits BEFORE
        # ``scope.resolve`` — so ``resolve`` is never called with it.
        seen: list = []
        real = ScopeFrame.resolve

        def _spy(self, ref, *, consumer=None):
            seen.append(ref)
            return real(self, ref, consumer=consumer)

        with mock.patch.object(ScopeFrame, "resolve", _spy):
            gen._resolve_agg_inputs_via_scope(
                base_render_order=["s0"], slots_by_id={"s0": slot}, scope=scope,
            )
        assert scope.join_paths.as_list() == []
        assert residual not in seen, f"residual arg was resolved: {seen!r}"


# --------------------------------------------------------------------------- #
# Group 2b — the raise-gate in ``_build_first_last_base_select`` uses the SAME
# ``_explicit_time_arg_of`` contract as the render seam (Codex F1). A first/last
# whose FIRST positional arg is a scalar (a later arg being a column) has NO
# explicit time arg under the first-arg-only contract; with no default time
# column the gate must raise the clean "ranking time" error. A retained
# ``any(isinstance(a, (ColumnKey, ColumnSqlKey)) ...)`` gate would instead skip
# the raise and later blow up building the ``ROW_NUMBER`` map — proving the two
# sites had drifted.
# --------------------------------------------------------------------------- #
class TestGateUsesSharedArgSelection:
    def test_scalar_first_arg_gate_requires_default_time(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        orders = _u_orders()  # no default_time_dimension
        bundle = _u_bundle(orders)
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(Decimal("5"), ColumnKey(leaf="created_at")),
        )
        slot = ValueSlot(
            id="s0", key=key, declared_name="x", public_name="x",
            phase=Phase.AGGREGATE, type=None,
        )
        pq = PlannedQuery(
            source_relation="orders", aggregate_slots=[slot], projection=["s0"],
        )
        from_clause, base_joins = gen._build_from_and_joins(
            source_model=orders, source_relation="orders",
            joined_paths=[], bundle=bundle,
        )
        with pytest.raises(ValueError, match="ranking time"):
            gen._build_first_last_base_select(
                planned_query=pq, bundle=bundle, source_model=orders,
                source_relation="orders", base_render_order=["s0"],
                slots_by_id={"s0": slot}, from_clause=from_clause,
                base_joins=base_joins,
            )


# --------------------------------------------------------------------------- #
# Group 3 — ``_resolve_explicit_time_col`` renders through the resolver when a
# bundle is available. Bare/joined outputs equal the pre-resolver f-string (so
# they pin equivalence); the reserved-word case is the new correctness gain.
# --------------------------------------------------------------------------- #
class TestResolveExplicitTimeColViaResolver:
    def _gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="postgres")

    def test_bare_local_time_arg(self) -> None:
        orders = _u_orders()
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(ColumnKey(leaf="created_at"),),
        )
        tc = self._gen()._resolve_explicit_time_col(
            key=key, source_model=orders, source_relation="orders",
            bundle=_u_bundle(orders),
        )
        assert tc == "orders.created_at"

    def test_joined_time_arg_uses_path_alias(self) -> None:
        orders = _u_orders()
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(ColumnKey(path=("customers",), leaf="signup_at"),),
        )
        tc = self._gen()._resolve_explicit_time_col(
            key=key, source_model=orders, source_relation="orders",
            bundle=_u_bundle(orders),
        )
        assert tc == "customers.signup_at"

    def test_reserved_word_relation_is_quoted(self) -> None:
        # DEV-1686 gain: a reserved-word relation must be quoted in the rendered
        # ORDER BY column. The pre-resolver f-string emitted it bare (invalid
        # SQL); the resolver quotes it — this is red until Stage 6 reroutes
        # rendering through ``ScopeFrame``.
        order = SlayerModel(
            name="order", sql_table="orders_tbl", data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
            ],
        )
        bundle = ResolvedSourceBundle(source_model=order, referenced_models=[order])
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(ColumnKey(leaf="created_at"),),
        )
        tc = self._gen()._resolve_explicit_time_col(
            key=key, source_model=order, source_relation="order", bundle=bundle,
        )
        assert '"order"' in tc, tc
        assert "created_at" in tc

    def test_derived_columnsqlkey_resolves_via_resolver(self) -> None:
        # A local derived (ColumnSqlKey) time arg resolves through the resolver
        # (bundle set): ``signup_at_alias`` (sql=``signup_at``) expands to the
        # underlying column, qualified under the source relation.
        customers = _u_customers()
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(ColumnSqlKey(path=(), model="customers", column_name="signup_at_alias"),),
        )
        tc = self._gen()._resolve_explicit_time_col(
            key=key, source_model=customers, source_relation="customers",
            bundle=_u_bundle(_u_orders()),
        )
        assert tc == "customers.signup_at", tc

    def test_missing_columnsqlkey_raises_value_error(self) -> None:
        # A ColumnSqlKey arg naming a column not on the source model raises the
        # clear ValueError BEFORE any resolve (bundle set — proves the not-found
        # guard precedes the ScopeFrame path, which would otherwise fall back to
        # the bare name and silently mis-rank).
        orders = _u_orders()
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(ColumnSqlKey(path=(), model="orders", column_name="not_a_real_col"),),
        )
        # Hoist the generator + bundle out so the ONLY call that can raise inside
        # ``pytest.raises`` is the one under test (Sonar S5778).
        gen = self._gen()
        bundle = _u_bundle(orders)
        with pytest.raises(ValueError, match="Derived time column 'not_a_real_col'"):
            gen._resolve_explicit_time_col(
                key=key, source_model=orders, source_relation="orders",
                bundle=bundle,
            )

    def test_path_bearing_columnsqlkey_raises_dev1526_with_bundle(self) -> None:
        # PINS SUPERSEDED MACHINERY. ``_resolve_explicit_time_col`` is
        # production-unreferenced since DEV-1748 — the ranked CTE resolves its
        # ranking key through its own scope, which is what removed the
        # limitation this guard describes (see the un-xfailed
        # ``test_a_joined_derived_time_arg_ranks_by_the_joined_expression`` in
        # tests/test_dev1748_first_last_matrix.py). Kept until PR 6 deletes the
        # helper, per P-J.
        #
        # The residual-hop guard fires BEFORE resolution even when a bundle is
        # available — the existing guard pin (test_reroot_aggregate_key.py) runs
        # bundle=None; this fixes the ordering with a bundle set.
        orders = _u_orders()
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(ColumnSqlKey(path=("regions",), model="regions", column_name="opened_day"),),
        )
        # Hoist the generator + bundle out so the ONLY call that can raise inside
        # ``pytest.raises`` is the one under test (Sonar S5778).
        gen = self._gen()
        bundle = _u_bundle(orders)
        with pytest.raises(NotImplementedError, match="DEV-1526"):
            gen._resolve_explicit_time_col(
                key=key, source_model=orders, source_relation="orders",
                bundle=bundle,
            )


# --------------------------------------------------------------------------- #
# Group 4 — end-to-end regression pins over a live SQLite DB. The join a
# first/last time arg crosses must land in the ranked subquery's FROM (so the
# ORDER BY ref is in scope), whether the arg is a plain joined column, a
# multi-hop path, a local derived column that reaches a joined table, or a
# join already introduced by a dimension (dedup).
# --------------------------------------------------------------------------- #
async def _multi_hop_engine() -> SlayerQueryEngine:
    """orders → customers → regions, seeded so ``last`` by a regions-level time
    column is deterministic per status.
    """
    return await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, status TEXT, amount REAL, "
            "created_at TEXT, customer_id INTEGER)",
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, region TEXT, region_id INTEGER, "
            "amount REAL, signup_at TEXT)",
            "CREATE TABLE regions ("
            "id INTEGER PRIMARY KEY, name TEXT, opened_at TEXT)",
        ],
        inserts=[
            ("INSERT INTO orders VALUES (?,?,?,?,?)", [
                (1, "paid", 10.0, "2024-01-01", 1),
                (2, "paid", 20.0, "2024-01-05", 2),
                (3, "open", 7.0, "2024-01-02", 1),
            ]),
            ("INSERT INTO customers VALUES (?,?,?,?,?)", [
                (1, "NA", 10, 100.0, "2023-06-01"),
                (2, "EU", 20, 50.0, "2023-07-01"),
            ]),
            ("INSERT INTO regions VALUES (?,?,?)", [
                (10, "NA", "2020-01-01"),
                (20, "EU", "2021-01-01"),  # opened later → wins `last`
            ]),
        ],
        models=[
            _u_regions(),
            _u_customers(),
            _u_orders(),
        ],
    )


async def test_e2e_noncomposite_local_crossing_time_arg() -> None:
    """A NON-composite local ``amount:last(customers.signup_at)`` with no time
    dimension must pull the ``customers`` join into the ranked subquery's FROM
    (the ORDER BY references ``customers.signup_at``). The existing coverage was
    the composite ``+ 1`` shape only.
    """
    engine = await _multi_hop_engine()
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:last(customers.signup_at)"}],
    ))
    assert resp.data, resp.sql
    assert re.search(r"JOIN\s+customers\b", resp.sql, re.I), resp.sql
    by_status = {row["orders.status"]: row for row in resp.data}
    last_key = next(k for k in by_status["paid"].keys() if "last" in k.lower())
    # paid orders map to customers 1 (signup 2023-06-01) and 2 (2023-07-01);
    # last by signup_at picks customer 2's order → amount 20.
    assert by_status["paid"][last_key] == pytest.approx(20.0), resp.sql
    assert by_status["open"][last_key] == pytest.approx(7.0), resp.sql


async def test_e2e_multi_hop_local_crossing_time_arg() -> None:
    """Codex F3 e2e: a two-hop local time arg
    (``amount:last(customers.regions.opened_at)``) pulls BOTH joins into the
    ranked subquery so the ORDER BY reference is in scope.
    """
    engine = await _multi_hop_engine()
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:last(customers.regions.opened_at)"}],
    ))
    assert resp.data, resp.sql
    assert re.search(r"JOIN\s+customers\b", resp.sql, re.I), resp.sql
    assert re.search(r"customers__regions", resp.sql, re.I), resp.sql
    by_status = {row["orders.status"]: row for row in resp.data}
    last_key = next(k for k in by_status["paid"].keys() if "last" in k.lower())
    # paid orders: cust 1 → region NA (opened 2020), cust 2 → region EU
    # (opened 2021). last by regions.opened_at picks EU → order 2 → amount 20.
    assert by_status["paid"][last_key] == pytest.approx(20.0), resp.sql


async def test_e2e_local_derived_crossing_time_arg() -> None:
    """Codex F6 (derived): a LOCAL derived time column whose ``Column.sql``
    reaches a joined table (``cust_signup`` = ``customers.signup_at``) must pull
    that join into the ranked subquery and rank by the expanded expression.

    (A derived column that references ANOTHER derived column — nested inlining —
    is a separate, pre-existing limitation of ``expand_derived_refs_sync`` that
    affects every consumer, not just time args, and is out of Stage 6 scope.)
    """
    orders = _u_orders(extra=[
        Column(name="cust_signup", sql="customers.signup_at", type=DataType.TIMESTAMP),
    ])
    engine = await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, status TEXT, amount REAL, "
            "created_at TEXT, customer_id INTEGER)",
            "CREATE TABLE customers ("
            "id INTEGER PRIMARY KEY, region TEXT, region_id INTEGER, "
            "amount REAL, signup_at TEXT)",
            "CREATE TABLE regions ("
            "id INTEGER PRIMARY KEY, name TEXT, opened_at TEXT)",
        ],
        inserts=[
            ("INSERT INTO orders VALUES (?,?,?,?,?)", [
                (1, "paid", 10.0, "2024-01-01", 1),
                (2, "paid", 20.0, "2024-01-05", 2),
            ]),
            ("INSERT INTO customers VALUES (?,?,?,?,?)", [
                (1, "NA", 10, 100.0, "2023-06-01"),
                (2, "EU", 20, 50.0, "2023-07-01"),  # later signup → wins `last`
            ]),
            ("INSERT INTO regions VALUES (?,?,?)", [(10, "NA", "2020-01-01")]),
        ],
        models=[_u_regions(), _u_customers(), orders],
    )
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:last(cust_signup)"}],
    ))
    assert resp.data, resp.sql
    assert re.search(r"JOIN\s+customers\b", resp.sql, re.I), resp.sql
    by_status = {row["orders.status"]: row for row in resp.data}
    last_key = next(k for k in by_status["paid"].keys() if "last" in k.lower())
    assert by_status["paid"][last_key] == pytest.approx(20.0), resp.sql


async def test_e2e_time_arg_join_dedup_with_dimension() -> None:
    """Codex F6 dedup: the crossing time arg isolates the aggregate into its own
    CTE, so the ``customers`` join appears once PER SCOPE — once in the host
    base (the dimension's row-level pull) and once inside the CTE (the time
    arg's pull, deduped against the CTE's own dimension pull) — never twice
    within one scope.

    The CTE is a ranked ``_rk_`` one rather than the generic ``_cm_`` one it was
    under DEV-1709: a first/last now isolates because it RANKS, which subsumes
    the crossing-input trigger that used to be its only reason.
    """
    engine = await _multi_hop_engine()
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "amount:last(customers.signup_at)"}],
    ))
    assert resp.data, resp.sql
    assert "_rk_" in resp.sql, (
        f"crossing time arg must isolate:\n{resp.sql}"
    )
    joins = re.findall(r"JOIN\s+customers\b", resp.sql, re.I)
    assert len(joins) == 2, (
        f"expected one customers join per scope (host base + isolation "
        f"CTE), got {len(joins)}:\n{resp.sql}"
    )
