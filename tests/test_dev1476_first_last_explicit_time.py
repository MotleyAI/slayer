"""Explicit time arg on first/last (bugs b, c, d-cross), end-to-end over SQLite."""
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
from slayer.engine.planned import ValueSlot
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.ranked_planner import (
    explicit_ranking_time_arg,
    resolve_ranking_time_key,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import SQLGenerator
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import ScopeFrame
from slayer.storage.yaml_storage import YAMLStorage




def _orders_model(with_amount: bool = False) -> SlayerModel:
    """Orders joined to customers; ``with_amount`` adds amount/created_at."""
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
    """Build a ``SlayerQueryEngine`` over a throwaway seeded SQLite file."""
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
    """SQLite with two orders per status, ``created_at`` strictly ordered."""
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




async def test_b_no_time_dimension_with_explicit_time_arg(
    engine_with_seeded_data,
) -> None:
    engine = engine_with_seeded_data
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:last(created_at)"}],
    ))
    assert resp.data, resp.sql
    by_status = {row["orders.status"]: row for row in resp.data}
    last_key = next(
        k for k in by_status["paid"].keys() if "last" in k.lower()
    )
    assert by_status["paid"][last_key] == pytest.approx(20.0)
    assert by_status["open"][last_key] == pytest.approx(14.0)




async def test_c_cross_model_bare_column_explicit_time_arg(
    engine_with_seeded_data,
) -> None:
    engine = engine_with_seeded_data
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.amount:last(customers.signup_at)"}],
    ))
    assert resp.data, resp.sql
    by_region = {row["orders.customers.region"]: row for row in resp.data}
    last_key = next(
        k for k in by_region["NA"].keys() if "last" in k.lower()
    )
    assert by_region["NA"][last_key] == pytest.approx(50.0)
    assert by_region["EU"][last_key] == pytest.approx(70.0)




async def test_local_first_last_over_derived_column_expands_inner_refs(
    engine_with_seeded_data,
) -> None:
    """Derived ``ColumnSqlKey`` aggregate source must qualify inner bare refs; the execute is the pin (an unqualified ref fails 'no such column')."""
    engine = engine_with_seeded_data
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
    assert set(by_region) == {"NA"}, resp.sql
    last_key = next(k for k in by_region["NA"].keys() if "last" in k.lower())
    assert by_region["NA"][last_key] == pytest.approx(50.0), resp.sql


async def test_cross_model_first_last_with_no_time_at_all_raises() -> None:
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
    """Target-model filter must be pushed INSIDE the ranked subquery, else the newest-but-excluded row wins ``_last_rn = 1`` and the outer MAX returns NULL."""
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
    """A ``Column.filter`` on the target column makes first/last use ``_last_rn_fN``/``_match_fN``; the bare ``_last_rn`` would be 'no such column'."""
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
    assert by_region["NA"][last_key] == pytest.approx(100.0), resp.sql


async def test_local_last_over_derived_complex_time_col_qualifies_under_join() -> None:
    """Both joined tables carry ``created_at``; a derived complex time arg must qualify the inner ref to ``orders`` or SQLite raises 'ambiguous column'."""
    engine = await _engine_from_sql(
        ddl=[
            "CREATE TABLE orders ("
            "id INTEGER PRIMARY KEY, amount REAL, "
            "created_at TEXT, customer_id INTEGER)",
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
    assert by_region["NA"][last_key] == pytest.approx(20.0), resp.sql




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
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(Decimal("5"),),
        )
        assert self._gen()._explicit_time_arg_of(key) is None


class TestTimeArgJoinDiscovery:
    def _gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="postgres")

    def _run(self, query: SlayerQuery, orders: SlayerModel) -> ScopeFrame:
        bundle = _u_bundle(orders)
        planned = plan_query(query=query, bundle=bundle)
        sub = (
            planned.regroup_attach_plans[0].producer_plan
            if planned.regroup_attach_plans
            else planned
        )
        slots = {
            s.id: s
            for s in (
                list(sub.row_slots)
                + list(sub.aggregate_slots)
                + list(sub.combined_expression_slots)
            )
        }
        order = list(sub.projection)
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
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.amount:last(customers.signup_at)"}],
        )
        bundle = _u_bundle(_u_orders())
        order, slots = _planned_slots(query, bundle)
        planned = plan_query(query=query, bundle=bundle)
        attach = next(
            a for a in planned.regroup_attach_plans
            if a.producer_root_model == "customers"
        )
        prod_agg = next(
            s for s in attach.producer_plan.aggregate_slots
            if isinstance(s.key, AggregateKey) and s.key.agg == "last"
        )
        assert prod_agg.key.grain == "target", prod_agg.key
        assert not any(isinstance(s.key, AggregateKey) for s in slots.values())
        gen = self._gen()
        scope = _host_scope(gen, source_model=_u_orders(), bundle=bundle)
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
        # Path-bearing ColumnSqlKey time arg (a hop past the target) must be skipped by discovery — no raise, no registration. Built by hand: only arises post-reroot.
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


class TestGateUsesSharedArgSelection:
    def _scalar_first_arg_key(self) -> AggregateKey:
        return AggregateKey(
            source=ColumnKey(leaf="amount"), agg="last",
            args=(Decimal("5"), ColumnKey(leaf="created_at")),
        )

    def test_scalar_first_positional_is_not_an_explicit_time_arg(self) -> None:
        assert explicit_ranking_time_arg(self._scalar_first_arg_key()) is None

    def test_scalar_first_arg_gate_requires_default_time(self) -> None:
        orders = _u_orders()  # no default_time_dimension
        bundle = _u_bundle(orders)
        key = self._scalar_first_arg_key()
        with pytest.raises(ValueError, match="ranking time"):
            resolve_ranking_time_key(key=key, root_model=orders, bundle=bundle)


async def _multi_hop_engine() -> SlayerQueryEngine:
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
    assert by_status["paid"][last_key] == pytest.approx(20.0), resp.sql
    assert by_status["open"][last_key] == pytest.approx(7.0), resp.sql


async def test_e2e_multi_hop_local_crossing_time_arg() -> None:
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
    assert by_status["paid"][last_key] == pytest.approx(20.0), resp.sql


async def test_e2e_local_derived_crossing_time_arg() -> None:
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
    """A crossing time arg isolates the aggregate into its own ``_cm_`` CTE; the ``customers`` join appears once per scope (host base + CTE), never twice within one."""
    engine = await _multi_hop_engine()
    resp = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "amount:last(customers.signup_at)"}],
    ))
    assert resp.data, resp.sql
    assert "_cm_" in resp.sql, (
        f"crossing time arg must isolate:\n{resp.sql}"
    )
    assert "ROW_NUMBER" in resp.sql.upper(), (
        f"the isolated CTE must still carry the ranking:\n{resp.sql}"
    )
    joins = re.findall(r"JOIN\s+customers\b", resp.sql, re.I)
    assert len(joins) == 2, (
        f"expected one customers join per scope (host base + isolation "
        f"CTE), got {len(joins)}:\n{resp.sql}"
    )
