"""Revisits are frame-local: definitions compose across frames; one typed frame never revisits."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
import sqlalchemy as sa
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, JoinCardinality, TimeGranularity
from slayer.core.errors import CircularJoinPathError
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.join_walker import follow, resolved_terminal, walk, walk_cancelling
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import _build_stage_graph, _stage_referenced_columns_for_base
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine, seeded_exec_engine

DS = "ds"
M2O = JoinCardinality.MANY_TO_ONE
CURRENCY = NumberFormat(type=NumberFormatType.CURRENCY, symbol="$")

# --------------------------------------------------------------------------- #
# Latest-child-pointer schema: orders.customer (m:1), customers.last_order / first_order (m:1).
# --------------------------------------------------------------------------- #
_ORDERS = [
    (11, 1, 10.0, False, "2024-01-10"),
    (12, 1, 20.0, True, "2024-02-10"),
    (21, 2, 5.0, False, "2024-03-05"),
]
_CUSTOMERS = [(1, "ann", 12, 11), (2, "bob", 21, 21)]

# Directed 3-cycle x → y → z → x and the A→B→A→B pair.
_X = [(1, 10, "x1"), (2, 20, "x2")]
_Y = [(10, 200, "y10"), (20, 100, "y20")]
_Z = [(100, 1, "z100"), (200, 2, "z200")]
_A = [(1, 10), (2, 20)]
_B = [(10, 2, 100.0), (20, 1, 200.0)]


def _seed(db_path: str, *, duck: bool) -> None:
    ddl = [
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE, big BOOLEAN, ordered_at TIMESTAMP)",
        "CREATE TABLE customers (id INTEGER, name VARCHAR, last_order_id INTEGER, first_order_id INTEGER)",
        "CREATE TABLE x (id INTEGER, y_id INTEGER, v VARCHAR)",
        "CREATE TABLE y (id INTEGER, z_id INTEGER, v VARCHAR)",
        "CREATE TABLE z (id INTEGER, x_id INTEGER, v VARCHAR)",
        "CREATE TABLE a (id INTEGER, b_id INTEGER)",
        "CREATE TABLE b (id INTEGER, a_id INTEGER, w DOUBLE)",
    ]
    inserts = [
        ("orders", _ORDERS), ("customers", _CUSTOMERS), ("x", _X), ("y", _Y), ("z", _Z),
        ("a", _A), ("b", _B),
    ]
    if duck:
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        for stmt in ddl:
            con.execute(stmt)
        for table, rows in inserts:
            con.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * len(rows[0]))})", rows)
        con.close()
        return
    with transaction(db_path) as con:
        cur = con.cursor()
        for stmt in ddl:
            cur.execute(stmt.replace("DOUBLE", "REAL").replace("BOOLEAN", "INTEGER").replace("VARCHAR", "TEXT"))
        for table, rows in inserts:
            cur.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * len(rows[0]))})", rows)


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source=DS, sql_table="orders", default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE, format=CURRENCY),
            Column(name="big", type=DataType.BOOLEAN),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="customers", name="customer",
                         join_pairs=[["customer_id", "id"]], cardinality=M2O)],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source=DS, sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="last_order_id", type=DataType.INT),
            Column(name="first_order_id", type=DataType.INT),
            Column(name="last_amount", type=DataType.DOUBLE, sql="last_order.amount",
                   label="Latest amount", format=CURRENCY),
            Column(name="first_amount", type=DataType.DOUBLE, sql="first_order.amount"),
            Column(name="last_ordered_at", type=DataType.TIMESTAMP, sql="last_order.ordered_at"),
        ],
        joins=[
            ModelJoin(target_model="orders", name="last_order",
                      join_pairs=[["last_order_id", "id"]], cardinality=M2O),
            ModelJoin(target_model="orders", name="first_order",
                      join_pairs=[["first_order_id", "id"]], cardinality=M2O),
        ],
        measures=[
            ModelMeasure(name="order_total", formula="sum(customer.amount)"),
            ModelMeasure(name="latest_total", formula="sum(last_order.amount)"),
            ModelMeasure(name="latest_big_share", formula="avg(last_order.big)"),
            ModelMeasure(name="last_ranked", formula="last(customer.amount)"),
            ModelMeasure(name="bad", formula="count(last_order.customer.id)"),
        ],
    )


def _cycle_models() -> list[SlayerModel]:
    def node(name: str, nxt: str, extra: Sequence[Column] = ()) -> SlayerModel:
        return SlayerModel(
            name=name, data_source=DS, sql_table=name,
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
                     Column(name=f"{nxt}_id", type=DataType.INT),
                     Column(name="v", type=DataType.TEXT), *extra],
            joins=[ModelJoin(target_model=nxt, join_pairs=[[f"{nxt}_id", "id"]], cardinality=M2O)],
        )

    return [
        node("x", "y", [Column(name="zv", type=DataType.TEXT, sql="y.z.v")]),
        node("y", "z"),
        node("z", "x", [Column(name="xzv", type=DataType.TEXT, sql="x.zv")]),
    ]


def _ab_models() -> list[SlayerModel]:
    a = SlayerModel(
        name="a", data_source=DS, sql_table="a",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="b_id", type=DataType.INT),
                 Column(name="bw", type=DataType.DOUBLE, sql="to_b.w")],
        joins=[ModelJoin(target_model="b", name="to_b", join_pairs=[["b_id", "id"]], cardinality=M2O)],
    )
    b = SlayerModel(
        name="b", data_source=DS, sql_table="b",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="a_id", type=DataType.INT),
                 Column(name="w", type=DataType.DOUBLE),
                 Column(name="abw", type=DataType.DOUBLE, sql="to_a.bw")],
        joins=[ModelJoin(target_model="a", name="to_a", join_pairs=[["a_id", "id"]], cardinality=M2O)],
    )
    return [a, b]


def _all_models() -> list[SlayerModel]:
    return [_orders(), _customers(), *_cycle_models(), *_ab_models()]


class _Ctx:
    def __init__(self, engine: SlayerQueryEngine, db_path: str, dialect: str) -> None:
        self.engine = engine
        self.db_path = db_path
        self.dialect = dialect

    def oracle(self, sql: str) -> list[tuple[Any, ...]]:
        """Execute a hand-written oracle query against the seeded rows."""
        prefix = "duckdb:///" if self.dialect == "duckdb" else "sqlite:///"
        with disposable_engine(prefix + self.db_path) as eng, eng.connect() as con:
            return [tuple(r) for r in con.execute(sa.text(sql)).fetchall()]


@pytest.fixture(params=["sqlite", "duckdb"])
async def ctx(request) -> AsyncIterator[_Ctx]:
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
        pytest.importorskip("duckdb_engine")
    async with seeded_exec_engine(
        dialect=dialect, seed=lambda p: _seed(p, duck=dialect == "duckdb"),
        models=_all_models(), datasource=DS, validate=False,
    ) as (engine, db_path):
        yield _Ctx(engine, db_path, dialect)


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _num(v: Any) -> float | None:
    return None if v is None else float(v)


def _rows(resp, *keys: str) -> list[tuple[Any, ...]]:
    return [tuple(r[k] for k in keys) for r in resp.data]


def _broadcasts(resp) -> list:
    return [w for w in resp.warnings or [] if getattr(w, "kind", None) == "broadcast"]


def _table_count(sql: str, *, table: str, dialect: str) -> int:
    return sum(1 for t in sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Table) if t.name == table)


_ORDERS_THROUGH_LAST = (
    "FROM orders o LEFT JOIN customers c ON o.customer_id = c.id "
    "LEFT JOIN orders lo ON c.last_order_id = lo.id"
)


# --------------------------------------------------------------------------- #
# column-definitions › Revisits are frame-local
# --------------------------------------------------------------------------- #
class TestDerivedColumnsCompose:
    async def test_derived_column_composes_across_a_cycle(self, ctx: _Ctx) -> None:
        oracle = ctx.oracle(f"SELECT o.id, lo.amount {_ORDERS_THROUGH_LAST} ORDER BY o.id")
        assert [(i, _num(v)) for i, v in oracle] == [(11, 20.0), (12, 20.0), (21, 5.0)]
        resp = await ctx.engine.execute(_q(
            dimensions=["id", "customer.last_amount"], order=[{"column": "id", "direction": "asc"}]))
        got = [(i, _num(v)) for i, v in _rows(resp, "orders.id", "orders.customer.last_amount")]
        assert got == [(i, _num(v)) for i, v in oracle]
        assert resp.sql is not None
        assert _table_count(resp.sql, table="orders", dialect=ctx.dialect) == 2
        meta = resp.attributes.dimensions["orders.customer.last_amount"]
        assert meta.format == CURRENCY
        assert meta.label == "Latest amount"

    async def test_same_chain_typed_in_one_frame_is_refused(self, ctx: _Ctx) -> None:
        query = _q(dimensions=["id", "customer.last_order.amount"])
        with pytest.raises(CircularJoinPathError) as ei:
            await ctx.engine.execute(query)
        assert ei.value.hop == "last_order"
        assert ei.value.revisited == "orders"

    async def test_aggregating_a_composed_derived_column_keeps_its_home(self, ctx: _Ctx) -> None:
        oracle = ctx.oracle(
            f"SELECT c.name, MAX(lo.amount) {_ORDERS_THROUGH_LAST} GROUP BY c.name ORDER BY c.name")
        assert [(n, _num(v)) for n, v in oracle] == [("ann", 20.0), ("bob", 5.0)]
        resp = await ctx.engine.execute(_q(
            dimensions=["customer.name"], measures=[{"formula": "max(customer.last_amount)", "name": "m"}],
            order=[{"column": "customer.name", "direction": "asc"}]))
        assert [(n, _num(v)) for n, v in _rows(resp, "orders.customer.name", "orders.m")] == [
            (n, _num(v)) for n, v in oracle]
        assert _broadcasts(resp) == []

    async def test_occurrences_through_different_edges_stay_distinct(self, ctx: _Ctx) -> None:
        oracle = ctx.oracle(
            "SELECT o.id, o.amount, fo.amount, lo.amount FROM orders o "
            "LEFT JOIN customers c ON o.customer_id = c.id "
            "LEFT JOIN orders fo ON c.first_order_id = fo.id "
            "LEFT JOIN orders lo ON c.last_order_id = lo.id ORDER BY o.id")
        norm = [(i, _num(a), _num(f), _num(lv)) for i, a, f, lv in oracle]
        assert norm == [(11, 10.0, 10.0, 20.0), (12, 20.0, 10.0, 20.0), (21, 5.0, 5.0, 5.0)]
        resp = await ctx.engine.execute(_q(
            dimensions=["id", "amount", "customer.first_amount", "customer.last_amount"],
            order=[{"column": "id", "direction": "asc"}]))
        got = [(i, _num(a), _num(f), _num(lv)) for i, a, f, lv in _rows(
            resp, "orders.id", "orders.amount", "orders.customer.first_amount", "orders.customer.last_amount")]
        assert got == norm
        assert resp.sql is not None
        assert _table_count(resp.sql, table="orders", dialect=ctx.dialect) == 3

    async def test_directed_three_cycle_composes(self, ctx: _Ctx) -> None:
        oracle = ctx.oracle(
            "SELECT z.id, z2.v FROM z LEFT JOIN x ON z.x_id = x.id LEFT JOIN y ON x.y_id = y.id "
            "LEFT JOIN z z2 ON y.z_id = z2.id ORDER BY z.id")
        assert oracle == [(100, "z200"), (200, "z100")]
        resp = await ctx.engine.execute(SlayerQuery.model_validate(
            {"source_model": "z", "dimensions": ["id", "xzv"], "order": [{"column": "id", "direction": "asc"}]}))
        assert _rows(resp, "z.id", "z.xzv") == oracle

    async def test_directed_three_cycle_typed_in_one_frame_is_refused(self, ctx: _Ctx) -> None:
        query = SlayerQuery.model_validate({"source_model": "z", "dimensions": ["x.y.z.v"]})
        with pytest.raises(CircularJoinPathError) as ei:
            await ctx.engine.execute(query)
        assert ei.value.revisited == "z"

    async def test_a_b_a_b_composition(self, ctx: _Ctx) -> None:
        oracle = ctx.oracle(
            "SELECT a.id, b2.w FROM a LEFT JOIN b ON a.b_id = b.id LEFT JOIN a a2 ON b.a_id = a2.id "
            "LEFT JOIN b b2 ON a2.b_id = b2.id ORDER BY a.id")
        assert [(i, _num(w)) for i, w in oracle] == [(1, 200.0), (2, 100.0)]
        resp = await ctx.engine.execute(SlayerQuery.model_validate(
            {"source_model": "a", "dimensions": ["id", "to_b.abw"], "order": [{"column": "id", "direction": "asc"}]}))
        assert [(i, _num(w)) for i, w in _rows(resp, "a.id", "a.to_b.abw")] == [
            (i, _num(w)) for i, w in oracle]

    async def test_composed_definitions_save_with_validation(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite"))
        engine = SlayerQueryEngine(storage=storage)
        models = [_orders(), _customers(), *_cycle_models(), *_ab_models()]
        for model in models:
            await engine.save_model(model.model_copy(update={
                "columns": [c for c in model.columns if c.sql is None], "measures": []}))
        for model in models:
            await engine.save_model(model)
        for name in ("customers", "z", "b"):
            assert await storage.get_model(name, data_source=DS) is not None


# --------------------------------------------------------------------------- #
# saved-measures › Saved measures compose across frames
# --------------------------------------------------------------------------- #
_ORDER_TOTAL_BY_NAME = (
    "SELECT c.name, (SELECT SUM(o2.amount) FROM orders o2 WHERE o2.customer_id = c.id) "
    "FROM customers c WHERE c.id IN (SELECT customer_id FROM orders) ORDER BY c.name"
)


class TestSavedMeasuresCompose:
    async def test_measure_crossing_back_to_the_host_is_attributed(self, ctx: _Ctx) -> None:
        oracle = [(n, _num(v)) for n, v in ctx.oracle(_ORDER_TOTAL_BY_NAME)]
        assert oracle == [("ann", 30.0), ("bob", 5.0)]
        resp = await ctx.engine.execute(_q(
            dimensions=["customer.name"], measures=["customer.order_total"],
            order=[{"column": "customer.name", "direction": "asc"}]))
        got = [(n, _num(v)) for n, v in _rows(resp, "orders.customer.name", "orders.customer.order_total")]
        assert got == oracle
        assert _broadcasts(resp) == []

    async def test_unattributable_host_dimension_broadcasts(self, ctx: _Ctx) -> None:
        (total,), = ctx.oracle(
            "SELECT SUM(o2.amount) FROM orders o2 WHERE o2.customer_id IN (SELECT customer_id FROM orders)")
        assert _num(total) == 35.0
        resp = await ctx.engine.execute(_q(dimensions=["id"], measures=["customer.order_total"]))
        assert [_num(v) for (v,) in _rows(resp, "orders.customer.order_total")] == [35.0] * 3
        assert _broadcasts(resp) != []

    async def test_unattributable_host_dimension_associates(self, ctx: _Ctx) -> None:
        oracle = [(i, _num(v)) for i, v in ctx.oracle(
            "SELECT o.id, (SELECT SUM(o2.amount) FROM orders o2 WHERE o2.customer_id = o.customer_id) "
            "FROM orders o ORDER BY o.id")]
        assert oracle == [(11, 30.0), (12, 30.0), (21, 5.0)]
        resp = await ctx.engine.execute(_q(
            dimensions=["id"], measures=["customer.order_total"], to_many_handling="associate",
            order=[{"column": "id", "direction": "asc"}]))
        assert [(i, _num(v)) for i, v in _rows(resp, "orders.id", "orders.customer.order_total")] == oracle

    async def test_measure_through_a_pointer_back_to_the_host(self, ctx: _Ctx) -> None:
        oracle = [(n, _num(v)) for n, v in ctx.oracle(
            "SELECT c.name, SUM(lo.amount) FROM customers c JOIN orders lo ON c.last_order_id = lo.id "
            "GROUP BY c.name ORDER BY c.name")]
        assert oracle == [("ann", 20.0), ("bob", 5.0)]
        resp = await ctx.engine.execute(_q(
            dimensions=["customer.name"], measures=["customer.latest_total"], to_many_handling="associate",
            order=[{"column": "customer.name", "direction": "asc"}]))
        got = [(n, _num(v)) for n, v in _rows(resp, "orders.customer.name", "orders.customer.latest_total")]
        assert got == oracle

    async def test_formula_revisiting_its_own_frame_is_refused(self, ctx: _Ctx) -> None:
        query = _q(dimensions=["customer.name"], measures=["customer.bad"])
        with pytest.raises(CircularJoinPathError) as ei:
            await ctx.engine.execute(query)
        assert ei.value.hop == "customer"
        assert ei.value.revisited == "customers"

    async def test_composing_measure_has_no_hand_written_equivalent(self, ctx: _Ctx) -> None:
        resp = await ctx.engine.execute(_q(
            dimensions=["customer.name"], measures=["customer.latest_total"], to_many_handling="associate"))
        assert resp.data
        query = _q(
            dimensions=["customer.name"],
            measures=[{"formula": "sum(customer.last_order.amount)", "name": "h"}],
            to_many_handling="associate")
        with pytest.raises(CircularJoinPathError):
            await ctx.engine.execute(query)


# --------------------------------------------------------------------------- #
# Composed occurrences keep their own type, format, ranking, time ownership and keys.
# --------------------------------------------------------------------------- #
class TestComposedOccurrenceMetadata:
    async def test_composed_measure_keeps_type_and_format(self, ctx: _Ctx) -> None:
        oracle = dict(ctx.oracle(
            "SELECT c.name, AVG(CAST(lo.big AS DOUBLE)) FROM customers c JOIN orders lo "
            "ON c.last_order_id = lo.id GROUP BY c.name"))
        assert {k: _num(v) for k, v in oracle.items()} == {"ann": 1.0, "bob": 0.0}
        native = await ctx.engine.execute(SlayerQuery.model_validate({
            "source_model": "customers", "dimensions": ["name"],
            "measures": [{"formula": "avg(last_order.big)", "name": "s"},
                         {"formula": "sum(last_order.amount)", "name": "t"}]}))
        assert native.attributes.measures["customers.s"].format == NumberFormat(type=NumberFormatType.PERCENT)
        assert native.attributes.measures["customers.t"].format == CURRENCY
        resp = await ctx.engine.execute(_q(
            dimensions=["customer.name"], measures=["customer.latest_big_share", "customer.latest_total"],
            to_many_handling="associate"))
        assert resp.attributes.measures["orders.customer.latest_big_share"].format == NumberFormat(
            type=NumberFormatType.PERCENT)
        assert resp.attributes.measures["orders.customer.latest_total"].format == CURRENCY
        got = {n: _num(v) for n, v in _rows(resp, "orders.customer.name", "orders.customer.latest_big_share")}
        assert got == {k: _num(v) for k, v in oracle.items()}

    async def test_last_ranks_over_a_composed_path(self, ctx: _Ctx) -> None:
        oracle = [(n, _num(v)) for n, v in ctx.oracle(
            "SELECT c.name, o2.amount FROM customers c JOIN orders o2 ON o2.customer_id = c.id "
            "WHERE o2.ordered_at = (SELECT MAX(o3.ordered_at) FROM orders o3 WHERE o3.customer_id = c.id) "
            "ORDER BY c.name")]
        assert oracle == [("ann", 20.0), ("bob", 5.0)]
        resp = await ctx.engine.execute(_q(
            dimensions=["customer.name"], measures=["customer.last_ranked"],
            order=[{"column": "customer.name", "direction": "asc"}]))
        got = [(n, _num(v)) for n, v in _rows(resp, "orders.customer.name", "orders.customer.last_ranked")]
        assert got == oracle

    async def test_time_dimension_is_owned_by_its_occurrence(self, ctx: _Ctx) -> None:
        oracle = ctx.oracle(
            f"SELECT CAST(lo.ordered_at AS VARCHAR), COUNT(*) {_ORDERS_THROUGH_LAST} "
            "GROUP BY lo.ordered_at ORDER BY lo.ordered_at DESC")
        assert [(str(t)[:7], n) for t, n in oracle] == [("2024-03", 1), ("2024-02", 2)]
        td = TimeDimension(dimension=ColumnRef(name="customer.last_ordered_at"),
                           granularity=TimeGranularity.MONTH)
        resp = await ctx.engine.execute(_q(
            time_dimensions=[td], measures=[{"formula": "count(*)", "name": "n"}],
            order=[{"column": "customer.last_ordered_at", "direction": "desc"}]))
        key = next(k for k in resp.columns if "last_ordered_at" in k)
        got = [(str(t)[:7], n) for t, n in _rows(resp, key, "orders.n")]
        assert got == [(str(t)[:7], n) for t, n in oracle]

    async def test_spelling_twins_of_a_composed_path_share_one_cache_key(self, ctx: _Ctx) -> None:
        for edge_spelled, model_spelled in (
            ("customer.last_amount", "orders.customer.last_amount"),
            ("customer.latest_total", "orders.customer.latest_total"),
        ):
            kw = {"to_many_handling": "associate"}
            if edge_spelled.endswith("total"):
                one = await ctx.engine.execute(_q(measures=[edge_spelled], **kw), dry_run=True)
                two = await ctx.engine.execute(_q(measures=[model_spelled], **kw), dry_run=True)
            else:
                one = await ctx.engine.execute(_q(dimensions=[edge_spelled]), dry_run=True)
                two = await ctx.engine.execute(_q(dimensions=[model_spelled]), dry_run=True)
            assert one.sql == two.sql


# --------------------------------------------------------------------------- #
# Composed key paths walk with revisits allowed; schema drift and cancellation.
# --------------------------------------------------------------------------- #
def _by_name() -> dict[str, SlayerModel]:
    return {m.name: m for m in (_orders(), _customers())}


class TestComposedWalk:
    def test_follow_allows_revisits_walk_does_not(self) -> None:
        models = _by_name()
        chain = follow(root=models["orders"], path=("customer", "last_order", "customer"),
                       models_by_name=models)
        assert chain is not None
        assert [e.target_model for e in chain] == ["customers", "orders", "customers"]
        terminal = resolved_terminal(root=models["orders"], path=("customer", "last_order"),
                                     models_by_name=models)
        assert terminal is not None
        assert terminal.name == "orders"
        with pytest.raises(CircularJoinPathError):
            walk(root=models["orders"], path=("customer", "last_order"), models_by_name=models)

    def test_typed_revisit_attributes_to_nothing_in_drift(self) -> None:
        models = _by_name()
        stage = SlayerQuery.model_validate({"source_model": "orders", "dimensions": ["customer.last_order.amount"]})
        graph = _build_stage_graph(stage=stage, stage_source_name="orders", models_by_name=models)
        assert _stage_referenced_columns_for_base(stage=stage, base_name="orders", graph=graph) == set()

    def test_composed_reference_attributes_to_its_declaring_model(self) -> None:
        models = _by_name()
        stage = SlayerQuery.model_validate({"source_model": "orders", "dimensions": ["customer.last_amount"]})
        graph = _build_stage_graph(stage=stage, stage_source_name="orders", models_by_name=models)
        assert _stage_referenced_columns_for_base(stage=stage, base_name="customers", graph=graph) == {
            "last_amount"}
        assert _stage_referenced_columns_for_base(stage=stage, base_name="orders", graph=graph) == set()


class TestCancellationToTheNearestOccurrence:
    def _cancel(self, *, owner_path: tuple[str, ...], tokens: tuple[str, ...],
                models: dict[str, SlayerModel] | None = None, root: str = "orders") -> tuple[str, ...] | None:
        models = models or _by_name()
        return walk_cancelling(root=models[root], owner_path=owner_path, tokens=tokens, models_by_name=models)

    def test_repeated_dataset_cancels_to_the_innermost_occurrence(self) -> None:
        assert self._cancel(owner_path=("customer", "last_order"), tokens=("orders",)) == (
            "customer", "last_order")

    def test_other_dataset_cancels_back_along_a_composed_stack(self) -> None:
        assert self._cancel(owner_path=("customer", "last_order"), tokens=("customers",)) == ("customer",)
        assert self._cancel(owner_path=("customer", "last_order", "customer"), tokens=("customers",)) == (
            "customer", "last_order", "customer")

    def test_stage_spelling_cancels_to_the_stage_occurrence(self) -> None:
        models = _by_name()
        stage = _orders().model_copy(update={"name": "stage_recent"}).with_spelling("recent")
        cust = _customers().model_copy(update={"joins": [
            *_customers().joins,
            ModelJoin(target_model="stage_recent", name="recent_order",
                      join_pairs=[["last_order_id", "id"]], cardinality=M2O),
        ]})
        models = {**models, "customers": cust, "stage_recent": stage}
        assert self._cancel(root="stage_recent", models=models,
                            owner_path=("customer", "last_order"), tokens=("recent",)) == ()
        assert self._cancel(root="stage_recent", models=models,
                            owner_path=("customer", "last_order"), tokens=("orders",)) == (
            "customer", "last_order")

    def test_incident_edge_name_hops_rather_than_cancels(self) -> None:
        notes = SlayerModel(
            name="notes", data_source=DS, sql_table="notes",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)])
        cust = _customers().model_copy(update={
            "columns": [*_customers().columns, Column(name="note_id", type=DataType.INT)],
            "joins": [*_customers().joins, ModelJoin(
                target_model="notes", name="orders", join_pairs=[["note_id", "id"]], cardinality=M2O)],
        })
        models = {**_by_name(), "customers": cust, "notes": notes}
        assert self._cancel(models=models, owner_path=("customer", "last_order", "customer"),
                            tokens=("orders",)) == ("customer", "last_order", "customer", "orders")
