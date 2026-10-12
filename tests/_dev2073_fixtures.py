"""Live FK schemas plus an ingest-and-query workspace for the FK-ingestion tests."""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import AsyncGenerator

import duckdb
from pydantic import BaseModel, ConfigDict, PrivateAttr

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

DS = "ds"

SALES_CHAIN = """
CREATE TABLE stores (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE sales (id INTEGER PRIMARY KEY, store_id INTEGER REFERENCES stores(id), total REAL);
INSERT INTO stores VALUES (1, 'North'), (2, 'South');
INSERT INTO sales VALUES (1, 1, 10.0), (2, 1, 15.0), (3, 2, 4.0);
"""

# transactions ↔ coupon_usages 2-cycle (non-mirrored data) + an unrelated chain.
TWO_CYCLE = """
CREATE TABLE transactions (id INTEGER PRIMARY KEY, coupon_usage_id INTEGER REFERENCES coupon_usages(id), amount REAL);
CREATE TABLE coupon_usages (id INTEGER PRIMARY KEY, transaction_id INTEGER REFERENCES transactions(id), code TEXT);
INSERT INTO transactions VALUES (1, 10, 5.0), (2, 11, 7.0), (3, NULL, 9.0);
INSERT INTO coupon_usages VALUES (10, 2, 'A'), (11, 1, 'B'), (12, 3, 'C');
""" + SALES_CHAIN

THREE_CYCLE = """
CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES depts(id), name TEXT, salary REAL);
CREATE TABLE depts (id INTEGER PRIMARY KEY, office_id INTEGER REFERENCES offices(id), name TEXT);
CREATE TABLE offices (id INTEGER PRIMARY KEY, manager_id INTEGER REFERENCES employees(id), city TEXT);
INSERT INTO employees VALUES (1, 1, 'Ann', 100.0), (2, 2, 'Ben', 80.0), (3, 1, 'Cy', 50.0);
INSERT INTO depts VALUES (1, 1, 'Eng'), (2, 2, 'Ops');
INSERT INTO offices VALUES (1, 2, 'Paris'), (2, 1, 'Rome');
"""

LATEST_CHILD = """
CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, last_order_id INTEGER REFERENCES orders(id));
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id), amount REAL);
INSERT INTO customers VALUES (1, 'Alice', 2), (2, 'Bob', 3);
INSERT INTO orders VALUES (1, 1, 10.0), (2, 1, 20.0), (3, 2, 30.0);
"""

MUTUAL_PK = """
CREATE TABLE users (id INTEGER PRIMARY KEY REFERENCES profiles(user_id), name TEXT);
CREATE TABLE profiles (user_id INTEGER PRIMARY KEY REFERENCES users(id), bio TEXT);
INSERT INTO users VALUES (1, 'Ann'), (2, 'Ben');
INSERT INTO profiles VALUES (1, 'hi'), (2, 'yo');
"""

# Portable to SQLite and DuckDB (DuckDB needs the referenced table first).
BILLING_SHIPPING = """
CREATE TABLE addresses (id INTEGER PRIMARY KEY, city TEXT);
CREATE TABLE orders (id INTEGER PRIMARY KEY, billing_address_id INTEGER REFERENCES addresses(id),
                     shipping_address_id INTEGER REFERENCES addresses(id), amount DOUBLE);
INSERT INTO addresses VALUES (1, 'Oslo'), (2, 'Lima'), (3, 'Kyiv');
INSERT INTO orders VALUES (1, 1, 2, 10.0), (2, 1, 1, 20.0), (3, 2, 3, 30.0);
"""
BILLING_CITIES = {(1, "Oslo"), (2, "Oslo"), (3, "Lima")}
SHIPPING_CITIES = {(1, "Lima"), (2, "Oslo"), (3, "Kyiv")}

CHAIN = """
CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id), amount REAL);
INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO orders VALUES (1, 1, 10.0), (2, 1, 20.0), (3, 2, 30.0);
"""

# x → y → z → x, every edge a singleton.
DIRECTED_3_CYCLE = """
CREATE TABLE x (id INTEGER PRIMARY KEY, y_id INTEGER REFERENCES y(id), v TEXT);
CREATE TABLE y (id INTEGER PRIMARY KEY, z_id INTEGER REFERENCES z(id), v TEXT);
CREATE TABLE z (id INTEGER PRIMARY KEY, x_id INTEGER REFERENCES x(id), v TEXT, amount REAL);
INSERT INTO x VALUES (1, 1, 'x1'), (2, 2, 'x2');
INSERT INTO y VALUES (1, 1, 'y1'), (2, 2, 'y2');
INSERT INTO z VALUES (1, 2, 'z1', 10.0), (2, 2, 'z2', 5.0);
"""

EdgeKey = tuple[str, str, tuple[tuple[str, str], ...]]


def run_script(*, db_path: str, dialect: str, script: str) -> None:
    if dialect == "duckdb":
        with contextlib.closing(duckdb.connect(db_path)) as con:
            con.execute(script)
        return
    with transaction(db_path) as con:
        con.executescript(script)


def edge_key(model: str, join: ModelJoin) -> EdgeKey:
    return (model, join.target_model, tuple((p[0], p[1]) for p in join.join_pairs))


class Live(BaseModel):
    """A live database plus the storage it is ingested into."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    db_path: str
    dialect: str
    storage: YAMLStorage
    ds: DatasourceConfig
    _engine: SlayerQueryEngine | None = PrivateAttr(default=None)

    def run(self, script: str) -> None:
        run_script(db_path=self.db_path, dialect=self.dialect, script=script)
        engine_factory.invalidate_engine(self.ds)

    async def ingest(self):
        return await ingest_datasource_idempotent(datasource=self.ds, storage=self.storage)

    async def model(self, name: str) -> SlayerModel:
        model = await self.storage.get_model(name, data_source=DS)
        assert model is not None, name
        return model

    async def models(self) -> dict[str, SlayerModel]:
        loaded, failed = await self.storage.load_models(data_source=DS)
        assert failed == []
        return {m.name: m for m in loaded}

    async def edges(self) -> dict[EdgeKey, str | None]:
        """Every stored join keyed by (declaring model, target, pairs) → its name."""
        return {
            edge_key(m.name, j): j.name
            for m in (await self.models()).values() for j in m.joins
        }

    async def query(self, **kwargs):
        if self._engine is None:
            self._engine = SlayerQueryEngine(storage=self.storage)
        return await self._engine.execute(SlayerQuery(**kwargs))

    async def aclose(self) -> None:
        if self._engine is not None:
            await self._engine.aclose()
        engine_factory.invalidate_engine(self.ds)


@contextlib.asynccontextmanager
async def live(script: str, *, dialect: str = "sqlite") -> AsyncGenerator[Live]:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "live.duckdb" if dialect == "duckdb" else "live.db")
        run_script(db_path=db_path, dialect=dialect, script=script)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        ds = DatasourceConfig(name=DS, type=dialect, database=db_path)
        await storage.save_datasource(ds)
        workspace = Live(db_path=db_path, dialect=dialect, storage=storage, ds=ds)
        try:
            yield workspace
        finally:
            await workspace.aclose()


def addition_for(result, model_name: str):
    return next(a for a in result.additions if a.model_name == model_name)


# Hand-built orders → addresses parallel pair, for surfaces that need no live database.
BILLING_PAIRS = [["billing_address_id", "id"]]
SHIPPING_PAIRS = [["shipping_address_id", "id"]]


def join_to(pairs: list[list[str]], name: str | None = None, target: str = "addresses") -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=pairs, name=name, cardinality=JoinCardinality.MANY_TO_ONE)


def orders_with(*joins: ModelJoin) -> SlayerModel:
    return SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="billing_address_id", type=DataType.INT),
            Column(name="shipping_address_id", type=DataType.INT),
            Column(name="legacy_addr", type=DataType.INT),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
        ],
        joins=list(joins),
    )


def addresses_model() -> SlayerModel:
    return SlayerModel(
        name="addresses", data_source=DS, sql_table="addresses",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="city", type=DataType.TEXT)],
    )


def named_pair() -> tuple[ModelJoin, ModelJoin]:
    return join_to(BILLING_PAIRS, "billing_address"), join_to(SHIPPING_PAIRS, "shipping_address")


def unnamed_pair() -> tuple[ModelJoin, ModelJoin]:
    return join_to(BILLING_PAIRS), join_to(SHIPPING_PAIRS)


@contextlib.asynccontextmanager
async def model_store(*models: SlayerModel) -> AsyncGenerator[YAMLStorage]:
    """A storage holding ``models`` under an in-memory SQLite datasource."""
    with tempfile.TemporaryDirectory() as tmp:
        store = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await store.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=":memory:"))
        for model in models:
            await store.save_model(model)
        yield store


async def stored_joins(storage: YAMLStorage, model: str = "orders") -> list[tuple[str | None, list[list[str]]]]:
    stored = await storage.get_model(model, data_source=DS)
    assert stored is not None
    return [(j.name, j.join_pairs) for j in stored.joins]
