"""DEV-1743 — live Postgres coverage for the dotted-canonical flip.

Byte-level SQL assertions cannot catch a silently-wrong join, so these run the
real query and assert per-path VALUES:

* a ``__``-named model joined and grouped returns the right per-label sums;
* two deep long-name join paths whose naive ``__`` aliases collide after
  Postgres's 63-byte truncation still resolve to their OWN relations.

Module-skips without ``pytest_postgresql``; the second class also fails today
because join aliases are not length-fitted (the DEV-1756-deferred defect).
"""

import uuid

import pytest

pytest.importorskip("pytest_postgresql")

import psycopg
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from slayer.async_utils import run_sync

postgresql_proc = factories.postgresql_proc(port=None)


def _fresh_db(proc):
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(host=proc.host, port=proc.port, user=proc.user,
                            dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()
    conn = psycopg.connect(host=proc.host, port=proc.port, user=proc.user,
                           dbname=db_name)
    return conn, db_name


def _drop_db(proc, db_name):
    admin = psycopg.connect(host=proc.host, port=proc.port, user=proc.user,
                            dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')
    admin.close()


def _storage_for(proc, db_name, tmpdir, models) -> YAMLStorage:
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(DatasourceConfig(
        name="testpg", type="postgres", host=proc.host, port=proc.port,
        database=db_name, username=proc.user, password="")))
    for m in models:
        run_sync(storage.save_model(m))
    return storage


# --------------------------------------------------------------------------- #
# A __-named model joined and grouped returns correct per-label sums.
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def _dunder_storage(postgresql_proc, tmp_path_factory):
    conn, db_name = _fresh_db(postgresql_proc)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE customer_region_dim "
                    "(id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, "
                    "cr_id INTEGER REFERENCES customer_region_dim(id), "
                    "amount NUMERIC(10,2) NOT NULL)")
        cur.executemany("INSERT INTO customer_region_dim VALUES (%s, %s)",
                        [(1, "North"), (2, "South")])
        cur.executemany("INSERT INTO orders VALUES (%s, %s, %s)",
                        [(1, 1, 10), (2, 2, 20), (3, 1, 5)])
        conn.commit()
        models = [
            SlayerModel(name="customer__region", sql_table="customer_region_dim",
                        data_source="testpg", columns=[
                            Column(name="id", type=DataType.DOUBLE, primary_key=True),
                            Column(name="label", type=DataType.TEXT)]),
            SlayerModel(name="orders", sql_table="orders", data_source="testpg",
                        columns=[
                            Column(name="id", type=DataType.DOUBLE, primary_key=True),
                            Column(name="cr_id", type=DataType.DOUBLE),
                            Column(name="amount", type=DataType.DOUBLE)],
                        joins=[ModelJoin(target_model="customer__region",
                                         join_pairs=[["cr_id", "id"]])]),
        ]
        storage = _storage_for(postgresql_proc, db_name,
                               str(tmp_path_factory.mktemp("dunder")), models)
        yield storage
    finally:
        conn.close()
        _drop_db(postgresql_proc, db_name)


@pytest.mark.integration
class TestDunderModelLiveValues:
    async def test_per_label_sums(self, _dunder_storage) -> None:
        engine = SlayerQueryEngine(storage=_dunder_storage)
        result = await engine.execute(query=SlayerQuery(
            source_model="orders",
            dimensions=["customer__region.label"],
            measures=[{"formula": "amount:sum"}],
        ))
        by_label = {r["orders.customer__region.label"]: r["orders.amount_sum"]
                    for r in result.data}
        assert by_label == {"North": 15, "South": 20}


# --------------------------------------------------------------------------- #
# Two deep long-name paths whose 63-byte-truncated aliases collide must still
# resolve to their OWN relations (correct distinct values).
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def _long_alias_storage(postgresql_proc, tmp_path_factory):
    conn, db_name = _fresh_db(postgresql_proc)
    try:
        cur = conn.cursor()
        # north/south leaves differ only after byte 63 of the emitted alias.
        cur.execute("CREATE TABLE household_entity_record_north "
                    "(id INTEGER PRIMARY KEY, tag TEXT NOT NULL)")
        cur.execute("CREATE TABLE household_entity_record_south "
                    "(id INTEGER PRIMARY KEY, tag TEXT NOT NULL)")
        cur.execute("CREATE TABLE consumer_entity_record (id INTEGER PRIMARY KEY, "
                    "north_id INTEGER REFERENCES household_entity_record_north(id), "
                    "south_id INTEGER REFERENCES household_entity_record_south(id))")
        cur.execute("CREATE TABLE customer_entity_record (id INTEGER PRIMARY KEY, "
                    "consumer_id INTEGER REFERENCES consumer_entity_record(id))")
        cur.execute("CREATE TABLE subscription_entity (id INTEGER PRIMARY KEY, "
                    "customer_id INTEGER REFERENCES customer_entity_record(id), "
                    "amount NUMERIC(10,2) NOT NULL)")
        cur.execute("INSERT INTO household_entity_record_north VALUES (1, 'N')")
        cur.execute("INSERT INTO household_entity_record_south VALUES (1, 'S')")
        cur.execute("INSERT INTO consumer_entity_record VALUES (1, 1, 1)")
        cur.execute("INSERT INTO customer_entity_record VALUES (1, 1)")
        cur.execute("INSERT INTO subscription_entity VALUES (1, 1, 100)")
        conn.commit()

        def m(name, cols, joins=()):
            return SlayerModel(name=name, sql_table=name, data_source="testpg",
                               columns=cols, joins=list(joins))
        idc = lambda n: Column(name=n, type=DataType.DOUBLE, primary_key=(n == "id"))  # noqa: E731
        models = [
            m("household_entity_record_north",
              [idc("id"), Column(name="tag", type=DataType.TEXT)]),
            m("household_entity_record_south",
              [idc("id"), Column(name="tag", type=DataType.TEXT)]),
            m("consumer_entity_record",
              [idc("id"), idc("north_id"), idc("south_id")],
              [ModelJoin(target_model="household_entity_record_north",
                         join_pairs=[["north_id", "id"]]),
               ModelJoin(target_model="household_entity_record_south",
                         join_pairs=[["south_id", "id"]])]),
            m("customer_entity_record", [idc("id"), idc("consumer_id")],
              [ModelJoin(target_model="consumer_entity_record",
                         join_pairs=[["consumer_id", "id"]])]),
            m("subscription_entity",
              [idc("id"), idc("customer_id"), Column(name="amount", type=DataType.DOUBLE)],
              [ModelJoin(target_model="customer_entity_record",
                         join_pairs=[["customer_id", "id"]])]),
        ]
        storage = _storage_for(postgresql_proc, db_name,
                               str(tmp_path_factory.mktemp("longalias")), models)
        yield storage
    finally:
        conn.close()
        _drop_db(postgresql_proc, db_name)


@pytest.mark.integration
class TestLongAliasCollisionLiveValues:
    async def test_colliding_paths_keep_distinct_values(self, _long_alias_storage) -> None:
        engine = SlayerQueryEngine(storage=_long_alias_storage)
        north = ("customer_entity_record.consumer_entity_record."
                 "household_entity_record_north.tag")
        south = ("customer_entity_record.consumer_entity_record."
                 "household_entity_record_south.tag")
        result = await engine.execute(query=SlayerQuery(
            source_model="subscription_entity",
            dimensions=[north, south],
            measures=[{"formula": "amount:sum"}],
        ))
        assert result.row_count == 1
        row = result.data[0]
        # The two long paths must read their OWN leaf, not a truncation-merged one.
        assert row[f"subscription_entity.{north}"] == "N"
        assert row[f"subscription_entity.{south}"] == "S"
