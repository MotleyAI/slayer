"""DEV-1756 against a live Postgres, which truncates identifiers past 63 bytes with
only a NOTICE, so emission tests pass while the server mis-answers. Covers two modes:
sibling aliases collapsing at a shared 63-byte prefix, and a lone over-limit alias
vanishing with no error (the more dangerous case).
"""

import re
import uuid

import pytest

pytest.importorskip("pytest_postgresql")

import psycopg
from pytest_postgresql import factories

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

postgresql_proc = factories.postgresql_proc(port=None)

DS = "testpg"

LONG_NAME = "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.name"
LONG_EMAIL = "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.email"


def _create_db(postgresql_proc):
    info = postgresql_proc
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()
    conn = psycopg.connect(host=info.host, port=info.port, user=info.user, dbname=db_name)
    return conn, db_name


def _drop_db(postgresql_proc, db_name):
    info = postgresql_proc
    admin = psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')
    admin.close()


@pytest.fixture(scope="module")
def _chain_storage(postgresql_proc, tmp_path_factory):
    """3-hop join chain whose projection aliases cross Postgres' 63-byte limit."""
    conn, db_name = _create_db(postgresql_proc)
    try:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE consumers (id INTEGER PRIMARY KEY, name TEXT, email TEXT, "
            "lifetime_value NUMERIC(10,2))"
        )
        cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, consumer_id INTEGER)")
        cur.execute("CREATE TABLE subscriptions (id INTEGER PRIMARY KEY, customer_id INTEGER)")
        cur.execute(
            "CREATE TABLE invoices (id INTEGER PRIMARY KEY, subscription_id INTEGER, "
            "status TEXT, total_amount NUMERIC(10,2))"
        )
        cur.executemany(
            "INSERT INTO consumers VALUES (%s, %s, %s, %s)",
            [(1, "Ann", "ann@example.io", 10), (2, "Bob", "bob@example.io", 20)],
        )
        cur.executemany("INSERT INTO customers VALUES (%s, %s)", [(1, 1), (2, 2)])
        cur.executemany("INSERT INTO subscriptions VALUES (%s, %s)", [(1, 1), (2, 2)])
        cur.executemany(
            "INSERT INTO invoices VALUES (%s, %s, %s, %s)",
            [(1, 1, "paid", 100), (2, 1, "paid", 50), (3, 2, "paid", 200)],
        )
        conn.commit()

        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("dev1756_pg")))
        info = postgresql_proc
        run_sync(storage.save_datasource(DatasourceConfig(
            name=DS, type="postgres", host=info.host, port=info.port,
            database=db_name, username=info.user, password="",
        )))

        run_sync(storage.save_model(SlayerModel(
            name="SandboxConsumer", sql_table="consumers", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="email", sql="email", type=DataType.TEXT),
                Column(name="lifetimeValue", sql="lifetime_value", type=DataType.DOUBLE),
            ],
        )))
        run_sync(storage.save_model(SlayerModel(
            name="SandboxCustomer", sql_table="customers", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="consumer_id", sql="consumer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="SandboxConsumer", join_pairs=[["consumer_id", "id"]])],
        )))
        run_sync(storage.save_model(SlayerModel(
            name="SandboxSubscription", sql_table="subscriptions", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="SandboxCustomer", join_pairs=[["customer_id", "id"]])],
        )))
        run_sync(storage.save_model(SlayerModel(
            name="SandboxInvoiceV2", sql_table="invoices", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="totalAmount", sql="total_amount", type=DataType.DOUBLE),
                Column(name="subscription_id", sql="subscription_id", type=DataType.INT),
            ],
            joins=[ModelJoin(
                target_model="SandboxSubscription", join_pairs=[["subscription_id", "id"]],
            )],
        )))
        yield storage
    finally:
        conn.close()
        _drop_db(postgresql_proc, db_name)


@pytest.fixture
def chain_env(_chain_storage):
    """Per-test engine — the async SQLAlchemy engine binds to the event loop."""
    return SlayerQueryEngine(storage=_chain_storage)


DEEP = "SandboxSubscription.SandboxCustomer.SandboxConsumer"


@pytest.mark.integration
class TestPostgresIdentifierLength:
    async def test_server_truncates_at_63_bytes(self, chain_env) -> None:
        """Pin the 63-byte truncation premise against the live server."""
        client = chain_env._client_for(
            await chain_env._resolve_datasource(
                model=await chain_env.storage.get_model("SandboxInvoiceV2", data_source=DS),
            ),
        )
        rows = await client.execute(sql=f'SELECT 1 AS "{LONG_EMAIL}"')
        assert list(rows[0])[0] != LONG_EMAIL, "server did not truncate; premise broken"
        assert len(list(rows[0])[0].encode()) == 63

    async def test_collapse_with_outer_wrap(self, chain_env) -> None:
        """Two over-limit siblings + ORDER BY outer wrap; raised AmbiguousColumnError before the fix."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[
                ColumnRef(name=f"{DEEP}.name"),
                ColumnRef(name=f"{DEEP}.email"),
                ColumnRef(name="status"),
            ],
            measures=[{"formula": "totalAmount:sum"}, {"formula": "*:count"}],
            order=[OrderItem(column="totalAmount:avg", direction="desc")],
            limit=10,
        )
        result = await chain_env.execute(query=query)
        # Pin the exact pairing; a wrong-column alias would still be "present with distinct values".
        got = {
            (row[LONG_NAME], row[LONG_EMAIL], row["SandboxInvoiceV2.status"]):
                (float(row["SandboxInvoiceV2.totalAmount_sum"]), row["SandboxInvoiceV2._count"])
            for row in result.data
        }
        assert got == {
            ("Ann", "ann@example.io", "paid"): (150.0, 2),
            ("Bob", "bob@example.io", "paid"): (200.0, 1),
        }

    async def test_collapse_without_outer_wrap(self, chain_env) -> None:
        """No outer wrap: the two siblings would silently collapse into one result key."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[
                ColumnRef(name=f"{DEEP}.name"),
                ColumnRef(name=f"{DEEP}.email"),
            ],
            measures=[{"formula": "*:count"}],
        )
        result = await chain_env.execute(query=query)
        assert result.data
        names = {row[LONG_NAME] for row in result.data}
        emails = {row[LONG_EMAIL] for row in result.data}
        assert names == {"Ann", "Bob"}
        assert emails == {"ann@example.io", "bob@example.io"}

    async def test_silent_loss_single_over_limit_alias(self, chain_env) -> None:
        """One over-limit alias, no sibling: Postgres keys the row by the truncated name and it vanishes."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.email")],
            measures=[{"formula": "totalAmount:sum"}],
        )
        result = await chain_env.execute(query=query)
        assert result.data
        for row in result.data:
            assert LONG_EMAIL in row, (
                f"over-limit alias missing from the result row: {sorted(row)}"
            )
            assert row[LONG_EMAIL] in {"ann@example.io", "bob@example.io"}

    async def test_values_are_correct_not_merely_present(self, chain_env) -> None:
        """Pin the aggregate; a wrong-column fitted alias would still be present."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.email")],
            measures=[{"formula": "totalAmount:sum"}],
        )
        result = await chain_env.execute(query=query)
        totals = {row[LONG_EMAIL]: float(row["SandboxInvoiceV2.totalAmount_sum"]) for row in result.data}
        assert totals == {"ann@example.io": 150.0, "bob@example.io": 200.0}

    async def test_response_columns_are_canonical(self, chain_env) -> None:
        """Response exposes canonical keys while the emitted SQL carries the fitted form."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.name"), ColumnRef(name=f"{DEEP}.email")],
            measures=[{"formula": "*:count"}],
        )
        result = await chain_env.execute(query=query)
        assert set(result.data[0]) >= {LONG_NAME, LONG_EMAIL}
        assert LONG_EMAIL not in result.sql

    async def test_cached_execution_round_trip(self, chain_env) -> None:
        """A cache hit returns canonical keys too, proving decode runs before storage."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.email")],
            measures=[{"formula": "*:count"}],
        )
        first = await chain_env.execute(query=query, cache=True)
        second = await chain_env.execute(query=query, cache=True)
        assert LONG_EMAIL in first.data[0]
        assert LONG_EMAIL in second.data[0]
        assert first.data == second.data

    async def test_deep_cross_model_measure_executes(self, chain_env) -> None:
        """Surface 3: the `_cm_` cross-model CTE name also crosses 63 bytes on this chain."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": f"{DEEP}.lifetimeValue:sum"}],
        )
        result = await chain_env.execute(query=query)
        assert "_cm_" in result.sql, "no cross-model CTE generated; test is vacuous"
        for name in re.findall(r"\b_cm_\w+", result.sql):
            assert len(name.encode()) <= 63, f"{name!r} exceeds the Postgres limit"
        alias = f"SandboxInvoiceV2.{DEEP}.lifetimeValue_sum"
        assert len(result.data) == 1
        assert float(result.data[0][alias]) == 30.0

    async def test_nested_query_backed_model_executes(self, chain_env) -> None:
        """Surface 4: virtual-model short names as output aliases; mixed-case exercises case-folding."""
        stage1 = SlayerQuery(
            name="stage1",
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.email")],
            measures=[{"formula": "totalAmount:sum"}],
        )
        stage2 = SlayerQuery(
            source_model="stage1",
            dimensions=[ColumnRef(name="SandboxSubscription__SandboxCustomer__SandboxConsumer__email")],
            measures=[{"formula": "totalAmount_sum:sum"}],
        )
        result = await chain_env.execute(query=[stage1, stage2])
        assert result.data
        assert len(result.data) == 2
