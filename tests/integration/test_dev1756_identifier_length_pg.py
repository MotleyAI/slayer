"""Identifier-length fitting against a live Postgres, which truncates identifiers past 63 bytes with
only a NOTICE, so emission tests pass while the server mis-answers. Covers two modes:
sibling aliases collapsing at a shared 63-byte prefix, and a lone over-limit alias
vanishing with no error (the more dangerous case).
"""

import re
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import OrderItem, QueryDimension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine

factories = pytest.importorskip("pytest_postgresql.factories")
psycopg = pytest.importorskip("psycopg")

postgresql_proc = factories.postgresql_proc(port=None)

DS = "testpg"

NAME_KEY = "SandboxSubscriptionRecord.SandboxCustomerAccount.SandboxConsumerProfile.name"
EMAIL_KEY = "SandboxSubscriptionRecord.SandboxCustomerAccount.SandboxConsumerProfile.email"


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
    """3-hop relationship chain whose projection aliases cross Postgres' 63-byte limit."""
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

        run_sync(storage.save_dataset(SlayerDataset(
            name="SandboxConsumerProfile", sql_table="consumers", data_source=DS,
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="name", sql="name", datatype=DataType.STRING),
                DatasetField(name="email", sql="email", datatype=DataType.STRING),
                DatasetField(name="lifetimeValue", sql="lifetime_value", datatype=DataType.FLOAT),
            ],
        )))
        run_sync(storage.save_dataset(SlayerDataset(
            name="SandboxCustomerAccount", sql_table="customers", data_source=DS,
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="consumer_id", sql="consumer_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="SandboxConsumerProfile", from_columns=["consumer_id"], to_columns=["id"])],
        )))
        run_sync(storage.save_dataset(SlayerDataset(
            name="SandboxSubscriptionRecord", sql_table="subscriptions", data_source=DS,
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="SandboxCustomerAccount", from_columns=["customer_id"], to_columns=["id"])],
        )))
        run_sync(storage.save_dataset(SlayerDataset(
            name="SandboxInvoiceV2", sql_table="invoices", data_source=DS,
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="totalAmount", sql="total_amount", datatype=DataType.FLOAT),
                DatasetField(name="subscription_id", sql="subscription_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(
                to="SandboxSubscriptionRecord", from_columns=["subscription_id"], to_columns=["id"],
            )],
        )))
        yield storage
    finally:
        conn.close()
        _drop_db(postgresql_proc, db_name)


@pytest.fixture
async def chain_env(_chain_storage: YAMLStorage) -> AsyncIterator[SlayerQueryEngine]:
    """Per-test engine — the async SQLAlchemy engine binds to the event loop."""
    async with owned_query_engine(storage=_chain_storage) as engine:
        yield engine


DEEP = "SandboxSubscriptionRecord.SandboxCustomerAccount.SandboxConsumerProfile"


@pytest.mark.integration
class TestPostgresIdentifierLength:
    async def test_server_truncates_at_63_bytes(self, chain_env: SlayerQueryEngine) -> None:
        """Pin the 63-byte truncation premise against the live server."""
        client = chain_env.client_for(
            await chain_env._resolve_datasource(
                dataset=cast(SlayerDataset, await chain_env.storage.get_dataset("SandboxInvoiceV2", data_source=DS)),
            ),
        )
        rows = (await client.execute(sql=f'SELECT 1 AS "{EMAIL_KEY}"')).rows
        assert list(rows[0])[0] != EMAIL_KEY, "server did not truncate; premise broken"
        assert len(list(rows[0])[0].encode()) == 63

    async def test_collapse_with_outer_wrap(self, chain_env: SlayerQueryEngine) -> None:
        """Two over-limit siblings + ORDER BY outer wrap; raised AmbiguousColumnError before the fix."""
        query = SlayerQuery.model_validate({"population": "SandboxInvoiceV2", "dimensions": [
                QueryDimension(field=f"{DEEP}.name"),
                QueryDimension(field=f"{DEEP}.email"),
                QueryDimension(field="SandboxInvoiceV2.status"),
            ], "metrics": [{"metric": "sum(SandboxInvoiceV2.totalAmount)"}, {"metric": "count(SandboxInvoiceV2.*)"}], "order": [OrderItem.model_validate({
                "field": "avg(SandboxInvoiceV2.totalAmount)",
                "direction": "desc",
            })], "limit": 10})
        result = await chain_env.execute(query=query)
        # Pin the exact pairing; a wrong-column alias would still be "present with distinct values".
        got = {
            (row[NAME_KEY], row[EMAIL_KEY], row["SandboxInvoiceV2.status"]):
                (float(row["SandboxInvoiceV2.totalAmount_sum"]), row["SandboxInvoiceV2._count"])
            for row in result.data
        }
        assert got == {
            ("Ann", "ann@example.io", "paid"): (150.0, 2),
            ("Bob", "bob@example.io", "paid"): (200.0, 1),
        }

    async def test_collapse_without_outer_wrap(self, chain_env: SlayerQueryEngine) -> None:
        """No outer wrap: the two siblings would silently collapse into one result key."""
        query = SlayerQuery.model_validate({"population": "SandboxInvoiceV2", "dimensions": [
                QueryDimension(field=f"{DEEP}.name"),
                QueryDimension(field=f"{DEEP}.email"),
            ], "metrics": [{"metric": "count(SandboxInvoiceV2.*)"}]})
        result = await chain_env.execute(query=query)
        assert result.data
        names = {row[NAME_KEY] for row in result.data}
        emails = {row[EMAIL_KEY] for row in result.data}
        assert names == {"Ann", "Bob"}
        assert emails == {"ann@example.io", "bob@example.io"}

    async def test_silent_loss_single_over_limit_alias(self, chain_env: SlayerQueryEngine) -> None:
        """One over-limit alias, no sibling: Postgres keys the row by the truncated name and it vanishes."""
        query = SlayerQuery.model_validate({
            "population": "SandboxInvoiceV2",
            "dimensions": [QueryDimension(field=f"{DEEP}.email")],
            "metrics": [{"metric": "sum(SandboxInvoiceV2.totalAmount)"}],
        })
        result = await chain_env.execute(query=query)
        assert result.data
        for row in result.data:
            assert EMAIL_KEY in row, (
                f"over-limit alias missing from the result row: {sorted(row)}"
            )
            assert row[EMAIL_KEY] in {"ann@example.io", "bob@example.io"}

    async def test_values_are_correct_not_merely_present(self, chain_env: SlayerQueryEngine) -> None:
        """Pin the aggregate; a wrong-column fitted alias would still be present."""
        query = SlayerQuery.model_validate({
            "population": "SandboxInvoiceV2",
            "dimensions": [QueryDimension(field=f"{DEEP}.email")],
            "metrics": [{"metric": "sum(SandboxInvoiceV2.totalAmount)"}],
        })
        result = await chain_env.execute(query=query)
        totals = {row[EMAIL_KEY]: float(row["SandboxInvoiceV2.totalAmount_sum"]) for row in result.data}
        assert totals == {"ann@example.io": 150.0, "bob@example.io": 200.0}

    async def test_response_columns_are_canonical(self, chain_env: SlayerQueryEngine) -> None:
        """Response exposes canonical keys while the emitted SQL carries the fitted form."""
        query = SlayerQuery.model_validate({
            "population": "SandboxInvoiceV2",
            "dimensions": [QueryDimension(field=f"{DEEP}.name"), QueryDimension(field=f"{DEEP}.email")],
            "metrics": [{"metric": "count(SandboxInvoiceV2.*)"}],
        })
        result = await chain_env.execute(query=query)
        assert set(result.data[0]) >= {NAME_KEY, EMAIL_KEY}
        assert EMAIL_KEY not in cast(str, result.sql)

    async def test_cached_execution_round_trip(self, chain_env: SlayerQueryEngine) -> None:
        """A cache hit returns canonical keys too, proving decode runs before storage."""
        query = SlayerQuery.model_validate({
            "population": "SandboxInvoiceV2",
            "dimensions": [QueryDimension(field=f"{DEEP}.email")],
            "metrics": [{"metric": "count(SandboxInvoiceV2.*)"}],
        })
        first = await chain_env.execute(query=query, cache=True)
        second = await chain_env.execute(query=query, cache=True)
        assert EMAIL_KEY in first.data[0]
        assert EMAIL_KEY in second.data[0]
        assert first.data == second.data

    async def test_deep_cross_model_measure_executes(self, chain_env: SlayerQueryEngine) -> None:
        """Surface 3: the `_cm_` cross-dataset CTE name also crosses 63 bytes on this chain."""
        query = SlayerQuery.model_validate({
            "population": "SandboxInvoiceV2",
            "dimensions": [QueryDimension(field="SandboxInvoiceV2.status")],
            "metrics": [{"metric": f"sum({DEEP}.lifetimeValue)"}],
        })
        result = await chain_env.execute(query=query)
        assert "_cm_" in cast(str, result.sql), "no cross-dataset CTE generated; test is vacuous"
        for name in re.findall(r"\b_cm_\w+", cast(str, result.sql)):
            assert len(name.encode()) <= 63, f"{name!r} exceeds the Postgres limit"
        alias = f"{DEEP}.lifetimeValue_sum"
        assert len(result.data) == 1
        assert float(result.data[0][alias]) == 30.0

    async def test_nested_query_backed_model_executes(self, chain_env: SlayerQueryEngine) -> None:
        """Surface 4: virtual-dataset short names as output aliases; mixed-case exercises case-folding."""
        stage1 = SlayerQuery.model_validate({
            "name": "stage1",
            "population": "SandboxInvoiceV2",
            "dimensions": [QueryDimension(field=f"{DEEP}.email")],
            "metrics": [{"metric": "sum(SandboxInvoiceV2.totalAmount)"}],
        })
        stage2 = SlayerQuery.model_validate({
            "population": "stage1",
            "dimensions": [QueryDimension(field="stage1.SandboxSubscriptionRecord__SandboxCustomerAccount__SandboxConsumerProfile__email")],
            "metrics": [{"metric": "sum(stage1.totalAmount_sum)"}],
        })
        result = await chain_env.execute(query=[stage1, stage2])
        assert result.data
        assert len(result.data) == 2
