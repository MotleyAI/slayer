"""DEV-1756 on a REAL Postgres server.

The whole point of this issue is that byte-level emission tests pass while the
server rejects (or worse, silently mis-answers) the query. Postgres caps
identifiers at 63 bytes and truncates past it with only a NOTICE, so these
failures are invisible to any test that only inspects generated SQL.

Two distinct failure modes are covered:

* **Collapse** — two sibling aliases share a 63-byte prefix. With the DEV-1444
  outer wrap in play the re-projection is ambiguous (``AmbiguousColumnError``);
  without it the two columns collapse into one in the result row.
* **Silent loss** — a SINGLE over-limit alias with no sibling. Postgres accepts
  the query and returns the row keyed by the truncated name, so the engine's
  canonical-alias lookup misses and a column silently disappears. No error is
  raised anywhere, which makes this the more dangerous of the two.
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
    """3-hop join chain with model names long enough that the projection
    aliases cross Postgres' 63-byte limit."""
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
        """Pin the premise against the actual server, so the rest of this file
        cannot pass for the wrong reason if NAMEDATALEN ever differs."""
        client = chain_env._get_client(
            await chain_env._resolve_datasource(
                model=await chain_env.storage.get_model("SandboxInvoiceV2", data_source=DS),
            ),
            ("probe", "probe"),
        )
        rows = await client.execute(sql=f'SELECT 1 AS "{LONG_EMAIL}"')
        assert list(rows[0])[0] != LONG_EMAIL, "server did not truncate; premise broken"
        assert len(list(rows[0])[0].encode()) == 63

    async def test_collapse_with_outer_wrap(self, chain_env) -> None:
        """The exact reported failure: two 73/74-byte siblings + an ORDER BY
        hoist that forces the DEV-1444 outer wrap. Raised AmbiguousColumnError
        before the fix."""
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
        # Exact rows: a fitted alias that resolved to the WRONG column would
        # still be "present with distinct values", so pin the actual pairing.
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
        """No ORDER BY, so no outer wrap and no server-side error — the two
        columns would silently collapse into one result key."""
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
        """The dangerous mode: ONE over-limit alias, no sibling to collide
        with. Postgres accepts the query and keys the row by the truncated
        name, so the column silently vanishes from the response."""
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
        """A fitted alias that pointed at the wrong column would still be
        'present'. Pin the actual aggregate."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.email")],
            measures=[{"formula": "totalAmount:sum"}],
        )
        result = await chain_env.execute(query=query)
        totals = {row[LONG_EMAIL]: float(row["SandboxInvoiceV2.totalAmount_sum"]) for row in result.data}
        assert totals == {"ann@example.io": 150.0, "bob@example.io": 200.0}

    async def test_response_columns_are_canonical(self, chain_env) -> None:
        """Consumers must never see the shortened form — that is what the
        read-side decode exists for."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.name"), ColumnRef(name=f"{DEEP}.email")],
            measures=[{"formula": "*:count"}],
        )
        result = await chain_env.execute(query=query)
        assert set(result.data[0]) >= {LONG_NAME, LONG_EMAIL}
        # ...while the SQL that actually ran carries the fitted form.
        assert LONG_EMAIL not in result.sql

    async def test_cached_execution_round_trip(self, chain_env) -> None:
        """The cache stores the DECODED response and re-keys on the fitted
        SQL; a hit must return canonical keys too."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.email")],
            measures=[{"formula": "*:count"}],
        )
        first = await chain_env.execute(query=query, cache=True)
        second = await chain_env.execute(query=query, cache=True)
        # Canonical on BOTH the fresh result and the cache hit — proving the
        # decode runs before storage, not only on the way out.
        assert LONG_EMAIL in first.data[0]
        assert LONG_EMAIL in second.data[0]
        assert first.data == second.data

    async def test_deep_cross_model_measure_executes(self, chain_env) -> None:
        """Surface 3: the `_cm_` CTE name is `_cm_` + the dotted alias, which
        also crosses 63 bytes on this chain."""
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": f"{DEEP}.lifetimeValue:sum"}],
        )
        result = await chain_env.execute(query=query)
        # The construct under test must actually be present...
        assert "_cm_" in result.sql, "no cross-model CTE generated; test is vacuous"
        for name in re.findall(r"\b_cm_\w+", result.sql):
            assert len(name.encode()) <= 63, f"{name!r} exceeds the Postgres limit"
        # ...and the value must be right, not merely non-empty.
        alias = f"SandboxInvoiceV2.{DEEP}.lifetimeValue_sum"
        assert len(result.data) == 1
        assert float(result.data[0][alias]) == 30.0

    async def test_nested_query_backed_model_executes(self, chain_env) -> None:
        """Surface 4: the virtual-model short names are emitted as output
        column aliases and referenced by the outer stage. Mixed-case shorts
        additionally exercise the case-folding regression."""
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
