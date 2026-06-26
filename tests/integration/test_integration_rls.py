"""Integration tests for forced-filter RLS (DEV-1578) — end-to-end against a
real SQLite database with two tenants.

Verifies that ``SlayerQueryEngine(storage, policy=...)`` silently scopes every
query — base, joins, profiling/sample data, dry-run preview — to the
configured tenant, that the column-presence probe is cached, and that the
``block`` / ``pass`` semantics behave on a tenant-less (shared) table.

Run with: poetry run pytest tests/integration/test_integration_rls.py -m integration
"""

import sqlite3

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ForcedFilterError
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.policy import ColumnFilterRule, SessionPolicy
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.profiling import profile_column
from slayer.engine.query_engine import SlayerQueryEngine

pytestmark = pytest.mark.integration

ORG_A = "orgA"
ORG_B = "orgB"


@pytest.fixture
async def rls_storage(tmp_path):
    """Two-tenant SQLite DB + YAML storage with orders / customers (both
    org-scoped) and a tenant-less exchange_rates table."""
    db_path = tmp_path / "rls.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            organization_uuid TEXT NOT NULL,
            name TEXT NOT NULL,
            region TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            organization_uuid TEXT NOT NULL,
            amount REAL NOT NULL,
            customer_id INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE exchange_rates (
            day TEXT NOT NULL,
            rate REAL NOT NULL
        )
        """
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?, ?, ?, ?)",
        [
            (1, ORG_A, "Alice", "US"),
            (2, ORG_A, "Bob", "EU"),
            (3, ORG_B, "Charlie", "APAC"),
        ],
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (1, ORG_A, 100.0, 1),
            (2, ORG_A, 200.0, 2),
            (3, ORG_B, 999.0, 3),
        ],
    )
    cur.executemany(
        "INSERT INTO exchange_rates VALUES (?, ?)",
        [("2025-01-01", 1.1), ("2025-02-01", 1.2)],
    )
    conn.commit()
    conn.close()

    from slayer.storage.yaml_storage import YAMLStorage

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))

    await storage.save_datasource(
        DatasourceConfig(name="rls_sqlite", type="sqlite", database=str(db_path))
    )

    await storage.save_model(
        SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="rls_sqlite",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="organization_uuid", sql="organization_uuid", type=DataType.TEXT),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="rls_sqlite",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="organization_uuid", sql="organization_uuid", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="customer_id", sql="customer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="exchange_rates",
            sql_table="exchange_rates",
            data_source="rls_sqlite",
            columns=[
                Column(name="day", sql="day", type=DataType.TEXT),
                Column(name="rate", sql="rate", type=DataType.DOUBLE),
            ],
        )
    )
    return storage


def _org_policy(org=ORG_A, **kw):
    return SessionPolicy(
        data_filters=[ColumnFilterRule(column="organization_uuid", value=org, **kw)]
    )


# -- base scoping ------------------------------------------------------------


async def test_no_policy_sees_all_orgs(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage)
    resp = await engine.execute(
        SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="*:count")])
    )
    assert resp.data[0]["orders._count"] == 3


async def test_policy_scopes_count_to_org(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    resp = await engine.execute(
        SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="*:count")])
    )
    assert resp.data[0]["orders._count"] == 2  # only orgA's two orders


async def test_policy_scopes_sum_to_org(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_B))
    resp = await engine.execute(
        SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="amount:sum")])
    )
    assert resp.data[0]["orders.amount_sum"] == pytest.approx(999.0)  # only orgB


# -- joins scoped on both sides ---------------------------------------------


async def test_join_scoped_both_sides(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    resp = await engine.execute(
        SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="customers.region")],
        )
    )
    regions = {row["orders.customers.region"] for row in resp.data}
    assert regions == {"US", "EU"}  # APAC belongs to orgB, must not appear
    assert "APAC" not in regions


# -- block / pass on tenant-less table --------------------------------------


async def test_block_fails_on_columnless_table(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    with pytest.raises(ForcedFilterError) as exc:
        await engine.execute(
            SlayerQuery(
                source_model="exchange_rates",
                measures=[ModelMeasure(formula="*:count")],
            )
        )
    assert exc.value.table == "exchange_rates"
    assert exc.value.column == "organization_uuid"


async def test_pass_allows_columnless_table(rls_storage):
    engine = SlayerQueryEngine(
        storage=rls_storage, policy=_org_policy(ORG_A, on_unapplicable="pass")
    )
    resp = await engine.execute(
        SlayerQuery(
            source_model="exchange_rates",
            measures=[ModelMeasure(formula="*:count")],
        )
    )
    assert resp.data[0]["exchange_rates._count"] == 2  # unfiltered, both rows


# -- profiling / sample data scoped -----------------------------------------


async def test_profiling_sample_values_org_scoped(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    customers = await rls_storage.get_model("customers", data_source="rls_sqlite")
    region_col = next(c for c in customers.columns if c.name == "region")
    sample = await profile_column(model=customers, column=region_col, engine=engine)
    assert sample is not None
    assert set(sample.sampled_values) == {"US", "EU"}  # APAC (orgB) excluded


# -- dry_run preview shows the wraps ----------------------------------------


async def test_dry_run_sql_contains_wraps(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    resp = await engine.execute(
        SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="*:count")]),
        dry_run=True,
    )
    flat = resp.sql.replace("\n", " ")
    assert "organization_uuid = 'orgA'" in flat
    assert (
        "FROM (SELECT * FROM orders WHERE organization_uuid = 'orgA') AS orders"
        in flat
    )


async def test_explain_sql_contains_wraps(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    resp = await engine.execute(
        SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="*:count")]),
        explain=True,
    )
    # explain returns the rewritten SQL it planned, so the wrap is visible
    assert "organization_uuid = 'orgA'" in resp.sql


# -- get_column_types under policy -------------------------------------------


async def test_get_column_types_scoped_ok(rls_storage):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    types = await engine.get_column_types(model_name="orders", data_source="rls_sqlite")
    assert isinstance(types, dict)  # probe succeeds against the wrapped SQL


async def test_get_column_types_blocking_policy_degrades(rls_storage):
    """A blocking policy on a tenant-less model must not crash the type probe;
    it degrades to {} (the rewrite raises inside the existing try/except)."""
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    types = await engine.get_column_types(
        model_name="exchange_rates", data_source="rls_sqlite"
    )
    assert types == {}


# -- None presence re-probes (don't cache None) ------------------------------


async def test_none_presence_reprobes_and_self_heals(rls_storage, monkeypatch):
    """A transient introspection failure (-> None -> fail closed) is not
    cached: once introspection recovers, the next query succeeds."""
    import slayer.engine.query_engine as qe

    real = qe._safe_get_columns
    state = {"fail": True}

    def flaky(*args, **kwargs):
        if state["fail"]:
            raise RuntimeError("transient introspection failure")
        return real(*args, **kwargs)

    monkeypatch.setattr(qe, "_safe_get_columns", flaky)
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))
    query = SlayerQuery(
        source_model="orders", measures=[ModelMeasure(formula="*:count")]
    )

    with pytest.raises(ForcedFilterError):
        await engine.execute(query)  # introspection failed -> fail closed

    state["fail"] = False
    resp = await engine.execute(query)  # recovered -> re-probed -> scoped result
    assert resp.data[0]["orders._count"] == 2


# -- column-presence cache ---------------------------------------------------


async def test_column_presence_is_cached(rls_storage, monkeypatch):
    engine = SlayerQueryEngine(storage=rls_storage, policy=_org_policy(ORG_A))

    import slayer.engine.query_engine as qe

    calls = {"n": 0}
    real = qe._safe_get_columns

    def counting(*args, **kwargs):
        calls["n"] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(qe, "_safe_get_columns", counting)

    query = SlayerQuery(
        source_model="orders", measures=[ModelMeasure(formula="*:count")]
    )
    await engine.execute(query)
    after_first = calls["n"]
    assert after_first >= 1  # orders table introspected at least once

    await engine.execute(query)
    assert calls["n"] == after_first  # second run served entirely from cache
