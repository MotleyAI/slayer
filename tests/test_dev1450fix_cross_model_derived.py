"""Cross-dataset derived-field completeness.

Extends the #4a/#4b ``FieldSqlKey`` support to the cross-dataset CTE path:

* Codex C1 — a cross-dataset aggregate target whose ``SlayerDataset.filters``
  references a derived field expands it (not a bogus ``target.derived``).
* Codex C2 — a query filter on a joined derived field routed into the
  target CTE renders the expanded predicate.
* CR[7] — ``agg_kwarg_canonical_str`` canonicalizes a ``FieldSqlKey``
  arg/kwarg instead of raising ``TypeError``.
* CR[9] — typed re-rooting re-roots field-valued kwargs (strips the
  agg-source/target prefix) instead of dropping the path.
* CR[11] — aggregate metadata for a derived source resolves via ``src.dataset``.
"""

from __future__ import annotations

import os
from slayer.storage.sqlite_conn import transaction
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import FieldSqlKey
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.refs import agg_kwarg_canonical_str
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Unit: CR[7] agg_kwarg_canonical_str handles FieldSqlKey
# ---------------------------------------------------------------------------


def test_agg_kwarg_canonical_str_columnsqlkey_local():
    v = FieldSqlKey(path=(), dataset="customers", field_name="net")
    assert agg_kwarg_canonical_str(v) == "net"


def test_agg_kwarg_canonical_str_columnsqlkey_joined():
    v = FieldSqlKey(path=("customers",), dataset="customers", field_name="net")
    assert agg_kwarg_canonical_str(v) == "customers.net"




# ---------------------------------------------------------------------------
# Engine fixture: orders -> customers, customers has derived fields
# ---------------------------------------------------------------------------


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, "
            "status TEXT, revenue REAL)"
        )
        cur.executemany(
            "INSERT INTO customers VALUES (?,?,?,?)",
            [
                (1, "NA", "active", 100.0),
                (2, "NA", "inactive", 50.0),
                (3, "EU", "active", 70.0),
            ],
        )
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)"
        )
        cur.executemany(
            "INSERT INTO orders VALUES (?,?,?)",
            [(1, 1, 10.0), (2, 1, 5.0), (3, 2, 7.0), (4, 3, 3.0), (5, 3, 9.0)],
        )

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    # Customers with a derived dataset filter (is_active) AND a derived
    # field used in a query filter (big_spender).
    await storage.save_dataset(
        SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="prod",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="region", datatype=DataType.STRING),
                DatasetField(name="status", datatype=DataType.STRING),
                DatasetField(name="revenue", datatype=DataType.FLOAT, label="Revenue"),
                DatasetField(name="is_active", expression="status = 'active'", datatype=DataType.BOOLEAN),
                DatasetField(name="big_spender", expression="revenue > 60", datatype=DataType.BOOLEAN),
                DatasetField(name="rev_x2", expression="revenue * 2", datatype=DataType.FLOAT),
            ],
            filters=["customers.is_active"],
        )
    )
    await storage.save_dataset(
        SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="prod",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
            relationships=[
                DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            ],
        )
    )
    yield SlayerQueryEngine(storage=storage)


async def test_c1_target_derived_model_filter_expands(engine):
    """The cross-dataset CTE applies the target's derived dataset filter
    (is_active = status='active') — inactive customers are excluded."""
    resp = await engine.execute(
        SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["customers.region"],
            "metrics": [{"metric": "sum(customers.revenue)"}],
        })
    )
    dry = await engine.execute(
        SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["customers.region"],
            "metrics": [{"metric": "sum(customers.revenue)"}],
        }),
        dry_run=True,
    )
    assert "is_active" not in dry.sql, dry.sql
    assert "status" in dry.sql
    assert "'active'" in dry.sql
    by_region = {
        r["customers.region"]: r["customers.revenue_sum"]
        for r in resp.data
    }
    # NA: only active customer 1 (100); customer 2 (inactive, 50) excluded —
    # its orders read the filtered-out customer as NULL. EU: active customer 3 (70).
    assert by_region == pytest.approx({"NA": 100.0, "EU": 70.0, None: None})


async def test_c2_routed_query_filter_on_joined_derived_column(engine):
    """A query filter on a joined derived field (customers.big_spender)
    routes into the target CTE and renders the expanded predicate."""
    q = SlayerQuery.model_validate({
        "population": "orders",
        "dimensions": ["customers.region"],
        "metrics": [{"metric": "sum(customers.revenue)"}],
        "filter": ["customers.big_spender == 1"],
    })
    dry = await engine.execute(q, dry_run=True)
    assert "big_spender" not in dry.sql, dry.sql
    assert "revenue > 60" in dry.sql.replace('"', "")
    resp = await engine.execute(q)
    by_region = {
        r["customers.region"]: r["customers.revenue_sum"]
        for r in resp.data
    }
    # big_spender = revenue > 60 AND is_active (active): customer 1 (100, NA),
    # customer 3 (70, EU). Customer 2 (50) fails both.
    assert by_region == pytest.approx({"NA": 100.0, "EU": 70.0})


async def test_derived_column_as_cross_model_agg_source(engine: SlayerQueryEngine):
    """A DERIVED field used as a cross-dataset aggregate source
    (``sum(customers.rev_x2)`` where rev_x2 = revenue*2) re-roots and
    expands correctly in the CTE, and aliases as ``rev_x2_sum`` (not the
    star ``_sum`` form)."""
    q = SlayerQuery.model_validate({
        "population": "orders",
        "dimensions": ["customers.region"],
        "metrics": [{"metric": "sum(customers.rev_x2)"}],
    })
    dry = await engine.execute(q, dry_run=True)
    assert "customers.rev_x2_sum" in dry.columns, dry.columns
    assert "_sum" not in [c.split(".")[-1] for c in dry.columns]
    resp = await engine.execute(q)
    by_region = {
        r["customers.region"]: r["customers.rev_x2_sum"]
        for r in resp.data
    }
    # is_active filter keeps cust1 (NA, 100*2=200) and cust3 (EU, 70*2=140); the
    # inactive customer's orders read it as NULL.
    assert by_region == pytest.approx({"NA": 200.0, "EU": 140.0, None: None})


async def test_cr11_cross_model_agg_over_derived_column_metadata(engine: SlayerQueryEngine):
    """An aggregate over a derived field resolves label/format via
    src.dataset (not a path walk that could miss it)."""
    resp = await engine.execute(
        SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["customers.region"],
            "metrics": [{"metric": "sum(customers.revenue)"}],
        })
    )
    # revenue carries label "Revenue"; the cross-dataset metric inherits it.
    meta = resp.attributes.metrics.get("customers.revenue_sum")
    assert meta is not None
    assert meta.label == "Revenue"
