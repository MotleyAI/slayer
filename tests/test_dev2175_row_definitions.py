"""Field filters, dataset filters and relationship keys under the row-definition name rule, in the ``expression`` spelling.

Specs: models/column-filters, models/model-filters, models/join-keys.
"""

from __future__ import annotations

import os
import re
import tempfile
from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.errors import DatasetFieldCycleError, RelationshipKeyError, SlayerError
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema import ingestion
from slayer.live_schema.schema_drift import LiveTable, diff_sql_table_dataset
from slayer.live_schema.type_refinement import refine_dict_with_live_schema
from slayer.storage.sqlite_conn import transaction
from tests._dev1902_fixtures import (
    AMOUNT_BY_CUSTOMER,
    AMOUNT_BY_REGION,
    AMOUNT_BY_TIER,
    ASSOC_CREDIT_BY_STATUS,
    CREDIT_WITH_NEW_ORDER,
    cells,
    make_exec_engine,
    warnings_of,
)
from tests._dev2171_fixtures import import_cube
from tests._dev2175_fixtures import (
    BACKENDS,
    assert_refused,
    by_id,
    customers_doc,
    dataset,
    dumped_field,
    engine_with,
    entry,
    fld,
    ids,
    metric_value,
    orders_doc,
    to_customers,
)

BACKEND = pytest.mark.parametrize("dialect", BACKENDS)
PAID_AMOUNT = fld("paid_amount", "amount", filter="status = 'paid'", datatype="Integer")


# ── field filters ─────────────────────────────────────────────────────────

class TestFieldFilter:
    @BACKEND
    async def test_single_column_aggregate_sql_unchanged(self, dialect: str) -> None:
        async with engine_with(dialect=dialect, docs=[orders_doc(fld("status"), PAID_AMOUNT)]) as (engine, _, _):
            dry = await engine.execute({"population": "orders", "metrics": [{"metric": "sum(orders.paid_amount)"}]},
                                       dry_run=True)
            assert re.search(r"SUM\(CASE WHEN .*status = 'paid' THEN .*amount END\)", dry.sql or "", re.I), dry.sql
            assert await metric_value(engine, metric="sum(orders.paid_amount)") == 30

    @BACKEND
    async def test_derived_field_over_a_filtered_field_expands_the_wrapped_value(self, dialect: str) -> None:
        doc = orders_doc(fld("status"), PAID_AMOUNT, fld("q_double", "orders.paid_amount * 2"))
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="q_double") == [20, 40, None]

    @BACKEND
    async def test_a_bare_own_name_in_a_filter_reads_the_source_column(self, dialect: str) -> None:
        doc = orders_doc(fld("amount", filter="amount > 5"), fld("der", "amount", filter="status = 'paid'"))
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="amount") == [10, 20, None]
            assert await by_id(engine, field="der") == [10, 20, None]

    async def test_a_filter_naming_another_derived_field_by_path_is_an_edge(self) -> None:
        doc = orders_doc(fld("amount"), fld("der", "amount * 2"), fld("der2", "amount", filter="orders.der > 30"))
        async with engine_with(dialect="duckdb", docs=[doc]) as (engine, _, _):
            assert await by_id(engine, field="der2") == [None, 20, None]

    async def test_a_filter_naming_its_own_field_by_path_is_a_cycle(self) -> None:
        ds = dataset(orders_doc(fld("amount"), fld("loop", "amount", filter="orders.loop > 0")))
        async with engine_with(dialect="duckdb") as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(ds)
        assert_refused(caught.value, "loop", kind=DatasetFieldCycleError)

    async def test_variables_substitute_into_the_dialect_entry_and_the_filter(self) -> None:
        vcol = fld("vcol", {"dialects": [entry("DUCKDB", "amount * {mult}")]}, filter="status = '{s}'", datatype="Integer")
        async with engine_with(dialect="duckdb", docs=[orders_doc(fld("status"), vcol)]) as (engine, _, _):
            dry = await engine.execute({"population": "orders", "metrics": [{"metric": "sum(orders.vcol)"}],
                                        "variables": {"mult": 2, "s": "paid"}}, dry_run=True)
            assert "{" not in (dry.sql or "")
            assert "'paid'" in (dry.sql or "")
            assert re.search(r"amount\s*\*\s*2", dry.sql or ""), dry.sql

    async def test_a_broken_filter_path_fails_at_save(self) -> None:
        ds = dataset(orders_doc(fld("amount"), fld("bad", "amount", filter="ghost.col = 1")))
        async with engine_with(dialect="duckdb") as (engine, _, _):
            with pytest.raises(SlayerError) as caught:
                await engine.save_dataset(ds)
        assert_refused(caught.value, "ghost")


# ── dataset filters ───────────────────────────────────────────────────────

class TestDatasetFilter:
    @BACKEND
    async def test_a_filter_past_a_renamed_field(self, dialect: str) -> None:
        doc = orders_doc(fld("status", "state"), filters=["status = 'paid'"])
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await ids(engine) == [1, 2]

    @BACKEND
    async def test_a_filter_reading_a_field_by_path(self, dialect: str) -> None:
        doc = orders_doc(fld("status", "state"), filters=["orders.status = 'paid'"])
        async with engine_with(dialect=dialect, docs=[doc]) as (engine, _, _):
            assert await ids(engine) == [2]


# ── relationship keys over renamed readers ──────────────────────────────────

def _graph() -> list[SlayerDataset]:
    """``orders → customers → regions`` with every key a renamed reader in the ``expression`` spelling."""
    regions = {"name": "regions", "data_source": "test", "sql_table": "regions", "fields": [
        fld("id", "region_pk", datatype="Integer", primary_key=True), fld("name")]}
    customers = {"name": "customers", "data_source": "test", "sql_table": "customers", "fields": [
        fld("id", "customer_pk", datatype="Integer", primary_key=True), fld("name"),
        fld("region_id", "region_fk", datatype="Integer"), fld("tier"), fld("credit", datatype="Float")],
        "relationships": [{"to": "regions", "from_columns": ["region_id"], "to_columns": ["id"]}]}
    orders = {"name": "orders", "data_source": "test", "sql_table": "orders", "fields": [
        fld("id", "order_pk", datatype="Integer", primary_key=True), fld("customer_id", "cust_fk", datatype="Integer"),
        fld("amount", datatype="Float"), fld("status")],
        "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}]}
    return [dataset(orders), dataset(customers), dataset(regions)]


@pytest.fixture(params=list(BACKENDS))
async def renamed(request) -> AsyncIterator[SlayerQueryEngine]:
    async for engine in make_exec_engine(request, datasets=_graph()):
        yield engine


def _amount_by(dimension: str) -> dict[str, Any]:
    return {"population": "orders", "dimensions": [dimension], "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}]}


class TestRenamedReaderKeys:
    async def test_a_renamed_source_foreign_key_executes(self, renamed: SlayerQueryEngine) -> None:
        resp = await renamed.execute(_amount_by("customers.name"))
        assert cells(resp, dim_suffix="customers.name", metric="amt") == AMOUNT_BY_CUSTOMER
        assert "cust_fk" in (resp.sql or "")

    async def test_a_renamed_target_primary_key_proves_the_hop(self, renamed: SlayerQueryEngine) -> None:
        resp = await renamed.execute(_amount_by("customers.tier"))
        assert cells(resp, dim_suffix="customers.tier", metric="amt") == AMOUNT_BY_TIER
        assert warnings_of(resp, "broadcast") == []

    async def test_renames_on_both_sides_across_two_hops(self, renamed: SlayerQueryEngine) -> None:
        resp = await renamed.execute(_amount_by("customers.regions.name"))
        assert cells(resp, dim_suffix="regions.name", metric="amt") == AMOUNT_BY_REGION

    async def test_association_over_a_renamed_primary_key_root(self, renamed: SlayerQueryEngine) -> None:
        resp = await renamed.execute({"population": "orders", "dimensions": ["orders.status"],
                                      "metrics": [{"metric": "sum(customers.credit)", "alias": "cr"}],
                                      "to_many_handling": "associate"})
        assert cells(resp, dim_suffix="status", metric="cr") == ASSOC_CREDIT_BY_STATUS

    async def test_population_pushdown_over_renamed_keys(self, renamed: SlayerQueryEngine) -> None:
        resp = await renamed.execute({"population": "customers", "metrics": [{"metric": "sum(customers.credit)", "alias": "cr"}],
                                      "filter": ["orders.status = 'new'"]})
        (row,) = resp.data
        assert float(next(v for k, v in row.items() if k.endswith("cr"))) == pytest.approx(CREDIT_WITH_NEW_ORDER)
        assert "cust_fk" in (resp.sql or "")
        assert "customer_pk" in (resp.sql or "")

    async def test_cardinality_detection_profiles_renamed_keys(self, renamed: SlayerQueryEngine) -> None:
        report = await renamed.detect_relationship_cardinality(data_source="test", dataset="orders")
        (finding,) = [f for f in report.findings if f.to == "customers"]
        assert str(finding.detected) == "many_to_one" or getattr(finding.detected, "value", None) == "many_to_one"


def test_a_mixed_case_physical_key_is_a_quoted_physical_reader() -> None:
    key = DatasetField.model_validate({"name": "id", "expression": '"CustPK"'})
    assert key.is_base
    assert (key.physical_name, key.physical_quoted) == ("CustPK", True)


async def test_a_mixed_case_physical_key_is_quoted_in_the_on_clause() -> None:
    customers = customers_doc()
    customers["fields"][0] = fld("id", '"CustPK"', primary_key=True, datatype="Integer")
    orders = orders_doc(fld("customer_id", datatype="Integer"), relationships=[to_customers()])
    async with engine_with(dialect="duckdb", docs=[customers, orders], validate=False) as (engine, _, _):
        dry = await engine.execute(_amount_by("customers.name") | {"metrics": [{"metric": "count(orders.*)"}]}, dry_run=True)
    assert '"CustPK"' in (dry.sql or "")


async def test_an_unresolvable_target_key_fails_closed_at_emission() -> None:
    customers = customers_doc()
    customers["fields"][0] = fld("id", "id + 0", primary_key=True, datatype="Integer")
    orders = orders_doc(fld("customer_id", datatype="Integer"), relationships=[to_customers()])
    async with engine_with(dialect="duckdb", docs=[customers, orders], validate=False) as (engine, _, _):
        query = _amount_by("customers.name") | {"metrics": [{"metric": "count(orders.*)"}]}
        with pytest.raises(RelationshipKeyError):
            await engine.execute(query, dry_run=True)


# ── drift and refinement ──────────────────────────────────────────────────

def _parties(key: dict[str, Any]) -> SlayerDataset:
    return dataset({"name": "parties", "data_source": "test", "sql_table": "parties",
                    "fields": [fld("id", datatype="Integer", primary_key=True), key],
                    "relationships": [{"to": "entities", "from_columns": [key["name"]], "to_columns": ["code"]}]})


def test_a_quoted_self_name_present_in_the_live_table_is_not_drift() -> None:
    live = LiveTable(columns={"id": DataType.INTEGER, "legalEntityType": DataType.STRING})
    entry_, dropped = diff_sql_table_dataset(dataset=_parties(fld("legalEntityType", '"legalEntityType"')),
                                             live_table=live, available_datasets_in_ds={"parties", "entities"})
    assert entry_ is None
    assert dropped == set()


def test_a_quoted_base_field_is_type_refined() -> None:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "live.db")
        with transaction(db_path) as conn:
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)")
            conn.executemany("INSERT INTO items VALUES (?, ?)", [(1, 10), (2, 20)])
        doc = {"name": "items", "sql_table": "items", "data_source": "live",
               "fields": [{"name": "quantity", "expression": '"qty"', "datatype": "Float"}]}
        ds = DatasourceConfig(name="live", type="sqlite", database=db_path)
        client = SlayerSQLClient(datasource=ds)
        try:
            changed = run_sync(refine_dict_with_live_schema(doc, ds, client))
        finally:
            client.close()
            engine_factory.invalidate_engine(ds)
    assert changed is True
    assert doc["fields"][0]["datatype"] == "Integer"


# ── importers and ingestion ───────────────────────────────────────────────

def test_cube_join_keys_are_member_names() -> None:
    orders = {"name": "orders", "sql_table": "orders", "dimensions": [
        {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True},
        {"name": "customer_id", "sql": "{CUBE}.customer_id", "type": "number"}],
        "joins": [{"name": "customers", "relationship": "many_to_one",
                   "sql": "{CUBE}.customer_id = {customers.id}"}]}
    customers = {"name": "customers", "sql_table": "customers", "dimensions": [
        {"name": "id", "sql": "{CUBE}.cust_pk", "type": "number", "primary_key": True}]}
    result = import_cube([orders, customers])
    by_name = {d.name: d for d in result.datasets}
    (rel,) = by_name["orders"].relationships
    assert (rel.from_columns, rel.to_columns) == (["customer_id"], ["id"])
    assert dumped_field(by_name["customers"], "id")["expression"] == "cust_pk"


async def test_ingestion_writes_expression() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "cnt.db")
        with transaction(db_path) as conn:
            conn.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE children (id INTEGER PRIMARY KEY, _count INTEGER REFERENCES parents(id))")
        ds = DatasourceConfig(name="cnt_ds", type="sqlite", database=db_path)
        client = SlayerSQLClient(datasource=ds)
        try:
            datasets = {d.name: d for d in await ingestion.ingest_datasource_async(ds, client=client)}
        finally:
            client.close()
            engine_factory.invalidate_engine(ds)
    renamed_fk = dumped_field(datasets["children"], "count_col")
    assert renamed_fk.get("expression") == "_count"
    assert "sql" not in renamed_fk
    (rel,) = datasets["children"].relationships
    assert rel.from_columns == ["count_col"]
