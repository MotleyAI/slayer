"""Ossie export: dataset sources, fields, datatypes and time role, keys."""

from pathlib import Path
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column
from slayer.osi.source import parse_source
from slayer.storage.yaml_storage import YAMLStorage
from tests._osi_export_fixtures import (
    BASIC_MEASURES, col, customers, monthly_revenue, orders, orders_columns, rich_orders, visits,
)
from tests._osi_export_helpers import (
    norm_value, dataset, dataset_names, eval_field, export, export_models, expression_sql, field, make_env,
    mentions, reimport, rows, run_sql, same_sql, save,
)
from tests._osi_helpers import ds, f, flat


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


def _values(data: list[Any]) -> list[list[str]]:
    return sorted(sorted(repr(norm_value(v)) for v in row) for row in data)


# ─────────────────────────────── sources ───────────────────────────────────

async def test_table_model_source_is_its_table(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    parsed = parse_source(dataset(result.document, "orders")["source"])
    assert not parsed.is_query
    assert parsed.table == "orders"


async def test_sql_mode_model_source_is_its_query(env) -> None:
    result = await export_models(env, visits())
    source = dataset(result.document, "visits")["source"]
    assert parse_source(source).is_query
    assert _values(run_sql(env, f"SELECT * FROM ({source}) AS t")) == _values(run_sql(env, visits().sql or ""))


async def test_filtered_table_model(env) -> None:
    active = orders(name="active_orders", columns=[*orders_columns(), col("deleted_at", DataType.TIMESTAMP)],
                    joins=[], filters=["deleted_at IS NULL", "status <> 'test'"], measures=BASIC_MEASURES)
    result = await export_models(env, active)
    ds_ = dataset(result.document, "active_orders")
    source = ds_["source"]
    assert parse_source(source).is_query
    assert sorted(r[0] for r in run_sql(env, f"SELECT o_orderkey FROM ({source}) AS t")) == [1, 2, 3]
    assert eval_field(env, ds_, "id", order_by="o_orderkey") == [1, 2, 3]


async def test_query_backed_model_source_is_its_generated_sql(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES), monthly_revenue())
    source = dataset(result.document, "monthly_revenue")["source"]
    assert parse_source(source).is_query
    assert "'US'" in source
    expected = [list(r.values()) for r in await rows(env, "monthly_revenue")]
    assert _values(run_sql(env, f"SELECT * FROM ({source}) AS t")) == _values(expected)


async def test_query_backed_model_reads_its_own_datasource(env, tmp_path: Path) -> None:
    await save(env, customers(), orders(measures=BASIC_MEASURES), monthly_revenue())
    other = (await make_env(tmp_path / "other", datasource="aaa")).model_copy(update={"store": env.store})
    storage = YAMLStorage(base_dir=str(env.store))
    await storage.save_datasource(other.config)
    await save(other, orders(sql_table=None, sql="SELECT * FROM orders WHERE amount > 1000", joins=[]))
    await storage.set_datasource_priority(["aaa", env.datasource])
    source = dataset((await export(env)).document, "monthly_revenue")["source"]
    assert "1000" not in source
    assert "'US'" in source


async def test_filter_on_joined_model_is_reported(env) -> None:
    b2b = orders(name="b2b_orders", filters=["customers.segment = 'b2b'"], measures=BASIC_MEASURES)
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES), b2b)
    assert "b2b_orders" not in dataset_names(result.document)
    [entry] = mentions(result, "b2b_orders", severity="dropped")
    assert "customers.segment = 'b2b'" in entry.message
    assert entry.suggestion


# ─────────────────────────────── fields ────────────────────────────────────

async def test_renamed_column(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    total = field(dataset(result.document, "orders"), "total_amount")
    assert total is not None
    assert same_sql(total["expression"]["sql"], "o_totalprice")


async def test_derived_column_over_derived_column_reads_only_physical_columns(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    orders_ds = dataset(result.document, "orders")
    net = field(orders_ds, "net")
    assert net is not None
    tree = sqlglot.parse_one(expression_sql(net, target="duckdb"), read="duckdb")
    assert {c.name for c in tree.find_all(exp.Column)} == {"price", "quantity", "discount"}
    assert eval_field(env, orders_ds, "net", order_by="o_orderkey") == pytest.approx([10, 4, 8.5, 4, 18])


async def test_filtered_column_is_masked(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    values = eval_field(env, dataset(result.document, "orders"), "completed_amount", order_by="o_orderkey")
    assert values[:3] == [10, None, 7]
    assert values == [10, None, 7, None, 20]


async def test_label_and_description(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    amount = field(dataset(result.document, "orders"), "amount")
    assert amount is not None
    assert amount["label"] == "Order amount"
    assert amount["description"] == "Gross order value"


async def test_hidden_column_is_a_field(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    assert field(dataset(result.document, "orders"), "note") is not None


async def test_imported_description_is_stable(env) -> None:
    context = {"instructions": "Gross value in USD.", "synonyms": ["revenue", "gross"]}
    doc = flat([ds("orders", fields=[f("id", "o_orderkey"),
                                     f("amount", description="Order amount", ai_context=context)],
                   primary_key=["id"])])
    imported, _ = await reimport(env, doc)
    amount = field(dataset((await export(imported)).document, "orders"), "amount")
    assert amount is not None
    assert amount["description"] == "Order amount"
    assert amount["ai_context"] == context


# ─────────────────────────── datatypes and time role ───────────────────────

_DATATYPES = [
    pytest.param("status", DataType.TEXT, None, "String", id="text"),
    pytest.param("quantity", DataType.INT, None, "Integer", id="int"),
    pytest.param("amount", DataType.DOUBLE, None, "Float", id="double"),
    pytest.param("o_totalprice", DataType.DOUBLE, "DECIMAL(18,2)", "Decimal", id="decimal"),
    pytest.param("o_totalprice", DataType.DOUBLE, "NUMERIC(10, 2)", "Decimal", id="numeric"),
    pytest.param("is_paid", DataType.BOOLEAN, None, "Boolean", id="boolean"),
    pytest.param("ordered_on", DataType.DATE, None, "Date", id="date"),
    pytest.param("ordered_at", DataType.TIMESTAMP, None, "DateTime", id="timestamp"),
    pytest.param("ordered_at", DataType.TIMESTAMP, "TIMESTAMP WITH TIME ZONE", "DateTimeTz", id="timestamptz"),
    pytest.param("ordered_at", DataType.TIMESTAMP, "TIMESTAMPTZ", "DateTimeTz", id="timestamptz-short"),
    pytest.param("note", DataType.UNKNOWN, "JSON", None, id="unknown"),
]


def _one_column_orders(column: Column) -> Any:
    return orders(columns=[col("id", DataType.INT, "o_orderkey", primary_key=True), column], joins=[],
                  measures=[], default_time_dimension=None)


@pytest.mark.parametrize(("physical", "type_", "db_type", "datatype"), _DATATYPES)
async def test_field_datatype(env, physical: str, type_: DataType, db_type: str | None,
                              datatype: str | None) -> None:
    column = col("v", type_, physical, db_type=db_type)
    result = await export_models(env, _one_column_orders(column))
    v = field(dataset(result.document, "orders"), "v")
    assert v is not None
    assert v.get("datatype") == datatype


async def test_recorded_ossie_datatype_used_when_consistent(env) -> None:
    column = col("v", DataType.TIMESTAMP, "ordered_at", meta={"osi_datatype": "DateTimeTz"})
    v = field(dataset((await export_models(env, _one_column_orders(column))).document, "orders"), "v")
    assert v is not None
    assert v["datatype"] == "DateTimeTz"


async def test_recorded_ossie_datatype_ignored_when_inconsistent(env) -> None:
    column = col("v", DataType.INT, "quantity", meta={"osi_datatype": "String"})
    v = field(dataset((await export_models(env, _one_column_orders(column))).document, "orders"), "v")
    assert v is not None
    assert v["datatype"] == "Integer"


async def test_temporal_column_has_no_explicit_time_role(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    ordered_on = field(dataset(result.document, "orders"), "ordered_on")
    assert ordered_on is not None
    assert "dimension" not in ordered_on


@pytest.mark.parametrize(("physical", "datatype", "is_time"), [
    pytest.param("ordered_at", "DateTime", False, id="temporal-opt-out"),
    pytest.param("order_year", "Integer", True, id="integer-year"),
])
async def test_explicit_time_role_survives(env, physical: str, datatype: str, is_time: bool) -> None:
    doc = flat([ds("orders", fields=[f("id", "o_orderkey"),
                                     f("v", physical, datatype=datatype, dimension={"is_time": is_time})],
                   primary_key=["id"])])
    imported, _ = await reimport(env, doc)
    v = field(dataset((await export(imported)).document, "orders"), "v")
    assert v is not None
    assert v["dimension"] == {"is_time": is_time}


# ─────────────────────────────── keys ──────────────────────────────────────

async def test_primary_and_unique_keys(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    cust = dataset(result.document, "customers")
    assert cust["primary_key"] == ["id"]
    assert ["email"] in cust["unique_keys"]
    assert dataset(result.document, "orders")["primary_key"] == ["id"]


async def test_composite_primary_key(env) -> None:
    model = orders(columns=[col("id", DataType.INT, "o_orderkey", primary_key=True),
                            col("customer_id", DataType.INT, "o_custkey", primary_key=True), col("amount")],
                   joins=[], default_time_dimension=None)
    assert dataset((await export_models(env, model)).document, "orders")["primary_key"] == ["id", "customer_id"]


async def test_unique_keys_deduplicated(env) -> None:
    model = customers(meta={"osi_unique_keys": [["email"], ["segment", "email"], ["segment", "email"]]})
    keys = dataset((await export_models(env, model)).document, "customers")["unique_keys"]
    assert sorted(keys) == [["email"], ["segment", "email"]]


async def test_key_on_filtered_column_is_reported(env) -> None:
    model = customers(meta={"osi_unique_keys": [["masked_email"]]})
    model.columns.append(col("masked_email", DataType.TEXT, "email", filter="segment = 'b2b'"))
    result = await export_models(env, model)
    cust = dataset(result.document, "customers")
    assert ["masked_email"] not in cust.get("unique_keys", [])
    assert field(cust, "masked_email") is not None
    assert mentions(result, "masked_email", severity="dropped")


async def test_key_on_slayer_only_column_is_reported(env) -> None:
    model = orders(measures=BASIC_MEASURES, meta={"osi_unique_keys": [["id", "segment"]]})
    model.columns.append(col("segment", DataType.TEXT, "customers.segment"))
    result = await export_models(env, customers(), model)
    assert ["id", "segment"] not in dataset(result.document, "orders").get("unique_keys", [])
    assert mentions(result, "segment", severity="dropped")
