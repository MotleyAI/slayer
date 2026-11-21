"""Field and metric expressions reference only what their scope allows (`field-scope`, `metric-scope`)."""

from pathlib import Path

import pytest

from tests._osi_helpers import by_name, cols, convert, documents, ds, f, flat, m, mentions, scalar, seed_db
from tests._ossie_validity_fixtures import (
    ENTRIES, ORDER_FIELDS, Entry, assert_violation, customers, orders, refuse, shop,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


# ─────────────────────────── field-scope ───────────────────────────────────

@pytest.mark.parametrize("qualifier", ["customers", "CUSTOMERS"])
@pytest.mark.parametrize("entry", ENTRIES)
def test_field_reading_another_datasets_column_is_refused(tmp_path: Path, entry: Entry, qualifier: str) -> None:
    doc = shop(datasets=[orders([*ORDER_FIELDS, f("seg", f"UPPER({qualifier}.segment)")]), customers()])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="field-scope", entity="seg",
                     mentions=(f"{qualifier}.segment",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_field_reading_another_datasets_field_is_refused(tmp_path: Path, entry: Entry) -> None:
    other = ds("Customers", source="customers", fields=[f("id", "c_custkey"), f("Region", "segment")])
    doc = shop(datasets=[orders([*ORDER_FIELDS, f("region", "UPPER(Customers.Region)")]), other],
               relationships=[], metrics=[])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="field-scope", entity="region",
                     mentions=("Customers.Region",))


async def test_qualifier_naming_the_own_table_source_reads_the_column(db: Path) -> None:
    big = ds("big", source="orders", fields=[f("order_id", "o_orderkey"), f("doubled", "orders.amount * 2")])
    result = convert(db=db, docs=documents(shop(datasets=[orders(), customers(), big])))
    assert "doubled" in cols(by_name(result)["big"])
    assert float(await scalar(result=result, db=db, dataset="big", expression="sum(doubled)")) == pytest.approx(44)


async def test_own_dataset_qualifier_reads_the_source_column(db: Path) -> None:
    result = convert(db=db, docs=documents(shop(datasets=[
        orders([*ORDER_FIELDS, f("doubled", "orders.amount * 2")]), customers()])))
    assert "doubled" in cols(by_name(result)["orders"])
    assert float(await scalar(result=result, db=db, dataset="orders", expression="sum(doubled)")) == pytest.approx(44)


def test_unknown_qualifier_is_reported_and_skipped(db: Path) -> None:
    result = convert(db=db, docs=documents(shop(datasets=[
        orders([*ORDER_FIELDS, f("t_amount", "t.amount")]), customers()])))
    model = by_name(result)["orders"]
    assert "t_amount" not in cols(model)
    assert {"order_id", "amount"} <= set(cols(model))
    assert mentions(result, "t_amount")


# ─────────────────────────── metric-scope ──────────────────────────────────

@pytest.mark.parametrize("entry", ENTRIES)
def test_unqualified_reference_in_a_model_metric_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(metrics=[m("total", "SUM(amount)")])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="total",
                     mentions=("amount",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_source_column_in_a_model_metric_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders([f("order_id", "o_orderkey"), f("customer_id", "o_custkey"),
                                 f("total_amount", "o_totalprice")]), customers()],
               metrics=[m("revenue", "SUM(orders.o_totalprice)")])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="revenue",
                     mentions=("orders.o_totalprice",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_unqualified_source_column_in_a_dataset_metric_is_refused(tmp_path: Path, entry: Entry) -> None:
    fields = [f("order_id", "o_orderkey"), f("customer_id", "o_custkey"), f("total_amount", "o_totalprice")]
    doc = shop(datasets=[orders(fields, metrics=[m("revenue", "SUM(o_totalprice)")]), customers()], metrics=[])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="revenue",
                     mentions=("o_totalprice",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_foreign_reference_in_a_dataset_metric_is_refused(tmp_path: Path, entry: Entry) -> None:
    metric = m("per_customer", "SUM(net_amount) / COUNT(DISTINCT customers.id)")
    doc = shop(datasets=[orders([*ORDER_FIELDS, f("net_amount", "amount")], metrics=[metric]), customers()],
               metrics=[])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="per_customer",
                     mentions=("customers.id",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_reference_to_another_metric_is_refused(tmp_path: Path, entry: Entry) -> None:
    metrics = [m("revenue", "SUM(amount)"), m("doubled", "revenue * 2")]
    doc = shop(datasets=[orders(metrics=metrics), customers()], metrics=[])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="doubled",
                     mentions=("revenue",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_three_part_reference_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(metrics=[m("total", "SUM(shop.orders.amount)")])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="total",
                     mentions=("orders.amount",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_field_named_source_takes_precedence_and_is_refused(tmp_path: Path, entry: Entry) -> None:
    metrics = [m("units", "SUM(source.quantity)")]
    doc = shop(datasets=[orders([*ORDER_FIELDS, f("source", "status")], metrics=metrics), customers()], metrics=[])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="units",
                     mentions=("source",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_source_qualifier_in_a_model_metric_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(metrics=[m("units", "SUM(source.quantity)")])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="units",
                     mentions=("source.quantity",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_scope_is_checked_on_a_parseable_vendor_form(tmp_path: Path, entry: Entry) -> None:
    metric = {"name": "total", "expression": {"dialects": [{"dialect": "SNOWFLAKE", "expression": "SUM(amount)"}]}}
    doc = shop(metrics=[metric])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="metric-scope", entity="total",
                     mentions=("amount",))


def test_unparseable_vendor_form_is_not_scope_checked(db: Path) -> None:
    metric = {"name": "total", "expression": {"dialects": [{"dialect": "SNOWFLAKE", "expression": "SUM(amount +* 2)"}]}}
    result = convert(db=db, docs=documents(flat([orders(), customers()], metrics=[metric])))
    assert mentions(result, "total")
