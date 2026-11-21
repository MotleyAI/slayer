"""SLayer and environment limits stay per-entity reports; the rest of a valid document imports."""

from pathlib import Path
from typing import Any

import pytest

from tests._osi_helpers import by_name, cols, convert, documents, ds, f, flat, m, mentions, seed_db, unconverted
from tests._ossie_validity_fixtures import ORDER_FIELDS, O2C, customers, orders, shop


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


def _mdx_metric(name: str = "mdx_only") -> dict[str, Any]:
    return {"name": name, "expression": {"dialects": [{"dialect": "MDX", "expression": "[Measures].[Amount]"},
                                                      {"dialect": "DAX", "expression": "SUM(orders[amount])"}]}}


_COMPUTED_KEY = {"name": "by_code", "from": "orders", "to": "customers", "from_columns": ["code_upper"],
                 "to_columns": ["code"]}


def test_limits_together_stay_reports(db: Path) -> None:
    doc = shop(datasets=[orders([*ORDER_FIELDS, f("code_upper", "UPPER(cust_code)"),
                                 f("total_usd", "total_cnets / 100.0")]),
                         customers([f("id", "c_custkey"), f("code", "cust_code")])],
               relationships=[_COMPUTED_KEY], metrics=[_mdx_metric()])
    result = convert(db=db, docs=documents(doc))
    assert set(by_name(result)) == {"orders", "customers"}
    assert unconverted(result, "mdx_only")
    assert mentions(result, "by_code")
    assert "total_usd" not in cols(by_name(result)["orders"])
    assert mentions(result, "total_usd")


def test_non_sql_dialects_only_name_the_dialects(db: Path) -> None:
    result = convert(db=db, docs=documents(shop(metrics=[_mdx_metric()])))
    [entry] = [e for e in result.unconverted_metrics if e.metric_name == "mdx_only"]
    assert "MDX" in entry.message and "DAX" in entry.message


@pytest.mark.parametrize("metric", ["SUM(source.quantty)", 'SUM(source."Quantity")'])
def test_source_column_the_source_lacks_is_unconverted(db: Path, metric: str) -> None:
    doc = shop(datasets=[orders(metrics=[m("units", metric)]), customers()], metrics=[])
    result = convert(db=db, docs=documents(doc))
    assert unconverted(result, "units")
    assert set(cols(by_name(result)["orders"])) == {f_["name"] for f_ in ORDER_FIELDS}


def test_ambiguous_query_label_is_reported(db: Path) -> None:
    query_ds = ds("labels", source="SELECT amount AS x, quantity AS x, o_orderkey AS id FROM orders",
                  fields=[f("id"), f("x")])
    result = convert(db=db, docs=documents(flat([query_ds])))
    assert "x" not in cols(by_name(result)["labels"])
    assert "id" in cols(by_name(result)["labels"])
    assert mentions(result, "x")


def test_dataset_name_slayer_cannot_store_is_reported(db: Path) -> None:
    doc = shop(datasets=[orders(), customers(), ds("shop/orders", source="orders", fields=[f("amount")])])
    result = convert(db=db, docs=documents(doc))
    assert set(by_name(result)) == {"orders", "customers"}
    assert mentions(result, "shop/orders")


def test_second_relationship_to_one_target_is_reported(db: Path) -> None:
    second = {**O2C, "name": "o2c_again"}
    result = convert(db=db, docs=documents(shop(relationships=[O2C, second])))
    assert len(by_name(result)["orders"].relationships) == 1
    assert mentions(result, "o2c_again")


def test_metric_without_a_join_path_is_unconverted(db: Path) -> None:
    visits = ds("visits", fields=[f("visit_id"), f("status")])
    doc = shop(datasets=[orders(), customers(), visits],
               metrics=[m("total_amount", "SUM(orders.amount)"), m("mixed", "SUM(orders.amount * visits.visit_id)")])
    result = convert(db=db, docs=documents(doc))
    assert unconverted(result, "mixed")
    assert not unconverted(result, "total_amount")


def test_malformed_slayer_payload_is_reported(db: Path) -> None:
    doc = shop(datasets=[orders(nonportable_extensions=[{"vendor_name": "SLAYER", "data": {"bogus": 1}}]),
                         customers()])
    result = convert(db=db, docs=documents(doc))
    assert "orders" in by_name(result)
    assert [e for e in mentions(result, "SLAYER") if e.dataset_name == "orders"]


def test_unreachable_source_is_reported(db: Path) -> None:
    doc = shop(datasets=[orders(), customers(), ds("ghost", source="no_such_table", fields=[f("amount")])])
    result = convert(db=db, docs=documents(doc))
    assert set(by_name(result)) == {"orders", "customers"}
    assert mentions(result, "ghost")
