"""Ossie-dialect forms follow the pinned Ossie SQL language (`ossie-sql`); vendor dialects are not judged."""

from pathlib import Path
from typing import Any

import pytest

from tests._osi_helpers import by_name, cols, convert, documents, f, f_dialects, m, mentions, seed_db, unconverted
from tests._ossie_validity_fixtures import (
    ENTRIES, ORDER_FIELDS, Entry, assert_violation, customers, orders, refuse, shop,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


def _field_doc(field: dict[str, Any]) -> dict[str, Any]:
    return shop(datasets=[orders([*ORDER_FIELDS, field]), customers()])


def _ossie_entry(name: str, text: str) -> dict[str, Any]:
    return {"name": name, "expression": {"dialects": [{"dialect": "OSSIE_SQL_2026", "expression": text}]}}


@pytest.mark.parametrize("text", [
    "amount +* 2",
    "(SELECT MAX(amount) FROM orders)",
    "WITH t AS (SELECT 1 AS x) SELECT x FROM t",
    "SELECT 1 UNION SELECT 2",
    "amount; DROP TABLE orders",
])
@pytest.mark.parametrize("entry", ENTRIES)
def test_field_outside_the_language_is_refused(tmp_path: Path, entry: Entry, text: str) -> None:
    error = refuse(_field_doc(f("bad", text)), entry=entry, directory=tmp_path)
    assert_violation(error, rule="ossie-sql", entity="bad")


@pytest.mark.parametrize("entry", ENTRIES)
def test_ossie_dialect_entry_without_sql_is_judged(tmp_path: Path, entry: Entry) -> None:
    error = refuse(_field_doc(_ossie_entry("bad", "amount +* 2")), entry=entry, directory=tmp_path)
    assert_violation(error, rule="ossie-sql", entity="bad")


@pytest.mark.parametrize("entry", ENTRIES)
def test_several_statements_in_a_metric_are_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders(metrics=[m("total", "SUM(amount); DROP TABLE orders")]), customers()], metrics=[])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="ossie-sql", entity="total")


@pytest.mark.parametrize("call", ["agg", "AGG", "Agg"])
@pytest.mark.parametrize("entry", ENTRIES)
def test_reserved_agg_is_refused_in_any_case(tmp_path: Path, entry: Entry, call: str) -> None:
    doc = shop(metrics=[m("total_amount", "SUM(orders.amount)"), m("again", f"{call}(orders.total_amount)")])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="ossie-sql", entity="again",
                     mentions=("AGG",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_filter_predicate_on_another_dataset_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(metrics=[m("b2b", "SUM(orders.amount) FILTER (WHERE customers.segment = 'b2b')")])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="ossie-sql", entity="b2b",
                     mentions=("customers.segment",))


@pytest.mark.parametrize("dialect", ["SNOWFLAKE", "ANSI_SQL"])
def test_vendor_dialect_is_not_judged(db: Path, dialect: str) -> None:
    metric = {"name": "total", "expression": {"dialects": [{"dialect": dialect, "expression": "SUM(amount); SELECT 1"}]}}
    result = convert(db=db, docs=documents(shop(datasets=[orders(metrics=[metric]), customers()], metrics=[])))
    assert unconverted(result, "total")


def test_unparseable_vendor_field_is_reported_and_the_rest_imports(db: Path) -> None:
    doc = _field_doc(f_dialects("bq", ("BIGQUERY", "amount +* 2")))
    result = convert(db=db, docs=documents(doc))
    model = by_name(result)["orders"]
    assert "bq" not in cols(model)
    assert {"order_id", "amount"} <= set(cols(model))
    assert mentions(result, "bq")


@pytest.mark.parametrize("superseded", ["amount +* 2", "amount * 2"])
def test_superseded_ossie_entry_gives_an_info_entry(db: Path, superseded: str) -> None:
    result = convert(db=db, docs=documents(_field_doc(f_dialects("ossie_twin", ("OSSIE_SQL_2026", superseded),
                                                                 sql="amount"))))
    assert "ossie_twin" in cols(by_name(result)["orders"])
    assert mentions(result, "ossie_twin", severity="info")
    assert not [e for e in mentions(result, "ossie_twin") if e.severity != "info"]


def test_superseded_ossie_entry_repeating_the_sql_reports_nothing(db: Path) -> None:
    result = convert(db=db, docs=documents(_field_doc(f_dialects("ossie_twin", ("OSSIE_SQL_2026", "amount"),
                                                                 sql="amount"))))
    assert "ossie_twin" in cols(by_name(result)["orders"])
    assert not mentions(result, "ossie_twin")
