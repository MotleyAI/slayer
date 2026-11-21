"""References resolve as Ossie identifiers: normalized-form matching, declared spellings stored."""

from pathlib import Path

import pytest

from slayer.core.syntax import AggCall, Ref, parse_expr
from slayer.ingest_report import ConversionResult
from slayer.osi.parser import parse_osi_path
from tests._osi_helpers import by_name, cols, convert, documents, f, m, mentions, metric_home, rel, seed_db, write_yaml
from tests._ossie_validity_fixtures import (
    ENTRIES, ORDER_FIELDS, Entry, assert_violation, customers, orders, refuse, shop,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


def _aggregated_ref(result: ConversionResult, metric: str) -> str:
    text = next(x.text for x in metric_home(result, metric).metrics if x.name == metric)
    parsed = parse_expr(text)
    assert isinstance(parsed, AggCall) and isinstance(parsed.source, Ref)
    return parsed.source.name


def _amount_doc(metric_sql: str, *, field: str = "Amount") -> dict:
    fields = [f("order_id", "o_orderkey"), f("customer_id", "o_custkey"), f(field, "amount")]
    return shop(datasets=[orders(fields, metrics=[m("total", metric_sql)]), customers()], metrics=[])


def test_key_and_relationship_names_in_another_case_store_the_declared_spelling(db: Path) -> None:
    relationship = rel("o2c", src="Orders", to="CUSTOMERS", from_columns=["CUSTOMER_ID"], to_columns=["Id"])
    doc = shop(datasets=[orders(primary_key=["Order_ID"]), customers(unique_keys=[["SEGMENT"]])],
               relationships=[relationship])
    models = by_name(convert(db=db, docs=documents(doc)))
    assert [c.name for c in models["orders"].fields if c.primary_key] == ["order_id"]
    [join] = models["orders"].relationships
    assert join.to == "customers"
    assert join.column_pairs == [("customer_id", "id")]
    assert (models["customers"].meta or {})["osi_unique_keys"] == [["segment"]]


@pytest.mark.parametrize("entry", ENTRIES)
def test_key_name_of_a_non_regular_field_matches_only_as_written(tmp_path: Path, entry: Entry) -> None:
    fields = [*ORDER_FIELDS, f("order key", "o_orderkey")]
    doc = shop(datasets=[orders(fields, primary_key=["ORDER KEY"]), customers()])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="undeclared-key-field", entity="orders",
                     mentions=("ORDER KEY",))


def test_quoted_upper_case_reference_reaches_a_regular_name(db: Path) -> None:
    result = convert(db=db, docs=documents(_amount_doc('SUM("AMOUNT")')))
    assert _aggregated_ref(result, "total") == "Amount"


def test_unquoted_reference_in_another_case_reaches_the_declared_name(db: Path) -> None:
    result = convert(db=db, docs=documents(_amount_doc("SUM(Amount)", field="amount")))
    assert _aggregated_ref(result, "total") == "amount"


@pytest.mark.parametrize("quoted", ['"Amount"', '"amount"'])
@pytest.mark.parametrize("entry", ENTRIES)
def test_quoted_reference_not_in_normalized_form_is_refused(tmp_path: Path, entry: Entry, quoted: str) -> None:
    error = refuse(_amount_doc(f"SUM({quoted})"), entry=entry, directory=tmp_path)
    assert_violation(error, rule="metric-scope", entity="total", mentions=(quoted,))


def test_names_that_are_not_regular_identifiers_pass(db: Path, tmp_path: Path) -> None:
    names = ["order id", 'say "hi"', "größe"]
    doc = shop(datasets=[orders([*ORDER_FIELDS, *(f(n, "status") for n in names), f("a.b", "status")]),
                         customers()])
    result = convert(db=db, docs=parse_osi_path(write_yaml(tmp_path / "shop.yaml", doc)))
    assert set(names) <= set(cols(by_name(result)["orders"]))
    assert mentions(result, "a.b")
