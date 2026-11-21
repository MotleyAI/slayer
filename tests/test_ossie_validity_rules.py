"""Each document rule refuses the import, naming the file, the entity and the rule."""

import importlib
import math
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from slayer.osi.converter import OsiConversionError
from slayer.osi.parser import parse_osi_path
from tests._osi_helpers import ds, f, flat, m, rel, write_yaml
from tests._ossie_validity_fixtures import (
    ENTRIES, O2C, Entry, assert_violation, customers, hits, orders, refuse, refuse_converter, refuse_path, shop,
    write_text,
)


def test_error_and_violation_live_in_the_validator_module() -> None:
    validation = importlib.import_module("slayer.osi.validation")
    assert validation.OsiConversionError is OsiConversionError
    violation = validation.OssieViolation(file="a.yaml", document="shop", location="dataset 'orders'",
                                          rule="duplicate-name", message="repeated")
    assert issubclass(OsiConversionError, Exception)
    error = OsiConversionError(violations=[violation, violation])
    assert error.violations == [violation, violation]
    assert len(str(error).splitlines()) == 2


# ─────────────────────────── unreadable-file ───────────────────────────────

@pytest.mark.parametrize(("name", "text"), [
    ("broken.yaml", "datasets: [\n  - name: orders\n"),
    ("broken.json", '{"name": "shop", "datasets": [}'),
    ("list.yaml", "- a\n- b\n"),
])
def test_unreadable_file_is_refused(tmp_path: Path, name: str, text: str) -> None:
    error = refuse_path(write_text(tmp_path / name, text))
    assert_violation(error, rule="unreadable-file", file=name)


def test_file_that_is_not_utf8_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "binary.yaml"
    path.write_bytes(b"name: \xff\xfe\n")
    assert_violation(refuse_path(path), rule="unreadable-file", file="binary.yaml")


# ─────────────────────────── duplicate-key ─────────────────────────────────

_DUPLICATE_YAML = """\
version: 0.3.0.dev0
name: shop
datasets:
- name: orders
  source: orders
  source: customers
  fields:
  - name: amount
    expression:
      sql: amount
"""

_DUPLICATE_JSON = """\
{
 "version": "0.3.0.dev0",
 "name": "shop",
 "datasets": [
  {"name": "orders",
   "source": "orders",
   "source": "customers",
   "fields": [{"name": "amount", "expression": {"sql": "amount"}}]}
 ]
}
"""


@pytest.mark.parametrize(("name", "text", "line"), [
    ("dup.yaml", _DUPLICATE_YAML, 6),
    ("dup.json", _DUPLICATE_JSON, 7),
])
def test_repeated_mapping_key_is_refused_with_its_line(tmp_path: Path, name: str, text: str, line: int) -> None:
    violation = assert_violation(refuse_path(write_text(tmp_path / name, text)), rule="duplicate-key", file=name,
                                 mentions=("source",))
    assert re.search(rf"\b{line}\b", f"{violation.location} {violation.message}")


# ─────────────────────────── schema ────────────────────────────────────────

def test_unknown_field_key_is_a_schema_violation(tmp_path: Path) -> None:
    doc = shop(datasets=[orders([f("order_id", "o_orderkey"), f("amount", format="0.00")]), customers()],
               relationships=[], metrics=[])
    assert_violation(refuse_path(write_yaml(tmp_path / "shop.yaml", doc)), rule="schema", entity="amount",
                     file="shop.yaml", mentions=("format",))


def test_every_schema_error_is_listed(tmp_path: Path) -> None:
    sourceless = {"name": "customers", "fields": [f("id", "c_custkey")]}
    doc = shop(datasets=[orders([f("order_id", "o_orderkey"), f("amount", format="0.00")]), sourceless],
               relationships=[], metrics=[])
    error = refuse_path(write_yaml(tmp_path / "shop.yaml", doc))
    assert_violation(error, rule="schema", entity="amount", mentions=("format",))
    assert_violation(error, rule="schema", entity="customers", mentions=("source",))


def test_document_without_name_is_a_schema_violation(tmp_path: Path) -> None:
    doc = {key: value for key, value in shop().items() if key != "name"}
    assert_violation(refuse_path(write_yaml(tmp_path / "shop.yaml", doc)), rule="schema", file="shop.yaml",
                     mentions=("name",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_empty_primary_key_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders(primary_key=[]), customers()])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="schema", entity="orders",
                     mentions=("primary_key",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_empty_unique_key_entry_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders(), customers(unique_keys=[[], ["segment"]])])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="schema", entity="customers",
                     mentions=("unique_keys",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_expression_with_neither_form_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(metrics=[m("total_amount", "SUM(orders.amount)"), {"name": "empty", "expression": {}}])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="schema", entity="empty")


@pytest.mark.parametrize("version", ["0.4.0", None])
def test_flat_document_of_unknown_or_no_version_is_refused(tmp_path: Path, version: str | None) -> None:
    doc = {**shop(), "version": version} if version else {k: v for k, v in shop().items() if k != "version"}
    error = refuse_path(write_yaml(tmp_path / "shop.yaml", doc))
    assert_violation(error, rule="schema", file="shop.yaml", mentions=("version",))
    if version:
        assert version in str(error)


# ─────────────────────────── extension-data ────────────────────────────────

def _with_extension(data: Any, *, on: str) -> dict[str, Any]:
    entry = [{"vendor_name": "ACME", "data": data}]
    if on == "dataset":
        return shop(datasets=[orders(nonportable_extensions=entry), customers()])
    fields = [f("order_id", "o_orderkey"), f("amount", nonportable_extensions=entry)]
    return shop(datasets=[orders(fields), customers()], relationships=[])


@pytest.mark.parametrize("entry", ENTRIES)
def test_unquoted_date_under_data_is_refused(tmp_path: Path, entry: Entry) -> None:
    error = refuse(_with_extension({"released": date(2024, 1, 1)}, on="dataset"), entry=entry, directory=tmp_path)
    assert_violation(error, rule="extension-data", entity="orders", mentions=("ACME",))


@pytest.mark.parametrize("value", [math.inf, -math.inf, math.nan])
def test_nested_non_finite_number_is_refused(tmp_path: Path, value: float) -> None:
    error = refuse(_with_extension({"stats": {"ratio": value}}, on="field"), entry="file", directory=tmp_path)
    assert_violation(error, rule="extension-data", entity="amount", mentions=("ACME",))


@pytest.mark.parametrize("data", [
    {"log": {"at": datetime(2024, 1, 1, tzinfo=timezone.utc)}},
    {"codes": {1: "one"}},
])
def test_nested_timestamp_or_non_string_key_is_refused(tmp_path: Path, data: dict[str, Any]) -> None:
    error = refuse(_with_extension(data, on="field"), entry="file", directory=tmp_path)
    assert_violation(error, rule="extension-data", entity="amount", mentions=("ACME",))


@pytest.mark.parametrize("data", [[1, 2], "text"])
def test_data_that_is_not_an_object_is_refused(tmp_path: Path, data: Any) -> None:
    error = refuse(_with_extension(data, on="dataset"), entry="file", directory=tmp_path)
    found = [*hits(error, rule="schema", entity="orders", mentions=("ACME",)),
             *hits(error, rule="extension-data", entity="orders", mentions=("ACME",))]
    assert found, str(error)


# ─────────────────────────── name-length ───────────────────────────────────

@pytest.mark.parametrize("entry", ENTRIES)
def test_name_longer_than_128_characters_is_refused(tmp_path: Path, entry: Entry) -> None:
    long_name = "x" * 129
    doc = shop(datasets=[orders([*_order_fields_without("quantity"), f(long_name, "quantity")]), customers()])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="name-length", entity=long_name,
                     mentions=("128",))


def test_name_of_128_characters_passes(tmp_path: Path) -> None:
    doc = shop(datasets=[orders([*_order_fields_without("quantity"), f("x" * 128, "quantity")]), customers()])
    assert parse_osi_path(write_yaml(tmp_path / "shop.yaml", doc))


def _order_fields_without(name: str) -> list[dict[str, Any]]:
    return [field for field in orders()["fields"] if field["name"] != name]


# ─────────────────────────── duplicate-name ────────────────────────────────

@pytest.mark.parametrize("entry", ENTRIES)
def test_repeated_relationship_name_is_refused(tmp_path: Path, entry: Entry) -> None:
    again = rel("orders_to_customers", src="orders", to="customers", from_columns=["customer_id"], to_columns=["id"])
    doc = shop(relationships=[again, again])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="duplicate-name",
                     entity="orders_to_customers")


@pytest.mark.parametrize("entry", ENTRIES)
def test_relationship_names_differing_only_in_case_are_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(relationships=[{**O2C, "name": "r"}, {**O2C, "name": "R"}])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="duplicate-name", mentions=("r", "R"))


@pytest.mark.parametrize("entry", ENTRIES)
def test_repeated_model_metric_name_in_any_case_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(metrics=[m("revenue", "SUM(orders.amount)"), m("Revenue", "SUM(orders.quantity)")])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="duplicate-name",
                     mentions=("revenue", "Revenue"))


@pytest.mark.parametrize("entry", ENTRIES)
def test_repeated_dataset_name_in_any_case_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders(), ds("Orders", source="orders", fields=[f("amount")]), customers()])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="duplicate-name",
                     mentions=("orders", "Orders"))


@pytest.mark.parametrize("entry", ENTRIES)
def test_field_and_dataset_metric_sharing_a_name_are_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders(metrics=[m("amount", "SUM(amount)")]), customers()])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="duplicate-name", entity="orders",
                     mentions=("amount",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_fields_differing_only_in_case_are_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders([*orders()["fields"], f("id", "o_orderkey"), f("ID", "o_custkey")]), customers()])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="duplicate-name", entity="orders",
                     mentions=("id", "ID"))


def test_dataset_name_repeated_across_documents_is_refused(tmp_path: Path) -> None:
    write_yaml(tmp_path / "a.yaml", flat([orders()], name="first"))
    write_yaml(tmp_path / "b.yaml", flat([ds("Orders", source="orders", fields=[f("amount")])], name="second"))
    error = refuse_path(tmp_path)
    assert_violation(error, rule="duplicate-name", mentions=("orders",))
    assert "first" in str(error) and "second" in str(error)


def test_dataset_name_repeated_across_documents_is_refused_by_the_converter() -> None:
    error = refuse_converter(flat([orders()], name="first"), flat([orders()], name="second"))
    assert_violation(error, rule="duplicate-name", mentions=("orders",))


# ─────────────────────────── unknown-dataset ───────────────────────────────

def test_relationship_to_a_dataset_of_another_document_is_refused(tmp_path: Path) -> None:
    write_yaml(tmp_path / "a.yaml", flat([orders()], relationships=[O2C], name="first"))
    write_yaml(tmp_path / "b.yaml", flat([customers()], name="second"))
    assert_violation(refuse_path(tmp_path), rule="unknown-dataset", entity="o2c", mentions=("customers",),
                     file="a.yaml", document="first")


@pytest.mark.parametrize("entry", ENTRIES)
def test_relationship_from_an_unknown_dataset_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(relationships=[rel("o2c", src="nowhere", to="customers", from_columns=["customer_id"],
                                  to_columns=["id"])])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="unknown-dataset", entity="o2c",
                     mentions=("nowhere",))


# ─────────────────────────── undeclared-key-field ──────────────────────────

@pytest.mark.parametrize("entry", ENTRIES)
def test_primary_key_on_a_source_column_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders(primary_key=["o_orderkey"]), customers()])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="undeclared-key-field", entity="orders",
                     mentions=("o_orderkey",))


@pytest.mark.parametrize("entry", ENTRIES)
def test_unique_key_on_a_source_column_is_refused(tmp_path: Path, entry: Entry) -> None:
    doc = shop(datasets=[orders(), customers(unique_keys=[["email"]])])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="undeclared-key-field",
                     entity="customers", mentions=("email",))


@pytest.mark.parametrize(("side", "columns"), [("from_columns", ["o_custkey"]), ("to_columns", ["c_custkey"])])
@pytest.mark.parametrize("entry", ENTRIES)
def test_relationship_on_a_source_column_is_refused(tmp_path: Path, entry: Entry, side: str,
                                                    columns: list[str]) -> None:
    doc = shop(relationships=[{**O2C, side: columns}])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="undeclared-key-field", entity="o2c",
                     mentions=(columns[0],))


# ─────────────────────────── relationship-arity ────────────────────────────

@pytest.mark.parametrize("entry", ENTRIES)
def test_relationship_key_lengths_must_match(tmp_path: Path, entry: Entry) -> None:
    doc = shop(relationships=[{**O2C, "from_columns": ["customer_id", "status"]}])
    assert_violation(refuse(doc, entry=entry, directory=tmp_path), rule="relationship-arity", entity="o2c")
