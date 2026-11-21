"""Earlier document shapes are upgraded, then validated like any other document."""

from pathlib import Path
from typing import Any

import pytest

from slayer.ingest_report import ConversionResult
from slayer.osi.parser import parse_osi_path
from tests._osi_helpers import by_name, convert, ds, entries, legacy, m, metric_home, seed_db, write_yaml
from tests._ossie_validity_fixtures import (
    O2C, assert_violation, customers, hits, orders, refuse_path, shop,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


def _upgrade_notes(result: ConversionResult) -> list[Any]:
    return [e for e in entries(result) if e.severity == "info" and "0.3.0.dev0" in e.message]


def _model(name: str, *datasets: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {"name": name, "datasets": list(datasets), **extra}


# ─────────────────────────── wrappers that upgrade ─────────────────────────

@pytest.mark.parametrize("root", [
    legacy(_model("a", orders()), _model("b", customers())),
    {"semantic_model": [_model("a", orders()), _model("b", customers())]},
    {"version": 1.0, "semantic_model": [_model("a", orders()), _model("b", customers())]},
    {"version": "0.1.1", "dialects": ["ANSI_SQL"], "vendors": ["ACME"],
     "semantic_model": [_model("a", orders()), _model("b", customers())]},
], ids=["array", "no-version", "float-1.0", "dialects-and-vendors"])
def test_wrapper_resolving_under_current_rules_imports_with_one_note(db: Path, tmp_path: Path,
                                                                     root: dict[str, Any]) -> None:
    result = convert(db=db, docs=parse_osi_path(write_yaml(tmp_path / "both.yaml", root)))
    assert set(by_name(result)) == {"orders", "customers"}
    assert len(_upgrade_notes(result)) == 1


def test_object_valued_wrapper_imports(db: Path, tmp_path: Path) -> None:
    root = {"version": "0.2.0.dev0", "semantic_model": _model("a", orders(), customers(),
                                                              relationships=[O2C],
                                                              metrics=[m("total", "SUM(orders.amount)")])}
    result = convert(db=db, docs=parse_osi_path(write_yaml(tmp_path / "a.yaml", root)))
    assert set(by_name(result)) == {"orders", "customers"}
    assert metric_home(result, "total").name == "orders"
    assert len(_upgrade_notes(result)) == 1


def test_root_keys_are_carried_onto_each_model(db: Path, tmp_path: Path) -> None:
    root = {**legacy(_model("a", orders()), _model("b", customers())), "description": "shared"}
    result = convert(db=db, docs=parse_osi_path(write_yaml(tmp_path / "both.yaml", root)))
    assert set(by_name(result)) == {"orders", "customers"}


# ─────────────────────────── wrappers that are refused ─────────────────────

def test_wrapper_relying_on_source_columns_is_refused_listing_each_reference(tmp_path: Path) -> None:
    bare = ds("orders", primary_key=["o_orderkey"],
              metrics=[m("total", "SUM(amount)"), m("units", "SUM(quantity)")])
    error = refuse_path(write_yaml(tmp_path / "old.yaml", legacy(_model("a", bare))))
    assert_violation(error, rule="metric-scope", entity="total", mentions=("amount",), file="old.yaml")
    assert_violation(error, rule="metric-scope", entity="units", mentions=("quantity",), file="old.yaml")
    assert_violation(error, rule="undeclared-key-field", entity="orders", mentions=("o_orderkey",))


def test_root_key_the_model_also_sets_is_refused(tmp_path: Path) -> None:
    root = {**legacy(_model("a", orders(), description="y")), "description": "x"}
    error = refuse_path(write_yaml(tmp_path / "old.yaml", root))
    assert_violation(error, rule="legacy-shape", file="old.yaml", document="a", mentions=("description",))


@pytest.mark.parametrize("root", [
    {"version": "0.2.0.dev0", "semantic_model": [_model("a", orders())], "datasets": [customers()]},
    {"version": "0.2.0.dev0", "semantic_model": []},
    {"version": "0.2.0.dev0", "semantic_model": None},
    {"version": "0.2.0.dev0", "semantic_model": "shop"},
    {"version": "0.2.0.dev0", "semantic_model": [_model("a", orders()), {}]},
    {"version": "0.2.0.dev0", "semantic_model": [_model("a", orders()), 1]},
    {"version": "0.9.0", "semantic_model": [_model("a", orders())]},
], ids=["mixed", "empty", "null", "string", "empty-element", "scalar-element", "unknown-version"])
def test_malformed_wrapper_is_refused(tmp_path: Path, root: dict[str, Any]) -> None:
    assert_violation(refuse_path(write_yaml(tmp_path / "old.yaml", root)), rule="legacy-shape", file="old.yaml")


def test_wrapped_0_3_document_is_refused(tmp_path: Path) -> None:
    root = {"version": "0.3.0.dev0", "semantic_model": [_model("a", orders())]}
    assert_violation(refuse_path(write_yaml(tmp_path / "old.yaml", root)), rule="legacy-shape", file="old.yaml",
                     mentions=("0.3.0.dev0",))


# ─────────────────────────── earlier flat documents ────────────────────────

@pytest.mark.parametrize("version", ["0.2.0.dev0", "0.1.0", "0.1.1", "1.0"])
def test_earlier_flat_document_imports_with_one_note(db: Path, tmp_path: Path, version: str) -> None:
    result = convert(db=db, docs=parse_osi_path(write_yaml(tmp_path / "shop.yaml", {**shop(), "version": version})))
    assert set(by_name(result)) == {"orders", "customers"}
    assert metric_home(result, "total_amount").name == "orders"
    [note] = _upgrade_notes(result)
    assert "shop" in note.message


def test_current_flat_document_has_no_upgrade_note(db: Path, tmp_path: Path) -> None:
    result = convert(db=db, docs=parse_osi_path(write_yaml(tmp_path / "shop.yaml", shop())))
    assert not _upgrade_notes(result)


# ─────────────────────────── legacy custom_extensions ──────────────────────

@pytest.mark.parametrize("data", ["[1, 2]", "not json"])
def test_legacy_string_that_is_not_an_object_is_refused(tmp_path: Path, data: str) -> None:
    doc = shop(datasets=[orders(custom_extensions=[{"vendor_name": "ACME", "data": data}]), customers()])
    error = refuse_path(write_yaml(tmp_path / "shop.yaml", doc))
    assert_violation(error, rule="legacy-shape", entity="orders", mentions=("ACME",), file="shop.yaml")
    assert not hits(error, rule="schema")
