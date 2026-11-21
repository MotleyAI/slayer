"""One error lists every violation of every file, in a stable order, before any database is read."""

import logging
import shutil
from pathlib import Path

import pytest

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.osi.converter import OsiConversionError, OsiToSlayerConverter
from slayer.osi.parser import parse_osi_path
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process
from tests._osi_helpers import DATASOURCE, FIXTURES, documents, ds, f, flat, legacy, m, rel, seed_db, write_yaml
from tests._ossie_validity_fixtures import (
    ENTRIES, Entry, assert_violation, customers, hits, orders, refuse, refuse_converter, refuse_path, shop,
)

_REPO = Path(__file__).resolve().parent.parent


def _three_violations() -> dict:
    r = rel("r", src="orders", to="customers", from_columns=["customer_id"], to_columns=["id"])
    return shop(datasets=[orders(primary_key=["o_orderkey"]), customers()], relationships=[r, r],
                metrics=[m("total", "SUM(amount)")])


def _lone(document: str, *, dataset: str, key: str = "order_id") -> dict:
    """A one-dataset document; ``key`` other than a declared field makes it invalid."""
    return flat([ds(dataset, source="orders", fields=[f("order_id", "o_orderkey"), f("amount")], primary_key=[key])],
                name=document)


def _invalid(document: str) -> dict:
    return _lone(document, dataset=f"{document}_orders", key="o_orderkey")


@pytest.mark.parametrize("entry", ENTRIES)
def test_several_violations_are_listed_in_one_error(tmp_path: Path, entry: Entry) -> None:
    error = refuse(_three_violations(), entry=entry, directory=tmp_path)
    assert_violation(error, rule="duplicate-name", entity="r")
    assert_violation(error, rule="undeclared-key-field", entity="orders", mentions=("o_orderkey",))
    assert_violation(error, rule="metric-scope", entity="total", mentions=("amount",))
    assert all(v.document == "shop" for v in error.violations)


def test_error_text_has_one_line_per_violation_naming_file_and_rule(tmp_path: Path) -> None:
    error = refuse_path(write_yaml(tmp_path / "shop.yaml", _three_violations()))
    lines = str(error).splitlines()
    assert len(lines) == len(error.violations) >= 3
    for line, violation in zip(lines, error.violations):
        assert "shop.yaml" in line and violation.rule in line and violation.location in line


def test_two_invalid_files_are_listed_and_the_valid_one_is_not_returned(tmp_path: Path) -> None:
    write_yaml(tmp_path / "a.yaml", _invalid("first"))
    write_yaml(tmp_path / "b.yaml", _lone("valid", dataset="valid_orders"))
    write_yaml(tmp_path / "c.yaml", _invalid("third"))
    error = refuse_path(tmp_path)
    assert {Path(v.file).name for v in error.violations} == {"a.yaml", "c.yaml"}
    assert hits(error, rule="undeclared-key-field", file="a.yaml", document="first")
    assert hits(error, rule="undeclared-key-field", file="c.yaml", document="third")


def test_violations_follow_file_then_document_order(tmp_path: Path) -> None:
    (tmp_path / "b_dir").mkdir()
    (tmp_path / "a_dir").mkdir()
    write_yaml(tmp_path / "b_dir" / "x.yaml", _invalid("in_b"))
    write_yaml(tmp_path / "root.yaml", _invalid("at_root"))
    second = {**_lone("second", dataset="d2", key="nada"), "metrics": [m("t", "SUM(amount)"), m("u", "SUM(x.y.z)")]}
    write_yaml(tmp_path / "a_dir" / "x.yaml", legacy(_lone("first", dataset="d1", key="nope"), second))
    error = refuse_path(tmp_path)
    order = [v.document for v in error.violations]
    assert order == sorted(order, key=["at_root", "first", "second", "in_b"].index)
    assert [Path(v.file).relative_to(tmp_path).as_posix() for v in error.violations] == sorted(
        (Path(v.file).relative_to(tmp_path).as_posix() for v in error.violations),
        key=["root.yaml", "a_dir/x.yaml", "b_dir/x.yaml"].index)
    sequence = [(v.file, v.document, v.rule, v.location, v.message) for v in error.violations]
    assert [(v.file, v.document, v.rule, v.location, v.message) for v in refuse_path(tmp_path).violations] == sequence


class _NoDatabase:
    """A client that fails on any use."""

    def __getattr__(self, name: str) -> None:
        raise AssertionError(f"the database was read ({name})")


def test_invalid_document_built_in_code_is_refused_before_the_database_is_read() -> None:
    with pytest.raises(OsiConversionError) as info:
        converter = OsiToSlayerConverter(documents=documents(_three_violations()), data_source=DATASOURCE)
        run_sync(converter.collect_metadata(client=_NoDatabase()))  # pyright: ignore[reportArgumentType]
    assert_violation(info.value, rule="undeclared-key-field", entity="orders", mentions=("o_orderkey",))
    assert_violation(refuse_converter(_three_violations()), rule="undeclared-key-field", entity="orders")


def test_schema_runs_on_the_typed_document() -> None:
    mapping = {key: value for key, value in shop().items() if key != "version"}
    assert_violation(refuse_converter(mapping), rule="schema", mentions=("version",))


def test_valid_document_built_in_code_constructs() -> None:
    OsiToSlayerConverter(documents=documents(shop()), data_source=DATASOURCE)


# ─────────────────────────── skipped and clean documents ───────────────────

@pytest.mark.parametrize("name", ["flights.ontology.yaml", "flights.mapping.yaml"])
def test_ontology_document_is_skipped_with_a_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture,
                                                     name: str) -> None:
    shutil.copy(FIXTURES / "upstream" / name, tmp_path / name)
    write_yaml(tmp_path / "shop.yaml", shop())
    with caplog.at_level(logging.WARNING):
        docs = parse_osi_path(tmp_path)
    assert [d.name for d in docs] == ["shop"]
    assert any(name in r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)


@pytest.mark.parametrize("path", [FIXTURES / "upstream", _REPO / "docs/examples/13_osi_import/shop.osi.yaml"],
                         ids=["upstream", "docs-shop"])
def test_shipped_examples_have_no_violation(path: Path) -> None:
    assert parse_osi_path(path)


# ─────────────────────────── CLI ───────────────────────────────────────────

def test_cli_refusal_exits_1_listing_violations_and_saves_nothing(tmp_path: Path) -> None:
    db = seed_db(directory=tmp_path)
    store = tmp_path / "store"
    run_sync(YAMLStorage(base_dir=str(store)).save_datasource(
        DatasourceConfig(name=DATASOURCE, type="duckdb", database=str(db))))
    docs = tmp_path / "docs"
    docs.mkdir()
    write_yaml(docs / "a.yaml", _invalid("first"))
    write_yaml(docs / "b.yaml", _lone("valid", dataset="valid_orders"))
    result = run_cli_in_process(["import-osi", str(docs), "--datasource", DATASOURCE, "--storage", str(store)],
                                uncaught_exit=True)
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "Traceback" not in output
    assert "a.yaml" in output and "undeclared-key-field" in output and "o_orderkey" in output
    assert run_sync(YAMLStorage(base_dir=str(store)).list_datasets()) == []
