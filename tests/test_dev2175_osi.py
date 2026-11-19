"""Ossie expression objects import verbatim, export as themselves, and the SLAYER payload moves to version 3.

Specs: importers/osi-import, importers/osi-export, compatibility/ossie-vocabulary.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import sqlglot
import yaml
from sqlglot import exp

from slayer.core.models import SlayerDataset
from slayer.ingest_report import ConversionResult
from tests._deprecated_input_helpers import recorded
from tests._osi_export_helpers import (
    dataset, entities, eval_field, export, field, make_env, normalized, payload, reimport, rows, save, slayer_entries,
    stored,
)
from tests._osi_helpers import (
    by_name, convert, disable_type_probe, documents, ds, f, f_dialects, flat, mentions, scalar, seed_db,
)

FIXTURES = Path(__file__).parent / "fixtures" / "ossie_vocabulary"
V2_DOCUMENT = FIXTURES / "export_payload_v2.osi.yaml"
ORDERS_DS = "osids"


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


@pytest.fixture
def emission(monkeypatch: pytest.MonkeyPatch) -> None:
    disable_type_probe(monkeypatch)


def _orders(*fields: dict[str, Any], metrics: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    dataset_doc = ds("orders", fields=[f("order_id", "o_orderkey"), f("amount"), *fields])
    if metrics:
        dataset_doc["metrics"] = metrics
    return flat([dataset_doc])


def _import(db: Path, *fields: dict[str, Any], metrics: list[dict[str, Any]] | None = None,
            **kwargs: Any) -> ConversionResult:
    return convert(db=db, docs=documents(_orders(*fields, metrics=metrics)), **kwargs)


def _expression(result: ConversionResult, name: str, *, dataset_name: str = "orders") -> Any:
    """The stored ``expression`` of field ``name``."""
    fields = by_name(result)[dataset_name].model_dump(mode="json", exclude_none=True)["fields"]
    (found,) = [x for x in fields if x["name"] == name]
    return found["expression"]


def _metric_dump(result: ConversionResult, name: str) -> dict[str, Any]:
    metrics = by_name(result)["orders"].model_dump(mode="json", exclude_none=True)["metrics"]
    (found,) = [x for x in metrics if x["name"] == name]
    return found


def _entry(dialect: str, text: str) -> dict[str, str]:
    return {"dialect": dialect, "expression": text}


# ── import: expression objects stored as written ─────────────────────────

@pytest.mark.usefixtures("emission")
class TestStoredAsWritten:
    def test_portable_form_and_a_vendor_dialect_kept_together(self, db: Path) -> None:
        result = _import(db, f_dialects("pick", ("SNOWFLAKE", "amount::NUMBER"), sql="amount"),
                         target_dialect="snowflake")
        assert _expression(result, "pick") == {"sql": "amount", "dialects": [_entry("SNOWFLAKE", "amount::NUMBER")]}

    def test_ansi_and_target_native_kept_together(self, db: Path) -> None:
        result = _import(db, f_dialects("pick", ("ANSI_SQL", "amount * 3"), ("POSTGRES", "amount * 4")),
                         target_dialect="postgres")
        assert _expression(result, "pick") == {
            "dialects": [_entry("ANSI_SQL", "amount * 3"), _entry("POSTGRES", "amount * 4")]}

    def test_datasource_type_aliases(self, db: Path) -> None:
        result = _import(db, f_dialects("pick", ("MDX", "[Measures].[x]"), ("POSTGRES", "amount*4")),
                         target_dialect="postgresql")
        assert _expression(result, "pick") == {
            "dialects": [_entry("MDX", "[Measures].[x]"), _entry("POSTGRES", "amount*4")]}
        assert not mentions(result, "POSTGRES", severity="info")

    def test_target_native_kept_verbatim(self, db: Path) -> None:
        result = _import(db, f_dialects("pick", ("SNOWFLAKE", "amount::NUMBER(10,2)")), target_dialect="snowflake")
        assert _expression(result, "pick") == {"dialects": [_entry("SNOWFLAKE", "amount::NUMBER(10,2)")]}

    def test_target_native_mixed_case_identifier_folded(self, tmp_path: Path) -> None:
        db = seed_db(directory=tmp_path, statements=[
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, orderamount DOUBLE)", "INSERT INTO orders VALUES (1, 2.5)"])
        doc = flat([ds("orders", fields=[f("id"), f_dialects("pick", ("SNOWFLAKE", "OrderAmount::NUMBER(10,2)"))])])
        result = convert(db=db, docs=documents(doc), target_dialect="snowflake")
        assert _expression(result, "pick") == {"dialects": [_entry("SNOWFLAKE", "orderamount::NUMBER(10,2)")]}


# ── import: an expression with no runnable form is translated ────────────

def _duckdb_entry(expression: Any) -> str:
    (text,) = [e["expression"] for e in expression["dialects"] if e["dialect"].upper() == "DUCKDB"]
    return text


class TestTranslated:
    async def test_foreign_vendor_dialect_as_last_resort(self, db: Path) -> None:
        result = _import(db, f_dialects("pick", ("BIGQUERY", "amount * 5")))
        expression = _expression(result, "pick")
        assert expression["dialects"][0] == _entry("BIGQUERY", "amount * 5")
        assert [e["dialect"] for e in expression["dialects"]] == ["BIGQUERY", "DUCKDB"]
        assert mentions(result, "BIGQUERY", severity="info")
        assert await scalar(result=result, db=db, dataset="orders", expression="sum(pick)") == 110

    async def test_override_first(self, db: Path) -> None:
        result = _import(db, f_dialects("pick", ("SNOWFLAKE", "amount * 2"), ("BIGQUERY", "amount * 3")),
                         dialect="SNOWFLAKE")
        assert "2" in _duckdb_entry(_expression(result, "pick"))
        assert await scalar(result=result, db=db, dataset="orders", expression="sum(pick)") == 44

    async def test_unreadable_override_falls_through(self, db: Path) -> None:
        result = _import(db, f_dialects("pick", ("MDX", "[Measures].[x]"), ("ANSI_SQL", "amount * 3")),
                         dialect="MDX")
        assert "3" in _duckdb_entry(_expression(result, "pick"))
        assert await scalar(result=result, db=db, dataset="orders", expression="sum(pick)") == 66

    async def test_portable_sql_slayer_cannot_read(self, db: Path) -> None:
        result = _import(db, f("zeroed", "ZEROIFNULL(amount)"))
        expression = _expression(result, "zeroed")
        assert expression["sql"] == "ZEROIFNULL(amount)"
        assert "amount IS NULL" in _duckdb_entry(expression)
        assert mentions(result, "zeroed")
        assert await scalar(result=result, db=db, dataset="orders", expression="sum(zeroed)") == 22


# ── import: a metric's entry for the target's dialect ────────────────────

class TestMetricTargetEntry:
    async def test_dropped_beside_portable_sql(self, db: Path) -> None:
        total = {"name": "total", "expression": {"sql": "SUM(amount)", "dialects": [_entry("DUCKDB", "sum(amount)")]}}
        result = _import(db, metrics=[total])
        assert "DUCKDB" not in json.dumps(_metric_dump(result, "total")).upper()
        assert [e for e in mentions(result, "total") if "DUCKDB" in e.message]
        assert await scalar(result=result, db=db, dataset="orders", expression="total") == 22

    async def test_translated_when_it_is_the_only_form(self, db: Path) -> None:
        total = {"name": "total", "expression": {"dialects": [_entry("DUCKDB", "sum(amount)")]}}
        result = _import(db, metrics=[total])
        assert "DUCKDB" not in json.dumps(_metric_dump(result, "total")).upper()
        assert [e for e in result.entries if "DUCKDB" in e.message]
        assert await scalar(result=result, db=db, dataset="orders", expression="total") == 22


# ── export ────────────────────────────────────────────────────────────────

def _orders_dataset(*fields: dict[str, Any]) -> SlayerDataset:
    return SlayerDataset.model_validate({
        "name": "orders", "sql_table": "orders", "data_source": ORDERS_DS,
        "fields": [{"name": "id", "expression": "o_orderkey", "datatype": "Integer", "primary_key": True},
                   *fields]})


def _identifiers(sql: str) -> set[str]:
    return {c.name for c in sqlglot.parse_one(sql, read="duckdb").find_all(exp.Column)}


class TestExportForms:
    async def test_portable_expression(self, tmp_path: Path) -> None:
        env = await make_env(tmp_path)
        await save(env, _orders_dataset({"name": "price"}, {"name": "quantity", "datatype": "Integer"},
                                        {"name": "gross", "expression": "price * quantity"}))
        gross = field(dataset((await export(env)).document, "orders"), "gross")
        assert gross is not None
        assert gross["expression"] == {"sql": "price * quantity"}

    async def test_renamed_column(self, tmp_path: Path) -> None:
        env = await make_env(tmp_path)
        await save(env, _orders_dataset({"name": "total_amount", "expression": "o_totalprice"}))
        total = field(dataset((await export(env)).document, "orders"), "total_amount")
        assert total is not None
        assert total["expression"] == {"sql": "o_totalprice"}

    async def test_derived_column_over_another_derived_column(self, tmp_path: Path) -> None:
        env = await make_env(tmp_path)
        await save(env, _orders_dataset({"name": "price"}, {"name": "quantity", "datatype": "Integer"},
                                        {"name": "discount"}, {"name": "gross", "expression": "price * quantity"},
                                        {"name": "net", "expression": "orders.gross - discount"}))
        net = field(dataset((await export(env)).document, "orders"), "net")
        assert net is not None
        assert _identifiers(net["expression"]["sql"]) == {"price", "quantity", "discount"}

    async def test_filtered_column(self, tmp_path: Path) -> None:
        env = await make_env(tmp_path)
        await save(env, _orders_dataset({"name": "completed_amount", "expression": "amount",
                                         "filter": "status = 'completed'"}))
        orders = dataset((await export(env)).document, "orders")
        assert eval_field(env, orders, "completed_amount", order_by="o_orderkey") == [10, None, 7, None, 20]

    async def test_export_and_re_import_keep_values(self, tmp_path: Path) -> None:
        env = await make_env(tmp_path)
        await save(env, _orders_dataset(
            {"name": "gross", "expression": "price * quantity"},
            {"name": "completed_amount", "expression": "amount", "filter": "status = 'completed'"},
            {"name": "legal_type", "expression": '"LegalType"'},
            {"name": "per_unit", "expression": {"dialects": [_entry("DUCKDB", "amount / quantity")]}}))
        query = {"population": "orders", "dimensions": ["id", "gross", "completed_amount", "legal_type", "per_unit"]}
        before = normalized(await rows(env, query))
        target, _ = await reimport(env, (await export(env)).document)
        assert normalized(await rows(target, query)) == before


# ── the SLAYER payload ────────────────────────────────────────────────────

def _payloads(document: dict[str, Any]) -> list[dict[str, Any]]:
    return [payload(e) for e in entities(document) if slayer_entries(e)]


async def test_a_v2_export_still_imports(tmp_path: Path) -> None:
    env = await make_env(tmp_path / "src")
    document = yaml.safe_load(V2_DOCUMENT.read_text())
    assert {b["version"] for b in _payloads(document)} == {2}
    with recorded() as caught:
        target, _ = await reimport(env, document)
    assert [w for w in caught if issubclass(w.category, FutureWarning)] == []
    orders = (await stored(target, "orders")).model_dump(mode="json", exclude_none=True)
    (amount_x2,) = [x for x in orders["fields"] if x["name"] == "amount_x2"]
    assert amount_x2["expression"] == {"dialects": [_entry("DUCKDB", "amount * 2")]}
    out = await rows(target, {"population": "orders", "dimensions": ["id", "amount", "amount_x2"],
                              "order": [{"field": "id"}]})
    assert [r["orders.amount_x2"] for r in out] == [2 * r["orders.amount"] for r in out]
