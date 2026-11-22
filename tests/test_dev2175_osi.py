"""Ossie expression objects import verbatim, export as themselves, and the SLAYER payload moves to version 3.

Specs: importers/osi-import, importers/osi-export, compatibility/ossie-vocabulary.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import sqlglot
import yaml
from sqlglot import exp

from slayer.core.models import SlayerDataset
from tests._deprecated_input_helpers import recorded
from tests._osi_export_helpers import (
    dataset, entities, eval_field, export, field, make_env, normalized, payload, reimport, rows, save, slayer_entries,
    stored,
)

FIXTURES = Path(__file__).parent / "fixtures" / "ossie_vocabulary"
V2_DOCUMENT = FIXTURES / "export_payload_v2.osi.yaml"
ORDERS_DS = "osids"


def _entry(dialect: str, text: str) -> dict[str, str]:
    return {"dialect": dialect, "expression": text}


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

    async def test_unquoted_path_inlined_in_any_case(self, tmp_path: Path) -> None:
        env = await make_env(tmp_path)
        await save(env, _orders_dataset({"name": "price"}, {"name": "quantity", "datatype": "Integer"},
                                        {"name": "discount"}, {"name": "Gross", "expression": "price * quantity"},
                                        {"name": "net", "expression": "ORDERS.gross - discount"}))
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
        query = {"population": "orders", "dimensions": ["orders.id", "orders.gross", "orders.completed_amount", "orders.legal_type", "orders.per_unit"]}
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
    out = await rows(target, {"population": "orders", "dimensions": ["orders.id", "orders.amount", "orders.amount_x2"],
                              "order": [{"field": "orders.id"}]})
    assert [r["orders.amount_x2"] for r in out] == [2 * r["orders.amount"] for r in out]
