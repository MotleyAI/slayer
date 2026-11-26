"""Saves judge what they add or change.

Spec: models/save-validation › Saves judge what they add or change.
"""

from __future__ import annotations

import re
import warnings
from typing import Any

import pytest
import yaml

from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.base import StorageBackend

from tests._dev2243_fixtures import (
    DS,
    SALES,
    STORAGES,
    dataset,
    document_engine,
    document_text,
    field,
    metric,
    relationship,
)
from tests._mcp_results import text_blocks

#: ``sales`` plus a model-scoped metric SLayer cannot evaluate yet (a window function).
WINDOWED = SALES + """\
  - name: running_rev
    expression:
      sql: SUM(orders.amount) OVER ()
"""

#: ``sales`` whose ``orders`` also declares the field ``order_date``.
DATED = SALES.replace("    metrics:\n      - name: total_revenue", """\
      - name: order_date
        expression: {sql: order_id}
        datatype: String
    metrics:
      - name: total_revenue""")

#: A model-scoped metric reading the field ``orders.amount`` directly.
READS_AMOUNT = document_text(
    datasets=[
        dataset("orders", primary_key=["order_id"],
                fields=[field("order_id"), field("customer_id"), field("amount", datatype="Float")]),
        dataset("customers", primary_key=["customer_id"], fields=[field("customer_id"), field("region")]),
    ],
    relationships=[relationship("orders_to_customers", src="orders", to="customers", from_columns=["customer_id"],
                                to_columns=["customer_id"])],
    metrics=[metric("amount_total", "SUM(orders.amount)")],
)


async def _mcp(storage: StorageBackend, tool: str, **arguments: Any) -> str:
    """One MCP ``tool`` call on a fresh server: its text, or the raised error's text."""
    server = create_mcp_server(storage=storage, _seed_help=False)
    try:
        blocks = text_blocks(await server.call_tool(name=tool, arguments=arguments))
        return "\n".join(getattr(b, "text", "") for b in blocks)
    except Exception as exc:  # noqa: BLE001 — the refusal text is the assertion subject
        return f"{type(exc).__name__}: {exc}"
    finally:
        await getattr(server, "_slayer_engine").aclose()


async def _text(engine: SlayerQueryEngine) -> str | None:
    return await engine.storage.get_semantic_model_text("sales", data_source=DS)


def _metric(text: str, name: str) -> dict[str, Any]:
    return next(m for m in yaml.safe_load(text)["metrics"] if m["name"] == name)


class TestUnrelatedEdit:
    @pytest.mark.parametrize("kind", STORAGES)
    async def test_an_unrelated_edit_beside_a_windowed_metric(self, kind: str) -> None:
        """Spec: models/save-validation › An unrelated edit beside a windowed metric."""
        async with document_engine(kind=kind, documents=(WINDOWED,)) as engine:
            before = await _text(engine)
            reply = await _mcp(engine.storage, "edit_dataset", dataset_name="customers", data_source=DS,
                               description="Customer master.")
            after = await _text(engine)
            customers = await engine.storage.get_dataset("customers", data_source=DS)
        assert before is not None
        assert after is not None
        assert "Error" not in reply, reply
        assert customers is not None, reply
        assert customers.description == "Customer master.", reply
        assert _metric(after, "running_rev") == _metric(before, "running_rev")
        assert "sql: SUM(orders.amount) OVER ()" in after

    async def test_an_unrelated_engine_save_beside_a_windowed_metric(self) -> None:
        """Spec: models/save-validation › An unrelated edit beside a windowed metric (Python save)."""
        async with document_engine(documents=(WINDOWED,)) as engine:
            before = await _text(engine)
            customers = await engine.storage.get_dataset("customers", data_source=DS)
            assert customers is not None
            await engine.save_dataset(customers.model_copy(update={"description": "Customer master."}))
            after = await _text(engine)
            saved = await engine.storage.get_dataset("customers", data_source=DS)
        assert before is not None
        assert after is not None
        assert saved is not None
        assert saved.description == "Customer master."
        assert _metric(after, "running_rev") == _metric(before, "running_rev")


class TestAuthoringRefused:
    async def test_authoring_a_windowed_metric_is_refused(self) -> None:
        """Spec: models/save-validation › Authoring a windowed metric saves; an unwindowable function is refused."""
        async with document_engine() as engine:
            reply = await _mcp(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                               metrics=[metric("running_rev", "SUM(SUM(orders.amount)) OVER ()")])
            after = await _text(engine)
            assert "Error" not in reply, reply
            assert after is not None
            assert _metric(after, "running_rev")["expression"] == {"sql": "SUM(SUM(orders.amount)) OVER ()"}
            total = await engine.execute({"population": "orders", "metrics": ["running_rev"]})
            by_status = await engine.execute({"population": "orders", "dimensions": ["orders.status"],
                                              "metrics": ["running_rev"]})
        assert [float(r["running_rev"]) for r in total.data] == [650.0]
        assert {r["orders.status"]: float(r["running_rev"]) for r in by_status.data} == {
            "completed": 650.0, "open": 650.0}

    async def test_authoring_a_metric_windowing_a_function_ossie_does_not_window_is_refused(self) -> None:
        """Spec: models/save-validation › Authoring a windowed metric saves; an unwindowable function is refused (an unwindowed function)."""
        async with document_engine(documents=(DATED,)) as engine:
            before = await _text(engine)
            ok = await _mcp(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                            metrics=[metric("plain", "last(orders.amount, orders.order_date)")])
            assert "Error" not in ok, ok
            stored = await _text(engine)
            reply = await _mcp(engine.storage, "edit_semantic_model", name="sales", data_source=DS,
                               metrics=[metric("newest", "last(orders.amount, orders.order_date) OVER ()")])
            after = await _text(engine)
        assert stored != before
        assert reply.startswith("Error"), reply
        assert "IllegalWindowInFilterError" not in reply, reply
        assert "uses a window function (OVER)" not in reply, reply
        assert "newest" in reply, reply
        assert re.search(r"\blast\b", reply, flags=re.IGNORECASE), reply
        assert after == stored


class TestBrokenDependents:
    async def test_renaming_a_referenced_field_warns_about_dependents(self) -> None:
        """Spec: models/save-validation › Renaming a referenced field warns about dependents."""
        async with document_engine(documents=(READS_AMOUNT,)) as engine:
            orders = await engine.storage.get_dataset("orders", data_source=DS)
            assert orders is not None
            renamed = orders.model_copy(update={"fields": [
                f.model_copy(update={"name": "net_amount"}) if f.name == "amount" else f for f in orders.fields]})
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.save_dataset(renamed)
            saved = await engine.storage.get_dataset("orders", data_source=DS)
        assert saved is not None
        assert saved.get_field("net_amount") is not None
        broken = [str(w.message) for w in caught if issubclass(w.category, UserWarning)
                  and "amount_total" in str(w.message) and "amount" in str(w.message).replace("amount_total", "")]
        assert broken, [str(w.message) for w in caught]
