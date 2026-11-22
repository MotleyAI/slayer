"""Shared documents and helpers for the Ossie corpus catch-up tests (source measures, validity, portable SQL)."""

from __future__ import annotations

import contextlib
import copy
import os
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import sqlglot
import yaml
from sqlglot import exp, parser

from slayer.core import slayer_extension as ext
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.base import StorageBackend
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2243_fixtures import DS, SALES, make_storage, metric, semantic_model_module, store_document
from tests._engine_helpers import owned_query_engine

AGG_SHAPES = ("anonymous", "agg")
MEASURE_REASON = "measure of its source"


def force_agg_shape(monkeypatch: pytest.MonkeyPatch, shape: str) -> None:
    """Make the default sqlglot reader parse ``AGG(...)`` as ``shape``; skips ``agg`` where sqlglot lacks ``exp.Agg``."""
    if shape == "anonymous":
        monkeypatch.delitem(parser.Parser.FUNCTIONS, "AGG", raising=False)
        expected: type = exp.Anonymous
    else:
        agg = getattr(exp, "Agg", None)
        if agg is None:
            pytest.skip("this sqlglot has no exp.Agg; the sqlglot-latest CI job runs this shape")
        expected = agg
    assert type(sqlglot.parse_one("AGG(a.b)")) is expected


def validity_errors(doc: dict[str, Any]) -> list[str]:
    return semantic_model_module().ossie_validity_errors(doc)


# --------------------------------------------------------------------------- #
# Upstream's renamed model (validation/tests/test_validate.py `_renamed_model`), as a mapping
# --------------------------------------------------------------------------- #
def _field(name: str, sql: str, dialect: str | None = None) -> dict[str, Any]:
    if dialect is None:
        return {"name": name, "expression": {"sql": sql}}
    return {"name": name, "expression": {"dialects": [{"dialect": dialect, "expression": sql}]}}


def renamed_model() -> dict[str, Any]:
    return {
        "version": "0.3.0.dev0",
        "name": "m",
        "datasets": [
            {"name": "orders", "source": "db.s.orders", "primary_key": ["order_id"],
             "fields": [_field("order_id", "o_orderkey"), _field("customer_id", "o_custkey"),
                        _field("total_usd", "total_cents / 100.0")]},
            {"name": "customers", "source": "db.s.customers", "primary_key": ["id"], "unique_keys": [["email"]],
             "fields": [_field("id", "c_custkey"), _field("email", "c_email")]},
        ],
        "relationships": [{"name": "orders_to_customers", "from": "orders", "to": "customers",
                           "from_columns": ["customer_id"], "to_columns": ["id"]}],
        "metrics": [{"name": "total_revenue", "expression": {"sql": "SUM(orders.total_usd)"}}],
    }


def orders_metrics(**metrics: str) -> dict[str, Any]:
    """The renamed model with these portable dataset-scoped metrics on ``orders``."""
    doc = renamed_model()
    doc["datasets"][0]["metrics"] = [{"name": n, "expression": {"sql": s}} for n, s in metrics.items()]
    return doc


def orders_with_field(expression: str | dict[str, Any], *, name: str = "revenue") -> dict[str, Any]:
    """The renamed model with one more field on ``orders``: a string is its ``sql``, a dict its expression object."""
    doc = renamed_model()
    expr = {"sql": expression} if isinstance(expression, str) else expression
    doc["datasets"][0]["fields"].append({"name": name, "expression": expr})
    return doc


def model_metric_over_order_revenue(expression: str) -> dict[str, Any]:
    """The renamed model with ``orders.order_revenue`` and one model-scoped metric ``total_revenue``."""
    doc = renamed_model()
    doc["datasets"][0]["metrics"] = [{"name": "order_revenue", "expression": {"sql": "SUM(total_usd)"}}]
    doc["metrics"] = [{"name": "total_revenue", "expression": {"sql": expression}}]
    return doc


# --------------------------------------------------------------------------- #
# The source-measure document over the Appendix B seed
# --------------------------------------------------------------------------- #
RPC_SM = "AGG(orders.revenue_total) / COUNT(DISTINCT customers.customer_id)"
RPC_OK = "AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)"


def source_measures_doc(*, extra_slayer_metrics: list[ext.ModelMetricContent] | None = None) -> dict[str, Any]:
    """``SALES`` plus ``orders.revenue_total = AGG(source.revenue)``, ``orders.order_count = COUNT(*)``, the
    model-scoped ``revenue_per_customer`` over ``revenue_total``, ``rpc_ok`` over ``total_revenue``, and
    ``doubled = revenue_per_customer * 2`` in the document's ``SLAYER`` entry."""
    doc = yaml.safe_load(SALES)
    orders = next(d for d in doc["datasets"] if d["name"] == "orders")
    orders["metrics"] += [metric("revenue_total", "AGG(source.revenue)"), metric("order_count", "COUNT(*)")]
    doc["metrics"] = [metric("revenue_per_customer", RPC_SM), metric("rpc_ok", RPC_OK)]
    slayer_metrics = [ext.ModelMetricContent(name="doubled", expression="revenue_per_customer * 2"),
                      *(extra_slayer_metrics or [])]
    payload = ext.DocumentPayload(entity="sales", metrics=slayer_metrics)
    doc["nonportable_extensions"] = [ext.encode(payload, ossie="sales")]
    return doc


def source_measures_text(**kwargs: Any) -> str:
    return yaml.safe_dump(source_measures_doc(**kwargs), sort_keys=False)


def with_relationship_named_source() -> str:
    """``SALES`` with ``customers.revenue = COUNT(*)`` and a to-one relationship ``source`` from orders to customers."""
    doc = copy.deepcopy(yaml.safe_load(SALES))
    customers = next(d for d in doc["datasets"] if d["name"] == "customers")
    customers["metrics"] = [metric("revenue", "COUNT(*)")]
    doc["relationships"].append({"name": "source", "from": "orders", "to": "customers",
                                 "from_columns": ["customer_id"], "to_columns": ["customer_id"]})
    return yaml.safe_dump(doc, sort_keys=False)


# --------------------------------------------------------------------------- #
# MCP helpers
# --------------------------------------------------------------------------- #
async def mcp_reply(storage: StorageBackend, tool: str, **arguments: Any) -> str:
    """One MCP ``tool`` call on a fresh server: its text, or the raised error's type and text."""
    server = create_mcp_server(storage=storage, _seed_help=False)
    try:
        blocks, _ = await server.call_tool(name=tool, arguments=arguments)
        return "\n".join(getattr(b, "text", "") for b in blocks)
    except Exception as exc:  # noqa: BLE001 — the refusal text is the assertion subject
        return f"{type(exc).__name__}: {exc}"
    finally:
        await getattr(server, "_slayer_engine").aclose()


async def stored_text(engine: SlayerQueryEngine, name: str = "sales") -> str:
    text = await engine.storage.get_semantic_model_text(name, data_source=DS)
    assert text is not None
    return text


# --------------------------------------------------------------------------- #
# A dated orders table for the portable-SQL writer tests
# --------------------------------------------------------------------------- #
DATED_ORDERS = TableSpec(
    name="orders", columns=[("order_id", "TEXT"), ("amount", "DOUBLE"), ("ordered_at", "DATE")],
    rows=[("O1", 100.0, "2026-01-05"), ("O2", 200.0, "2026-01-20"), ("O3", 50.0, "2026-02-01")],
)
#: ``ordered_at + 7 days`` per order.
DUE = {"O1": "2026-01-12", "O2": "2026-01-27", "O3": "2026-02-08"}


@contextlib.asynccontextmanager
async def dated_engine(*, kind: str = "yaml", documents: tuple[str, ...] = (), base: Path | None = None,
                       ) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing DuckDB engine over ``DATED_ORDERS`` with ``documents`` stored in datasource ``shop``."""
    with tempfile.TemporaryDirectory() as tmp:
        root = base or Path(tmp)
        db_path = os.path.join(tmp, "dated.duckdb")
        seed_backend("duckdb", db_path, [DATED_ORDERS])
        storage = make_storage(kind, root)
        ds = DatasourceConfig(name=DS, type="duckdb", database=db_path)
        async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
            for text in documents:
                await store_document(storage, text)
            yield engine


def dates(rows: list[dict[str, Any]], *, key: str, value: str) -> dict[str, str]:
    """``{row[key]: str(row[value])[:10]}``."""
    return {r[key]: str(r[value])[:10] for r in rows}
