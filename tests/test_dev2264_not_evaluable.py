"""Spec: models/semantic-model-documents › Entities SLayer cannot evaluate yet still load (expressions SLayer cannot
parse), plus the superset cases Ossie rejects but upstream ``validate.py`` passes, which stay stored and evaluated."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.errors import UnevaluableEntityError
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.service import SearchService
from tests._dev2243_fixtures import (
    DS, dataset, document_engine, document_text, error, field, importer_module, make_storage, metric, relationship,
)
from tests._dataset_access_fixtures import mcp_text

SUBQUERY = "(SELECT 1)"
UNPARSEABLE = [
    pytest.param(SUBQUERY, id="scalar-subquery"),
    pytest.param("CASE WHEN EXISTS (SELECT 1) THEN 1 ELSE 0 END", id="exists"),
    pytest.param("CASE WHEN amount IN (SELECT 1) THEN 1 ELSE 0 END", id="in-subquery"),
    pytest.param("(SELECT 1 UNION SELECT 2)", id="union"),
]


def orders_doc(*extra_fields: dict[str, Any], metrics: list[dict[str, Any]] | None = None,
               model_metrics: list[dict[str, Any]] | None = None) -> str:
    """Document ``ledger``: ``orders`` over the Appendix B seed plus ``extra_fields``."""
    return document_text(name="ledger", datasets=[
        dataset("orders", primary_key=["order_id"], metrics=metrics or [],
                fields=[field("order_id"), field("amount", datatype="Float"), field("status"), *extra_fields]),
    ], metrics=model_metrics)


def form(name: str, *, sql: str | None = None, entries: tuple[tuple[str, str], ...] = ()) -> dict[str, Any]:
    expression: dict[str, Any] = {"dialects": [{"dialect": d, "expression": e} for d, e in entries]} if entries else {}
    if sql is not None:
        expression = {"sql": sql, **expression}
    return {"name": name, "expression": expression, "datatype": "Integer"}


async def field_warning(engine: SlayerQueryEngine, name: str) -> str | None:
    orders = await engine.storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    [hit] = [f for f in orders.fields if f.name == name]
    return hit.load_warning


async def total_amount(engine: SlayerQueryEngine) -> float:
    resp = await engine.execute({"population": "orders", "metrics": [{"metric": "sum(orders.amount)", "alias": "v"}]})
    return float(resp.data[0]["v"])


def _no_database(_self: SlayerQueryEngine, _datasource: DatasourceConfig) -> Any:
    raise AssertionError("database accessed")


# --------------------------------------------------------------------------- #
# Expressions SLayer cannot parse load not evaluable
# --------------------------------------------------------------------------- #
async def test_an_expression_slayer_cannot_parse_loads_not_evaluable() -> None:
    """Spec scenario: An expression SLayer cannot parse loads not evaluable."""
    async with document_engine(documents=(orders_doc(field("flag", SUBQUERY, datatype="Integer")),)) as engine:
        _, errs = await engine.storage.load_datasets(data_source=DS)
        assert errs == []
        warning = await field_warning(engine, "flag")
        assert warning
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.orders.flag", entity_type="field")
        assert warning in inspected
        assert await total_amount(engine) == 650.0
        with pytest.raises(UnevaluableEntityError, match="flag") as info:
            await engine.execute({"population": "orders", "dimensions": ["orders.flag"]})
        assert info.value.reason == warning


async def test_an_unparseable_fields_warning_shows_on_summary_and_search() -> None:
    async with document_engine(documents=(orders_doc(field("flag", SUBQUERY, datatype="Integer")),)) as engine:
        warning = await field_warning(engine, "flag")
        assert warning
        summary = await mcp_text(engine.storage, "datasets_summary", datasource_name=DS, compact=False)
        assert warning in summary
        hits = await SearchService(storage=engine.storage).search(question="flag", compact=False, max_results=20)
        [hit] = [h for h in hits.results if h.id == f"{DS}.orders.flag"]
        assert warning in f"{hit.text} {hit.description or ''}"


@pytest.mark.parametrize("sql", UNPARSEABLE)
async def test_every_unparseable_construct_loads_not_evaluable(sql: str) -> None:
    async with document_engine(documents=(orders_doc(field("flag", sql, datatype="Integer")),)) as engine:
        assert await field_warning(engine, "flag")
        assert await field_warning(engine, "amount") is None
        assert await total_amount(engine) == 650.0
        with pytest.raises(error("UnevaluableEntityError"), match="flag"):
            await engine.execute({"population": "orders", "dimensions": ["orders.flag"]})


async def test_import_osi_reports_an_unparseable_field(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: An expression SLayer cannot parse loads not evaluable (import report)."""
    storage = make_storage("yaml", tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    monkeypatch.setattr(SlayerQueryEngine, "client_for", _no_database)
    path = tmp_path / "ledger.yaml"
    path.write_text(orders_doc(field("flag", SUBQUERY, datatype="Integer")), encoding="utf-8")
    result = await importer_module().import_osi(storage=storage, path=path, data_source=DS)
    assert result.stored == ["ledger"]
    [entry] = [e for e in result.entries if e.kind == "not_evaluable"]
    assert entry.entity == "flag"
    orders = await storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    [flag] = [f for f in orders.fields if f.name == "flag"]
    assert flag.load_warning
    assert flag.load_warning in entry.message


# Which form runs on DuckDB: its own entry, else the portable form (``sql``, else an OSSIE_SQL_2026 entry); an entry in
# another dialect is translated into a DuckDB entry. Only a portable form goes through SLayer's expression reader.
@pytest.mark.parametrize(("x", "evaluable"), [
    pytest.param(form("x", sql=SUBQUERY), False, id="sql-alone"),
    pytest.param(form("x", entries=(("OSSIE_SQL_2026", SUBQUERY),)), False, id="portable-entry-alone"),
    pytest.param(form("x", entries=(("OSSIE_SQL_2026", SUBQUERY), ("SNOWFLAKE", "amount"))), False,
                 id="portable-entry-beside-another-vendor"),
    pytest.param(form("x", sql=SUBQUERY, entries=(("DUCKDB", "amount"),)), True, id="sql-beside-native-entry"),
    pytest.param(form("x", sql="amount", entries=(("DUCKDB", SUBQUERY),)), True, id="native-entry-runs-verbatim"),
    pytest.param(form("x", entries=(("SNOWFLAKE", SUBQUERY),)), True, id="vendor-only"),
    pytest.param(form("x", entries=(("ANSI_SQL", SUBQUERY),)), True, id="ansi-only"),
])
async def test_not_evaluable_follows_the_form_that_runs(x: dict[str, Any], evaluable: bool) -> None:
    async with document_engine(documents=(orders_doc(x),)) as engine:
        warning = await field_warning(engine, "x")
        query = {"population": "orders", "dimensions": ["orders.x"]}
        if evaluable:
            assert warning is None
            assert (await engine.execute(query)).data
        else:
            assert warning
            with pytest.raises(error("UnevaluableEntityError"), match="x"):
                await engine.execute(query)


async def test_sql_keywords_inside_literals_stay_evaluable() -> None:
    """Spec scenario: SQL keywords inside literals stay evaluable."""
    kind = field("kind", "CASE WHEN status IN ('select', 'from') THEN 'where' END")
    async with document_engine(documents=(orders_doc(kind),)) as engine:
        assert await field_warning(engine, "kind") is None
        resp = await engine.execute({"population": "orders", "dimensions": ["orders.kind"]})
        assert {r["orders.kind"] for r in resp.data} == {None}


async def test_an_unparseable_dataset_metric_loads_not_evaluable() -> None:
    doc = orders_doc(metrics=[metric("odd", f"SUM(amount) + {SUBQUERY}"), metric("total", "SUM(amount)")])
    async with document_engine(documents=(doc,)) as engine:
        orders = await engine.storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        warnings = {m.name: m.load_warning for m in orders.metrics}
        assert warnings["odd"]
        assert warnings["total"] is None
        resp = await engine.execute({"population": "orders", "metrics": ["orders.total"]})
        assert [float(r["orders.total"]) for r in resp.data] == [650.0]
        with pytest.raises(error("UnevaluableEntityError"), match="odd"):
            await engine.execute({"population": "orders", "metrics": ["orders.odd"]})


async def test_an_unparseable_model_scoped_metric_loads_not_evaluable() -> None:
    doc = orders_doc(model_metrics=[metric("odd", f"SUM(orders.amount) + {SUBQUERY}"),
                                    metric("revenue", "SUM(orders.amount)")])
    async with document_engine(documents=(doc,)) as engine:
        warnings = {m.name: m.load_warning for m in await engine.storage.list_model_scoped_metrics(DS)}
        assert warnings["odd"]
        assert warnings["revenue"] is None
        resp = await engine.execute({"population": "orders", "metrics": ["revenue"]})
        assert [float(r["revenue"]) for r in resp.data] == [650.0]
        with pytest.raises(error("UnevaluableEntityError"), match="odd"):
            await engine.execute({"population": "orders", "metrics": ["odd"]})


# --------------------------------------------------------------------------- #
# Superset: what upstream validate.py passes stays stored and evaluated
# --------------------------------------------------------------------------- #
def shop_doc(*, orders_fields: tuple[dict[str, Any], ...] = (),model_metrics: list[dict[str, Any]] | None = None) -> str:
    return document_text(
        name="shop",
        datasets=[
            dataset("orders", primary_key=["order_id"],
                    fields=[field("order_id"), field("customer_id"), field("amount", datatype="Float"),
                            *orders_fields]),
            dataset("customers", primary_key=["customer_id"], fields=[field("customer_id"), field("region")]),
        ],
        relationships=[relationship("orders_to_customers", src="orders", to="customers",
                                    from_columns=["customer_id"], to_columns=["customer_id"])],
        metrics=model_metrics,
    )


async def test_a_name_over_128_characters_is_stored_and_evaluated() -> None:
    long_name = "amount_" + "x" * 122
    assert len(long_name) == 129
    async with document_engine(documents=(shop_doc(orders_fields=(field(long_name, "amount", datatype="Float"),)),)) \
            as engine:
        resp = await engine.execute({"population": "orders",
                                     "metrics": [{"metric": f"sum(orders.{long_name})", "alias": "v"}]})
        assert [float(r["v"]) for r in resp.data] == [650.0]


async def test_a_field_qualified_by_another_dataset_is_stored_and_evaluated() -> None:
    buyer_region = field("buyer_region", "customers.region")
    async with document_engine(documents=(shop_doc(orders_fields=(buyer_region,)),)) as engine:
        resp = await engine.execute({"population": "orders", "dimensions": ["orders.buyer_region"],
                                     "metrics": [{"metric": "sum(orders.amount)", "alias": "v"}],
                                     "order": [{"field": "orders.buyer_region"}]})
        assert [(r["orders.buyer_region"], float(r["v"])) for r in resp.data] == [("EAST", 350.0), ("WEST", 300.0)]


async def test_a_model_metric_filter_on_another_dataset_is_stored_and_evaluated() -> None:
    east = metric("east_revenue", "SUM(orders.amount) FILTER (WHERE customers.region = 'EAST')")
    async with document_engine(documents=(shop_doc(model_metrics=[east]),)) as engine:
        resp = await engine.execute({"population": "orders", "metrics": ["east_revenue"]})
        assert [float(r["east_revenue"]) for r in resp.data] == [350.0]


async def test_a_model_metric_naming_another_is_stored_and_evaluated() -> None:
    metrics = [metric("revenue", "SUM(orders.amount)"), metric("double_revenue", "revenue * 2")]
    async with document_engine(documents=(shop_doc(model_metrics=metrics),)) as engine:
        resp = await engine.execute({"population": "orders", "metrics": ["double_revenue"]})
        assert [float(r["double_revenue"]) for r in resp.data] == [1300.0]
