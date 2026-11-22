"""Spec: models/semantic-model-documents › Entities SLayer cannot evaluate yet still load (expressions SLayer cannot
parse), plus the superset cases Ossie rejects but upstream ``validate.py`` passes, which stay stored and evaluated."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core import slayer_extension as ext
from slayer.core.enums import DataType
from slayer.core.errors import UnevaluableEntityError
from slayer.core.models import DatasourceConfig
from slayer.core.ossie_expression import OSIExpression
from slayer.core.syntax import MAX_NESTING, parse_expr
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.service import SearchService
from slayer.sql.ossie_dialects import expression_digest
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
    pytest.param("(" * 100 + "amount" + ")" * 100, id="nesting-too-deep"),
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
@pytest.mark.parametrize(("x", "runs"), [
    pytest.param(form("x", sql=SUBQUERY), None, id="sql-alone"),
    pytest.param(form("x", entries=(("OSSIE_SQL_2026", SUBQUERY),)), None, id="portable-entry-alone"),
    pytest.param(form("x", entries=(("OSSIE_SQL_2026", SUBQUERY), ("SNOWFLAKE", "amount"))), None,
                 id="portable-entry-beside-another-vendor"),
    pytest.param(form("x", sql=SUBQUERY, entries=(("DUCKDB", "amount"),)), "amount", id="sql-beside-native-entry"),
    pytest.param(form("x", sql="amount", entries=(("DUCKDB", SUBQUERY),)), "subquery",
                 id="native-entry-runs-verbatim"),
    pytest.param(form("x", entries=(("SNOWFLAKE", SUBQUERY),)), "subquery", id="vendor-only"),
    pytest.param(form("x", entries=(("ANSI_SQL", SUBQUERY),)), "subquery", id="ansi-only"),
])
async def test_not_evaluable_follows_the_form_that_runs(x: dict[str, Any], runs: str | None) -> None:
    """``runs``: which expression the DuckDB query evaluates (``amount`` or ``(SELECT 1)``), ``None`` when none."""
    async with document_engine(documents=(orders_doc(x),)) as engine:
        warning = await field_warning(engine, "x")
        query = {"population": "orders", "dimensions": ["orders.x"]}
        if runs is not None:
            assert warning is None
            rows = (await engine.execute({**query, "dimensions": ["orders.x", "orders.amount"]})).data
            assert rows
            expected = [float(r["orders.amount"]) if runs == "amount" else 1.0 for r in rows]
            assert [float(r["orders.x"]) for r in rows] == expected
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


def test_an_expression_nested_past_the_limit_is_a_parse_error() -> None:
    with pytest.raises(ValueError, match=f"nest more than {MAX_NESTING} deep"):
        parse_expr("(" * (MAX_NESTING + 1) + "amount" + ")" * (MAX_NESTING + 1))


def test_parentheses_inside_literals_do_not_count_towards_the_limit() -> None:
    deep = "'" + "(" * 100 + "'"
    outer = MAX_NESTING - 1  # concat( is the last level
    assert parse_expr("(" * outer + f"concat(status, {deep})" + ")" * outer) is not None


def restored_field(*, core_sql: str, expression: str) -> dict[str, Any]:
    """Field ``x`` written as ``core_sql``, with a matching ``SLAYER`` entry restoring ``expression``."""
    payload = ext.FieldPayload(entity="x", field=ext.FieldContent(expression=expression, datatype=DataType.INTEGER))
    entry = ext.encode(payload, ossie=expression_digest(OSIExpression(sql=core_sql)))
    return {**form("x", sql=core_sql), "nonportable_extensions": [entry]}


async def test_a_restored_field_is_judged_by_the_expression_it_restores() -> None:
    async with document_engine(documents=(orders_doc(restored_field(core_sql=SUBQUERY, expression="amount")),)) as engine:
        assert await field_warning(engine, "x") is None
        rows = (await engine.execute({"population": "orders", "dimensions": ["orders.x", "orders.amount"]})).data
        assert rows
        assert [float(r["orders.x"]) for r in rows] == [float(r["orders.amount"]) for r in rows]


async def test_a_restored_unparseable_field_loads_not_evaluable() -> None:
    async with document_engine(documents=(orders_doc(restored_field(core_sql="amount", expression=SUBQUERY)),)) as engine:
        assert await field_warning(engine, "x")
        with pytest.raises(error("UnevaluableEntityError"), match="x"):
            await engine.execute({"population": "orders", "dimensions": ["orders.x"]})


async def test_a_restored_field_drops_the_window_warning_of_its_document_form() -> None:
    windowed = restored_field(core_sql="SUM(amount) OVER ()", expression="amount")
    async with document_engine(documents=(orders_doc(windowed),)) as engine:
        assert await field_warning(engine, "x") is None
        rows = (await engine.execute({"population": "orders", "dimensions": ["orders.x", "orders.amount"]})).data
        assert [float(r["orders.x"]) for r in rows] == [float(r["orders.amount"]) for r in rows]
