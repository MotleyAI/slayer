"""Every AGG rule holds whichever node sqlglot parses ``AGG(...)`` into (``exp.Anonymous`` before 30.19, ``exp.Agg``
from 30.19). The ``agg`` half skips where sqlglot lacks the node; CI's sqlglot-latest job runs it.

Spec: models/semantic-model-documents › Both sqlglot shapes of AGG are checked.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

import pytest

from tests._dev2243_fixtures import DS, SALES, document_engine, error, metric
from tests._dev2277_fixtures import by
from tests._dev2394_fixtures import (
    AGG_SHAPES, MEASURE_REASON, force_agg_shape, mcp_reply, model_metric_over_order_revenue, orders_metrics,
    orders_with_field, source_measures_text, stored_text, validity_errors,
)

DATASET_AGG = "AGG in a dataset-scoped metric takes a single source.measure reference"


@pytest.fixture(params=AGG_SHAPES)
def shape(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    force_agg_shape(monkeypatch, request.param)
    return request.param


pytestmark = pytest.mark.usefixtures("shape")


# --------------------------------------------------------------------------- #
# The validity check
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("doc", "expected"), [
    pytest.param(lambda: model_metric_over_order_revenue("AGG(nope)"), "AGG(nope) is not qualified", id="model-agg-nope"),
    pytest.param(lambda: model_metric_over_order_revenue("AGG(orders.order_revenue)"), None, id="model-agg-ok"),
    pytest.param(lambda: model_metric_over_order_revenue("AGG(orders.total_usd)"),
                 "names a field, and fields are never measures", id="model-agg-field"),
    pytest.param(lambda: model_metric_over_order_revenue("AGG(orders.order_revenue) FILTER (WHERE orders.order_id = 1)"),
                 "has a FILTER (WHERE ...) clause; AGG does not take one", id="model-agg-filter"),
    pytest.param(lambda: orders_metrics(revenue_total="AGG(source.revenue)"), None, id="dataset-source-measure"),
    pytest.param(lambda: orders_metrics(revenue_total="AGG(total_usd)"), DATASET_AGG, id="dataset-agg-field"),
    pytest.param(lambda: orders_with_field("AGG(source.revenue)"), "calls AGG in a field expression", id="field-agg"),
])
def test_the_validity_check_reads_both_shapes(doc: Any, expected: str | None) -> None:
    errors = validity_errors(doc())
    if expected is None:
        assert errors == []
    else:
        assert len(errors) == 1, errors
        assert expected in errors[0]


def test_agg_in_a_field_is_not_also_a_field_aggregate() -> None:
    errors = validity_errors(orders_with_field("AGG(source.revenue)"))
    assert len(errors) == 1, errors
    assert "OVER clause" not in errors[0]


def test_names_inside_a_dataset_agg_are_not_undeclared_fields() -> None:
    errors = validity_errors(orders_metrics(revenue_total="AGG(order_revenue)"))
    assert len(errors) == 1, errors
    assert "undeclared" not in errors[0]


# --------------------------------------------------------------------------- #
# The model-metric save rules
# --------------------------------------------------------------------------- #
@pytest.fixture
async def sales() -> AsyncGenerator[Any]:
    async with document_engine() as engine:
        yield engine


async def test_a_valid_model_scoped_agg_saves_and_evaluates(sales: Any) -> None:
    reply = await mcp_reply(sales.storage, "edit_semantic_model", name="sales", data_source=DS,
                            metrics=[metric("rev_total", "AGG(orders.total_revenue)")])
    assert "Error" not in reply, reply
    resp = await sales.execute({"dimensions": ["customers.region"], "metrics": [{"metric": "rev_total", "alias": "r"}]})
    values = by(resp, "customers.region", "r")
    assert (values["EAST"], values["WEST"]) == (350.0, 300.0)


@pytest.mark.parametrize(("sql", "error_name", "phrase"), [
    ("AGG(orders.amount)", "AggArgumentError", "AGG takes a dataset metric"),
    ("AGG(revenue_per_customer)", "AggArgumentError", "AGG takes a dataset metric"),
    ("AGG(orders.total_revenue + 1)", "AggArgumentError", "AGG takes a dataset metric"),
    ("AGG(orders.total_revenue) FILTER (WHERE orders.status = 'paid')", "AggFilterError", "AGG takes no FILTER clause"),
], ids=["field", "model-scoped-metric", "other-expression", "filter"])
async def test_a_model_scoped_agg_refusal_holds_for_both_shapes(sales: Any, sql: str, error_name: str,
                                                                phrase: str) -> None:
    before = await stored_text(sales)
    reply = await mcp_reply(sales.storage, "edit_semantic_model", name="sales", data_source=DS,
                            metrics=[metric("bad_agg", sql)])
    assert error_name in reply, reply
    assert phrase.lower() in reply.lower(), reply
    assert await stored_text(sales) == before


async def test_a_source_measure_dataset_metric_save_is_refused_for_both_shapes(sales: Any) -> None:
    before = await stored_text(sales)
    reply = await mcp_reply(sales.storage, "edit_dataset", dataset_name="orders", data_source=DS,
                            metrics=[{"name": "revenue", "expression": "AGG(source.revenue)"}])
    assert "UnevaluableEntityError" in reply, reply
    assert MEASURE_REASON in reply, reply
    assert await stored_text(sales) == before


# --------------------------------------------------------------------------- #
# The document loader's warning
# --------------------------------------------------------------------------- #
async def test_the_loader_warns_on_source_measures_for_both_shapes() -> None:
    async with document_engine(documents=(source_measures_text(),)) as engine:
        orders = await engine.storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        metrics = {m.name: m for m in orders.metrics}
        assert MEASURE_REASON in (metrics["revenue_total"].load_warning or "")
        assert metrics["order_count"].load_warning is None
        model = {m.name: m for m in await engine.storage.list_model_scoped_metrics(DS)}
        assert "revenue_total" in (model["revenue_per_customer"].load_warning or "")
        assert model["rpc_ok"].load_warning is None
        with pytest.raises(error("UnevaluableEntityError"), match="revenue_total"):
            await engine.execute({"population": "orders", "metrics": ["orders.revenue_total"]})


async def test_the_loader_keeps_model_scoped_agg_evaluable_for_both_shapes() -> None:
    async with document_engine(documents=(SALES,)) as engine:
        [rpc] = await engine.storage.list_model_scoped_metrics(DS)
        assert rpc.load_warning is None
        resp = await engine.execute({"metrics": ["revenue_per_customer"]})
    assert [float(r["revenue_per_customer"]) for r in resp.data] == [162.5]
