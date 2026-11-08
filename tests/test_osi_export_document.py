"""Ossie export: one valid, deterministic document per datasource; subsets; no direct database access."""

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database.client import DelegatingSqlClient
from tests._osi_export_fixtures import (
    BASIC_MEASURES, col, customers, measure, monthly_revenue, orders, rich_orders, visits,
)
from tests._osi_export_helpers import (
    carries, dataset, dataset_names, doc_text, export, export_models, make_env, mentions, reimport, relationships,
    save, validate,
)
from tests._osi_helpers import ds, f, flat


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path, description="Shop data")


@pytest.mark.parametrize("extensions", [True, False])
async def test_exported_document_validates(env, extensions: bool) -> None:
    result = await export_models(env, customers(), rich_orders(), visits(), monthly_revenue(), extensions=extensions)
    validate(result.document)
    assert result.document["version"] == "0.2.0.dev0"
    assert set(dataset_names(result.document)) == {"customers", "monthly_revenue", "orders", "visits"}


async def test_deterministic_output(env) -> None:
    await save(env, customers(), rich_orders(), visits(), monthly_revenue())
    first, second = await export(env), await export(env)
    assert doc_text(first.document) == doc_text(second.document)


async def test_datasets_ordered_by_model_name(env) -> None:
    await save(env, visits(), customers(), orders(measures=BASIC_MEASURES))
    assert dataset_names((await export(env)).document) == ["customers", "orders", "visits"]


async def _refuse(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("database accessed")


class _FailingClient(DelegatingSqlClient):
    """Fails on every database call."""

    execute = get_result_column_types = server_profile = ping = list_schemas = default_schema = _refuse
    current_catalog = list_objects = reflect_columns = reflect_table_keys = table_comment = _refuse
    schema_description = _refuse


async def test_no_database_access_for_table_and_sql_mode_models(env, monkeypatch: pytest.MonkeyPatch) -> None:
    await save(env, customers(), rich_orders(), visits())
    original = SlayerQueryEngine.client_for

    def failing(self: SlayerQueryEngine, datasource: DatasourceConfig) -> _FailingClient:
        return _FailingClient(inner=original(self, datasource))

    monkeypatch.setattr(SlayerQueryEngine, "client_for", failing)
    result = await export(env)
    validate(result.document)
    assert dataset_names(result.document) == ["customers", "orders", "visits"]


async def test_name_and_description_from_datasource(env) -> None:
    result = await export_models(env, customers())
    assert result.document["name"] == env.datasource
    assert result.document["description"] == "Shop data"


def _retail_document() -> dict:
    doc = flat([ds("customers", fields=[f("id", "c_custkey"), f("segment")], primary_key=["id"]),
                ds("visits", fields=[f("visit_id"), f("status")])], name="retail")
    doc["description"] = "Retail model"
    return doc


async def test_name_and_description_from_shared_semantic_model(env) -> None:
    imported, _ = await reimport(env, _retail_document())
    document = (await export(imported)).document
    assert document["name"] == "retail"
    assert document["description"] == "Retail model"


async def test_semantic_model_not_shared_by_every_model_falls_back_to_datasource(env) -> None:
    imported, _ = await reimport(env, _retail_document())
    await save(imported, orders(measures=BASIC_MEASURES, joins=[]))
    document = (await export(imported)).document
    assert document["name"] == env.datasource
    assert document["description"] == "Shop data"


async def test_join_to_excluded_model_is_reported(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES), subset=["orders"])
    assert dataset_names(result.document) == ["orders"]
    assert relationships(result.document) == []
    [entry] = mentions(result, "customers", severity="dropped")
    assert entry.suggestion is not None
    assert "customers" in entry.suggestion
    validate(result.document)


@pytest.mark.parametrize("name", ["segment", "discounted"])
async def test_slayer_only_entity_needing_excluded_model_is_reported(env, name: str) -> None:
    model = orders(measures=[*BASIC_MEASURES, measure("discounted", "sum(amount * customers.rate)")])
    model.columns.append(col("segment", DataType.TEXT, "customers.segment"))
    result = await export_models(env, customers(), model, subset=["orders"])
    [entry] = mentions(result, name, severity="dropped")
    assert entry.suggestion is not None
    assert "customers" in entry.suggestion
    assert not carries(dataset(result.document, "orders"), name)


async def test_measure_needing_excluded_model_is_reported(env) -> None:
    measures = [*BASIC_MEASURES, measure("customer_discount", "sum(customers.discount)")]
    result = await export_models(env, customers(), orders(measures=measures), subset=["orders"])
    assert mentions(result, "customer_discount", severity="dropped")
    assert all(m["name"] != "customer_discount" for d in result.document["datasets"] for m in d.get("metrics") or [])
