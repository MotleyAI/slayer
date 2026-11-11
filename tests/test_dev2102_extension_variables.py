"""What only a stage extension's definitions read — a ``{var}``, a model — counts as read by the stage."""

from __future__ import annotations

import json

import pytest

from slayer.core.models import ModelJoin, SlayerModel
from slayer.engine.model_reads import models_read_by
from slayer.inspect.model_render import render_model_inspection

from tests._dev1966_fixtures import customers_model, dev1966_engine, m, orders_model, query
from tests._dev2102_fixtures import DS, countries, customers, ext, join, orders, q, regions

_OVER = {"source_name": "orders", "columns": [
    {"name": "over", "sql": "CASE WHEN amount >= {floor} THEN amount ELSE 0 END", "type": "DOUBLE"}]}


def _over_rev(**fields) -> SlayerModel:
    return SlayerModel(name="over_rev", data_source="test", source_queries=[query(
        source_model="orders", extensions=[_OVER], dimensions=["customer_id"], measures=[m("sum(over)", "rev")])],
        **fields)


def _wrap(name: str, floor: int) -> SlayerModel:
    return SlayerModel(name=name, data_source="test", query_variables={"floor": floor}, source_queries=[
        query(source_model="over_rev", dimensions=["customer_id"], measures=[m("sum(rev)", "r")])])


async def test_conflicting_contexts_of_an_extension_variable_fail_closed() -> None:
    models = [orders_model(), customers_model(), _over_rev(query_variables={"floor": 0}),
              _wrap("wrap_a", 20), _wrap("wrap_b", 30)]
    s1 = query(name="s1", source_model="wrap_a", dimensions=["customer_id"], measures=[m("sum(r)", "a")])
    s2 = query(name="s2", source_model="wrap_b", dimensions=["customer_id"], measures=[m("sum(r)", "b")])
    root = query(source_model="s1", extensions=[{"source_name": "s1", "joins": [
        {"target_model": "s2", "join_pairs": [["customer_id", "customer_id"]]}]}],
        dimensions=["customer_id", "a", "s2.b"])
    async with dev1966_engine("sqlite", models=models) as e:
        with pytest.raises(ValueError, match="over_rev"):
            await e.execute([s1, s2, root])


async def test_inspection_reports_an_extension_variable_as_required() -> None:
    async with dev1966_engine("sqlite", models=[orders_model(), customers_model()]) as e:
        rendered = await render_model_inspection(model=_over_rev(), storage=e.storage, engine=None, format="json")
    assert json.loads(rendered)["backing_query"]["required_variables"] == ["floor"]


@pytest.mark.parametrize("extension", [
    ext("customers", columns=[{"name": "region_name", "sql": "regions.name", "type": "TEXT"}]),
    ext("customers", measures=[{"name": "region_count", "formula": "count(regions.id)"}]),
], ids=["column", "measure"])
def test_a_joined_models_extension_reads_what_its_definitions_read(extension: dict) -> None:
    home = ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])
    query = q(source_model="orders", dimensions=["status"], extensions=[extension])
    reads = models_read_by(query, models=[orders(), customers(home), regions()], data_source=DS)
    assert (DS, "regions") in reads


_TO_REGIONS = join("regions", [["region_id", "id"]])


@pytest.mark.parametrize(("source", "dimensions", "extension"), [
    ({"source_model": "orders"}, ["customers.regions.countries.name"], ext("customers", joins=[_TO_REGIONS])),
    ({"source_model": "orders"}, ["status"], ext("customers", joins=[_TO_REGIONS],
                                               columns=[{"name": "country", "sql": "regions.countries.name",
                                                         "type": "TEXT"}])),
    ({}, ["customers.regions.countries.name"], ext("customers", joins=[_TO_REGIONS])),
], ids=["query-reference", "extension-column", "anchorless"])
def test_a_path_through_an_extension_join_reads_its_downstream_models(
    source: dict, dimensions: list, extension: dict,
) -> None:
    to_countries = ModelJoin(target_model="countries", join_pairs=[["country_id", "id"]])
    models = [orders(), customers(), regions().model_copy(update={"joins": [to_countries]}), countries()]
    reads = models_read_by(q(**source, dimensions=dimensions, extensions=[extension]), models=models, data_source=DS)
    assert (DS, "countries") in reads


_TO_CUSTOMERS = join("customers", [["customer_id", "id"]])


@pytest.mark.parametrize(("dimensions", "extension"), [
    (["customer_id"], ext("s", joins=[_TO_CUSTOMERS], measures=[{"name": "n", "formula": "count(customers.regions.id)"}])),
    (["customer_id"], ext("s", joins=[_TO_CUSTOMERS],
                          columns=[{"name": "region", "sql": "customers.regions.name", "type": "TEXT"}])),
    (["customer_id", "customers.regions.name"], ext("s", joins=[_TO_CUSTOMERS])),
], ids=["measure", "column", "query-reference"])
@pytest.mark.parametrize("data_source", [DS, ""], ids=["datasource", "no-datasource"])
def test_a_sibling_stage_extension_reads_through_its_joins(dimensions: list, extension: dict, data_source: str) -> None:
    home = ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])
    backed = SlayerModel(name="backed", data_source=data_source, source_queries=[
        q(name="s", source_model="orders", dimensions=["customer_id"]),
        q(source_model="s", dimensions=dimensions, extensions=[extension])])
    assert (DS, "regions") in models_read_by(backed, models=[orders(), customers(home), regions()])
