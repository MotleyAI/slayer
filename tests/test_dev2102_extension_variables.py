"""What only a stage extension's definitions read — a ``{var}``, a dataset — counts as read by the stage."""

from __future__ import annotations

import json

import pytest

from slayer.core.models import DatasetRelationship, SlayerDataset
from slayer.engine.dataset_reads import datasets_read_by
from slayer.inspect.dataset_render import render_dataset_inspection

from tests._dev1966_fixtures import customers_dataset, dev1966_engine, m, orders_dataset, query
from tests._dev2102_fixtures import DS, countries, customers, ext, join, orders, q, regions

_OVER = {"dataset": "orders", "metrics": [
    {"name": "rev", "expression": "sum(iif(amount >= {floor}, amount, 0))"}]}


def _over_rev(**fields) -> SlayerDataset:
    return SlayerDataset(name="over_rev", data_source="test", source_queries=[query(
        population="orders", extensions=[_OVER], dimensions=["customer_id"], metrics=["rev"])],
        **fields)


def _wrap(name: str, floor: int) -> SlayerDataset:
    return SlayerDataset(name=name, data_source="test", query_variables={"floor": floor}, source_queries=[
        query(population="over_rev", dimensions=["customer_id"], metrics=[m("sum(rev)", "r")])])


async def test_conflicting_contexts_of_an_extension_variable_fail_closed() -> None:
    models = [orders_dataset(), customers_dataset(), _over_rev(query_variables={"floor": 0}),
              _wrap("wrap_a", 20), _wrap("wrap_b", 30)]
    s1 = query(name="s1", population="wrap_a", dimensions=["customer_id"], metrics=[m("sum(r)", "a")])
    s2 = query(name="s2", population="wrap_b", dimensions=["customer_id"], metrics=[m("sum(r)", "b")])
    root = query(population="s1", extensions=[{"dataset": "s1", "relationships": [
        {"to": "s2", "from_columns": ["customer_id"], "to_columns": ["customer_id"]}]}],
        dimensions=["customer_id", "a", "s2.b"])
    async with dev1966_engine("sqlite", datasets=models) as e:
        with pytest.raises(ValueError, match="over_rev"):
            await e.execute([s1, s2, root])


async def test_inspection_reports_an_extension_variable_as_required() -> None:
    async with dev1966_engine("sqlite", datasets=[orders_dataset(), customers_dataset()]) as e:
        rendered = await render_dataset_inspection(dataset=_over_rev(), storage=e.storage, engine=None, format="json")
    assert json.loads(rendered)["backing_query"]["required_variables"] == ["floor"]


@pytest.mark.parametrize("extension", [
    ext("customers", metrics=[{"name": "region_count", "expression": "count(regions.id)"}]),
], ids=["measure"])
def test_a_joined_models_extension_reads_what_its_definitions_read(extension: dict) -> None:
    home = DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])
    query = q(population="orders", dimensions=["status"], extensions=[extension])
    reads = datasets_read_by(query, datasets=[orders(), customers(home), regions()], data_source=DS)
    assert (DS, "regions") in reads


_TO_REGIONS = join("regions", [["region_id", "id"]])


@pytest.mark.parametrize(("source", "dimensions", "extension"), [
    ({"population": "orders"}, ["customers.regions.countries.name"], ext("customers", relationships=[_TO_REGIONS])),
    ({"population": "orders"}, ["status"], ext("customers", relationships=[_TO_REGIONS],
                                               metrics=[{"name": "countries_n",
                                                          "expression": "count_distinct(regions.countries.name)"}])),
    ({}, ["customers.regions.countries.name"], ext("customers", relationships=[_TO_REGIONS])),
], ids=["query-reference", "extension-measure", "anchorless"])
def test_a_path_through_an_extension_join_reads_its_downstream_models(
    source: dict, dimensions: list, extension: dict,
) -> None:
    to_countries = DatasetRelationship(to="countries", from_columns=["country_id"], to_columns=["id"])
    models = [orders(), customers(), regions().model_copy(update={"relationships": [to_countries]}), countries()]
    reads = datasets_read_by(q(**source, dimensions=dimensions, extensions=[extension]), datasets=models, data_source=DS)
    assert (DS, "countries") in reads


_TO_CUSTOMERS = join("customers", [["customer_id", "id"]])


@pytest.mark.parametrize(("dimensions", "extension"), [
    (["customer_id"], ext("s", relationships=[_TO_CUSTOMERS], metrics=[{"name": "n", "expression": "count(customers.regions.id)"}])),
    (["customer_id", {"field": "upper(customers.regions.name)", "alias": "region"}],
     ext("s", relationships=[_TO_CUSTOMERS])),
    (["customer_id", "customers.regions.name"], ext("s", relationships=[_TO_CUSTOMERS])),
], ids=["measure", "computed-dimension", "query-reference"])
@pytest.mark.parametrize("data_source", [DS, ""], ids=["datasource", "no-datasource"])
def test_a_sibling_stage_extension_reads_through_its_joins(dimensions: list, extension: dict, data_source: str) -> None:
    home = DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])
    backed = SlayerDataset(name="backed", data_source=data_source, source_queries=[
        q(name="s", population="orders", dimensions=["customer_id"]),
        q(population="s", dimensions=dimensions, extensions=[extension])])
    assert (DS, "regions") in datasets_read_by(backed, datasets=[orders(), customers(home), regions()])


@pytest.mark.parametrize("column", ["name", "label"], ids=["stored-column-name", "fresh-name"])
def test_a_sibling_stage_shadows_a_same_named_stored_model(column: str) -> None:
    backed = SlayerDataset(name="backed", data_source=DS, source_queries=[
        q(name="customers", population="orders", dimensions=["customer_id"]),
        q(population="customers", metrics=[column], extensions=[
            ext("customers", metrics=[{"name": column, "expression": "sum(customer_id * 10)"}])])])
    assert datasets_read_by(backed, datasets=[orders(), customers(), regions()]) == {(DS, "orders")}


def test_a_path_through_chained_sibling_stages_reads_its_downstream_models() -> None:
    home = DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])
    backed = SlayerDataset(name="backed", data_source=DS, source_queries=[
        q(name="s2", population="orders", dimensions=["customer_id"]),
        q(name="s1", population="orders", dimensions=["customer_id"]),
        q(population="s1", dimensions=["customer_id", "s2.customers.regions.name"], extensions=[
            ext("s1", relationships=[join("s2", [["customer_id", "customer_id"]])]), ext("s2", relationships=[_TO_CUSTOMERS])])])
    assert (DS, "regions") in datasets_read_by(backed, datasets=[orders(), customers(home), regions()])


def test_a_short_form_reference_reads_the_models_on_its_route() -> None:
    home = DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])
    query = q(population="orders", dimensions=["regions.name"])
    reads = datasets_read_by(query, datasets=[orders(), customers(home), regions()], data_source=DS)
    assert {(DS, "customers"), (DS, "regions")} <= reads


def test_a_product_read_reads_the_unrelated_model() -> None:
    query = q(population="orders", dimensions=["status"], metrics=[{"metric": "count(countries.id)", "alias": "n"}])
    reads = datasets_read_by(query, datasets=[orders(), customers(), regions(), countries()], data_source=DS)
    assert (DS, "countries") in reads


def test_a_rootless_sibling_stage_extension_reads_through_its_joins() -> None:
    home = DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])
    backed = SlayerDataset(name="backed", data_source="", source_queries=[
        q(name="s", dimensions=["orders.customer_id"]),
        q(population="s", dimensions=["customer_id", "customers.regions.name"],
          extensions=[ext("s", relationships=[_TO_CUSTOMERS])])])
    assert (DS, "regions") in datasets_read_by(backed, datasets=[orders(), customers(home), regions()])


def test_a_sibling_join_key_its_extension_defines_is_one_column() -> None:
    home = DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])
    backed = SlayerDataset(name="backed", data_source=DS, source_queries=[
        q(name="s", population="orders", dimensions=[{"field": "customer_id", "alias": "customer_key"}]),
        q(population="s", dimensions=["customers.regions.name"], extensions=[ext(
            "s", relationships=[join("customers", [["customer_key", "id"]])])])])
    assert (DS, "regions") in datasets_read_by(backed, datasets=[orders(), customers(home), regions()])

