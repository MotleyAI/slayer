"""Differential harness: the static readers against the statement the engine plans for the same query."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.extensions import query_scopes
from slayer.engine.dataset_reads import datasets_read_by
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.statement_reads import StatementReads
from slayer.live_schema.schema_drift import ToDeleteEntry, WholeDatasetDelete, field_dependents
from tests._dev2102_fixtures import (
    DS, CUSTOMER_PAIRS, base_datasets, customers, ext, join, q, regions, respelled_dataset, shop,
)

_HARNESS_QB = "harness_qb"
_AMOUNT = {"metric": "sum(amount)", "alias": "a"}
_REGION_PAIRS = [["region_id", "id"]]
_SPENT_BY_CUSTOMER = {"population": "orders", "dimensions": ["customer_id"],
                      "metrics": [{"metric": "sum(amount)", "alias": "spent"}]}


def _datasets() -> list[SlayerDataset]:
    """The shop, with ``customers → regions → countries`` for short-form routes and a query-backed ``cust_spend``."""
    home = DatasetRelationship(to="regions", from_columns=[p[0] for p in _REGION_PAIRS], to_columns=[p[1] for p in _REGION_PAIRS])
    country = DatasetRelationship(to="countries", from_columns=["country_id"], to_columns=["id"])
    replaced = {"customers": customers(home), "regions": regions().model_copy(update={"relationships": [country]})}
    cust_spend = SlayerDataset(name="cust_spend", data_source=DS,
                             source_queries=[SlayerQuery.model_validate(_SPENT_BY_CUSTOMER)])
    return [replaced.get(m.name, m) for m in base_datasets()] + [cust_spend, _cities()]


def _cities(*extra: dict, name: str = "cities_src") -> SlayerDataset:
    """A stored dataset over ``addresses``."""
    return SlayerDataset.model_validate({"name": name, "data_source": DS, "sql_table": "addresses", "fields": [
        {"name": "id", "datatype": "Integer", "primary_key": True}, {"name": "city", "datatype": "String"}, *extra]})

_CORPUS: list[Any] = [
    pytest.param(q(population="orders", dimensions=["regions.name"], metrics=[_AMOUNT],
                   extensions=[ext("orders", relationships=[join("regions", [["ship_region_id", "id"]], name="ship_region")])]),
                 id="root-extension-join"),
    pytest.param(q(population="orders", dimensions=[{"field": "upper(customers.tier)", "alias": "tier_label"}],
                   metrics=[_AMOUNT]), id="joined-computed-dimension"),
    pytest.param(q(population="orders", metrics=[{"metric": "customers.credit_total", "alias": "ct"}],
                   extensions=[ext("customers", metrics=[{"name": "credit_total", "expression": "sum(credit)"}])]),
                 id="joined-extension-measure"),
    pytest.param(q(population="orders", dimensions=["status", "customers.regions.countries.name"],
                   extensions=[ext("regions", metrics=[{"name": "nm", "expression": "max(upper(name))"}])]),
                 id="multi-hop-stored-path"),
    pytest.param(q(population="orders", metrics=[{"metric": "customers.regions.nm", "alias": "nm"}],
                   extensions=[ext("regions", metrics=[{"name": "nm", "expression": "max(upper(name))"}])]),
                 id="multi-hop-extension-measure"),
    pytest.param([q(name="s", population="orders", dimensions=["customer_id"], metrics=[_AMOUNT]),
                  q(population="s", dimensions=["customers.tier"], metrics=[{"metric": "sum(a)", "alias": "t"}],
                    extensions=[ext("s", relationships=[join("customers", [["customer_id", "id"]])])])],
                 id="sibling-extension-join"),
    pytest.param([q(name="s2", population="orders", dimensions=["customer_id"]),
                  q(name="s1", population="orders", dimensions=["customer_id"]),
                  q(population="s1", dimensions=["customer_id", "s2.customers.regions.name"], extensions=[
                      ext("s1", relationships=[join("s2", [["customer_id", "customer_id"]])]),
                      ext("s2", relationships=[join("customers", [["customer_id", "id"]])])])],
                 id="chained-siblings"),
    pytest.param([q(name="s", population="orders", dimensions=["customer_id"]),
                  q(population="s", dimensions=["customer_id", "customers.name"],
                    extensions=[ext("customers", relationships=[join("s", [["id", "customer_id"]])])])],
                 id="join-into-the-sibling-source"),
    pytest.param([q(name="customers", population="orders", dimensions=["customer_id"],
                    metrics=[{"metric": "sum(amount)", "alias": "spent"}]),
                  q(population="customers", metrics=[{"metric": "double_spent", "alias": "t"}],
                    extensions=[ext("customers", metrics=[{"name": "double_spent", "expression": "sum(spent * 2)"}])])],
                 id="sibling-shadows-stored"),
    pytest.param(q(population="orders", dimensions=["customer_id"],
                   metrics=[_AMOUNT, {"metric": "cust_spend.spent2", "alias": "s2"}], extensions=[
                     ext("orders", relationships=[join("cust_spend", [["customer_id", "customer_id"]])]),
                     ext("cust_spend", metrics=[{"name": "spent2", "expression": "sum(spent * 2)"}])]),
                 id="query-backed-join-target-extension"),
    pytest.param(q(population="cust_spend", dimensions=["customer_id"], metrics=["mm"],
                   extensions=[ext("cust_spend", metrics=[{"name": "mm", "expression": "sum(spent)"}])]),
                 id="query-backed-source-extension"),
    pytest.param(q(population="orders", dimensions=["regions.name"], metrics=[_AMOUNT]), id="short-form-route"),
    pytest.param(q(population="orders", dimensions=["status"],
                   metrics=[{"metric": "count(countries.id)", "alias": "n"}]), id="product-read"),
    pytest.param(q(population="customers", dimensions=["tier"], metrics=[{"metric": "c10", "alias": "t"}],
                   extensions=[ext("customers", metrics=[{"name": "c10", "expression": "sum(credit)"}])]),
                 id="root-extension-measure"),
    pytest.param(q(population="customers", dimensions=["tier"], metrics=[{"metric": "count(*)", "alias": "n"}],
                   extensions=[ext("customers", relationships=[join("vip_list", [["id", "customer_id"]], join_type="inner")])]),
                 id="inner-root-join"),
    pytest.param(q(population="orders", dimensions=["status"], metrics=[_AMOUNT], extensions=[
                     ext("customers", relationships=[join("regions", _REGION_PAIRS, join_type="inner")])]),
                 id="inner-join-on-a-joined-model"),
    pytest.param(q(population="orders", dimensions=["status"], metrics=[_AMOUNT], extensions=[
                     ext("orders", relationships=[join("customers", CUSTOMER_PAIRS, name="c", join_type="inner")])]),
                 id="inner-over-a-stored-edge-alias"),
    pytest.param(q(population="orders", dimensions=["billing_address.city", "shipping_address.city"],
                   metrics=[_AMOUNT]), id="stored-parallel-named-edges"),
    pytest.param(q(population="orders", dimensions=["bill.city"], metrics=[_AMOUNT], extensions=[
                     ext("addresses", relationships=[join("orders", [["id", "billing_address_id"]], name="bill")]),
                     ext("orders", relationships=[join("addresses", [["billing_address_id", "id"]], name="bill")])]),
                 id="reversed-stored-edge-reuse"),
    pytest.param([q(name="s", dimensions=["orders.customer_id"]),
                  q(population="s", dimensions=["customer_id", "customers.regions.name"],
                    extensions=[ext("s", relationships=[join("customers", [["customer_id", "id"]])])])],
                 id="rootless-sibling"),
    pytest.param([q(name="s", population="orders", dimensions=[{"field": "customer_id", "alias": "customer_key"}]),
                  q(population="s", dimensions=["customers.regions.name"], extensions=[ext(
                      "s", relationships=[join("customers", [["customer_key", "id"]])])])],
                 id="sibling-extension-defined-join-key"),
    pytest.param(q(population="orders", dimensions=["status"], metrics=[_AMOUNT],
                   filter=["customers.regions.name = 'North'"]), id="filter-through-a-path"),
    pytest.param(q(population="shipments", dimensions=["origin.city", "dest.city"],
                   metrics=[{"metric": "count(*)", "alias": "n"}]), id="parallel-edges-one-key-pair"),
    pytest.param(q(population="orders", dimensions=["status"], metrics=[_AMOUNT], extensions=[
                     ext("customers", relationships=[join("vip_list", [["id", "customer_id"]], join_type="inner")])]),
                 id="inner-two-hops-from-the-root"),
    pytest.param([q(name="s", population="cust_spend", dimensions=["customer_id"], metrics=["mm"],
                    extensions=[ext("cust_spend", metrics=[{"name": "mm", "expression": "sum(spent)"}])]),
                  q(population="s", dimensions=["customers.name"], metrics=[{"metric": "sum(mm)", "alias": "t"}],
                    extensions=[ext("s", relationships=[join("customers", [["customer_id", "id"]])])])],
                 id="query-backed-extension-in-a-non-root-stage"),
]


async def _planned(engine: SlayerQueryEngine, payload: Any) -> tuple[set[str], StatementReads]:
    """The stored datasets the planned statement reads, and its element reads; nothing executes."""
    main, named, ds, chain = await engine._normalize_input(payload, runtime_kwarg={}, prefer_data_source=None)
    prepared = await engine._prepare_pipeline(
        query=main, named_queries=named, runtime_kwarg={}, prefer_data_source=ds, splice_chain=chain,
    )
    return set(prepared.touched), prepared.reads


async def _stored(engine: SlayerQueryEngine, name: str) -> SlayerDataset:
    """``name`` as saved, query-backed caches included."""
    model = await engine.storage.get_dataset(name, data_source=DS)
    assert model is not None
    return model


def _access_reads(qb: SlayerDataset, *, universe: list[SlayerDataset]) -> set[tuple[str, str]]:
    """What the access filter sees ``qb`` read: ``datasets_read_by``, closed over what each read dataset reads."""
    by_id = {(m.data_source, m.name): m for m in universe}
    seen: set[tuple[str, str]] = set()
    pending = list(datasets_read_by(qb, datasets=universe))
    while pending:
        key = pending.pop()
        if key not in seen:
            seen.add(key)
            if key in by_id:
                pending.extend(datasets_read_by(by_id[key], datasets=universe))
    return seen


def _keys(datasets: dict[str, SlayerDataset], reads: StatementReads) -> set[tuple[str, str]]:
    """Primary keys and every join key the statement crosses."""
    pks = {(m.name, c.name) for m in datasets.values() for c in m.fields if c.primary_key}
    crossed = {(owner, col) for declaring, target, _, pairs in reads.relationships
               for s, t in pairs for owner, col in ((declaring, s), (target, t))}
    return pks | crossed


def _drops_qb(entries: list[ToDeleteEntry]) -> bool:
    return any(e.dataset_name == _HARNESS_QB for e in entries if isinstance(e, WholeDatasetDelete) or e.remove.fields)


@pytest.mark.parametrize("payload", _CORPUS)
async def test_static_readers_cover_the_planned_statement(payload: Any) -> None:
    stored = _datasets()
    async with shop("sqlite", stored) as engine:
        touched, reads = await _planned(engine, payload)
        qb = await engine.create_dataset_from_query(query=payload, name=_HARNESS_QB, save=False)
        stored = [await _stored(engine, m.name) for m in stored]
    universe = [*stored, qb]
    by_name = {m.name: m for m in stored}

    assert {(DS, name) for name in touched} <= _access_reads(qb, universe=universe)

    keys = _keys(by_name, reads)
    physical = {
        (m, c) for m, c in reads.fields
        if m in by_name and by_name[m].sql_table and by_name[m].get_field(c) is not None and (m, c) not in keys
    }
    undetected = {
        (m, c) for m, c in physical
        if not _drops_qb(field_dependents(datasets=universe, dataset=by_name[m], field=c))
    }
    assert not undetected, f"drift misses {_HARNESS_QB} as a dependent of {sorted(undetected)}"


async def test_a_stage_extension_reaches_the_stored_model_beside_a_root_of_another_source() -> None:
    stage = q(name="s", population="orders", metrics=[{"metric": "Customers.tier2", "alias": "n"}],
              extensions=[ext("customers", metrics=[{"name": "tier2", "expression": "max(tier)"}])])
    stages = [stage, q(population="cities_src", dimensions=["city"])]
    async with shop("sqlite", respelled_dataset(_datasets(), name="customers", spelling="Customers")) as engine:
        touched, reads = await _planned(engine, stages)
    assert "Customers" in touched
    assert ("Customers", "tier") in reads.fields


@pytest.mark.parametrize("data_source", [DS, ""], ids=["own-datasource", "no-datasource"])
def test_a_stage_source_written_in_another_case_is_read(data_source: str) -> None:
    backed = SlayerDataset(name="backed", data_source=data_source, source_queries=[
        q(population="customers", dimensions=["name", "region_label"])])
    label = DatasetField(name="region_label", sql="regions.name", datatype=DataType.STRING)
    models = [m.model_copy(update={"fields": [*m.fields, label]}) if m.name == "Customers" else m
              for m in respelled_dataset(_datasets(), name="customers", spelling="Customers")]
    assert {(DS, "Customers"), (DS, "regions")} <= datasets_read_by(backed, datasets=models)


def _root_source_and_base(
    stages: list[SlayerQuery], datasets: list[SlayerDataset], *, data_source: str | None,
) -> tuple[Any, str | None]:
    """The root's static scope: its source as respelled and the stored dataset its chain ends on."""
    (scope,) = query_scopes(stages, datasets_by_ds={DS: {m.name: m for m in datasets}}, data_source=data_source)[-1]
    assert scope.query is not None
    return scope.query.population, scope.base


def test_a_sibling_written_in_another_case_is_followed() -> None:
    stages = [q(name="Base", population="customers", dimensions=["name"]), q(population="base", dimensions=["name"])]
    models = respelled_dataset(_datasets(), name="customers", spelling="Customers")
    assert _root_source_and_base(stages, models, data_source=None) == ("Base", "Customers")


def test_a_sibling_shadows_a_stored_model_whatever_the_case() -> None:
    stages = [q(name="Customers", population="orders", dimensions=["customer_id"]),
              q(population="CUSTOMERS", dimensions=["customer_id"])]
    assert _root_source_and_base(stages, _datasets(), data_source=DS) == ("Customers", "orders")


_CUSTOMER_NAMES = {"population": "orders", "dimensions": ["customers.name"],
                   "metrics": [{"metric": "count(*)", "alias": "n"}]}


@pytest.mark.parametrize(("stages", "column"), [
    pytest.param([q(name="s", population="cities_src", dimensions=["city"]), q(**_CUSTOMER_NAMES)],
                 "orders.customers.name", id="other-stage"),
])
async def test_a_stage_source_is_local_to_its_own_stage(stages: list[SlayerQuery | dict], column: str) -> None:
    async with shop("sqlite", _datasets()) as engine:
        touched, reads = await _planned(engine, stages)
        resp = await engine.execute(stages)
    assert {"orders", "customers"} <= touched
    assert ("customers", "name") in reads.fields
    assert {row[column] for row in resp.data} - {None} <= {"Ann", "Bob", "Cid", "Dee", "Eve"}


_LABELS = _cities({"name": "label", "sql": "city", "datatype": "String"}, name="labels_src")
_STORED_LABEL_READ = {"population": "orders", "dimensions": ["customers.label"],
                      "metrics": [{"metric": "count(*)", "alias": "n"}]}


@pytest.mark.parametrize("stages", [
    [q(name="s", population="labels_src", dimensions=["label"]), q(**_STORED_LABEL_READ)],
    [q(name="s", **_STORED_LABEL_READ), q(population="labels_src", dimensions=["label"])],
], ids=["other-stage", "other-root"])
async def test_a_stored_derived_column_closes_over_its_own_definition_beside_another_source(
    stages: list[SlayerQuery | dict],
) -> None:
    labelled = customers(DatasetRelationship(to="regions", from_columns=[p[0] for p in _REGION_PAIRS], to_columns=[p[1] for p in _REGION_PAIRS])).model_copy(update={"fields": [
        *customers().fields, DatasetField(name="label", sql="upper(customers.name)", datatype=DataType.STRING)]})
    async with shop("sqlite", [labelled if m.name == "customers" else m for m in _datasets()] + [_LABELS]) as engine:
        _, reads = await _planned(engine, stages)
    assert {("customers", "label"), ("customers", "name")} <= reads.fields
    assert ("customers", "city") not in reads.fields
