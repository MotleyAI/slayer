"""Differential harness: the static readers against the statement the engine plans for the same query."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.join_walker import LIST_CHILD_PREFIX
from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.extensions import query_scopes
from slayer.engine.model_reads import models_read_by
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.statement_reads import StatementReads
from slayer.live_schema.schema_drift import WholeModelDelete, column_dependents
from tests._dev2102_fixtures import (
    DS, CUSTOMER_PAIRS, base_models, customers, ext, join, q, regions, respelled_model, shop,
)

_HARNESS_QB = "harness_qb"
_TIER_LABEL = {"name": "tier_label", "sql": "upper(tier)", "type": "TEXT"}
_AMOUNT = {"formula": "sum(amount)", "name": "a"}
_REGION_PAIRS = [["region_id", "id"]]
_SPENT_BY_CUSTOMER = {"source_model": "orders", "dimensions": ["customer_id"],
                      "measures": [{"formula": "sum(amount)", "name": "spent"}]}


def _models() -> list[SlayerModel]:
    """The shop, with ``customers → regions → countries`` for short-form routes and a query-backed ``cust_spend``."""
    home = ModelJoin(target_model="regions", join_pairs=_REGION_PAIRS)
    country = ModelJoin(target_model="countries", join_pairs=[["country_id", "id"]])
    replaced = {"customers": customers(home), "regions": regions().model_copy(update={"joins": [country]})}
    cust_spend = SlayerModel(name="cust_spend", data_source=DS,
                             source_queries=[SlayerQuery.model_validate(_SPENT_BY_CUSTOMER)])
    return [replaced.get(m.name, m) for m in base_models()] + [cust_spend]


_INLINE_CUSTOMERS = {"name": "customers", "data_source": DS, "sql_table": "customers", "columns": [
    {"name": "id", "type": "INT", "primary_key": True}, {"name": "credit", "type": "DOUBLE"},
    {"name": "tier", "type": "TEXT"}]}

_CORPUS: list[Any] = [
    pytest.param(q(source_model="orders", dimensions=["regions.name"], measures=[_AMOUNT],
                   extensions=[ext("orders", joins=[join("regions", [["ship_region_id", "id"]], name="ship_region")])]),
                 id="root-extension-join"),
    pytest.param(q(source_model="orders", dimensions=["customers.tier_label"], measures=[_AMOUNT],
                   extensions=[ext("customers", columns=[_TIER_LABEL])]), id="joined-extension-column"),
    pytest.param(q(source_model="orders", measures=[{"formula": "customers.credit_total", "name": "ct"}],
                   extensions=[ext("customers", measures=[{"name": "credit_total", "formula": "sum(credit)"}])]),
                 id="joined-extension-measure"),
    pytest.param(q(source_model="orders", dimensions=["status", "customers.regions.countries.name"],
                   extensions=[ext("regions", columns=[{"name": "nm", "sql": "upper(name)", "type": "TEXT"}])]),
                 id="multi-hop-stored-path"),
    pytest.param([q(name="s", source_model="orders", dimensions=["customer_id"], measures=[_AMOUNT]),
                  q(source_model="s", dimensions=["customers.tier"], measures=[{"formula": "sum(a)", "name": "t"}],
                    extensions=[ext("s", joins=[join("customers", [["customer_id", "id"]])])])],
                 id="sibling-extension-join"),
    pytest.param([q(name="s2", source_model="orders", dimensions=["customer_id"]),
                  q(name="s1", source_model="orders", dimensions=["customer_id"]),
                  q(source_model="s1", dimensions=["customer_id", "s2.customers.regions.name"], extensions=[
                      ext("s1", joins=[join("s2", [["customer_id", "customer_id"]])]),
                      ext("s2", joins=[join("customers", [["customer_id", "id"]])])])],
                 id="chained-siblings"),
    pytest.param([q(name="s", source_model="orders", dimensions=["customer_id"]),
                  q(source_model="s", dimensions=["customer_id", "customers.name"],
                    extensions=[ext("customers", joins=[join("s", [["id", "customer_id"]])])])],
                 id="join-into-the-sibling-source"),
    pytest.param([q(name="customers", source_model="orders", dimensions=["customer_id"],
                    measures=[{"formula": "sum(amount)", "name": "spent"}]),
                  q(source_model="customers", measures=[{"formula": "sum(double_spent)", "name": "t"}],
                    extensions=[ext("customers", columns=[
                        {"name": "double_spent", "sql": "spent * 2", "type": "DOUBLE"}])])],
                 id="sibling-shadows-stored"),
    pytest.param(q(source_model="orders", dimensions=["cust_spend.spent2"], measures=[_AMOUNT], extensions=[
                     ext("orders", joins=[join("cust_spend", [["customer_id", "customer_id"]])]),
                     ext("cust_spend", columns=[{"name": "spent2", "sql": "spent * 2", "type": "DOUBLE"}])]),
                 id="query-backed-join-target-extension"),
    pytest.param(q(source_model="cust_spend", dimensions=["customer_id"], measures=["mm"],
                   extensions=[ext("cust_spend", measures=[{"name": "mm", "formula": "sum(spent)"}])]),
                 id="query-backed-source-extension"),
    pytest.param(q(source_model="orders", dimensions=["regions.name"], measures=[_AMOUNT]), id="short-form-route"),
    pytest.param(q(source_model="orders", dimensions=["status"],
                   measures=[{"formula": "count(countries.id)", "name": "n"}]), id="product-read"),
    pytest.param(q(source_model=_INLINE_CUSTOMERS, dimensions=["tier"], measures=[{"formula": "c10", "name": "t"}],
                   extensions=[ext("customers", measures=[{"name": "c10", "formula": "sum(credit)"}])]),
                 id="inline-source"),
    pytest.param(q(source_model="customers", dimensions=["tier"], measures=[{"formula": "count(*)", "name": "n"}],
                   extensions=[ext("customers", joins=[join("vip_list", [["id", "customer_id"]], join_type="inner")])]),
                 id="inner-root-join"),
    pytest.param(q(source_model="orders", dimensions=["status"], measures=[_AMOUNT], extensions=[
                     ext("customers", joins=[join("regions", _REGION_PAIRS, join_type="inner")])]),
                 id="inner-join-on-a-joined-model"),
    pytest.param(q(source_model="orders", dimensions=["status"], measures=[_AMOUNT], extensions=[
                     ext("orders", joins=[join("customers", CUSTOMER_PAIRS, name="c", join_type="inner")])]),
                 id="inner-over-a-stored-edge-alias"),
    pytest.param(q(source_model="orders", dimensions=["billing_address.city", "shipping_address.city"],
                   measures=[_AMOUNT]), id="stored-parallel-named-edges"),
    pytest.param(q(source_model="orders", dimensions=["bill.city"], measures=[_AMOUNT], extensions=[
                     ext("addresses", joins=[join("orders", [["id", "billing_address_id"]], name="bill")]),
                     ext("orders", joins=[join("addresses", [["billing_address_id", "id"]], name="bill")])]),
                 id="reversed-stored-edge-reuse"),
    pytest.param([q(name="s", dimensions=["orders.customer_id"]),
                  q(source_model="s", dimensions=["customer_id", "customers.regions.name"],
                    extensions=[ext("s", joins=[join("customers", [["customer_id", "id"]])])])],
                 id="rootless-sibling"),
    pytest.param([q(name="s", source_model="orders", dimensions=["customer_id"]),
                  q(source_model="s", dimensions=["customers.regions.name"], extensions=[ext(
                      "s", columns=[{"name": "customer_key", "sql": "customer_id", "type": "INT"}],
                      joins=[join("customers", [["customer_key", "id"]])])])],
                 id="sibling-extension-defined-join-key"),
    pytest.param(q(source_model="orders", dimensions=["status"], measures=[_AMOUNT],
                   filters=["customers.regions.name = 'North'"]), id="filter-through-a-path"),
    pytest.param(q(source_model="shipments", dimensions=["origin.city", "dest.city"],
                   measures=[{"formula": "count(*)", "name": "n"}]), id="parallel-edges-one-key-pair"),
    pytest.param(q(source_model="orders", dimensions=["status"], measures=[_AMOUNT], extensions=[
                     ext("customers", joins=[join("vip_list", [["id", "customer_id"]], join_type="inner")])]),
                 id="inner-two-hops-from-the-root"),
    pytest.param([q(name="s", source_model="cust_spend", dimensions=["customer_id"], measures=["mm"],
                    extensions=[ext("cust_spend", measures=[{"name": "mm", "formula": "sum(spent)"}])]),
                  q(source_model="s", dimensions=["customers.name"], measures=[{"formula": "sum(mm)", "name": "t"}],
                    extensions=[ext("s", joins=[join("customers", [["customer_id", "id"]])])])],
                 id="query-backed-extension-in-a-non-root-stage"),
]


async def _planned(engine: SlayerQueryEngine, payload: Any) -> tuple[set[str], StatementReads]:
    """The stored models the planned statement reads, and its element reads; nothing executes."""
    main, named, ds, chain = await engine._normalize_input(payload, runtime_kwarg={}, prefer_data_source=None)
    prepared = await engine._prepare_pipeline(
        query=main, named_queries=named, runtime_kwarg={}, prefer_data_source=ds, splice_chain=chain,
    )
    return set(prepared.touched), prepared.reads


async def _stored(engine: SlayerQueryEngine, name: str) -> SlayerModel:
    """``name`` as saved, query-backed caches included."""
    model = await engine.storage.get_model(name, data_source=DS)
    assert model is not None
    return model


def _access_reads(qb: SlayerModel, *, universe: list[SlayerModel]) -> set[tuple[str, str]]:
    """What the access filter sees ``qb`` read: ``models_read_by``, closed over what each read model reads."""
    by_id = {(m.data_source, m.name): m for m in universe}
    seen: set[tuple[str, str]] = set()
    pending = list(models_read_by(qb, models=universe))
    while pending:
        key = pending.pop()
        if key not in seen:
            seen.add(key)
            if key in by_id:
                pending.extend(models_read_by(by_id[key], models=universe))
    return seen


def _keys(models: dict[str, SlayerModel], reads: StatementReads) -> set[tuple[str, str]]:
    """Primary keys and every join key the statement crosses."""
    pks = {(m.name, c.name) for m in models.values() for c in m.columns if c.primary_key}
    crossed = {(owner, col) for declaring, target, _, pairs in reads.joins
               for s, t in pairs for owner, col in ((declaring, s), (target, t))}
    return pks | crossed


def _drops_qb(entries: list) -> bool:
    return any(e.model_name == _HARNESS_QB for e in entries if isinstance(e, WholeModelDelete) or e.remove.columns)


@pytest.mark.parametrize("payload", _CORPUS)
async def test_static_readers_cover_the_planned_statement(payload: Any) -> None:
    stored = _models()
    async with shop("sqlite", stored) as engine:
        touched, reads = await _planned(engine, payload)
        qb = await engine.create_model_from_query(query=payload, name=_HARNESS_QB, save=False)
        stored = [await _stored(engine, m.name) for m in stored]
    universe = [*stored, qb]
    by_name = {m.name: m for m in stored}

    assert {(DS, name) for name in touched} <= _access_reads(qb, universe=universe)

    keys = _keys(by_name, reads)
    physical = {
        (m, c) for m, c in reads.columns
        if m in by_name and by_name[m].sql_table and by_name[m].get_column(c) is not None and (m, c) not in keys
    }
    undetected = {
        (m, c) for m, c in physical
        if not _drops_qb(column_dependents(models=universe, model=by_name[m], column=c))
    }
    assert not undetected, f"drift misses {_HARNESS_QB} as a dependent of {sorted(undetected)}"


_INLINE_OVER_ADDRESSES = {"name": "customers", "data_source": DS, "sql_table": "addresses", "columns": [
    {"name": "id", "type": "INT", "primary_key": True}, {"name": "city", "type": "TEXT"}]}


async def test_an_inline_source_is_not_a_read_of_its_stored_namesake() -> None:
    async with shop("sqlite", _models()) as engine:
        touched, reads = await _planned(engine, q(source_model=_INLINE_OVER_ADDRESSES, dimensions=["city"]))
    assert touched == set()
    assert not {m for m, _ in reads.columns} & {"customers"}


async def test_an_inline_stage_source_is_not_a_read_of_its_stored_namesake() -> None:
    stages = [q(name="s", source_model=_INLINE_OVER_ADDRESSES, dimensions=["city"]),
              q(source_model="s", dimensions=["city"])]
    async with shop("sqlite", _models()) as engine:
        touched, reads = await _planned(engine, stages)
    assert touched == set()
    assert not {m for m, _ in reads.columns} & {"customers"}


@pytest.mark.parametrize("stages", [
    [q(name="s", source_model=_INLINE_OVER_ADDRESSES, dimensions=["city"]),
     q(source_model="customers", dimensions=["name"], measures=[{"formula": "count(*)", "name": "n"}])],
    [q(name="s", source_model="customers", dimensions=["name"]),
     q(source_model=_INLINE_OVER_ADDRESSES, dimensions=["city"])],
], ids=["stored-root", "stored-stage"])
async def test_a_stage_sourced_from_the_stored_namesake_still_reads_it(stages: list[SlayerQuery]) -> None:
    async with shop("sqlite", _models()) as engine:
        touched, reads = await _planned(engine, stages)
    assert touched == {"customers"}
    assert ("customers", "name") in reads.columns
    assert ("customers", "city") not in reads.columns


async def test_a_stage_extension_reaches_the_stored_model_beside_an_inline_root_of_another_case() -> None:
    stage = q(name="s", source_model="orders", dimensions=["Customers.tier2"], measures=[{"formula": "count(*)", "name": "n"}],
              extensions=[ext("customers", columns=[{"name": "tier2", "sql": "tier", "type": "TEXT"}])])
    stages = [stage, q(source_model=_INLINE_OVER_ADDRESSES, dimensions=["city"])]
    async with shop("sqlite", respelled_model(_models(), name="customers", spelling="Customers")) as engine:
        touched, reads = await _planned(engine, stages)
    assert "Customers" in touched
    assert ("Customers", "tier") in reads.columns


@pytest.mark.parametrize("data_source", [DS, ""], ids=["own-datasource", "no-datasource"])
def test_a_stage_source_written_in_another_case_is_read(data_source: str) -> None:
    backed = SlayerModel(name="backed", data_source=data_source, source_queries=[
        q(source_model="customers", dimensions=["name", "region_label"])])
    label = Column(name="region_label", sql="regions.name", type=DataType.TEXT)
    models = [m.model_copy(update={"columns": [*m.columns, label]}) if m.name == "Customers" else m
              for m in respelled_model(_models(), name="customers", spelling="Customers")]
    assert {(DS, "Customers"), (DS, "regions")} <= models_read_by(backed, models=models)


def _root_source_and_base(
    stages: list[SlayerQuery], models: list[SlayerModel], *, data_source: str | None,
) -> tuple[Any, str | None]:
    """The root's static scope: its source as respelled and the stored model its chain ends on."""
    (scope,) = query_scopes(stages, models_by_ds={DS: {m.name: m for m in models}}, data_source=data_source)[-1]
    assert scope.query is not None
    return scope.query.source_model, scope.base


def test_a_sibling_written_in_another_case_is_followed() -> None:
    stages = [q(name="Base", source_model="customers", dimensions=["name"]), q(source_model="base", dimensions=["name"])]
    models = respelled_model(_models(), name="customers", spelling="Customers")
    assert _root_source_and_base(stages, models, data_source=None) == ("Base", "Customers")


def test_a_sibling_shadows_a_stored_model_whatever_the_case() -> None:
    stages = [q(name="Customers", source_model="orders", dimensions=["customer_id"]),
              q(source_model="CUSTOMERS", dimensions=["customer_id"])]
    assert _root_source_and_base(stages, _models(), data_source=DS) == ("Customers", "orders")


async def test_an_inline_sources_saved_measure_is_not_a_read_of_its_stored_namesake() -> None:
    inline = {**_INLINE_OVER_ADDRESSES, "measures": [{"name": "n_city", "formula": "count_distinct(city)"}]}
    async with shop("sqlite", _models()) as engine:
        _, reads = await _planned(engine, q(source_model=inline, measures=["n_city"]))
    assert not reads.measures


async def test_a_spliced_stage_with_an_inline_source_is_not_a_read_of_its_stored_namesake() -> None:
    async with shop("sqlite", _models()) as engine:
        await engine.create_model_from_query(
            query=q(source_model=_INLINE_OVER_ADDRESSES, dimensions=["city"]), name="cities")
        touched, reads = await _planned(engine, q(source_model="cities", dimensions=["city"]))
    assert "customers" not in touched
    assert not {m for m, _ in reads.columns} & {"customers"}


async def test_a_list_child_of_an_inline_source_is_not_a_persisted_read() -> None:
    inline = {"name": "customers", "data_source": DS, "sql": "SELECT 1 AS id, ['a', 'b'] AS tags", "columns": [
        {"name": "id", "type": "INT", "primary_key": True},
        {"name": "tags", "type": "ARRAY", "element_type": "TEXT"}]}
    async with shop("duckdb", _models()) as engine:
        _, reads = await _planned(engine, q(source_model=inline, dimensions=["tags.value"]))
    assert not {m for m, _ in reads.columns if m.startswith(LIST_CHILD_PREFIX)}


_CUSTOMER_NAMES = {"source_model": "orders", "dimensions": ["customers.name"],
                   "measures": [{"formula": "count(*)", "name": "n"}]}


@pytest.mark.parametrize(("stages", "column"), [
    pytest.param([q(name="s", source_model=_INLINE_OVER_ADDRESSES, dimensions=["city"]), q(**_CUSTOMER_NAMES)],
                 "orders.customers.name", id="inline-stage"),
    pytest.param([q(name="s", **_CUSTOMER_NAMES),
                  q(source_model=_INLINE_OVER_ADDRESSES, dimensions=["city", "s.customers__name"],
                    extensions=[ext("customers", joins=[join("s", [["id", "n"]])])])],
                 "customers.s.customers__name", id="inline-root"),
])
async def test_an_inline_source_is_local_to_its_own_stage(stages: list[SlayerQuery | dict], column: str) -> None:
    async with shop("sqlite", _models()) as engine:
        touched, reads = await _planned(engine, stages)
        resp = await engine.execute(stages)
    assert {"orders", "customers"} <= touched
    assert ("customers", "name") in reads.columns
    assert {row[column] for row in resp.data} - {None} <= {"Ann", "Bob", "Cid", "Dee", "Eve"}


_INLINE_LABEL = {**_INLINE_OVER_ADDRESSES, "columns": [
    *_INLINE_OVER_ADDRESSES["columns"], {"name": "label", "sql": "city", "type": "TEXT"}]}
_STORED_LABEL_READ = {"source_model": "orders", "dimensions": ["customers.label"],
                      "measures": [{"formula": "count(*)", "name": "n"}]}


@pytest.mark.parametrize("stages", [
    [q(name="s", source_model=_INLINE_LABEL, dimensions=["label"]), q(**_STORED_LABEL_READ)],
    [q(name="s", **_STORED_LABEL_READ), q(source_model=_INLINE_LABEL, dimensions=["label"])],
], ids=["inline-stage", "inline-root"])
async def test_a_stored_derived_column_closes_over_its_own_definition_beside_an_inline_namesake(
    stages: list[SlayerQuery | dict],
) -> None:
    labelled = customers(ModelJoin(target_model="regions", join_pairs=_REGION_PAIRS)).model_copy(update={"columns": [
        *customers().columns, Column(name="label", sql="upper(name)", type=DataType.TEXT)]})
    async with shop("sqlite", [labelled if m.name == "customers" else m for m in _models()]) as engine:
        _, reads = await _planned(engine, stages)
    assert {("customers", "label"), ("customers", "name")} <= reads.columns
    assert ("customers", "city") not in reads.columns
