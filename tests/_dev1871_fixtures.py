"""Shared fixtures for DEV-1871 — route-aware rootless population inference.

Three topologies, each isolated in its own datasource, exercising the routing
legs the DEV-1866 fixtures cannot:

* ``ds_lit``      — ``stores`` has a DIRECT unknown-cardinality edge to
  ``plants`` (literal resolution, not to-one) AND a provably to-one route
  stores → hub → plants; literal precedence means the direct edge is used and
  the candidate fails, never the reinterpreted route.
* ``ds_fanroute`` — firm → office (to-one) → desks (to-many): the unique
  full-graph route binding accepts fans out, so it is not determination.
* ``ds_tworoutes``— sale → {cust, store} → city, all hops to-one: two
  fan-out-free routes to ``city`` ⇒ a routed probe is ambiguous.

The chain topology (orders → customers → regions) is reused from the DEV-1866
fixtures for short-form/full-path parity and routed hop counting.
"""

from __future__ import annotations

import os
import tempfile

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1866_fixtures import DS_CHAIN, chain_models

DS_LIT = "ds_lit"
DS_FANROUTE = "ds_fanroute"
DS_TWOROUTES = "ds_tworoutes"


def lit_models() -> list[SlayerModel]:
    """stores —(unknown card)→ plants, plus the to-one route stores → hub → plants."""
    plants = SlayerModel(
        name="plants", data_source=DS_LIT, sql_table="plants",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="code", type=DataType.INT),
            Column(name="name", type=DataType.TEXT),
        ],
    )
    hub = SlayerModel(
        name="hub", data_source=DS_LIT, sql_table="hub",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="plant_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                target_model="plants", join_pairs=[["plant_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    stores = SlayerModel(
        name="stores", data_source=DS_LIT, sql_table="stores",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="kind", type=DataType.TEXT),
            Column(name="plant_ref", type=DataType.INT),
            Column(name="hub_id", type=DataType.INT),
        ],
        joins=[
            # Direct edge: undeclared cardinality into a non-unique column —
            # resolvable literally, never provably to-one.
            ModelJoin(
                target_model="plants", join_pairs=[["plant_ref", "code"]],
                cardinality=None,
            ),
            ModelJoin(
                target_model="hub", join_pairs=[["hub_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [plants, hub, stores]


def fanroute_models() -> list[SlayerModel]:
    """firm → office to-one; office → desks fans out (unique route overall)."""
    office = SlayerModel(
        name="office", data_source=DS_FANROUTE, sql_table="office",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
        ],
    )
    firm = SlayerModel(
        name="firm", data_source=DS_FANROUTE, sql_table="firm",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="title", type=DataType.TEXT),
            Column(name="office_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                target_model="office", join_pairs=[["office_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    desks = SlayerModel(
        name="desks", data_source=DS_FANROUTE, sql_table="desks",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="office_ref", type=DataType.INT),
            Column(name="id_tag", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="office", join_pairs=[["office_ref", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [office, firm, desks]


def lit_models_by_name() -> dict[str, SlayerModel]:
    return {m.name: m for m in lit_models()}


def fanroute_models_by_name() -> dict[str, SlayerModel]:
    return {m.name: m for m in fanroute_models()}


def tworoutes_models() -> list[SlayerModel]:
    """sale → cust → city and sale → store → city, every hop to-one."""
    city = SlayerModel(
        name="city", data_source=DS_TWOROUTES, sql_table="city",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )
    cust = SlayerModel(
        name="cust", data_source=DS_TWOROUTES, sql_table="cust",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="city_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                target_model="city", join_pairs=[["city_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    store = SlayerModel(
        name="store", data_source=DS_TWOROUTES, sql_table="store",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="city_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                target_model="city", join_pairs=[["city_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    sale = SlayerModel(
        name="sale", data_source=DS_TWOROUTES, sql_table="sale",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="code", type=DataType.TEXT),
            Column(name="cust_id", type=DataType.INT),
            Column(name="store_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                target_model="cust", join_pairs=[["cust_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
            ModelJoin(
                target_model="store", join_pairs=[["store_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [city, cust, store, sale]


_TOPOLOGIES: list[tuple[str, list[SlayerModel]]] = [
    (DS_CHAIN, chain_models()),
    (DS_LIT, lit_models()),
    (DS_FANROUTE, fanroute_models()),
    (DS_TWOROUTES, tworoutes_models()),
]


async def make_routing_storage() -> YAMLStorage:
    """Metadata-only storage (no DB rows) with every routing topology saved."""
    d = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    for ds, models in _TOPOLOGIES:
        await storage.save_datasource(
            DatasourceConfig(name=ds, type="sqlite", database=":memory:"),
        )
        for model in models:
            await storage.save_model(model, _validate=False)
    return storage
