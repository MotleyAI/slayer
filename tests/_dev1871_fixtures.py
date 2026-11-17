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

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1866_fixtures import DS_CHAIN, chain_models

DS_LIT = "ds_lit"
DS_FANROUTE = "ds_fanroute"
DS_TWOROUTES = "ds_tworoutes"


def lit_models() -> list[SlayerDataset]:
    """stores —(unknown card)→ plants, plus the to-one route stores → hub → plants."""
    plants = SlayerDataset(
        name="plants", data_source=DS_LIT, sql_table="plants",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="code", datatype=DataType.INT),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )
    hub = SlayerDataset(
        name="hub", data_source=DS_LIT, sql_table="hub",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="plant_id", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(
                to="plants", join_pairs=[["plant_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    stores = SlayerDataset(
        name="stores", data_source=DS_LIT, sql_table="stores",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="kind", datatype=DataType.TEXT),
            DatasetField(name="plant_ref", datatype=DataType.INT),
            DatasetField(name="hub_id", datatype=DataType.INT),
        ],
        relationships=[
            # Direct edge: undeclared cardinality into a non-unique column —
            # resolvable literally, never provably to-one.
            DatasetRelationship(
                to="plants", join_pairs=[["plant_ref", "code"]],
                cardinality=None,
            ),
            DatasetRelationship(
                to="hub", join_pairs=[["hub_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [plants, hub, stores]


def fanroute_models() -> list[SlayerDataset]:
    """firm → office to-one; office → desks fans out (unique route overall)."""
    office = SlayerDataset(
        name="office", data_source=DS_FANROUTE, sql_table="office",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region", datatype=DataType.TEXT),
        ],
    )
    firm = SlayerDataset(
        name="firm", data_source=DS_FANROUTE, sql_table="firm",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="title", datatype=DataType.TEXT),
            DatasetField(name="office_id", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(
                to="office", join_pairs=[["office_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    desks = SlayerDataset(
        name="desks", data_source=DS_FANROUTE, sql_table="desks",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="office_ref", datatype=DataType.INT),
            DatasetField(name="id_tag", datatype=DataType.TEXT),
        ],
        relationships=[
            DatasetRelationship(
                to="office", join_pairs=[["office_ref", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [office, firm, desks]


def lit_models_by_name() -> dict[str, SlayerDataset]:
    return {m.name: m for m in lit_models()}


def fanroute_models_by_name() -> dict[str, SlayerDataset]:
    return {m.name: m for m in fanroute_models()}


def tworoutes_models() -> list[SlayerDataset]:
    """sale → cust → city and sale → store → city, every hop to-one."""
    city = SlayerDataset(
        name="city", data_source=DS_TWOROUTES, sql_table="city",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )
    cust = SlayerDataset(
        name="cust", data_source=DS_TWOROUTES, sql_table="cust",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="city_id", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(
                to="city", join_pairs=[["city_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    store = SlayerDataset(
        name="store", data_source=DS_TWOROUTES, sql_table="store",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="city_id", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(
                to="city", join_pairs=[["city_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    sale = SlayerDataset(
        name="sale", data_source=DS_TWOROUTES, sql_table="sale",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="code", datatype=DataType.TEXT),
            DatasetField(name="cust_id", datatype=DataType.INT),
            DatasetField(name="store_id", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(
                to="cust", join_pairs=[["cust_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
            DatasetRelationship(
                to="store", join_pairs=[["store_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [city, cust, store, sale]


_TOPOLOGIES: list[tuple[str, list[SlayerDataset]]] = [
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
            await storage.save_dataset(model, _validate=False)
    return storage
