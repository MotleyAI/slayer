"""DEV-1931 fixtures: DEV-1832 Graph A + custom aggregations whose weight
defaults exercise each resolution frame (root-local, owner-local, mixed,
owner-reachable-dotted, fanning). ``ambiguous_owner_models`` covers the
ambiguous-hop fail-closed case."""

from __future__ import annotations

from typing import List

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    SlayerDataset,
)

from tests._dev1832_fixtures import dev1832_models


def _model(models: List[SlayerDataset], name: str) -> SlayerDataset:
    return next(m for m in models if m.name == name)


def dev1931_models() -> List[SlayerDataset]:
    """DEV-1832 Graph A + the DEV-1931 custom aggregations. Host first (orders)."""
    models = dev1832_models()
    cust = _model(models=models, name="customers")
    cust.aggregations.extend([
        Aggregation(
            name="wsum_host", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="orders.amount")]),
        Aggregation(
            name="wsum_host_expr", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="orders.amount * 1")]),
        Aggregation(
            name="wsum_mixed", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="spend + orders.amount")]),
        Aggregation(
            name="wsum_regions_pop", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="regions.pop")]),
        # Bare owner-local; also reused over a multi-leaf expression source.
        Aggregation(
            name="wsum_bare", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="spend")]),
        # Unreachable from both owner and root → fail closed.
        Aggregation(
            name="wsum_nowhere", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="nowhere.col")]),
        # regions resolves from the owner, plans is missing on it → fail closed.
        Aggregation(
            name="wsum_partial", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="regions.plans.fee")]),
    ])
    regions = _model(models=models, name="regions")
    # F1: w1 widens the home to customers; w2 (`bad_pop`) crosses the fanning hop.
    regions.aggregations.append(Aggregation(
        name="wfan_widen", expression="SUM({value} * {w1} * {w2})",
        params=[AggregationParam(name="w1", sql="customers.spend"),
                AggregationParam(name="w2", sql="bad_pop")]))
    return models


def ambiguous_owner_models() -> List[SlayerDataset]:
    """``o → ag`` via two parallel edges (ambiguous); ``r → ag`` clean. ``wscore``
    (on ``o``) defaults to ``ag.score`` — ambiguous from ``o``, must fail closed,
    never re-anchor at ``r`` where ``ag`` is cleanly reachable."""
    r = SlayerDataset(
        name="r", data_source="test", sql_table="r",
        fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                 DatasetField(name="o_id", datatype=DataType.INT),
                 DatasetField(name="ag_id", datatype=DataType.INT)],
        relationships=[DatasetRelationship(to="o", join_pairs=[["o_id", "id"]]),
               DatasetRelationship(to="ag", join_pairs=[["ag_id", "id"]])])
    o = SlayerDataset(
        name="o", data_source="test", sql_table="o",
        fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                 DatasetField(name="a1", datatype=DataType.INT),
                 DatasetField(name="a2", datatype=DataType.INT),
                 DatasetField(name="val", datatype=DataType.DOUBLE)],
        relationships=[DatasetRelationship(to="ag", join_pairs=[["a1", "id"]]),
               DatasetRelationship(to="ag", join_pairs=[["a2", "id"]])],
        aggregations=[Aggregation(
            name="wscore", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="ag.score")])])
    ag = SlayerDataset(
        name="ag", data_source="test", sql_table="ag",
        fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                 DatasetField(name="score", datatype=DataType.DOUBLE)])
    return [r, o, ag]


# Oracles over the DEV-1832 orders rows (orphan order o8 has NULL customer).
#: SUM(customers.spend * orders.amount), home = orders.
WSUM_HOST_VALUE = 15010.0
#: SUM(customers.spend * (customers.spend + orders.amount)), home = orders.
WSUM_MIXED_VALUE = 102510.0
