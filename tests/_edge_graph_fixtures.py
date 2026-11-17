"""Model graphs built from bare ``(source, target, edge_name)`` edge tuples, for route-search unit tests."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset


def edge_graph(
    *, nodes: set[str], edges: list[tuple[str, str, str | None]],
) -> dict[str, SlayerDataset]:
    """One model per node; each edge is an unannotated join declared on its source."""
    joins: dict[str, list[DatasetRelationship]] = {n: [] for n in nodes}
    for source, target, name in edges:
        joins[source].append(DatasetRelationship(to=target, join_pairs=[["x_id", "id"]], name=name))
    return {
        n: SlayerDataset(
            name=n, data_source="ds", sql_table=n,
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="x_id", datatype=DataType.INT),
            ],
            relationships=joins[n],
        )
        for n in sorted(nodes)
    }
