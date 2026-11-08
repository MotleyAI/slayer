"""Model graphs built from bare ``(source, target, edge_name)`` edge tuples, for route-search unit tests."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel


def edge_graph(
    *, nodes: set[str], edges: list[tuple[str, str, str | None]],
) -> dict[str, SlayerModel]:
    """One model per node; each edge is an unannotated join declared on its source."""
    joins: dict[str, list[ModelJoin]] = {n: [] for n in nodes}
    for source, target, name in edges:
        joins[source].append(ModelJoin(target_model=target, join_pairs=[["x_id", "id"]], name=name))
    return {
        n: SlayerModel(
            name=n, data_source="ds", sql_table=n,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="x_id", type=DataType.INT),
            ],
            joins=joins[n],
        )
        for n in sorted(nodes)
    }
