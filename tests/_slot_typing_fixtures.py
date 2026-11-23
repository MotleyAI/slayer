"""A ``SlotTyping`` over a small shop, for tests that build a ``ValueRegistry`` / ``ProjectionPlanner`` by hand.

Datasets: ``orders`` → ``customers`` → ``regions`` (LEFT, many_to_one).
"""

from __future__ import annotations

from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.scope import DatasetScope
from slayer.engine.key_metadata import SlotTyping
from slayer.ir.source_bundle import ResolvedSourceBundle

__all__ = ["SHOP_TYPING"]


def _edge(to: str, column: str) -> DatasetRelationship:
    return DatasetRelationship(
        to=to, from_columns=[column], to_columns=["id"],
        join_type=JoinType.LEFT, cardinality=RelationshipCardinality.MANY_TO_ONE,
    )


def _dataset(name: str, fields: dict[str, DataType], edges: list[DatasetRelationship]) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source="test", sql_table=name, relationships=edges,
        fields=[DatasetField(name=n, datatype=t, primary_key=n == "id") for n, t in fields.items()],
    )


_ORDERS = _dataset("orders", {
    "id": DataType.INTEGER, "customer_id": DataType.INTEGER, "status": DataType.STRING,
    "revenue": DataType.FLOAT, "amount": DataType.FLOAT, "created_at": DataType.DATETIME,
    "updated_at": DataType.DATETIME, "opened_at": DataType.DATETIME, "reviewed_at": DataType.DATETIME,
}, [_edge("customers", "customer_id")])
_CUSTOMERS = _dataset("customers", {
    "id": DataType.INTEGER, "region_id": DataType.INTEGER, "revenue": DataType.FLOAT,
    "created_at": DataType.DATETIME, "signed_up_at": DataType.DATETIME,
}, [_edge("regions", "region_id")])
_REGIONS = _dataset("regions", {"id": DataType.INTEGER, "created_at": DataType.DATETIME}, [])

#: Field leaves rooted at ``orders``.
SHOP_TYPING = SlotTyping(
    scope=DatasetScope(population=_ORDERS),
    bundle=ResolvedSourceBundle(dialect="postgres", population=_ORDERS, referenced_datasets=[_CUSTOMERS, _REGIONS]),
)
