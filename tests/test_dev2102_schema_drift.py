"""Schema drift attributes a saved stage's reads through its query extensions' definitions."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.live_schema.schema_drift import WholeModelDelete, column_dependents


def _orders() -> SlayerModel:
    return SlayerModel(name="orders", data_source="ds", sql_table="orders", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="tax", type=DataType.DOUBLE),
        Column(name="status", type=DataType.TEXT),
        Column(name="buyer_ref", type=DataType.INT),
    ])


def _customers() -> SlayerModel:
    return SlayerModel(name="customers", data_source="ds", sql_table="customers", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="name", type=DataType.TEXT),
        Column(name="code", type=DataType.TEXT, unique=True),
    ])


def _qb(*, extension: ModelExtension, **stage: object) -> SlayerModel:
    query = SlayerQuery.model_validate({"source_model": "orders", "extensions": [extension], **stage})
    return SlayerModel(name="qb", data_source="ds", source_queries=[query])


def _whole_dropped(*, qb: SlayerModel, model: SlayerModel, column: str) -> set[str]:
    entries = column_dependents(models=[_orders(), _customers(), qb], model=model, column=column)
    return {e.model_name for e in entries if isinstance(e, WholeModelDelete)}


_NET = ModelExtension(source_name="orders", columns=[Column(name="net", sql="amount - tax", type=DataType.DOUBLE)])
_BUYER = ModelExtension(source_name="orders", joins=[
    ModelJoin(target_model="customers", join_pairs=[["buyer_ref", "id"]], name="buyer"),
])


class TestExtensionDefinitionDependencies:
    def test_extension_column_sql_reading_dropped_column(self) -> None:
        qb = _qb(extension=_NET, dimensions=["net"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_column_used_in_measure(self) -> None:
        qb = _qb(extension=_NET, measures=[{"formula": "sum(net)", "name": "n"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_column_filter_reading_dropped_column(self) -> None:
        ext = ModelExtension(source_name="orders", columns=[
            Column(name="paid", sql="amount", type=DataType.DOUBLE, filter="status = 'paid'"),
        ])
        qb = _qb(extension=ext, measures=[{"formula": "sum(paid)", "name": "p"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="status") == {"qb"}

    def test_extension_measure_reading_dropped_column(self) -> None:
        ext = ModelExtension(source_name="orders", measures=[ModelMeasure(name="taxes", formula="sum(tax)")])
        qb = _qb(extension=ext, measures=[{"formula": "taxes"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_column_unused_by_the_stage(self) -> None:
        qb = _qb(extension=_NET, measures=[{"formula": "sum(amount)", "name": "a"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == set()

    def test_extension_join_local_key_dropped(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=["buyer.name"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_extension_join_foreign_key_dropped(self) -> None:
        ext = ModelExtension(source_name="orders", joins=[
            ModelJoin(target_model="customers", join_pairs=[["status", "code"]], name="by_code"),
        ])
        qb = _qb(extension=ext, dimensions=["by_code.name"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_customers(), column="code") == {"qb"}

    def test_extension_join_unused_by_the_stage(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=["status"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == set()
