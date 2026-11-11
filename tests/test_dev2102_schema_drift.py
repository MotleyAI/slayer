"""Schema drift attributes a saved stage's reads through its query extensions' definitions."""

from __future__ import annotations

from slayer.core.enums import DataType, JoinType
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

    def test_extension_column_aliasing_a_dropped_column(self) -> None:
        ext = ModelExtension(source_name="orders", columns=[Column(name="gross", sql="amount", type=DataType.DOUBLE)])
        qb = _qb(extension=ext, measures=[{"formula": "sum(gross)", "name": "g"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="amount") == {"qb"}


_UPPER_BUYER = Column(name="buyer_name", sql="UPPER(buyer.name)", type=DataType.TEXT)


def _orders_with_stored_buyer() -> SlayerModel:
    return _orders().model_copy(update={"joins": [
        ModelJoin(target_model="customers", join_pairs=[["buyer_ref", "id"]], name="buyer"),
    ]})


class TestExtensionDefinitionJoinPaths:
    def test_extension_column_through_extension_join(self) -> None:
        ext = _BUYER.model_copy(update={"columns": [_UPPER_BUYER]})
        qb = _qb(extension=ext, dimensions=["buyer_name"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_extension_column_through_stored_join(self) -> None:
        orders = _orders_with_stored_buyer()
        qb = _qb(
            extension=ModelExtension(source_name="orders", columns=[_UPPER_BUYER]),
            dimensions=["buyer_name"], measures=[{"formula": "count(*)"}],
        )
        entries = column_dependents(models=[orders, _customers(), qb], model=orders, column="buyer_ref")
        assert {e.model_name for e in entries if isinstance(e, WholeModelDelete)} == {"qb"}

    def test_extension_measure_through_extension_join(self) -> None:
        ext = _BUYER.model_copy(update={"measures": [ModelMeasure(name="buyers", formula="count_distinct(buyer.name)")]})
        qb = _qb(extension=ext, measures=[{"formula": "buyers"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_sibling_stage_extension_column_through_extension_join(self) -> None:
        ext = ModelExtension(source_name="s", joins=_BUYER.joins, columns=[_UPPER_BUYER])
        qb = SlayerModel(name="qb", data_source="ds", source_queries=[
            SlayerQuery.model_validate({"name": "s", "source_model": "orders", "dimensions": ["buyer_ref"]}),
            SlayerQuery.model_validate({"source_model": "s", "extensions": [ext], "dimensions": ["buyer_name"]}),
        ])
        assert _whole_dropped(qb=qb, model=_customers(), column="name") == {"qb"}

    def test_extension_column_through_join_unused_by_the_stage(self) -> None:
        ext = _BUYER.model_copy(update={"columns": [_UPPER_BUYER]})
        qb = _qb(extension=ext, dimensions=["status"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == set()


def _inner(*pair: str) -> ModelExtension:
    return ModelExtension(source_name="orders", joins=[
        ModelJoin(target_model="customers", join_pairs=[list(pair)], join_type=JoinType.INNER),
    ])


class TestInnerExtensionJoinRestriction:
    def test_local_key_dropped(self) -> None:
        qb = _qb(extension=_inner("buyer_ref", "id"), measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_target_primary_key_dropped(self) -> None:
        qb = _qb(extension=_inner("buyer_ref", "id"), measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_customers(), column="id") == {"qb"}

    def test_target_key_dropped(self) -> None:
        qb = _qb(extension=_inner("status", "code"), measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_customers(), column="code") == {"qb"}

    def test_left_join_unused_by_the_stage(self) -> None:
        qb = _qb(extension=_BUYER, measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == set()

    def test_restriction_declared_on_a_routed_model(self) -> None:
        customers = _customers().model_copy(update={"columns": [
            *_customers().columns, Column(name="region_id", type=DataType.INT),
        ]})
        regions = SlayerModel(name="regions", data_source="ds", sql_table="regions", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
        ])
        ext = ModelExtension(source_name="customers", joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]], join_type=JoinType.INNER),
        ])
        query = SlayerQuery.model_validate(
            {"source_model": "orders", "extensions": [ext], "measures": [{"formula": "count(*)"}]},
        )
        qb = SlayerModel(name="qb", data_source="ds", source_queries=[query])
        models = [_orders_with_stored_buyer(), customers, regions, qb]
        entries = column_dependents(models=models, model=customers, column="region_id")
        assert {e.model_name for e in entries if isinstance(e, WholeModelDelete)} == {"qb"}


class TestExtensionDefinitionFrames:
    def test_definition_revisiting_the_root_attributes_its_own_join(self) -> None:
        orders = _orders().model_copy(update={"joins": [
            ModelJoin(target_model="customers", join_pairs=[["buyer_ref", "id"]], name="customer"),
        ]})
        customers = _customers().model_copy(update={
            "columns": [*_customers().columns, Column(name="last_order_id", type=DataType.INT)],
            "joins": [ModelJoin(target_model="orders", join_pairs=[["last_order_id", "id"]], name="last_order")],
        })
        ext = ModelExtension(source_name="customers", columns=[
            Column(name="latest", sql="last_order.amount", type=DataType.DOUBLE)])
        qb = _qb(extension=ext, dimensions=["customer.latest"], measures=[{"formula": "count(*)"}])
        entries = column_dependents(models=[orders, customers, qb], model=customers, column="last_order_id")
        assert {e.model_name for e in entries if isinstance(e, WholeModelDelete)} == {"qb"}


def test_an_inline_source_shadows_the_stored_model_it_is_named_like() -> None:
    inline = _customers().model_copy(update={"sql_table": "crm_customers"})
    qb = SlayerModel(name="qb", data_source="ds", source_queries=[
        SlayerQuery.model_validate({"source_model": inline, "measures": [{"formula": "count(code)", "name": "c"}]})])
    entries = column_dependents(models=[_customers(), qb], model=_customers(), column="code")
    assert {e.model_name for e in entries if isinstance(e, WholeModelDelete)} == set()
