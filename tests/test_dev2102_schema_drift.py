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


def _qb(*, extension: ModelExtension | None = None, **stage: object) -> SlayerModel:
    extensions = {"extensions": [extension]} if extension else {}
    query = SlayerQuery.model_validate({"source_model": "orders", **extensions, **stage})
    return SlayerModel(name="qb", data_source="ds", source_queries=[query])


def _whole_dropped(*, qb: SlayerModel, model: SlayerModel, column: str) -> set[str]:
    entries = column_dependents(models=[_orders(), _customers(), qb], model=model, column=column)
    return {e.model_name for e in entries if isinstance(e, WholeModelDelete)}


_NET = ModelExtension(source_name="orders", measures=[ModelMeasure(name="net", formula="sum(amount - tax)")])
_NET_DIMENSION = {"expression": "amount - tax", "name": "net"}
_BUYER = ModelExtension(source_name="orders", joins=[
    ModelJoin(target_model="customers", join_pairs=[["buyer_ref", "id"]], name="buyer"),
])


class TestExtensionDefinitionDependencies:
    def test_computed_dimension_reading_dropped_column(self) -> None:
        qb = _qb(dimensions=[_NET_DIMENSION], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_measure_over_an_expression(self) -> None:
        qb = _qb(extension=_NET, measures=[{"formula": "net", "name": "n"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_measure_condition_reading_dropped_column(self) -> None:
        ext = ModelExtension(source_name="orders", measures=[
            ModelMeasure(name="paid", formula="sum(iif(status = 'paid', amount, 0))"),
        ])
        qb = _qb(extension=ext, measures=[{"formula": "paid", "name": "p"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="status") == {"qb"}

    def test_extension_measure_reading_dropped_column(self) -> None:
        ext = ModelExtension(source_name="orders", measures=[ModelMeasure(name="taxes", formula="sum(tax)")])
        qb = _qb(extension=ext, measures=[{"formula": "taxes"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_measure_unused_by_the_stage(self) -> None:
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

    def test_extension_measure_aliasing_a_dropped_column(self) -> None:
        ext = ModelExtension(source_name="orders", measures=[ModelMeasure(name="gross", formula="sum(amount)")])
        qb = _qb(extension=ext, measures=[{"formula": "gross", "name": "g"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="amount") == {"qb"}


_UPPER_BUYER = {"expression": "UPPER(buyer.name)", "name": "buyer_name"}


def _orders_with_stored_buyer() -> SlayerModel:
    return _orders().model_copy(update={"joins": [
        ModelJoin(target_model="customers", join_pairs=[["buyer_ref", "id"]], name="buyer"),
    ]})


class TestExtensionDefinitionJoinPaths:
    def test_computed_dimension_through_extension_join(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=[_UPPER_BUYER], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_computed_dimension_through_stored_join(self) -> None:
        orders = _orders_with_stored_buyer()
        qb = _qb(dimensions=[_UPPER_BUYER], measures=[{"formula": "count(*)"}])
        entries = column_dependents(models=[orders, _customers(), qb], model=orders, column="buyer_ref")
        assert {e.model_name for e in entries if isinstance(e, WholeModelDelete)} == {"qb"}

    def test_extension_measure_through_extension_join(self) -> None:
        ext = _BUYER.model_copy(update={"measures": [ModelMeasure(name="buyers", formula="count_distinct(buyer.name)")]})
        qb = _qb(extension=ext, measures=[{"formula": "buyers"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_sibling_stage_computed_dimension_through_extension_join(self) -> None:
        ext = ModelExtension(source_name="s", joins=_BUYER.joins)
        qb = SlayerModel(name="qb", data_source="ds", source_queries=[
            SlayerQuery.model_validate({"name": "s", "source_model": "orders", "dimensions": ["buyer_ref"]}),
            SlayerQuery.model_validate({"source_model": "s", "extensions": [ext], "dimensions": [_UPPER_BUYER]}),
        ])
        assert _whole_dropped(qb=qb, model=_customers(), column="name") == {"qb"}

    def test_extension_measure_through_join_unused_by_the_stage(self) -> None:
        ext = _BUYER.model_copy(update={"measures": [
            ModelMeasure(name="buyer_names", formula="count_distinct(UPPER(buyer.name))")]})
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
        ext = ModelExtension(source_name="customers", measures=[
            ModelMeasure(name="latest", formula="max(last_order.amount)")])
        qb = _qb(extension=ext, measures=[{"formula": "customer.latest", "name": "l"}])
        entries = column_dependents(models=[orders, customers, qb], model=customers, column="last_order_id")
        assert {e.model_name for e in entries if isinstance(e, WholeModelDelete)} == {"qb"}


def test_a_computed_dimension_over_an_expression_depends_on_what_it_reads() -> None:
    qb = _qb(dimensions=[{"expression": "(amount - tax) + 1", "name": "net1"}], measures=[{"formula": "count(*)"}])
    assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}
