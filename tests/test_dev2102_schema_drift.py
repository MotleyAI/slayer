"""Schema drift attributes a saved stage's reads through its query extensions' definitions."""

from __future__ import annotations

from slayer.core.enums import DataType, JoinType
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import DatasetExtension, SlayerQuery
from slayer.live_schema.schema_drift import WholeDatasetDelete, field_dependents


def _orders() -> SlayerDataset:
    return SlayerDataset(name="orders", data_source="ds", sql_table="orders", fields=[
        DatasetField(name="id", datatype=DataType.INT, primary_key=True),
        DatasetField(name="amount", datatype=DataType.DOUBLE),
        DatasetField(name="tax", datatype=DataType.DOUBLE),
        DatasetField(name="status", datatype=DataType.TEXT),
        DatasetField(name="buyer_ref", datatype=DataType.INT),
    ])


def _customers() -> SlayerDataset:
    return SlayerDataset(name="customers", data_source="ds", sql_table="customers", fields=[
        DatasetField(name="id", datatype=DataType.INT, primary_key=True),
        DatasetField(name="name", datatype=DataType.TEXT),
        DatasetField(name="code", datatype=DataType.TEXT, unique=True),
    ])


def _qb(*, extension: DatasetExtension | None = None, **stage: object) -> SlayerDataset:
    extensions = {"extensions": [extension]} if extension else {}
    query = SlayerQuery.model_validate({"population": "orders", **extensions, **stage})
    return SlayerDataset(name="qb", data_source="ds", source_queries=[query])


def _whole_dropped(*, qb: SlayerDataset, model: SlayerDataset, column: str) -> set[str]:
    entries = field_dependents(datasets=[_orders(), _customers(), qb], dataset=model, field=column)
    return {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)}


_NET = DatasetExtension(source_name="orders", metrics=[DatasetMetric(name="net", expression="sum(amount - tax)")])
_NET_DIMENSION = {"expression": "amount - tax", "name": "net"}
_BUYER = DatasetExtension(source_name="orders", relationships=[
    DatasetRelationship(to="customers", join_pairs=[["buyer_ref", "id"]], name="buyer"),
])


class TestExtensionDefinitionDependencies:
    def test_computed_dimension_reading_dropped_column(self) -> None:
        qb = _qb(dimensions=[_NET_DIMENSION], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_measure_over_an_expression(self) -> None:
        qb = _qb(extension=_NET, measures=[{"formula": "net", "name": "n"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_measure_condition_reading_dropped_column(self) -> None:
        ext = DatasetExtension(source_name="orders", metrics=[
            DatasetMetric(name="paid", expression="sum(iif(status = 'paid', amount, 0))"),
        ])
        qb = _qb(extension=ext, measures=[{"formula": "paid", "name": "p"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="status") == {"qb"}

    def test_extension_measure_reading_dropped_column(self) -> None:
        ext = DatasetExtension(source_name="orders", metrics=[DatasetMetric(name="taxes", expression="sum(tax)")])
        qb = _qb(extension=ext, measures=[{"formula": "taxes"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}

    def test_extension_measure_unused_by_the_stage(self) -> None:
        qb = _qb(extension=_NET, measures=[{"formula": "sum(amount)", "name": "a"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="tax") == set()

    def test_extension_join_local_key_dropped(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=["buyer.name"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_extension_join_foreign_key_dropped(self) -> None:
        ext = DatasetExtension(source_name="orders", relationships=[
            DatasetRelationship(to="customers", join_pairs=[["status", "code"]], name="by_code"),
        ])
        qb = _qb(extension=ext, dimensions=["by_code.name"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_customers(), column="code") == {"qb"}

    def test_extension_join_unused_by_the_stage(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=["status"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == set()

    def test_extension_measure_aliasing_a_dropped_column(self) -> None:
        ext = DatasetExtension(source_name="orders", metrics=[DatasetMetric(name="gross", expression="sum(amount)")])
        qb = _qb(extension=ext, measures=[{"formula": "gross", "name": "g"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="amount") == {"qb"}


_UPPER_BUYER = {"expression": "UPPER(buyer.name)", "name": "buyer_name"}


def _orders_with_stored_buyer() -> SlayerDataset:
    return _orders().model_copy(update={"joins": [
        DatasetRelationship(to="customers", join_pairs=[["buyer_ref", "id"]], name="buyer"),
    ]})


class TestExtensionDefinitionJoinPaths:
    def test_computed_dimension_through_extension_join(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=[_UPPER_BUYER], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_computed_dimension_through_stored_join(self) -> None:
        orders = _orders_with_stored_buyer()
        qb = _qb(dimensions=[_UPPER_BUYER], measures=[{"formula": "count(*)"}])
        entries = field_dependents(datasets=[orders, _customers(), qb], dataset=orders, field="buyer_ref")
        assert {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)} == {"qb"}

    def test_extension_measure_through_extension_join(self) -> None:
        ext = _BUYER.model_copy(update={"metrics": [DatasetMetric(name="buyers", expression="count_distinct(buyer.name)")]})
        qb = _qb(extension=ext, measures=[{"formula": "buyers"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == {"qb"}

    def test_sibling_stage_computed_dimension_through_extension_join(self) -> None:
        ext = DatasetExtension(source_name="s", relationships=_BUYER.relationships)
        qb = SlayerDataset(name="qb", data_source="ds", source_queries=[
            SlayerQuery.model_validate({"name": "s", "population": "orders", "dimensions": ["buyer_ref"]}),
            SlayerQuery.model_validate({"population": "s", "extensions": [ext], "dimensions": [_UPPER_BUYER]}),
        ])
        assert _whole_dropped(qb=qb, model=_customers(), column="name") == {"qb"}

    def test_extension_measure_through_join_unused_by_the_stage(self) -> None:
        ext = _BUYER.model_copy(update={"metrics": [
            DatasetMetric(name="buyer_names", expression="count_distinct(UPPER(buyer.name))")]})
        qb = _qb(extension=ext, dimensions=["status"], measures=[{"formula": "count(*)"}])
        assert _whole_dropped(qb=qb, model=_orders(), column="buyer_ref") == set()


def _inner(*pair: str) -> DatasetExtension:
    return DatasetExtension(source_name="orders", relationships=[
        DatasetRelationship(to="customers", join_pairs=[list(pair)], join_type=JoinType.INNER),
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
            *_customers().fields, DatasetField(name="region_id", datatype=DataType.INT),
        ]})
        regions = SlayerDataset(name="regions", data_source="ds", sql_table="regions", fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
        ])
        ext = DatasetExtension(source_name="customers", relationships=[
            DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]], join_type=JoinType.INNER),
        ])
        query = SlayerQuery.model_validate(
            {"population": "orders", "extensions": [ext], "metrics": [{"expression": "count(*)"}]},
        )
        qb = SlayerDataset(name="qb", data_source="ds", source_queries=[query])
        models = [_orders_with_stored_buyer(), customers, regions, qb]
        entries = field_dependents(datasets=models, dataset=customers, field="region_id")
        assert {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)} == {"qb"}


class TestExtensionDefinitionFrames:
    def test_definition_revisiting_the_root_attributes_its_own_join(self) -> None:
        orders = _orders().model_copy(update={"joins": [
            DatasetRelationship(to="customers", join_pairs=[["buyer_ref", "id"]], name="customer"),
        ]})
        customers = _customers().model_copy(update={
            "columns": [*_customers().fields, DatasetField(name="last_order_id", datatype=DataType.INT)],
            "joins": [DatasetRelationship(to="orders", join_pairs=[["last_order_id", "id"]], name="last_order")],
        })
        ext = DatasetExtension(source_name="customers", metrics=[
            DatasetMetric(name="latest", expression="max(last_order.amount)")])
        qb = _qb(extension=ext, measures=[{"formula": "customer.latest", "name": "l"}])
        entries = field_dependents(datasets=[orders, customers, qb], dataset=customers, field="last_order_id")
        assert {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)} == {"qb"}


def test_a_computed_dimension_over_an_expression_depends_on_what_it_reads() -> None:
    qb = _qb(dimensions=[{"expression": "(amount - tax) + 1", "name": "net1"}], measures=[{"formula": "count(*)"}])
    assert _whole_dropped(qb=qb, model=_orders(), column="tax") == {"qb"}
