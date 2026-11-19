"""Schema drift attributes a saved stage's reads through its query extensions' definitions."""

from __future__ import annotations

from slayer.core.enums import DataType, JoinType
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import DatasetExtension, SlayerQuery
from slayer.live_schema.schema_drift import WholeDatasetDelete, field_dependents


def _orders() -> SlayerDataset:
    return SlayerDataset(name="orders", data_source="ds", sql_table="orders", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="amount", datatype=DataType.FLOAT),
        DatasetField(name="tax", datatype=DataType.FLOAT),
        DatasetField(name="status", datatype=DataType.STRING),
        DatasetField(name="buyer_ref", datatype=DataType.INTEGER),
    ])


def _customers() -> SlayerDataset:
    return SlayerDataset(name="customers", data_source="ds", sql_table="customers", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="name", datatype=DataType.STRING),
        DatasetField(name="code", datatype=DataType.STRING, unique=True),
    ])


def _qb(*, extension: DatasetExtension | None = None, **stage: object) -> SlayerDataset:
    extensions = {"extensions": [extension]} if extension else {}
    query = SlayerQuery.model_validate({"population": "orders", **extensions, **stage})
    return SlayerDataset(name="qb", data_source="ds", source_queries=[query])


def _whole_dropped(*, qb: SlayerDataset, dataset: SlayerDataset, column: str) -> set[str]:
    entries = field_dependents(datasets=[_orders(), _customers(), qb], dataset=dataset, field=column)
    return {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)}


_NET = DatasetExtension(dataset="orders", metrics=[DatasetMetric(name="net", expression="sum(amount - tax)")])
_NET_DIMENSION = {"field": "amount - tax", "alias": "net"}
_BUYER = DatasetExtension(dataset="orders", relationships=[
    DatasetRelationship(to="customers", from_columns=["buyer_ref"], to_columns=["id"], name="buyer"),
])


class TestExtensionDefinitionDependencies:
    def test_computed_dimension_reading_dropped_column(self) -> None:
        qb = _qb(dimensions=[_NET_DIMENSION], metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="tax") == {"qb"}

    def test_extension_measure_over_an_expression(self) -> None:
        qb = _qb(extension=_NET, metrics=[{"metric": "orders.net", "alias": "n"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="tax") == {"qb"}

    def test_extension_measure_condition_reading_dropped_column(self) -> None:
        ext = DatasetExtension(dataset="orders", metrics=[
            DatasetMetric(name="paid", expression="sum(iif(status = 'paid', amount, 0))"),
        ])
        qb = _qb(extension=ext, metrics=[{"metric": "orders.paid", "alias": "p"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="status") == {"qb"}

    def test_extension_measure_reading_dropped_column(self) -> None:
        ext = DatasetExtension(dataset="orders", metrics=[DatasetMetric(name="taxes", expression="sum(tax)")])
        qb = _qb(extension=ext, metrics=[{"metric": "orders.taxes"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="tax") == {"qb"}

    def test_extension_measure_unused_by_the_stage(self) -> None:
        qb = _qb(extension=_NET, metrics=[{"metric": "sum(orders.amount)", "alias": "a"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="tax") == set()

    def test_extension_join_local_key_dropped(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=["buyer.name"], metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="buyer_ref") == {"qb"}

    def test_extension_join_foreign_key_dropped(self) -> None:
        ext = DatasetExtension(dataset="orders", relationships=[
            DatasetRelationship(to="customers", from_columns=["status"], to_columns=["code"], name="by_code"),
        ])
        qb = _qb(extension=ext, dimensions=["by_code.name"], metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_customers(), column="code") == {"qb"}

    def test_extension_join_unused_by_the_stage(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=["orders.status"], metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="buyer_ref") == set()

    def test_extension_measure_aliasing_a_dropped_column(self) -> None:
        ext = DatasetExtension(dataset="orders", metrics=[DatasetMetric(name="gross", expression="sum(amount)")])
        qb = _qb(extension=ext, metrics=[{"metric": "orders.gross", "alias": "g"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="amount") == {"qb"}


_UPPER_BUYER = {"field": "UPPER(buyer.name)", "alias": "buyer_name"}


def _orders_with_stored_buyer() -> SlayerDataset:
    return _orders().model_copy(update={"relationships": [
        DatasetRelationship(to="customers", from_columns=["buyer_ref"], to_columns=["id"], name="buyer"),
    ]})


class TestExtensionDefinitionJoinPaths:
    def test_computed_dimension_through_extension_join(self) -> None:
        qb = _qb(extension=_BUYER, dimensions=[_UPPER_BUYER], metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="buyer_ref") == {"qb"}

    def test_computed_dimension_through_stored_join(self) -> None:
        orders = _orders_with_stored_buyer()
        qb = _qb(dimensions=[_UPPER_BUYER], metrics=[{"metric": "count(orders.*)"}])
        entries = field_dependents(datasets=[orders, _customers(), qb], dataset=orders, field="buyer_ref")
        assert {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)} == {"qb"}

    def test_extension_measure_through_extension_join(self) -> None:
        ext = _BUYER.model_copy(update={"metrics": [DatasetMetric(name="buyers", expression="count_distinct(buyer.name)")]})
        qb = _qb(extension=ext, metrics=[{"metric": "orders.buyers"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="buyer_ref") == {"qb"}

    def test_sibling_stage_computed_dimension_through_extension_join(self) -> None:
        ext = DatasetExtension(dataset="s", relationships=_BUYER.relationships)
        qb = SlayerDataset(name="qb", data_source="ds", source_queries=[
            SlayerQuery.model_validate({"name": "s", "population": "orders", "dimensions": ["orders.buyer_ref"]}),
            SlayerQuery.model_validate({"population": "s", "extensions": [ext], "dimensions": [_UPPER_BUYER]}),
        ])
        assert _whole_dropped(qb=qb, dataset=_customers(), column="name") == {"qb"}

    def test_extension_measure_through_join_unused_by_the_stage(self) -> None:
        ext = _BUYER.model_copy(update={"metrics": [
            DatasetMetric(name="buyer_names", expression="count_distinct(UPPER(buyer.name))")]})
        qb = _qb(extension=ext, dimensions=["orders.status"], metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="buyer_ref") == set()


def _inner(*pair: str) -> DatasetExtension:
    return DatasetExtension(dataset="orders", relationships=[
        DatasetRelationship(to="customers", from_columns=[p[0] for p in [list(pair)]], to_columns=[p[1] for p in [list(pair)]], join_type=JoinType.INNER),
    ])


class TestInnerExtensionJoinRestriction:
    def test_local_key_dropped(self) -> None:
        qb = _qb(extension=_inner("buyer_ref", "id"), metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="buyer_ref") == {"qb"}

    def test_target_primary_key_dropped(self) -> None:
        qb = _qb(extension=_inner("buyer_ref", "id"), metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_customers(), column="id") == {"qb"}

    def test_target_key_dropped(self) -> None:
        qb = _qb(extension=_inner("status", "code"), metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_customers(), column="code") == {"qb"}

    def test_left_join_unused_by_the_stage(self) -> None:
        qb = _qb(extension=_BUYER, metrics=[{"metric": "count(orders.*)"}])
        assert _whole_dropped(qb=qb, dataset=_orders(), column="buyer_ref") == set()

    def test_restriction_declared_on_a_routed_model(self) -> None:
        customers = _customers().model_copy(update={"fields": [
            *_customers().fields, DatasetField(name="region_id", datatype=DataType.INTEGER),
        ]})
        regions = SlayerDataset(name="regions", data_source="ds", sql_table="regions", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        ])
        ext = DatasetExtension(dataset="customers", relationships=[
            DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"], join_type=JoinType.INNER),
        ])
        query = SlayerQuery.model_validate(
            {"population": "orders", "extensions": [ext], "metrics": [{"metric": "count(orders.*)"}]},
        )
        qb = SlayerDataset(name="qb", data_source="ds", source_queries=[query])
        models = [_orders_with_stored_buyer(), customers, regions, qb]
        entries = field_dependents(datasets=models, dataset=customers, field="region_id")
        assert {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)} == {"qb"}


class TestExtensionDefinitionFrames:
    def test_definition_revisiting_the_root_attributes_its_own_join(self) -> None:
        orders = _orders().model_copy(update={"relationships": [
            DatasetRelationship(to="customers", from_columns=["buyer_ref"], to_columns=["id"], name="customer"),
        ]})
        customers = _customers().model_copy(update={
            "fields": [*_customers().fields, DatasetField(name="last_order_id", datatype=DataType.INTEGER)],
            "relationships": [DatasetRelationship(to="orders", from_columns=["last_order_id"], to_columns=["id"], name="last_order")],
        })
        ext = DatasetExtension(dataset="customers", metrics=[
            DatasetMetric(name="latest", expression="max(last_order.amount)")])
        qb = _qb(extension=ext, metrics=[{"metric": "customer.latest", "alias": "l"}])
        entries = field_dependents(datasets=[orders, customers, qb], dataset=customers, field="last_order_id")
        assert {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)} == {"qb"}


def test_a_computed_dimension_over_an_expression_depends_on_what_it_reads() -> None:
    qb = _qb(dimensions=[{"field": "(orders.amount - orders.tax) + 1", "alias": "net1"}], metrics=[{"metric": "count(orders.*)"}])
    assert _whole_dropped(qb=qb, dataset=_orders(), column="tax") == {"qb"}
