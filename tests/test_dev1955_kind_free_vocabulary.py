"""The derived-column error vocabulary carries no ``sql``/``filter`` kind and one
declaring-model field, so the storage and engine save doors cannot disagree."""

from __future__ import annotations

import inspect
import warnings

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import (
    CircularRelationshipPathError,
    DerivedFieldCircularError,
    DerivedFieldFanningError,
)
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

DS = "ds"
_KIND_LABELS = ("sql reference", "filter reference")


def _circular(**overrides: str) -> DerivedFieldCircularError:
    kwargs = {
        "column": "COL", "reference": "A.B.leaf", "root_model": "ROOT",
        "revisited": "REV", "hop": "HOP", "via": "VIA",
    }
    kwargs.update(overrides)
    return DerivedFieldCircularError(**kwargs)


def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source=DS, sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )


def _customers(*, cols: tuple[DatasetField, ...] = ()) -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source=DS, sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INT),
            DatasetField(name="spend", datatype=DataType.DOUBLE),
            *cols,
        ],
        relationships=[DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]],
                         cardinality=RelationshipCardinality.MANY_TO_ONE)],
    )


def _orders(*, cols: tuple[DatasetField, ...] = (), joins: tuple[DatasetRelationship, ...] = ()) -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INT),
            DatasetField(name="amount", datatype=DataType.DOUBLE),
            *cols,
        ],
        relationships=list(joins),
    )


def _line_items() -> SlayerDataset:
    return SlayerDataset(
        name="line_items", data_source=DS, sql_table="line_items",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="order_id", datatype=DataType.INT),
            DatasetField(name="qty", datatype=DataType.DOUBLE),
        ],
    )


def _li_join(*, cardinality: RelationshipCardinality | None = None) -> DatasetRelationship:
    return DatasetRelationship(to="line_items", join_pairs=[["id", "order_id"]],
                     cardinality=cardinality)


def _params(cls: type) -> dict[str, inspect.Parameter]:
    return dict(inspect.signature(cls.__init__).parameters)


class TestFanningErrorVocabulary:
    def test_signature_has_no_kind_and_requires_reference(self) -> None:
        params = _params(DerivedFieldFanningError)
        assert "kind" not in params
        assert params["reference"].default is inspect.Parameter.empty

    def test_no_kind_attribute_and_new_message(self) -> None:
        exc = DerivedFieldFanningError(
            field="li_qty", dataset="orders", hop="line_items", reference="line_items.qty",
        )
        assert not hasattr(exc, "kind")
        assert exc.reference == "line_items.qty"
        msg = str(exc)
        assert msg.startswith(
            "Derived column 'li_qty' on model 'orders' references 'line_items.qty', "
            "crossing a fanning join hop to 'line_items': "
        )
        assert "<aggregation>(line_items.qty)" in msg
        assert not any(label in msg for label in _KIND_LABELS)


class TestCircularErrorVocabulary:
    def test_signature_has_no_kind_or_model(self) -> None:
        params = _params(DerivedFieldCircularError)
        assert "kind" not in params
        assert "model" not in params

    def test_model_is_root_model_alias(self) -> None:
        exc = _circular()
        assert isinstance(exc, CircularRelationshipPathError)
        assert exc.dataset == "ROOT"
        assert exc.dataset == exc.root_model
        assert not hasattr(exc, "kind")

    def test_model_is_read_only(self) -> None:
        exc = _circular()
        with pytest.raises(AttributeError):
            exc.dataset = "OTHER"  # pyright: ignore[reportAttributeAccessIssue]

    def test_new_message(self) -> None:
        msg = str(_circular())
        assert msg.startswith(
            "Derived column 'COL' on model 'ROOT' references 'A.B.leaf', which "
            "revisits model 'REV' (hop 'HOP' from 'VIA'): "
        )
        assert "not a column of 'ROOT'" in msg
        assert "Reference the column on 'REV'" in msg
        assert "declare the aggregate on 'VIA'" in msg
        assert not any(label in msg for label in _KIND_LABELS)


class TestUnprovenWarning:
    async def test_message_has_no_kind_label(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_dataset(_line_items())
        orders = _orders(
            cols=(DatasetField(name="li_qty", sql="line_items.qty", datatype=DataType.DOUBLE),),
            joins=(_li_join(),),
        )
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            await storage.save_dataset(orders)
        msgs = [str(w.message) for w in rec if "li_qty" in str(w.message)]
        assert len(msgs) == 1
        assert msgs[0].startswith(
            "Derived column 'li_qty' on model 'orders' has a reference crossing "
            "an unproven join hop to 'line_items' "
        )
        assert not any(label in msgs[0] for label in _KIND_LABELS)

    async def test_sql_and_filter_across_same_hop_warn_once(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_dataset(_line_items())
        orders = _orders(
            cols=(DatasetField(name="big_qty", sql="line_items.qty", datatype=DataType.DOUBLE,
                         filter="line_items.qty >= 2"),),
            joins=(_li_join(),),
        )
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            await storage.save_dataset(orders)
        hits = [w for w in rec
                if "big_qty" in str(w.message) and "line_items" in str(w.message)]
        assert len(hits) == 1
        assert await storage.get_dataset("orders", data_source=DS) is not None


class TestFanningReferenceSpelling:
    async def test_host_prefixed_reference_kept(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_dataset(_line_items())
        orders = _orders(
            cols=(DatasetField(name="li_qty", sql="orders.line_items.qty", datatype=DataType.DOUBLE),),
            joins=(_li_join(cardinality=RelationshipCardinality.ONE_TO_MANY),),
        )
        with pytest.raises(DerivedFieldFanningError) as ei:
            await storage.save_dataset(orders)
        exc = ei.value
        assert exc.reference == "orders.line_items.qty"
        assert exc.hop == "line_items"
        assert "<aggregation>(orders.line_items.qty)" in str(exc)
        assert ":<aggregation>" not in str(exc)

    async def test_filter_reference_is_the_offending_spelling(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_dataset(_line_items())
        orders = _orders(
            cols=(DatasetField(name="big_item", sql="amount", datatype=DataType.DOUBLE,
                         filter="line_items.qty >= 2"),),
            joins=(_li_join(cardinality=RelationshipCardinality.ONE_TO_MANY),),
        )
        with pytest.raises(DerivedFieldFanningError) as ei:
            await storage.save_dataset(orders)
        assert ei.value.reference == "line_items.qty"
        assert not any(label in str(ei.value) for label in _KIND_LABELS)


class TestEngineDoorCrossModelAttribution:
    async def test_revisit_inside_referenced_column_names_its_declaring_model(
        self, tmp_path,
    ) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite"))
        await storage.save_dataset(_regions())
        await storage.save_dataset(
            _customers(cols=(DatasetField(name="bad", datatype=DataType.DOUBLE, sql="spend",
                                    filter="regions.customers.spend > 0"),)),
            _validate=False,
        )
        orders = _orders(
            cols=(DatasetField(name="outer", datatype=DataType.DOUBLE, sql="customers.bad * 2"),),
            joins=(DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]],
                             cardinality=RelationshipCardinality.MANY_TO_ONE),),
        )
        engine = SlayerQueryEngine(storage=storage)
        with pytest.raises(DerivedFieldCircularError) as ei:
            await engine.save_dataset(orders)
        exc = ei.value
        assert exc.field == "bad"
        assert exc.dataset == "customers"
        assert exc.root_model == "customers"
        assert exc.reference == "regions.customers.spend"
        assert exc.revisited == "customers"
        msg = str(exc)
        assert "Derived column 'bad' on model 'customers'" in msg
        assert "not a column of 'customers'" in msg
        assert "'orders'" not in msg
        assert not any(label in msg for label in _KIND_LABELS)
        assert await storage.get_dataset("orders", data_source=DS) is None
