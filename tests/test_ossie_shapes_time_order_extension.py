"""Time dimensions name their result ``alias``, order items take ``field`` as text, extensions name their ``dataset``.

Spec: compatibility/ossie-vocabulary › Time dimensions, order items and extensions use Ossie's words.

The 1.x and pre-release spellings below are deliberate: keep this file out of any respelling pass.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from slayer.core.errors import SlayerError, UnknownKeyError
from slayer.core.query import DatasetExtension, OrderItem, SlayerQuery, TimeDimension
from tests._deprecated_input_helpers import deprecated_in, deprecations, errors_in, payload_of, recorded
from tests._ossie_shapes_fixtures import execute, shop_engine_store

EXTENSION_METRICS = [{"name": "aov", "expression": "avg(amount)"}]


class TestTimeDimension:
    async def test_an_aliased_time_dimension_names_its_column(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        response = await execute(storage, {"population": "orders", "metrics": ["count(*)"], "time_dimensions": [
            {"field": "ordered_at", "granularity": "month", "alias": "m"}]})
        assert response.columns[0].split(".")[-1] == "m"
        assert len(response.data) > 1

    def test_an_alias_dumps_as_alias(self) -> None:
        td = TimeDimension.model_validate({"field": "ordered_at", "granularity": "month", "alias": "m"})
        assert td.alias == "m"
        dumped = td.model_dump(mode="json", exclude_none=True)
        assert dumped["alias"] == "m"
        assert "name" not in dumped

    def test_an_object_field_is_refused(self) -> None:
        with pytest.raises((ValidationError, SlayerError)):
            TimeDimension.model_validate({"field": {"name": "ordered_at"}, "granularity": "month"})

    def test_a_time_dimension_name_is_refused(self) -> None:
        with pytest.raises(ValidationError) as info:
            TimeDimension.model_validate({"field": "ordered_at", "granularity": "month", "name": "m"})
        [err] = [e for e in errors_in(info.value) if isinstance(e, UnknownKeyError)]
        assert err.key == "name"


class TestOrderItem:
    def test_field_is_text_with_a_derived_reference(self) -> None:
        item = OrderItem.model_validate({"field": "customers.region", "direction": "desc"})
        assert item.field == "customers.region"
        assert item.ref is not None
        assert item.ref.full_name == "customers.region"
        assert item.model_dump(mode="json", exclude_none=True) == {"field": "customers.region", "direction": "desc"}

    def test_an_expression_has_no_reference(self) -> None:
        item = OrderItem.model_validate({"field": "sum(amount)"})
        assert item.field == "sum(amount)"
        assert item.ref is None

    def test_a_reference_object_is_refused(self) -> None:
        with pytest.raises((ValidationError, SlayerError)) as info:
            OrderItem.model_validate({"field": {"name": "region", "dataset": "customers"}})
        assert any(isinstance(e, SlayerError) for e in errors_in(info.value))

    def test_a_column_order_item_converts(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({"population": "orders", "metrics": ["count(*)"],
                                                "order": [{"column": "status"}]})
        assert [o.field for o in query.order or []] == ["status"]
        assert len([w for w in deprecations(caught) if "column" in payload_of(w).old]) == 1

    async def test_an_order_item_sorts_by_dotted_text(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        response = await execute(storage, {
            "population": "orders", "dimensions": ["customers.region"], "metrics": ["count(*)"],
            "order": [{"field": "customers.region", "direction": "desc"}]})
        regions = [r[response.columns[0]] for r in response.data]
        assert regions == sorted(regions, reverse=True)
        assert len(set(regions)) > 1


class TestExtension:
    def test_an_extension_names_its_dataset(self) -> None:
        ext = DatasetExtension.model_validate({"dataset": "orders", "metrics": EXTENSION_METRICS})
        assert ext.dataset == "orders"
        dumped = ext.model_dump(mode="json", exclude_none=True)
        assert dumped["dataset"] == "orders"
        assert "source_name" not in dumped

    async def test_an_extension_metric_is_available(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        with recorded() as caught:
            response = await execute(storage, {"population": "orders", "metrics": ["aov"],
                                               "extensions": [{"dataset": "orders", "metrics": EXTENSION_METRICS}]})
        assert deprecations(caught) == []
        [row] = response.data
        assert row[response.columns[0]] == pytest.approx(35.0)

    async def test_an_old_extension_key_converts(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        with recorded():
            old = await execute(storage, {"population": "orders", "metrics": ["aov"],
                                          "extensions": [{"source_name": "orders", "metrics": EXTENSION_METRICS}]})
        new = await execute(storage, {"population": "orders", "metrics": ["aov"],
                                      "extensions": [{"dataset": "orders", "metrics": EXTENSION_METRICS}]})
        assert old.data == new.data
        [payload] = deprecated_in(old.warnings)
        assert "source_name" in payload.old
        assert "dataset" in payload.new
