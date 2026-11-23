"""Dataset objects validate assignments.

Spec: compatibility/ossie-vocabulary › Dataset objects validate assignments.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
import slayer.mcp.server as mcp_server
from slayer.mcp.server import create_mcp_server
from slayer.storage.base import StorageBackend

from tests._deprecated_input_helpers import recorded
from tests._ossie_shapes_fixtures import execute, shop_engine_store
from tests._ossie_vocabulary_fixtures import shop_orders
from tests._stored_upgrade_fixtures import DS
from tests._validation_refusals import construction_refusal, refusal, state
from tests._mcp_results import text_blocks

RELATIONSHIP = {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}


def _orders() -> SlayerDataset:
    return SlayerDataset.model_validate(shop_orders())


def _refused_assignment(obj: BaseModel, attribute: str, value: Any) -> Any:
    """The refusal assigning ``value`` raises; the object is checked unchanged."""
    before = state(obj)
    with pytest.raises(Exception) as caught:  # noqa: PT011 — compared with construction's refusal below
        setattr(obj, attribute, value)
    assert state(obj) == before
    return refusal(caught.value)


class TestInvalidAssignment:
    def test_a_field_label_must_be_text(self) -> None:
        field = DatasetField(name="amount")
        got = _refused_assignment(field, "label", 5)
        assert got == construction_refusal(DatasetField, {"name": "amount", "label": 5})

    def test_a_relationship_keeps_its_key_lists_paired(self) -> None:
        relationship = DatasetRelationship.model_validate(RELATIONSHIP)
        got = _refused_assignment(relationship, "from_columns", ["a", "b"])
        assert got == construction_refusal(DatasetRelationship, {**RELATIONSHIP, "from_columns": ["a", "b"]})

    def test_a_dataset_refuses_two_fields_of_one_name_and_keeps_its_list(self) -> None:
        dataset = _orders()
        kept = dataset.fields
        names = [f.name for f in kept]
        got = _refused_assignment(dataset, "fields", [*kept, DatasetField(name="amount")])
        assert dataset.fields is kept
        assert [f.name for f in dataset.fields] == names
        data = shop_orders()
        assert got == construction_refusal(SlayerDataset, {**data, "fields": [*data["fields"], {"name": "amount"}]})

    def test_a_field_with_an_expression_refuses_the_deprecated_sql_too(self) -> None:
        field = DatasetField(name="amount", expression="amount")
        got = _refused_assignment(field, "sql", "a + b")
        assert got == construction_refusal(DatasetField, {"name": "amount", "expression": "amount", "sql": "a + b"})

    def test_a_datasource_port_must_be_a_number(self) -> None:
        datasource = DatasourceConfig(name=DS, type="sqlite", database="x.db")
        got = _refused_assignment(datasource, "port", "abc")
        assert got == construction_refusal(
            DatasourceConfig, {"name": DS, "type": "sqlite", "database": "x.db", "port": "abc"})


class TestValidAssignment:
    async def test_assigned_label_and_description_apply_and_the_dataset_queries(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        amount = orders.get_field("amount")
        assert amount is not None
        amount.label = "Order amount"
        orders.description = "Every order."
        assert amount.label == "Order amount"
        assert orders.description == "Every order."
        await storage.save_dataset(orders)
        stored = await storage.get_dataset("orders", data_source=DS)
        assert stored is not None
        stored_amount = stored.get_field("amount")
        assert stored_amount is not None
        assert stored_amount.label == "Order amount"
        assert stored.description == "Every order."
        response = await execute(storage, {"population": "orders", "metrics": ["sum(orders.amount)"]})
        assert len(response.data) == 1


class TestLinkedAttributes:
    def test_a_table_backed_dataset_switches_to_sql_in_one_step(self) -> None:
        dataset = _orders()
        dataset.assign(sql="SELECT * FROM orders", sql_table=None)
        assert dataset.sql == "SELECT * FROM orders"
        assert dataset.sql_table is None

    def test_a_relationship_changes_both_key_lists_in_one_step(self) -> None:
        relationship = DatasetRelationship.model_validate(RELATIONSHIP)
        relationship.assign(from_columns=["a", "b"], to_columns=["c", "d"])
        assert relationship.column_pairs == [("a", "c"), ("b", "d")]

    def test_a_refused_change_leaves_every_attribute_unchanged(self) -> None:
        dataset = _orders()
        before = state(dataset)
        with pytest.raises(Exception) as caught:  # noqa: PT011 — compared with construction's refusal below
            dataset.assign(description="Every order.", sql="SELECT * FROM orders")
        assert state(dataset) == before
        assert refusal(caught.value) == construction_refusal(
            SlayerDataset, {**shop_orders(), "description": "Every order.", "sql": "SELECT * FROM orders"})

    def test_a_change_naming_no_field_is_refused_and_changes_nothing(self) -> None:
        number_format = NumberFormat()
        before = state(number_format)
        with pytest.raises(ValueError, match="no field '_note'"):
            number_format.assign(symbol="$", _note=True)
        assert state(number_format) == before

    @pytest.mark.parametrize("rename_first", [False, True])
    def test_the_final_state_decides_whatever_the_keyword_order(self, rename_first: bool) -> None:
        dataset = SlayerDataset.model_validate({"name": "old", "sql_table": "t", "data_source": DS,
                                                "fields": [{"name": "amount"}, {"name": "discount"}]})
        fields = [{"name": "amount"}, {"name": "discount"},
                  {"name": "net", "sql": "amount - discount"}, {"name": "copy", "sql": "net"}]
        changes: dict[str, Any] = {"name": "new", "fields": fields} if rename_first else {"fields": fields, "name": "new"}
        with recorded():
            dataset.assign(**changes)
        copy = dataset.get_field("copy")
        assert copy is not None
        assert copy.expression == "new.net"

    def test_the_deprecated_key_pairs_attribute_writes_both_key_lists(self) -> None:
        relationship = DatasetRelationship.model_validate(RELATIONSHIP)
        with recorded() as caught:
            setattr(relationship, "join_pairs", [("a", "c"), ("b", "d")])
        assert relationship.from_columns == ["a", "b"]
        assert relationship.to_columns == ["c", "d"]
        future = [w for w in caught if issubclass(w.category, FutureWarning)]
        assert len(future) == 1
        assert "column_pairs" in str(future[0].message)


async def _edit(storage: StorageBackend, **arguments: Any) -> str:
    """One MCP ``edit_dataset`` call on a fresh server: its text, or the raised error's text."""
    server = create_mcp_server(storage=storage, _seed_help=False)
    try:
        blocks = text_blocks(await server.call_tool(name="edit_dataset", arguments={"data_source": DS, **arguments}))
        return "\n".join(getattr(b, "text", "") for b in blocks)
    except Exception as exc:  # noqa: BLE001 — the refusal text is the assertion subject
        return f"{type(exc).__name__}: {exc}"
    finally:
        await getattr(server, "_slayer_engine").aclose()


async def _stored(storage: StorageBackend, name: str) -> SlayerDataset:
    dataset = await storage.get_dataset(name, data_source=DS)
    assert dataset is not None
    return dataset


class TestEditSourceSwitch:
    async def test_the_source_switches_in_every_direction(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        people = {"name": "people", "sql_table": "customers", "data_source": DS,
                  "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                             {"name": "region", "datatype": "String"}]}
        await storage.save_dataset(SlayerDataset.model_validate(people))

        reply = await _edit(storage, dataset_name="people", sql="SELECT * FROM customers")
        stored = await _stored(storage, "people")
        assert (stored.sql, stored.sql_table, stored.source_queries) == ("SELECT * FROM customers", None, None), reply

        reply = await _edit(storage, dataset_name="people", sql_table="customers")
        stored = await _stored(storage, "people")
        assert (stored.sql, stored.sql_table, stored.source_queries) == (None, "customers", None), reply

        stages = [{"population": "customers", "dimensions": ["customers.region"]}]
        reply = await _edit(storage, dataset_name="people", source_queries=stages)
        stored = await _stored(storage, "people")
        assert (stored.sql, stored.sql_table) == (None, None), reply
        assert stored.source_queries is not None
        assert len(stored.source_queries) == 1
        assert [f.name for f in stored.fields] == ["region"]

        reply = await _edit(storage, dataset_name="people", sql_table="customers")
        stored = await _stored(storage, "people")
        assert (stored.sql, stored.sql_table, stored.source_queries) == (None, "customers", None), reply

    async def test_a_switch_to_source_queries_refreshes_the_whole_dataset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage = await shop_engine_store(tmp_path)
        people = {"name": "people", "sql_table": "customers", "data_source": DS,
                  "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                             {"name": "region", "datatype": "String"}]}
        await storage.save_dataset(SlayerDataset.model_validate(people))
        refreshes: list[dict[str, Any]] = []

        async def record(**kwargs: Any) -> list[str]:
            refreshes.append(kwargs)
            return []

        monkeypatch.setattr(mcp_server, "handle_edit_refresh", record)
        stages = [{"population": "customers", "dimensions": ["customers.region"]}]
        reply = await _edit(storage, dataset_name="people", source_queries=stages)
        assert [r["dataset_level_change"] for r in refreshes] == [True], reply

    async def test_a_switch_whose_call_removes_the_metrics_applies(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        people = {"name": "people", "sql_table": "customers", "data_source": DS,
                  "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                             {"name": "region", "datatype": "String"}],
                  "metrics": [{"name": "headcount", "expression": "count(*)"}]}
        await storage.save_dataset(SlayerDataset.model_validate(people))
        stages = [{"population": "customers", "dimensions": ["customers.region"]}]
        reply = await _edit(storage, dataset_name="people", source_queries=stages, remove={"metrics": ["headcount"]})
        stored = await _stored(storage, "people")
        assert stored.source_queries is not None, reply
        assert stored.metrics == []

    async def test_a_switch_the_final_state_refuses_leaves_the_dataset_unchanged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage = await shop_engine_store(tmp_path)
        before = (await _stored(storage, "orders")).model_dump()
        loaded: list[SlayerDataset] = []
        get_dataset = storage.get_dataset

        async def capture(*args: Any, **kwargs: Any) -> SlayerDataset | None:
            out = await get_dataset(*args, **kwargs)
            if out is not None:
                loaded.append(out)
            return out

        monkeypatch.setattr(storage, "get_dataset", capture)
        stages = [{"population": "customers", "dimensions": ["customers.region"]}]
        reply = await _edit(storage, dataset_name="orders", source_queries=stages)
        assert "cannot declare metrics" in reply
        edited = loaded[0]
        assert (edited.sql_table, edited.sql, edited.source_queries) == ("orders", None, None)
        assert edited.model_dump() == before
        monkeypatch.undo()
        assert (await _stored(storage, "orders")).model_dump() == before


class TestConstructionNormalizations:
    def test_a_deprecated_sql_naming_one_column_reads_as_the_expression(self) -> None:
        field = DatasetField.model_validate({"name": "total", "sql": "amount"})
        assert (field.expression, field.sql, field.from_sql) == ("amount", None, True)

    def test_a_currency_format_defaults_its_symbol(self) -> None:
        assert NumberFormat(type=NumberFormatType.CURRENCY).symbol == "$"

    def test_an_sql_mode_list_field_defaults_to_opaque_without_touching_the_prebuilt_field(self) -> None:
        prebuilt = DatasetField(name="tags", datatype=DataType.ARRAY)
        dataset = SlayerDataset(name="d", sql="SELECT tags FROM t", data_source=DS, fields=[prebuilt])
        assert dataset.fields[0].element_type is DataType.OPAQUE
        assert prebuilt.element_type is None

    def test_a_prebuilt_sql_field_placed_in_a_dataset_keeps_its_origin(self) -> None:
        prebuilt = DatasetField.model_validate({"name": "total", "sql": "amount"})
        before = state(prebuilt)
        dataset = SlayerDataset(name="d", sql_table="t", data_source=DS, fields=[prebuilt])
        assert dataset.fields[0].from_sql is True
        assert dataset.fields[0].expression == "amount"
        assert state(prebuilt) == before


def test_a_deprecated_sql_assigned_after_construction_stays_as_written() -> None:
    field = DatasetField(name="amount")
    field.sql = "amount"
    assert (field.sql, field.expression) == ("amount", None)
