"""An sql-mode base field is matched by the column it reads, quoting removed. Spec: models/schema-drift."""

from __future__ import annotations

from typing import Any

from slayer.core.enums import DataType
from slayer.core.models import SlayerDataset
from slayer.live_schema.schema_drift import (
    EditDatasetDelete, RetypeFinding, UntypedFinding, compute_datasource_drops, diff_sql_dataset,
    sql_dataset_retype_findings,
)
from slayer.sql.dialects.reflection import ResultColumnType


def _payments(*, datatype: str | None = "Float", **extra: Any) -> SlayerDataset:
    total: dict[str, Any] = {"name": "total", "expression": '"amount"', "datatype": datatype}  # null: undeclared
    return SlayerDataset.model_validate({
        "name": "payments", "sql": "SELECT id, amount FROM raw_payments", "data_source": "warehouse",
        "fields": [{"name": "id", "datatype": "Integer", "primary_key": True}, total],
        "metrics": [{"name": "revenue", "expression": "sum(total)"}],
        **extra,
    })


def _drops(dataset: SlayerDataset, live: list[str]) -> list[Any]:
    diffs = {dataset.name: diff_sql_dataset(dataset=dataset, live_columns=live)}
    return compute_datasource_drops(datasets=[dataset], sql_table_diffs={}, sql_diffs=diffs)


def test_a_quoted_base_field_is_not_dropped() -> None:
    dataset = _payments()
    assert diff_sql_dataset(dataset=dataset, live_columns=["id", "amount"]) == (None, set())
    assert _drops(dataset, ["id", "amount"]) == []


def test_a_quoted_base_field_is_typed_from_its_column() -> None:
    live = {"id": ResultColumnType(type=DataType.INTEGER), "amount": ResultColumnType(type=DataType.STRING)}
    findings = sql_dataset_retype_findings(dataset=_payments(), live_columns=live)
    assert [(f.field, f.live_type) for f in findings if isinstance(f, RetypeFinding)] == [("total", DataType.STRING)]


def test_an_untyped_quoted_base_field_gets_its_columns_type() -> None:
    live = {"id": ResultColumnType(type=DataType.INTEGER), "amount": ResultColumnType(type=DataType.FLOAT)}
    dataset = _payments(datatype=None, semantic_model="payments_doc")
    findings = sql_dataset_retype_findings(dataset=dataset, live_columns=live)
    assert [(f.field, f.live_type) for f in findings if isinstance(f, UntypedFinding)] == [("total", DataType.FLOAT)]


def test_a_quoted_base_field_whose_column_vanished_is_dropped() -> None:
    [drop] = _drops(_payments(), ["id"])
    assert isinstance(drop, EditDatasetDelete)
    assert drop.remove.fields == ["total"]
    assert drop.remove.metrics == ["revenue"]
    reasons = {r.target: r.reason for r in drop.reasons}
    assert "'total'" in reasons["field:total"]
