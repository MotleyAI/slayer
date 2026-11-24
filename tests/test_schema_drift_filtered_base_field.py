"""Schema drift checks the source column of every base field, filtered ones included. Spec: models/schema-drift."""

from __future__ import annotations

from typing import Any

from slayer.core.enums import DataType
from slayer.core.models import SlayerDataset, relationship_key_error
from slayer.live_schema.schema_drift import (
    EditDatasetDelete, LiveTable, RetypeFinding, compute_datasource_drops, diff_sql_table_dataset, retype_findings,
)


def _orders(fields: list[dict[str, Any]], metrics: list[dict[str, Any]]) -> SlayerDataset:
    return SlayerDataset.model_validate({
        "name": "orders", "sql_table": "orders", "data_source": "warehouse",
        "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                   {"name": "status", "datatype": "String"}, *fields],
        "metrics": metrics,
    })


def _paid_orders() -> SlayerDataset:
    return _orders(
        fields=[{"name": "amount", "expression": "amount", "datatype": "Float"},
                {"name": "paid_amount", "expression": "amount", "datatype": "Float", "filter": "status = 'paid'"}],
        metrics=[{"name": "revenue", "expression": "sum(amount)"},
                 {"name": "paid_revenue", "expression": "sum(paid_amount)"}],
    )


def _drop(dataset: SlayerDataset, live: dict[str, DataType]) -> EditDatasetDelete:
    diffs = {dataset.name: diff_sql_table_dataset(dataset=dataset, live_table=LiveTable(columns=live),
                                                  available_datasets_in_ds={dataset.name})}
    [drop] = compute_datasource_drops(datasets=[dataset], sql_table_diffs=diffs, sql_diffs={})
    assert isinstance(drop, EditDatasetDelete)
    return drop


def _reasons(drop: EditDatasetDelete) -> dict[str, str]:
    return {r.target: r.reason for r in drop.reasons}


def test_a_filtered_field_whose_column_vanished_is_dropped() -> None:
    drop = _drop(_paid_orders(), {"id": DataType.INTEGER, "status": DataType.STRING})
    assert sorted(drop.remove.fields) == ["amount", "paid_amount"]
    assert sorted(drop.remove.metrics) == ["paid_revenue", "revenue"]
    reasons = _reasons(drop)
    assert "'amount'" in reasons["field:amount"]
    assert "'amount'" in reasons["field:paid_amount"]
    assert "paid_amount" in reasons["metric:paid_revenue"]


def test_the_diff_reports_the_filtered_field_dropped() -> None:
    dataset = _paid_orders()
    _, dropped = diff_sql_table_dataset(dataset=dataset, live_table=LiveTable(columns={"id": DataType.INTEGER}),
                                        available_datasets_in_ds={"orders"})
    assert {"amount", "paid_amount"} <= dropped


def test_a_filtered_field_over_a_retyped_column_is_a_retype_finding() -> None:
    live = LiveTable(columns={"id": DataType.INTEGER, "status": DataType.STRING, "amount": DataType.STRING})
    findings = [f for f in retype_findings(dataset=_paid_orders(), live_table=live) if isinstance(f, RetypeFinding)]
    assert sorted((f.field, f.stored_type, f.live_type) for f in findings) == [
        ("amount", DataType.FLOAT, DataType.STRING), ("paid_amount", DataType.FLOAT, DataType.STRING),
    ]


def test_two_fields_on_one_quoted_column_and_metrics_naming_either_are_removed() -> None:
    dataset = _orders(
        fields=[{"name": "amt", "expression": '"Amount"', "datatype": "Float"},
                {"name": "paid_amt", "expression": '"Amount"', "datatype": "Float", "filter": "status = 'paid'"}],
        metrics=[{"name": "total", "expression": "sum(amt)"}, {"name": "paid_total", "expression": "sum(paid_amt)"}],
    )
    drop = _drop(dataset, {"id": DataType.INTEGER, "status": DataType.STRING})
    assert sorted(drop.remove.fields) == ["amt", "paid_amt"]
    assert sorted(drop.remove.metrics) == ["paid_total", "total"]


def test_a_present_quoted_column_drops_nothing() -> None:
    dataset = _orders(
        fields=[{"name": "paid_amt", "expression": '"Amount"', "datatype": "Float", "filter": "status = 'paid'"}],
        metrics=[{"name": "paid_total", "expression": "sum(paid_amt)"}],
    )
    entry, dropped = diff_sql_table_dataset(
        dataset=dataset,
        live_table=LiveTable(columns={"id": DataType.INTEGER, "status": DataType.STRING, "Amount": DataType.FLOAT}),
        available_datasets_in_ds={"orders"},
    )
    assert entry is None and dropped == set()


def test_a_filtered_field_is_still_not_a_relationship_key() -> None:
    dataset = _paid_orders()
    error = relationship_key_error(dataset="customers", target="orders", key="paid_amount", side="orders",
                                   fields=dataset.fields)
    assert error is not None
    assert "paid_amount" in str(error)
