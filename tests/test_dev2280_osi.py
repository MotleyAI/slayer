"""Instant comparisons export as standard SQL (spec: importers/osi-export)."""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from tests._osi_export_fixtures import DATASOURCE
from tests._osi_export_helpers import all_metrics, dataset, export_datasets, make_env, mentions, metric, reimport, rows

EV_DDL = [
    "CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP, amount DOUBLE)",
    "INSERT INTO ev VALUES (1, '2024-03-05 08:00:00', 10), (2, '2024-04-10 09:30:00', 20)",
]


def ev_dataset(*measures: DatasetMetric) -> SlayerDataset:
    return SlayerDataset(
        name="ev", sql_table="ev", data_source=DATASOURCE,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="ts", datatype=DataType.DATETIME),
                 DatasetField(name="amount", datatype=DataType.FLOAT)],
        metrics=list(measures),
    )


def compares(sql: str, *, op: type[Expression], literal: str) -> bool:
    """Whether ``sql`` holds ``ts <op> '<literal>'``."""
    return any(
        isinstance(node.expression, exp.Literal) and node.expression.this == literal
        and isinstance(node.this, exp.Column) and node.this.name == "ts"
        for node in sqlglot.parse_one(sql).find_all(op)
    )


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path, statements=EV_DDL)


class TestExport:
    async def test_date_only_bound_in_a_filtered_measure(self, env) -> None:
        early = DatasetMetric(name="early", expression="count(*) FILTER (WHERE ts <= '2024-04-10')")
        result = await export_datasets(env, ev_dataset(early))
        exported = metric(dataset(result.document, "ev"), "early")
        assert exported is not None, result.entries
        assert compares(exported["expression"]["sql"], op=exp.LTE, literal="2024-04-10"), exported
        original = await rows(env, {"population": "ev", "metrics": ["ev.early"]})
        imported, _ = await reimport(env, result.document)
        again = await rows(imported, {"population": "ev", "metrics": ["ev.early"]})
        assert int(again[0]["ev.early"]) == int(original[0]["ev.early"]) == 1

    async def test_date_time_bound(self, env) -> None:
        late = DatasetMetric(name="late", expression="count(*) FILTER (WHERE ts > '2024-04-10 09:00:00')")
        result = await export_datasets(env, ev_dataset(late))
        exported = metric(dataset(result.document, "ev"), "late")
        assert exported is not None, result.entries
        assert compares(exported["expression"]["sql"], op=exp.GT, literal="2024-04-10 09:00:00"), exported

    @pytest.mark.parametrize("formula", [
        "count(*) FILTER (WHERE ts in '2024-04-10')", "count(*) FILTER (WHERE ts = '2025-Q1')",
        "count(*) FILTER (WHERE ts >= 'last month')",
    ])
    async def test_period_and_relative_comparisons_stay_slayer_only(self, env, formula: str) -> None:
        result = await export_datasets(env, ev_dataset(DatasetMetric(name="p", expression=formula)), extensions=False)
        assert all(x["name"] != "p" for x in all_metrics(result.document))
        assert mentions(result, "p", severity="dropped"), result.entries

