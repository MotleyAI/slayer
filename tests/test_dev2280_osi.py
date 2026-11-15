"""Instant comparisons export as standard SQL, and imported date predicates keep Ossie's meaning
(specs: importers/osi-export, importers/osi-import)."""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from tests._dev2171_fixtures import import_osi, seed, value
from tests._osi_export_fixtures import DATASOURCE
from tests._osi_export_helpers import all_metrics, dataset, export_models, make_env, mentions, metric, reimport, rows
from tests._osi_helpers import ds, f, flat, m

EV_DDL = [
    "CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TIMESTAMP, amount DOUBLE)",
    "INSERT INTO ev VALUES (1, '2024-03-05 08:00:00', 10), (2, '2024-04-10 09:30:00', 20)",
]
IMPORT_DDL = [s.replace(" ts ", " created_at ").replace("(id, ts", "(id, created_at") for s in EV_DDL]


def ev_model(*measures: ModelMeasure) -> SlayerModel:
    return SlayerModel(
        name="ev", sql_table="ev", data_source=DATASOURCE,
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="ts", type=DataType.TIMESTAMP),
                 Column(name="amount", type=DataType.DOUBLE)],
        measures=list(measures),
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
        early = ModelMeasure(name="early", formula="count(*) FILTER (WHERE ts <= '2024-04-10')")
        result = await export_models(env, ev_model(early))
        exported = metric(dataset(result.document, "ev"), "early")
        assert exported is not None, result.entries
        assert compares(exported["expression"]["sql"], op=exp.LTE, literal="2024-04-10"), exported
        original = await rows(env, {"source_model": "ev", "measures": ["early"]})
        imported, _ = await reimport(env, result.document)
        again = await rows(imported, {"source_model": "ev", "measures": ["early"]})
        assert int(again[0]["ev.early"]) == int(original[0]["ev.early"]) == 1

    async def test_date_time_bound(self, env) -> None:
        late = ModelMeasure(name="late", formula="count(*) FILTER (WHERE ts > '2024-04-10 09:00:00')")
        result = await export_models(env, ev_model(late))
        exported = metric(dataset(result.document, "ev"), "late")
        assert exported is not None, result.entries
        assert compares(exported["expression"]["sql"], op=exp.GT, literal="2024-04-10 09:00:00"), exported

    @pytest.mark.parametrize("formula", [
        "count(*) FILTER (WHERE ts in '2024-04-10')", "count(*) FILTER (WHERE ts = '2025-Q1')",
        "count(*) FILTER (WHERE ts >= 'last month')",
    ])
    async def test_period_and_relative_comparisons_stay_slayer_only(self, env, formula: str) -> None:
        result = await export_models(env, ev_model(ModelMeasure(name="p", formula=formula)), extensions=False)
        assert all(x["name"] != "p" for x in all_metrics(result.document))
        assert mentions(result, "p", severity="dropped"), result.entries


class TestImport:
    @pytest.fixture
    def db(self, tmp_path: Path) -> Path:
        return seed(tmp_path, IMPORT_DDL)

    @pytest.mark.parametrize(("sql", "expected"), [
        ("COUNT(*) FILTER (WHERE created_at <= '2024-04-10')", 1),
        ("SUM(CASE WHEN created_at > '2024-04-10' THEN amount END)", 20),
    ])
    async def test_date_only_bound_keeps_ossie_meaning(self, db: Path, sql: str, expected: int) -> None:
        document = flat([ds("ev", fields=[f("id"), f("created_at"), f("amount")], metrics=[m("v", sql)])])
        result = import_osi(document, db=db)
        got = await value(models=result.models, db=db, model="ev", formula="v")
        assert got is not None
        assert float(got) == expected, got
