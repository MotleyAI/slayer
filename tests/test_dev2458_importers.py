"""DISTINCT and percentile aggregates import with their meaning.

Specs: importers/meaning-preservation › DISTINCT and percentile aggregates import with their meaning;
importers/dbt-import › dbt percentile flags import with their meaning.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

import duckdb
import pytest

from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeProject
from slayer.dbt.converter import ConversionResult, DbtToSlayerConverter
from slayer.dbt.models import (
    DbtMeasure, DbtMeasureAggParams, DbtMetric, DbtMetricTypeParams, DbtProject, DbtSemanticModel,
)
from slayer.importers.aggregates import ExprResult, convert_expression
from slayer.sql.dialects import dialect_for_ds_type
from tests._dev2171_fixtures import DS, cube_issues, datasets_of, has_metric, import_cube, seed, value
from tests._dev2243_fixtures import metric
from tests.test_dev2243_documents import ddl_engine, metric_value, parity_doc

ID = {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True}

#: qty 1, 2, 2, 1, 3, NULL → distinct 1, 2, 3; amount 10, 20, 30, 40, 60, NULL.
QTY_DDL = (
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, qty INTEGER, amount INTEGER, status VARCHAR)",
    "INSERT INTO orders VALUES (1, 1, 10, 'a'), (2, 2, 20, 'a'), (3, 2, 30, 'b'), (4, 1, 40, 'b'), "
    "(5, 3, 60, 'b'), (6, NULL, NULL, 'c')",
)


def _run(expr: str, *, ds_type: str | None = None) -> ExprResult:
    return convert_expression(
        expr, entity_name="m", owner_of=lambda _q, _c: "orders", ref_of=lambda _m, c: c,
        sql_dialect=dialect_for_ds_type(ds_type) if ds_type else None,
    )


def _cube(sql: str, *, name: str = "m") -> dict[str, Any]:
    return {"name": "orders", "sql_table": "orders", "dimensions": [ID],
            "measures": [{"name": name, "type": "number", "sql": sql}]}


def _cube_text(sql: str, *, target: str | None = None) -> str | None:
    project = CubeProject.model_validate({"cubes": [_cube(sql)]})
    result = CubeToSlayerConverter(project=project, data_source=DS, target_dialect=target).convert()
    found = datasets_of(result)["orders"].get_metric("m")
    return None if found is None else found.text


def _source_value(db: Path, sql: str) -> Any:
    with contextlib.closing(duckdb.connect(str(db))) as con:
        row = con.execute(f"SELECT {sql} FROM orders").fetchone()
        assert row is not None
        return row[0]


# ── the shared SQL aggregate importer ─────────────────────────────────────

@pytest.mark.parametrize(("sql", "expected"), [
    ("SUM(DISTINCT amount)", "sum_distinct(amount)"),
    ("sum(distinct amount)", "sum_distinct(amount)"),
    ("AVG(DISTINCT amount)", "avg_distinct(amount)"),
    ("MIN(DISTINCT amount)", "min(amount)"),
    ("MAX(DISTINCT amount)", "max(amount)"),
    ("PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY amount)", "percentile_disc(amount, p=0.9)"),
    ("PERCENTILE_DISC(0) WITHIN GROUP (ORDER BY amount)", "percentile_disc(amount, p=0)"),
    ("APPROX_PERCENTILE(amount, 0.5)", "percentile_approx(amount, p=0.5)"),
])
def test_converts(sql: str, expected: str) -> None:
    result = _run(sql)
    assert result.ok, result.reason
    assert result.expression == expected


@pytest.mark.parametrize(("sql", "head"), [
    ("SUM(DISTINCT amount) FILTER (WHERE status = 'paid')", "sum_distinct(amount)"),
    ("AVG(DISTINCT amount) FILTER (WHERE status = 'paid')", "avg_distinct(amount)"),
    ("PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount) FILTER (WHERE status = 'paid')",
     "percentile_disc(amount, p=0.5)"),
    ("APPROX_PERCENTILE(amount, 0.5) FILTER (WHERE status = 'paid')", "percentile_approx(amount, p=0.5)"),
])
def test_filtered_form_converts(sql: str, head: str) -> None:
    result = _run(sql)
    assert result.ok, result.reason
    assert result.expression is not None
    assert result.expression.startswith(head)
    assert "FILTER" in result.expression.upper()


@pytest.mark.parametrize("ds_type", ["trino", "snowflake"])
def test_approximate_percentile_on_trino_and_snowflake(ds_type: str) -> None:
    """Spec: Approximate percentile on Trino and Snowflake."""
    result = _run("APPROX_PERCENTILE(amount, 0.5)", ds_type=ds_type)
    assert result.ok, result.reason
    assert result.expression == "percentile_approx(amount, p=0.5)"


@pytest.mark.parametrize("sql", [
    "APPROX_PERCENTILE(amount, 0.5, 100)",
    "APPROX_PERCENTILE(amount, amount)",
    "APPROX_PERCENTILE(amount, 1.5)",
    "APPROX_PERCENTILE(amount, -0.1)",
    "PERCENTILE_DISC(1.5) WITHIN GROUP (ORDER BY amount)",
    "PERCENTILE_DISC(amount) WITHIN GROUP (ORDER BY amount)",
], ids=["accuracy", "non-literal", "above-one", "negative", "disc-above-one", "disc-non-literal"])
def test_an_unusable_fraction_or_extra_argument_is_dropped(sql: str) -> None:
    result = _run(sql, ds_type="trino")
    assert not result.ok
    assert result.expression is None
    assert result.reason


@pytest.mark.parametrize(("sql", "construct"), [
    ("STDDEV(DISTINCT amount)", "DISTINCT"),
    ("COUNT(DISTINCT a, b)", "DISTINCT"),
    ("SUM(DISTINCT a, b)", "DISTINCT"),
])
def test_unsupported_distinct_stays_dropped(sql: str, construct: str) -> None:
    """Spec: Unsupported DISTINCT stays dropped."""
    result = _run(sql)
    assert not result.ok
    assert construct in (result.reason or "").upper()


# ── Cube ──────────────────────────────────────────────────────────────────

class TestCube:
    async def test_distinct_sum(self, tmp_path: Path) -> None:
        """Spec: Cube DISTINCT sum."""
        db = seed(tmp_path, QTY_DDL)
        assert _cube_text("SUM(DISTINCT {CUBE}.qty)") == "sum_distinct(qty)"
        result = import_cube([_cube("SUM(DISTINCT {CUBE}.qty)")], db=db)
        source = _source_value(db, "SUM(DISTINCT qty)")
        assert source == 6
        assert await value(datasets=result.datasets, db=db, dataset="orders", expression="m") == source

    @pytest.mark.parametrize(("sql", "source"), [
        ("AVG(DISTINCT {CUBE}.qty)", "AVG(DISTINCT qty)"),
        ("MIN(DISTINCT {CUBE}.qty)", "MIN(qty)"),
        ("MAX(DISTINCT {CUBE}.qty)", "MAX(qty)"),
        ("PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY {CUBE}.amount)",
         "PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY amount)"),
        ("SUM(DISTINCT {CUBE}.qty) FILTER (WHERE {CUBE}.status = 'b')", "SUM(DISTINCT qty) FILTER (WHERE status = 'b')"),
    ], ids=["avg", "min", "max", "percentile-disc", "filtered-sum"])
    async def test_keeps_the_source_value(self, tmp_path: Path, sql: str, source: str) -> None:
        db = seed(tmp_path, QTY_DDL)
        result = import_cube([_cube(sql)], db=db)
        assert has_metric(result, "orders", "m"), result.report.issues
        expected = _source_value(db, source)
        assert float(await value(datasets=result.datasets, db=db, dataset="orders", expression="m")) == \
            pytest.approx(float(expected))

    def test_discrete_percentile(self) -> None:
        """Spec: Discrete percentile."""
        assert _cube_text("PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY {CUBE}.amount)") == \
            "percentile_disc(amount, p=0.9)"

    @pytest.mark.parametrize("target", ["trino", "snowflake"])
    def test_approximate_percentile_on_a_trino_and_snowflake_target(self, target: str) -> None:
        """Spec: Approximate percentile on Trino and Snowflake."""
        assert _cube_text("APPROX_PERCENTILE({CUBE}.amount, 0.5)", target=target) == "percentile_approx(amount, p=0.5)"

    def test_bucketed_approximate_quantiles_are_dropped(self) -> None:
        """Spec: Bucketed approximate quantiles are dropped."""
        project = CubeProject.model_validate({"cubes": [_cube("APPROX_QUANTILES({CUBE}.amount, 100)[OFFSET(50)]",
                                                              name="p50")]})
        result = CubeToSlayerConverter(project=project, data_source=DS, target_dialect="bigquery").convert()
        assert not has_metric(result, "orders", "p50")
        assert cube_issues(result, "p50"), result.report.issues

    @pytest.mark.parametrize("sql", ["STDDEV(DISTINCT {CUBE}.amount)", "COUNT(DISTINCT {CUBE}.qty, {CUBE}.amount)"],
                             ids=["stddev", "count-two-operands"])
    def test_unsupported_distinct_stays_dropped(self, sql: str) -> None:
        """Spec: Unsupported DISTINCT stays dropped."""
        result = import_cube([_cube(sql, name="bad")])
        assert not has_metric(result, "orders", "bad")
        assert [i for i in cube_issues(result, "bad") if "DISTINCT" in i.message.upper()], result.report.issues


# ── Ossie documents (portable sql and dialect entries) ───────────────────

#: Parity seed: orders.customer_id 101, 101, 102; amount 10, 5, 7; status completed, open, completed.
@pytest.mark.parametrize(("sql", "expected"), [
    pytest.param("SUM(DISTINCT customer_id)", 203, id="sum-distinct"),
    pytest.param("SUM(DISTINCT customer_id) FILTER (WHERE status = 'open')", 101, id="filtered-sum-distinct"),
    pytest.param("AVG(DISTINCT customer_id)", 101.5, id="avg-distinct"),
    pytest.param("MIN(DISTINCT amount)", 5, id="min-distinct"),
    pytest.param("MAX(DISTINCT amount)", 10, id="max-distinct"),
    pytest.param("PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount)", 7, id="percentile-disc"),
])
async def test_an_ossie_metric_keeps_its_meaning(tmp_path: Path, sql: str, expected: float) -> None:
    assert float(await metric_value(tmp_path, dataset_name="orders", sql=sql)) == pytest.approx(expected)


async def test_an_ossie_approximate_percentile_is_within_tolerance(tmp_path: Path) -> None:
    found = await metric_value(tmp_path, dataset_name="orders", sql="APPROX_PERCENTILE(amount, 0.5)")
    assert float(found) == pytest.approx(7, abs=1)


@pytest.mark.parametrize(("expression", "expected"), [
    ("AVG(DISTINCT customer_id)", 101.5),
    ("SUM(DISTINCT customer_id)", 203),
])
async def test_an_ossie_dialect_entry_keeps_its_meaning(tmp_path: Path, expression: str, expected: float) -> None:
    entry = {"name": "v", "expression": {"dialects": [{"dialect": "DUCKDB", "expression": expression}]}}
    async with ddl_engine(tmp_path, parity_doc(extra={"orders": {"metrics": [entry, metric("ok", "SUM(amount)")]}})) \
            as engine:
        resp = await engine.execute({"population": "orders", "metrics": ["orders.v"]})
        assert float(resp.data[0]["orders.v"]) == pytest.approx(expected)


# ── dbt percentile flags ──────────────────────────────────────────────────

def _dbt(params: DbtMeasureAggParams, *, metrics: list[DbtMetric] | None = None) -> ConversionResult:
    project = DbtProject(
        semantic_models=[DbtSemanticModel(name="orders", model="orders", measures=[
            DbtMeasure(name="p_amount", agg="percentile", expr="amount", agg_params=params)])],
        metrics=metrics or [],
    )
    return DbtToSlayerConverter(project=project, data_source="test").convert()


def _dbt_text(result: ConversionResult, name: str) -> str | None:
    found = datasets_of(result)["orders"].get_metric(name)
    return None if found is None else found.text


def _dbt_reports(result: ConversionResult, name: str) -> list[Any]:
    return [e for e in (*result.unconverted_metrics, *result.warnings) if name in (e.metric_name or e.message)]


class TestDbtPercentileFlags:
    def test_discrete_percentile_measure(self) -> None:
        """Spec: Discrete percentile measure."""
        result = _dbt(DbtMeasureAggParams(percentile=0.9, use_discrete_percentile=True))
        assert _dbt_text(result, "p_amount") == "percentile_disc(amount, p=0.9)"

    def test_approximate_percentile_measure(self) -> None:
        """Spec: Approximate percentile measure."""
        result = _dbt(DbtMeasureAggParams(percentile=0.9, use_approximate_percentile=True))
        assert _dbt_text(result, "p_amount") == "percentile_approx(amount, p=0.9)"

    def test_both_flags_on_a_measure(self) -> None:
        """Spec: Both flags."""
        result = _dbt(DbtMeasureAggParams(percentile=0.5, use_discrete_percentile=True,
                                          use_approximate_percentile=True))
        assert _dbt_text(result, "p_amount") == "percentile_disc(amount, p=0.5)"
        assert _dbt_reports(result, "p_amount") == []

    def test_both_flags_on_a_filtered_metric(self) -> None:
        """Spec: Both flags."""
        filtered = DbtMetric(name="us_p50", type="simple",
                             type_params=DbtMetricTypeParams.model_validate({"measure": "p_amount"}),
                             filter="{{ Dimension('orders__region') }} = 'US'")
        result = _dbt(DbtMeasureAggParams(percentile=0.5, use_discrete_percentile=True,
                                          use_approximate_percentile=True), metrics=[filtered])
        text = _dbt_text(result, "us_p50")
        assert text is not None, _dbt_reports(result, "us_p50")
        assert text.startswith("percentile_disc(")
        assert text.endswith("p=0.5)")
        assert _dbt_reports(result, "us_p50") == []

    @pytest.mark.parametrize(("flag", "agg"), [("use_discrete_percentile", "percentile_disc"),
                                               ("use_approximate_percentile", "percentile_approx")])
    def test_each_flag_on_a_filtered_metric(self, flag: str, agg: str) -> None:
        filtered = DbtMetric(name="us_p90", type="simple",
                             type_params=DbtMetricTypeParams.model_validate({"measure": "p_amount"}),
                             filter="{{ Dimension('orders__region') }} = 'US'")
        result = _dbt(DbtMeasureAggParams.model_validate({"percentile": 0.9, flag: True}), metrics=[filtered])
        text = _dbt_text(result, "us_p90")
        assert text is not None, _dbt_reports(result, "us_p90")
        assert text.startswith(f"{agg}(")
        assert text.endswith("p=0.9)")

    def test_missing_percentile_value(self) -> None:
        """Spec: Missing percentile value."""
        result = _dbt(DbtMeasureAggParams(use_discrete_percentile=True))
        assert _dbt_text(result, "p_amount") is None
        assert _dbt_reports(result, "p_amount")
