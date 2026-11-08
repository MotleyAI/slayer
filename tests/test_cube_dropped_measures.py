"""Cube import: measures over a dropped (unparseable-SQL) column, and calc measures depending on them, are dropped and reported."""

import pytest

from slayer.core.models import SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import (
    CubeCube,
    CubeDimension,
    CubeMeasure,
    CubeProject,
    CubeView,
    CubeViewCubeRef,
)
from slayer.cube.report import CubeConversionReport, CubeIssueCategory

_BROKEN_SQL = "{CUBE}.amount +* ((("


def _orders_cube() -> CubeCube:
    return CubeCube(
        name="orders", sql_table="public.orders",
        measures=[
            CubeMeasure(name="count", type="count"),
            CubeMeasure(name="bad_total", type="sum", sql=_BROKEN_SQL),
            CubeMeasure(name="bad_ratio", type="number", sql="{bad_total} / {count}"),
            CubeMeasure(name="bad_ratio2", type="number", sql="{bad_ratio} * 2"),
            # Collides with the `status` dimension → emitted as `status_measure`.
            CubeMeasure(name="status", type="sum", sql=_BROKEN_SQL),
            CubeMeasure(name="ok_total", type="sum", sql="{CUBE}.amount"),
        ],
        dimensions=[
            CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True),
            CubeDimension(name="status", sql="{CUBE}.status", type="string"),
        ],
    )


@pytest.fixture
def converted() -> tuple[dict[str, SlayerModel], CubeConversionReport]:
    view = CubeView(name="orders_view", cubes=[CubeViewCubeRef(
        join_path="orders", includes=["count", "bad_total", "status", "ok_total"])])
    result = CubeToSlayerConverter(
        project=CubeProject(cubes=[_orders_cube()], views=[view]), data_source="ds",
    ).convert()
    return {m.name: m for m in result.models}, result.report


def _measure_issues(report: CubeConversionReport, *members: str) -> list:
    return [i for i in report.issues
            if i.category == CubeIssueCategory.COMPLEX_MEASURE and i.member in members]


def test_agg_measure_over_dropped_column_is_dropped_and_reported(converted) -> None:
    models, report = converted
    assert models["orders"].get_measure("bad_total") is None
    issues = _measure_issues(report, "bad_total")
    assert issues, report.issues
    assert any("sum(bad_total_col)" in i.message for i in issues), issues
    assert all(":sum" not in i.message for i in issues), issues


def test_dependent_calc_measures_are_dropped_and_reported(converted) -> None:
    models, report = converted
    orders = models["orders"]
    assert orders.get_measure("bad_ratio") is None
    assert orders.get_measure("bad_ratio2") is None
    assert _measure_issues(report, "bad_ratio")
    assert _measure_issues(report, "bad_ratio2")


def test_collision_renamed_measure_over_dropped_column_is_dropped(converted) -> None:
    models, report = converted
    assert models["orders"].get_measure("status_measure") is None
    assert _measure_issues(report, "status", "status_measure")


def test_unaffected_measures_survive(converted) -> None:
    models, _ = converted
    formulas = {m.name: m.formula for m in models["orders"].measures}
    assert formulas["count"] == "count(*)"
    assert formulas["ok_total"] == "sum(amount)"


def test_calc_measure_over_bare_column_is_dropped_and_reported() -> None:
    cube = CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="doubled", type="number", sql="{CUBE}.amount * 2")],
        dimensions=[
            CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True),
            CubeDimension(name="amount", sql="{CUBE}.amount", type="number"),
        ],
    )
    result = CubeToSlayerConverter(project=CubeProject(cubes=[cube]), data_source="ds").convert()
    orders = next(m for m in result.models if m.name == "orders")
    assert orders.get_measure("doubled") is None
    assert _measure_issues(result.report, "doubled")


def test_view_does_not_reexport_dropped_measures(converted) -> None:
    models, _ = converted
    view = models["orders_view"]
    names = {m.name for m in view.measures}
    assert "count" in names
    assert "ok_total" in names
    assert "bad_total" not in names
    assert not any(n.startswith("status") for n in names)
