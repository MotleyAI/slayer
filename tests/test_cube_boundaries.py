"""Negative validator-boundary tests.

A naive converter would let collisions / reserved names / broken SQL throw a
`ValidationError` and lose the whole model. The converter must instead route
them to the report and emit what it safely can — `convert()` never raises.
"""

import pytest

from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import (
    CubeCube,
    CubeDimension,
    CubeJoin,
    CubeMeasure,
    CubeProject,
)
from slayer.cube.report import CubeIssueCategory

DS = "test_ds"


def _convert(project: CubeProject):
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    return {m.name: m for m in result.models}, result.report


@pytest.mark.parametrize("name", ["cumsum", "rank"])
def test_measure_named_after_transform_is_imported(name):
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name=name, type="sum", sql="{CUBE}.amount")],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = _convert(project)
    assert models["orders"].get_measure(name) is not None


def test_dimension_measure_name_overlap_does_not_raise():
    """A dimension and a measure that would share a name (SLayer forbids the
    overlap) must be disambiguated, not crash the model."""
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="revenue", type="sum", sql="{CUBE}.amount")],
        dimensions=[CubeDimension(name="revenue", sql="{CUBE}.revenue_flag",
                                  type="string")],
    )])
    models, _ = _convert(project)
    orders = models["orders"]
    col_names = {c.name for c in orders.columns}
    measure_names = {m.name for m in orders.measures}
    assert not (col_names & measure_names)  # disjoint after disambiguation


def test_whole_run_survives_one_broken_cube():
    """A structurally-broken cube must not abort conversion of the others."""
    project = CubeProject(cubes=[
        CubeCube(name="good", sql_table="public.good",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="bad"),  # no source
    ])
    models, report = _convert(project)
    assert "good" in models
    assert report.issues


def test_structurally_broken_column_sql_dropped_via_offline_validation():
    """Codex #7: a translated Column.sql that doesn't parse as SQL must be caught
    by the offline validation pass and dropped (complex_sql), not persisted to
    blow up later at enrichment."""
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[
            CubeDimension(name="id", sql="{CUBE}.id", type="number"),
            CubeDimension(name="broken", sql="{CUBE}.amount + )", type="number"),
        ],
    )])
    models, report = _convert(project)
    assert models["orders"].get_column("broken") is None
    assert models["orders"].get_column("id") is not None
    assert any(i.category == CubeIssueCategory.COMPLEX_SQL for i in report.issues)


def test_cube_name_with_illegal_chars_is_reported_not_fatal():
    """A Cube name containing `.`/`:` (rejected by SlayerModel.name) must be
    routed to the report, not crash the run."""
    project = CubeProject(cubes=[
        CubeCube(name="weird.name", sql_table="public.x"),
        CubeCube(name="ok", sql_table="public.ok",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
    ])
    models, report = _convert(project)
    assert "weird.name" not in models
    assert "ok" in models
    assert report.issues


def test_join_on_without_equality_yields_no_empty_join_pairs():
    """An ON with no equality must drop the join (never construct ModelJoin with
    empty join_pairs, which the validator rejects)."""
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="customers", relationship="many_to_one",
                                 sql="{CUBE}.customer_id")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="customers", sql_table="public.customers",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
    ])
    models, report = _convert(project)
    assert models["orders"].joins == []
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_JOIN for i in report.issues)
