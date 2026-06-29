"""Tests for Cube `extends` flattening (slayer/cube/extends.py + converter).

DEV-1608 §5. Flatten base members into children (child wins); abstract bases
emitted hidden; cycles reported.
"""

from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeCube, CubeDimension, CubeMeasure, CubeProject
from slayer.cube.report import CubeIssueCategory

DS = "test_ds"


def _convert(project: CubeProject):
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    return {m.name: m for m in result.models}, result.report


def test_child_inherits_base_members():
    project = CubeProject(cubes=[
        CubeCube(name="base_events", sql_table="public.events", public=False,
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                           primary_key=True),
                             CubeDimension(name="event_type", sql="{CUBE}.event_type",
                                           type="string")],
                 measures=[CubeMeasure(name="count", type="count")]),
        CubeCube(name="clicks", extends="base_events", sql_table="public.clicks",
                 measures=[CubeMeasure(name="click_value", type="sum",
                                       sql="{CUBE}.value")]),
    ])
    models, _ = _convert(project)
    clicks = models["clicks"]
    # Inherited dimensions + measure, plus the child's own measure.
    assert clicks.get_column("id") is not None
    assert clicks.get_column("event_type") is not None
    assert clicks.get_measure("count") is not None
    assert clicks.get_measure("click_value") is not None
    assert clicks.sql_table == "public.clicks"  # child source wins


def test_abstract_base_emitted_hidden():
    project = CubeProject(cubes=[
        CubeCube(name="base_events", sql_table="public.events", public=False,
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="clicks", extends="base_events", sql_table="public.clicks"),
    ])
    models, _ = _convert(project)
    assert models["base_events"].hidden is True
    assert models["clicks"].hidden is False


def test_child_overrides_inherited_member():
    project = CubeProject(cubes=[
        CubeCube(name="base", sql_table="public.base", public=False,
                 dimensions=[CubeDimension(name="label", sql="{CUBE}.old_label",
                                           type="string")]),
        CubeCube(name="child", extends="base", sql_table="public.child",
                 dimensions=[CubeDimension(name="label", sql="{CUBE}.new_label",
                                           type="string")]),
    ])
    models, _ = _convert(project)
    # Child wins on name conflict.
    assert models["child"].get_column("label").sql == "new_label"


def test_multi_level_extends_chain():
    project = CubeProject(cubes=[
        CubeCube(name="a", sql_table="public.a", public=False,
                 dimensions=[CubeDimension(name="x", sql="{CUBE}.x", type="number")]),
        CubeCube(name="b", extends="a", sql_table="public.b", public=False,
                 dimensions=[CubeDimension(name="y", sql="{CUBE}.y", type="number")]),
        CubeCube(name="c", extends="b", sql_table="public.c",
                 dimensions=[CubeDimension(name="z", sql="{CUBE}.z", type="number")]),
    ])
    models, _ = _convert(project)
    c = models["c"]
    assert c.get_column("x") is not None  # from a, transitively
    assert c.get_column("y") is not None  # from b
    assert c.get_column("z") is not None  # own


def test_extends_cycle_is_reported():
    project = CubeProject(cubes=[
        CubeCube(name="a", extends="b", sql_table="public.a"),
        CubeCube(name="b", extends="a", sql_table="public.b"),
    ])
    _, report = _convert(project)
    assert any(i.category == CubeIssueCategory.EXTENDS_CYCLE for i in report.issues)
