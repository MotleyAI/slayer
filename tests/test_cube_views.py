"""Tests for Cube views → SLayer facade models (slayer/cube/converter.py).

DEV-1608 §6 + Codex #1/#3 corrections: facade measures reference the underlying
Column (not the Cube measure name); facade source mirrors the root cube's mode.
"""

from slayer.core.models import SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import (
    CubeCube,
    CubeDimension,
    CubeJoin,
    CubeMeasure,
    CubeProject,
    CubeView,
    CubeViewCubeRef,
)
from slayer.cube.report import CubeIssueCategory

DS = "test_ds"


def _orders_customers_cubes(*, orders_sql_mode: bool = False) -> list[CubeCube]:
    orders_kwargs = (
        {"sql": "SELECT * FROM public.orders"} if orders_sql_mode
        else {"sql_table": "public.orders"}
    )
    return [
        CubeCube(
            name="orders", **orders_kwargs,
            joins=[CubeJoin(name="customers", relationship="many_to_one",
                            sql="{CUBE}.customer_id = {customers.id}")],
            measures=[CubeMeasure(name="count", type="count"),
                      CubeMeasure(name="total_revenue", type="sum", sql="{CUBE}.amount")],
            dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                      primary_key=True),
                        CubeDimension(name="status", sql="{CUBE}.status", type="string")],
        ),
        CubeCube(
            name="customers", sql_table="public.customers",
            measures=[CubeMeasure(name="lifetime_value", type="sum", sql="{CUBE}.ltv")],
            dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                      primary_key=True),
                        CubeDimension(name="name", sql="{CUBE}.name", type="string"),
                        CubeDimension(name="region", sql="{CUBE}.region", type="string")],
        ),
    ]


def _view() -> CubeView:
    return CubeView(name="orders_overview", cubes=[
        CubeViewCubeRef(join_path="orders",
                        includes=["count", "total_revenue", "status"]),
        CubeViewCubeRef(join_path="orders.customers", prefix=True,
                        includes=["name", "region", "lifetime_value"]),
    ])


def _convert(project: CubeProject) -> tuple[dict[str, SlayerModel], object]:
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    return {m.name: m for m in result.models}, result.report


def test_view_facade_model_basic_shape():
    project = CubeProject(cubes=_orders_customers_cubes(), views=[_view()])
    models, _ = _convert(project)
    view = models["orders_overview"]
    assert view.sql_table == "public.orders"        # rooted on orders
    assert view.meta["cube_kind"] == "view"
    # join to customers present on the facade
    assert any(j.target_model == "customers" for j in view.joins)


def test_view_root_dimension_is_local_derived_column():
    project = CubeProject(cubes=_orders_customers_cubes(), views=[_view()])
    models, _ = _convert(project)
    view = models["orders_overview"]
    assert view.get_column("status") is not None


def test_view_prefixed_joined_dimension_references_joined_column():
    project = CubeProject(cubes=_orders_customers_cubes(), views=[_view()])
    models, _ = _convert(project)
    view = models["orders_overview"]
    # prefix: true → "<cube>_<member>" (Cube prepends the cube name verbatim).
    col = view.get_column("customers_name")
    assert col is not None
    assert col.sql == "customers.name"


def test_view_root_measure_carries_underlying_column():
    """Codex #1: a root-cube measure re-export needs the underlying column on
    the facade, referenced by `<col>:<agg>` (NOT the measure name)."""
    project = CubeProject(cubes=_orders_customers_cubes(), views=[_view()])
    models, _ = _convert(project)
    view = models["orders_overview"]
    m = view.get_measure("total_revenue")
    assert m is not None
    col_ref = m.formula.split(":")[0].strip()
    assert view.get_column(col_ref) is not None  # underlying column copied onto facade
    assert m.formula.endswith(":sum")


def test_view_joined_measure_is_cross_model_underlying_column_ref():
    """Codex #1: joined-cube measure → `customers.<underlying_col>:<agg>`
    (the underlying column `ltv`, never the measure name `lifetime_value`)."""
    project = CubeProject(cubes=_orders_customers_cubes(), views=[_view()])
    models, _ = _convert(project)
    view = models["orders_overview"]
    m = view.get_measure("customers_lifetime_value")  # prefix: "<cube>_<member>"
    assert m is not None
    assert m.formula == "customers.ltv:sum"


def test_view_count_measure_maps_to_star_count():
    project = CubeProject(cubes=_orders_customers_cubes(), views=[_view()])
    models, _ = _convert(project)
    assert models["orders_overview"].get_measure("count").formula == "*:count"


def test_view_default_filters_become_model_filters():
    view = _view()
    view.default_filters = [{"member": "orders.status", "operator": "equals",
                             "values": ["completed"]}]
    project = CubeProject(cubes=_orders_customers_cubes(), views=[view])
    models, _ = _convert(project)
    filters = " ".join(models["orders_overview"].filters)
    assert "completed" in filters


def test_view_on_sql_backed_root_mirrors_sql_source():
    """Codex #3: facade mirrors the root cube's source mode, not always sql_table."""
    project = CubeProject(cubes=_orders_customers_cubes(orders_sql_mode=True),
                          views=[_view()])
    models, _ = _convert(project)
    view = models["orders_overview"]
    assert view.sql_table is None
    assert view.sql == "SELECT * FROM public.orders"


def test_view_folders_parked_in_meta_and_reported():
    view = _view()
    view.folders = [{"name": "Revenue", "includes": ["total_revenue"]}]
    project = CubeProject(cubes=_orders_customers_cubes(), views=[view])
    models, report = _convert(project)
    assert models["orders_overview"].meta["cube_unmapped"]["folders"]
    assert any(i.category == CubeIssueCategory.FOLDERS_UNMAPPED for i in report.issues)


def test_view_excludes_drops_member():
    view = CubeView(name="ov", cubes=[
        CubeViewCubeRef(join_path="orders", includes="*", excludes=["status"]),
    ])
    project = CubeProject(cubes=_orders_customers_cubes(), views=[view])
    models, _ = _convert(project)
    assert models["ov"].get_column("status") is None
    assert models["ov"].get_measure("total_revenue") is not None


def test_view_includes_star_takes_all_members():
    view = CubeView(name="ov", cubes=[
        CubeViewCubeRef(join_path="orders", includes="*"),
    ])
    project = CubeProject(cubes=_orders_customers_cubes(), views=[view])
    models, _ = _convert(project)
    ov = models["ov"]
    assert ov.get_column("status") is not None
    assert ov.get_measure("total_revenue") is not None
    assert ov.get_measure("count") is not None


def test_view_disconnected_members_reported():
    cubes = _orders_customers_cubes()
    cubes.append(CubeCube(name="weather", sql_table="public.weather",
                          dimensions=[CubeDimension(name="temp", sql="{CUBE}.temp",
                                                    type="number")]))
    view = CubeView(name="ov", cubes=[
        CubeViewCubeRef(join_path="orders", includes=["status"]),
        CubeViewCubeRef(join_path="weather", includes=["temp"]),  # not joined to orders
    ])
    project = CubeProject(cubes=cubes, views=[view])
    _models, report = _convert(project)
    assert any(i.category == CubeIssueCategory.DISCONNECTED_VIEW for i in report.issues)


def test_view_fanout_risk_reported():
    cubes = [
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="line_items", relationship="one_to_many",
                                 sql="{CUBE}.id = {line_items.order_id}")],
                 measures=[CubeMeasure(name="total_revenue", type="sum", sql="{CUBE}.amount")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                           primary_key=True)]),
        CubeCube(name="line_items", sql_table="public.line_items",
                 dimensions=[CubeDimension(name="order_id", sql="{CUBE}.order_id",
                                           type="number"),
                             CubeDimension(name="sku", sql="{CUBE}.sku", type="string")]),
    ]
    view = CubeView(name="ov", cubes=[
        CubeViewCubeRef(join_path="orders", includes=["total_revenue"]),
        CubeViewCubeRef(join_path="orders.line_items", includes=["sku"]),
    ])
    project = CubeProject(cubes=cubes, views=[view])
    _models, report = _convert(project)
    assert any(i.category == CubeIssueCategory.VIEW_FANOUT_RISK for i in report.issues)


def test_view_extends_flattens_member_lists():
    base = CubeView(name="base_view", cubes=[
        CubeViewCubeRef(join_path="orders", includes=["status"])])
    child = CubeView(name="child_view", extends="base_view", cubes=[
        CubeViewCubeRef(join_path="orders", includes=["total_revenue"])])
    project = CubeProject(cubes=_orders_customers_cubes(), views=[base, child])
    models, _ = _convert(project)
    child_model = models["child_view"]
    assert child_model.get_column("status") is not None        # inherited
    assert child_model.get_measure("total_revenue") is not None  # own


def test_view_dropped_when_root_cube_not_emitted():
    # orders has no source → not emitted; a view rooted on it can't be built.
    cubes = [
        CubeCube(name="orders"),  # no source
        CubeCube(name="customers", sql_table="public.customers",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
    ]
    view = CubeView(name="ov", cubes=[
        CubeViewCubeRef(join_path="orders", includes=["id"])])
    project = CubeProject(cubes=cubes, views=[view])
    models, report = _convert(project)
    assert "ov" not in models
    assert any(i.category in (CubeIssueCategory.AMBIGUOUS_VIEW_ROOT,
                              CubeIssueCategory.DISCONNECTED_VIEW)
               for i in report.issues)
