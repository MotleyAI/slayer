"""Smoke tests: enrich/generate SQL against converted Cube models.

DEV-1608 §12 (Codex test-gap). These don't just assert converter *output* — they
push the converted models through the engine's enrichment + SQL generation, which
is where the §6/§4.4 mapping breaks actually surface (e.g. a facade measure that
referenced a Cube measure *name* instead of the underlying Column would raise
"Column '<name>' not found" at enrichment).
"""


from slayer.core.query import ColumnRef, SlayerQuery
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import (
    CubeCube,
    CubeDimension,
    CubeJoin,
    CubeMeasure,
    CubeMeasureFilter,
    CubeProject,
    CubeView,
    CubeViewCubeRef,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage

DS = "cube_ds"


def _orders_customers_view_project() -> CubeProject:
    return CubeProject(
        cubes=[
            CubeCube(
                name="orders", sql_table="public.orders",
                joins=[CubeJoin(name="customers", relationship="many_to_one",
                                sql="{CUBE}.customer_id = {customers.id}")],
                measures=[
                    CubeMeasure(name="count", type="count"),
                    CubeMeasure(name="total_revenue", type="sum", sql="{CUBE}.amount"),
                    CubeMeasure(name="completed_revenue", type="sum", sql="{CUBE}.amount",
                                filters=[CubeMeasureFilter(sql="{CUBE}.status = 'completed'")]),
                ],
                dimensions=[
                    CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True),
                    CubeDimension(name="status", sql="{CUBE}.status", type="string"),
                    CubeDimension(name="customer_id", sql="{CUBE}.customer_id", type="number"),
                ],
            ),
            CubeCube(
                name="customers", sql_table="public.customers",
                measures=[CubeMeasure(name="lifetime_value", type="sum", sql="{CUBE}.ltv")],
                dimensions=[
                    CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True),
                    CubeDimension(name="name", sql="{CUBE}.name", type="string"),
                ],
            ),
        ],
        views=[CubeView(name="orders_overview", cubes=[
            CubeViewCubeRef(join_path="orders", includes=["count", "total_revenue",
                                                          "completed_revenue", "status"]),
            CubeViewCubeRef(join_path="orders.customers", prefix=True,
                            includes=["name", "lifetime_value"]),
        ])],
    )


async def _save_converted(tmp_path) -> tuple[SlayerQueryEngine, dict]:
    result = CubeToSlayerConverter(
        project=_orders_customers_view_project(), data_source=DS).convert()
    storage = YAMLStorage(base_dir=str(tmp_path))
    for model in result.models:
        await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    return engine, {m.name: m for m in result.models}


async def _gen_sql(engine: SlayerQueryEngine, query: SlayerQuery, model) -> str:
    enriched = await engine._enrich(query=query, model=model)
    return SQLGenerator(dialect="sqlite").generate(enriched=enriched)


async def test_view_cross_model_measure_resolves_to_underlying_column(tmp_path):
    """The facade measure `customers_lifetime_value` must resolve through the
    join to the underlying `ltv` column — this is the Codex #1 regression."""
    engine, models = await _save_converted(tmp_path)
    view = models["orders_overview"]
    query = SlayerQuery(
        source_model="orders_overview",
        dimensions=[ColumnRef(name="status")],
        measures=[{"formula": "customers_lifetime_value"}],
    )
    sql = await _gen_sql(engine, query, view)  # must not raise "Column not found"
    assert "ltv" in sql.lower()
    assert "sum" in sql.lower()


async def test_view_status_dimension_generates(tmp_path):
    engine, models = await _save_converted(tmp_path)
    view = models["orders_overview"]
    query = SlayerQuery(
        source_model="orders_overview",
        dimensions=[ColumnRef(name="status")],
        measures=[{"formula": "total_revenue"}],
    )
    sql = await _gen_sql(engine, query, view)
    assert "status" in sql.lower()


async def test_filtered_and_unfiltered_measures_emit_distinct_sql(tmp_path):
    """Codex #4 at the SQL layer: total_revenue (plain SUM) and
    completed_revenue (SUM over CASE WHEN) must both appear, distinctly."""
    engine, models = await _save_converted(tmp_path)
    orders = models["orders"]
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "total_revenue"}, {"formula": "completed_revenue"}],
    )
    sql = await _gen_sql(engine, query, orders)
    assert sql.lower().count("sum(") >= 2
    assert "case" in sql.lower()  # the filtered one wraps amount in CASE WHEN
