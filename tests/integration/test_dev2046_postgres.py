"""Boolean aggregation inputs on a real Postgres (no ``sum`` / ``min`` / ``max`` over boolean there).

Spec: aggregations/boolean-inputs.
"""

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2046_fixtures import (
    FLAG_BY_REGION,
    FLAG_TOTALS,
    GT15_TOTALS,
    VIP_ASSOC_SUM_BY_REGION,
    by_dim,
    dev2046_models,
    m,
    orders_q,
    seed_statements,
)

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg_engine(postgresql, tmp_path) -> SlayerQueryEngine:
    cur = postgresql.cursor()
    for stmt in seed_statements("postgres"):
        cur.execute(stmt)
    postgresql.commit()
    storage = YAMLStorage(base_dir=str(tmp_path))
    info = postgresql.info
    await storage.save_datasource(DatasourceConfig(
        name="pg", type="postgres", host=info.host, port=info.port,
        database=info.dbname, username=info.user, password="",
    ))
    for model in dev2046_models(data_source="pg"):
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


def _num(value):
    return None if value is None else float(value)


@pytest.mark.integration
async def test_column_aggregates(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(measures=[
        m("sum(flag)", "s"), m("avg(flag)", "a"), m("min(flag)", "mn"), m("max(flag)", "mx"),
        m("count(flag)", "c"), m("count_distinct(flag)", "cd"),
    ]))
    row = resp.data[0]
    assert row["orders.s"] == FLAG_TOTALS["sum"] and not isinstance(row["orders.s"], bool)
    assert _num(row["orders.a"]) == pytest.approx(FLAG_TOTALS["avg"])
    assert row["orders.mn"] is FLAG_TOTALS["min"]
    assert row["orders.mx"] is FLAG_TOTALS["max"]
    assert row["orders.c"] == FLAG_TOTALS["count"]
    assert row["orders.cd"] == FLAG_TOTALS["count_distinct"]


@pytest.mark.integration
async def test_by_region(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(
        dimensions=["region"], measures=[m(f"{agg}(flag)", agg) for agg in ("sum", "avg", "min", "max")],
    ))
    assert by_dim(resp, "region", "sum") == FLAG_BY_REGION["sum"]
    assert {r: pytest.approx(_num(v)) for r, v in by_dim(resp, "region", "avg").items()} == FLAG_BY_REGION["avg"]
    assert by_dim(resp, "region", "min") == FLAG_BY_REGION["min"]
    assert by_dim(resp, "region", "max") == FLAG_BY_REGION["max"]


@pytest.mark.integration
async def test_comparison_source(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(measures=[
        m("sum(amount > 15)", "s"), m("avg(amount > 15)", "a"), m("count(amount > 15)", "c"),
        m("min(amount > 15)", "mn"), m("max(amount > 15)", "mx"), m("sum(big_order)", "sb"),
    ]))
    row = resp.data[0]
    assert row["orders.s"] == GT15_TOTALS["sum"]
    assert _num(row["orders.a"]) == pytest.approx(GT15_TOTALS["avg"])
    assert row["orders.c"] == GT15_TOTALS["count"]
    assert row["orders.mn"] is False and row["orders.mx"] is True
    assert row["orders.sb"] == GT15_TOTALS["sum"]


@pytest.mark.integration
@pytest.mark.parametrize("having", ["max(flag) = true", "sum(flag) > 1", "sum(amount > 15) > 1", "max(amount > 15) = true"])
async def test_post_aggregation_filters(pg_engine: SlayerQueryEngine, having: str) -> None:
    resp = await pg_engine.execute(orders_q(dimensions=["region"], measures=[m("sum(flag)")], filters=[having]))
    assert by_dim(resp, "region") == {"east": FLAG_BY_REGION["sum"]["east"]}


@pytest.mark.integration
async def test_association_pick(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(
        dimensions=["region"],
        measures=[m("sum(customers.vip)", "s"), m("max(customers.vip)", "mx")],
        to_many_handling="associate",
    ))
    assert by_dim(resp, "region", "s") == VIP_ASSOC_SUM_BY_REGION
    assert by_dim(resp, "region", "mx") == {"east": True, "west": False}
