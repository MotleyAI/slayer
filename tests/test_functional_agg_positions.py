"""Every position that accepts an aggregation takes the functional spelling: query measures, filters, order,
model measures (saved and hand-authored YAML), ModelExtension, inline source_model, source_queries stages,
transforms and arithmetic, and computed dimensions (with their guards)."""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import DistinctDimensionValuesError
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import (
    ColumnRef,
    ModelExtension,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1740_fixtures import gen as gen40
from tests._engine_helpers import make_seeded_sqlite_engine


# ===========================================================================
# Harness: dry-run the functional spelling.
# ===========================================================================


def _orders(**overrides) -> SlayerModel:
    fields = dict(
        name="orders",
        data_source="test",
        sql_table="orders",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="price", type=DataType.DOUBLE),
            Column(name="balance", type=DataType.DOUBLE),
            Column(name="updated_at", type=DataType.TIMESTAMP),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )
    fields.update(overrides)
    return SlayerModel(**fields)


def _customers(**overrides) -> SlayerModel:
    fields = dict(
        name="customers",
        data_source="test",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="score", type=DataType.DOUBLE),
        ],
    )
    fields.update(overrides)
    return SlayerModel(**fields)


async def _dry(
    query,
    *,
    model: SlayerModel | None = None,
    extra_models: list | None = None,
    dialect: str = "postgres",
):
    model = model or _orders()
    extra_models = extra_models if extra_models is not None else [_customers()]
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name=model.data_source, type=dialect)
        )
        await storage.save_model(model)
        for extra in extra_models:
            await storage.save_model(extra)
        engine = SlayerQueryEngine(storage=storage)
        return await engine.execute(query, dry_run=True)


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


# ===========================================================================
# Query measures.
# ===========================================================================


class TestMeasurePosition:
    async def test_simple_measure(self) -> None:
        f = await _dry(_q(measures=["sum(revenue)"], dimensions=["status"]))
        assert "orders.revenue_sum" in f.columns

    async def test_star_count_measure(self) -> None:
        f = await _dry(_q(measures=["count(*)"], dimensions=["status"]))
        assert "orders._count" in f.columns

    async def test_cross_model_measure(self) -> None:
        f = await _dry(_q(measures=["sum(customers.score)"], dimensions=["status"]))
        assert "orders.customers.score_sum" in f.columns

    async def test_cross_model_star_count(self) -> None:
        f = await _dry(_q(measures=["count(customers.*)"], dimensions=["status"]))
        assert "orders.customers._count" in f.columns

    async def test_percentile_measure(self) -> None:
        f = await _dry(_q(measures=["percentile(price, p=0.9)"]))
        assert "orders.price_percentile_p_0_9" in f.columns

    async def test_windowed_measure(self) -> None:
        td = [TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH)]
        await _dry(_q(measures=["sum(revenue, window='90d')"], dimensions=["status"], time_dimensions=td))

    async def test_weighted_avg_measure(self) -> None:
        await _dry(_q(measures=["weighted_avg(revenue, weight=price)"]))

    async def test_ranked_last_measure(self) -> None:
        await _dry(_q(measures=["last(balance, updated_at)"]))


# ===========================================================================
# Filters (HAVING routing).
# ===========================================================================


class TestFilterPosition:
    async def test_having_filter(self) -> None:
        f = await _dry(_q(measures=["sum(revenue)"], dimensions=["status"], filters=["sum(revenue) > 100"]))
        assert "HAVING" in f.sql.upper()

    async def test_arithmetic_filter(self) -> None:
        await _dry(_q(measures=["sum(revenue)"], dimensions=["status"], filters=["sum(revenue) / count(*) > 5"]))

    def test_construction_time_custom_agg_filter(self) -> None:
        # No model context at construction — the unknown functional name defers to binding.
        q = SlayerQuery(
            source_model="orders",
            measures=["my_agg(price)"],
            filters=["my_agg(price) > 0"],
        )
        assert q.filters


# ===========================================================================
# Order.
# ===========================================================================


class TestOrderPosition:
    async def test_order_by_functional_agg(self) -> None:
        f = await _dry(_q(measures=["sum(revenue)"], dimensions=["status"],
                          order=[OrderItem.model_validate({"column": "sum(revenue)", "direction": "desc"})]))
        assert "ORDER BY" in f.sql.upper()

    async def test_order_by_functional_star_count(self) -> None:
        await _dry(_q(measures=["count(*)"], dimensions=["status"],
                      order=[OrderItem.model_validate({"column": "count(*)", "direction": "desc"})]))

    async def test_order_custom_agg_placeholder(self) -> None:
        model = _orders(
            aggregations=[Aggregation(name="my_agg", formula="AVG({value})")]
        )
        await _dry(_q(measures=["my_agg(price)"], dimensions=["status"],
                      order=[OrderItem.model_validate({"column": "my_agg(price)", "direction": "desc"})]), model=model)

    async def test_order_serialization_round_trip(self) -> None:
        q1 = _q(
            measures=["sum(revenue)"],
            dimensions=["status"],
            order=[OrderItem(column="sum(revenue)", direction="desc")],
        )
        q2 = SlayerQuery.model_validate(q1.model_dump())
        # The author's functional spelling survives coercion and round-trip.
        assert q1.order[0].raw_formula == "sum(revenue)"
        assert q2.order[0].raw_formula == "sum(revenue)"
        r1 = await _dry(q1)
        r2 = await _dry(q2)
        assert r1.sql == r2.sql


# ===========================================================================
# Model measures — saved and hand-authored YAML.
# ===========================================================================


class TestModelMeasurePosition:
    async def test_saved_model_measure(self) -> None:
        model = _orders(measures=[ModelMeasure(name="rev", formula="sum(revenue)")])
        f = await _dry(_q(measures=["rev"], dimensions=["status"]), model=model)
        assert "orders.rev" in f.columns

    async def test_hand_authored_yaml_measure(self, tmp_path) -> None:
        # The functional formula is edited into the YAML directly (no save pass).
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
        await storage.save_model(_orders(measures=[ModelMeasure(name="rev", formula="max(revenue)")]))
        await storage.save_model(_customers())
        path = tmp_path / "models" / "test" / "orders.yaml"
        text = path.read_text()
        assert "max(revenue)" in text
        path.write_text(text.replace("max(revenue)", "sum(revenue)"))
        engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path)))
        resp = await engine.execute(_q(measures=["rev"], dimensions=["status"]), dry_run=True)
        assert resp.sql is not None
        assert "SUM(" in resp.sql.upper()
        assert "orders.rev" in resp.columns


# ===========================================================================
# ModelExtension and inline source_model.
# ===========================================================================


class TestExtensionPositions:
    async def test_model_extension_measure(self) -> None:
        ext = ModelExtension(source_name="orders", measures=[ModelMeasure(name="rev", formula="sum(revenue)")])
        f = await _dry(SlayerQuery.model_validate({
            "source_model": "orders",
            "extensions": [ext],
            "measures": ["rev"],
            "dimensions": ["status"],
        }))
        assert "orders.rev" in f.columns

    async def test_inline_source_model_measure(self) -> None:
        inline = ModelExtension.model_validate({
            "source_name": "orders",
            "measures": [{"name": "rev", "formula": "sum(revenue)"}],
        })
        f = await _dry(SlayerQuery.model_validate({
            "source_model": "orders",
            "extensions": [inline],
            "measures": ["rev"],
            "dimensions": ["status"],
        }))
        assert "orders.rev" in f.columns


# ===========================================================================
# Multi-stage source queries.
# ===========================================================================


class TestStagePositions:
    async def test_stage_formula(self) -> None:
        stage1 = SlayerQuery.model_validate({
            "name": "stage1",
            "source_model": "orders",
            "dimensions": ["status"],
            "measures": [{"formula": "sum(revenue)"}],
        })
        root = SlayerQuery.model_validate({
            "source_model": "stage1",
            "dimensions": ["status"],
            "measures": [{"formula": "max(revenue_sum)"}],
        })
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="postgres")
            )
            await storage.save_model(_orders())
            await storage.save_model(_customers())
            engine = SlayerQueryEngine(storage=storage)
            f = await engine.execute([stage1, root], dry_run=True)
        assert f.columns

    async def test_saved_query_backed_model(self) -> None:
        backed = SlayerModel(
            name="daily",
            data_source="test",
            source_queries=[
                SlayerQuery.model_validate({
                    "source_model": "orders",
                    "dimensions": ["status"],
                    "measures": [{"formula": "sum(revenue)", "name": "rev"}],
                })
            ],
        )
        query = SlayerQuery.model_validate({
            "source_model": "daily",
            "dimensions": ["status"],
            "measures": ["max(rev)"],
        })
        f = await _dry(query, model=_orders(), extra_models=[_customers(), backed])
        assert "daily.rev_max" in f.columns


# ===========================================================================
# Transforms and arithmetic containing functional aggregations.
# ===========================================================================


class TestCompositePositions:
    async def test_transform_over_functional_agg(self) -> None:
        td = [TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH)]
        await _dry(_q(measures=["cumsum(sum(revenue))"], time_dimensions=td))

    async def test_arithmetic_of_functional_aggs(self) -> None:
        f = await _dry(_q(measures=[{"formula": "sum(revenue) / count(*)", "name": "aov"}], dimensions=["status"]))
        assert "orders.aov" in f.columns


# ===========================================================================
# Computed dimensions — guards.
# ===========================================================================

_CASE_FUNC = "CASE WHEN sum(amount, partition_by=city) > 5000 THEN 1 ELSE 0 END"


def _band_query(expr: str, **kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    kw.setdefault("dimensions", ["region", {"expression": expr, "name": "band"}])
    kw.setdefault("measures", [ModelMeasure(formula="sum(amount)", name="t")])
    return SlayerQuery(**kw)


class TestComputedDimensionPosition:
    async def test_partitioned_aggregate(self) -> None:
        assert await gen40(_band_query(_CASE_FUNC))

    async def test_bare_aggregate_guard(self) -> None:
        query = _band_query("CASE WHEN sum(amount) > 5000 THEN 1 ELSE 0 END")
        with pytest.raises(ValueError, match="partition_by"):
            await gen40(query)

    async def test_raw_rows_rejection(self) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "orders",
            "dimensions": [{"expression": _CASE_FUNC, "name": "band"}],
            "distinct_dimension_values": False,
        })
        with pytest.raises(DistinctDimensionValuesError):
            await gen40(query)


# ===========================================================================
# Executed on seeded SQLite.
# ===========================================================================


class TestExecuted:
    async def test_functional_query_returns_rows(self, tmp_path) -> None:
        db_path = os.path.join(str(tmp_path), "parity.db")
        with transaction(db_path) as con:
            cur = con.cursor()
            cur.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
                "status TEXT, revenue REAL, price REAL, balance REAL, "
                "updated_at TEXT, created_at TEXT)"
            )
            cur.executemany(
                "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)",
                [
                    (1, 1, "new", 10.0, 2.0, 100.0, "2024-01-05", "2024-01-01"),
                    (2, 1, "old", 5.0, 1.0, 110.0, "2024-02-05", "2024-02-01"),
                    (3, 2, "new", 7.0, 3.0, 120.0, "2024-03-05", "2024-03-01"),
                    (4, 2, "old", 9.0, 4.0, 130.0, "2024-04-05", "2024-04-01"),
                ],
            )
            cur.execute(
                "CREATE TABLE customers (id INTEGER PRIMARY KEY, score REAL)"
            )
            cur.executemany(
                "INSERT INTO customers VALUES (?,?)", [(1, 5.0), (2, 7.0)]
            )
        engine = await make_seeded_sqlite_engine(
            base_dir=os.path.join(str(tmp_path), "store"),
            db_path=db_path,
            models=[_orders(), _customers()],
        )
        f = await engine.execute(
            _q(
                measures=["sum(revenue)", "count(*)"],
                dimensions=[ColumnRef(name="status")],
                filters=["sum(revenue) > 10"],
                order=[OrderItem(column="sum(revenue)", direction="desc")],
            )
        )
        assert list(f.columns) == ["orders.status", "orders.revenue_sum", "orders._count"]
        assert [(r["orders.status"], r["orders.revenue_sum"], r["orders._count"]) for r in f.data] == [
            ("new", 17.0, 2), ("old", 14.0, 2)]
