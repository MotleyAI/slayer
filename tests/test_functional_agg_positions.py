"""Position parity for functional aggregations (DEV-1826 task 1.2).

For every position that accepts colon aggregations, the functional spelling
must produce byte-identical SQL and identical result-column keys: query
measures, filters, order, model measures (saved and hand-authored YAML),
ModelExtension, inline source_model, source_queries stages, transforms and
arithmetic, computed dimensions (with identical guards), and mixed-grain
arithmetic.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType, TimeGranularity
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
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1740_fixtures import gen as gen40
from tests._dev1839_fixtures import MEASURE_DIFF, MIXED_RANK, gen as gen39, q as q39
from tests._engine_helpers import make_seeded_sqlite_engine


# ===========================================================================
# Harness: dry-run both spellings, assert identical SQL + result keys.
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


async def _assert_twins(
    q_func,
    q_colon,
    *,
    model: SlayerModel | None = None,
    extra_models: list | None = None,
    colon_model: SlayerModel | None = None,
):
    f = await _dry(q_func, model=model, extra_models=extra_models)
    c = await _dry(q_colon, model=colon_model or model, extra_models=extra_models)
    assert f.sql == c.sql
    assert list(f.columns) == list(c.columns)
    return f


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


# ===========================================================================
# Query measures.
# ===========================================================================


class TestMeasurePosition:
    async def test_simple_measure(self) -> None:
        f = await _assert_twins(
            _q(measures=["sum(revenue)"], dimensions=["status"]),
            _q(measures=["revenue:sum"], dimensions=["status"]),
        )
        assert "orders.revenue_sum" in f.columns

    async def test_star_count_measure(self) -> None:
        f = await _assert_twins(
            _q(measures=["count(*)"], dimensions=["status"]),
            _q(measures=["*:count"], dimensions=["status"]),
        )
        assert "orders._count" in f.columns

    async def test_cross_model_measure(self) -> None:
        await _assert_twins(
            _q(measures=["sum(customers.score)"], dimensions=["status"]),
            _q(measures=["customers.score:sum"], dimensions=["status"]),
        )

    async def test_cross_model_star_count(self) -> None:
        f = await _assert_twins(
            _q(measures=["count(customers.*)"], dimensions=["status"]),
            _q(measures=["customers.*:count"], dimensions=["status"]),
        )
        assert "orders.customers._count" in f.columns

    async def test_percentile_measure(self) -> None:
        f = await _assert_twins(
            _q(measures=["percentile(price, p=0.9)"]),
            _q(measures=["price:percentile(p=0.9)"]),
        )
        assert "orders.price_percentile_p_0_9" in f.columns

    async def test_windowed_measure(self) -> None:
        td = [TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH)]
        await _assert_twins(
            _q(
                measures=["sum(revenue, window='90d')"],
                dimensions=["status"],
                time_dimensions=td,
            ),
            _q(
                measures=["revenue:sum(window='90d')"],
                dimensions=["status"],
                time_dimensions=td,
            ),
        )

    async def test_weighted_avg_measure(self) -> None:
        await _assert_twins(
            _q(measures=["weighted_avg(revenue, weight=price)"]),
            _q(measures=["revenue:weighted_avg(weight=price)"]),
        )

    async def test_ranked_last_measure(self) -> None:
        await _assert_twins(
            _q(measures=["last(balance, updated_at)"]),
            _q(measures=["balance:last(updated_at)"]),
        )


# ===========================================================================
# Filters (HAVING routing) and cross-spelling measure matching.
# ===========================================================================


class TestFilterPosition:
    async def test_having_filter(self) -> None:
        f = await _assert_twins(
            _q(
                measures=["sum(revenue)"],
                dimensions=["status"],
                filters=["sum(revenue) > 100"],
            ),
            _q(
                measures=["revenue:sum"],
                dimensions=["status"],
                filters=["revenue:sum > 100"],
            ),
        )
        assert "HAVING" in f.sql.upper()

    async def test_arithmetic_filter(self) -> None:
        await _assert_twins(
            _q(
                measures=["sum(revenue)"],
                dimensions=["status"],
                filters=["sum(revenue) / count(*) > 5"],
            ),
            _q(
                measures=["revenue:sum"],
                dimensions=["status"],
                filters=["revenue:sum / *:count > 5"],
            ),
        )

    async def test_functional_measure_colon_filter_match(self) -> None:
        await _assert_twins(
            _q(
                measures=[{"formula": "sum(revenue)", "name": "rev"}],
                dimensions=["status"],
                filters=["revenue:sum > 100"],
            ),
            _q(
                measures=[{"formula": "revenue:sum", "name": "rev"}],
                dimensions=["status"],
                filters=["revenue:sum > 100"],
            ),
        )

    async def test_colon_measure_functional_filter_match(self) -> None:
        await _assert_twins(
            _q(
                measures=[{"formula": "revenue:sum", "name": "rev"}],
                dimensions=["status"],
                filters=["sum(revenue) > 100"],
            ),
            _q(
                measures=[{"formula": "revenue:sum", "name": "rev"}],
                dimensions=["status"],
                filters=["revenue:sum > 100"],
            ),
        )

    def test_construction_time_custom_agg_filter(self) -> None:
        # No model context at construction — the unknown functional name must
        # defer to binding, exactly like the colon spelling.
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
        f = await _assert_twins(
            _q(
                measures=["sum(revenue)"],
                dimensions=["status"],
                order=[OrderItem(column="sum(revenue)", direction="desc")],
            ),
            _q(
                measures=["revenue:sum"],
                dimensions=["status"],
                order=[OrderItem(column="revenue:sum", direction="desc")],
            ),
        )
        assert "ORDER BY" in f.sql.upper()

    async def test_order_by_functional_star_count(self) -> None:
        await _assert_twins(
            _q(
                measures=["count(*)"],
                dimensions=["status"],
                order=[OrderItem(column="count(*)", direction="desc")],
            ),
            _q(
                measures=["*:count"],
                dimensions=["status"],
                order=[OrderItem(column="*:count", direction="desc")],
            ),
        )

    async def test_order_custom_agg_placeholder(self) -> None:
        model = _orders(
            aggregations=[Aggregation(name="my_agg", formula="AVG({value})")]
        )
        await _assert_twins(
            _q(
                measures=["my_agg(price)"],
                dimensions=["status"],
                order=[OrderItem(column="my_agg(price)", direction="desc")],
            ),
            _q(
                measures=["price:my_agg"],
                dimensions=["status"],
                order=[OrderItem(column="price:my_agg", direction="desc")],
            ),
            model=model,
        )

    async def test_order_serialization_round_trip(self) -> None:
        q1 = _q(
            measures=["sum(revenue)"],
            dimensions=["status"],
            order=[OrderItem(column="sum(revenue)", direction="desc")],
        )
        q2 = SlayerQuery.model_validate(q1.model_dump())
        # The author's functional spelling survives coercion and round-trip —
        # never rewritten to colon form.
        assert q1.order[0].raw_formula == "sum(revenue)"
        assert q2.order[0].raw_formula == "sum(revenue)"
        r1 = await _dry(q1)
        r2 = await _dry(q2)
        assert r1.sql == r2.sql


# ===========================================================================
# Every builtin aggregation — SQL-level twin parity (or identical errors).
# ===========================================================================

_REQUIRED_ARGS = {
    "weighted_avg": "weight=price",
    "corr": "other=price",
    "covar_samp": "other=price",
    "covar_pop": "other=price",
    "percentile": "p=0.5",
}


async def _sql_or_error(query) -> tuple:
    try:
        resp = await _dry(query)
        return ("ok", resp.sql, list(resp.columns))
    except Exception as exc:  # noqa: BLE001 — parity needs the error identity
        return ("err", type(exc).__name__, str(exc))


class TestEveryBuiltinSqlParity:
    @pytest.mark.parametrize("agg", sorted(BUILTIN_AGGREGATIONS))
    async def test_builtin_twin(self, agg: str) -> None:
        args = _REQUIRED_ARGS.get(agg)
        func = f"{agg}(revenue, {args})" if args else f"{agg}(revenue)"
        colon = f"revenue:{agg}({args})" if args else f"revenue:{agg}"
        f = await _sql_or_error(_q(measures=[func], dimensions=["status"]))
        c = await _sql_or_error(_q(measures=[colon], dimensions=["status"]))
        assert f == c


# ===========================================================================
# Model measures — saved and hand-authored YAML.
# ===========================================================================


class TestModelMeasurePosition:
    async def test_saved_model_measure(self) -> None:
        model_f = _orders(measures=[ModelMeasure(name="rev", formula="sum(revenue)")])
        model_c = _orders(measures=[ModelMeasure(name="rev", formula="revenue:sum")])
        await _assert_twins(
            _q(measures=["rev"], dimensions=["status"]),
            _q(measures=["rev"], dimensions=["status"]),
            model=model_f,
            colon_model=model_c,
        )

    async def test_hand_authored_yaml_measure(self, tmp_path) -> None:
        # The functional formula is edited into the YAML directly (no save
        # pass) — the pure load-and-query path must accept it.
        responses = {}
        for label in ("colon", "func"):
            base = tmp_path / label
            storage = YAMLStorage(base_dir=str(base))
            await storage.save_datasource(
                DatasourceConfig(name="test", type="postgres")
            )
            await storage.save_model(
                _orders(measures=[ModelMeasure(name="rev", formula="revenue:sum")])
            )
            await storage.save_model(_customers())
            if label == "func":
                path = base / "models" / "test" / "orders.yaml"
                text = path.read_text()
                assert "revenue:sum" in text
                path.write_text(text.replace("revenue:sum", "sum(revenue)"))
            engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(base)))
            responses[label] = await engine.execute(
                _q(measures=["rev"], dimensions=["status"]), dry_run=True
            )
        assert responses["func"].sql == responses["colon"].sql
        assert list(responses["func"].columns) == list(responses["colon"].columns)


# ===========================================================================
# ModelExtension and inline source_model.
# ===========================================================================


class TestExtensionPositions:
    async def test_model_extension_measure(self) -> None:
        def ext(formula: str) -> ModelExtension:
            return ModelExtension(
                source_name="orders",
                measures=[ModelMeasure(name="rev", formula=formula)],
            )

        await _assert_twins(
            SlayerQuery(
                source_model=ext("sum(revenue)"),
                measures=["rev"],
                dimensions=["status"],
            ),
            SlayerQuery(
                source_model=ext("revenue:sum"),
                measures=["rev"],
                dimensions=["status"],
            ),
        )

    async def test_inline_source_model_measure(self) -> None:
        def inline(formula: str) -> dict:
            return {
                "source_name": "orders",
                "measures": [{"name": "rev", "formula": formula}],
            }

        await _assert_twins(
            SlayerQuery(
                source_model=inline("sum(revenue)"),
                measures=["rev"],
                dimensions=["status"],
            ),
            SlayerQuery(
                source_model=inline("revenue:sum"),
                measures=["rev"],
                dimensions=["status"],
            ),
        )


# ===========================================================================
# Multi-stage source queries.
# ===========================================================================


class TestStagePositions:
    async def test_stage_formula_twin(self) -> None:
        def stages(formula: str) -> list[SlayerQuery]:
            stage1 = SlayerQuery(
                name="stage1",
                source_model="orders",
                dimensions=["status"],
                measures=[{"formula": formula}],
            )
            root = SlayerQuery(
                source_model="stage1",
                dimensions=["status"],
                measures=[{"formula": "revenue_sum:max"}],
            )
            return [stage1, root]

        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="postgres")
            )
            await storage.save_model(_orders())
            await storage.save_model(_customers())
            engine = SlayerQueryEngine(storage=storage)
            f = await engine.execute(stages("sum(revenue)"), dry_run=True)
            c = await engine.execute(stages("revenue:sum"), dry_run=True)
        assert f.sql == c.sql
        assert list(f.columns) == list(c.columns)

    async def test_saved_query_backed_model_twin(self) -> None:
        def backed(formula: str) -> SlayerModel:
            return SlayerModel(
                name="daily",
                data_source="test",
                source_queries=[
                    SlayerQuery(
                        source_model="orders",
                        dimensions=["status"],
                        measures=[{"formula": formula, "name": "rev"}],
                    )
                ],
            )

        query = SlayerQuery(
            source_model="daily", dimensions=["status"], measures=["rev:max"]
        )
        f = await _dry(
            query, model=_orders(), extra_models=[_customers(), backed("sum(revenue)")]
        )
        c = await _dry(
            query, model=_orders(), extra_models=[_customers(), backed("revenue:sum")]
        )
        assert f.sql == c.sql
        assert list(f.columns) == list(c.columns)


# ===========================================================================
# Transforms and arithmetic containing functional aggregations.
# ===========================================================================


class TestCompositePositions:
    async def test_transform_over_functional_agg(self) -> None:
        td = [TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH)]
        await _assert_twins(
            _q(measures=["cumsum(sum(revenue))"], time_dimensions=td),
            _q(measures=["cumsum(revenue:sum)"], time_dimensions=td),
        )

    async def test_arithmetic_of_functional_aggs(self) -> None:
        await _assert_twins(
            _q(
                measures=[{"formula": "sum(revenue) / count(*)", "name": "aov"}],
                dimensions=["status"],
            ),
            _q(
                measures=[{"formula": "revenue:sum / *:count", "name": "aov"}],
                dimensions=["status"],
            ),
        )


# ===========================================================================
# Computed dimensions (DEV-1740) — same SQL, same guards.
# ===========================================================================

_CASE_FUNC = "CASE WHEN sum(amount, partition_by=city) > 5000 THEN 1 ELSE 0 END"
_CASE_COLON = "CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END"


def _band_query(expr: str, **kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    kw.setdefault("dimensions", ["region", {"expression": expr, "name": "band"}])
    kw.setdefault("measures", [ModelMeasure(formula="amount:sum", name="t")])
    return SlayerQuery(**kw)


class TestComputedDimensionPosition:
    async def test_partitioned_aggregate_twin(self) -> None:
        assert await gen40(_band_query(_CASE_FUNC)) == await gen40(
            _band_query(_CASE_COLON)
        )

    async def test_bare_aggregate_guard_parity(self) -> None:
        q_func = _band_query("CASE WHEN sum(amount) > 5000 THEN 1 ELSE 0 END")
        with pytest.raises(ValueError, match="partition_by") as e_func:
            await gen40(q_func)
        q_colon = _band_query("CASE WHEN amount:sum > 5000 THEN 1 ELSE 0 END")
        with pytest.raises(ValueError, match="partition_by") as e_colon:
            await gen40(q_colon)
        assert str(e_func.value) == str(e_colon.value)

    async def test_raw_rows_rejection_parity(self) -> None:
        def mk(expr: str) -> SlayerQuery:
            return SlayerQuery(
                source_model="orders",
                dimensions=[{"expression": expr, "name": "band"}],
                distinct_dimension_values=False,
            )

        q_func = mk(_CASE_FUNC)
        with pytest.raises(DistinctDimensionValuesError) as e_func:
            await gen40(q_func)
        q_colon = mk(_CASE_COLON)
        with pytest.raises(DistinctDimensionValuesError) as e_colon:
            await gen40(q_colon)
        assert str(e_func.value) == str(e_colon.value)


# ===========================================================================
# Mixed-grain arithmetic (DEV-1839) — functional twins.
# ===========================================================================

_MEASURE_DIFF_FUNC = (
    "sum(amount, partition_by=region) - sum(amount, partition_by=city)"
)
_MIXED_RANK_FUNC = (
    "rank(sum(amount, partition_by=region) - sum(amount, partition_by=city))"
)


class TestMixedGrainPosition:
    async def test_mixed_grain_measure_twin(self) -> None:
        def mk(expr: str) -> SlayerQuery:
            return q39(
                dimensions=["region", "city", "channel"],
                measures=[ModelMeasure(formula=expr, name="diff")],
            )

        assert await gen39(mk(_MEASURE_DIFF_FUNC)) == await gen39(mk(MEASURE_DIFF))

    async def test_mixed_grain_rank_dimension_twin(self) -> None:
        def mk(expr: str) -> SlayerQuery:
            return q39(
                dimensions=["region", "city", {"expression": expr, "name": "rr"}],
                measures=[ModelMeasure(formula="amount:sum", name="t")],
            )

        assert await gen39(mk(_MIXED_RANK_FUNC)) == await gen39(mk(MIXED_RANK))


# ===========================================================================
# Executed parity on seeded SQLite — same rows, not just same SQL.
# ===========================================================================


class TestExecutedParity:
    async def test_functional_query_returns_identical_rows(self, tmp_path) -> None:
        db_path = os.path.join(str(tmp_path), "parity.db")
        con = sqlite3.connect(db_path)
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
        con.commit()
        con.close()
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
        c = await engine.execute(
            _q(
                measures=["revenue:sum", "*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=["revenue:sum > 10"],
                order=[OrderItem(column="revenue:sum", direction="desc")],
            )
        )
        assert list(f.columns) == list(c.columns)
        assert f.data == c.data
        assert f.data, "parity fixture returned no rows"
