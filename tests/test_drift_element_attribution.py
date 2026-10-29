"""Query-time drift attribution blames only what the failing statement reads; SQLite temporal-over-TEXT is not drift
(spec: models/schema-drift)."""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.join_edges import JoinEdgeRef
from slayer.core.models import Aggregation, Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.engine import query_engine
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import DeleteReason, EditModelDelete, RemoveSpec, WholeModelDelete
from slayer.sql import engine_factory
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

_SCHEMA = """
CREATE TABLE t (id INTEGER PRIMARY KEY, x REAL, s TEXT, ts TEXT, d TEXT);
INSERT INTO t VALUES (1, 0.0, 'a', '2024-01-01 10:00:00', '2024-01-01');
CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT);
CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, customer_id INTEGER);
INSERT INTO customers VALUES (1, 'US');
INSERT INTO orders VALUES (1, 10.0, 1);
"""

# Fails on SQLite for a non-drift reason: ln(0) raises inside the UDF.
_LN_FAILURE = {"source_model": "t", "measures": [{"formula": "sum(ln(x))"}]}


def _t(*, ts: DataType = DataType.TEXT, filters: list[str] | None = None,
       measures: list[ModelMeasure] | None = None) -> SlayerModel:
    return SlayerModel(
        name="t", sql_table="t", data_source="ds",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="x", type=DataType.DOUBLE),
            Column(name="s", type=DataType.TEXT),
            Column(name="ts", type=ts),
            Column(name="y", sql="x * 2", type=DataType.DOUBLE),
        ],
        filters=filters or [],
        measures=measures or [],
    )


def _shop() -> list[SlayerModel]:
    return [
        SlayerModel(name="customers", sql_table="customers", data_source="ds", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT)]),
        SlayerModel(name="orders", sql_table="orders", data_source="ds", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="customer_id", type=DataType.INT),
            Column(name="bad", sql="no_such_fn(id)", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])]),
    ]


class _Env:
    def __init__(self, *, engine: SlayerQueryEngine, db_path: str) -> None:
        self.engine = engine
        self.db_path = db_path

    def live(self, sql: str) -> None:
        with transaction(self.db_path) as conn:
            conn.executescript(sql)


@contextlib.asynccontextmanager
async def _env(tmp: Path, *models: SlayerModel) -> AsyncGenerator[_Env]:
    db_path = str(tmp / "live.db")
    with transaction(db_path) as conn:
        conn.executescript(_SCHEMA)
    storage = YAMLStorage(base_dir=str(tmp / "storage"))
    await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=db_path))
    for m in models:
        await storage.save_model(m)
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield _Env(engine=engine, db_path=db_path)
    finally:
        await engine.aclose()


async def _unwrapped(engine: SlayerQueryEngine, query: Any) -> SQLAlchemyError:
    with pytest.raises(SQLAlchemyError) as exc:
        await engine.execute(query)
    assert not isinstance(exc.value, SchemaDriftError), exc.value
    return exc.value


async def _wrapped(engine: SlayerQueryEngine, query: Any) -> SchemaDriftError:
    with pytest.raises(SchemaDriftError) as exc:
        await engine.execute(query)
    return exc.value


def _removes(err: SchemaDriftError, *, model: str, column: str) -> bool:
    return any(isinstance(e, EditModelDelete) and e.model_name == model and column in e.remove.columns
               for e in err.to_delete)


class TestUnreadDriftLeavesTheError:
    async def test_unrelated_drifted_column(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t(ts=DataType.INT)) as env:
            err = await _unwrapped(env.engine, _LN_FAILURE)
        assert "user-defined function" in str(err).lower(), err

    async def test_unread_dropped_column_and_its_measure(self, tmp_path: Path) -> None:
        model = _t(measures=[ModelMeasure(name="n_s", formula="count_distinct(s)")])
        async with _env(tmp_path, model) as env:
            env.live("ALTER TABLE t DROP COLUMN s")
            await _unwrapped(env.engine, _LN_FAILURE)

    async def test_an_uncrossed_join_losing_its_key(self, tmp_path: Path) -> None:
        async with _env(tmp_path, *_shop()) as env:
            env.live("ALTER TABLE orders DROP COLUMN customer_id")
            err = await _unwrapped(env.engine, {"source_model": "orders", "measures": [{"formula": "sum(bad)"}]})
        assert "no_such_fn" in str(err).lower(), err


class TestReadDriftWraps:
    async def test_a_read_dropped_column(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t()) as env:
            env.live("ALTER TABLE t DROP COLUMN x")
            err = await _wrapped(env.engine, {"source_model": "t", "measures": [{"formula": "sum(x)"}]})
        assert _removes(err, model="t", column="x"), err.to_delete

    async def test_a_read_dropped_column_beside_unrelated_drift(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t(ts=DataType.INT)) as env:
            env.live("ALTER TABLE t DROP COLUMN x")
            err = await _wrapped(env.engine, {"source_model": "t", "measures": [{"formula": "sum(x)"}]})
        assert _removes(err, model="t", column="x"), err.to_delete

    async def test_a_column_read_through_a_derived_column(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t()) as env:
            env.live("ALTER TABLE t DROP COLUMN x")
            await _wrapped(env.engine, {"source_model": "t", "measures": [{"formula": "sum(y)"}]})

    async def test_a_join_key(self, tmp_path: Path) -> None:
        async with _env(tmp_path, *_shop()) as env:
            env.live("ALTER TABLE orders DROP COLUMN customer_id")
            await _wrapped(env.engine, {"source_model": "orders", "dimensions": ["customers.region"],
                                        "measures": [{"formula": "sum(amount)"}]})

    async def test_a_used_model_measure(self, tmp_path: Path) -> None:
        model = _t(measures=[ModelMeasure(name="total", formula="sum(x)")])
        async with _env(tmp_path, model) as env:
            env.live("ALTER TABLE t DROP COLUMN x")
            await _wrapped(env.engine, {"source_model": "t", "measures": ["total"]})

    async def test_a_model_filter_always_counts_as_read(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t(filters=["s <> 'zzz'"])) as env:
            env.live("ALTER TABLE t DROP COLUMN s")
            await _wrapped(env.engine, {"source_model": "t", "measures": [{"formula": "sum(x)"}]})


_ORDERS_BAD = {"source_model": "orders", "measures": [{"formula": "sum(bad)"}]}


def _instrumented_orders() -> SlayerModel:
    orders = _shop()[1]
    return orders.model_copy(update={
        "measures": [ModelMeasure(name="m_bad", formula="sum(bad)")],
        "aggregations": [Aggregation(name="wbad", formula="SUM({value})")],
    })


def _inject(remove: RemoveSpec) -> Any:
    """Append an ``orders`` removal to the read-set drift entries the query-time path validates."""
    real = query_engine.validate_datasource

    async def _with_entry(**kwargs: Any) -> list[Any]:
        entries = await real(**kwargs)
        if any(m.name == "orders" for m in kwargs["models"]):
            entries = [*entries, EditModelDelete(model_name="orders", data_source="ds", remove=remove,
                                                 reasons=[DeleteReason(target="injected", reason="injected")])]
        return entries

    return patch.object(query_engine, "validate_datasource", side_effect=_with_entry)


def _join_removal() -> RemoveSpec:
    join = _shop()[1].joins[0]
    return RemoveSpec(join_edges=[JoinEdgeRef.of(join)])


class TestInjectedRemovals:
    async def test_a_crossed_join(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(_join_removal()):
                await _wrapped(env.engine, {**_ORDERS_BAD, "dimensions": ["customers.region"]})

    async def test_an_uncrossed_join(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(_join_removal()):
                await _unwrapped(env.engine, _ORDERS_BAD)

    async def test_a_measure_used_by_name(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(RemoveSpec(measures=["m_bad"])):
                await _wrapped(env.engine, {"source_model": "orders", "measures": ["m_bad"]})

    async def test_an_unused_measure(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(RemoveSpec(measures=["m_bad"])):
                await _unwrapped(env.engine, _ORDERS_BAD)

    async def test_a_used_aggregation(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(RemoveSpec(aggregations=["wbad"])):
                await _wrapped(env.engine, {"source_model": "orders", "measures": [{"formula": "wbad(bad)"}]})

    async def test_an_unused_aggregation(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(RemoveSpec(aggregations=["wbad"])):
                await _unwrapped(env.engine, _ORDERS_BAD)


class TestInvalidSqlModel:
    async def test_its_own_invalid_sql_drop_is_no_evidence(self, tmp_path: Path) -> None:
        broken = SlayerModel(name="sq_broken", data_source="ds",
                             sql="SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM orders", columns=[
                                 Column(name="id", type=DataType.TEXT, primary_key=True),
                                 Column(name="amount", type=DataType.DOUBLE)])
        async with _env(tmp_path, *_shop(), broken) as env:
            entries = [e for e in await env.engine.validate_models(data_source="ds") if e.model_name == "sq_broken"]
            err = await _unwrapped(env.engine, {"source_model": "sq_broken", "measures": [{"formula": "count(*)"}]})
        assert [(type(e), e.cause) for e in entries] == [(WholeModelDelete, "invalid_sql")], entries
        assert "no_such_function" in str(err).lower(), err


def _dated(*, ts: DataType, d: DataType) -> SlayerModel:
    return SlayerModel(name="t", sql_table="t", data_source="ds", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="x", type=DataType.DOUBLE),
        Column(name="ts", type=ts),
        Column(name="d", type=d)])


class TestSqliteTemporalOverText:
    async def test_validation_reports_nothing(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _dated(ts=DataType.TIMESTAMP, d=DataType.DATE)) as env:
            entries = await env.engine.validate_models(data_source="ds")
        assert entries == [], entries

    async def test_query_time_attribution_does_not_blame_it(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _dated(ts=DataType.TIMESTAMP, d=DataType.DATE)) as env:
            await _unwrapped(env.engine, _LN_FAILURE)

    async def test_a_non_temporal_declaration_over_text_is_still_drift(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _dated(ts=DataType.INT, d=DataType.DATE)) as env:
            entries = await env.engine.validate_models(data_source="ds")
        assert [e.remove.columns for e in entries if isinstance(e, EditModelDelete)] == [["ts"]], entries


class TestOtherDialectsKeepTheBucketRule:
    async def test_duckdb_timestamp_over_varchar_is_drift(self) -> None:
        duckdb = pytest.importorskip("duckdb")
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "live.duckdb")
            with contextlib.closing(duckdb.connect(db_path)) as con:
                con.execute("CREATE TABLE t (id INTEGER, x DOUBLE, ts VARCHAR, d VARCHAR)")
            ds = DatasourceConfig(name="ds", type="duckdb", database=db_path)
            storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
            await storage.save_datasource(ds)
            await storage.save_model(_dated(ts=DataType.TIMESTAMP, d=DataType.TEXT))
            engine = SlayerQueryEngine(storage=storage)
            try:
                entries = await engine.validate_models(data_source="ds")
            finally:
                engine.close()
                engine_factory.invalidate_engine(ds)
        assert [e.remove.columns for e in entries if isinstance(e, EditModelDelete)] == [["ts"]], entries
