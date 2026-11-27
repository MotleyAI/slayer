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
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.relationship_edges import RelationshipEdgeRef
from slayer.core.models import Aggregation, DatasetField, DatasourceConfig, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.live_schema import schema_drift
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import DeleteReason, EditDatasetDelete, RemoveSpec, RetypeFinding, WholeDatasetDelete
from slayer.database import engine_factory
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

# Fails on SQLite for a non-drift reason: the derived field calls a function SQLite lacks.
_DB_FAILURE = {"population": "t", "metrics": [{"metric": "sum(t.bad)"}]}


def _t(*, ts: DataType = DataType.STRING, filters: list[str] | None = None,
       metrics: list[DatasetMetric] | None = None) -> SlayerDataset:
    return SlayerDataset(
        name="t", sql_table="t", data_source="ds",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="x", datatype=DataType.FLOAT),
            DatasetField(name="s", datatype=DataType.STRING),
            DatasetField(name="ts", datatype=ts),
            DatasetField(name="y", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="x * 2")]), datatype=DataType.FLOAT),
            DatasetField(name="bad", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="no_such_fn(x)")]), datatype=DataType.FLOAT),
        ],
        filters=filters or [],
        metrics=metrics or [],
    )


def _shop() -> list[SlayerDataset]:
    return [
        SlayerDataset(name="customers", sql_table="customers", data_source="ds", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region", datatype=DataType.STRING)]),
        SlayerDataset(name="orders", sql_table="orders", data_source="ds", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="bad", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="no_such_fn(id)")]), datatype=DataType.FLOAT)],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])]),
    ]


class _Env:
    def __init__(self, *, engine: SlayerQueryEngine, db_path: str) -> None:
        self.engine = engine
        self.db_path = db_path

    def live(self, sql: str) -> None:
        with transaction(self.db_path) as conn:
            conn.executescript(sql)


@contextlib.asynccontextmanager
async def _env(tmp: Path, *datasets: SlayerDataset) -> AsyncGenerator[_Env]:
    db_path = str(tmp / "live.db")
    with transaction(db_path) as conn:
        conn.executescript(_SCHEMA)
    storage = YAMLStorage(base_dir=str(tmp / "storage"))
    await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=db_path))
    for m in datasets:
        await storage.save_dataset(m)
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


def _removes(err: SchemaDriftError, *, dataset: str, field: str) -> bool:
    return any(isinstance(e, EditDatasetDelete) and e.dataset_name == dataset and field in e.remove.fields
               for e in err.to_delete)


class TestUnreadDriftLeavesTheError:
    async def test_unrelated_drifted_column(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t(ts=DataType.INTEGER)) as env:
            err = await _unwrapped(env.engine, _DB_FAILURE)
        assert "no_such_fn" in str(err).lower(), err

    async def test_unread_dropped_column_and_its_measure(self, tmp_path: Path) -> None:
        model = _t(metrics=[DatasetMetric(name="n_s", expression="count_distinct(s)")])
        async with _env(tmp_path, model) as env:
            env.live("ALTER TABLE t DROP COLUMN s")
            await _unwrapped(env.engine, _DB_FAILURE)

    async def test_a_host_column_named_like_a_producer_column(self, tmp_path: Path) -> None:
        customers, orders = _shop()
        stale = customers.model_copy(update={"fields": [*customers.fields, DatasetField(name="amount", datatype=DataType.FLOAT)]})
        async with _env(tmp_path, stale, orders) as env:
            err = await _unwrapped(env.engine, {"population": "customers", "dimensions": ["customers.region"], "metrics": [
                {"metric": "orders.sum(amount)", "alias": "total"}, {"metric": "orders.sum(bad)", "alias": "b"}]})
        assert "no_such_fn" in str(err).lower(), err

    async def test_an_uncrossed_join_losing_its_key(self, tmp_path: Path) -> None:
        async with _env(tmp_path, *_shop()) as env:
            env.live("ALTER TABLE orders DROP COLUMN customer_id")
            err = await _unwrapped(env.engine, {"population": "orders", "metrics": [{"metric": "sum(orders.bad)"}]})
        assert "no_such_fn" in str(err).lower(), err


class TestReadDriftWraps:
    async def test_a_read_dropped_column(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t()) as env:
            env.live("ALTER TABLE t DROP COLUMN x")
            err = await _wrapped(env.engine, {"population": "t", "metrics": [{"metric": "sum(t.x)"}]})
        assert _removes(err, dataset="t", field="x"), err.to_delete

    async def test_a_read_dropped_column_beside_unrelated_drift(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t(ts=DataType.INTEGER)) as env:
            env.live("ALTER TABLE t DROP COLUMN x")
            err = await _wrapped(env.engine, {"population": "t", "metrics": [{"metric": "sum(t.x)"}]})
        assert _removes(err, dataset="t", field="x"), err.to_delete

    async def test_a_column_read_through_a_derived_column(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t()) as env:
            env.live("ALTER TABLE t DROP COLUMN x")
            await _wrapped(env.engine, {"population": "t", "metrics": [{"metric": "sum(t.y)"}]})

    async def test_a_join_key(self, tmp_path: Path) -> None:
        async with _env(tmp_path, *_shop()) as env:
            env.live("ALTER TABLE orders DROP COLUMN customer_id")
            await _wrapped(env.engine, {"population": "orders", "dimensions": ["customers.region"],
                                        "metrics": [{"metric": "sum(orders.amount)"}]})

    @pytest.mark.parametrize("formula", ["orders.sum(amount)", "sum(orders.amount)"])
    async def test_a_column_a_cross_model_producer_reads(self, tmp_path: Path, formula: str) -> None:
        async with _env(tmp_path, *_shop()) as env:
            env.live("ALTER TABLE orders DROP COLUMN amount")
            err = await _wrapped(env.engine, {"population": "customers", "dimensions": ["customers.region"],
                                              "metrics": [{"metric": formula, "alias": "total"}]})
        assert _removes(err, dataset="orders", field="amount"), err.to_delete

    async def test_a_used_model_measure(self, tmp_path: Path) -> None:
        model = _t(metrics=[DatasetMetric(name="total", expression="sum(x)")])
        async with _env(tmp_path, model) as env:
            env.live("ALTER TABLE t DROP COLUMN x")
            await _wrapped(env.engine, {"population": "t", "metrics": ["t.total"]})

    async def test_a_model_filter_always_counts_as_read(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _t(filters=["s <> 'zzz'"])) as env:
            env.live("ALTER TABLE t DROP COLUMN s")
            await _wrapped(env.engine, {"population": "t", "metrics": [{"metric": "sum(t.x)"}]})


_ORDERS_BAD = {"population": "orders", "metrics": [{"metric": "sum(orders.bad)"}]}


def _instrumented_orders() -> SlayerDataset:
    orders = _shop()[1]
    return orders.model_copy(update={
        "metrics": [DatasetMetric(name="m_bad", expression="sum(bad)")],
        "aggregations": [Aggregation(name="wbad", expression="SUM({value})")],
    })


def _inject(remove: RemoveSpec) -> Any:
    """Append an ``orders`` removal to the read-set drift entries the query-time path validates."""
    real = schema_drift.validate_datasource

    async def _with_entry(**kwargs: Any) -> list[Any]:
        entries = await real(**kwargs)
        if any(m.name == "orders" for m in kwargs["datasets"]):
            entries = [*entries, EditDatasetDelete(dataset_name="orders", data_source="ds", remove=remove,
                                                 reasons=[DeleteReason(target="injected", reason="injected")])]
        return entries

    return patch.object(schema_drift, "validate_datasource", side_effect=_with_entry)


def _relationship_removal() -> RemoveSpec:
    join = _shop()[1].relationships[0]
    return RemoveSpec(relationship_edges=[RelationshipEdgeRef.of(join)])


class TestInjectedRemovals:
    async def test_a_crossed_join(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(_relationship_removal()):
                await _wrapped(env.engine, {**_ORDERS_BAD, "dimensions": ["customers.region"]})

    async def test_an_uncrossed_join(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(_relationship_removal()):
                await _unwrapped(env.engine, _ORDERS_BAD)

    async def test_a_measure_used_by_name(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(RemoveSpec(metrics=["m_bad"])):
                await _wrapped(env.engine, {"population": "orders", "metrics": ["orders.m_bad"]})

    async def test_an_unused_measure(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(RemoveSpec(metrics=["m_bad"])):
                await _unwrapped(env.engine, _ORDERS_BAD)

    async def test_a_used_aggregation(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(RemoveSpec(aggregations=["wbad"])):
                await _wrapped(env.engine, {"population": "orders", "metrics": [{"metric": "wbad(orders.bad)"}]})

    async def test_an_unused_aggregation(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _shop()[0], _instrumented_orders()) as env:
            with _inject(RemoveSpec(aggregations=["wbad"])):
                await _unwrapped(env.engine, _ORDERS_BAD)


class TestInvalidSqlModel:
    async def test_its_own_invalid_sql_drop_is_no_evidence(self, tmp_path: Path) -> None:
        broken = SlayerDataset(name="sq_broken", data_source="ds",
                             sql="SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM orders", fields=[
                                 DatasetField(name="id", datatype=DataType.STRING, primary_key=True),
                                 DatasetField(name="amount", datatype=DataType.FLOAT)])
        async with _env(tmp_path, *_shop(), broken) as env:
            entries = [e for e in await env.engine.validate_datasets(data_source="ds") if e.dataset_name == "sq_broken"]
            err = await _unwrapped(env.engine, {"population": "sq_broken", "metrics": [{"metric": "count(sq_broken.*)"}]})
        assert [(type(e), e.cause if isinstance(e, WholeDatasetDelete) else None) for e in entries] == [
            (WholeDatasetDelete, "invalid_sql")], entries
        assert "no_such_function" in str(err).lower(), err


def _dated(*, ts: DataType, d: DataType) -> SlayerDataset:
    return SlayerDataset(name="t", sql_table="t", data_source="ds", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="x", datatype=DataType.FLOAT),
        DatasetField(name="ts", datatype=ts),
        DatasetField(name="d", datatype=d),
        DatasetField(name="bad", expression=ExpressionObject(dialects=[DialectEntry(dialect=d, expression="no_such_fn(x)") for d in ("DUCKDB", "SQLITE")]), datatype=DataType.FLOAT)])


def _retypes(entries: list[Any]) -> list[tuple[str, DataType, DataType]]:
    return [(e.field, e.stored_type, e.live_type) for e in entries if isinstance(e, RetypeFinding)]


def _deletes(entries: list[Any], field: str) -> list[EditDatasetDelete]:
    return [e for e in entries if isinstance(e, EditDatasetDelete) and field in e.remove.fields]


class TestSqliteTemporalOverText:
    async def test_validation_reports_nothing(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _dated(ts=DataType.DATETIME, d=DataType.DATE)) as env:
            entries = await env.engine.validate_datasets(data_source="ds")
        assert entries == [], entries

    async def test_query_time_attribution_does_not_blame_it(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _dated(ts=DataType.DATETIME, d=DataType.DATE)) as env:
            await _unwrapped(env.engine, _DB_FAILURE)

    async def test_a_non_temporal_declaration_over_text_is_still_drift(self, tmp_path: Path) -> None:
        async with _env(tmp_path, _dated(ts=DataType.INTEGER, d=DataType.DATE)) as env:
            entries = await env.engine.validate_datasets(data_source="ds")
        assert _retypes(entries) == [("ts", DataType.INTEGER, DataType.STRING)], entries
        assert not _deletes(entries, "ts"), entries


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
            await storage.save_dataset(_dated(ts=DataType.DATETIME, d=DataType.STRING))
            engine = SlayerQueryEngine(storage=storage)
            try:
                entries = await engine.validate_datasets(data_source="ds")
            finally:
                engine.close()
                engine_factory.invalidate_engine(ds)
        assert _retypes(entries) == [("ts", DataType.DATETIME, DataType.STRING)], entries
        assert not _deletes(entries, "ts"), entries
