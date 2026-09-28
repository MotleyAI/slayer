"""Query-time schema-drift attribution: read-set scope, scoped introspection, snapshot reuse."""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlglot import exp
from sqlglot.expressions.core import Expression

import slayer.engine.schema_drift as schema_drift
from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine import ingestion
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import EditModelDelete, LiveTable, WholeModelDelete
from slayer.engine.schema_scope import SchemaRef
from slayer.sql import sqlite_introspect
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

_TTL_S = 60.0

_SCHEMA = """
CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT);
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
CREATE TABLE orders (
    id INTEGER PRIMARY KEY, amount REAL, customer_id INTEGER, product_id INTEGER
);
INSERT INTO customers VALUES (1, 'US');
INSERT INTO products VALUES (1, 'widget', 2.0);
INSERT INTO orders VALUES (1, 10.0, 1, 1);
"""


def _base_models(ds: str) -> list[SlayerModel]:
    return [
        SlayerModel(
            name="customers", sql_table="customers", data_source=ds,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region", type=DataType.TEXT),
            ],
        ),
        SlayerModel(
            name="products", sql_table="products", data_source=ds,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
                Column(name="price", type=DataType.DOUBLE),
            ],
        ),
        SlayerModel(
            name="orders", sql_table="orders", data_source=ds,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="customer_id", type=DataType.INT),
                Column(name="product_id", type=DataType.INT),
                # Derived, so never drift; executing it fails for a non-drift reason.
                Column(name="bad", sql="no_such_fn(id)", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
                ModelJoin(target_model="products", join_pairs=[["product_id", "id"]]),
            ],
        ),
    ]


def _sql_model(*, name: str, sql: str, ds: str = "ds") -> SlayerModel:
    return SlayerModel(
        name=name, sql=sql, data_source=ds,
        columns=[
            # A 0-row SQLite trial reports every column as TEXT.
            Column(name="id", type=DataType.TEXT, primary_key=True),
            Column(name="bad", sql="no_such_fn(id)", type=DataType.DOUBLE),
        ],
    )


# Reads orders + customers (not products) and fails for a non-drift reason.
_UNRELATED_FAILURE = {
    "source_model": "orders",
    "dimensions": ["customers.region"],
    "measures": [{"formula": "sum(bad)"}],
}


class _Clock:
    """Injectable monotonic clock."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class _Env:
    def __init__(self, *, engine: SlayerQueryEngine, db_path: str, clock: _Clock, tmp: Path) -> None:
        self.engine = engine
        self.db_path = db_path
        self.clock = clock
        self.tmp = tmp

    def live(self, sql: str) -> None:
        with transaction(self.db_path) as conn:
            conn.executescript(sql)

    async def save(self, model: SlayerModel) -> None:
        await self.engine.storage.save_model(model)


class _Spies:
    def __init__(self, *, introspect: MagicMock, listing: MagicMock, trial: MagicMock, probe: MagicMock) -> None:
        self.introspect = introspect
        self.listing = listing
        self.trial = trial
        self.probe = probe

    def introspected(self) -> list[str]:
        return [c.kwargs["table_name"] for c in self.introspect.call_args_list]

    def listed_databases(self) -> set[str]:
        return {str(c.kwargs["inspector"].bind.url.database) for c in self.listing.call_args_list}

    def trialled(self) -> list[str]:
        return [c.kwargs["model"].name for c in self.trial.call_args_list]

    def probed_tables(self) -> set[str]:
        return {c.kwargs["table"] for c in self.probe.call_args_list}


def _install_clock(engine: SlayerQueryEngine, clock: _Clock) -> None:
    engine._drift_snapshots = schema_drift.LiveSnapshotCache(clock=clock)


async def _make_env(tmp: Path, *, schema: str, models: list[SlayerModel]) -> _Env:
    db_path = str(tmp / "live.db")
    with transaction(db_path) as conn:
        conn.executescript(schema)
    storage = YAMLStorage(base_dir=str(tmp / "storage"))
    await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=db_path))
    for m in models:
        await storage.save_model(m)
    engine = SlayerQueryEngine(storage=storage)
    clock = _Clock()
    _install_clock(engine, clock)
    return _Env(engine=engine, db_path=db_path, clock=clock, tmp=tmp)


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[_Env]:
    # No pre-drop connection: SQLite's EXPLAIN plans against a pooled connection's stale schema.
    e = await _make_env(tmp_path, schema=_SCHEMA, models=_base_models("ds"))
    try:
        yield e
    finally:
        await e.engine.aclose()


@pytest.fixture
def spies(env: _Env) -> Any:
    with patch.object(
        schema_drift, "_introspect_one_table", wraps=schema_drift._introspect_one_table,
    ) as introspect, patch.object(
        ingestion, "list_ingestable_objects", wraps=ingestion.list_ingestable_objects,
    ) as listing, patch.object(
        schema_drift, "_live_columns_for_sql_model", wraps=schema_drift._live_columns_for_sql_model,
    ) as trial, patch.object(
        sqlite_introspect, "probe_sqlite_integer_column",
        wraps=sqlite_introspect.probe_sqlite_integer_column,
    ) as probe:
        yield _Spies(introspect=introspect, listing=listing, trial=trial, probe=probe)


async def _fails_unwrapped(
    engine: SlayerQueryEngine, query: Any, *, explain: bool = False,
) -> SQLAlchemyError:
    with pytest.raises(SQLAlchemyError) as exc:
        await engine.execute(query, explain=explain)
    assert not isinstance(exc.value, SchemaDriftError)
    return exc.value


async def _fails_with_drift(
    engine: SlayerQueryEngine, query: Any, *, explain: bool = False,
) -> SchemaDriftError:
    with pytest.raises(SchemaDriftError) as exc:
        await engine.execute(query, explain=explain)
    return exc.value


def _blamed(err: SchemaDriftError) -> set[str]:
    return {e.model_name for e in err.to_delete}


_EXPLAIN = pytest.mark.parametrize("explain", [False, True], ids=["data-query", "explain"])


class TestReadSetScope:
    @_EXPLAIN
    async def test_drift_in_a_read_join_target_wraps(self, env: _Env, explain: bool) -> None:
        env.live("DROP TABLE customers")
        err = await _fails_with_drift(env.engine, {
            "source_model": "orders", "dimensions": ["customers.region"],
            "measures": [{"formula": "sum(amount)"}],
        }, explain=explain)
        assert "customers" in _blamed(err)
        assert any(
            isinstance(e, WholeModelDelete) and e.model_name == "customers" for e in err.to_delete
        )
        assert isinstance(err.__cause__, SQLAlchemyError)

    @_EXPLAIN
    async def test_drift_only_in_an_unread_join_component_model_does_not_wrap(
        self, env: _Env, explain: bool,
    ) -> None:
        env.live("ALTER TABLE products DROP COLUMN name")
        err = await _fails_unwrapped(env.engine, _UNRELATED_FAILURE, explain=explain)
        assert "no_such_fn" in str(err).lower()

    @pytest.mark.parametrize(
        ("dropped", "query", "named"),
        [
            pytest.param(
                "customers",
                {"source_model": "orders", "dimensions": ["customers.region"],
                 "measures": [{"formula": "sum(amount)"}]},
                None,
                id="single-stage",
            ),
            pytest.param(
                "customers",
                {"source_model": "s1", "measures": [{"formula": "sum(total)"}]},
                {"name": "s1", "source_model": "orders", "dimensions": ["customers.region"],
                 "measures": [{"formula": "sum(amount)", "name": "total"}]},
                id="multi-stage",
            ),
            pytest.param(
                "orders",
                {"source_model": "customers", "dimensions": ["region"],
                 "measures": [{"formula": "sum(orders.amount)"}]},
                None,
                id="cross-model-producer",
            ),
            pytest.param(
                "orders",
                {"source_model": "customers", "measures": [{"formula": "count(*)"}],
                 "filters": ["orders.amount > 5"]},
                None,
                id="filter-semi-join",
            ),
        ],
    )
    async def test_every_statement_shape_attributes_its_reads(
        self, env: _Env, dropped: str, query: dict, named: dict | None,
    ) -> None:
        env.live(f"DROP TABLE {dropped}")
        payload: Any = [named, query] if named is not None else query
        err = await _fails_with_drift(env.engine, payload)
        assert dropped in _blamed(err)
        assert dropped in err.models

    async def test_a_pruned_stored_stage_is_not_read(self, env: _Env) -> None:
        await env.engine.create_model_from_query(
            query=[
                {"name": "unused", "source_model": "products", "dimensions": ["name"],
                 "measures": [{"formula": "count(*)", "name": "n"}]},
                {"name": "s1", "source_model": "orders", "dimensions": ["customers.region"],
                 "measures": [{"formula": "sum(amount)", "name": "total"}]},
                {"source_model": "s1", "measures": [{"formula": "sum(total)", "name": "grand"}]},
            ],
            name="qb_pruned",
        )
        stored = await env.engine.storage.get_model("qb_pruned", data_source="ds")
        assert stored is not None
        # Swap s1's measure for one that fails at execution (not drift).
        stages = [
            s.model_copy(update={"measures": [{"formula": "sum(bad)", "name": "total"}]})
            if s.name == "s1" else s
            for s in stored.source_queries or []
        ]
        await env.save(stored.model_copy(update={"source_queries": stages}))
        query = {"source_model": "qb_pruned", "measures": [{"formula": "sum(grand)"}]}
        dry = await env.engine.execute(query, dry_run=True)
        assert "products" not in (dry.sql or ""), "fixture must prune the unused stage"

        env.live("ALTER TABLE products DROP COLUMN name")
        await _fails_unwrapped(env.engine, query)

    async def test_query_backed_source_is_read_through_its_stages(self, env: _Env) -> None:
        await env.engine.create_model_from_query(
            query=[
                {"name": "s1", "source_model": "orders", "dimensions": ["customers.region"],
                 "measures": [{"formula": "sum(amount)", "name": "total"}]},
                {"source_model": "s1", "measures": [{"formula": "sum(total)", "name": "grand"}]},
            ],
            name="qb",
        )
        env.live("ALTER TABLE orders DROP COLUMN amount")
        err = await _fails_with_drift(
            env.engine, {"source_model": "qb", "measures": [{"formula": "sum(grand)"}]},
        )
        assert "orders" in _blamed(err)


class TestPayload:
    async def test_models_lists_only_the_blamed_models(self, env: _Env) -> None:
        env.live("ALTER TABLE customers DROP COLUMN region")
        err = await _fails_with_drift(env.engine, {
            "source_model": "orders", "dimensions": ["customers.region"],
            "measures": [{"formula": "sum(amount)"}],
        })
        assert err.models == ["customers"]
        assert err.models == sorted(_blamed(err))

    async def test_models_are_sorted_and_distinct_across_blamed_entries(self, env: _Env) -> None:
        # Dropping customers blames it wholesale and cascades orders' join.
        env.live("DROP TABLE customers")
        err = await _fails_with_drift(env.engine, {
            "source_model": "orders", "dimensions": ["customers.region"],
            "measures": [{"formula": "sum(amount)"}],
        })
        assert err.models == ["customers", "orders"]
        assert err.models == sorted(_blamed(err))


class TestScopedInspection:
    async def test_only_the_read_table_is_introspected(self, tmp_path: Path) -> None:
        schema = "".join(
            f"CREATE TABLE t{i} (id INTEGER PRIMARY KEY, v REAL);" for i in range(10)
        )
        models = [
            SlayerModel(
                name=f"t{i}", sql_table=f"t{i}", data_source="ds",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="v", type=DataType.DOUBLE),
                    Column(name="bad", sql="no_such_fn(v)", type=DataType.DOUBLE),
                ],
            )
            for i in range(10)
        ]
        e = await _make_env(tmp_path, schema=schema, models=models)
        try:
            with patch.object(
                schema_drift, "_introspect_one_table", wraps=schema_drift._introspect_one_table,
            ) as introspect, patch.object(
                sqlite_introspect, "probe_sqlite_integer_column",
                wraps=sqlite_introspect.probe_sqlite_integer_column,
            ) as probe:
                await _fails_unwrapped(
                    e.engine, {"source_model": "t3", "measures": [{"formula": "sum(bad)"}]},
                )
            assert [c.kwargs["table_name"] for c in introspect.call_args_list] == ["t3"]
            assert {c.kwargs["table"] for c in probe.call_args_list} == {"t3"}
        finally:
            await e.engine.aclose()

    async def test_unread_sql_models_are_not_trial_executed(self, env: _Env, spies: _Spies) -> None:
        await env.save(_sql_model(name="sq_orders", sql="SELECT id, amount FROM orders"))
        await env.save(_sql_model(name="sq_customers", sql="SELECT id, region FROM customers"))
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert spies.trialled() == []

    async def test_only_the_read_sql_model_is_trial_executed(self, env: _Env, spies: _Spies) -> None:
        await env.save(_sql_model(name="sq_orders", sql="SELECT id, amount FROM orders"))
        await env.save(_sql_model(name="sq_customers", sql="SELECT id, region FROM customers"))
        await _fails_unwrapped(
            env.engine, {"source_model": "sq_orders", "measures": [{"formula": "sum(bad)"}]},
        )
        assert spies.trialled() == ["sq_orders"]

    async def test_a_schema_qualified_read_table_resolves_like_full_validation(
        self, env: _Env, spies: _Spies,
    ) -> None:
        orders = await env.engine.storage.get_model("orders", data_source="ds")
        assert orders is not None
        await env.save(orders.model_copy(update={"sql_table": "main.orders"}))
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert "orders" in spies.introspected()

    async def test_a_read_view_resolves_like_full_validation(self, env: _Env) -> None:
        env.live("CREATE VIEW v_orders AS SELECT id, amount FROM orders")
        await env.save(SlayerModel(
            name="orders_view", sql_table="v_orders", data_source="ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="bad", sql="no_such_fn(id)", type=DataType.DOUBLE),
            ],
        ))
        await _fails_unwrapped(
            env.engine, {"source_model": "orders_view", "measures": [{"formula": "sum(bad)"}]},
        )

    async def test_explicit_validation_stays_whole_datasource(self, env: _Env, spies: _Spies) -> None:
        env.live(
            "ALTER TABLE products DROP COLUMN name; ALTER TABLE customers DROP COLUMN region;"
        )
        await _fails_unwrapped(
            env.engine, {"source_model": "orders", "measures": [{"formula": "sum(bad)"}]},
        )
        entries = await env.engine.validate_models(data_source="ds")
        assert {e.model_name for e in entries} >= {"customers", "products"}
        assert {"customers", "products", "orders"} <= set(spies.introspected())


class TestSnapshotReuse:
    async def test_repeated_failures_reuse_the_snapshot(self, env: _Env, spies: _Spies) -> None:
        for _ in range(5):
            await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert spies.listing.call_count == 1
        assert sorted(spies.introspected()) == ["customers", "orders"]

    async def test_concurrent_failures_share_one_snapshot(self, env: _Env, spies: _Spies) -> None:
        results = await asyncio.gather(
            *(env.engine.execute(_UNRELATED_FAILURE) for _ in range(4)), return_exceptions=True,
        )
        assert all(
            isinstance(r, SQLAlchemyError) and not isinstance(r, SchemaDriftError) for r in results
        ), results
        assert sorted(spies.introspected()) == ["customers", "orders"]

    async def test_sql_model_trial_result_is_reused(self, env: _Env, spies: _Spies) -> None:
        await env.save(_sql_model(name="sq_orders", sql="SELECT id, amount FROM orders"))
        for _ in range(3):
            await _fails_unwrapped(
                env.engine, {"source_model": "sq_orders", "measures": [{"formula": "sum(bad)"}]},
            )
        assert spies.trialled() == ["sq_orders"]

    async def test_read_sql_models_sharing_sql_are_trialled_once(
        self, env: _Env, spies: _Spies,
    ) -> None:
        sql = "SELECT id, amount FROM orders"
        await env.save(_sql_model(name="sq_b", sql=sql))
        await env.save(_sql_model(name="sq_a", sql=sql).model_copy(update={
            "joins": [ModelJoin(target_model="sq_b", join_pairs=[["id", "id"]])],
        }))
        await _fails_unwrapped(env.engine, {
            "source_model": "sq_a", "dimensions": ["sq_b.id"],
            "measures": [{"formula": "sum(bad)"}],
        })
        assert len(spies.trialled()) == 1

    async def test_broken_sql_model_classification_is_reused(self, env: _Env, spies: _Spies) -> None:
        await env.save(_sql_model(
            name="sq_broken", sql="SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM orders",
        ))
        with patch.object(
            schema_drift, "_source_tables_resolve", wraps=schema_drift._source_tables_resolve,
        ) as resolve:
            for _ in range(2):
                await _fails_unwrapped(
                    env.engine, {"source_model": "sq_broken", "measures": [{"formula": "count(*)"}]},
                )
        assert spies.trialled() == ["sq_broken"]
        assert resolve.call_count == 1

    async def test_facts_expire(self, env: _Env, spies: _Spies) -> None:
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        env.clock.now += _TTL_S + 1
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert spies.listing.call_count == 2
        assert spies.introspected().count("orders") == 2

    async def test_relisting_does_not_extend_the_snapshot(self, env: _Env, spies: _Spies) -> None:
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        env.clock.now += _TTL_S / 2
        env.live("CREATE TABLE orders_v2 AS SELECT * FROM orders")
        orders = await env.engine.storage.get_model("orders", data_source="ds")
        assert orders is not None
        await env.save(orders.model_copy(update={"sql_table": "orders_v2"}))
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert spies.listing.call_count == 2
        env.clock.now += _TTL_S / 2 + 1
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert spies.listing.call_count == 3

    async def test_unavailable_introspection_is_reused_as_no_verdict(
        self, env: _Env, spies: _Spies,
    ) -> None:
        env.live("DROP TABLE orders")
        spies.listing.side_effect = RuntimeError("permission denied")
        query = {"source_model": "orders", "measures": [{"formula": "sum(amount)"}]}
        for _ in range(2):
            err = await _fails_unwrapped(env.engine, query)
            assert "no such table" in str(err).lower()
        assert spies.listing.call_count == 1

    async def test_a_model_edit_within_the_window_is_honoured(self, env: _Env, spies: _Spies) -> None:
        env.live("ALTER TABLE orders DROP COLUMN amount")
        first = await _fails_with_drift(
            env.engine, {"source_model": "orders", "measures": [{"formula": "sum(amount)"}]},
        )
        assert any(
            isinstance(e, EditModelDelete) and e.model_name == "orders"
            and "amount" in e.remove.columns
            for e in first.to_delete
        )
        orders = await env.engine.storage.get_model("orders", data_source="ds")
        assert orders is not None
        await env.save(orders.model_copy(update={
            "columns": [c for c in orders.columns if c.name != "amount"],
        }))
        await _fails_unwrapped(
            env.engine, {"source_model": "orders", "measures": [{"formula": "sum(bad)"}]},
        )
        assert spies.listing.call_count == 1

    async def test_a_newly_created_table_is_not_reported_dropped(
        self, env: _Env, spies: _Spies,
    ) -> None:
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        env.live("CREATE TABLE orders_v2 AS SELECT * FROM orders")
        orders = await env.engine.storage.get_model("orders", data_source="ds")
        assert orders is not None
        await env.save(orders.model_copy(update={"sql_table": "orders_v2"}))
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert spies.listing.call_count == 2

    async def test_a_dropped_tables_absence_is_reused(self, env: _Env, spies: _Spies) -> None:
        env.live("DROP TABLE orders")
        query = {"source_model": "orders", "measures": [{"formula": "sum(amount)"}]}
        for _ in range(3):
            err = await _fails_with_drift(env.engine, query)
            assert any(
                isinstance(e, WholeModelDelete) and e.model_name == "orders" for e in err.to_delete
            )
        assert spies.listing.call_count == 1

    async def test_a_same_named_table_in_another_listed_schema_triggers_a_relist(
        self, tmp_path: Path,
    ) -> None:
        db_path = str(tmp_path / "live.db")
        with transaction(db_path) as conn:
            conn.executescript("CREATE TABLE seed (id INTEGER);")
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        listed_objects: dict[str | None, list[str]] = {"a": ["orders"], "b": []}

        def _listing(*, inspector: Any, ref: SchemaRef, include_views: bool) -> list[Any]:
            return [SimpleNamespace(name=n) for n in listed_objects[ref.name]]

        def _orders(schema: str) -> SlayerModel:
            return SlayerModel(
                name="orders", sql_table=f"{schema}.orders", data_source="ds",
                columns=[Column(name="id", type=DataType.TEXT, primary_key=True)],
            )

        snapshot = schema_drift.DriftSnapshot(created_at=0.0)
        with patch.object(
            schema_drift, "_live_schema_refs", return_value=[SchemaRef(name="a"), SchemaRef(name="b")],
        ), patch.object(
            ingestion, "list_ingestable_objects", side_effect=_listing,
        ) as listing, patch.object(
            schema_drift, "_introspect_one_table", return_value=LiveTable(columns={"id": DataType.TEXT}),
        ):
            first = await schema_drift.validate_datasource(
                datasource=ds, models=[_orders("a")], snapshot=snapshot,
            )
            # b.orders is created after the listing; only its basename was listed (in a).
            listed_objects["b"].append("orders")
            second = await schema_drift.validate_datasource(
                datasource=ds, models=[_orders("b")], snapshot=snapshot,
            )
        assert first == []
        assert second == []
        assert listing.call_count == 4

    async def test_a_same_named_table_in_an_unread_schema_is_not_introspected(
        self, tmp_path: Path,
    ) -> None:
        db_path = str(tmp_path / "live.db")
        with transaction(db_path) as conn:
            conn.executescript("CREATE TABLE seed (id INTEGER);")
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        model = SlayerModel(
            name="orders", sql_table="a.orders", data_source="ds",
            columns=[Column(name="id", type=DataType.TEXT, primary_key=True)],
        )
        with patch.object(
            schema_drift, "_live_schema_refs", return_value=[SchemaRef(name="a"), SchemaRef(name="b")],
        ), patch.object(
            ingestion, "list_ingestable_objects",
            side_effect=lambda **_: [SimpleNamespace(name="orders")],
        ), patch.object(
            schema_drift, "_introspect_one_table", return_value=LiveTable(columns={"id": DataType.TEXT}),
        ) as introspect:
            entries = await schema_drift.validate_datasource(
                datasource=ds, models=[model],
                snapshot=schema_drift.DriftSnapshot(created_at=0.0),
            )
        assert entries == []
        assert [c.kwargs["ref"].name for c in introspect.call_args_list] == ["a"]

    async def test_explicit_validation_reads_live(self, env: _Env, spies: _Spies) -> None:
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        env.live("ALTER TABLE customers DROP COLUMN region")
        entries = await env.engine.validate_models(data_source="ds")
        assert any(
            isinstance(e, EditModelDelete) and e.model_name == "customers"
            and "region" in e.remove.columns
            for e in entries
        )
        assert spies.listing.call_count == 2


class TestResolvedDatasource:
    async def test_attribution_validates_only_the_executing_datasource(
        self, env: _Env, spies: _Spies, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for other in ("ds_a", "ds_b"):
            path = str(env.tmp / f"{other}.db")
            with transaction(path) as conn:
                conn.executescript(f"CREATE TABLE {other}_t (id INTEGER PRIMARY KEY);")
            await env.engine.storage.save_datasource(
                DatasourceConfig(name=other, type="sqlite", database=path),
            )
            await env.save(SlayerModel(
                name=f"{other}_t", sql_table=f"{other}_t", data_source=other,
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
            ))
        spies.listing.reset_mock()
        render = SlayerQueryEngine._plan_and_render

        async def _foreign_source(self: SlayerQueryEngine, **kwargs: Any) -> Any:
            # A pipeline model always carries a data_source; make it disagree with the executing one.
            rendered = await render(self, **kwargs)
            foreign = rendered.model.model_copy(update={"data_source": "ds_a"})
            return rendered.model_copy(update={"model": foreign})

        monkeypatch.setattr(SlayerQueryEngine, "_plan_and_render", _foreign_source)
        await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert spies.listed_databases() == {env.db_path}


_INTROSPECT = schema_drift._introspect_one_table


def _customers_unreadable(**kwargs: Any) -> LiveTable:
    if kwargs["table_name"] == "customers":
        raise SQLAlchemyError("metadata read timed out")
    return _INTROSPECT(**kwargs)


class TestUnreadableTable:
    async def test_a_listed_table_that_fails_to_introspect_is_not_reported_dropped(
        self, env: _Env,
    ) -> None:
        with patch.object(schema_drift, "_introspect_one_table", side_effect=_customers_unreadable):
            entries = await env.engine.validate_models(data_source="ds")
        assert "customers" not in {e.model_name for e in entries}

    async def test_an_unreadable_read_table_is_no_attribution_evidence(self, env: _Env) -> None:
        with patch.object(schema_drift, "_introspect_one_table", side_effect=_customers_unreadable):
            await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)

    async def test_an_all_unreadable_read_set_leaves_other_reads_attributable(
        self, env: _Env,
    ) -> None:
        env.live("ALTER TABLE customers DROP COLUMN region")
        with patch.object(schema_drift, "_introspect_one_table", side_effect=_customers_unreadable):
            await _fails_unwrapped(env.engine, {
                "source_model": "customers", "dimensions": ["region"],
                "measures": [{"formula": "count(*)"}],
            })
            env.live("ALTER TABLE orders DROP COLUMN amount")
            err = await _fails_with_drift(
                env.engine, {"source_model": "orders", "measures": [{"formula": "sum(amount)"}]},
            )
        assert "orders" in _blamed(err)

    async def test_an_unreadable_table_is_read_once_per_snapshot(self, env: _Env) -> None:
        with patch.object(
            schema_drift, "_introspect_one_table", side_effect=_customers_unreadable,
        ) as introspect:
            for _ in range(2):
                await _fails_unwrapped(env.engine, _UNRELATED_FAILURE)
        assert [c.kwargs["table_name"] for c in introspect.call_args_list].count("customers") == 1


async def _unreachable_engine(tmp: Path) -> SlayerQueryEngine:
    """An engine whose one sqlite datasource lives in a missing directory, so every connect fails."""
    storage = YAMLStorage(base_dir=str(tmp / "storage"))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=str(tmp / "missing" / "live.db")),
    )
    await storage.save_model(_base_models("ds")[0])
    await storage.save_model(_sql_model(name="sq_customers", sql="SELECT id, region FROM customers"))
    engine = SlayerQueryEngine(storage=storage)
    _install_clock(engine, _Clock())
    return engine


class TestUnreachableDatasource:
    @pytest.mark.parametrize("source", ["customers", "sq_customers"])
    async def test_an_unreachable_datasource_is_reused_as_no_verdict(
        self, tmp_path: Path, source: str,
    ) -> None:
        engine = await _unreachable_engine(tmp_path)
        query = {"source_model": source, "measures": [{"formula": "count(*)"}]}
        try:
            with patch.object(
                schema_drift._LiveConnection, "open", autospec=True,
                side_effect=schema_drift._LiveConnection.open,
            ) as connect, patch.object(
                schema_drift, "_live_columns_for_sql_model",
                wraps=schema_drift._live_columns_for_sql_model,
            ) as trial:
                await _fails_unwrapped(engine, query)
                attempts = connect.call_count + trial.call_count
                await _fails_unwrapped(engine, query)
            assert attempts >= 1
            assert connect.call_count + trial.call_count == attempts
        finally:
            await engine.aclose()

    async def test_explicit_validation_of_an_unreachable_datasource_reports_no_sql_drift(
        self, tmp_path: Path,
    ) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=str(tmp_path / "missing" / "live.db")),
        )
        await storage.save_model(_sql_model(name="sq_customers", sql="SELECT id, region FROM customers"))
        engine = SlayerQueryEngine(storage=storage)
        try:
            with pytest.raises(SQLAlchemyError):
                await engine.validate_models(data_source="ds")
        finally:
            await engine.aclose()


_STAMP = "slayer_model"
_FIT_MARKER_RE = re.compile(r"_[0-9a-f]{8}_")
_LONG = "AnExtremelyLongModelNameThatForcesIdentifierFitting"

_QB_STAGES = [
    {"name": "s1", "source_model": "orders", "dimensions": ["customers.region"],
     "measures": [{"formula": "sum(amount)", "name": "total"}]},
    {"source_model": "s1", "measures": [{"formula": "sum(total)", "name": "grand"}]},
]

# (id, input, expected read set); query-backed models qb / qb_pruned are saved by the test.
_READ_SET_CORPUS = [
    pytest.param(
        {"source_model": "orders", "dimensions": ["customers.region"],
         "measures": [{"formula": "sum(amount)"}]},
        {"orders", "customers"}, id="single-stage",
    ),
    pytest.param(
        [_QB_STAGES[0], {"source_model": "s1", "measures": [{"formula": "sum(total)"}]}],
        {"orders", "customers"}, id="multi-stage",
    ),
    pytest.param(
        {"source_model": "qb", "measures": [{"formula": "sum(grand)"}]},
        {"qb", "orders", "customers"}, id="query-backed-splice",
    ),
    pytest.param(
        {"source_model": "qb_pruned", "measures": [{"formula": "sum(grand)"}]},
        {"qb_pruned", "orders", "customers"}, id="pruned-splice",
    ),
    pytest.param(
        {"source_model": "customers", "dimensions": ["region"],
         "measures": [{"formula": "sum(orders.amount)"}]},
        {"customers", "orders"}, id="cross-model-producer",
    ),
    pytest.param(
        {"source_model": "customers", "measures": [{"formula": "count(*)"}],
         "filters": ["orders.amount > 5"]},
        {"customers", "orders"}, id="filter-semi-join",
    ),
    pytest.param(
        {"source_model": "sq_orders", "measures": [{"formula": "count(*)"}]},
        {"sq_orders"}, id="sql-model",
    ),
]


def _long_chain_models(ds: str) -> list[SlayerModel]:
    return [
        SlayerModel(
            name=f"{_LONG}Invoice", sql_table="invoices", data_source=ds,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="customer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model=f"{_LONG}Customer", join_pairs=[["customer_id", "id"]])],
        ),
        SlayerModel(
            name=f"{_LONG}Customer", sql_table="customers", data_source=ds,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="consumer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model=f"{_LONG}Consumer", join_pairs=[["consumer_id", "id"]])],
        ),
        SlayerModel(
            name=f"{_LONG}Consumer", sql_table="consumers", data_source=ds,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="lifetime_value", type=DataType.DOUBLE),
            ],
        ),
    ]


def _assert_relations_stamped(statement: Expression, *, model_by_table: dict[str, str]) -> set[str]:
    """Every physical model relation carries the stamp, stage/CTE relations never; returns stamped sql models."""
    ctes = {c.alias_or_name for c in statement.find_all(exp.CTE)}
    sql_models = [s for s in statement.find_all(exp.Subquery) if _STAMP in s.meta]
    inside_sql_models = {id(n) for s in sql_models for n in s.this.walk()}
    for table in statement.find_all(exp.Table):
        if id(table) in inside_sql_models:
            continue
        stamp = table.meta.get(_STAMP)
        if table.name in ctes:
            assert stamp is None, f"stage/CTE relation {table.name!r} stamped {stamp!r}"
        else:
            assert stamp == model_by_table[table.name], f"{table.sql()} stamped {stamp!r}"
    return {s.meta[_STAMP] for s in sql_models}


async def _render(engine: SlayerQueryEngine, payload: Any) -> tuple[Expression, set[str], str]:
    """The final statement AST, the prepared read set, and the executed SQL for ``payload``."""
    main, named, ds, chain = await engine._normalize_input(
        payload, runtime_kwarg={}, prefer_data_source=None,
    )
    rendered = await engine._plan_and_render(
        query=main, named_queries=named, runtime_kwarg={}, prefer_data_source=ds,
        splice_chain=chain, as_statement=True,
    )
    prepared = await engine._prepare_pipeline(
        query=main, named_queries=named, runtime_kwarg={}, prefer_data_source=ds,
        splice_chain=chain,
    )
    assert rendered.statement is not None
    return rendered.statement, set(prepared.touched), prepared.sql


class TestReadSetStamp:
    @pytest.fixture
    async def corpus_env(self, env: _Env) -> _Env:
        await env.save(_sql_model(name="sq_orders", sql="SELECT id, amount FROM orders"))
        await env.engine.create_model_from_query(query=_QB_STAGES, name="qb")
        await env.engine.create_model_from_query(
            query=[
                {"name": "unused", "source_model": "products", "dimensions": ["name"],
                 "measures": [{"formula": "count(*)", "name": "n"}]},
                *_QB_STAGES,
            ],
            name="qb_pruned",
        )
        return env

    @pytest.mark.parametrize(("payload", "expected"), _READ_SET_CORPUS)
    async def test_read_set_is_the_stamped_relations_of_the_final_statement(
        self, corpus_env: _Env, payload: Any, expected: set[str],
    ) -> None:
        statement, touched, _ = await _render(corpus_env.engine, payload)
        sql_models = _assert_relations_stamped(
            statement,
            model_by_table={"orders": "orders", "customers": "customers", "products": "products"},
        )
        assert sql_models == expected & {"sq_orders"}
        assert touched == expected

    async def test_over_limit_identifiers_keep_the_stamp(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(
            name="pg", type="postgres", host="localhost", port=5432,
            database="x", username="u", password="p",
        ))
        for m in _long_chain_models("pg"):
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)
        try:
            statement, touched, sql = await _render(engine, {
                "source_model": f"{_LONG}Invoice",
                "dimensions": ["status", f"{_LONG}Customer.{_LONG}Consumer.lifetime_value"],
                "measures": [{"formula": "count(*)"}],
            })
        finally:
            await engine.aclose()
        assert _FIT_MARKER_RE.search(sql), "fixture must force identifier fitting"
        _assert_relations_stamped(statement, model_by_table={
            "invoices": f"{_LONG}Invoice", "customers": f"{_LONG}Customer",
            "consumers": f"{_LONG}Consumer",
        })
        assert touched == {f"{_LONG}Invoice", f"{_LONG}Customer", f"{_LONG}Consumer"}


class TestLiveSnapshotCache:
    @staticmethod
    def _ds(i: int) -> DatasourceConfig:
        return DatasourceConfig(name=f"ds{i}", type="sqlite", database=f"/nonexistent/ds{i}.db")

    async def test_least_recently_used_datasource_is_evicted_past_the_cap(self) -> None:
        clock = _Clock()
        cache = schema_drift.LiveSnapshotCache(clock=clock)
        for i in range(256):
            async with cache.acquire(self._ds(i)):
                pass
        async with cache.acquire(self._ds(0)):
            pass
        async with cache.acquire(self._ds(256)):
            pass
        assert len(cache) == 256
        clock.now += 1
        async with cache.acquire(self._ds(0)) as recently_used:
            assert recently_used.created_at == clock.now - 1
        async with cache.acquire(self._ds(1)) as evicted:
            assert evicted.created_at == clock.now

    async def test_a_snapshot_that_expires_while_waiting_is_replaced(self) -> None:
        clock = _Clock()
        cache = schema_drift.LiveSnapshotCache(clock=clock)
        release = asyncio.Event()

        async def _holder() -> None:
            async with cache.acquire(self._ds(0)):
                await release.wait()

        async def _waiter() -> float:
            async with cache.acquire(self._ds(0)) as snapshot:
                return snapshot.created_at

        holder = asyncio.create_task(_holder())
        await asyncio.sleep(0)
        waiter = asyncio.create_task(_waiter())
        await asyncio.sleep(0)
        clock.now += _TTL_S + 1
        release.set()
        await holder
        assert await waiter == clock.now

    async def test_a_held_snapshot_is_not_swept(self) -> None:
        clock = _Clock()
        cache = schema_drift.LiveSnapshotCache(clock=clock)
        release = asyncio.Event()
        held = asyncio.Event()

        async def _holder() -> None:
            async with cache.acquire(self._ds(0)):
                held.set()
                await release.wait()

        holder = asyncio.create_task(_holder())
        await held.wait()
        clock.now += _TTL_S + 1
        async with cache.acquire(self._ds(1)):
            pass
        assert len(cache) == 2
        release.set()
        await holder

    async def test_a_held_snapshot_is_not_evicted(self) -> None:
        clock = _Clock()
        cache = schema_drift.LiveSnapshotCache(clock=clock)
        release = asyncio.Event()
        held = asyncio.Event()

        async def _holder() -> None:
            async with cache.acquire(self._ds(0)):
                held.set()
                await release.wait()

        holder = asyncio.create_task(_holder())
        await held.wait()
        for i in range(1, 258):
            async with cache.acquire(self._ds(i)):
                pass
        release.set()
        await holder
        clock.now += 1
        async with cache.acquire(self._ds(0)) as snapshot:
            assert snapshot.created_at == clock.now - 1

    @pytest.mark.parametrize("teardown", ["close", "aclose"])
    async def test_engine_teardown_drops_snapshots(self, env: _Env, teardown: str) -> None:
        async with env.engine._drift_snapshots.acquire(self._ds(0)):
            pass
        result = getattr(env.engine, teardown)()
        if teardown == "aclose":
            await result
        assert len(env.engine._drift_snapshots) == 0

    async def test_expired_snapshots_are_swept_on_access(self) -> None:
        clock = _Clock()
        cache = schema_drift.LiveSnapshotCache(clock=clock)
        async with cache.acquire(self._ds(0)):
            pass
        clock.now += _TTL_S + 1
        async with cache.acquire(self._ds(1)):
            pass
        assert len(cache) == 1
