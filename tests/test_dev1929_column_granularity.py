"""DEV-1929: a model column carries a time-bucket ``granularity``, and a finer /
non-nesting time dimension over a bucketed model column — hand-set, query-backed
cache, or sibling-stage stand-in — is the same typed re-bucketing error as over a
stage column. Same or nesting-coarser still binds and executes.
"""

from __future__ import annotations

import json
import os
from slayer.storage.sqlite_conn import transaction
import tempfile

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import TimeDimensionColumnError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import bind_time_dimension
from slayer.engine.elaborate_env import check_time_dimension_column
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    model_from_stage_schema,
)
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1471_fixtures import BACKENDS, date_str, make_engine, orders_table_spec

TG = TimeGranularity

# (id, customer_id, amount, region, created_at, shipped_at)
_SEED = [
    (1, 100, 100.0, "W", "2024-11-10", None),
    (2, 100, 200.0, "W", "2024-12-05", None),
    (3, 101, 300.0, "E", "2025-01-15", None),
    (4, 101, 50.0, "E", "2025-01-20", None),
    (5, 102, 500.0, "N", "2025-02-10", None),
]
_MONTH_SUMS = {"2024-11-01": 100.0, "2024-12-01": 200.0, "2025-01-01": 350.0, "2025-02-01": 500.0}
_YEAR_SUMS = {"2024-01-01": 300.0, "2025-01-01": 850.0}


def _orders_columns(*, created_at_granularity=None) -> list[Column]:
    return [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
        Column(name="region", sql="region", type=DataType.TEXT),
        Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP,
               granularity=created_at_granularity),
        Column(name="shipped_at", sql="shipped_at", type=DataType.TIMESTAMP),
    ]


def _orders_model(*, gran=None, data_source="ds") -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        default_time_dimension="created_at",
        columns=_orders_columns(created_at_granularity=gran),
    )


async def _seeded_engine(backend: str, tmp: str, *, gran=None) -> SlayerQueryEngine:
    return await make_engine(
        backend, base_dir=os.path.join(tmp, "store"),
        db_path=os.path.join(tmp, f"t.{backend}"),
        tables=[orders_table_spec(_SEED)], models=[_orders_model(gran=gran)],
    )


async def _dryrun_engine(models: list[SlayerModel]) -> SlayerQueryEngine:
    """Storage + engine with no real DB — enough for plan-time binding/checking."""
    storage = YAMLStorage(base_dir=tempfile.mkdtemp())
    await storage.save_datasource(DatasourceConfig(name=models[0].data_source, type="sqlite"))
    for model in models:
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


def _by_bucket(rows: list[dict]) -> dict:
    """Map ``created_at`` bucket (first 10 chars) → the row's single measure value."""
    out: dict = {}
    for r in rows:
        date_k = next(k for k in r if k.endswith("created_at"))
        val_k = next(k for k in r if k != date_k)
        out[date_str(r[date_k])] = r[val_k]
    return out


def _col(model: SlayerModel | None, name: str) -> Column:
    assert model is not None
    return next(c for c in model.columns if c.name == name)


async def _call(server, *, name: str, arguments: dict) -> str:
    blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return blocks[0].text


# ---------------------------------------------------------------------------
# Construction — Column.granularity field + non-temporal validator (task 2.1).
# ---------------------------------------------------------------------------
class TestColumnConstruction:
    @pytest.mark.parametrize("t", [DataType.TIMESTAMP, DataType.DATE])
    def test_temporal_column_accepts_granularity(self, t) -> None:
        assert Column(name="created_at", type=t, granularity=TG.MONTH).granularity == TG.MONTH

    def test_string_column_rejects_granularity(self) -> None:
        with pytest.raises(ValueError) as exc:
            Column(name="created_at", type=DataType.TEXT, granularity=TG.MONTH)
        msg = str(exc.value)
        assert "created_at" in msg
        assert "month" in msg
        assert "TEXT" in msg

    def test_default_type_rejects_granularity(self) -> None:
        with pytest.raises(ValueError) as exc:
            Column(name="ts", granularity=TG.MONTH)
        msg = str(exc.value)
        assert "ts" in msg
        assert "month" in msg
        assert "TEXT" in msg

    def test_dict_string_spelling_accepted(self) -> None:
        c = Column.model_validate({"name": "created_at", "type": "time", "granularity": "month"})
        assert c.granularity == TG.MONTH

    def test_model_dump_roundtrips_the_enum(self) -> None:
        c = Column(name="created_at", type=DataType.TIMESTAMP, granularity=TG.MONTH)
        assert Column.model_validate(c.model_dump()).granularity == TG.MONTH
        assert Column.model_validate(c.model_dump(mode="json")).granularity == TG.MONTH

    async def test_yaml_save_load_roundtrip(self) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite"))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            columns=[Column(name="created_at", type=DataType.TIMESTAMP, granularity=TG.MONTH)],
        ))
        loaded = await storage.get_model("orders")
        assert _col(loaded, "created_at").granularity == TG.MONTH


# ---------------------------------------------------------------------------
# Binder + checker — the ModelScope arm reads Column.granularity (task 2.2);
# the re-bucketing message is generalised, no "upstream stage" (task 2.3).
# ---------------------------------------------------------------------------
class TestBinderAndChecker:
    def _orders_customers(self, *, gran_o=None, gran_c=None):
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="ds",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="signup_at", type=DataType.TIMESTAMP, granularity=gran_c),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP, granularity=gran_o),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        return orders, customers

    def test_bare_column_returns_handset_granularity(self) -> None:
        orders, customers = self._orders_customers(gran_o=TG.DAY)
        bound = bind_time_dimension(
            TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.DAY),
            scope=ModelScope(source_model=orders),
            bundle=ResolvedSourceBundle(dialect="postgres", source_model=orders, referenced_models=[customers]),
        )
        assert bound.upstream_granularity == TG.DAY
        assert bound.column_type == DataType.TIMESTAMP

    def test_dotted_join_path_returns_handset_granularity(self) -> None:
        orders, customers = self._orders_customers(gran_c=TG.MONTH)
        bound = bind_time_dimension(
            TimeDimension(dimension=ColumnRef(name="customers.signup_at"), granularity=TG.YEAR),
            scope=ModelScope(source_model=orders),
            bundle=ResolvedSourceBundle(dialect="postgres", source_model=orders, referenced_models=[customers]),
        )
        assert bound.upstream_granularity == TG.MONTH

    def test_rebucket_message_is_generalised(self) -> None:
        with pytest.raises(TimeDimensionColumnError) as exc:
            check_time_dimension_column(
                name="monthly.created_at", column_type=DataType.TIMESTAMP,
                upstream_granularity=TG.MONTH, requested_granularity=TG.DAY,
            )
        msg = str(exc.value)
        assert "month" in msg
        assert "day" in msg
        assert "already bucketed" in msg
        assert "bucket the raw column instead" in msg
        assert "nesting-coarser" in msg
        assert "upstream stage" not in msg


# ---------------------------------------------------------------------------
# Query-backed cache stamping (task 2.4) — cache columns + persistence.
# ---------------------------------------------------------------------------
class TestQueryBackedCache:
    async def test_bucketed_column_stamped_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            model = await engine.create_model_from_query(
                query={"source_model": "orders",
                       "measures": [{"formula": "amount:sum", "name": "rev"}],
                       "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]},
                name="monthly",
            )
            assert _col(model, "created_at").granularity == TG.MONTH
            assert _col(model, "created_at").type in (DataType.DATE, DataType.TIMESTAMP)
            assert _col(model, "rev").granularity is None
            reloaded = await engine.storage.get_model("monthly")
            assert _col(reloaded, "created_at").granularity == TG.MONTH
            assert _col(reloaded, "rev").granularity is None

    async def test_unbucketed_columns_carry_no_granularity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            plain = await engine.create_model_from_query(
                query={"source_model": "orders", "dimensions": ["created_at"],
                       "measures": [{"formula": "amount:sum", "name": "rev"}]},
                name="raw_ts",
            )
            assert all(c.granularity is None for c in plain.columns)
            # A time dimension over the unbucketed cached column still binds.
            await engine.execute(query=SlayerQuery.model_validate({
                "source_model": "raw_ts",
                "time_dimensions": [{"dimension": "created_at", "granularity": "day"}],
                "measures": [{"formula": "rev:sum"}],
            }), dry_run=True)
            maxed = await engine.create_model_from_query(
                query={"source_model": "orders", "dimensions": ["region"],
                       "measures": [{"formula": "created_at:max"}]},
                name="maxed",
            )
            assert all(c.granularity is None for c in maxed.columns)
            # A time dimension over the unbucketed aggregate output binds.
            agg_col = next(c.name for c in maxed.columns if c.name != "region")
            await engine.execute(query=SlayerQuery.model_validate({
                "source_model": "maxed",
                "time_dimensions": [{"dimension": agg_col, "granularity": "day"}],
                "measures": [{"formula": "*:count"}],
            }), dry_run=True)
            # A computed dimension is cached without a granularity.
            band = "CASE WHEN amount:sum(partition_by=region) > 100 THEN 'big' ELSE 'small' END"
            banded = await engine.create_model_from_query(
                query={"source_model": "orders",
                       "dimensions": [{"expression": band, "name": "size_band"}],
                       "measures": [{"formula": "amount:sum", "name": "rev"}]},
                name="banded",
            )
            assert all(c.granularity is None for c in banded.columns)

    async def test_nested_query_backed_models_compose(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            await engine.create_model_from_query(
                query={"source_model": "orders",
                       "measures": [{"formula": "amount:sum", "name": "rev"}],
                       "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]},
                name="monthly",
            )
            yearly = await engine.create_model_from_query(
                query={"source_model": "monthly",
                       "measures": [{"formula": "rev:sum"}],
                       "time_dimensions": [{"dimension": "created_at", "granularity": "year"}]},
                name="yearly",
            )
            assert _col(yearly, "created_at").granularity == TG.YEAR
            finer = SlayerQuery.model_validate({
                "source_model": "yearly",
                "time_dimensions": [{"dimension": "created_at", "granularity": "day"}],
                "measures": [{"formula": "rev_sum:sum"}],
            })
            with pytest.raises(TimeDimensionColumnError):
                await engine.execute(query=finer, dry_run=True)


# ---------------------------------------------------------------------------
# Executed on SQLite + DuckDB — the issue repro and its valid neighbours.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("backend", BACKENDS)
class TestQueryBackedExecuted:
    async def _monthly_engine(self, backend: str, tmp: str) -> SlayerQueryEngine:
        engine = await _seeded_engine(backend, tmp)
        await engine.create_model_from_query(
            query={"source_model": "orders",
                   "measures": [{"formula": "amount:sum", "name": "rev"}],
                   "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]},
            name="monthly",
        )
        return engine

    async def test_issue_repro_finer_rejected(self, backend: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await self._monthly_engine(backend, tmp)
            finer = SlayerQuery.model_validate({
                "source_model": "monthly",
                "time_dimensions": [{"dimension": "created_at", "granularity": "day"}],
                "measures": [{"formula": "rev:sum"}],
            })
            with pytest.raises(TimeDimensionColumnError):
                await engine.execute(query=finer)

    async def test_same_granularity_matches_direct(self, backend: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await self._monthly_engine(backend, tmp)
            resp = await engine.execute(query=SlayerQuery.model_validate({
                "source_model": "monthly",
                "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
                "measures": [{"formula": "rev:sum"}],
            }))
            assert _by_bucket(resp.data) == _MONTH_SUMS

    async def test_coarser_year_aggregates(self, backend: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await self._monthly_engine(backend, tmp)
            resp = await engine.execute(query=SlayerQuery.model_validate({
                "source_model": "monthly",
                "time_dimensions": [{"dimension": "created_at", "granularity": "year"}],
                "measures": [{"formula": "rev:sum"}],
            }))
            assert _by_bucket(resp.data) == _YEAR_SUMS


# ---------------------------------------------------------------------------
# Hand-set granularity — executed (bare) and plan-time (dotted join path).
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("backend", BACKENDS)
class TestHandSetExecuted:
    async def test_handset_day_rejects_hour_and_executes(self, backend: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine(backend, tmp, gran=TG.DAY)
            finer = SlayerQuery.model_validate({
                "source_model": "orders",
                "time_dimensions": [{"dimension": "created_at", "granularity": "hour"}],
                "measures": [{"formula": "amount:sum", "name": "rev"}],
            })
            with pytest.raises(TimeDimensionColumnError):
                await engine.execute(query=finer, dry_run=True)
            day = await engine.execute(query=SlayerQuery.model_validate({
                "source_model": "orders",
                "time_dimensions": [{"dimension": "created_at", "granularity": "day"}],
                "measures": [{"formula": "amount:sum", "name": "rev"}],
            }))
            assert len(day.data) == len(_SEED)
            month = await engine.execute(query=SlayerQuery.model_validate({
                "source_model": "orders",
                "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
                "measures": [{"formula": "amount:sum", "name": "rev"}],
            }))
            assert _by_bucket(month.data) == _MONTH_SUMS


class TestHandSetJoinPath:
    async def test_dotted_handset_rejects_finer_and_binds_coarser(self) -> None:
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP, granularity=TG.MONTH),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        engine = await _dryrun_engine([orders, customers])
        finer = SlayerQuery.model_validate({
            "source_model": "orders",
            "time_dimensions": [{"dimension": "customers.signup_at", "granularity": "day"}],
            "measures": [{"formula": "*:count", "name": "n"}],
        })
        with pytest.raises(TimeDimensionColumnError):
            await engine.execute(query=finer, dry_run=True)
        await engine.execute(query=SlayerQuery.model_validate({
            "source_model": "orders",
            "time_dimensions": [{"dimension": "customers.signup_at", "granularity": "year"}],
            "measures": [{"formula": "*:count", "name": "n"}],
        }), dry_run=True)


# ---------------------------------------------------------------------------
# Sibling-stage stand-ins carry the bucket (task 2.5).
# ---------------------------------------------------------------------------
class TestSiblingStandIns:
    def test_synthetic_model_carries_granularity(self) -> None:
        schema = StageSchema(relation_name="s1", columns=[
            StageColumn(name="created_at", sql_alias="created_at", public_alias="created_at",
                        type=DataType.TIMESTAMP, granularity=TG.MONTH),
            StageColumn(name="rev", sql_alias="rev", public_alias="rev", type=DataType.DOUBLE),
        ])
        synth = model_from_stage_schema(name="s1", schema=schema, data_source="ds")
        assert _col(synth, "created_at").granularity == TG.MONTH
        assert _col(synth, "rev").granularity is None

    def test_dotted_ref_to_sibling_standin_reads_the_bucket(self) -> None:
        # A stage joining a sibling resolves it as a synthetic referenced model;
        # a dotted time dimension over it reads the stamped granularity.
        schema = StageSchema(relation_name="s1", columns=[
            StageColumn(name="created_at", sql_alias="created_at", public_alias="created_at",
                        type=DataType.TIMESTAMP, granularity=TG.MONTH),
        ])
        sibling = model_from_stage_schema(name="s1", schema=schema, data_source="ds")
        host = SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            columns=[Column(name="id", type=DataType.DOUBLE, primary_key=True)],
            joins=[ModelJoin(target_model="s1", join_pairs=[["id", "created_at"]])],
        )
        bundle = ResolvedSourceBundle(dialect="postgres", source_model=host, referenced_models=[sibling])
        bound = bind_time_dimension(
            TimeDimension(dimension=ColumnRef(name="s1.created_at"), granularity=TG.DAY),
            scope=ModelScope(source_model=host), bundle=bundle,
        )
        assert bound.upstream_granularity == TG.MONTH
        with pytest.raises(TimeDimensionColumnError):
            check_time_dimension_column(
                name="s1.created_at", column_type=bound.column_type,
                upstream_granularity=bound.upstream_granularity, requested_granularity=TG.DAY,
            )
        check_time_dimension_column(  # month re-binds
            name="s1.created_at", column_type=bound.column_type,
            upstream_granularity=bound.upstream_granularity, requested_granularity=TG.MONTH,
        )

    async def test_model_extension_over_bucketed_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            s1 = {"name": "s1", "source_model": "orders",
                  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
                  "measures": [{"formula": "amount:sum", "name": "rev"}]}
            finer = {"source_model": {"source_name": "s1"},
                     "time_dimensions": [{"dimension": "created_at", "granularity": "day"}],
                     "measures": [{"formula": "rev:sum"}]}
            with pytest.raises(TimeDimensionColumnError):
                await engine.execute(query=[s1, finer], dry_run=True)
            coarser = {"source_model": {"source_name": "s1"},
                       "time_dimensions": [{"dimension": "created_at", "granularity": "year"}],
                       "measures": [{"formula": "rev:sum"}]}
            resp = await engine.execute(query=[s1, coarser])
            assert _by_bucket(resp.data) == _YEAR_SUMS

    async def test_stage_join_to_bucketed_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            s1 = {"name": "s1", "source_model": "orders", "dimensions": ["id"],
                  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
                  "measures": [{"formula": "amount:sum", "name": "rev"}]}

            def outer(gran: str) -> dict:
                return {
                    "source_model": {"source_name": "orders",
                                     "joins": [{"target_model": "s1", "join_pairs": [["id", "id"]]}]},
                    "dimensions": ["id"],
                    "time_dimensions": [{"dimension": "s1.created_at", "granularity": gran}],
                    "measures": [{"formula": "amount:sum", "name": "x"}],
                }
            finer = outer("day")
            with pytest.raises(TimeDimensionColumnError):
                await engine.execute(query=[s1, finer], dry_run=True)
            await engine.execute(query=[s1, outer("month")], dry_run=True)  # binds


# ---------------------------------------------------------------------------
# One message for a bucketed stage column and a bucketed model column (task 2.3).
# ---------------------------------------------------------------------------
class TestUnifiedMessage:
    async def test_stage_and_model_columns_share_one_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            s1 = {"name": "s1", "source_model": "orders",
                  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
                  "measures": [{"formula": "amount:sum", "name": "rev"}]}
            stage_outer = {"source_model": "s1",
                           "time_dimensions": [{"dimension": "created_at", "granularity": "day"}],
                           "measures": [{"formula": "rev:sum"}]}
            with pytest.raises(TimeDimensionColumnError) as stage_exc:
                await engine.execute(query=[s1, stage_outer], dry_run=True)
            await engine.create_model_from_query(
                query={"source_model": "orders",
                       "measures": [{"formula": "amount:sum", "name": "rev"}],
                       "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]},
                name="monthly",
            )
            model_finer = SlayerQuery.model_validate({
                "source_model": "monthly",
                "time_dimensions": [{"dimension": "created_at", "granularity": "day"}],
                "measures": [{"formula": "rev:sum"}]})
            with pytest.raises(TimeDimensionColumnError) as model_exc:
                await engine.execute(query=model_finer, dry_run=True)
            assert str(stage_exc.value) == str(model_exc.value)
            assert "upstream stage" not in str(stage_exc.value)


# ---------------------------------------------------------------------------
# Ingestion never sets granularity (decision 4 — green regression pin).
# ---------------------------------------------------------------------------
class TestIngestion:
    async def test_ingestion_leaves_granularity_unset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "live.db")
            with transaction(db) as conn:
                conn.executescript(
                    "CREATE TABLE events (id INTEGER PRIMARY KEY, created_at TEXT, label TEXT);"
                    "INSERT INTO events VALUES (1, '2024-01-01', 'a');"
                )
            storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
            ds = DatasourceConfig(name="ds", type="sqlite", database=db)
            await storage.save_datasource(ds)
            await ingest_datasource_idempotent(datasource=ds, storage=storage)
            names = await storage.list_models("ds")
            assert names
            for n in names:
                model = await storage.get_model(n, data_source="ds")
                assert model is not None
                assert all(getattr(c, "granularity", None) is None for c in model.columns)


# ---------------------------------------------------------------------------
# MCP model tools round-trip the granularity (task 2.6).
# ---------------------------------------------------------------------------
class TestMCPSurfaces:
    async def _server(self, tmp: str):
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=os.path.join(tmp, "db.sqlite")))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            columns=[Column(name="created_at", type=DataType.TIMESTAMP),
                     Column(name="note", type=DataType.TEXT)],
        ))
        return create_mcp_server(storage=storage, _seed_help=False), storage

    async def test_create_model_accepts_granularity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=os.path.join(tmp, "db.sqlite")))
            server = create_mcp_server(storage=storage, _seed_help=False)
            await _call(server, name="create_model", arguments={
                "name": "events", "data_source": "ds", "sql_table": "events",
                "columns": [{"name": "created_at", "type": "time", "granularity": "month"}]})
            assert _col(await storage.get_model("events"), "created_at").granularity == TG.MONTH

    async def test_edit_model_sets_preserves_and_clears(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, storage = await self._server(tmp)
            await _call(server, name="edit_model", arguments={
                "model_name": "orders", "columns": [{"name": "created_at", "granularity": "month"}]})
            assert _col(await storage.get_model("orders"), "created_at").granularity == TG.MONTH
            await _call(server, name="edit_model", arguments={
                "model_name": "orders", "columns": [{"name": "created_at", "description": "when"}]})
            assert _col(await storage.get_model("orders"), "created_at").granularity == TG.MONTH
            await _call(server, name="edit_model", arguments={
                "model_name": "orders", "columns": [{"name": "created_at", "granularity": None}]})
            assert _col(await storage.get_model("orders"), "created_at").granularity is None

    async def test_edit_model_rejects_granularity_on_non_temporal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, storage = await self._server(tmp)
            result = await _call(server, name="edit_model", arguments={
                "model_name": "orders", "columns": [{"name": "note", "granularity": "month"}]})
            assert "note" in result
            assert "month" in result
            assert "TEXT" in result
            assert getattr(_col(await storage.get_model("orders"), "note"), "granularity", None) is None

    async def test_inspect_model_shows_set_granularity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, _storage = await self._server(tmp)
            await _call(server, name="edit_model", arguments={
                "model_name": "orders", "columns": [{"name": "created_at", "granularity": "month"}]})
            out = await _call(server, name="inspect_model", arguments={
                "model_name": "orders", "format": "json"})
            cols = {c["name"]: c for c in json.loads(out)["columns"]}
            assert cols["created_at"].get("granularity") == "month"
            assert "granularity" not in cols["note"]
