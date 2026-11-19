"""A model column carries a time-bucket ``granularity``, and a finer /
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
from slayer.core.errors import TimeDimensionFieldError
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.core.scope import DatasetScope, StageField, StageSchema
from slayer.engine.binding import bind_time_dimension
from slayer.engine.elaborate_env import check_time_dimension_field
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    dataset_from_stage_schema,
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


def _orders_columns(*, created_at_granularity=None) -> list[DatasetField]:
    return [
        DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
        DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
        DatasetField(name="region", sql="region", datatype=DataType.STRING),
        DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME,
               granularity=created_at_granularity),
        DatasetField(name="shipped_at", sql="shipped_at", datatype=DataType.DATETIME),
    ]


def _orders_dataset(*, gran=None, data_source="ds") -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source=data_source,
        default_time_dimension="created_at",
        fields=_orders_columns(created_at_granularity=gran),
    )


async def _seeded_engine(backend: str, tmp: str, *, gran=None) -> SlayerQueryEngine:
    return await make_engine(
        backend, base_dir=os.path.join(tmp, "store"),
        db_path=os.path.join(tmp, f"t.{backend}"),
        tables=[orders_table_spec(_SEED)], datasets=[_orders_dataset(gran=gran)],
    )


async def _dryrun_engine(datasets: list[SlayerDataset]) -> SlayerQueryEngine:
    """Storage + engine with no real DB — enough for plan-time binding/checking."""
    storage = YAMLStorage(base_dir=tempfile.mkdtemp())
    await storage.save_datasource(DatasourceConfig(name=datasets[0].data_source, type="sqlite"))
    for model in datasets:
        await storage.save_dataset(model)
    return SlayerQueryEngine(storage=storage)


def _by_bucket(rows: list[dict]) -> dict:
    """Map ``created_at`` bucket (first 10 chars) → the row's single measure value."""
    out: dict = {}
    for r in rows:
        date_k = next(k for k in r if k.endswith("created_at"))
        val_k = next(k for k in r if k != date_k)
        out[date_str(r[date_k])] = r[val_k]
    return out


def _col(dataset: SlayerDataset | None, name: str) -> DatasetField:
    assert dataset is not None
    return next(c for c in dataset.fields if c.name == name)


async def _call(server, *, name: str, arguments: dict) -> str:
    blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return blocks[0].text


# ---------------------------------------------------------------------------
# Construction — Column.granularity field + non-temporal validator (task 2.1).
# ---------------------------------------------------------------------------
class TestColumnConstruction:
    @pytest.mark.parametrize("t", [DataType.DATETIME, DataType.DATE])
    def test_temporal_column_accepts_granularity(self, t) -> None:
        assert DatasetField(name="created_at", datatype=t, granularity=TG.MONTH).granularity == TG.MONTH

    def test_string_column_rejects_granularity(self) -> None:
        with pytest.raises(ValueError) as exc:
            DatasetField(name="created_at", datatype=DataType.STRING, granularity=TG.MONTH)
        msg = str(exc.value)
        assert "created_at" in msg
        assert "month" in msg
        assert "String" in msg

    def test_default_type_rejects_granularity(self) -> None:
        with pytest.raises(ValueError) as exc:
            DatasetField(name="ts", granularity=TG.MONTH)
        msg = str(exc.value)
        assert "ts" in msg
        assert "month" in msg
        assert "String" in msg

    def test_dict_string_spelling_accepted(self) -> None:
        c = DatasetField.model_validate({"name": "created_at", "datatype": "time", "granularity": "month"})
        assert c.granularity == TG.MONTH

    def test_model_dump_roundtrips_the_enum(self) -> None:
        c = DatasetField(name="created_at", datatype=DataType.DATETIME, granularity=TG.MONTH)
        assert DatasetField.model_validate(c.model_dump()).granularity == TG.MONTH
        assert DatasetField.model_validate(c.model_dump(mode="json")).granularity == TG.MONTH

    async def test_yaml_save_load_roundtrip(self) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite"))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="ds",
            fields=[DatasetField(name="created_at", datatype=DataType.DATETIME, granularity=TG.MONTH)],
        ))
        loaded = await storage.get_dataset("orders")
        assert _col(loaded, "created_at").granularity == TG.MONTH


# ---------------------------------------------------------------------------
# Binder + checker — the ModelScope arm reads Column.granularity (task 2.2);
# the re-bucketing message is generalised, no "upstream stage" (task 2.3).
# ---------------------------------------------------------------------------
class TestBinderAndChecker:
    def _orders_customers(self, *, gran_o=None, gran_c=None):
        customers = SlayerDataset(
            name="customers", sql_table="customers", data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="signup_at", datatype=DataType.DATETIME, granularity=gran_c),
            ],
        )
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="created_at", datatype=DataType.DATETIME, granularity=gran_o),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
        return orders, customers

    def test_bare_column_returns_handset_granularity(self) -> None:
        orders, customers = self._orders_customers(gran_o=TG.DAY)
        bound = bind_time_dimension(
            TimeDimension(field="created_at", granularity=TG.DAY),
            scope=DatasetScope(population=orders),
            bundle=ResolvedSourceBundle(dialect="postgres", population=orders, referenced_datasets=[customers]),
        )
        assert bound.bucketed_leaves == (("created_at", TG.DAY),)
        assert bound.field_type == DataType.DATETIME

    def test_dotted_join_path_returns_handset_granularity(self) -> None:
        orders, customers = self._orders_customers(gran_c=TG.MONTH)
        bound = bind_time_dimension(
            TimeDimension(field="customers.signup_at", granularity=TG.YEAR),
            scope=DatasetScope(population=orders),
            bundle=ResolvedSourceBundle(dialect="postgres", population=orders, referenced_datasets=[customers]),
        )
        assert bound.bucketed_leaves == (("customers.signup_at", TG.MONTH),)

    def test_rebucket_message_is_generalised(self) -> None:
        with pytest.raises(TimeDimensionFieldError) as exc:
            check_time_dimension_field(
                name="monthly.created_at", field_type=DataType.DATETIME,
                bucketed_leaves=(("monthly.created_at", TG.MONTH),), requested_granularity=TG.DAY,
            )
        msg = str(exc.value)
        assert "month" in msg
        assert "day" in msg
        assert "already bucketed" in msg
        assert "bucket the raw field instead" in msg
        assert "nesting-coarser" in msg
        assert "upstream stage" not in msg


# ---------------------------------------------------------------------------
# Query-backed cache stamping (task 2.4) — cache columns + persistence.
# ---------------------------------------------------------------------------
class TestQueryBackedCache:
    async def test_bucketed_column_stamped_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            model = await engine.create_dataset_from_query(
                query={"population": "orders",
                       "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
                       "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]},
                name="monthly",
            )
            assert _col(model, "created_at").granularity == TG.MONTH
            assert _col(model, "created_at").datatype in (DataType.DATE, DataType.DATETIME)
            assert _col(model, "rev").granularity is None
            reloaded = await engine.storage.get_dataset("monthly")
            assert _col(reloaded, "created_at").granularity == TG.MONTH
            assert _col(reloaded, "rev").granularity is None

    async def test_unbucketed_columns_carry_no_granularity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            plain = await engine.create_dataset_from_query(
                query={"population": "orders", "dimensions": ["orders.created_at"],
                       "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]},
                name="raw_ts",
            )
            assert all(c.granularity is None for c in plain.fields)
            # A time dimension over the unbucketed cached column still binds.
            await engine.execute(query=SlayerQuery.model_validate({
                "population": "raw_ts",
                "time_dimensions": [{"field": "raw_ts.created_at", "granularity": "day"}],
                "metrics": [{"metric": "sum(raw_ts.rev)"}],
            }), dry_run=True)
            maxed = await engine.create_dataset_from_query(
                query={"population": "orders", "dimensions": ["orders.region"],
                       "metrics": [{"metric": "max(orders.created_at)"}]},
                name="maxed",
            )
            assert all(c.granularity is None for c in maxed.fields)
            # A time dimension over the unbucketed aggregate output binds.
            agg_col = next(c.name for c in maxed.fields if c.name != "region")
            await engine.execute(query=SlayerQuery.model_validate({
                "population": "maxed",
                "time_dimensions": [{"field": agg_col, "granularity": "day"}],
                "metrics": [{"metric": "count(maxed.*)"}],
            }), dry_run=True)
            # A computed dimension is cached without a granularity.
            band = "CASE WHEN sum(orders.amount, partition_by=orders.region) > 100 THEN 'big' ELSE 'small' END"
            banded = await engine.create_dataset_from_query(
                query={"population": "orders",
                       "dimensions": [{"field": band, "alias": "size_band"}],
                       "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]},
                name="banded",
            )
            assert all(c.granularity is None for c in banded.fields)

    async def test_nested_query_backed_models_compose(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            await engine.create_dataset_from_query(
                query={"population": "orders",
                       "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
                       "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]},
                name="monthly",
            )
            yearly = await engine.create_dataset_from_query(
                query={"population": "monthly",
                       "metrics": [{"metric": "sum(monthly.rev)"}],
                       "time_dimensions": [{"field": "monthly.created_at", "granularity": "year"}]},
                name="yearly",
            )
            assert _col(yearly, "created_at").granularity == TG.YEAR
            finer = SlayerQuery.model_validate({
                "population": "yearly",
                "time_dimensions": [{"field": "yearly.created_at", "granularity": "day"}],
                "metrics": [{"metric": "sum(yearly.rev_sum)"}],
            })
            with pytest.raises(TimeDimensionFieldError):
                await engine.execute(query=finer, dry_run=True)


# ---------------------------------------------------------------------------
# Executed on SQLite + DuckDB — the issue repro and its valid neighbours.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("backend", BACKENDS)
class TestQueryBackedExecuted:
    async def _monthly_engine(self, backend: str, tmp: str) -> SlayerQueryEngine:
        engine = await _seeded_engine(backend, tmp)
        await engine.create_dataset_from_query(
            query={"population": "orders",
                   "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
                   "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]},
            name="monthly",
        )
        return engine

    async def test_issue_repro_finer_rejected(self, backend: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await self._monthly_engine(backend, tmp)
            finer = SlayerQuery.model_validate({
                "population": "monthly",
                "time_dimensions": [{"field": "monthly.created_at", "granularity": "day"}],
                "metrics": [{"metric": "sum(monthly.rev)"}],
            })
            with pytest.raises(TimeDimensionFieldError):
                await engine.execute(query=finer)

    async def test_same_granularity_matches_direct(self, backend: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await self._monthly_engine(backend, tmp)
            resp = await engine.execute(query=SlayerQuery.model_validate({
                "population": "monthly",
                "time_dimensions": [{"field": "monthly.created_at", "granularity": "month"}],
                "metrics": [{"metric": "sum(monthly.rev)"}],
            }))
            assert _by_bucket(resp.data) == _MONTH_SUMS

    async def test_coarser_year_aggregates(self, backend: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await self._monthly_engine(backend, tmp)
            resp = await engine.execute(query=SlayerQuery.model_validate({
                "population": "monthly",
                "time_dimensions": [{"field": "monthly.created_at", "granularity": "year"}],
                "metrics": [{"metric": "sum(monthly.rev)"}],
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
                "population": "orders",
                "time_dimensions": [{"field": "orders.created_at", "granularity": "hour"}],
                "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            })
            with pytest.raises(TimeDimensionFieldError):
                await engine.execute(query=finer, dry_run=True)
            day = await engine.execute(query=SlayerQuery.model_validate({
                "population": "orders",
                "time_dimensions": [{"field": "orders.created_at", "granularity": "day"}],
                "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            }))
            assert len(day.data) == len(_SEED)
            month = await engine.execute(query=SlayerQuery.model_validate({
                "population": "orders",
                "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
                "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            }))
            assert _by_bucket(month.data) == _MONTH_SUMS


class TestHandSetJoinPath:
    async def test_dotted_handset_rejects_finer_and_binds_coarser(self) -> None:
        customers = SlayerDataset(
            name="customers", sql_table="customers", data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="signup_at", sql="signup_at", datatype=DataType.DATETIME, granularity=TG.MONTH),
            ],
        )
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
        engine = await _dryrun_engine([orders, customers])
        finer = SlayerQuery.model_validate({
            "population": "orders",
            "time_dimensions": [{"field": "customers.signup_at", "granularity": "day"}],
            "metrics": [{"metric": "count(orders.*)", "alias": "n"}],
        })
        with pytest.raises(TimeDimensionFieldError):
            await engine.execute(query=finer, dry_run=True)
        await engine.execute(query=SlayerQuery.model_validate({
            "population": "orders",
            "time_dimensions": [{"field": "customers.signup_at", "granularity": "year"}],
            "metrics": [{"metric": "count(orders.*)", "alias": "n"}],
        }), dry_run=True)


# ---------------------------------------------------------------------------
# Sibling-stage stand-ins carry the bucket (task 2.5).
# ---------------------------------------------------------------------------
class TestSiblingStandIns:
    def test_synthetic_model_carries_granularity(self) -> None:
        schema = StageSchema(relation_name="s1", fields=[
            StageField(name="created_at", sql_alias="created_at", public_alias="created_at",
                        datatype=DataType.DATETIME, granularity=TG.MONTH),
            StageField(name="rev", sql_alias="rev", public_alias="rev", datatype=DataType.FLOAT),
        ])
        synth = dataset_from_stage_schema(name="s1", schema=schema, data_source="ds")
        assert _col(synth, "created_at").granularity == TG.MONTH
        assert _col(synth, "rev").granularity is None

    def test_dotted_ref_to_sibling_standin_reads_the_bucket(self) -> None:
        # A stage joining a sibling resolves it as a synthetic referenced model;
        # a dotted time dimension over it reads the stamped granularity.
        schema = StageSchema(relation_name="s1", fields=[
            StageField(name="created_at", sql_alias="created_at", public_alias="created_at",
                        datatype=DataType.DATETIME, granularity=TG.MONTH),
        ])
        sibling = dataset_from_stage_schema(name="s1", schema=schema, data_source="ds")
        host = SlayerDataset(
            name="orders", sql_table="orders", data_source="ds",
            fields=[DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True)],
            relationships=[DatasetRelationship(to="s1", from_columns=["id"], to_columns=["created_at"])],
        )
        bundle = ResolvedSourceBundle(dialect="postgres", population=host, referenced_datasets=[sibling])
        bound = bind_time_dimension(
            TimeDimension(field="s1.created_at", granularity=TG.DAY),
            scope=DatasetScope(population=host), bundle=bundle,
        )
        assert bound.bucketed_leaves == (("s1.created_at", TG.MONTH),)
        with pytest.raises(TimeDimensionFieldError):
            check_time_dimension_field(
                name="s1.created_at", field_type=bound.field_type,
                bucketed_leaves=bound.bucketed_leaves, requested_granularity=TG.DAY,
            )
        check_time_dimension_field(  # month re-binds
            name="s1.created_at", field_type=bound.field_type,
            bucketed_leaves=bound.bucketed_leaves, requested_granularity=TG.MONTH,
        )

    async def test_model_extension_over_bucketed_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            s1 = {"name": "s1", "population": "orders",
                  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
                  "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]}
            finer = {"population": "s1", "extensions": [{"dataset": "s1"}],
                     "time_dimensions": [{"field": "s1.created_at", "granularity": "day"}],
                     "metrics": [{"metric": "sum(s1.rev)"}]}
            with pytest.raises(TimeDimensionFieldError):
                await engine.execute(query=[s1, finer], dry_run=True)
            coarser = {"population": "s1", "extensions": [{"dataset": "s1"}],
                       "time_dimensions": [{"field": "s1.created_at", "granularity": "year"}],
                       "metrics": [{"metric": "sum(s1.rev)"}]}
            resp = await engine.execute(query=[s1, coarser])
            assert _by_bucket(resp.data) == _YEAR_SUMS

    async def test_stage_join_to_bucketed_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            s1 = {"name": "s1", "population": "orders", "dimensions": ["orders.id"],
                  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
                  "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]}

            def outer(gran: str) -> dict:
                return {
                    "population": "orders",
                    "extensions": [{"dataset": "orders",
                                    "relationships": [{"to": "s1", "from_columns": ["id"], "to_columns": ["id"]}]}],
                    "dimensions": ["orders.id"],
                    "time_dimensions": [{"field": "s1.created_at", "granularity": gran}],
                    "metrics": [{"metric": "sum(orders.amount)", "alias": "x"}],
                }
            finer = outer("day")
            with pytest.raises(TimeDimensionFieldError):
                await engine.execute(query=[s1, finer], dry_run=True)
            await engine.execute(query=[s1, outer("month")], dry_run=True)  # binds


# ---------------------------------------------------------------------------
# One message for a bucketed stage column and a bucketed model column (task 2.3).
# ---------------------------------------------------------------------------
class TestUnifiedMessage:
    async def test_stage_and_model_columns_share_one_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            engine = await _seeded_engine("sqlite", tmp)
            s1 = {"name": "s1", "population": "orders",
                  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
                  "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]}
            stage_outer = {"population": "s1",
                           "time_dimensions": [{"field": "s1.created_at", "granularity": "day"}],
                           "metrics": [{"metric": "sum(s1.rev)"}]}
            with pytest.raises(TimeDimensionFieldError) as stage_exc:
                await engine.execute(query=[s1, stage_outer], dry_run=True)
            await engine.create_dataset_from_query(
                query={"population": "orders",
                       "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
                       "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]},
                name="monthly",
            )
            model_finer = SlayerQuery.model_validate({
                "population": "monthly",
                "time_dimensions": [{"field": "monthly.created_at", "granularity": "day"}],
                "metrics": [{"metric": "sum(monthly.rev)"}]})
            with pytest.raises(TimeDimensionFieldError) as model_exc:
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
            client = SlayerSQLClient(datasource=ds)
            try:
                await ingest_datasource_idempotent(datasource=ds, storage=storage, client=client)
            finally:
                await client.aclose()
                client.close()
            names = await storage.list_datasets("ds")
            assert names
            for n in names:
                model = await storage.get_dataset(n, data_source="ds")
                assert model is not None
                assert all(getattr(c, "granularity", None) is None for c in model.fields)


# ---------------------------------------------------------------------------
# MCP model tools round-trip the granularity (task 2.6).
# ---------------------------------------------------------------------------
class TestMCPSurfaces:
    async def _server(self, tmp: str):
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=os.path.join(tmp, "db.sqlite")))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="ds",
            fields=[DatasetField(name="created_at", datatype=DataType.DATETIME),
                     DatasetField(name="note", datatype=DataType.STRING)],
        ))
        return create_mcp_server(storage=storage, _seed_help=False), storage

    async def test_create_model_accepts_granularity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=os.path.join(tmp, "db.sqlite")))
            server = create_mcp_server(storage=storage, _seed_help=False)
            await _call(server, name="create_dataset", arguments={
                "name": "events", "data_source": "ds", "sql_table": "events",
                "fields": [{"name": "created_at", "datatype": "time", "granularity": "month"}]})
            assert _col(await storage.get_dataset("events"), "created_at").granularity == TG.MONTH

    async def test_edit_model_sets_preserves_and_clears(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, storage = await self._server(tmp)
            await _call(server, name="edit_dataset", arguments={
                "dataset_name": "orders", "fields": [{"name": "created_at", "granularity": "month"}]})
            assert _col(await storage.get_dataset("orders"), "created_at").granularity == TG.MONTH
            await _call(server, name="edit_dataset", arguments={
                "dataset_name": "orders", "fields": [{"name": "created_at", "description": "when"}]})
            assert _col(await storage.get_dataset("orders"), "created_at").granularity == TG.MONTH
            await _call(server, name="edit_dataset", arguments={
                "dataset_name": "orders", "fields": [{"name": "created_at", "granularity": None}]})
            assert _col(await storage.get_dataset("orders"), "created_at").granularity is None

    async def test_edit_model_rejects_granularity_on_non_temporal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, storage = await self._server(tmp)
            result = await _call(server, name="edit_dataset", arguments={
                "dataset_name": "orders", "fields": [{"name": "note", "granularity": "month"}]})
            assert "note" in result
            assert "month" in result
            assert "String" in result
            assert getattr(_col(await storage.get_dataset("orders"), "note"), "granularity", None) is None

    async def test_inspect_model_shows_set_granularity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            server, _storage = await self._server(tmp)
            await _call(server, name="edit_dataset", arguments={
                "dataset_name": "orders", "fields": [{"name": "created_at", "granularity": "month"}]})
            out = await _call(server, name="inspect", arguments={
                "entity_type": "dataset", "reference": "orders", "compact": False, "format": "json"})
            cols = {c["name"]: c for c in json.loads(out)["fields"]}
            assert cols["created_at"].get("granularity") == "month"
            assert "granularity" not in cols["note"]
