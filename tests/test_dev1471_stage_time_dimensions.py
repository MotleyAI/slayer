"""Time dimensions bind on stage datasets: binder facts, checker re-bucketing rules, and executed cross-stage values on SQLite + DuckDB."""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import (
    IllegalScopeReferenceError,
    TimeDimensionFieldError,
    UnknownReferenceError,
)
from slayer.core.keys import FieldKey, TimeTruncKey
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.core.scope import StageField, StageSchema
from slayer.engine.binding import bind_time_dimension
from slayer.engine.elaborate_env import check_time_dimension_field
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1471_fixtures import (
    BACKENDS,
    date_str,
    make_engine,
    orders_model,
    orders_table_spec,
    region_chain_models,
    region_chain_tables,
)

TG = TimeGranularity

# --- seeds (id, customer_id, amount, region, created_at, shipped_at) ---
_MONTHLY = [
    (1, 100, 100.0, "W", "2024-11-10", None),
    (2, 100, 200.0, "W", "2024-12-05", None),
    (3, 101, 300.0, "E", "2025-01-15", None),
    (4, 101, 50.0, "E", "2025-01-20", None),
    (5, 102, 500.0, "N", "2025-02-10", None),
]
_COHORT = [
    (1, 100, 10.0, "W", "2024-12-05", None),
    (2, 100, 10.0, "W", "2025-03-20", None),
    (3, 101, 10.0, "E", "2025-02-10", None),
    (4, 102, 10.0, "N", "2025-02-25", None),
    (5, 103, 10.0, "S", "2024-11-15", None),
    (6, 104, 10.0, "W", "2025-03-01", None),
]
_GRAIN = [
    (1, 100, 100.0, "W", "2025-01-10", None),
    (2, 101, 50.0, "W", "2025-01-20", None),
    (3, 102, 200.0, "W", "2025-02-15", None),
    (4, 103, 300.0, "E", "2025-01-25", None),
]


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(dialect="postgres", population=orders_model(), referenced_datasets=[])


def _stage(*columns: StageField) -> StageSchema:
    return StageSchema(relation_name="s1", fields=list(columns))


async def _dryrun_engine(models: list[SlayerDataset]) -> SlayerQueryEngine:
    """Storage + engine with no real DB — enough for plan-time binding/checking."""
    storage = YAMLStorage(base_dir=tempfile.mkdtemp())
    await storage.save_datasource(DatasourceConfig(name=models[0].data_source, type="sqlite"))
    for model in models:
        await storage.save_dataset(model)
    return SlayerQueryEngine(storage=storage)


# ---------------------------------------------------------------------------
# Binder level — a stage TimeDimension resolves against the StageSchema.
# ---------------------------------------------------------------------------
class TestStageBinder:
    def test_bucketed_stage_column_binds_with_facts(self) -> None:
        stage = _stage(StageField(
            name="created_at", sql_alias="created_at",
            datatype=DataType.TIMESTAMP, granularity=TG.MONTH,
        ))
        td = TimeDimension(field="created_at", granularity=TG.YEAR)
        bound = bind_time_dimension(td, scope=stage, bundle=_bundle())
        assert isinstance(bound.bound.value_key, TimeTruncKey)
        assert bound.bound.value_key.source == FieldKey(path=(), leaf="created_at")
        assert bound.bound.value_key.granularity == "year"
        assert bound.field_type == DataType.TIMESTAMP
        assert bound.bucketed_leaves == (("created_at", TG.MONTH),)

    def test_raw_stage_column_has_no_upstream_granularity(self) -> None:
        stage = _stage(StageField(
            name="created_at", sql_alias="created_at", datatype=DataType.TIMESTAMP,
        ))
        td = TimeDimension(field="created_at", granularity=TG.DAY)
        bound = bind_time_dimension(td, scope=stage, bundle=_bundle())
        assert bound.bucketed_leaves == ()
        assert bound.field_type == DataType.TIMESTAMP

    def test_unknown_stage_column_raises_unknown_reference(self) -> None:
        stage = _stage(StageField(name="created_at", sql_alias="created_at", datatype=DataType.TIMESTAMP))
        td = TimeDimension(field="not_a_column", granularity=TG.MONTH)
        bundle = _bundle()
        with pytest.raises(UnknownReferenceError, match="created_at"):  # lists the stage's columns
            bind_time_dimension(td, scope=stage, bundle=bundle)

    def test_dotted_name_on_stage_raises_illegal_scope(self) -> None:
        stage = _stage(StageField(name="created_at", sql_alias="created_at", datatype=DataType.TIMESTAMP))
        td = TimeDimension(field="customers.created_at", granularity=TG.MONTH)
        bundle = _bundle()
        with pytest.raises(IllegalScopeReferenceError):
            bind_time_dimension(td, scope=stage, bundle=bundle)


# ---------------------------------------------------------------------------
# Checker level — the re-bucketing type rule (check_time_dimension_column).
# ---------------------------------------------------------------------------
class TestReBucketingChecker:
    def test_non_temporal_column_rejected(self) -> None:
        with pytest.raises(TimeDimensionFieldError) as exc:
            check_time_dimension_field(
                name="s1.region", field_type=DataType.TEXT,
                requested_granularity=TG.MONTH,
            )
        assert "temporal" in str(exc.value)
        assert "TEXT" in str(exc.value)

    def test_untyped_column_fails_closed(self) -> None:
        with pytest.raises(TimeDimensionFieldError) as exc:
            check_time_dimension_field(
                name="s1.mystery", field_type=None,
                requested_granularity=TG.MONTH,
            )
        assert "temporal" in str(exc.value)

    def test_finer_granularity_rejected(self) -> None:
        with pytest.raises(TimeDimensionFieldError) as exc:
            check_time_dimension_field(
                name="s1.created_at", field_type=DataType.TIMESTAMP,
                bucketed_leaves=(("s1.created_at", TG.MONTH),), requested_granularity=TG.DAY,
            )
        msg = str(exc.value)
        assert "month" in msg
        assert "day" in msg
        assert "coarser" in msg  # the mandated remedy

    @pytest.mark.parametrize(
        "upstream,requested",
        [(TG.MONTH, TG.WEEK), (TG.WEEK, TG.MONTH)],
    )
    def test_non_nesting_rejected_both_ways(self, upstream, requested) -> None:
        with pytest.raises(TimeDimensionFieldError) as exc:
            check_time_dimension_field(
                name="s1.created_at", field_type=DataType.TIMESTAMP,
                bucketed_leaves=(("s1.created_at", upstream),), requested_granularity=requested,
            )
        msg = str(exc.value)
        assert upstream.value in msg
        assert requested.value in msg
        assert "coarser" in msg  # the mandated remedy

    @pytest.mark.parametrize(
        "upstream,requested",
        [(TG.MONTH, TG.MONTH), (TG.MONTH, TG.YEAR), (TG.DAY, TG.WEEK), (None, TG.DAY)],
    )
    def test_valid_rebucketing_passes(self, upstream, requested) -> None:
        # Same, nesting-coarser, or any granularity on an un-truncated column.
        check_time_dimension_field(
            name="s1.created_at", field_type=DataType.TIMESTAMP,
            bucketed_leaves=() if upstream is None else (("s1.created_at", upstream),),
            requested_granularity=requested,
        )


# ---------------------------------------------------------------------------
# End-to-end binder / checker errors on a real cross-stage query (plan time).
# ---------------------------------------------------------------------------
class TestStageBindingErrorsEndToEnd:
    async def test_finer_granularity_end_to_end(self) -> None:
        engine = await _dryrun_engine([orders_model()])
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.DAY)],
            "metrics": [{"expression": "sum(rev)", "name": "rev"}],
        })
        with pytest.raises(TimeDimensionFieldError):
            await engine.execute(query=[inner, outer], dry_run=True)

    async def test_finer_rebucket_after_passthrough_end_to_end(self) -> None:
        # s1 buckets month, s2 re-projects the bucket as a plain dimension (which
        # drops its type from the stage schema), s3 re-buckets finer: the untyped
        # fail-closed rule rejects it, so a silent wrong re-bucket never lands.
        engine = await _dryrun_engine([orders_model()])
        s1 = SlayerQuery.model_validate({
            "name": "s1", "population": "orders",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        s2 = SlayerQuery.model_validate({
            "name": "s2", "population": "s1", "dimensions": ["created_at"],
            "metrics": [{"expression": "sum(rev)"}],
        })
        s3 = SlayerQuery.model_validate({
            "population": "s2",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.DAY)],
            "metrics": [{"expression": "sum(rev_sum)"}],
        })
        with pytest.raises(TimeDimensionFieldError):
            await engine.execute(query=[s1, s2, s3], dry_run=True)

    async def test_non_temporal_column_end_to_end(self) -> None:
        engine = await _dryrun_engine([orders_model()])
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders", "dimensions": ["region"],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "time_dimensions": [TimeDimension(field="region", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(rev)", "name": "rev"}],
        })
        with pytest.raises(TimeDimensionFieldError):
            await engine.execute(query=[inner, outer], dry_run=True)

    async def test_unknown_flat_name_end_to_end(self) -> None:
        engine = await _dryrun_engine([orders_model()])
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "time_dimensions": [TimeDimension(field="nope", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(rev)", "name": "rev"}],
        })
        with pytest.raises(UnknownReferenceError, match="created_at"):  # lists the stage's columns
            await engine.execute(query=[inner, outer], dry_run=True)

    async def test_dotted_name_end_to_end(self) -> None:
        engine = await _dryrun_engine([orders_model()])
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "time_dimensions": [TimeDimension(field="customers.created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(rev)", "name": "rev"}],
        })
        with pytest.raises(IllegalScopeReferenceError):
            await engine.execute(query=[inner, outer], dry_run=True)


# ---------------------------------------------------------------------------
# Executed values on SQLite + DuckDB.
# ---------------------------------------------------------------------------
async def _exec_stages(backend: str, tmp: str, *, tables, models, stages) -> list[dict]:
    engine = await make_engine(
        backend, base_dir=os.path.join(tmp, "store"),
        db_path=os.path.join(tmp, f"t.{backend}"), tables=tables, models=models,
    )
    resp = await engine.execute(query=stages)
    return resp.data


@pytest.mark.parametrize("backend", BACKENDS)
class TestStageReBucketExecuted:
    async def test_same_granularity_multi_hop_flat_name(self, backend: str) -> None:
        tables = region_chain_tables(
            regions=[(10, "West", "2025-01-10"), (11, "East", "2025-02-20"), (12, "North", "2025-01-25")],
            customers=[(100, 10), (101, 11), (102, 12)],
            orders=[(1000, 100), (1001, 100), (1002, 101), (1003, 102), (1004, 102)],
        )
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders",
            "time_dimensions": [TimeDimension(
                field="customers.regions.last_activity_at", granularity=TG.MONTH,
            )],
            "metrics": [{"expression": "count(*)", "name": "n"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "time_dimensions": [TimeDimension(
                field="customers__regions__last_activity_at", granularity=TG.MONTH,
            )],
            "metrics": [{"expression": "sum(n)"}],
        })
        with tempfile.TemporaryDirectory() as tmp:
            data = await _exec_stages(backend, tmp, tables=tables, models=region_chain_models(), stages=[inner, outer])
        key = "s1.customers__regions__last_activity_at"
        got = {date_str(r[key]): r["s1.n_sum"] for r in data}
        assert got == {"2025-01-01": 4, "2025-02-01": 1}

    async def test_coarser_year_over_month(self, backend: str) -> None:
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.YEAR)],
            "metrics": [{"expression": "sum(rev)"}],
        })
        with tempfile.TemporaryDirectory() as tmp:
            data = await _exec_stages(backend, tmp, tables=[orders_table_spec(_MONTHLY)], models=[orders_model()], stages=[inner, outer])
        got = {date_str(r["s1.created_at"]): r["s1.rev_sum"] for r in data}
        assert got == {"2024-01-01": 300.0, "2025-01-01": 850.0}

    async def test_raw_temporal_column(self, backend: str) -> None:
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders", "dimensions": ["created_at"],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(rev)"}],
        })
        with tempfile.TemporaryDirectory() as tmp:
            data = await _exec_stages(backend, tmp, tables=[orders_table_spec(_MONTHLY)], models=[orders_model()], stages=[inner, outer])
        got = {date_str(r["s1.created_at"]): r["s1.rev_sum"] for r in data}
        assert got == {"2024-11-01": 100.0, "2024-12-01": 200.0, "2025-01-01": 350.0, "2025-02-01": 500.0}

    async def test_cohort_partition_by_auto_name_with_same_column_filter(self, backend: str) -> None:
        col = "created_at_max_partition_by_customer_id"
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders", "dimensions": ["customer_id"],
            "metrics": [{"expression": "max(created_at, partition_by=customer_id)"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "time_dimensions": [TimeDimension(field=col, granularity=TG.MONTH)],
            "filter": [f"{col} >= '2025-01-01'"],
            "metrics": [{"expression": "count(*)", "name": "cohort_size"}],
        })
        with tempfile.TemporaryDirectory() as tmp:
            engine = await make_engine(
                backend, base_dir=os.path.join(tmp, "store"),
                db_path=os.path.join(tmp, f"t.{backend}"),
                tables=[orders_table_spec(_COHORT)], models=[orders_model()],
            )
            resp = await engine.execute(query=[inner, outer])
            got = {date_str(r[f"s1.{col}"]): r["s1.cohort_size"] for r in resp.data}
            assert got == {"2025-02-01": 2, "2025-03-01": 2}
            # Every granularity binds on an aggregate-output column — day too.
            day_outer = outer.model_copy(update={
                "time_dimensions": [TimeDimension(field=col, granularity=TG.DAY)],
            })
            await engine.execute(query=[inner, day_outer], dry_run=True)  # must not raise

    async def test_three_stage_chain(self, backend: str) -> None:
        s1 = SlayerQuery.model_validate({
            "name": "s1", "population": "orders",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        s2 = SlayerQuery.model_validate({
            "name": "s2", "population": "s1",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(rev)"}],
        })
        s3 = SlayerQuery.model_validate({
            "population": "s2",
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.YEAR)],
            "metrics": [{"expression": "sum(rev_sum)"}],
        })
        with tempfile.TemporaryDirectory() as tmp:
            data = await _exec_stages(backend, tmp, tables=[orders_table_spec(_MONTHLY)], models=[orders_model()], stages=[s1, s2, s3])
        got = {date_str(r["s2.created_at"]): r["s2.rev_sum_sum"] for r in data}
        assert got == {"2024-01-01": 300.0, "2025-01-01": 850.0}

    async def test_stage_dimension_joins_stage_bucket_grain(self, backend: str) -> None:
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders", "dimensions": ["region", "created_at"],
            "metrics": [{"expression": "sum(amount)", "name": "rev"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1", "dimensions": ["region"],
            "time_dimensions": [TimeDimension(field="created_at", granularity=TG.MONTH)],
            "metrics": [{"expression": "sum(rev)"}],
        })
        with tempfile.TemporaryDirectory() as tmp:
            data = await _exec_stages(backend, tmp, tables=[orders_table_spec(_GRAIN)], models=[orders_model()], stages=[inner, outer])
        got = {(r["s1.region"], date_str(r["s1.created_at"])): r["s1.rev_sum"] for r in data}
        assert got == {
            ("W", "2025-01-01"): 150.0,
            ("W", "2025-02-01"): 200.0,
            ("E", "2025-01-01"): 300.0,
        }
