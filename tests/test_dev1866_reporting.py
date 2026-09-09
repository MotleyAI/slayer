"""DEV-1866 — the effective population is reported on every response path.

``SlayerResponse.population`` / ``.population_inferred`` are populated uniformly
across execute, dry-run, explain, cache miss/hit, refresh (real re-execution),
run-by-name, explicit vs inferred vs inline/extension source models, and the
Python client's decode path. Engine-level datasource forwarding into inference
is covered here too.
"""

from __future__ import annotations

import sqlite3
import tempfile

import pytest

from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.cache import CacheConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1866_fixtures import (
    DS_CHAIN,
    DS_WIDGETS_A,
    make_chain_exec_engine,
    make_inference_storage,
    seed_chain_storage,
)

_INFERRED = SlayerQuery(
    dimensions=["customers.region"],
    measures=[{"formula": "orders.amount:sum", "name": "rev"}],
)


@pytest.fixture
async def engine():
    async for e in make_chain_exec_engine("sqlite"):
        yield e


class TestExplicitVsInferredReporting:
    async def test_inferred_flag_true(self, engine) -> None:
        resp = await engine.execute(_INFERRED)
        assert resp.population == "customers"
        assert resp.population_inferred is True

    async def test_explicit_string_flag_false(self, engine) -> None:
        resp = await engine.execute(SlayerQuery(
            source_model="customers", dimensions=["region"],
        ))
        assert resp.population == "customers"
        assert resp.population_inferred is False

    async def test_explicit_extension_reports_source_name(self, engine) -> None:
        resp = await engine.execute(SlayerQuery(
            source_model={
                "source_name": "customers",
                "columns": [{"name": "region_copy", "sql": "region"}],
            },
            dimensions=["region"],
        ))
        assert resp.population == "customers"
        assert resp.population_inferred is False

    async def test_explicit_inline_model_reports_its_name(self, engine) -> None:
        resp = await engine.execute(SlayerQuery(
            source_model=SlayerModel(
                name="inl_cust", data_source=DS_CHAIN, sql_table="customers",
                columns=[Column(name="region", type=DataType.TEXT)],
            ),
            dimensions=["region"],
        ))
        assert resp.population == "inl_cust"
        assert resp.population_inferred is False

    async def test_run_by_name_reports_population(self, engine) -> None:
        await engine.create_model_from_query(
            query=SlayerQuery(
                source_model="customers",
                dimensions=["region"],
                measures=[ModelMeasure(formula="orders.amount:sum", name="rev")],
            ),
            name="cust_by_region",
        )
        resp = await engine.execute("cust_by_region")
        assert resp.population == "customers"
        assert resp.population_inferred is False


class TestReportedAcrossExecutionModes:
    async def test_dry_run(self, engine) -> None:
        resp = await engine.execute(_INFERRED, dry_run=True)
        assert resp.data == []
        assert resp.population == "customers"
        assert resp.population_inferred is True

    async def test_explain(self, engine) -> None:
        resp = await engine.execute(_INFERRED, explain=True)
        assert resp.population == "customers"
        assert resp.population_inferred is True

    async def test_cache_miss_then_hit(self, engine) -> None:
        miss = await engine.execute(_INFERRED, cache=True)
        assert engine.cache_size == 1  # stored
        hit = await engine.execute(_INFERRED, cache=True)
        assert engine.cache_size == 1  # served from cache, not re-stored
        assert miss.population == hit.population == "customers"
        assert miss.population_inferred is hit.population_inferred is True

    async def test_cache_twin_reports_current_inferred_flag(self, engine) -> None:
        """Explicit and inferred twins share a cache key (identical SQL); a cache
        hit must report the CURRENT query's inferred flag, not the stored one."""
        explicit = SlayerQuery(
            source_model="customers",
            dimensions=["customers.region"],
            measures=[{"formula": "orders.amount:sum", "name": "rev"}],
        )
        first = await engine.execute(explicit, cache=True)
        assert first.population_inferred is False
        assert engine.cache_size == 1
        # Same SQL ⇒ cache hit, but this query inferred its population.
        second = await engine.execute(_INFERRED, cache=True)
        assert engine.cache_size == 1
        assert second.population == "customers"
        assert second.population_inferred is True

    async def test_refresh_reexecution_preserves_population(self, tmp_path) -> None:
        storage, db_path = await seed_chain_storage(str(tmp_path))
        engine = SlayerQueryEngine(storage=storage)
        engine.cache_config = CacheConfig(refresh_keys=[("orders", "MAX(ordered_at)")])
        await engine.execute(_INFERRED, cache=True)
        con = sqlite3.connect(db_path)
        con.execute("INSERT INTO orders VALUES (5, 2, 'ok', 7.0, '2024-12-31')")
        con.commit()
        con.close()
        result = await engine.refresh()
        assert result.refreshed  # the entry was actually re-executed
        refreshed = await engine.execute(_INFERRED, cache=True)
        assert refreshed.population == "customers"
        assert refreshed.population_inferred is True


class TestEngineDatasourceForwarding:
    async def test_execute_forwards_datasource_to_inference(self) -> None:
        engine = SlayerQueryEngine(storage=await make_inference_storage())
        # No datasource ⇒ the widgets collision is ambiguous.
        with pytest.raises(PopulationInferenceError) as ei:
            await engine.execute(SlayerQuery(dimensions=["widgets.x"]), dry_run=True)
        assert ei.value.reason is PopulationErrorReason.AMBIGUOUS_DATASOURCE
        # Pinning the datasource resolves it (dry-run keeps it DB-free).
        resp = await engine.execute(
            SlayerQuery(dimensions=["widgets.x"]),
            data_source=DS_WIDGETS_A, dry_run=True,
        )
        assert resp.population == "widgets"
        assert resp.population_inferred is True

    async def test_inferred_datasource_pins_bundle_resolution(self) -> None:
        """The winning model name may also exist in another datasource; anchor
        voting fixes exactly one, and that datasource must pin bundle resolution
        so the bare model name never resolves ambiguously."""
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        for ds in ("dsa", "dsb"):
            await storage.save_datasource(
                DatasourceConfig(name=ds, type="sqlite", database=":memory:"),
            )
        regions = SlayerModel(
            name="regions", data_source="dsa", sql_table="regions",
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
                     Column(name="name", type=DataType.TEXT)],
        )
        cust_a = SlayerModel(
            name="customers", data_source="dsa", sql_table="customers",
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
                     Column(name="region_id", type=DataType.INT),
                     Column(name="name", type=DataType.TEXT)],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                             cardinality=JoinCardinality.MANY_TO_ONE)],
        )
        cust_b = SlayerModel(  # name collides with dsa.customers
            name="customers", data_source="dsb", sql_table="customers",
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
                     Column(name="name", type=DataType.TEXT)],
        )
        for model in (regions, cust_a, cust_b):
            await storage.save_model(model, _validate=False)
        engine = SlayerQueryEngine(storage=storage)
        # regions scopes to dsa; customers (the winner) exists in both dsa and dsb.
        resp = await engine.execute(
            SlayerQuery(dimensions=["customers.name", "regions.name"]), dry_run=True,
        )
        assert resp.population == "customers"
        assert resp.population_inferred is True


class TestClientDecode:
    def test_parse_response_decodes_population(self) -> None:
        resp = SlayerClient._parse_response({
            "data": [{"customers.region": "North"}],
            "columns": ["customers.region"],
            "sql": "SELECT 1",
            "population": "customers",
            "population_inferred": True,
        })
        assert resp.population == "customers"
        assert resp.population_inferred is True
