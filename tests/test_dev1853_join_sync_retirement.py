"""DEV-1853 — ``join_sync`` mirroring and the scoped inversion helper are gone.

Reverse traversal is computed, never materialized: saving an INNER join
creates no mirror, the storage wrapper and dbt mirroring are deleted, and no
parallel traversal logic survives outside the shared walker.
"""

from __future__ import annotations

import importlib.util
import inspect
import tempfile

import slayer.search.graph as search_graph
import slayer.storage.base as storage_base
from slayer.core.enums import JoinType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.engine import join_safety
from slayer.storage.base import resolve_storage


def test_join_sync_module_is_deleted() -> None:
    assert importlib.util.find_spec("slayer.storage.join_sync") is None


def test_wrap_join_sync_wiring_is_deleted() -> None:
    assert not hasattr(storage_base, "_wrap_join_sync")


def test_dbt_converter_no_longer_mirrors() -> None:
    assert not hasattr(DbtToSlayerConverter, "_mirror_inner_joins")


def test_scoped_inversion_helper_is_subsumed() -> None:
    assert not hasattr(join_safety, "resolve_correlation_hop")


def test_search_graph_has_no_join_sync_dependency() -> None:
    assert "join_sync" not in inspect.getsource(search_graph)


async def test_saving_an_inner_join_creates_no_mirror() -> None:
    # Through the factory path that used to apply the JoinSyncStorage wrapper.
    with tempfile.TemporaryDirectory() as d:
        storage = resolve_storage(d)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="customers", data_source="ds", sql_table="customers",
            columns=[Column(name="id", primary_key=True)]))
        await storage.save_model(SlayerModel(
            name="orders", data_source="ds", sql_table="orders",
            columns=[Column(name="id", primary_key=True),
                     Column(name="customer_id")],
            joins=[ModelJoin(target_model="customers",
                             join_pairs=[["customer_id", "id"]],
                             join_type=JoinType.INNER)]))
        customers = await storage.get_model("customers", data_source="ds")
        assert customers is not None
        assert customers.joins == []
