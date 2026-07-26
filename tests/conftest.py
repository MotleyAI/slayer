"""Shared test fixtures."""

import os
import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.embeddings import client as embedding_client
from slayer.storage.yaml_storage import YAMLStorage
from tests.parity_xfails import PARITY_XFAILS


def pytest_collection_modifyitems(config, items):
    """DEV-1704 Stage 0: pin every recorded main-parity gap as ``xfail(strict=True)``.

    The typed pipeline is the active path; a set of main-branch feature tests
    fail against it until later DEV-1703 stages absorb the feature. Rather than
    delete or skip them (which would silently drop coverage), each is marked
    strict-xfail keyed by its exact node id — so it flips back to a hard failure
    the moment the feature lands, and DEV-1485 (Stage 11) can gate on
    ``tests/parity_xfails.py`` being empty. See the registry module for the
    per-gap reason / owning stage.
    """
    for item in items:
        reason = PARITY_XFAILS.get(item.nodeid)
        if reason is not None:
            item.add_marker(pytest.mark.xfail(reason=reason, strict=True))


@pytest.fixture(autouse=True)
def _disable_embedding_channel_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the embedding channel off for every test by default.

    Two reasons:

    * Without this, tests that exercise the real write paths
      (``save_memory`` / ``ingest`` / ``edit_model``) would attempt
      live ``litellm.aembedding`` calls — costing money on a dev
      machine that has ``OPENAI_API_KEY`` set, and emitting per-entity
      bubble-up warnings on CI that doesn't.
    * Tests that *do* want to exercise the embedding code path
      (``test_embedding_retriever.py``, ``test_search_three_channel.py``)
      explicitly monkeypatch ``is_available`` back to ``True`` in their
      local fixtures, so this autouse default doesn't interfere.

    Per the spec, bubble-up of *runtime* embed failures is intentional;
    this fixture isolates "channel disabled by env" from "channel
    available and failing".
    """
    embedding_client.is_available.cache_clear()
    monkeypatch.setattr(embedding_client, "is_available", lambda: False)


@pytest.fixture
def sample_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test_ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
    )


@pytest.fixture
def sample_datasource() -> DatasourceConfig:
    return DatasourceConfig(
        name="test_ds",
        type="postgres",
        host="localhost",
        port=5432,
        database="testdb",
        username="user",
        password="pass",
    )


@pytest.fixture
def yaml_storage(sample_datasource: DatasourceConfig) -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        storage.save_datasource(sample_datasource)
        yield storage


@pytest.fixture
async def mydb_orders_storage() -> AsyncIterator[YAMLStorage]:
    """DEV-1428: a YAMLStorage seeded with a single ``mydb`` datasource
    and a minimal ``orders`` model (id PK + amount column). Shared by
    every DEV-1428 test that just needs *some* live entity to resolve
    memory references against; centralised here to keep the per-test
    setup blocks from drifting into Sonar duplication-density failures.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=os.path.join(tmpdir, "store"))
        await storage.save_datasource(
            DatasourceConfig(
                name="mydb", type="sqlite", database=":memory:",
            )
        )
        await storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="mydb",
                columns=[
                    Column(name="id", sql="id", primary_key=True),
                    Column(name="amount", sql="amount"),
                ],
            )
        )
        yield storage
