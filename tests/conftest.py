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


def _collects_full_registry(config) -> bool:
    """True when this run collects the entire non-integration tree, so every
    registry key (all non-integration) is expected to match a collected test.

    False for ``-k`` keyword filters, path-subset runs, and *positive* ``-m``
    selections (e.g. ``-m integration``) — those legitimately collect a subset,
    so the stale-key check must not fire. A deselecting marker (``-m "not
    integration ..."``) still collects every registry test, so it stays full.
    """
    if config.option.keyword:
        return False
    markexpr = (config.option.markexpr or "").strip()
    if markexpr and not markexpr.startswith("not "):
        return False
    args = [a for a in config.args if a.rstrip("/") not in ("", "tests")]
    return not args


def pytest_collection_modifyitems(config, items):
    """DEV-1704 Stage 0: pin every recorded main-parity gap as ``xfail(strict=True)``.

    The typed pipeline is the active path; a set of main-branch feature tests
    fail against it until later DEV-1703 stages absorb the feature. Rather than
    delete or skip them (which would silently drop coverage), each is marked
    strict-xfail keyed by its exact node id — so it flips back to a hard failure
    the moment the feature lands, and DEV-1485 (Stage 11) can gate on
    ``tests/parity_xfails.py`` being empty. See the registry module for the
    per-gap reason / owning issue.

    The registry is self-policing in both directions: ``strict=True`` catches a
    key whose test has been *fixed* (XPASS -> failure), and the stale-key check
    below catches a key that matches *no* collected test (renamed test, param-id
    churn from a sqlglot bump, an already-deleted test) so a zombie entry can't
    silently rot and block the Stage-11 gate.
    """
    consumed = set()
    for item in items:
        reason = PARITY_XFAILS.get(item.nodeid)
        if reason is not None:
            item.add_marker(pytest.mark.xfail(reason=reason, strict=True))
            consumed.add(item.nodeid)

    stale = set(PARITY_XFAILS) - consumed
    if stale and _collects_full_registry(config):
        raise pytest.UsageError(
            f"tests/parity_xfails.py has {len(stale)} stale key(s) matching no "
            "collected test (renamed/deleted test or changed param id) — fix or "
            "remove them:\n  " + "\n  ".join(sorted(stale))
        )


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
