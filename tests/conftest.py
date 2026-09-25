"""Shared test fixtures."""

import gc
import os
import tempfile
from collections.abc import AsyncIterator, Iterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.embeddings import client as embedding_client
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

from tests import _statement_render_law as statement_render_law
from tests._dev1824_fixtures import make_exec_engine


@pytest.fixture(scope="session", autouse=True)
def _dispose_engines_at_session_end() -> Iterator[None]:
    """DEV-1943 gate: dispose the factory cache and force a collection at session
    end, inside the last test's teardown — while pytest's warning filters and
    unraisable hook are still installed in every xdist worker (a
    ``pytest_sessionfinish`` hook can run after they are gone)."""
    yield
    engine_factory.reset_cache()
    gc.collect()


# DEV-1943 gate scope: the pyproject `filterwarnings` errors are measured and
# enforced on the unit suite. The integration suite's async/thread execution plus
# SQLAlchemy pool teardown has a harder sqlite finalizer edge (same class as the
# `:memory:`/DuckDB pool-hygiene non-goals), so downgrade the two sqlite gate
# warnings to warnings for integration tests only — best-effort there, hard error
# on the unit suite. CI runs the two suites separately (`-m "not integration"` vs
# `-m integration`), so this per-item downgrade never reaches the unit gate.
_INTEGRATION_DIR = os.path.join(os.path.dirname(__file__), "integration")
_SQLITE_GATE_RELAXATIONS = (
    "default:unclosed database:ResourceWarning",
    "default:Exception ignored while finalizing database connection"
    ":pytest.PytestUnraisableExceptionWarning",
)


def pytest_collection_modifyitems(items) -> None:
    for item in items:
        if str(item.path).startswith(_INTEGRATION_DIR + os.sep):
            for spec in _SQLITE_GATE_RELAXATIONS:
                item.add_marker(pytest.mark.filterwarnings(spec))


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


@pytest.fixture(autouse=True)
def _enable_scope_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    """DEV-1705: validate scope-closure on every generated statement.

    Sets ``SLAYER_VALIDATE_SCOPES=1`` so the generator's post-mangle, pre-RLS
    ``maybe_validate_scopes`` hook (``slayer/sql/scope_check.py``) runs for
    every emitted statement across the suite. A *provable* out-of-scope
    reference raises ``ScopeLeakError`` at generation time — turning
    DEV-1703's "no gaps" invariant into a failing test. The validator is
    sound-on-corpus (no false positives); if a currently-passing statement
    trips it, that is either a genuine latent leak (pin strict-xfail to its
    owning stage) or a validator bug (fix the validator) — never silence it.
    """
    monkeypatch.setenv("SLAYER_VALIDATE_SCOPES", "1")


@pytest.fixture(autouse=True)
def _no_statement_render_during_composition(monkeypatch: pytest.MonkeyPatch) -> None:
    """sql.arc42.md §3.1: rendering a statement inside an AST builder fails the test."""
    statement_render_law.install(monkeypatch)


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


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    """Executing engine over the shared dev1739-family models, one per backend.

    Modules built on another fixture family override this locally.
    """
    async for engine in make_exec_engine(request):
        yield engine
