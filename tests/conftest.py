"""Shared test fixtures."""

import datetime
import gc
import io
import os
import shutil
import sys
import tempfile
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from filelock import FileLock
from pydantic import BaseModel, ConfigDict

from slayer import async_utils
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.demo import jaffle_shop
from slayer.embeddings import client as embedding_client
from slayer.database import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

from tests import _emitted_reparse_law as emitted_reparse_law
from tests import _statement_render_law as statement_render_law
from tests._dev1824_fixtures import make_exec_engine
from tests._usage_reporting_capture import CaptureServer

pytest_plugins = ["tests._sharding", "tests._async_engine_release_law", "pytester"]


class JaffleGeneratorSpy:
    """Stands in for jaffle_shop ``generate_data``: returns the shared CSVs, counts builds."""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.calls = 0

    def __call__(self, *args: object, **kwargs: object) -> str:
        self.calls += 1
        return self.data_dir


def _run_wide_dir(tmp_path_factory: pytest.TempPathFactory, worker_id: str, name: str) -> Path:
    """A dir shared by every xdist worker of this run; guard writes with ``FileLock(f"{dir}.lock")``."""
    if worker_id == "master":
        return tmp_path_factory.mktemp(name)
    root = tmp_path_factory.getbasetemp().parent / name
    root.mkdir(exist_ok=True)
    return root


@pytest.fixture(scope="session")
def jafgen_dir(tmp_path_factory: pytest.TempPathFactory, worker_id: str) -> str:
    """One jafgen run (1 year) per run, shared by every xdist worker; tests only read it."""
    root = _run_wide_dir(tmp_path_factory, worker_id, "jafgen")
    done = root / "data_dir.txt"
    with FileLock(f"{root}.lock"):
        if not done.exists():
            data_dir = jaffle_shop.generate_data(output_dir=str(root), years=1, stream=io.StringIO())
            done.write_text(data_dir)
    return done.read_text()


@pytest.fixture
def jaffle_generator(monkeypatch: pytest.MonkeyPatch, jafgen_dir: str) -> JaffleGeneratorSpy:
    spy = JaffleGeneratorSpy(jafgen_dir)
    monkeypatch.setattr(jaffle_shop, "generate_data", spy)
    return spy


DOC_DEMO_YEARS = 2


@pytest.fixture(scope="session")
def shared_demo_duckdb(tmp_path_factory: pytest.TempPathFactory, worker_id: str) -> str:
    """The 2-year demo DuckDB, built once per run and shared by every xdist worker."""
    root = _run_wide_dir(tmp_path_factory, worker_id, "shared_demo")
    db_path = jaffle_shop.resolve_demo_db_path(str(root))
    with FileLock(f"{root}.lock"):
        if not os.path.exists(db_path):
            jaffle_shop.build_jaffle_shop(db_path=db_path, years=DOC_DEMO_YEARS, stream=io.StringIO())
    return db_path


@pytest.fixture(scope="session")
def doc_demo_storage(tmp_path_factory: pytest.TempPathFactory, shared_demo_duckdb: str) -> YAMLStorage:
    """This worker's own copy of the shared demo, set up through the reuse path."""
    store_dir = str(tmp_path_factory.mktemp("doc_demo"))
    shutil.copy2(shared_demo_duckdb, jaffle_shop.resolve_demo_db_path(store_dir))
    storage = YAMLStorage(base_dir=store_dir)
    _, _, built = jaffle_shop.ensure_demo_datasource(
        storage, storage_path=store_dir, years=DOC_DEMO_YEARS, stream=io.StringIO(),
    )
    assert not built
    return storage


@pytest.fixture(scope="session", autouse=True)
def _usage_reporting_off_by_default() -> Iterator[None]:
    """Our own test runs never report usage; ``usage_reporting_env`` re-enables it."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("SLAYER_USAGE_REPORTING", "off")
        yield


class UsageReportingEnv(BaseModel):
    """Isolated usage-reporting environment: tmp config dirs, capture endpoint, injectable clock."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    root: Path
    storage_dir: Path
    capture: CaptureServer
    monkeypatch: pytest.MonkeyPatch

    def set_now(self, now: datetime.datetime) -> None:
        self.monkeypatch.setattr("slayer.usage_reporting.clock.now", lambda: now)

    def subprocess_env(self, **overrides: str | None) -> dict[str, str]:
        # Embedding channel off, as in-process: no live litellm calls from a dev machine's key.
        env = {k: v for k, v in os.environ.items() if k not in ("OPENAI_API_KEY", "SLAYER_EMBEDDING_MODEL")}
        for key, value in overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        return env


@pytest.fixture
def usage_reporting_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[UsageReportingEnv]:
    """Usage reporting on, config under ``tmp_path``, reports posted to a local capture server."""
    capture = CaptureServer().start()
    for var in ("DO_NOT_TRACK", "CI", "SLAYER_STORAGE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("SLAYER_USAGE_REPORTING", "on")
    monkeypatch.setenv("SLAYER_USAGE_REPORTING_ENDPOINT", capture.url)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))
    storage_dir = tmp_path / "store"
    storage_dir.mkdir()
    env = UsageReportingEnv(root=tmp_path, storage_dir=storage_dir, capture=capture, monkeypatch=monkeypatch)
    try:
        yield env
    finally:
        usage_reporting = sys.modules.get("slayer.usage_reporting")
        if usage_reporting is not None:
            usage_reporting.reset()
        capture.stop()


@pytest.fixture(scope="session", autouse=True)
def _dispose_engines_at_session_end() -> Iterator[None]:
    """Resource-leak gate: dispose the factory cache and force a collection at session
    end, inside the last test's teardown — while pytest's warning filters and
    unraisable hook are still installed in every xdist worker (a
    ``pytest_sessionfinish`` hook can run after they are gone)."""
    yield
    async_utils.shutdown_bridge()
    engine_factory.reset_cache()
    gc.collect()


# Resource-leak gate scope: the pyproject `filterwarnings` errors are measured and
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
    """Validate scope-closure on every generated statement.

    Sets ``SLAYER_VALIDATE_SCOPES=1`` so the generator's post-mangle, pre-RLS
    ``maybe_validate_scopes`` hook (``slayer/sql/scope_check.py``) runs for
    every emitted statement across the suite. A *provable* out-of-scope
    reference raises ``ScopeLeakError`` at generation time — turning
    the "no gaps" invariant into a failing test. The validator is
    sound-on-corpus (no false positives); if a currently-passing statement
    trips it, that is either a genuine latent leak (pin strict-xfail to its
    owning stage) or a validator bug (fix the validator) — never silence it.
    """
    monkeypatch.setenv("SLAYER_VALIDATE_SCOPES", "1")


@pytest.fixture(autouse=True)
def _no_statement_render_during_composition(monkeypatch: pytest.MonkeyPatch) -> None:
    """python.sql.arc42.md §3.1: rendering a statement inside an AST builder fails the test."""
    statement_render_law.install(monkeypatch)


@pytest.fixture(autouse=True)
def _no_emitted_statement_reparse(
    monkeypatch: pytest.MonkeyPatch, _no_statement_render_during_composition: None,
) -> Iterator[None]:
    """python.sql.arc42.md §3.1: an execution parsing a statement it rendered fails the test."""
    emitted_reparse_law.install(monkeypatch)
    yield
    found = emitted_reparse_law.take_violations()
    if found:
        pytest.fail("\n\n".join(found), pytrace=False)


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
async def mydb_orders_storage() -> AsyncIterator[YAMLStorage]:
    """A YAMLStorage seeded with a single ``mydb`` datasource
    and a minimal ``orders`` model (id PK + amount column). Shared by
    every memory-reference test that just needs *some* live entity to resolve
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
