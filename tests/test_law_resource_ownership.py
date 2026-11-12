"""Resource-ownership ratchet (laws L1/L2/L3).

Import-binding-aware AST check over ``slayer/**`` and ``tests/**``:

* ``sqlite3.connect`` only in the one door (``slayer/storage/sqlite_conn.py``);
* ``create_engine`` only in ``engine_factory``, a dialect engine-build hook, or
  the disposing test helper (``tests/_engine_helpers.py``);
* ``create_async_engine`` only in the client's async builder;
* production ``.dispose()`` only in the engine owners (``engine_factory``, the client).

Plus the pyproject warning gate is pinned, (3.13+) a self-test proves a
leaking test turns the suite red, and self-tests of the async-engine release law
(``tests/_async_engine_release_law.py``). Zero tolerance — no baseline, like
``ALLOWED_EXPRESSIVENESS`` in the guard ratchet.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import textwrap
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Literal

import pytest
from pydantic import BaseModel

REPO = Path(__file__).resolve().parent.parent
SLAYER = REPO / "slayer"
TESTS = REPO / "tests"

#: The single home permitted for each concern (repo-relative posix paths).
DOOR = "slayer/storage/sqlite_conn.py"
ENGINE_FACTORY = "slayer/database/engine_factory.py"
ENGINE_HELPERS = "tests/_engine_helpers.py"
CLIENT = "slayer/database/client.py"
DIALECTS = "slayer/sql/dialects/"

_ON_313 = sys.version_info >= (3, 13)


# --------------------------------------------------------------------------- #
# Import-binding-aware call matcher.
# --------------------------------------------------------------------------- #
def _import_bindings(tree: ast.AST) -> dict[str, str]:
    """local name -> canonical dotted origin, from the module's imports."""
    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    bindings[alias.asname] = alias.name
                else:
                    top = alias.name.split(".")[0]
                    bindings[top] = top
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            for alias in node.names:
                bindings[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return bindings


def _dotted(node: ast.AST) -> list[str] | None:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return [*base, node.attr] if base is not None else None
    return None


def _canonical(func: ast.AST, bindings: dict[str, str]) -> str | None:
    """Resolve a call target to its canonical dotted path via the bindings."""
    chain = _dotted(func)
    if not chain:
        # An attribute call on an unnamed base (``self._x[0].dispose()``) still has a leaf.
        return f"<expr>.{func.attr}" if isinstance(func, ast.Attribute) else None
    base = chain[0]
    if base in bindings:
        resolved = bindings[base].split(".") + chain[1:]
    else:
        resolved = chain
    return ".".join(resolved)


def _concern(canonical: str | None) -> str | None:
    """Which resource concern a canonical target is, or None."""
    if not canonical:
        return None
    parts = canonical.split(".")
    leaf = parts[-1]
    if leaf == "connect" and parts[0] == "sqlite3":
        return "sqlite"                       # sqlite3.connect / sqlite3.dbapi2.connect
    if leaf == "create_engine" and "sqlalchemy" in parts:
        return "engine"
    if leaf == "create_async_engine":
        return "async_engine"
    if leaf == "dispose":
        return "dispose"
    return None


def _iter_calls(node: ast.AST, bindings: dict[str, str], stack: tuple[str, ...] = ()):
    """Yield (canonical, concern, lineno, enclosing-func-name stack) per Call."""
    if isinstance(node, ast.Call):
        canonical = _canonical(node.func, bindings)
        concern = _concern(canonical)
        if concern is not None:
            assert canonical is not None
            yield canonical, concern, node.lineno, stack
    for child in ast.iter_child_nodes(node):
        child_stack = (
            (*stack, child.name)
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            else stack
        )
        yield from _iter_calls(child, bindings, child_stack)


def _is_allowed(relpath: str, concern: str, stack: tuple[str, ...]) -> bool:
    if concern == "sqlite":
        return relpath == DOOR
    if concern == "engine":
        if relpath in (ENGINE_FACTORY, ENGINE_HELPERS):
            return True
        if relpath.startswith(DIALECTS):
            return any(
                fn == "build_engine" or (fn.startswith("_build_") and fn.endswith("_engine"))
                for fn in stack
            )
        return False
    if concern == "async_engine":
        return relpath == CLIENT and "_get_async_engine" in stack
    if concern == "dispose":
        # Tests dispose the engines they build; in production only the owners do.
        return relpath in (ENGINE_FACTORY, CLIENT) or not relpath.startswith("slayer/")
    return False


def _iter_py() -> list[Path]:
    return [p for root in (SLAYER, TESTS) for p in sorted(root.rglob("*.py"))]


def _violations() -> list[str]:
    out: list[str] = []
    for path in _iter_py():
        rel = path.relative_to(REPO).as_posix()
        tree = ast.parse(path.read_text())
        bindings = _import_bindings(tree)
        for canonical, concern, lineno, stack in _iter_calls(tree, bindings):
            if not _is_allowed(rel, concern, stack):
                out.append(f"{rel}:{lineno}: {concern} call `{canonical}` outside its allowed home")
    return out


def test_resource_ownership_law_holds() -> None:
    violations = _violations()
    assert not violations, (
        f"{len(violations)} resource-ownership violation(s):\n" + "\n".join(violations)
    )


# --------------------------------------------------------------------------- #
# The warning gate is pinned in pyproject.
# --------------------------------------------------------------------------- #
def _filterwarnings() -> list[str]:
    data = tomllib.loads((REPO / "pyproject.toml").read_text())
    return list(data["tool"]["pytest"]["ini_options"].get("filterwarnings", []))


def test_gate_config_is_pinned() -> None:
    filters = _filterwarnings()
    assert "error:unclosed database:ResourceWarning" in filters, filters
    # Scoped to sqlite's finalizer message: a non-sqlite driver's connection
    # finalizer (e.g. aiomysql after the loop closes) must stay a warning.
    assert (
        "error:Exception ignored while finalizing database connection"
        ":pytest.PytestUnraisableExceptionWarning" in filters
    ), filters


# --------------------------------------------------------------------------- #
# Matcher self-checks: every alias form, both concerns, allowed-predicate.
# --------------------------------------------------------------------------- #
class TestMatcherSelfChecks:
    @staticmethod
    def _concerns(source: str) -> list[tuple[str, str]]:
        tree = ast.parse(textwrap.dedent(source))
        bindings = _import_bindings(tree)
        return [(c.split(".")[-1], concern) for c, concern, *_ in _iter_calls(tree, bindings)]

    @pytest.mark.parametrize(
        "source, want_leaf, want_concern",
        [
            ("import sqlite3 as s\ns.connect('x')", "connect", "sqlite"),
            ("from sqlite3 import connect as c\nc('x')", "connect", "sqlite"),
            ("from sqlite3 import dbapi2\ndbapi2.connect('x')", "connect", "sqlite"),
            ("import sqlite3\nsqlite3.dbapi2.connect('x')", "connect", "sqlite"),
            ("import sqlalchemy as sa\nsa.create_engine('x')", "create_engine", "engine"),
            ("import sqlalchemy as sa\nsa.engine.create_engine('x')", "create_engine", "engine"),
            ("from sqlalchemy import create_engine\ncreate_engine('x')", "create_engine", "engine"),
            (
                "from sqlalchemy.ext.asyncio import create_async_engine\ncreate_async_engine('x')",
                "create_async_engine", "async_engine",
            ),
        ],
    )
    def test_alias_forms_are_detected(self, source, want_leaf, want_concern) -> None:
        assert (want_leaf, want_concern) in self._concerns(source)

    def test_unrelated_connect_is_not_flagged(self) -> None:
        assert self._concerns("import duckdb\nduckdb.connect('x')") == []

    def test_create_async_engine_is_not_matched_as_sync(self) -> None:
        concerns = self._concerns(
            "from sqlalchemy.ext.asyncio import create_async_engine\ncreate_async_engine('x')"
        )
        assert ("create_async_engine", "engine") not in concerns

    def test_dialect_create_engine_allowed_only_in_build_hooks(self) -> None:
        dialect = "slayer/sql/dialects/x.py"
        assert _is_allowed(dialect, "engine", ("build_engine",))
        assert _is_allowed(dialect, "engine", ("_build_oauth_engine",))
        assert not _is_allowed(dialect, "engine", ("some_helper",))
        assert not _is_allowed("slayer/sql/other.py", "engine", ("build_engine",))

    def test_sqlite_allowed_only_in_the_door(self) -> None:
        assert _is_allowed(DOOR, "sqlite", ())
        assert not _is_allowed("slayer/storage/sqlite_storage.py", "sqlite", ())

    @pytest.mark.parametrize(
        "source", ["engine.dispose()", "self._opened[0].dispose()", "e.pool.dispose()"],
    )
    def test_dispose_calls_are_detected(self, source) -> None:
        assert ("dispose", "dispose") in self._concerns(source)

    def test_dispose_allowed_only_in_the_engine_owners(self) -> None:
        assert _is_allowed(ENGINE_FACTORY, "dispose", ())
        assert _is_allowed(CLIENT, "dispose", ())
        assert _is_allowed("tests/test_x.py", "dispose", ())
        assert not _is_allowed("slayer/live_schema/ingestion.py", "dispose", ())

    def test_async_engine_allowed_only_in_the_builder(self) -> None:
        assert _is_allowed(CLIENT, "async_engine", ("_get_async_engine",))
        assert not _is_allowed(CLIENT, "async_engine", ("execute",))


# --------------------------------------------------------------------------- #
# Gate self-test: a leaking test turns a run red under the repo's pyproject.
# --------------------------------------------------------------------------- #
_LEAKY_TEST = """
import gc
import sqlite3


def test_leaks_a_connection(tmp_path):
    for _ in range(4):
        sqlite3.connect(str(tmp_path / "leak.db"))
    gc.collect()
"""


@pytest.mark.skipif(not _ON_313, reason="`unclosed database` ResourceWarning is 3.13+")
def test_gate_fails_a_leaking_run(tmp_path) -> None:
    leaky = tmp_path / "test_leaky_scratch.py"
    leaky.write_text(_LEAKY_TEST)
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest", str(leaky),
            "-c", str(REPO / "pyproject.toml"),
            "-p", "no:cacheprovider", "-p", "no:xdist", "-q",
        ],
        cwd=str(tmp_path), capture_output=True, text=True,
    )
    combined = (proc.stdout + proc.stderr).lower()
    assert proc.returncode != 0, f"gate let the leak pass:\n{combined}"
    assert "unclosed database" in combined, combined


# --------------------------------------------------------------------------- #
# Async-engine release law: every async engine a client hands out is disposed on
# its own loop before that loop closes, else the building test fails.
# --------------------------------------------------------------------------- #
def test_abandons_marker_is_registered() -> None:
    data = tomllib.loads((REPO / "pyproject.toml").read_text())
    markers = data["tool"]["pytest"]["ini_options"]["markers"]
    assert any(m.startswith("abandons_async_engines:") for m in markers), markers


_LAW_DS = "law_ds"

# Seconds before a scratch pytest run counts as hung.
_SCRATCH_TIMEOUT = 300

_LAW_INI = """\
[pytest]
asyncio_mode = auto
asyncio_default_fixture_loop_scope = function
markers =
    abandons_async_engines: the test abandons closed loops' async engines
"""

_LAW_CONFTEST = """\
pytest_plugins = ["tests._async_engine_release_law"]
"""

# An unreachable server: building an async engine never connects.
_LAW_KIT = f"""\
from slayer.core.models import DatasourceConfig

DS = DatasourceConfig(
    name="{_LAW_DS}", type="postgres", host="127.0.0.1", port=9,
    database="none", username="u", password="",
)


async def hand_out(client):
    return client._get_async_engine()
"""

_LAW_FUNCTION = """\
import pytest

from slayer.database.client import SlayerSQLClient
from _lawkit import DS, hand_out


@pytest.fixture
async def leaky_client():
    client = SlayerSQLClient(DS)
    await hand_out(client)
    yield client


def test_function_fixture_leak(leaky_client):
    pass


@pytest.fixture
async def builds_on_teardown():
    client = SlayerSQLClient(DS)
    yield
    await hand_out(client)


def test_fixture_teardown_leak(builds_on_teardown):
    pass


async def test_inline_client_dropped():
    client = SlayerSQLClient(DS)
    await hand_out(client)
    del client


async def test_released_client_passes():
    client = SlayerSQLClient(DS)
    await hand_out(client)
    await client.aclose()


@pytest.fixture
async def leaks_then_teardown_raises():
    client = SlayerSQLClient(DS)
    await hand_out(client)
    yield
    raise RuntimeError("teardown boom")


def test_teardown_raises_and_leaks(leaks_then_teardown_raises):
    pass
"""

_LAW_ENGINES = """\
import pytest

from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine
from _lawkit import DS, hand_out


@pytest.fixture
async def owned(tmp_path):
    storage = YAMLStorage(base_dir=str(tmp_path))
    async with owned_query_engine(storage=storage, datasources=[DS]) as engine:
        await hand_out(engine.client_for(DS))
        yield engine


def test_through_the_helper(owned):
    pass


async def test_client_factory_leak(tmp_path):
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path)), client_factory=SlayerSQLClient)
    await hand_out(engine.client_for(DS))
"""

_LAW_SYNC = """\
import asyncio

from slayer.database.client import SlayerSQLClient
from _lawkit import DS, hand_out


def test_asyncio_run_leak():
    asyncio.run(hand_out(SlayerSQLClient(DS)))


def test_asyncio_run_released():
    client = SlayerSQLClient(DS)

    async def _use():
        await hand_out(client)
        await client.aclose()

    asyncio.run(_use())


def test_disposed_on_another_loop():
    engine = asyncio.run(hand_out(SlayerSQLClient(DS)))
    asyncio.run(engine.dispose())


def test_dispose_that_raises(monkeypatch):
    client = SlayerSQLClient(DS)

    def _boom(*_args, **_kwargs):
        raise RuntimeError("dispose failed")

    async def _use():
        engine = await hand_out(client)
        monkeypatch.setattr(engine.sync_engine.pool, "dispose", _boom)
        await client.aclose()

    asyncio.run(_use())
"""

_LAW_ABANDON = """\
import asyncio

import pytest

from slayer.database.client import SlayerSQLClient
from _lawkit import DS, hand_out


def _abandon_two(client):
    asyncio.run(hand_out(client))
    asyncio.run(hand_out(client))
    client.close()


def test_abandons_unmarked():
    _abandon_two(SlayerSQLClient(DS))


@pytest.mark.abandons_async_engines
def test_abandons_marked():
    _abandon_two(SlayerSQLClient(DS))


@pytest.mark.abandons_async_engines
def test_marked_still_fails_a_leak():
    _abandon_two(SlayerSQLClient(DS))
    asyncio.run(hand_out(SlayerSQLClient(DS)))
"""

_LAW_MODULE_LEAK = """\
import pytest_asyncio

from slayer.database.client import SlayerSQLClient
from _lawkit import DS, hand_out


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def module_client():
    client = SlayerSQLClient(DS)
    await hand_out(client)
    yield client


def test_module_builder(module_client):
    pass


def test_module_later(module_client):
    pass
"""

_LAW_MODULE_RELEASED = """\
import pytest_asyncio

from slayer.database.client import SlayerSQLClient
from _lawkit import DS, hand_out


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def module_client():
    client = SlayerSQLClient(DS)
    await hand_out(client)
    yield client
    await client.aclose()


def test_module_released_first(module_client):
    pass


def test_module_released_last(module_client):
    pass
"""

_LAW_SESSION_LEAK = """\
import pytest_asyncio

from slayer.database.client import SlayerSQLClient
from _lawkit import DS, hand_out


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def session_client():
    client = SlayerSQLClient(DS)
    await hand_out(client)
    yield client


def test_session_builder(session_client):
    pass


def test_session_last(session_client):
    pass
"""

# The bridge loop closes at session end, as in the repo conftest.
_LAW_BRIDGE_CONFTEST = _LAW_CONFTEST + """
import pytest

from slayer import async_utils


@pytest.fixture(scope="session", autouse=True)
def _shutdown_bridge_at_session_end():
    yield
    async_utils.shutdown_bridge()
"""

_LAW_BRIDGE = """\
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from _lawkit import DS, hand_out


def test_bridge_engine(tmp_path):
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path)))
    engine.run_sync(hand_out(engine.client_for(DS)))


def test_after_bridge():
    pass
"""

#: Scratch projects, each run once in its own pytest process; modules run in listed order.
_LAW_PROJECTS: dict[str, dict[str, str]] = {
    "main": {
        "conftest.py": _LAW_CONFTEST,
        "test_a_function.py": _LAW_FUNCTION,
        "test_b_engines.py": _LAW_ENGINES,
        "test_c_sync.py": _LAW_SYNC,
        "test_d_abandon.py": _LAW_ABANDON,
        "test_e_module_leak.py": _LAW_MODULE_LEAK,
        "test_f_module_released.py": _LAW_MODULE_RELEASED,
    },
    "session": {"conftest.py": _LAW_CONFTEST, "test_session.py": _LAW_SESSION_LEAK},
    "bridge": {"conftest.py": _LAW_BRIDGE_CONFTEST, "test_bridge.py": _LAW_BRIDGE},
}


class LawCase(BaseModel):
    """A scratch test and the test its teardown must blame (``None``: it passes)."""

    project: str
    node: str
    blames: str | None
    keeps: str | None = None  # an error the teardown raised itself, which must survive the judgment


_LAW_CASES = [
    LawCase(project="main", node="test_a_function.py::test_function_fixture_leak",
            blames="test_a_function.py::test_function_fixture_leak"),
    LawCase(project="main", node="test_a_function.py::test_fixture_teardown_leak",
            blames="test_a_function.py::test_fixture_teardown_leak"),
    LawCase(project="main", node="test_a_function.py::test_inline_client_dropped",
            blames="test_a_function.py::test_inline_client_dropped"),
    LawCase(project="main", node="test_a_function.py::test_released_client_passes", blames=None),
    # A raising teardown is still judged, and its own error kept.
    LawCase(project="main", node="test_a_function.py::test_teardown_raises_and_leaks",
            blames="test_a_function.py::test_teardown_raises_and_leaks", keeps="RuntimeError: teardown boom"),
    LawCase(project="main", node="test_b_engines.py::test_through_the_helper", blames=None),
    LawCase(project="main", node="test_b_engines.py::test_client_factory_leak",
            blames="test_b_engines.py::test_client_factory_leak"),
    LawCase(project="main", node="test_c_sync.py::test_asyncio_run_leak",
            blames="test_c_sync.py::test_asyncio_run_leak"),
    LawCase(project="main", node="test_c_sync.py::test_asyncio_run_released", blames=None),
    LawCase(project="main", node="test_c_sync.py::test_disposed_on_another_loop",
            blames="test_c_sync.py::test_disposed_on_another_loop"),
    LawCase(project="main", node="test_c_sync.py::test_dispose_that_raises",
            blames="test_c_sync.py::test_dispose_that_raises"),
    LawCase(project="main", node="test_d_abandon.py::test_abandons_unmarked",
            blames="test_d_abandon.py::test_abandons_unmarked"),
    LawCase(project="main", node="test_d_abandon.py::test_abandons_marked", blames=None),
    LawCase(project="main", node="test_d_abandon.py::test_marked_still_fails_a_leak",
            blames="test_d_abandon.py::test_marked_still_fails_a_leak"),
    # A scoped loop's engine is judged when that loop closes, not mid-scope.
    LawCase(project="main", node="test_e_module_leak.py::test_module_builder", blames=None),
    LawCase(project="main", node="test_e_module_leak.py::test_module_later",
            blames="test_e_module_leak.py::test_module_builder"),
    LawCase(project="main", node="test_f_module_released.py::test_module_released_first", blames=None),
    LawCase(project="main", node="test_f_module_released.py::test_module_released_last", blames=None),
    LawCase(project="session", node="test_session.py::test_session_builder", blames=None),
    LawCase(project="session", node="test_session.py::test_session_last",
            blames="test_session.py::test_session_builder"),
    LawCase(project="bridge", node="test_bridge.py::test_bridge_engine", blames=None),
    LawCase(project="bridge", node="test_bridge.py::test_after_bridge", blames=None),
]


Outcome = Literal["passed", "failed", "error", "skipped"]

_JUNIT_OUTCOMES: tuple[tuple[str, Outcome], ...] = (("failure", "failed"), ("error", "error"), ("skipped", "skipped"))


class ScratchOutcome(BaseModel):
    """One scratch test's JUnit outcome."""

    outcome: Outcome
    message: str


class ScratchRun(BaseModel):
    outcomes: dict[str, ScratchOutcome]
    output: str


def _junit_outcomes(xml_path: Path) -> dict[str, ScratchOutcome]:
    outcomes: dict[str, ScratchOutcome] = {}
    for case in ET.parse(xml_path).getroot().iter("testcase"):
        node = f"{case.get('classname', '').replace('.', '/')}.py::{case.get('name')}"
        outcome: Outcome = "passed"
        message = ""
        for tag, name in _JUNIT_OUTCOMES:
            found = case.find(tag)
            if found is not None:
                outcome = name
                message = f"{found.get('message', '')}\n{found.text or ''}"
                break
        outcomes[node] = ScratchOutcome(outcome=outcome, message=message)
    return outcomes


def _run_scratch(root: Path, files: dict[str, str]) -> ScratchRun:
    (root / "pytest.ini").write_text(_LAW_INI)
    (root / "_lawkit.py").write_text(_LAW_KIT)
    for name, source in files.items():
        (root / name).write_text(source)
    xml = root / "junit.xml"
    tests = [name for name in files if name.startswith("test_")]
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest", *tests,
            "-p", "no:cacheprovider", "-p", "no:xdist", "-p", "no:randomly",
            "-q", "--continue-on-collection-errors", f"--junitxml={xml}",
        ],
        cwd=str(root), capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(REPO)}, timeout=_SCRATCH_TIMEOUT,
    )
    output = proc.stdout + proc.stderr
    return ScratchRun(outcomes=_junit_outcomes(xml) if xml.exists() else {}, output=output)


@pytest.fixture(scope="module")
def law_runs(tmp_path_factory: pytest.TempPathFactory) -> dict[str, ScratchRun]:
    return {
        project: _run_scratch(tmp_path_factory.mktemp(f"law_{project}"), files)
        for project, files in _LAW_PROJECTS.items()
    }


@pytest.mark.parametrize("case", _LAW_CASES, ids=[c.node for c in _LAW_CASES])
def test_async_engine_release_law(case: LawCase, law_runs: dict[str, ScratchRun]) -> None:
    run = law_runs[case.project]
    assert case.node in run.outcomes, f"{case.node} did not run:\n{run.output}"
    got = run.outcomes[case.node]
    if case.blames is None:
        assert got.outcome == "passed", f"{case.node}: {got.outcome}\n{got.message}"
        return
    assert got.outcome == "error", f"{case.node}: {got.outcome}\n{got.message}\n{run.output}"
    assert got.message.startswith("failed on teardown"), got.message
    assert case.blames in got.message, got.message
    assert _LAW_DS in got.message, got.message
    if case.keeps is not None:
        assert case.keeps in got.message, got.message
