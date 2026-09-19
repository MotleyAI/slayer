"""DEV-1943 resource-ownership ratchet (laws L1/L2/L3).

Import-binding-aware AST check over ``slayer/**`` and ``tests/**``:

* ``sqlite3.connect`` only in the one door (``slayer/storage/sqlite_conn.py``);
* ``create_engine`` only in ``engine_factory``, a dialect engine-build hook, or
  the disposing test helper (``tests/_engine_helpers.py``);
* ``create_async_engine`` only in the client's async builder.

Plus the pyproject warning gate is pinned, and (3.13+) a self-test proves a
leaking test turns the suite red. Zero tolerance — no baseline, like
``ALLOWED_EXPRESSIVENESS`` in the guard ratchet.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SLAYER = REPO / "slayer"
TESTS = REPO / "tests"

#: The single home permitted for each concern (repo-relative posix paths).
DOOR = "slayer/storage/sqlite_conn.py"
ENGINE_FACTORY = "slayer/sql/engine_factory.py"
ENGINE_HELPERS = "tests/_engine_helpers.py"
CLIENT = "slayer/sql/client.py"
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
        return None
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
    assert "error::pytest.PytestUnraisableExceptionWarning" in filters, filters


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
