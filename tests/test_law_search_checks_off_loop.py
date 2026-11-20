"""Search's lazily importing checks never run on the event loop: no ``async def`` under ``slayer/`` calls them directly.

Calls resolve through import bindings, plus bare names inside the defining module. Nested sync ``def``s and
lambdas are skipped; passing a function as a reference (to ``asyncio.to_thread``) is allowed.
"""

from __future__ import annotations

import ast
import importlib
import textwrap
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.test_law_resource_ownership import _canonical, _import_bindings

REPO = Path(__file__).resolve().parent.parent
SLAYER = REPO / "slayer"

BANNED = frozenset({
    "slayer.embeddings.client.is_available",
    "slayer.embeddings.client.truncate_text_for_model",
    "slayer.search.graph.is_available",
})
_SKIPPED_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)


def _module_name(path: Path) -> str:
    parts = list(path.relative_to(REPO).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _bindings(tree: ast.Module, module: str) -> dict[str, str]:
    """Imports, plus the module's own top-level functions under their qualified names."""
    bindings = _import_bindings(tree)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name not in bindings:
            bindings[node.name] = f"{module}.{node.name}"
    return bindings


def _calls_in_body(func: ast.AsyncFunctionDef) -> Iterator[ast.Call]:
    """Calls in ``func``'s own body; nested functions, lambdas and classes are their own scopes."""
    stack: list[ast.AST] = list(func.body)
    while stack:
        node = stack.pop()
        if isinstance(node, _SKIPPED_SCOPES):
            continue
        if isinstance(node, ast.Call):
            yield node
        stack.extend(ast.iter_child_nodes(node))


def violations(source: str, *, module: str, label: str) -> list[str]:
    tree = ast.parse(source)
    bindings = _bindings(tree, module)
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for call in _calls_in_body(node):
            target = _canonical(call.func, bindings)
            if target in BANNED:
                found.append(f"{label}:{call.lineno} {node.name}() calls {target}")
    return sorted(found)


def test_banned_targets_exist() -> None:
    for target in BANNED:
        module, _, name = target.rpartition(".")
        assert callable(getattr(importlib.import_module(module), name)), target


def test_no_async_def_calls_a_lazily_importing_search_check() -> None:
    found = [
        v
        for path in sorted(SLAYER.rglob("*.py"))
        for v in violations(
            path.read_text(encoding="utf-8"), module=_module_name(path), label=str(path.relative_to(REPO)),
        )
    ]
    assert not found, "await the async gate instead:\n" + "\n".join(found)


@pytest.mark.parametrize(
    ("module", "source"),
    [
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f():
                ec.is_available()
        """, id="module-alias"),
        pytest.param("slayer.x", """
            import slayer.search.graph
            async def f():
                slayer.search.graph.is_available()
        """, id="dotted-import"),
        pytest.param("slayer.x", """
            from slayer.embeddings.client import truncate_text_for_model as trunc
            async def f():
                trunc("t")
        """, id="from-import-alias"),
        pytest.param("slayer.search.graph", """
            def is_available():
                return True
            async def f():
                is_available()
        """, id="bare-name-in-defining-module"),
        pytest.param("slayer.x", """
            from slayer.search import graph
            class C:
                async def m(self):
                    if graph.is_available():
                        pass
        """, id="method"),
    ],
)
def test_law_flags(module: str, source: str) -> None:
    assert len(violations(textwrap.dedent(source), module=module, label="snippet")) == 1


@pytest.mark.parametrize(
    ("module", "source"),
    [
        pytest.param("slayer.x", """
            import asyncio
            from slayer.embeddings import client as ec
            async def f():
                await asyncio.to_thread(ec.is_available)
        """, id="reference-to-to-thread"),
        pytest.param("slayer.x", """
            def is_available():
                return True
            async def f():
                is_available()
        """, id="unrelated-same-named-function"),
        pytest.param("slayer.x", """
            from slayer.database import client
            async def f():
                client.is_available()
        """, id="same-name-other-module"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f():
                def g():
                    return ec.is_available()
                h = lambda: ec.truncate_text_for_model("t")
                return g, h
        """, id="nested-sync-def-and-lambda"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            def f():
                return ec.is_available()
        """, id="sync-caller"),
    ],
)
def test_law_allows(module: str, source: str) -> None:
    assert violations(textwrap.dedent(source), module=module, label="snippet") == []
