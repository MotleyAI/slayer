"""Search's lazily importing checks never run on the event loop: no ``async def`` under ``slayer/`` calls them directly.

Calls resolve lexically: imports (relative ones too) and the defining module's own functions, through enclosing
function scopes, with each scope's locals and comprehension targets shadowing them; a name a scope imports twice
may be either. Calls in nested sync ``def``s and lambdas are skipped; passing a function as a reference (to
``asyncio.to_thread``) is allowed.
"""

from __future__ import annotations

import ast
import importlib
import textwrap
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path

import pytest

from tests.test_law_resource_ownership import _canonical, _dotted

REPO = Path(__file__).resolve().parent.parent
SLAYER = REPO / "slayer"

BANNED = frozenset({
    "slayer.embeddings.client.is_available",
    "slayer.embeddings.client.truncate_text_for_model",
    "slayer.search.graph.is_available",
})
_FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
_SCOPES = (*_FUNCTIONS, ast.ClassDef)
_COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)
_Function = ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda


def _module_name(path: Path) -> str:
    parts = list(path.relative_to(REPO).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _package_name(path: Path) -> str:
    return ".".join(path.relative_to(REPO).parts[:-1])


def _scope_nodes(body: Iterable[ast.AST]) -> list[ast.AST]:
    """Nodes of one scope; nested functions and classes are yielded but not entered."""
    out: list[ast.AST] = []
    stack = list(body)
    while stack:
        node = stack.pop()
        out.append(node)
        if not isinstance(node, _SCOPES):
            stack.extend(ast.iter_child_nodes(node))
    return out


def _imports(nodes: Iterable[ast.AST], package: str) -> dict[str, frozenset[str]]:
    """local name -> every canonical dotted origin one scope's imports give it (re-imports are flow-insensitive)."""
    pkg_parts = package.split(".") if package else []
    pairs: list[tuple[str, str]] = []
    for node in nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                pairs.append((alias.asname or top, alias.name if alias.asname else top))
        elif isinstance(node, ast.ImportFrom):
            base = pkg_parts[: len(pkg_parts) - node.level + 1] if node.level else []
            prefix = ".".join([*base, *([node.module] if node.module else [])])
            pairs.extend((alias.asname or alias.name, f"{prefix}.{alias.name}") for alias in node.names)
    bindings: dict[str, frozenset[str]] = {}
    for name, origin in pairs:
        bindings[name] = bindings.get(name, frozenset()) | {origin}
    return bindings


def _targets(func: ast.expr, env: dict[str, frozenset[str]]) -> set[str | None]:
    """Every canonical path a call target can resolve to."""
    chain = _dotted(func)
    origins = env.get(chain[0], frozenset()) if chain else frozenset()
    if not chain or not origins:
        return {_canonical(func, {})}
    return {_canonical(func, {chain[0]: origin}) for origin in origins}


def _comprehension_targets(comp: ast.AST) -> list[ast.Name]:
    generators = comp.generators if isinstance(comp, _COMPREHENSIONS) else []
    return [n for g in generators for n in ast.walk(g.target) if isinstance(n, ast.Name)]


def _local_names(func: _Function, nodes: list[ast.AST]) -> set[str]:
    """Names ``func``'s own scope binds, minus ``global``/``nonlocal`` ones; comprehension targets are not."""
    args = func.args
    params = [*args.posonlyargs, *args.args, *args.kwonlyargs, *filter(None, [args.vararg, args.kwarg])]
    comp_targets = {id(n) for c in nodes for n in _comprehension_targets(c)}
    stored = {
        n.id for n in nodes if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store) and id(n) not in comp_targets
    }
    defs = {n.name for n in nodes if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    caught = {n.name for n in nodes if isinstance(n, ast.ExceptHandler) and n.name}
    declared = {name for n in nodes if isinstance(n, (ast.Global, ast.Nonlocal)) for name in n.names}
    return ({a.arg for a in params} | stored | defs | caught) - declared


def _comprehension_parts(
    comp: ast.ListComp | ast.SetComp | ast.DictComp | ast.GeneratorExp, shadow: frozenset[str],
) -> list[tuple[ast.AST, frozenset[str]]]:
    """A comprehension's subexpressions in evaluation order, each seeing only the targets bound before it."""
    parts: list[tuple[ast.AST, frozenset[str]]] = []
    for gen in comp.generators:
        parts.append((gen.iter, shadow))
        shadow = shadow | {n.id for n in ast.walk(gen.target) if isinstance(n, ast.Name)}
        parts.extend((cond, shadow) for cond in gen.ifs)
    results = [comp.key, comp.value] if isinstance(comp, ast.DictComp) else [comp.elt]
    return parts + [(expr, shadow) for expr in results]


def _calls(body: Iterable[ast.AST]) -> Iterator[tuple[ast.Call, frozenset[str]]]:
    """Calls in one scope, each with the comprehension targets shadowing names at that point."""
    stack: list[tuple[ast.AST, frozenset[str]]] = [(node, frozenset()) for node in body]
    while stack:
        node, shadow = stack.pop()
        if isinstance(node, _SCOPES):
            continue
        if isinstance(node, _COMPREHENSIONS):
            stack.extend(_comprehension_parts(node, shadow))
            continue
        if isinstance(node, ast.Call):
            yield node, shadow
        stack.extend((child, shadow) for child in ast.iter_child_nodes(node))


def _visit(
    body: Sequence[ast.AST], env: dict[str, frozenset[str]], *, package: str, label: str, found: list[str],
) -> None:
    for node in _scope_nodes(body):
        if isinstance(node, ast.ClassDef):
            _visit(node.body, env, package=package, label=label, found=found)  # class names don't reach methods
            continue
        if not isinstance(node, _FUNCTIONS):
            continue
        inner_body: list[ast.AST] = [node.body] if isinstance(node, ast.Lambda) else list(node.body)
        inner_nodes = _scope_nodes(inner_body)
        local = _local_names(node, inner_nodes)
        inner_env = {k: v for k, v in env.items() if k not in local} | _imports(inner_nodes, package)
        if isinstance(node, ast.AsyncFunctionDef):
            for call, shadow in _calls(inner_body):
                visible = {k: v for k, v in inner_env.items() if k not in shadow}
                for target in sorted(t for t in _targets(call.func, visible) if t in BANNED):
                    found.append(f"{label}:{call.lineno} {node.name}() calls {target}")
        _visit(inner_body, inner_env, package=package, label=label, found=found)


def violations(source: str, *, module: str, label: str, package: str | None = None) -> list[str]:
    tree = ast.parse(source)
    package = module.rpartition(".")[0] if package is None else package
    env = _imports(_scope_nodes(tree.body), package)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name not in env:
            env[node.name] = frozenset({f"{module}.{node.name}"})
    found: list[str] = []
    _visit(tree.body, env, package=package, label=label, found=found)
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
            path.read_text(encoding="utf-8"),
            module=_module_name(path),
            label=str(path.relative_to(REPO)),
            package=_package_name(path),
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
        pytest.param("slayer.search.service", """
            from . import graph
            async def f():
                graph.is_available()
        """, id="relative-import"),
        pytest.param("slayer.search.service", """
            from ..embeddings.client import is_available
            async def f():
                is_available()
        """, id="relative-parent-import"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f(other):
                ec.is_available()
        """, id="unshadowed-alias-beside-parameter"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f(xs):
                names = [ec for ec in xs]
                ec.is_available()
                return names
        """, id="comprehension-target-does-not-shadow-outside"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            def g():
                from slayer.database import client as ec
                return ec
            async def f():
                ec.is_available()
        """, id="import-in-another-function-does-not-leak"),
        pytest.param("slayer.x", """
            def outer():
                from slayer.embeddings import client as ec
                async def f():
                    ec.is_available()
                return f
        """, id="enclosing-function-import"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f(xs):
                return [ec.truncate_text_for_model(x) for x in xs]
        """, id="call-inside-comprehension"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f(pick):
                return [x for ec in pick(ec.is_available())]
        """, id="first-iterable-precedes-its-target"),
        pytest.param("slayer.x", """
            async def f():
                from slayer.embeddings.client import is_available as check
                check()
                from slayer.database.client import is_available as check
        """, id="later-reimport-does-not-hide-earlier-binding"),
    ],
)
def test_law_flags(module: str, source: str) -> None:
    assert len(violations(textwrap.dedent(source), module=module, label="snippet")) == 1


def test_law_resolves_relative_imports_in_a_package_init() -> None:
    source = "from .graph import is_available\nasync def f():\n    is_available()\n"
    assert len(violations(source, module="slayer.search", label="snippet", package="slayer.search")) == 1


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
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f(ec):
                ec.is_available()
        """, id="parameter-shadows-alias"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f(make):
                ec = make()
                ec.is_available()
        """, id="local-assignment-shadows-alias"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f(clients):
                return [ec.is_available() for ec in clients]
        """, id="comprehension-target-shadows-inside"),
        pytest.param("slayer.x", """
            from slayer.embeddings import client as ec
            async def f():
                from slayer.database import client as ec
                ec.is_available()
        """, id="function-local-import-rebinds-alias"),
    ],
)
def test_law_allows(module: str, source: str) -> None:
    assert violations(textwrap.dedent(source), module=module, label="snippet") == []
