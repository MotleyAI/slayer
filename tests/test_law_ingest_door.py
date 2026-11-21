"""One ingest door: surfaces and protocols ingest only through the idempotent pass.

Import-binding-aware AST check over the ``surfaces`` and ``protocols`` buckets of ``architecture/model.c4``.
"""

from __future__ import annotations

import ast
import textwrap
from collections.abc import Iterator
from pathlib import Path

from tests.test_law_resource_ownership import _canonical, _dotted

REPO = Path(__file__).resolve().parent.parent
_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)

#: The ``surfaces`` and ``protocols`` bucket packages.
PACKAGES = (
    "slayer.api", "slayer.mcp", "slayer.cli", "slayer.client", "slayer.inspect", "slayer.demo", "slayer.__main__",
    "slayer.flight", "slayer.pg_facade", "slayer.facade",
)

#: The scan-and-return entry points, which persist nothing on their own.
_SCANS = {"ingest_datasource_report_async", "ingest_datasource_report", "ingest_datasource_async", "ingest_datasource"}
#: The engine's scan-only wrappers.
_ENGINE_SCANS = {"ingest", "ingest_sync"}


def _door(func: ast.AST, bindings: dict[str, set[str]]) -> str | None:
    """The bypassing ingest a call target may run, or None."""
    if isinstance(func, ast.Attribute) and func.attr in _ENGINE_SCANS:
        return f"engine scan `.{func.attr}(`"
    chain = _dotted(func)
    base = chain[0] if chain else ""
    for origin in sorted(bindings.get(base, ())) or [None]:
        canonical = _canonical(func, {base: origin} if origin else {})
        if canonical and canonical.split(".")[-1] in _SCANS:
            return f"scan `{canonical}`"
    return None


def _in_scope(scope: ast.AST) -> Iterator[ast.AST]:
    """The nodes of ``scope``, stopping at (but yielding) nested scopes."""
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop()
        yield node
        if not isinstance(node, _SCOPES):
            stack.extend(ast.iter_child_nodes(node))


def _origins(node: ast.Import | ast.ImportFrom) -> Iterator[tuple[str, str]]:
    """An import's (local name, dotted origin) pairs, relative imports included."""
    if isinstance(node, ast.Import):
        for alias in node.names:
            top = alias.name.split(".")[0]
            yield alias.asname or top, alias.name if alias.asname else top
        return
    module = "." * node.level + (node.module or "")
    for alias in node.names:
        yield alias.asname or alias.name, f"{module}.{alias.name}"


def _sites(scope: ast.AST, inherited: dict[str, set[str]] | None = None) -> list[tuple[int, str]]:
    """Bypassing calls in ``scope``; a name may be any origin it is bound to in this or an enclosing scope."""
    bindings = {name: set(origins) for name, origins in (inherited or {}).items()}
    nodes = list(_in_scope(scope))
    for node in nodes:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for name, origin in _origins(node):
                bindings.setdefault(name, set()).add(origin)
    out: list[tuple[int, str]] = []
    for node in nodes:
        if isinstance(node, ast.Call) and (door := _door(node.func, bindings)) is not None:
            out.append((node.lineno, door))
        if isinstance(node, _SCOPES):
            out.extend(_sites(node, bindings))
    return sorted(out)


def _package_paths(package: str) -> list[Path]:
    base = REPO / Path(*package.split("."))
    if base.is_dir():
        return sorted(base.rglob("*.py"))
    return [base.with_suffix(".py")]


def _violations() -> list[str]:
    out: list[str] = []
    for package in PACKAGES:
        for path in _package_paths(package):
            rel = path.relative_to(REPO).as_posix()
            out.extend(f"{rel}:{line}: {door}" for line, door in _sites(ast.parse(path.read_text())))
    return out


def test_surfaces_ingest_only_through_the_idempotent_pass() -> None:
    violations = _violations()
    assert not violations, (
        f"{len(violations)} ingest(s) bypassing the idempotent pass:\n" + "\n".join(violations)
    )


def test_every_package_exists() -> None:
    for package in PACKAGES:
        paths = _package_paths(package)
        assert paths, package
        assert all(path.exists() for path in paths), package


def test_the_scanner_sees_every_kind_of_bypass() -> None:
    source = textwrap.dedent("""
        from slayer.live_schema import ingestion as engine_ingestion
        from slayer.live_schema.ingestion import ingest_datasource_report_async as scan
        from slayer.live_schema.ingestion import ingest_datasource_report, ingest_datasource_async
        import slayer.live_schema.ingestion

        async def f(engine, ds, client):
            await engine_ingestion.ingest_datasource_report_async(datasource=ds, client=client)
            await scan(datasource=ds, client=client)
            ingest_datasource_report(datasource=ds)
            await ingest_datasource_async(datasource=ds)
            slayer.live_schema.ingestion.ingest_datasource(datasource=ds)
            await engine.ingest(ds)
            engine.ingest_sync(ds)
    """)
    assert len(_sites(ast.parse(source))) == 7


def test_the_idempotent_pass_is_not_a_bypass() -> None:
    source = textwrap.dedent("""
        from slayer.live_schema import ingestion as engine_ingestion

        async def f(engine, ds, storage, client):
            await engine_ingestion.ingest_datasource_idempotent(datasource=ds, storage=storage, client=client)
            await engine.ingest_idempotent(ds)
            engine.ingest_idempotent_sync(ds)
    """)
    assert _sites(ast.parse(source)) == []


def test_the_scanner_resolves_relative_import_aliases() -> None:
    source = textwrap.dedent("""
        from ..live_schema.ingestion import ingest_datasource_async as scan

        async def f(ds):
            await scan(datasource=ds)
    """)
    assert len(_sites(ast.parse(source))) == 1


def test_the_scanner_resolves_names_per_scope() -> None:
    source = textwrap.dedent("""
        async def f(ds):
            from slayer.live_schema.ingestion import ingest_datasource_async as scan
            await scan(datasource=ds)

        async def g(ds, storage):
            from slayer.live_schema.ingestion import ingest_datasource_idempotent as scan
            await scan(datasource=ds, storage=storage)
    """)
    assert [line for line, _ in _sites(ast.parse(source))] == [4]


def test_a_rebound_name_may_be_any_of_its_origins() -> None:
    source = textwrap.dedent("""
        from slayer.live_schema.ingestion import ingest_datasource_async as run

        async def f(ds, storage, flag):
            if flag:
                from slayer.live_schema.ingestion import ingest_datasource_idempotent as run
            await run(datasource=ds, storage=storage)
    """)
    assert [line for line, _ in _sites(ast.parse(source))] == [7]
