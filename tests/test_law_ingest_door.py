"""One ingest door: surfaces and protocols ingest only through the idempotent pass.

Import-binding-aware AST check over the ``surfaces`` and ``protocols`` buckets of ``architecture/model.c4``.
"""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path

from tests.test_law_resource_ownership import _canonical, _import_bindings

REPO = Path(__file__).resolve().parent.parent

#: The ``surfaces`` and ``protocols`` bucket packages.
PACKAGES = (
    "slayer.api", "slayer.mcp", "slayer.cli", "slayer.client", "slayer.inspect", "slayer.demo", "slayer.__main__",
    "slayer.flight", "slayer.pg_facade", "slayer.facade",
)

#: The scan-and-return entry points, which persist nothing on their own.
_SCANS = {"ingest_datasource_report_async", "ingest_datasource_report", "ingest_datasource_async", "ingest_datasource"}
#: The engine's scan-only wrappers.
_ENGINE_SCANS = {"ingest", "ingest_sync"}


def _door(func: ast.AST, bindings: dict[str, str]) -> str | None:
    """The bypassing ingest a call target runs, or None."""
    if isinstance(func, ast.Attribute) and func.attr in _ENGINE_SCANS:
        return f"engine scan `.{func.attr}(`"
    canonical = _canonical(func, bindings)
    if canonical and canonical.split(".")[-1] in _SCANS:
        return f"scan `{canonical}`"
    return None


def _sites(tree: ast.AST) -> list[tuple[int, str]]:
    bindings = _import_bindings(tree)
    return [
        (node.lineno, door)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and (door := _door(node.func, bindings)) is not None
    ]


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
        assert paths and all(path.exists() for path in paths), package


def test_the_scanner_sees_every_kind_of_bypass() -> None:
    source = textwrap.dedent("""
        from slayer.engine import ingestion as engine_ingestion
        from slayer.engine.ingestion import ingest_datasource_report_async as scan
        from slayer.engine.ingestion import ingest_datasource_report, ingest_datasource_async
        import slayer.engine.ingestion

        async def f(engine, ds, client):
            await engine_ingestion.ingest_datasource_report_async(datasource=ds, client=client)
            await scan(datasource=ds, client=client)
            ingest_datasource_report(datasource=ds)
            await ingest_datasource_async(datasource=ds)
            slayer.engine.ingestion.ingest_datasource(datasource=ds)
            await engine.ingest(ds)
            engine.ingest_sync(ds)
    """)
    assert len(_sites(ast.parse(source))) == 7


def test_the_idempotent_pass_is_not_a_bypass() -> None:
    source = textwrap.dedent("""
        from slayer.engine import ingestion as engine_ingestion

        async def f(engine, ds, storage, client):
            await engine_ingestion.ingest_datasource_idempotent(datasource=ds, storage=storage, client=client)
            await engine.ingest_idempotent(ds)
            engine.ingest_idempotent_sync(ds)
    """)
    assert _sites(ast.parse(source)) == []
