"""Check the query examples and demo references extracted from documentation pages on the demo."""

from __future__ import annotations

from typing import Any

from scripts.doc_examples import (
    IN_SCOPE_GLOBS,
    REPO_ROOT,
    DocBlock,
    DocPage,
    DocQuery,
    DocReference,
    extract_queries,
    in_scope_pages,
    load_page,
    parse_page,
)
from slayer.async_utils import run_sync
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.memories.resolver import resolve_entity
from slayer.storage.base import StorageBackend

__all__ = [
    "IN_SCOPE_GLOBS",
    "REPO_ROOT",
    "DocBlock",
    "DocPage",
    "DocQuery",
    "DocReference",
    "extract_queries",
    "in_scope_pages",
    "load_page",
    "page_failures",
    "parse_page",
    "query_failure",
    "reference_failure",
]


def _first_line(exc: BaseException) -> str:
    lines = str(exc).strip().splitlines()
    first = lines[0] if lines else ""
    name = type(exc).__name__
    return first if first.startswith(name) else f"{name}: {first}"


def query_failure(*, storage: StorageBackend, query: Any) -> str | None:
    """Why ``query`` fails on the demo (error, or no rows); ``None`` when it returns rows."""
    engine = SlayerQueryEngine(storage=storage)
    try:
        rows = engine.execute_sync(query).data
    except Exception as exc:  # noqa: BLE001 — any failure is reported with its location
        return _first_line(exc)
    finally:
        engine.close()
    return None if rows else "returned no rows"


def reference_failure(*, storage: StorageBackend, reference: str) -> str | None:
    """Why ``reference`` does not resolve as ``slayer inspect`` resolves it; ``None`` when it does."""
    try:
        run_sync(resolve_entity(reference, storage=storage))
    except Exception as exc:  # noqa: BLE001
        return _first_line(exc)
    return None


def page_failures(*, page: DocPage, storage: StorageBackend) -> list[str]:
    """Every ``page:line: reason`` failure of the doc-example check on ``page``."""
    failures = [
        f"{page.path}:{b.line}: {b.kind} mentions source_model outside any extracted query"
        for b in page.unextracted_blocks()
    ]
    for q in page.checked_queries():
        reason = query_failure(storage=storage, query=q.query)
        if reason is not None:
            failures.append(f"{page.path}:{q.line}: {reason}")
    for r in page.references:
        reason = reference_failure(storage=storage, reference=r.reference)
        if reason is not None:
            failures.append(f"{page.path}:{r.line}: {r.reference}: {reason}")
    return failures
