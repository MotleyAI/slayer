"""DEV-1450 stage 7b.7 — shared legacy-SQL parity oracle.

Generator slices 7b.8–7b.13 rewrite ``slayer/sql/generator.py`` to
consume the typed ``PlannedQuery`` shape directly. Each slice asserts
parity against the production legacy path:

    legacy = SQLGenerator().generate(enriched=await engine._enrich(q, model))
    new    = generate_from_planned(plan_query(q, bundle), dialect=...)
    assert_sql_equivalent(legacy, new)

This module centralises the helpers each slice needs so the per-stage
test files stay tight (model fixtures + a parametrised query list +
the parity call). The earlier plan called for a
``PlannedQuery → EnrichedQuery`` adapter to bridge the two paths, but
reproducing the bulk of ``slayer/engine/enrichment.py`` in throwaway
test-only code is the wrong trade-off — direct comparison against
``_enrich`` is both simpler and a stronger oracle.

The helpers here are deleted at the end of 7b.15 alongside the rest
of the legacy-only test surface (per the DEV-1452 follow-up).
"""

from __future__ import annotations

import difflib
from typing import Dict, Optional

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage


__all__ = [
    "assert_sql_equivalent",
    "build_storage_with_models",
    "legacy_sql_for",
    "norm_sql",
]


def norm_sql(sql: str) -> str:
    """Whitespace-canonical SQL. Collapses runs of whitespace into one space.

    The parity oracle compares syntactic SQL identity modulo whitespace;
    semantic-equivalence comparisons (sqlglot AST equality) would hide
    alias / order / CTE-shape regressions that matter to consumers.
    """
    return " ".join(sql.split())


async def legacy_sql_for(
    *,
    engine: SlayerQueryEngine,
    model: SlayerModel,
    query: SlayerQuery,
    named_queries: Optional[Dict[str, SlayerQuery]] = None,
    dialect: Optional[str] = None,
) -> str:
    """Render legacy SQL by routing through ``engine._enrich`` + ``SQLGenerator.generate``.

    This is the production code path the new pipeline must match
    bit-for-bit (modulo whitespace) for every supported query shape.
    Both methods are async / can touch storage, so the helper is async.

    ``named_queries`` mirrors the production multi-stage / cross-model
    code path: list-execution and ``query_nested`` both pass a name →
    SlayerQuery map so ``_enrich`` can resolve join targets and rerooted
    cross-model measures against named sibling stages. Slices that don't
    exercise multi-stage shapes can omit it.

    ``dialect`` follows the same fallback chain ``_enrich`` itself uses:
    when ``None``, ``_enrich`` resolves it from the model's datasource
    via storage (postgres default otherwise). Pass an explicit value to
    pin the dialect for tests that need non-postgres rendering.
    """
    enriched = await engine._enrich(
        query=query,
        model=model,
        named_queries=named_queries or {},
        dialect=dialect,
    )
    gen = SQLGenerator(dialect=dialect) if dialect is not None else SQLGenerator()
    return gen.generate(enriched=enriched)


def assert_sql_equivalent(legacy: str, new: str) -> None:
    """Whitespace-canonical equality with a useful diff on failure.

    Used by slice tests in stages 7b.8–7b.13. Raising ``AssertionError``
    keeps pytest's failure rendering happy. The diff is token-based so
    short SQL differences surface as a one-token hunk rather than full
    multi-line output.
    """
    if norm_sql(legacy) == norm_sql(new):
        return
    diff = "\n".join(
        difflib.unified_diff(
            norm_sql(legacy).split(),
            norm_sql(new).split(),
            fromfile="legacy",
            tofile="new",
            lineterm="",
        ),
    )
    raise AssertionError(
        f"SQL parity failed.\n"
        f"--- legacy ---\n{legacy}\n"
        f"--- new ---\n{new}\n"
        f"--- token diff ---\n{diff}\n",
    )


async def build_storage_with_models(
    tmp_path,
    *models: SlayerModel,
) -> YAMLStorage:
    """YAMLStorage seeded with the given models in order.

    Save-time validation is permissive on missing join targets (unsaved
    targets are silently skipped by the reachable-column validator), so
    save order isn't required for correctness. Saving targets before
    sources still improves best-effort save-time validation coverage,
    so callers conventionally pass leaf models first.
    """
    storage = YAMLStorage(base_dir=str(tmp_path))
    for m in models:
        await storage.save_model(m)
    return storage
