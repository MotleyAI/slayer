"""DEV-1865 — the materialize-and-filter fallback for unlowered masks.

Correctness never depends on a lowering rule existing: with the HAVING lowering
forced off, a measure mask routes through the general outer-WHERE path over
materialized hidden columns and produces identical results.
"""

from __future__ import annotations

import pytest

from slayer.sql import generator as generator_module

from tests._dev1865_fixtures import ModelMeasure, make_exec_engine, q

# A combined-select shape (the partitioned measure attaches a producer CTE), so
# the outer scope the fallback routes to actually exists.
_QUERY_KWARGS = dict(
    dimensions=["region", "city"],
    filters=["amount:sum > 55"],
    measures=[
        ModelMeasure(formula="amount:sum", name="s"),
        ModelMeasure(formula="amount:sum(partition_by=region)", name="part"),
    ],
)


def _rows(resp):
    return sorted(tuple(sorted(r.items())) for r in resp.data)


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
async def test_forced_fallback_matches_the_having_lowering(
    backend, monkeypatch,
) -> None:
    request = type("R", (), {"param": backend})()
    async for engine in make_exec_engine(request):
        lowered = await engine.execute(q(**_QUERY_KWARGS))
        lowered_sql = (await engine.execute(q(**_QUERY_KWARGS), dry_run=True)).sql
        assert lowered_sql is not None and "HAVING" in lowered_sql
        break

    monkeypatch.setattr(generator_module, "_FORCE_MASK_FALLBACK", True)
    async for engine in make_exec_engine(request):
        fallback = await engine.execute(q(**_QUERY_KWARGS))
        fallback_sql = (await engine.execute(q(**_QUERY_KWARGS), dry_run=True)).sql
        assert fallback_sql is not None
        assert "HAVING" not in fallback_sql, "the fallback did not engage"
        break

    assert _rows(fallback) == _rows(lowered)
