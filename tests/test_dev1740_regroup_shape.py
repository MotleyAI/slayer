"""DEV-1740 Part B2 — desugar SQL shape (dry-run).

Filter routing across the synthesized stage boundary (base-row predicates push
into the aggregate CTE; computed-dimension predicates never do), and one
synthesized aggregate stage per DISTINCT partition-key set (dedup by identity).
"""

from __future__ import annotations

import re

import pytest

from slayer.core.query import ModelMeasure, SlayerQuery

from tests._dev1740_fixtures import cm_cte_bodies, gen

# Desugar SQL-shape acceptance for B2 — deferred to DEV-1825 (regroup primitive).
pytestmark = pytest.mark.skip(
    reason="B2 aggregate-then-regroup deferred to DEV-1825 (regroup primitive)"
)


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


BAND = "CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END"


def _count_isolated_ctes(sql: str) -> int:
    """Distinct isolated-aggregate CTE definitions (``_cm_<name> AS (``)."""
    return len({m.group(1) for m in re.finditer(r"(_cm_\w+)\s+AS\s*\(", sql)})


class TestFilterRoutingAcrossStageBoundary:
    async def test_base_row_filter_pushed_computed_predicate_not(self) -> None:
        sql = await gen(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["band == 1", "status == 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="band_total")],
        ))
        cte = cm_cte_bodies(sql)
        # The base-row predicate reaches the partition CTE (DEV-1739 rule)...
        assert "status" in cte
        # ...but the band threshold / predicate is applied only after join-back,
        # never inside the aggregate stage.
        assert "5000" not in cte


class TestOneStagePerDistinctPartitionSet:
    async def test_two_distinct_sets_emit_two_ctes(self) -> None:
        band = ("CASE WHEN amount:sum(partition_by=city) > 5000 "
                "AND amount:sum(partition_by=region) > 10000 THEN 1 ELSE 0 END")
        sql = await gen(_q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert _count_isolated_ctes(sql) == 2

    async def test_repeated_aggregate_dedups_to_one_cte(self) -> None:
        # The same partitioned aggregate referenced twice must intern to ONE
        # synthesized stage (dedup by AggregateKey identity).
        band = ("CASE WHEN amount:sum(partition_by=city) > 5000 "
                "THEN amount:sum(partition_by=city) ELSE 0 END")
        sql = await gen(_q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert _count_isolated_ctes(sql) == 1
