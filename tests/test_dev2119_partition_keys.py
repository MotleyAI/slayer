"""Row-level expressions as ``partition_by=`` keys (spec: queries/partitioned-aggregates)."""

from __future__ import annotations

import pytest

from slayer.core.errors import PartitionKeyError
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2119_fixtures import BACKENDS, EFF, EFF_NAME, by_bucket, by_dims, column, engine_for, m, q, td

UPPER_CITY = {("EU", "BERLIN"): 70.0, ("EU", "PARIS"): 50.0, ("US", "NYC"): 90.0, ("US", "PARIS"): 50.0}
#: ``sum(amount, partition_by=BAND)`` per (region, band): hi = NYC 90 + berlin 50, lo = the rest.
BAND = "CASE WHEN sum(amount, partition_by=city) > 45 THEN 'hi' ELSE 'lo' END"
BAND_TOTALS = {("EU", "hi"): 140.0, ("EU", "lo"): 70.0, ("US", "hi"): 140.0, ("US", "lo"): 70.0}


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with engine_for(request.param) as e:
        yield e


def _dims(resp, *names: str) -> dict[tuple, float | None]:
    return by_dims(resp, [f"orders.{n}" for n in names])


class TestComputedDimensionPosition:
    async def test_expression_key_equals_named_computed_dimension(self, engine: SlayerQueryEngine) -> None:
        expr = await engine.execute(q(dimensions=["region", "upper(city)"],
                                      measures=[m("sum(amount, partition_by=upper(city))")]))
        named = await engine.execute(q(dimensions=["region", {"expression": "upper(city)", "name": "ucity"}],
                                       measures=[m("sum(amount, partition_by=ucity)")]))
        assert _dims(expr, "region", "upper_city") == UPPER_CITY, expr.sql
        assert _dims(named, "region", "ucity") == UPPER_CITY

    async def test_expression_key_not_a_query_dimension_fails(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["region"], measures=[m("sum(amount, partition_by=upper(city))")])
        with pytest.raises(PartitionKeyError) as ei:
            await engine.execute(query, dry_run=True)
        msg = str(ei.value)
        assert "upper(city)" in msg
        assert "computed dimension" in msg.lower()
        assert "ModelExtension" not in msg

    async def test_finer_grain_exemption(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"],
                                      measures=[m("avg(sum(amount, partition_by=[region, upper(city)]))")]))
        assert _dims(resp, "region") == {("EU",): 40.0, ("US",): 65.0}, resp.sql


class TestRankFamily:
    async def test_rank_partitioned_by_expression(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            dimensions=["region", "upper(city)"],
            measures=[m("rank(sum(amount), partition_by=upper(city), direction='desc')")],
        ))
        assert _dims(resp, "region", "upper_city") == {
            ("EU", "BERLIN"): 1.0, ("EU", "PARIS"): 2.0, ("US", "NYC"): 1.0, ("US", "PARIS"): 1.0,
        }, resp.sql


class TestTimeDimensionSource:
    async def test_key_equal_to_expression_source_is_its_bucket(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], time_dimensions=[td(EFF)],
                                      measures=[m(f"sum(amount, partition_by={EFF})")]))
        assert by_bucket(resp, column(resp, EFF_NAME), "orders.v", by=["orders.region"]) == {
            ("EU", "2025-01"): 10.0, ("EU", "2025-02"): 50.0, ("EU", "2025-04"): 50.0,
            ("US", "2025-02"): 50.0, ("US", "2025-03"): 100.0,
        }, resp.sql

    async def test_source_at_several_granularities_is_ambiguous(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["region"], time_dimensions=[td(EFF), td(EFF, "year")],
                  measures=[m(f"sum(amount, partition_by={EFF})")])
        with pytest.raises(PartitionKeyError, match="ambiguous"):
            await engine.execute(query, dry_run=True)


class TestAttributability:
    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    async def test_joined_leaf_over_unproven_hop(self, engine: SlayerQueryEngine, mode: str) -> None:
        query = q(dimensions=["region"], to_many_handling=mode,
                  measures=[m("sum(amount, partition_by=upper(regions.manager))")])
        with pytest.raises(PartitionKeyError) as ei:
            await engine.execute(query, dry_run=True)
        msg = str(ei.value)
        assert "regions.manager" in msg
        assert "unproven" in msg
        assert "attributable" in msg
        assert "Declare join cardinality" in msg


class TestAggregateBearingKey:
    async def test_inline_key_rejected_with_computed_dimension_remedy(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["region"], measures=[m(f"sum(amount, partition_by={BAND})")])
        with pytest.raises(PartitionKeyError) as ei:
            await engine.execute(query, dry_run=True)
        msg = str(ei.value)
        assert "sum(amount" in msg
        assert "computed dimension" in msg.lower()
        assert "ModelExtension" not in msg

    async def test_key_equal_to_computed_dimension_executes_as_it(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region", {"expression": BAND, "name": "band"}],
                                      measures=[m(f"sum(amount, partition_by={BAND})")]))
        assert _dims(resp, "region", "band") == BAND_TOTALS, resp.sql


class TestMixedSourceFirst:
    async def test_first_over_mixed_source_picks_at_ranked_row(self, engine: SlayerQueryEngine) -> None:
        # avg(price) per product: A 7/3, B 14/3; earliest by created_at: EU row 1 (qty 1, A), US row 6 (qty 1, B).
        resp = await engine.execute(q(dimensions=["region"],
                                      measures=[m("first(quantity * avg(price, partition_by=product))")]))
        got = _dims(resp, "region")
        assert got == {("EU",): pytest.approx(7 / 3), ("US",): pytest.approx(14 / 3)}, resp.sql
