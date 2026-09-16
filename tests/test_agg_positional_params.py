"""Positional aggregation parameters fold onto declared parameter order
(spec: aggregations/functional-form — "Positional parameters fold onto
declared parameter order"). ``percentile(x, 0.9)`` ≡ ``p=0.9`` for built-in,
custom, colon, and re-aggregation-outer spellings; duplicates and excess
positionals fail closed; first/last ranking columns are untouched."""

from __future__ import annotations

import pytest

from tests._dev1847_fixtures import (
    INNER_CR,
    ModelMeasure,
    make_exec_engine,
    region_key,
    sales_q,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


async def _rows(engine, *measures: str) -> list[dict]:
    resp = await engine.execute(sales_q(
        dimensions=["region"], measures=list(measures)))
    return sorted(resp.data, key=lambda r: (r["sales.region"] is None,
                                            str(r["sales.region"])))


class TestPositionalEqualsNamed:
    async def test_percentile_functional(self, exec_engine):
        pos = await _rows(exec_engine, "percentile(amount, 0.9)")
        named = await _rows(exec_engine, "percentile(amount, p=0.9)")
        assert pos == named  # identical values AND result keys

    async def test_percentile_colon(self, exec_engine):
        pos = await _rows(exec_engine, "amount:percentile(0.9)")
        named = await _rows(exec_engine, "amount:percentile(p=0.9)")
        assert pos == named

    async def test_custom_aggregation_declared_order(self, exec_engine):
        pos = await _rows(exec_engine, "wavg(amount, id)")
        named = await _rows(exec_engine, "wavg(amount, weight=id)")
        assert pos == named

    async def test_reaggregation_outer(self, exec_engine):
        pos = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"percentile({INNER_CR}, 0.9)",
                                   name="p")]))
        named = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"percentile({INNER_CR}, p=0.9)",
                                   name="p")]))
        assert region_key(pos) == region_key(named)


class TestPositionalErrors:
    async def test_duplicate_positional_and_named(self, exec_engine):
        with pytest.raises(ValueError, match="both positionally and by name"):
            await _rows(exec_engine, "percentile(amount, 0.9, p=0.5)")

    async def test_excess_positionals(self, exec_engine):
        with pytest.raises(ValueError, match="takes at most 1 parameter"):
            await _rows(exec_engine, "percentile(amount, 0.9, 0.5)")


class TestRankedPositionalUntouched:
    async def test_last_ranking_column_still_positional(self, exec_engine):
        rows = await _rows(exec_engine, "last(amount, id)")
        by_region = {r["sales.region"]: r["sales.amount_last_id"] for r in rows}
        # last-by-id per region from the fixture rows.
        assert by_region["North"] == pytest.approx(60.0)
        assert by_region["East"] == pytest.approx(80.0)
