"""DEV-1847 task 1.6 — equivalence sweep vs the manual multi-stage
``source_queries`` encoding, plus the no-placeholder-leak invariant (SQLite +
DuckDB). For each outer aggregation, the re-aggregation must return the same
executed values as stage-1-sum / stage-2-reaggregate. Fails until the feature
lands.
"""

from __future__ import annotations

import pytest

from tests._dev1847_fixtures import (
    INNER_CR,
    ModelMeasure,
    SlayerModel,
    SlayerQuery,
    gen,
    make_exec_engine,
    reagg,
    sales_q,
)

OUTER_AGGS = ["avg", "count", "min", "max", "median"]


def _sq_model(outer: str) -> SlayerModel:
    """Two-stage encoding of ``<outer>(sum(amount, partition_by=[city,region]))``
    grouped by region."""
    return SlayerModel(
        name=f"sq_{outer}", data_source="test",
        source_queries=[
            SlayerQuery(
                name=f"s1_{outer}", source_model="sales",
                dimensions=["region", "city"],
                measures=[ModelMeasure(formula="amount:sum", name="ct")]),
            SlayerQuery(
                source_model=f"s1_{outer}", dimensions=["region"],
                measures=[ModelMeasure(formula=f"ct:{outer}", name="v")]),
        ])


def _by_region_suffix(resp, suffix):
    rcol = next(c for c in resp.columns if c.endswith(".region"))
    vcol = next(c for c in resp.columns if c.endswith(suffix))
    return {row[rcol]: row[vcol] for row in resp.data}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    from tests._dev1847_fixtures import dev1847_models
    models = dev1847_models() + [_sq_model(o) for o in OUTER_AGGS]
    async for engine in make_exec_engine(request, models=models):
        yield engine


class TestEquivalenceVsSourceQueries:
    @pytest.mark.parametrize("outer", OUTER_AGGS)
    async def test_reaggregation_matches_manual_encoding(self, exec_engine, outer):
        """Each re-aggregation returns the manual multi-stage encoding's values,
        region by region."""
        reagg_resp = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[reagg(outer, INNER_CR, name="v")]))
        manual_resp = await exec_engine.execute(f"sq_{outer}")
        got = _by_region_suffix(reagg_resp, ".v")
        want = _by_region_suffix(manual_resp, ".v")
        assert set(got) == set(want)
        for region in want:
            g, w = got[region], want[region]
            if w is None:
                assert g is None
            else:
                assert float(g) == pytest.approx(float(w))


class TestNoPlaceholderLeak:
    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb", "postgres", "bigquery"])
    async def test_reaggregation_sql_has_no_placeholder_leak(self, dialect):
        """The emitted SQL never leaks a ``__regroup__`` placeholder column."""
        sql = await gen(sales_q(
            dimensions=["region"],
            measures=[reagg("avg", INNER_CR, name="acr")]), dialect=dialect)
        assert "__regroup__" not in sql, sql
