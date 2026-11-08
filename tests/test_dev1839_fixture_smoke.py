"""DEV-1839 fixture smoke — the shared fixtures import cleanly, the rank/diff
oracles are recomputable from the base totals, and the dataset-facing oracles
agree with executed values on both backends."""

from __future__ import annotations

import pytest

from tests._dev1839_fixtures import (
    CITY_TOTAL,
    DUAL_MEASURE_RANK_OF,
    EXPLICIT_PART_RANK_OF,
    GRAND_TOTAL,
    KEYLESS_RANK_OF,
    MIXED_DIFF_OF,
    MIXED_RANK_OF,
    ModelMeasure,
    NESTED_RANK_OF,
    OK_REGION_TOTAL,
    RCC_TOTAL,
    REGION_CUMSUM,
    REGION_MONTH_TOTAL,
    REGION_TOTAL,
    SAMEGRAIN_DIFF_OF,
    SAMEGRAIN_RANK_OF,
    SUBSET_RANK_OF,
    make_exec_engine,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _ranks_desc(values: dict) -> dict:
    """RANK() over ``values`` descending (ties share the smallest rank)."""
    ordered = sorted(values.values(), reverse=True)
    return {k: 1 + ordered.index(v) for k, v in values.items()}


class TestOracleConsistency:
    def test_mixed_diff_and_rank(self) -> None:
        assert MIXED_DIFF_OF == {
            (r, c): REGION_TOTAL[r] - CITY_TOTAL[(r, c)] for (r, c) in CITY_TOTAL
        }
        assert MIXED_RANK_OF == _ranks_desc(MIXED_DIFF_OF)

    def test_keyless_rank(self) -> None:
        shares = {r: t / GRAND_TOTAL for r, t in REGION_TOTAL.items()}
        assert KEYLESS_RANK_OF == _ranks_desc(shares)

    def test_subset_rank(self) -> None:
        diffs = {
            (r, c): CITY_TOTAL[(r, c)] - REGION_TOTAL[r] for (r, c) in CITY_TOTAL
        }
        assert SUBSET_RANK_OF == _ranks_desc(diffs)

    def test_region_cumsum_and_nested_rank(self) -> None:
        for (region, month), value in REGION_CUMSUM.items():
            assert value == pytest.approx(sum(
                v for (r, m), v in REGION_MONTH_TOTAL.items()
                if r == region and m <= month
            ))
        nested_vals = {
            (r, c, m): REGION_CUMSUM[(r, m)] - CITY_TOTAL[(r, c)]
            for (r, c, m) in NESTED_RANK_OF
        }
        assert NESTED_RANK_OF == _ranks_desc(nested_vals)

    def test_explicit_partition_rank(self) -> None:
        for region in REGION_TOTAL:
            group = {k: v for k, v in MIXED_DIFF_OF.items() if k[0] == region}
            for key, rank in _ranks_desc(group).items():
                assert EXPLICIT_PART_RANK_OF[key] == rank

    def test_dual_measure_rank(self) -> None:
        broadcast = {
            (r, c, ch): MIXED_DIFF_OF[(r, c)] for (r, c, ch) in RCC_TOTAL
        }
        assert DUAL_MEASURE_RANK_OF == _ranks_desc(broadcast)

    def test_samegrain_oracles(self) -> None:
        sums = {r: REGION_TOTAL[r] + OK_REGION_TOTAL[r] for r in REGION_TOTAL}
        assert SAMEGRAIN_RANK_OF == _ranks_desc(sums)
        assert SAMEGRAIN_DIFF_OF == {
            r: REGION_TOTAL[r] - OK_REGION_TOTAL[r] for r in REGION_TOTAL
        }

    def test_rcc_totals_roll_up(self) -> None:
        assert sum(RCC_TOTAL.values()) == GRAND_TOTAL
        for (r, c), total in CITY_TOTAL.items():
            assert sum(
                v for (r2, c2, _), v in RCC_TOTAL.items() if (r2, c2) == (r, c)
            ) == total


class TestSeededDataset:
    async def test_channel_grain_totals(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region", "city", "channel"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = rows_by(resp, "orders.region", "orders.city", "orders.channel")
        assert set(by) == set(RCC_TOTAL)
        for key, total in RCC_TOTAL.items():
            assert float(by[key]["orders.s"]) == pytest.approx(total)

    async def test_ok_region_totals(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="ok_amount:sum", name="s")],
        ))
        by = rows_by(resp, "orders.region")
        assert {k[0]: float(r["orders.s"]) for k, r in by.items()} == pytest.approx(
            OK_REGION_TOTAL
        )
