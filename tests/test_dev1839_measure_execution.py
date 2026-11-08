"""DEV-1839 executed ground truth (SQLite + DuckDB) for grain-union
broadcasting in MEASURE and FILTER contexts — the ``queries/
partitioned-aggregates`` delta scenarios. Mixed- and same-grain bare composite
arithmetic is the D10 lift (fails with a leaked-placeholder error today);
plain+partitioned mix, transform-as-measure, and filter routing are locking.

Scenario coverage map (spec: openspec …/specs/queries/partitioned-aggregates):
  Mixed-grain arithmetic as a measure ......... TestCompositeMeasureLift
  Same-grain partitioned arithmetic ........... TestCompositeMeasureLift
  Plain and partitioned aggregates mix ........ TestConformingSurfaces
  Transform over mixed-grain arithmetic ....... TestConformingSurfaces
  Filter over mixed-grain arithmetic .......... TestConformingSurfaces
"""

from __future__ import annotations

import pytest

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1839_fixtures import (
    CITY_TOTAL,
    GRAND_TOTAL,
    MEASURE_DIFF,
    MIXED_DIFF_OF,
    MIXED_RANK,
    MIXED_RANK_OF,
    ModelMeasure,
    REGION_TOTAL,
    SAMEGRAIN_DIFF,
    SAMEGRAIN_DIFF_OF,
    make_exec_engine,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


async def _dry_scope_closed(engine, query, dialect: str) -> None:
    dry = await engine.execute(query, dry_run=True)
    assert dry.sql is not None
    assert "__regroup__" not in dry.sql, f"placeholder leaked:\n{dry.sql}"
    assert_scope_closed(dry.sql, dialect=dialect)


class TestCompositeMeasureLift:
    """D10 — bare composite arithmetic over two-plus partitioned aggregates."""

    async def test_mixed_grain_arithmetic_as_measure(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula=MEASURE_DIFF, name="d"),
                ModelMeasure(formula="amount:sum", name="s"),
            ],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(MIXED_DIFF_OF)
        assert len(resp.data) == len(MIXED_DIFF_OF)
        for key, r in by.items():
            assert float(r["orders.d"]) == pytest.approx(MIXED_DIFF_OF[key]), f"{key}"
            assert float(r["orders.s"]) == pytest.approx(CITY_TOTAL[key]), f"{key}"
        await _dry_scope_closed(engine, query, dialect)

    async def test_same_grain_partitioned_arithmetic_as_measure(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=SAMEGRAIN_DIFF, name="d")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region")
        assert len(resp.data) == len(SAMEGRAIN_DIFF_OF)
        for key, r in by.items():
            assert float(r["orders.d"]) == pytest.approx(
                SAMEGRAIN_DIFF_OF[key[0]]
            ), f"{key}"
        await _dry_scope_closed(engine, query, dialect)

    async def test_keyless_operand_broadcasts_in_measure(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(
                formula="amount:sum(partition_by=region) - "
                        "amount:sum(partition_by=[])",
                name="d",
            )],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(CITY_TOTAL)
        for key, r in by.items():
            assert float(r["orders.d"]) == pytest.approx(
                REGION_TOTAL[key[0]] - GRAND_TOTAL
            ), f"{key}"
        await _dry_scope_closed(engine, query, dialect)

    async def test_scalar_call_over_partitioned_operands(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=f"abs({MEASURE_DIFF})", name="d")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(MIXED_DIFF_OF)
        for key, r in by.items():
            assert float(r["orders.d"]) == pytest.approx(
                abs(MIXED_DIFF_OF[key])
            ), f"{key}"
        await _dry_scope_closed(engine, query, dialect)

    async def test_case_over_partitioned_operands(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(
                formula=f"CASE WHEN {MEASURE_DIFF} > 0 THEN 1 ELSE 0 END",
                name="d",
            )],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(MIXED_DIFF_OF)
        for key, r in by.items():
            assert int(r["orders.d"]) == (1 if MIXED_DIFF_OF[key] > 0 else 0), f"{key}"
        await _dry_scope_closed(engine, query, dialect)


class TestConformingSurfaces:
    async def test_plain_and_partitioned_mix(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(
                formula="amount:sum - amount:sum(partition_by=region)", name="d",
            )],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(CITY_TOTAL)
        for key, r in by.items():
            assert float(r["orders.d"]) == pytest.approx(
                CITY_TOTAL[key] - REGION_TOTAL[key[0]]
            ), f"{key}"
        await _dry_scope_closed(engine, query, dialect)

    async def test_transform_over_mixed_as_measure(self, exec_backend) -> None:
        # At the (region, city) query grain the measure rank coincides with the
        # union-grain rank; the dual-role test (union_dim_execution) separates
        # the two on a finer grain.
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula=MIXED_RANK, name="r"),
                ModelMeasure(formula="amount:sum", name="s"),
            ],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(MIXED_RANK_OF)
        for key, r in by.items():
            assert int(r["orders.r"]) == MIXED_RANK_OF[key], f"{key}"

        solo = await engine.execute(q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        solo_by = rows_by(solo, "orders.region", "orders.city")
        assert set(solo_by) == set(by)
        for key, r in by.items():
            assert float(r["orders.s"]) == pytest.approx(
                float(solo_by[key]["orders.s"])
            ), f"{key}"
        await _dry_scope_closed(engine, query, dialect)

    async def test_filter_over_mixed_arithmetic(self, exec_backend) -> None:
        dialect, engine = exec_backend
        unfiltered = rows_by(await engine.execute(q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        query = q(
            dimensions=["region", "city"],
            filters=[f"{MEASURE_DIFF} > 0"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == {k for k, v in MIXED_DIFF_OF.items() if v > 0}
        for key, r in by.items():
            assert float(r["orders.s"]) == pytest.approx(
                float(unfiltered[key]["orders.s"])
            ), f"{key}"
        await _dry_scope_closed(engine, query, dialect)

    async def test_filtered_expression_also_selected(self, exec_backend) -> None:
        # Codex (test review): selecting the filtered expression itself pins the
        # filter's routing — surviving d values must equal both the oracle and
        # the unfiltered run. Needs D10, so this variant is TDD, not locking.
        _, engine = exec_backend
        measures = [
            ModelMeasure(formula="amount:sum", name="s"),
            ModelMeasure(formula=MEASURE_DIFF, name="d"),
        ]
        unfiltered = rows_by(await engine.execute(q(
            dimensions=["region", "city"], measures=measures,
        )), "orders.region", "orders.city")
        resp = await engine.execute(q(
            dimensions=["region", "city"],
            filters=[f"{MEASURE_DIFF} > 0"],
            measures=measures,
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == {k for k, v in MIXED_DIFF_OF.items() if v > 0}
        for key, r in by.items():
            assert float(r["orders.d"]) == pytest.approx(MIXED_DIFF_OF[key]), f"{key}"
            assert float(r["orders.d"]) == pytest.approx(
                float(unfiltered[key]["orders.d"])
            ), f"{key}"
