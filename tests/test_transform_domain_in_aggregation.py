"""A transform inside an aggregation evaluates over its operand's domain (spec: queries/transforms).

City seed of ``test_transform_operand_grain``: city totals A 35, B 70, C 50, D 60; region totals North 105 (rank 2),
South 110 (rank 1); region-month totals North 40 and 65, South 70 and 40. Appendix B cases use ``_dev2212_fixtures``.
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import SlayerError
from slayer.core.query import SlayerQuery
from slayer.engine.compile import stages
from slayer.engine.elaborate import elaborate_query
from slayer.ir.elaborated import ElaboratedStage
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev2212_fixtures import BACKENDS, kinds, run, seed_engine
from tests.test_transform_operand_grain import (
    AT_RC,
    AT_RCM,
    AT_RM,
    BY_REGION,
    FEB,
    JAN,
    MONTH,
    RC,
    RCM,
    RM,
    WIN_AT_RM,
    A,
    C,
    R,
    _assert_cells,
    _by,
    _cities,
    _city,
    _city_months,
    _mt,
    _region_months,
    city_dataset,
)

RANK = f"rank({BY_REGION}, direction='desc')"
RANK_OF_MAX = f"rank(max({AT_RC}, partition_by={R}), direction='desc')"
RANK_OF_LAST = f"rank(last({WIN_AT_RM}), direction='desc')"
PERCENT_RANK = f"percent_rank({BY_REGION})"


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _regions(north: Any, south: Any, keys: tuple[str, ...] = RC) -> dict:
    """``north`` / ``south`` on every cell of the seed at ``keys``."""
    if keys == RC:
        return _cities(north, north, south, south)
    if keys == RM:
        return _region_months(north, north, south, south)
    return {"North": north, "South": south}


# --------------------------------------------------------------------------- #
# As an aggregation source
# --------------------------------------------------------------------------- #
async def test_a_coarser_operand_ranks_its_own_cells(backend: str) -> None:
    resp = await _city(backend, RC, _mt(RANK, "r"), _mt(f"max({RANK})", "m"))
    _assert_cells(_by(resp, RC, "r"), _regions(2, 1))
    _assert_cells(_by(resp, RC, "m"), _regions(2, 1))
    associated = kinds(resp, "associated")
    assert associated
    assert any("city" in str(w) for w in associated), associated


@pytest.mark.parametrize(("expr", "keys", "expected"), [
    (f"max({RANK_OF_MAX})", RC, _regions(1, 2)),
    (f"max({RANK_OF_LAST})", RM, _regions(2, 1, RM)),
    (f"max({RANK} + {AT_RC})", ("orders.region",), {"North": 72, "South": 61}),
    (f"sum({RANK})", RC, _regions(2, 1)),
    (f"max({PERCENT_RANK})", RC, _regions(0, 1)),
], ids=["re-aggregated-operand", "windowed-operand-under-last", "finer-sibling", "sum-of-rank", "percent-rank"])
async def test_an_aggregation_source_reads_the_transforms_cells(backend: str, expr: str, keys: tuple[str, ...],
                                                                expected: dict) -> None:
    _assert_cells(_by(await _city(backend, keys, _mt(expr)), keys), expected)


# --------------------------------------------------------------------------- #
# As an aggregation parameter
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("weight", "expected"), [
    (RANK, 320 / 6),
    (RANK_OF_MAX, 325 / 6),
    (PERCENT_RANK, 55),
], ids=["rank", "rank-of-re-aggregated", "percent-rank"])
async def test_an_aggregation_parameter_reads_the_transforms_cells(backend: str, weight: str,
                                                                   expected: float) -> None:
    resp = await _city(backend, (), _mt(f"weighted_avg({AT_RC}, weight={weight})"))
    assert len(resp.data) == 1, resp.data
    assert float(resp.data[0]["m"]) == pytest.approx(expected, abs=1e-3)


async def test_a_windowed_parameter_reads_the_transforms_cells(backend: str) -> None:
    resp = await _city(backend, (MONTH,), _mt(f"weighted_avg({AT_RCM}, weight={RANK_OF_LAST})"))
    _assert_cells(_by(resp, (MONTH,)), {JAN: 25, FEB: 34})


# --------------------------------------------------------------------------- #
# Filter and order positions
# --------------------------------------------------------------------------- #
async def test_a_filter_reads_the_metric_position_value(backend: str) -> None:
    resp = await _city(backend, RC, _mt(f"sum({A})"), filter=[f"max({RANK}) = 2"])
    _assert_cells(_by(resp, RC), {("North", "A"): 35, ("North", "B"): 70})


async def test_an_order_key_reads_the_metric_position_value(backend: str) -> None:
    resp = await _city(backend, RC, _mt(f"sum({A})"), order=[{"field": f"max({RANK})", "direction": "asc"}])
    assert [r[R] for r in resp.data] == ["South", "South", "North", "North"]


# --------------------------------------------------------------------------- #
# Transforms already evaluated over their own cells keep their values
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("expr", "keys", "expected"), [
    (f"sum({A} * {RANK})", RC, _cities(70, 140, 50, 60)),
    (f"max(cumsum({AT_RM}) + 0 * {AT_RCM})", RM, _region_months(40, 105, 70, 110)),
    (f"max(time_shift(cumsum({AT_RM}), -1))", RCM, _city_months(None, 40, None, 40, None, None, 70)),
    (f"max(time_shift(rank({AT_RM}, direction='desc'), -1))", RCM, _city_months(None, 3, None, 3, None, None, 1)),
], ids=["row-level-source", "cumsum-beside-a-finer-sibling", "time-shift-of-cumsum", "time-shift-of-rank"])
async def test_transforms_already_over_their_own_cells_keep_their_values(backend: str, expr: str,
                                                                         keys: tuple[str, ...],
                                                                         expected: dict) -> None:
    _assert_cells(_by(await _city(backend, keys, _mt(expr)), keys), expected)


async def test_a_carrier_keeps_cells_only_the_transforms_home_reaches(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=["customers.region"],
                         metrics=[_mt("sum(rank(sum(returns.amount), direction='desc'))")])
    _assert_cells(_by(resp, ("customers.region",)), {"EAST": 1, "NORTH": 2})


@pytest.mark.xfail(strict=True, reason="DEV-2511: the transform ranks the sibling dataset's cells")
@pytest.mark.parametrize(("transform", "expected"), [
    ("rank(sum(returns.amount), direction='asc')", {"EAST": 2, "NORTH": 1}),
    ("percent_rank(sum(returns.amount))", {"EAST": 1.0, "NORTH": 0.0}),
    ("rank(count(returns.return_id), direction='asc')", {"EAST": 1, "NORTH": 1}),
], ids=["rank", "percent-rank", "tied-rank"])
async def test_a_sibling_from_another_dataset_keeps_the_transforms_cells(backend: str, transform: str,
                                                                         expected: dict) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=["customers.region"],
                         metrics=[_mt(f"sum({transform} + 0 * count(orders.order_id))")])
    got = _by(resp, ("customers.region",))
    # A cell only the sibling supplies has no transform value.
    assert {k: got.get(k) for k in {*expected, *got}} == {**dict.fromkeys(got), **expected}, got


# --------------------------------------------------------------------------- #
# Plan-time invariant: a transform evaluated over a plan's rows has its operand grain
# --------------------------------------------------------------------------- #
def _elaborated(metric: str, *dimensions: Any) -> ElaboratedStage:
    query = SlayerQuery.model_validate({"population": "orders", "dimensions": list(dimensions),
                                        "metrics": [_mt(metric)]})
    return elaborate_query(query=query, bundle=ResolvedSourceBundle(
        dialect="duckdb", population=city_dataset(), referenced_datasets=[]))


def _check_in_place(elab: ElaboratedStage) -> None:
    answer = elab.prebound.declared_metrics[-1].bound.value_key
    stages._assert_in_place_transform_grains(elab.prebound, in_place=frozenset({answer}))


def test_a_transform_over_rows_finer_than_its_operand_is_refused_at_plan_time() -> None:
    elab = _elaborated(RANK, R, C)
    with pytest.raises(ValueError) as ei:
        _check_in_place(elab)
    assert not isinstance(ei.value, SlayerError), "an internal invariant, not a user-facing error"


@pytest.mark.parametrize("dimensions", [
    (R, C),
    (R, C, {"field": AT_RC, "alias": "total"}),
], ids=["same-grain", "determined-member-pruned"])
def test_a_transform_over_rows_at_its_operand_grain_is_accepted(dimensions: tuple[Any, ...]) -> None:
    _check_in_place(_elaborated(f"rank({AT_RC}, direction='desc')", *dimensions))
