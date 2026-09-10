"""LAW dice–slice under associate (semantics.arc42.md §3 law 5): filtering the
population to ``d = v`` yields the same aggregate value as slicing the ``v``
cell of the same query grouped by ``d``. Holds under ``to_many_handling:
"associate"`` for every reachable dimension; the broadcast default does not
satisfy it (its warning hint is the disclosure).

Spec: openspec …/specs/queries/semantics — "Dice–slice correspondence under
associate". Generated over a measure × dimension × value family on SQLite +
DuckDB; the two runs are compared to each other, so no hand oracle is needed.
"""

from __future__ import annotations

import pytest

from tests._dev1841_fixtures import (
    ModelMeasure,
    assoc_q,
    make_exec_engine,
    rows_by,
)

#: metric root spans local column, star-count, deep hop, and percentile family.
MEASURES = {
    "spend": "customers.spend:sum",
    "count": "customers.*:count",
    "pop": "customers.regions.pop:sum",
    "median": "customers.spend:median",
}
#: reachable dimensions and their (host-local) values.
DIMENSIONS = {"status": ["ok", "new"], "channel": ["app", "web"]}

FAMILY = [
    pytest.param(m, d, v, id=f"{m}-{d}-{v}")
    for m in MEASURES
    for d, values in DIMENSIONS.items()
    for v in values
]


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.mark.parametrize("measure_key,dim,value", FAMILY)
async def test_filtered_value_equals_sliced_cell(exec_backend, measure_key, dim, value):
    _, engine = exec_backend
    measure = ModelMeasure(formula=MEASURES[measure_key], name="m")

    filtered = await engine.execute(
        assoc_q(measures=[measure], filters=[f"{dim} = '{value}'"]))
    sliced = await engine.execute(
        assoc_q(dimensions=[dim], measures=[measure]))

    filtered_value = float(filtered.data[0]["orders.m"])
    sliced_cell = rows_by(sliced, f"orders.{dim}")[(value,)]["orders.m"]
    assert filtered_value == pytest.approx(float(sliced_cell)), (
        f"LAW dice–slice violated for {measure_key} on {dim}={value!r}: "
        f"filtered {filtered_value} != sliced {sliced_cell}")
