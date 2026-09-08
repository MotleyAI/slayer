"""LAW broadcast coherence (semantics.arc42.md §3 law 2): coercing then
combining equals combining then coercing — a formula measure ``a ⊕ b`` matches
its operands queried separately and combined client-side (SQL NULL
propagation), and coercions compose along grain chains."""

from __future__ import annotations

import pytest

from tests._law_harness import (
    OPERANDS,
    ModelMeasure,
    canon,
    make_law_engine,
    month_key,
    month_td,
    pair_params,
    q,
    values_equal,
)


@pytest.fixture(params=pair_params())
async def pair_case(request):
    dialect, pair, grain = request.param
    async for engine, _db_path in make_law_engine(dialect):
        yield engine, pair, grain


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine, _db_path in make_law_engine(request.param):
        yield engine


def _pair_kwargs(pair, grain: str) -> dict:
    if grain == "rc":
        kwargs: dict = {"dimensions": ["region", "city"]}
        if pair.with_month:
            kwargs["time_dimensions"] = month_td()
    else:
        kwargs = {"dimensions": ["region"], "time_dimensions": month_td()}
    return kwargs


def _row_key(row: dict, kwargs: dict) -> tuple:
    key = tuple(row[f"orders.{d}"] for d in kwargs["dimensions"])
    if "time_dimensions" in kwargs:
        key += (month_key(row["orders.ordered_at"]),)
    return key


def _combine(op: str, a, b):
    if a is None or b is None:
        return None
    a, b = float(a), float(b)
    return {"+": a + b, "-": a - b, "*": a * b}[op]


async def test_formula_equals_client_side_combine(pair_case):
    engine, pair, grain = pair_case
    kwargs = _pair_kwargs(pair, grain)
    left = await engine.execute(q(**kwargs, measures=[
        ModelMeasure(formula=f"{OPERANDS[pair.a]} {pair.op} {OPERANDS[pair.b]}",
                     name="x"),
    ]))
    right = await engine.execute(q(**kwargs, measures=[
        ModelMeasure(formula=OPERANDS[pair.a], name="a"),
        ModelMeasure(formula=OPERANDS[pair.b], name="b"),
    ]))
    left_rows = {_row_key(r, kwargs): r["orders.x"] for r in left.data}
    right_rows = {_row_key(r, kwargs): r for r in right.data}
    assert len(left_rows) == len(left.data), "duplicate group keys (left)"
    assert len(right_rows) == len(right.data), "duplicate group keys (right)"
    assert set(left_rows) == set(right_rows), (
        f"LAW broadcast coherence violated — combine moved the row set; "
        f"pair={pair} grain={grain}"
    )
    for key, row in right_rows.items():
        expected = _combine(pair.op, row["orders.a"], row["orders.b"])
        assert values_equal(left_rows[key], expected), (
            f"LAW broadcast coherence violated — cell {key}: formula gave "
            f"{left_rows[key]!r}, client-side combine gave {expected!r}; "
            f"pair={pair} grain={grain}"
        )


async def test_ill_typed_operand_refuses_identically_in_both_phrasings(exec_engine):
    """part(city) outside a city-bearing grain is ill-typed (axiom 6); the
    typed refusal must not depend on phrasing (formula vs separate measures).
    The rm-grain pair sample excludes part_city for exactly this reason."""
    kwargs: dict = {"dimensions": ["region"], "time_dimensions": month_td()}
    refusal = r"partition_by column 'city' is not a query dimension"
    with pytest.raises(ValueError, match=refusal):
        await exec_engine.execute(q(**kwargs, measures=[
            ModelMeasure(
                formula=f"{OPERANDS['part_city']} + {OPERANDS['plain']}",
                name="x",
            ),
        ]))
    with pytest.raises(ValueError, match=refusal):
        await exec_engine.execute(q(**kwargs, measures=[
            ModelMeasure(formula=OPERANDS["part_city"], name="a"),
            ModelMeasure(formula=OPERANDS["plain"], name="b"),
        ]))


async def test_region_grain_operand_agrees_along_the_grain_chain(exec_engine):
    """Coercions compose: one region-grain operand read at (region),
    (region, city), and (region, city, month) yields the same per-region value
    all along the chain."""
    rt = ModelMeasure(formula=OPERANDS["part_region"], name="rt")
    per_region: list[dict] = []
    for kwargs in (
        {"dimensions": ["region"]},
        {"dimensions": ["region", "city"]},
        {"dimensions": ["region", "city"], "time_dimensions": month_td()},
    ):
        resp = await exec_engine.execute(q(**kwargs, measures=[rt]))
        values: dict = {}
        for row in resp.data:
            values.setdefault(row["orders.region"], set()).add(
                canon(row["orders.rt"]),
            )
        for region, seen in values.items():
            assert len(seen) == 1, (
                f"LAW broadcast coherence violated — region-grain operand not "
                f"constant within region {region!r} at {kwargs}: {seen}"
            )
        per_region.append({region: next(iter(seen)) for region, seen in values.items()})
    assert per_region[0] == per_region[1] == per_region[2], (
        f"LAW broadcast coherence violated — coercion chain disagrees: "
        f"{per_region}"
    )
