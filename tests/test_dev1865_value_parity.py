"""DEV-1865 — position values agree with measure position.

The twin-query law: a query Q that filters on a measure-typed predicate E keeps
exactly the cells that a query Q' declaring E as a measure (no filter) reports
as passing E, with every shared column value identical. Run on SQLite + DuckDB.

The cross-model-partitioned shape is currently red (the DEV-1824 guard); the
plain / cross-model / windowed shapes exercise the same law and pin the
value-preserving invariant.
"""

from __future__ import annotations

import pytest

from tests._dev1865_fixtures import ModelMeasure, month_td, q


def _rowkey(row, dim_cols):
    return tuple(row[c] for c in dim_cols)


async def _twin_query_law(engine, *, dims, value_formula, threshold, td=None):
    """Assert Q (filter E>threshold) == Q' cells where E>threshold, values equal."""
    base = {"dimensions": dims}
    if td is not None:
        base["time_dimensions"] = td
    display = [ModelMeasure(formula="amount:sum", name="s")]
    filtered = (await engine.execute(q(
        **base, measures=display, filters=[f"{value_formula} > {threshold}"],
    ))).data
    twin = (await engine.execute(q(
        **base, measures=display + [ModelMeasure(formula=value_formula, name="e")],
    ))).data
    assert filtered, "threshold kept no rows — the law would be vacuous"
    dim_cols = sorted(set(filtered[0]) - {"orders.s"})
    (e_col,) = set(twin[0]) - set(filtered[0])
    twin_by = {_rowkey(r, dim_cols): r for r in twin}
    expected = {
        k for k, r in twin_by.items()
        if r[e_col] is not None and float(r[e_col]) > threshold
    }
    assert {_rowkey(r, dim_cols) for r in filtered} == expected
    for r in filtered:
        twin_row = twin_by[_rowkey(r, dim_cols)]
        assert float(r["orders.s"]) == pytest.approx(float(twin_row["orders.s"]))


_SHAPES = [
    pytest.param(["region"], "amount:sum", 60, None, id="plain-local"),
    pytest.param(["customers.tier"], "customers.spend:sum", 175, None, id="cross-model"),
    pytest.param(
        ["customers.regions.name", "customers.tier"],
        "customers.spend:sum(partition_by=customers.regions.name)", 100, None,
        id="cross-model-partitioned",
    ),
    pytest.param(
        ["region"], "amount:sum(window='90d', partition_by=region)", 40, month_td(),
        id="windowed",
    ),
]


class TestTwinQueryLaw:
    @pytest.mark.parametrize("dims, value_formula, threshold, td", _SHAPES)
    async def test_measure_typed_filter_agrees_with_measure_position(
        self, exec_engine, dims, value_formula, threshold, td,
    ) -> None:
        await _twin_query_law(
            exec_engine, dims=dims, value_formula=value_formula,
            threshold=threshold, td=td,
        )


class TestNullPredicateDropsRow:
    async def test_null_measure_predicate_drops_every_cell(self, exec_engine) -> None:
        # ``nomatch`` sums a never-true filtered column → NULL for every group;
        # NULL > 0 is NULL, so WHERE/HAVING drops the cell.
        twin = (await exec_engine.execute(q(
            dimensions=["region"],
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(formula="nomatch:sum", name="e"),
            ],
        ))).data
        assert twin
        assert all(r["orders.e"] is None for r in twin)
        filtered = (await exec_engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            filters=["nomatch:sum > 0"],
        ))).data
        assert filtered == []


class TestPositionSupportCannotLag:
    async def test_measure_legal_expression_filters_and_orders(self, exec_engine) -> None:
        # Once an expression executes as a declared measure, the same expression
        # must be accepted as a filter and as an order target — no extra support.
        expr = "customers.spend:sum(partition_by=customers.regions.name)"
        dims = ["customers.regions.name", "customers.tier"]
        as_measure = (await exec_engine.execute(q(
            dimensions=dims, measures=[ModelMeasure(formula=expr, name="e")],
        ))).data
        assert as_measure
        as_filter = (await exec_engine.execute(q(
            dimensions=dims,
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
            filters=[f"{expr} > 100"],
        ))).data
        assert 0 < len(as_filter) <= len(as_measure)
        ordered = (await exec_engine.execute(q(
            dimensions=dims,
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
            order=[{"column": expr, "direction": "desc"}],
        ))).data
        assert len(ordered) == len(as_measure)
        # Sorted by the region partition total desc (RegN=300 before RegS=50);
        # the value itself stays hidden.
        rank = {"RegN": 0, "RegS": 1}
        regions = [r["orders.customers.regions.name"] for r in ordered]
        assert regions == sorted(regions, key=lambda r: rank[r])
