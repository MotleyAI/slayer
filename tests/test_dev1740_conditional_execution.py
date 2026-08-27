"""DEV-1740 Part A — conditionals executed on SQLite AND DuckDB.

A conditional over an aggregate is a POST/AGGREGATE-phase measure (it bands
whole groups); nested conditionals give an ordinal. A CASE living in a
``Column.sql`` (Mode-A raw SQL) must keep working — pinned so Part A does not
regress the existing escape hatch. Expectations are hand-computed from
``tests/_dev1740_fixtures.py``.
"""

from __future__ import annotations

import pytest

from slayer.core.query import ModelMeasure, SlayerQuery

from tests._dev1740_fixtures import (
    REGION_SUM,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


class TestConditionalMeasureOverAggregate:
    async def test_simple_band_over_group_total(self, exec_engine) -> None:
        # EU total 14000 >= 10000 -> 1 ; US total 8000 -> 0.
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[
                ModelMeasure(formula="amount:sum", name="rev"),
                ModelMeasure(
                    formula="CASE WHEN amount:sum >= 10000 THEN 1 ELSE 0 END",
                    name="big",
                ),
            ],
        ))
        by = rows_by(resp, "orders.region")
        assert float(by[("EU",)]["orders.rev"]) == pytest.approx(REGION_SUM["EU"])
        assert int(by[("EU",)]["orders.big"]) == 1
        assert int(by[("US",)]["orders.big"]) == 0

    async def test_nested_conditional_is_an_ordinal(self, exec_engine) -> None:
        # EU 14000 -> 2 ; US 8000 -> 1.
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[
                ModelMeasure(
                    formula=(
                        "CASE WHEN amount:sum >= 14000 THEN 2 "
                        "WHEN amount:sum >= 8000 THEN 1 ELSE 0 END"
                    ),
                    name="tier",
                ),
            ],
        ))
        by = rows_by(resp, "orders.region")
        assert int(by[("EU",)]["orders.tier"]) == 2
        assert int(by[("US",)]["orders.tier"]) == 1

    async def test_iif_spelling_matches_case(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[
                ModelMeasure(formula="iif(amount:sum >= 10000, 1, 0)", name="big"),
            ],
        ))
        by = rows_by(resp, "orders.region")
        assert int(by[("EU",)]["orders.big"]) == 1
        assert int(by[("US",)]["orders.big"]) == 0


class TestConditionalInFilter:
    async def test_case_predicate_removes_a_group(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            filters=["CASE WHEN amount:sum >= 10000 THEN 1 ELSE 0 END == 1"],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        ))
        regions = {r["orders.region"] for r in resp.data}
        assert regions == {"EU"}


class TestNullBranchKeepsValue:
    async def test_null_else_yields_null_not_dropped(self, exec_engine) -> None:
        # US falls to the (missing) ELSE -> NULL, but the US row must remain.
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[
                ModelMeasure(formula="amount:sum", name="rev"),
                ModelMeasure(
                    formula="CASE WHEN amount:sum >= 10000 THEN 1 END", name="big",
                ),
            ],
        ))
        by = rows_by(resp, "orders.region")
        assert set(by) == {("EU",), ("US",)}
        assert int(by[("EU",)]["orders.big"]) == 1
        assert by[("US",)]["orders.big"] is None


class TestBranchTypingThroughPlanning:
    async def test_incomparable_branches_raise_naming_both_types(self) -> None:
        # THEN 'big' (TEXT) vs ELSE 0 (INT) — a plan-time error naming both.
        q = _q(
            dimensions=["region"],
            measures=[
                ModelMeasure(
                    formula="CASE WHEN amount:sum >= 100 THEN 'big' ELSE 0 END",
                    name="bad",
                ),
            ],
        )
        with pytest.raises(ValueError) as ei:
            await gen(q)
        msg = str(ei.value).upper()
        assert "TEXT" in msg
        assert "INT" in msg

    async def test_numeric_widened_branches_execute(self, exec_engine) -> None:
        # INT / DOUBLE branches widen to DOUBLE and execute cleanly.
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[
                ModelMeasure(
                    formula="CASE WHEN amount:sum >= 10000 THEN 1 ELSE 0.5 END",
                    name="w",
                ),
            ],
        ))
        by = rows_by(resp, "orders.region")
        assert float(by[("EU",)]["orders.w"]) == pytest.approx(1.0)
        assert float(by[("US",)]["orders.w"]) == pytest.approx(0.5)


class TestOrderByCaseComposite:
    async def test_order_by_bare_case_expression(self, exec_engine) -> None:
        # ORDER BY a CASE composite that is NOT projected — it must get a hidden
        # slot and sort correctly. EU total 17000 > 12000 → 0 (first); US → 1.
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
            order=[{"column": "CASE WHEN amount:sum > 12000 THEN 0 ELSE 1 END",
                    "direction": "asc"}],
        ))
        regions = [r["orders.region"] for r in resp.data]
        assert regions == ["EU", "US"]


class TestConditionalOverCrossModelAggregate:
    async def test_case_over_joined_measure(self, exec_engine) -> None:
        # CASE over a cross-model aggregate (customers.spend:sum lives in a _cm_
        # CTE). By tier: gold=150 → 0, silver=500 → 1.
        resp = await exec_engine.execute(_q(
            dimensions=["customers.tier"],
            measures=[
                ModelMeasure(
                    formula="CASE WHEN customers.spend:sum > 300 THEN 1 ELSE 0 END",
                    name="b",
                ),
            ],
        ))
        by = {r["orders.customers.tier"]: int(r["orders.b"]) for r in resp.data}
        assert by == {"gold": 0, "silver": 1}


class TestConditionalNestedInScalarCall:
    async def test_conditional_inside_coalesce(self, exec_engine) -> None:
        # An iif as a scalar-call arg must render (not be mistaken for
        # a literal) and its inner aggregate must be discovered.
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[
                ModelMeasure(
                    formula="coalesce(iif(amount:sum >= 10000, 1, 0), -1)",
                    name="c",
                ),
            ],
        ))
        by = rows_by(resp, "orders.region")
        assert int(by[("EU",)]["orders.c"]) == 1
        assert int(by[("US",)]["orders.c"]) == 0


class TestCaseInModelFormula:
    async def test_case_in_a_model_measure_formula(self, exec_engine) -> None:
        # A CASE living in a model-level measure formula (added via extension),
        # referenced by name — pins "valid in model formulas".
        resp = await exec_engine.execute(SlayerQuery(
            source_model={
                "source_name": "orders",
                "measures": [{
                    "formula": "CASE WHEN amount:sum >= 10000 THEN 1 ELSE 0 END",
                    "name": "big",
                }],
            },
            dimensions=["region"],
            measures=[ModelMeasure(formula="big")],
        ))
        by = rows_by(resp, "orders.region")
        assert int(by[("EU",)]["orders.big"]) == 1
        assert int(by[("US",)]["orders.big"]) == 0


class TestIifWrongArityAtBind:
    async def test_two_arg_iif_raises_naming_iif(self) -> None:
        # Arity is enforced by the generic scalar path at bind time (3, 3).
        q = _q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="iif(amount:sum >= 10000, 1)", name="b")],
        )
        with pytest.raises(ValueError, match=r"iif"):
            await gen(q)


class TestConditionalFilterPullsJoin:
    async def test_case_over_joined_column_in_filter(self, exec_engine) -> None:
        # A ROW-phase CASE over a JOINED column in a filter must pull the
        # customers join (gold customers 1 & 3 → orders 1, 2, 5, 6 — all EU).
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            filters=["CASE WHEN customers.tier = 'gold' THEN 1 ELSE 0 END == 1"],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        ))
        by = rows_by(resp, "orders.region")
        assert set(by) == {("EU",)}
        assert float(by[("EU",)]["orders.rev"]) == pytest.approx(8000.0)


class TestConditionalOverTransform:
    async def test_case_over_change_transform(self, exec_engine) -> None:
        # A CASE over a transform must plan and execute like the scalar-call
        # equivalent (``coalesce(change(...), 0)``). Months: Jan 9200,
        # Feb 14000, Mar 1800 → change NULL, +4800, -12200 → up 0, 1, 0.
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(
                    formula="CASE WHEN change(amount:sum) > 0 THEN 1 ELSE 0 END",
                    name="up",
                ),
            ],
        ))
        by = {month_key(r["orders.ordered_at"]): int(r["orders.up"])
              for r in resp.data}
        assert by == {"2024-01": 0, "2024-02": 1, "2024-03": 0}


class TestChangeOverComputedDimension:
    async def test_shift_partitions_by_the_computed_dimension(self, exec_engine) -> None:
        # The shifted CTE must GROUP BY / join back on the computed dimension,
        # not just the time bucket. band = amount > 5000 (only row 9, Feb).
        # band0 monthly sums: Jan 9200, Feb 8000, Mar 1800 → change NULL,
        # -1200, -6200; band1: Feb 6000 → change NULL (no band1 January).
        resp = await exec_engine.execute(_q(
            dimensions=[{"expression": "CASE WHEN amount > 5000 THEN 1 ELSE 0 END",
                         "name": "band"}],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="rev"),
                ModelMeasure(formula="change(amount:sum)", name="chg"),
            ],
        ))
        by = {(int(r["orders.band"]), month_key(r["orders.ordered_at"])): r
              for r in resp.data}
        assert by[(0, "2024-01")]["orders.chg"] is None
        assert float(by[(0, "2024-02")]["orders.chg"]) == pytest.approx(-1200.0)
        assert float(by[(0, "2024-03")]["orders.chg"]) == pytest.approx(-6200.0)
        assert by[(1, "2024-02")]["orders.chg"] is None


class TestCaseInColumnSql:
    async def test_mode_a_case_column_still_executes(self, exec_engine) -> None:
        # A CASE in Column.sql (raw SQL) grouped as a dimension — the existing
        # escape hatch, pinned against Part A regressions. Row 9 (amount 6000)
        # is the only per-row band 1; everything else bands 0.
        resp = await exec_engine.execute(SlayerQuery(
            source_model={
                "source_name": "orders",
                "columns": [{
                    "name": "row_band",
                    "sql": "CASE WHEN amount > 5000 THEN 1 ELSE 0 END",
                    "type": "INT",
                }],
            },
            dimensions=["row_band"],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        ))
        by = {int(r["orders.row_band"]): float(r["orders.rev"]) for r in resp.data}
        assert by == {0: pytest.approx(19000.0), 1: pytest.approx(6000.0)}
