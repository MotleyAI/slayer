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

from tests._dev1740_fixtures import REGION_SUM, gen, make_exec_engine, rows_by


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
        assert "TEXT" in msg and "INT" in msg

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
