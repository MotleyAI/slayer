"""Shifted evaluation honours population restriction and cross-model partition keys
(queries/transforms, "Composite-input time_shift"): association-restricted filters,
associate-mode fanning dimensions, an attributable ``customers.tier`` key, and the
fanning-hop key typed error — on Graph A, SQLite + DuckDB. Every oracle is
re-derived from the raw rows by ``TestOraclesFromRawRows``."""

from __future__ import annotations

from collections import defaultdict

import pytest

from tests._dev1832_fixtures import (
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    ModelMeasure,
    make_exec_engine,
    month_key,
    month_td,
    orders_q,
)

SHARE = "amount:sum / amount:sum(partition_by=[ordered_at])"
EVENT_VALUE = "customers.regions.region_events.value"
JAN, FEB, MAR, APR = "2024-01", "2024-02", "2024-03", "2024-04"

#: Filter ``region_events.value > 40`` keeps orders of region-1 customers.
ASSOC_FILTER_SHARE = {("ok", JAN): None, ("new", JAN): None,
                      ("ok", FEB): 13 / 33, ("new", FEB): 20 / 33, ("ok", APR): None}
#: Associate mode by the fanning event value (region 1 → 50, region 2 → 30).
ASSOC_DIM_SHIFT = {(None, MAR): None, (30.0, MAR): None, (30.0, APR): 5.0,
                   (50.0, JAN): None, (50.0, FEB): 33.0, (50.0, APR): None}
#: Prior month's share of the tier's all-time total.
TIER_SHARE = {(None, MAR): None, ("bronze", MAR): None, ("gold", JAN): None,
              ("gold", MAR): None, ("gold", APR): 0.1, ("silver", FEB): None,
              ("silver", APR): None}

#: Distinct ``region_events.value`` per region (region 1 carries two 50s).
_REGION_EVENT_VALUE = {1: 50.0, 2: 30.0}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _cells(resp, dim: str) -> dict:
    out = {}
    for r in resp.data:
        key = r[dim]
        out[(float(key) if isinstance(key, (int, float)) else key,
             month_key(r["orders.ordered_at"]))] = r["orders.t"]
    assert len(out) == len(resp.data), "duplicate result rows for one cell"
    return out


def _assert_cells(got: dict, expected: dict) -> None:
    assert set(got) == set(expected)
    for cell, want in expected.items():
        if want is None:
            assert got[cell] is None, (cell, got[cell])
        else:
            assert got[cell] is not None, cell
            assert float(got[cell]) == pytest.approx(want), (cell, got[cell])


class TestPopulationRestriction:
    async def test_association_filter_restricts_shifted_evaluation(self, exec_engine) -> None:
        resp = await exec_engine.execute(orders_q(
            dimensions=["status"], time_dimensions=month_td(),
            filters=[f"{EVENT_VALUE} > 40"],
            measures=[ModelMeasure(formula=f"time_shift({SHARE}, -1)", name="t")]))
        _assert_cells(_cells(resp, "orders.status"), ASSOC_FILTER_SHARE)

    async def test_associate_mode_fanning_dimension(self, exec_engine) -> None:
        """Regression guard: already correct before the producer rewrite."""
        kw = {"dimensions": [EVENT_VALUE], "time_dimensions": month_td(),
              "to_many_handling": "associate"}
        resp = await exec_engine.execute(orders_q(
            measures=[ModelMeasure(formula="time_shift(amount:sum, -1)", name="t")], **kw))
        _assert_cells(_cells(resp, f"orders.{EVENT_VALUE}"), ASSOC_DIM_SHIFT)
        plain = await exec_engine.execute(orders_q(
            measures=[ModelMeasure(formula="amount:sum", name="s")], **kw))
        assert len(resp.data) == len(plain.data)


class TestCrossModelPartitionKey:
    async def test_attributable_tier_key(self, exec_engine) -> None:
        resp = await exec_engine.execute(orders_q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="time_shift(amount:sum / amount:sum(partition_by=[customers.tier]), -1)",
                name="t")]))
        _assert_cells(_cells(resp, "orders.customers.tier"), TIER_SHARE)

    async def test_fanning_hop_key_is_typed_error(self, exec_engine) -> None:
        """Regression guard: the input-safety error, never a value."""
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(orders_q(
                dimensions=["status"], time_dimensions=month_td(),
                measures=[ModelMeasure(
                    formula=f"time_shift(amount:sum / amount:sum(partition_by=[{EVENT_VALUE}]), -1)",
                    name="t")]))
        msg = str(ei.value)
        assert "region_events" in msg, msg
        assert "every partition key must be attributable" in msg, msg


# --------------------------------------------------------------------------- #
# Double-entry: every oracle above re-derived from the seeded rows.
# --------------------------------------------------------------------------- #
def _customers() -> dict:
    return {c[0]: {"region_id": c[1], "tier": c[3]} for c in _CUSTOMERS_ROWS}


def _orders() -> list:
    cust = _customers()
    out = []
    for o in _ORDERS_ROWS:
        c = cust.get(o[1])
        out.append({"status": o[2], "amount": o[4], "month": o[5][:7],
                    "region_id": c["region_id"] if c else None,
                    "tier": c["tier"] if c else None})
    return out


def _prev(month: str) -> str:
    y, m = int(month[:4]), int(month[5:])
    return f"{y - 1}-12" if m == 1 else f"{y}-{m - 1:02d}"


def _shift(cur: dict, cells: set) -> dict:
    return {(k, mo): cur.get((k, _prev(mo))) for k, mo in cells}


class TestOraclesFromRawRows:
    def test_association_filter_share(self) -> None:
        rows = [o for o in _orders()
                if _REGION_EVENT_VALUE.get(o["region_id"], 0.0) > 40]
        cell, month = defaultdict(float), defaultdict(float)
        for o in rows:
            cell[(o["status"], o["month"])] += o["amount"]
            month[o["month"]] += o["amount"]
        share = {k: v / month[k[1]] for k, v in cell.items()}
        assert _shift(share, set(cell)) == pytest.approx(ASSOC_FILTER_SHARE)

    def test_associate_mode_dimension(self) -> None:
        cell = defaultdict(float)
        for o in _orders():
            cell[(_REGION_EVENT_VALUE.get(o["region_id"]), o["month"])] += o["amount"]
        assert _shift(dict(cell), set(cell)) == ASSOC_DIM_SHIFT

    def test_tier_share(self) -> None:
        cell, tier = defaultdict(float), defaultdict(float)
        for o in _orders():
            cell[(o["tier"], o["month"])] += o["amount"]
            tier[o["tier"]] += o["amount"]
        share = {k: v / tier[k[0]] for k, v in cell.items()}
        assert _shift(share, set(cell)) == pytest.approx(TIER_SHARE)
