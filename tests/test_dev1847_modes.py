"""DEV-1847 task 1.2/3.2 — the to_many_handling mode axis at the re-aggregation
seam (SQLite + DuckDB). Partition_by=city alone spans regions, so region is
unattributable to the operand dataset: the default broadcasts a self-announcing
global value, associate reconciles per region, error refuses. Fails until the
feature lands.

Spec: openspec …/specs/queries/semantics — "Unattributable outer dimension
broadcasts / associates / refuses".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError

from tests._dev1847_fixtures import (
    ASSOCIATE_AVG_CITY_BY_REGION,
    BROADCAST_GLOBAL_AVG_CITY,
    INNER_CITY,
    INNER_CR,
    SHAPE_B_BAND_TOTAL,
    SPEND_BAND_EXPR,
    ModelMeasure,
    associated_warnings,
    broadcast_warnings,
    chain_q,
    make_exec_engine,
    reagg,
    region_key,
    rows_by,
    sales_q,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(mode=None):
    kw = {"dimensions": ["region"], "measures": [reagg("avg", INNER_CITY, name="acc")]}
    if mode is not None:
        kw["to_many_handling"] = mode
    return sales_q(**kw)


class TestBroadcast:
    async def test_default_broadcasts_global_and_warns(self, exec_engine):
        """Scenario: Unattributable outer dimension broadcasts with a warning —
        EVERY region row carries the global average of city totals, and the
        warning names region with a non-empty reason plus the associate remedy."""
        resp = await exec_engine.execute(_q())
        vals = {k[0]: v["sales.acc"] for k, v in region_key(resp).items()}
        # the global value lands on every region row, not just a couple.
        for value in vals.values():
            assert float(value) == pytest.approx(BROADCAST_GLOBAL_AVG_CITY)
        (w,) = broadcast_warnings(resp)
        region_dims = [d for d in w.dimensions if d.dimension == "region"]
        assert region_dims and region_dims[0].reason
        assert "associate" in w.hint.lower()


class TestAssociate:
    async def test_associate_reconciles_per_region(self, exec_engine):
        """Scenario: Unattributable outer dimension associates on request — each
        region aggregates the distinct city cells associated with it; Alpha's
        total counts in both North and South."""
        resp = await exec_engine.execute(_q("associate"))
        vals = {k[0]: v["sales.acc"] for k, v in region_key(resp).items()}
        for region, expected in ASSOCIATE_AVG_CITY_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)
        (w,) = associated_warnings(resp)
        assert "region" in " ".join(w.dimensions) or "region" in w.human_message()

    async def test_associate_differs_from_broadcast(self, exec_engine):
        """The two modes are observably different on this shape."""
        assoc = await exec_engine.execute(_q("associate"))
        bcast = await exec_engine.execute(_q("broadcast"))
        a = {k[0]: v["sales.acc"] for k, v in region_key(assoc).items()}
        b = {k[0]: v["sales.acc"] for k, v in region_key(bcast).items()}
        assert float(a["North"]) != pytest.approx(float(b["North"]))


class TestErrorMode:
    async def test_error_mode_refuses_with_clear_message(self, exec_engine):
        """Scenario: Unattributable outer dimension refuses under error mode —
        a clear error naming the dimension and the remedy, never wrong numbers."""
        with pytest.raises((SlayerError, ValueError)) as ei:
            await exec_engine.execute(_q("error"))
        msg = str(ei.value)
        assert not isinstance(ei.value, NotImplementedError)
        # a genuine error-mode refusal, not the generic expression-nesting gate
        # (whose message echoes 'partition_by' from the formula).
        assert "nested inside the expression aggregated" not in msg
        assert "region" in msg
        assert "partition_by" in msg


class TestExplicitOuterKeyUnattributable:
    """Task 1.3 — the outer aggregation's OWN explicit partition key, when a
    query dimension unattributable to the operand dataset, resolves per mode."""

    def _pq(self, mode):
        # product is a query dimension but not a function of the [city,region]
        # operand cells, so it is unattributable to the operand dataset.
        return sales_q(
            dimensions=["region", "product"], to_many_handling=mode,
            measures=[reagg("avg", INNER_CR, name="acc", partition_by="product")])

    async def test_default_broadcasts_and_warns(self, exec_engine):
        resp = await exec_engine.execute(self._pq("broadcast"))
        (w,) = broadcast_warnings(resp)
        assert any(d.dimension == "product" for d in w.dimensions)

    async def test_error_mode_refuses(self, exec_engine):
        with pytest.raises((SlayerError, ValueError)) as ei:
            await exec_engine.execute(self._pq("error"))
        assert not isinstance(ei.value, NotImplementedError)
        assert "product" in str(ei.value)


class TestExpressionGrainNonTransitivity:
    async def test_expression_grain_determines_only_itself(self, exec_engine):
        """Deferred from stage 2: an expression inner grain (spend_band)
        determines only itself — region is NOT attributed through it; each row
        carries its band's cell value and region broadcasts with a warning."""
        band = {"expression": SPEND_BAND_EXPR, "name": "spend_band"}
        resp = await exec_engine.execute(sales_q(
            dimensions=[band, "region"],
            measures=[ModelMeasure(
                formula="avg(sum(amount, partition_by=spend_band))", name="abt")]))
        by = rows_by(resp, "sales.spend_band", "sales.region")
        for (bandv, _region), cell in by.items():
            assert float(cell["sales.abt"]) == pytest.approx(
                SHAPE_B_BAND_TOTAL[bandv])
        (w,) = broadcast_warnings(resp)
        assert any(d.dimension == "region" for d in w.dimensions)


class TestJoinedDimensionSeeding:
    async def test_unseeded_joined_dimension_broadcasts(self, exec_engine):
        """A joined dim reachable to-one from the host but NOT seeded by the
        operand grain broadcasts the global value with a warning — never a
        silent per-group association (Codex review find)."""
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name"],
            measures=[ModelMeasure(
                formula="avg(sum(amount, partition_by=amount))", name="a")]))
        assert len(resp.data) == 2  # one row per region, no hidden duplicates
        vals = {row["corders.customers.regions.name"]: float(row["corders.a"])
                for row in resp.data}
        assert vals == {"North": 42.5, "South": 42.5}  # global avg of the 4 cells
        (w,) = broadcast_warnings(resp)
        assert "determined by the operand grain" in w.human_message()
