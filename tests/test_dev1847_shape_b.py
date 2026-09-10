"""DEV-1847 task 1.4/3.3 — shape B (partition_by an attach-carrying computed
dimension) and the re-aggregation-operand finer-grain exemption (spec:
queries/partitioned-aggregates)."""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError

from tests._dev1847_fixtures import (
    AVG_CITY_TOTAL_BY_REGION,
    INNER_CR,
    SHAPE_B_BAND_TOTAL,
    SPEND_BAND_EXPR,
    ModelMeasure,
    make_exec_engine,
    reagg,
    region_key,
    rows_by,
    sales_q,
)

BAND = {"expression": SPEND_BAND_EXPR, "name": "spend_band"}
# A second computed dimension whose expression nests an aggregate partitioned by
# the attach-carrying spend_band (the doubly-nested attach).
BAND2 = {"expression": "CASE WHEN amount:sum(partition_by=spend_band) > 100 "
                       "THEN 'big' ELSE 'small' END", "name": "size"}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestPartitionByBandedDimension:
    async def test_measure_partitioned_by_banded_dimension(self, exec_engine):
        """The band total broadcasts across the band's rows."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", BAND],
            measures=[ModelMeasure(formula="amount:sum", name="tot"),
                      ModelMeasure(formula="amount:sum(partition_by=spend_band)",
                                   name="bt")]))
        by = rows_by(resp, "sales.region", "sales.spend_band")
        assert float(by[("North", "hi")]["sales.bt"]) == pytest.approx(
            SHAPE_B_BAND_TOTAL["hi"])
        assert float(by[("North", "lo")]["sales.bt"]) == pytest.approx(
            SHAPE_B_BAND_TOTAL["lo"])

    async def test_partition_by_computed_dim_no_longer_raises_nested_attach(
        self, exec_engine,
    ):
        """Partitioning by an attach-carrying computed dimension compiles."""
        try:
            await exec_engine.execute(sales_q(
                dimensions=["region", BAND],
                measures=[ModelMeasure(formula="amount:sum(partition_by=spend_band)",
                                       name="bt")]))
        except NotImplementedError as exc:  # pragma: no cover - the contract
            pytest.fail(f"nested-attach guard still present: {exc}")

    async def test_nested_inside_another_computed_dimension(self, exec_engine):
        """A second computed dimension partitioned by spend_band executes."""
        resp = await exec_engine.execute(sales_q(
            dimensions=[BAND, BAND2],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        by = rows_by(resp, "sales.spend_band", "sales.size")
        # hi band total 340 > 100 -> 'big'; lo band total 85 <= 100 -> 'small'.
        assert ("hi", "big") in by
        assert ("lo", "small") in by


class TestCombinedConsumerExemption:
    async def test_reaggregation_operand_keeps_finer_grain_exemption(self, exec_engine):
        """An inner partition key that is not a query dimension is legal."""
        try:
            resp = await exec_engine.execute(sales_q(
                dimensions=["region"],
                measures=[reagg("avg", INNER_CR, name="acr")]))
        except SlayerError as exc:
            pytest.fail(f"re-aggregation operand wrongly hit the partition-key "
                        f"rule: {exc}")
        vals = {k[0]: v["sales.acr"] for k, v in region_key(resp).items()}
        assert float(vals["North"]) == pytest.approx(AVG_CITY_TOTAL_BY_REGION["North"])

    async def test_outer_explicit_key_still_carries_the_rule(self, exec_engine):
        """The outer's OWN explicit partition key must be a query dimension."""
        with pytest.raises((SlayerError, ValueError)) as ei:
            await exec_engine.execute(sales_q(
                dimensions=["region"],
                measures=[reagg("avg", INNER_CR, name="acr", partition_by="product")]))
        msg = str(ei.value)
        assert not isinstance(ei.value, NotImplementedError)
        # The pure-attached operand is accepted; the failure is the outer key's
        # combined-consumer rule, NOT the generic expression-nesting gate (whose
        # message echoes 'product' from the formula).
        assert "nested inside the expression aggregated" not in msg
        assert "product" in msg


class TestFilterAndOrderSurfaces:
    async def test_filter_partition_by_computed_dim(self, exec_engine):
        """A FILTER's partition_by= resolves the computed dimension name too
        (Codex review find) — only the hi band (340 > 100) survives."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", BAND],
            measures=[ModelMeasure(formula="amount:sum", name="tot")],
            filters=["amount:sum(partition_by=spend_band) > 100"]))
        labels = {row["sales.spend_band"] for row in resp.data}
        assert resp.data and labels == {"hi"}

    async def test_order_partition_by_computed_dim(self, exec_engine):
        """A raw ORDER BY target's partition_by= resolves it as well — hi band
        rows (340) sort before lo (90)."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", BAND],
            measures=[ModelMeasure(formula="amount:sum", name="tot")],
            order=[{"column": "amount:sum(partition_by=spend_band)",
                    "direction": "desc"}]))
        labels = [row["sales.spend_band"] for row in resp.data]
        assert labels == sorted(labels, key=lambda x: x != "hi")
