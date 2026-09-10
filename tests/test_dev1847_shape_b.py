"""DEV-1847 task 1.4/3.3 — shape B (partition_by an attach-carrying computed
dimension) and the narrowed combined-consumer partition-key rule (SQLite +
DuckDB). Directly exercises the removal of ``_reraise_nested_attach`` and the
re-aggregation-operand finer-grain exemption.

Spec: openspec …/specs/queries/partitioned-aggregates — "Partitioning by an
attach-carrying computed dimension", "Combined-consumer partition keys are query
dimensions" (MODIFIED) › "Re-aggregation operands keep the finer-grain
exemption".
"""

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
        """Scenario: Measure partitioned by a banded dimension — the band total
        broadcasts across the band's rows; no nested-attach NotImplementedError."""
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
        """The ``_reraise_nested_attach`` site is removed — partitioning by an
        attach-carrying computed dimension compiles rather than raising
        NotImplementedError."""
        try:
            await exec_engine.execute(sales_q(
                dimensions=["region", BAND],
                measures=[ModelMeasure(formula="amount:sum(partition_by=spend_band)",
                                       name="bt")]))
        except NotImplementedError as exc:  # pragma: no cover - the contract
            pytest.fail(f"nested-attach guard still present: {exc}")

    async def test_nested_inside_another_computed_dimension(self, exec_engine):
        """Scenario: Nested inside another computed dimension — a second computed
        dimension whose expression partitions by spend_band plans and executes
        without the nested-attach error."""
        resp = await exec_engine.execute(sales_q(
            dimensions=[BAND, BAND2],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        by = rows_by(resp, "sales.spend_band", "sales.size")
        # hi band total 340 > 100 -> 'big'; lo band total 85 <= 100 -> 'small'.
        assert ("hi", "big") in by
        assert ("lo", "small") in by


class TestCombinedConsumerExemption:
    async def test_reaggregation_operand_keeps_finer_grain_exemption(self, exec_engine):
        """Scenario: Re-aggregation operands keep the finer-grain exemption — an
        inner partition key (city) that is not a query dimension is legal because
        the aggregate is consumed only as the outer operand."""
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
        """The outer aggregation's OWN explicit partition key must still be a
        query dimension — here ``product`` is not, so it fails cleanly."""
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
