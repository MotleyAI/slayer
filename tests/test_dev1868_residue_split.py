"""DEV-1868 residue split (design D6; spec: queries/computed-dimensions): the
transform-in-dimension arm becomes two deliberate typed errors — permanent type
rules, no issue references."""

from __future__ import annotations

import re

import pytest

from tests._dev1836_fixtures import ModelMeasure, gen, month_td, q


async def _raises(band: str) -> str:
    query = q(
        dimensions=["customers.tier", {"expression": band, "name": "b"}],
        time_dimensions=month_td(),
        measures=[ModelMeasure(formula="amount:sum", name="s")],
    )
    with pytest.raises(ValueError) as ei:
        await gen(query=query)
    message = str(ei.value)
    assert not re.search(r"DEV-\d+", message)  # a type rule, not a deferral
    return message


class TestTransformInDimensionResidue:
    async def test_transform_without_an_aggregate_input(self) -> None:
        message = await _raises(
            "CASE WHEN cumsum(amount) > 50 THEN 1 ELSE 0 END")
        assert re.search(r"(?i)aggregate", message)

    async def test_transform_over_an_ungrained_aggregate(self) -> None:
        message = await _raises(
            "CASE WHEN cumsum(amount:sum) > 50 THEN 1 ELSE 0 END")
        assert "partition_by" in message  # the remedy
        assert re.search(r"(?i)dimension", message)  # the self-containment rationale

    async def test_mixed_grained_and_ungrained_aggregates(self) -> None:
        message = await _raises(
            "CASE WHEN cumsum(amount:sum(partition_by=status) + amount:sum) > 50 "
            "THEN 1 ELSE 0 END")
        assert "partition_by" in message
        assert "amount" in message  # names the ungrained aggregate

    async def test_the_two_violations_raise_distinct_errors(self) -> None:
        no_agg = await _raises("CASE WHEN cumsum(amount) > 50 THEN 1 ELSE 0 END")
        ungrained = await _raises(
            "CASE WHEN cumsum(amount:sum) > 50 THEN 1 ELSE 0 END")
        assert no_agg != ungrained
