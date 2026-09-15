"""DEV-1892 task 1.5 — the generalised determination helper, spec'd in
queries/semantics › "Outer dimension seeded by a nested-path entity key" and
"A nested-path non-key grain field seeds nothing beyond itself": a joined
model's unique key in the inner grain seeds a to-one chain (the deliberate
expansion — attributable, no warning), while a non-key grain field seeds only
itself.
"""

from __future__ import annotations

import pytest

from tests._dev1847_fixtures import (
    ModelMeasure,
    broadcast_warnings,
    chain_q,
    make_exec_engine,
    rows_by,
)
from tests._dev1892_fixtures import (
    FK_SEED_AVG_BY_REGION_NAME,
    SEEDED_AVG_BY_REGION_ID,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestNestedPathKeySeed:
    @pytest.mark.parametrize("dim, col, oracle", [
        ("customers.region_id", "corders.customers.region_id",
         SEEDED_AVG_BY_REGION_ID),
        # a further to-one hop off the seeded key: regions.name.
        ("customers.regions.name", "corders.customers.regions.name",
         {"North": 35.0, "South": 100.0}),
    ])
    async def test_joined_key_seeds_attribution(self, exec_engine, dim, col, oracle):
        """Scenario: outer dimension seeded by a nested-path entity key — the
        inner grain contains customers.id, so a column of that model and one
        reached from it over a to-one hop are attributable exactly, with NO
        broadcast warning (the pre-DEV-1892 rule broadcast them)."""
        resp = await exec_engine.execute(chain_q(
            dimensions=[dim],
            measures=[ModelMeasure(
                formula="avg(sum(amount, partition_by=customers.id))", name="a")]))
        by = rows_by(resp, col)
        for key, expected in oracle.items():
            assert float(by[(key,)]["corders.a"]) == pytest.approx(expected)
        assert broadcast_warnings(resp) == []


class TestForeignKeySeedsToOneTarget:
    async def test_fk_grain_field_determines_its_to_one_target(self, exec_engine):
        """Scenario: a foreign-key grain field determines its to-one target
        (Axiom 1) — inner grain customers.region_id pins one region row per cell,
        so customers.regions.name is attributable exactly, no broadcast warning."""
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name"],
            measures=[ModelMeasure(
                formula="avg(sum(amount, partition_by=customers.region_id))",
                name="a")]))
        by = rows_by(resp, "corders.customers.regions.name")
        for name, expected in FK_SEED_AVG_BY_REGION_NAME.items():
            assert float(by[(name,)]["corders.a"]) == pytest.approx(expected)
        assert broadcast_warnings(resp) == []


class TestNestedPathNonKeyDoesNotSeed:
    async def test_non_key_grain_field_seeds_nothing_beyond_itself(self, exec_engine):
        """Scenario: a nested-path non-key grain field seeds nothing beyond
        itself — inner grain customers.region_id does not pin a customer, so a
        sibling column of the same model (customers.id) stays unattributable and
        broadcasts with a warning. (Its join-key hop to regions.name IS seeded —
        the deliberate expansion — and is covered separately.)"""
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.id"],
            measures=[ModelMeasure(
                formula="avg(sum(amount, partition_by=customers.region_id))",
                name="a")]))
        assert broadcast_warnings(resp)
