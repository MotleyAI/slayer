"""DEV-1836 D3 reroot rules over a provably to-one reverse hop.

The producer roots at the aggregate's model and reaches HOST coordinates back
over the declared reverse join: a host time dimension re-roots leaf-wise
(TimeTruncKey included), and a host-sibling filter binds the HOST's join
instance (via-host preferred over the root's own same-named join).
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.scope_check import assert_scope_closed

from tests._engine_helpers import make_seeded_sqlite_engine


def _models() -> list[SlayerModel]:
    policy = SlayerModel(
        name="policy", sql_table="policy", data_source="test",
        columns=[
            Column(name="policy_identifier", type=DataType.INT, primary_key=True),
            Column(name="policy_number", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="party_a", type=DataType.INT),
        ],
        joins=[
            ModelJoin(target_model="policy_amount",
                      join_pairs=[["policy_identifier", "policy_identifier"]],
                      join_type="inner"),
            ModelJoin(target_model="party", join_pairs=[["party_a", "id"]]),
        ],
    )
    policy_amount = SlayerModel(
        name="policy_amount", sql_table="policy_amount", data_source="test",
        columns=[
            Column(name="pa_id", type=DataType.INT, primary_key=True),
            Column(name="policy_identifier", type=DataType.INT),
            Column(name="policy_amount", type=DataType.DOUBLE),
            Column(name="party_b", type=DataType.INT),
        ],
        joins=[
            # The provably to-one reverse hop (policy_identifier is policy's PK).
            ModelJoin(target_model="policy",
                      join_pairs=[["policy_identifier", "policy_identifier"]],
                      join_type="inner"),
            # Same sibling model as the host, joined on a DIFFERENT key.
            ModelJoin(target_model="party", join_pairs=[["party_b", "id"]]),
        ],
    )
    party = SlayerModel(
        name="party", sql_table="party", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="code", type=DataType.TEXT),
        ],
    )
    return [policy, policy_amount, party]


def _seed(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE policy (policy_identifier INTEGER PRIMARY KEY, policy_number TEXT, created_at TEXT, party_a INTEGER)")
    conn.execute("CREATE TABLE policy_amount (pa_id INTEGER PRIMARY KEY, policy_identifier INTEGER, policy_amount REAL, party_b INTEGER)")
    conn.execute("CREATE TABLE party (id INTEGER PRIMARY KEY, code TEXT)")
    conn.executemany("INSERT INTO party VALUES (?, ?)", [(1, "X"), (2, "Y")])
    conn.executemany("INSERT INTO policy VALUES (?, ?, ?, ?)", [
        (1, "POL-1", "2024-01-15", 1),
        (2, "POL-2", "2024-02-10", 2),
    ])
    # party_b deliberately OPPOSITE of the owning policy's party_a, so the
    # root's own party join selects different rows than the host's instance.
    conn.executemany("INSERT INTO policy_amount VALUES (?, ?, ?, ?)", [
        (10, 1, 100.0, 2), (11, 1, 200.0, 2),
        (20, 2, 300.0, 1), (21, 2, 400.0, 1),
    ])
    conn.commit()
    conn.close()


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "revhop.db")
        _seed(db)
        yield await make_seeded_sqlite_engine(
            base_dir=os.path.join(d, "store"), db_path=db, models=_models(),
        )


CM = ModelMeasure(formula="policy_amount.policy_amount:sum", name="cm")
MONTH = [TimeDimension(dimension=ColumnRef(name="created_at"),
                       granularity=TimeGranularity.MONTH)]


class TestTimeTruncGrainReroots:
    async def test_host_time_dimension_grain_is_exact_per_month(self, engine) -> None:
        """The TimeTruncKey grain member re-roots through the reverse hop —
        the producer buckets by policy.created_at, exact per month."""
        resp = await engine.execute(SlayerQuery(
            source_model="policy", time_dimensions=list(MONTH), measures=[CM],
        ))
        assert resp.sql is not None
        assert_scope_closed(resp.sql, dialect="sqlite")
        by_month = {r["policy.created_at"][:7]: r for r in resp.data}
        assert by_month["2024-01"]["policy.cm"] == pytest.approx(300.0)
        assert by_month["2024-02"]["policy.cm"] == pytest.approx(700.0)

    async def test_windowed_cross_model_buckets_by_rerooted_host_td(self, engine) -> None:
        """The windowed producer's synthesized bucket is the re-rooted host TD
        (the window_td_key call site) — it must execute, not mis-bind."""
        resp = await engine.execute(SlayerQuery(
            source_model="policy", time_dimensions=list(MONTH),
            measures=[ModelMeasure(
                formula="policy_amount.policy_amount:sum(window='20d')", name="w",
            )],
        ))
        assert resp.sql is not None
        assert_scope_closed(resp.sql, dialect="sqlite")
        by_month = {r["policy.created_at"][:7]: r for r in resp.data}
        # Values calibrated against the LOCAL windowed measure on identical
        # dates/amounts — cross-model must match local window semantics.
        assert by_month["2024-01"]["policy.w"] == pytest.approx(300.0)
        assert by_month["2024-02"]["policy.w"] is None


class TestSiblingFilterBindsTheHostInstance:
    async def test_via_host_instance_wins_over_the_roots_own_join(self, engine) -> None:
        """``party.code`` in host coordinates is the HOST's party join
        (policy.party_a). The producer must inherit that instance via the
        reverse hop — not the root's own ``party_b`` join, which selects the
        opposite rows here."""
        resp = await engine.execute(SlayerQuery(
            source_model="policy",
            dimensions=[ColumnRef(name="policy_number")],
            measures=[CM],
            filters=["party.code = 'X'"],
        ))
        rows = {r["policy.policy_number"]: r for r in resp.data}
        assert set(rows) == {"POL-1"}, rows
        assert rows["POL-1"]["policy.cm"] == pytest.approx(300.0), (
            "the producer bound the root's own party join (party_b) instead of "
            "the host's instance"
        )
