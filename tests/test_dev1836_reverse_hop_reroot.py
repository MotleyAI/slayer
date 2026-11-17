"""DEV-1836 D3 reroot rules over a provably to-one reverse hop.

The producer roots at the aggregate's model and reaches HOST coordinates back
over the INVERTED forward join (DEV-1853 — no declared reverse edge): a host
time dimension re-roots leaf-wise (TimeTruncKey included), and a host-sibling
filter binds the HOST's join instance (via-host preferred over the root's own
same-named join).
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import FieldRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.scope_check import assert_scope_closed
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import make_seeded_sqlite_engine


def _models() -> list[SlayerDataset]:
    policy = SlayerDataset(
        name="policy", sql_table="policy", data_source="test",
        fields=[
            DatasetField(name="policy_identifier", datatype=DataType.INT, primary_key=True),
            DatasetField(name="policy_number", datatype=DataType.TEXT),
            DatasetField(name="created_at", datatype=DataType.TIMESTAMP),
            DatasetField(name="party_a", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(to="policy_amount",
                      join_pairs=[["policy_identifier", "policy_identifier"]],
                      join_type="inner"),
            DatasetRelationship(to="party", join_pairs=[["party_a", "id"]]),
        ],
    )
    policy_amount = SlayerDataset(
        name="policy_amount", sql_table="policy_amount", data_source="test",
        fields=[
            DatasetField(name="pa_id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="policy_identifier", datatype=DataType.INT),
            DatasetField(name="policy_amount", datatype=DataType.DOUBLE),
            DatasetField(name="party_b", datatype=DataType.INT),
        ],
        relationships=[
            # DEV-1853: no declared reverse edge — the policy_amount → policy
            # hop is the INVERTED policy → policy_amount edge, provably to-one
            # structurally (policy_identifier is policy's PK).
            # Same sibling model as the host, joined on a DIFFERENT key.
            DatasetRelationship(to="party", join_pairs=[["party_b", "id"]]),
        ],
    )
    party = SlayerDataset(
        name="party", sql_table="party", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="code", datatype=DataType.TEXT),
        ],
    )
    return [policy, policy_amount, party]


def _seed(db_path: str) -> None:
    with transaction(db_path) as conn:
        conn.execute("CREATE TABLE policy (policy_identifier INTEGER PRIMARY KEY, policy_number TEXT, created_at TEXT, party_a INTEGER)")
        conn.execute("CREATE TABLE policy_amount (pa_id INTEGER PRIMARY KEY, policy_identifier INTEGER, policy_amount REAL, party_b INTEGER)")
        conn.execute("CREATE TABLE party (id INTEGER PRIMARY KEY, code TEXT)")
        conn.executemany("INSERT INTO party VALUES (?, ?)", [(1, "X"), (2, "Y")])
        # Full datetimes: a date-only string sorts BELOW a computed datetime
        # window bound ("2024-02-10" < "2024-02-10 00:00:00"), which would turn
        # the inclusive-boundary case below into a string-comparison artifact.
        conn.executemany("INSERT INTO policy VALUES (?, ?, ?, ?)", [
            (1, "POL-1", "2024-01-15 00:00:00", 1),
            (2, "POL-2", "2024-02-10 00:00:00", 2),
        ])
        # party_b deliberately OPPOSITE of the owning policy's party_a, so the
        # root's own party join selects different rows than the host's instance.
        conn.executemany("INSERT INTO policy_amount VALUES (?, ?, ?, ?)", [
            (10, 1, 100.0, 2), (11, 1, 200.0, 2),
            (20, 2, 300.0, 1), (21, 2, 400.0, 1),
        ])


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "revhop.db")
        _seed(db)
        yield await make_seeded_sqlite_engine(
            base_dir=os.path.join(d, "store"), db_path=db, models=_models(),
        )


CM = DatasetMetric(expression="sum(policy_amount.policy_amount)", name="cm")
MONTH = [TimeDimension(field="created_at",
                       granularity=TimeGranularity.MONTH)]


class TestTimeTruncGrainReroots:
    async def test_host_time_dimension_grain_is_exact_per_month(self, engine) -> None:
        """The TimeTruncKey grain member re-roots through the reverse hop —
        the producer buckets by policy.created_at, exact per month."""
        resp = await engine.execute(SlayerQuery(
            population="policy", time_dimensions=list(MONTH), metrics=[CM],
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
            population="policy", time_dimensions=list(MONTH),
            metrics=[DatasetMetric(
                expression="sum(policy_amount.policy_amount, window='20d')", name="w",
            )],
        ))
        assert resp.sql is not None
        assert_scope_closed(resp.sql, dialect="sqlite")
        by_month = {r["policy.created_at"][:7]: r for r in resp.data}
        # Values calibrated against the LOCAL windowed measure on identical
        # dates/amounts — cross-model must match local window semantics. The
        # 20-day February window starts on Feb 10 inclusive, so POL-2's two
        # amounts are in.
        assert by_month["2024-01"]["policy.w"] == pytest.approx(300.0)
        assert by_month["2024-02"]["policy.w"] == pytest.approx(700.0)


class TestSiblingFilterBindsTheHostInstance:
    async def test_via_host_instance_wins_over_the_roots_own_join(self, engine) -> None:
        """``party.code`` in host coordinates is the HOST's party join
        (policy.party_a). The producer must inherit that instance via the
        reverse hop — not the root's own ``party_b`` join, which selects the
        opposite rows here."""
        resp = await engine.execute(SlayerQuery(
            population="policy",
            dimensions=[FieldRef(name="policy_number")],
            metrics=[CM],
            filter=["party.code = 'X'"],
        ))
        rows = {r["policy.policy_number"]: r for r in resp.data}
        assert set(rows) == {"POL-1"}, rows
        assert rows["POL-1"]["policy.cm"] == pytest.approx(300.0), (
            "the producer bound the root's own party join (party_b) instead of "
            "the host's instance"
        )
