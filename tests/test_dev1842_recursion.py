"""DEV-1842 task 1.3 — recursion, depth, and cycles for dotted saved measures.

Recursion runs both directions (a target measure referencing other saved
measures; a host measure containing a dotted ref); depth is bounded (default 32,
env-configurable); a cross-model measure cycle raises naming the (model, measure)
chain, detected on descent — distinct from the single-hop round-trip of task 1.6.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import MetricRecursionLimitError, SpellingConflictError
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset

from tests._deprecated_input_helpers import deprecations, payload_of, recorded
from tests._dev1842_fixtures import gen, make_exec_engine, month_td, q
from tests._engine_helpers import _engine_generate


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _canon(resp) -> list:
    out = []
    for r in resp.data:
        out.append(tuple(sorted(
            ((k, round(v, 6) if isinstance(v, float) else v) for k, v in r.items()),
            key=lambda kv: kv[0],
        )))
    return sorted(out, key=repr)


class TestNestedSavedMeasure:
    async def test_nested_saved_measure_matches_full_expansion(self, exec_backend):
        """``customers.aov_big`` (= ``aov * 2`` = ``(spend:sum/*:count) * 2``)."""
        _, engine = exec_backend
        dotted = await engine.execute(
            q(dimensions=["customers.tier"],
              metrics=[{"metric": "customers.aov_big", "alias": "x"}]),
        )
        hand = await engine.execute(
            q(dimensions=["customers.tier"],
              metrics=[{"metric":
                         "(sum(customers.spend) / count(customers.*)) * 2",
                         "alias": "x"}]),
        )
        assert _canon(dotted) == _canon(hand)


class TestHostMeasureContainingDottedRef:
    async def test_bare_host_measure_resolves_its_dotted_ref(self):
        """A bare host measure whose formula contains a dotted cross-model
        reference (``host_runs_dotted`` = ``cumsum(customers.aov)``) resolves
        recursively — identical SQL to writing the dotted form inline."""
        bare = q(dimensions=["customers.tier"],
                 metrics=[{"metric": "host_runs_dotted", "alias": "x"}],
                 time_dimensions=month_td())
        inline = q(dimensions=["customers.tier"],
                   metrics=[{"metric": "cumsum(customers.aov)", "alias": "x"}],
                   time_dimensions=month_td())
        assert await gen(bare) == await gen(inline)


class TestCrossModelCycle:
    async def test_cycle_errors_naming_the_measure_chain(self):
        """``customers.cyc_c`` → ``orders.cyc_o`` → ``customers.cyc_c`` is a
        cross-model measure cycle. The error names the full (model, measure)
        chain, not a wrong or double-counted value."""
        query = q(metrics=[{"metric": "customers.cyc_c", "alias": "x"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        message = str(ei.value).lower()
        assert "cyc_c" in message
        assert "cyc_o" in message
        assert "cycl" in message or "circular" in message


class TestDepthLimit:
    """A dotted reference into a same-model chain (``sub.d1`` → ``d2`` → ``d3``)
    respects the configurable depth cap; exceeding it raises naming the chain."""

    def _datasets(self):
        sub = SlayerDataset(
            name="sub", data_source="test", sql_table="sub",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
            metrics=[
                DatasetMetric(name="d1", expression="d2"),
                DatasetMetric(name="d2", expression="d3"),
                DatasetMetric(name="d3", expression="sum(amount)"),
            ],
        )
        main = SlayerDataset(
            name="main", data_source="test", sql_table="main",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="sub_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="sub", from_columns=["sub_id"], to_columns=["id"])],
        )
        return main, sub

    async def _generate(self, expression: str) -> str:
        main, sub = self._datasets()
        return await _engine_generate(
            query=q(population="main",
                    metrics=[{"metric": expression, "alias": "x"}]),
            dataset=main, extra_datasets=[sub], dialect="duckdb", validate=False,
        )

    async def test_depth_limit_exceeded_raises(self):
        with mock.patch.dict(os.environ, {"SLAYER_METRIC_EXPANSION_DEPTH": "1"}):
            with pytest.raises(MetricRecursionLimitError) as ei:
                await self._generate("sub.d1")
            assert ei.value.limit == 1

    async def test_the_pre_2_0_depth_variable_still_applies_with_a_deprecation(self):
        with mock.patch.dict(os.environ, {"SLAYER_MEASURE_EXPANSION_DEPTH": "1"}), recorded() as caught:
            with pytest.raises(MetricRecursionLimitError) as ei:
                await self._generate("sub.d1")
        assert ei.value.limit == 1
        assert {(p.old, p.new) for p in map(payload_of, deprecations(caught))} == {
            ("SLAYER_MEASURE_EXPANSION_DEPTH", "SLAYER_METRIC_EXPANSION_DEPTH")}

    async def test_both_depth_variables_are_refused(self):
        both = {"SLAYER_MEASURE_EXPANSION_DEPTH": "1", "SLAYER_METRIC_EXPANSION_DEPTH": "2"}
        with mock.patch.dict(os.environ, both), recorded():
            with pytest.raises(SpellingConflictError):
                await self._generate("sub.d1")

    async def test_within_depth_limit_resolves(self):
        # The same chain resolves to ``sum(sub.amount)`` under the default cap.
        dotted = await self._generate("sub.d1")
        hand = await self._generate("sum(sub.amount)")
        assert dotted == hand
