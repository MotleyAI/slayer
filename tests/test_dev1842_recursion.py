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
from slayer.core.errors import MeasureRecursionLimitError
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel

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
              measures=[{"formula": "customers.aov_big", "name": "x"}]),
        )
        hand = await engine.execute(
            q(dimensions=["customers.tier"],
              measures=[{"formula":
                         "(customers.spend:sum / customers.*:count) * 2",
                         "name": "x"}]),
        )
        assert _canon(dotted) == _canon(hand)


class TestHostMeasureContainingDottedRef:
    async def test_bare_host_measure_resolves_its_dotted_ref(self):
        """A bare host measure whose formula contains a dotted cross-model
        reference (``host_runs_dotted`` = ``cumsum(customers.aov)``) resolves
        recursively — identical SQL to writing the dotted form inline."""
        bare = q(dimensions=["customers.tier"],
                 measures=[{"formula": "host_runs_dotted", "name": "x"}],
                 time_dimensions=month_td())
        inline = q(dimensions=["customers.tier"],
                   measures=[{"formula": "cumsum(customers.aov)", "name": "x"}],
                   time_dimensions=month_td())
        assert await gen(bare) == await gen(inline)


class TestCrossModelCycle:
    async def test_cycle_errors_naming_the_measure_chain(self):
        """``customers.cyc_c`` → ``orders.cyc_o`` → ``customers.cyc_c`` is a
        cross-model measure cycle. The error names the full (model, measure)
        chain, not a wrong or double-counted value."""
        query = q(measures=[{"formula": "customers.cyc_c", "name": "x"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        message = str(ei.value).lower()
        assert "cyc_c" in message
        assert "cyc_o" in message
        assert "cycl" in message or "circular" in message


class TestDepthLimit:
    """A dotted reference into a same-model chain (``sub.d1`` → ``d2`` → ``d3``)
    respects the configurable depth cap; exceeding it raises naming the chain."""

    def _models(self):
        sub = SlayerModel(
            name="sub", data_source="test", sql_table="sub",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
            ],
            measures=[
                ModelMeasure(name="d1", formula="d2"),
                ModelMeasure(name="d2", formula="d3"),
                ModelMeasure(name="d3", formula="amount:sum"),
            ],
        )
        main = SlayerModel(
            name="main", data_source="test", sql_table="main",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="sub_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="sub", join_pairs=[["sub_id", "id"]])],
        )
        return main, sub

    async def _generate(self, formula: str) -> str:
        main, sub = self._models()
        return await _engine_generate(
            query=q(source_model="main",
                    measures=[{"formula": formula, "name": "x"}]),
            model=main, extra_models=[sub], dialect="duckdb", validate=False,
        )

    async def test_depth_limit_exceeded_raises(self):
        with mock.patch.dict(os.environ, {"SLAYER_MEASURE_EXPANSION_DEPTH": "1"}):
            with pytest.raises(MeasureRecursionLimitError) as ei:
                await self._generate("sub.d1")
            assert ei.value.limit == 1

    async def test_within_depth_limit_resolves(self):
        # The same chain resolves to ``sub.amount:sum`` under the default cap.
        dotted = await self._generate("sub.d1")
        hand = await self._generate("sub.amount:sum")
        assert dotted == hand
