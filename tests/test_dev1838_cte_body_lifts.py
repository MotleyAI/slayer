"""The CTE-body deferrals lift (design D2 hoist; the guard
list empties).

Spec: openspec …/specs/queries/computed-dimensions — "Nested attaches render
inside CTE bodies": a row regroup attach, a partitioned-aggregate combined
attach, and a re-rooted first/last sub-plan carrying producers each compile
and execute when nested where the plan renders as a CTE body (a non-final
stage, or inside another producer), hoisting into one flat WITH.

Feature-missing today, per shape (divergences.md "broken shapes"): the
nested-attach-in-producer shapes leak a ``__regroup__`` placeholder into the
sub-plan's CTE (invalid SQL / bind error); producer-carrying non-final stages
execute correctly but nest a WITH inside the stage CTE (two ``exp.With``
nodes); the guard messages still live in the sources. The keep-working and
cardinality pins are green today.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

import slayer
from slayer.core.query import DatasetExtension, QueryMetric

from tests._dev1838_fixtures import (
    BAND,
    BAND1_BY_REGION,
    BAND_GOLD,
    BAND_TIER_ORDER,
    BAND_WMAX_BY_REGION,
    LASTDIM_GOLD,
    OrderItem,
    SPEND_LAST_BAND,
    SlayerQuery,
    broadcast_warnings,
    cte_aliases,
    make_exec_engine,
    month_td,
    q,
    rows_by,
)
from tests.test_dev1837_guards import ARM_COMBINED_CTE_BODY, ARM_ROW_CTE_BODY

M = QueryMetric(metric="sum(orders.amount)", alias="m")

#: The generator's re-rooted first/last CTE-body residual (generator.py) —
#: the third guard the lift removes.
ARM_REROOTED_RANKED = (
    "A re-rooted cross-model first/last whose sub-plan needs more than the "
    "ranked CTE itself is not yet supported"
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _assert_one_flat_with(sql: str, dialect: str) -> None:
    tree = sqlglot.parse_one(sql, read=dialect)
    with_nodes = list(tree.find_all(exp.With))
    assert len(with_nodes) == 1, f"{len(with_nodes)} WITH clauses:\n{sql}"


async def _dry_sql(engine, query) -> str:
    dry = await engine.execute(query, dry_run=True)
    assert "__regroup__" not in dry.sql, dry.sql
    return dry.sql


class TestNestedAttachInsideProducer:
    """A producer sub-plan (filtered-local / host-grain wrap) whose grain
    carries a nested attach hoists and executes — today these shapes emit a
    leaked ``__regroup__`` column or fail binding."""

    async def test_row_attach_inside_filtered_local_subplan(self, exec_backend):
        dialect, engine = exec_backend
        query = q(dimensions=["orders.status", BAND],
                  metrics=[M, QueryMetric(metric="sum(orders.gold_amount)", alias="g")])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.status", "band")
        got = {
            (s, int(b)): (float(r["m"]),
                          None if r["g"] is None else float(r["g"]))
            for (s, b), r in by.items()
        }
        assert set(got) == set(BAND_GOLD)
        for key, (m, g) in BAND_GOLD.items():
            assert got[key][0] == pytest.approx(m), key
            if g is None:
                assert got[key][1] is None, key
            else:
                assert got[key][1] == pytest.approx(g), key
        _assert_one_flat_with(await _dry_sql(engine, query), dialect)

    async def test_row_attach_inside_host_grain_wrap_subplan(self, exec_backend):
        dialect, engine = exec_backend
        query = q(dimensions=["orders.status", BAND], metrics=[M],
                  order=[OrderItem(field="customers.tier",
                                   direction="asc")])
        resp = await engine.execute(query)
        got = [(r["orders.status"], int(r["band"]), float(r["m"]))
               for r in resp.data]
        assert got == BAND_TIER_ORDER
        _assert_one_flat_with(await _dry_sql(engine, query), dialect)

    async def test_ranked_producer_inside_filtered_local_subplan(
        self, exec_backend,
    ):
        """The first/last direction: a last-partitioned dimension's ranked
        producer rides inside the filtered-local sub-plan's grain."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["orders.status", {"field": "last(orders.amount, partition_by=orders.city)",
                                   "alias": "cl"}],
            metrics=[QueryMetric(metric="sum(orders.gold_amount)", alias="g")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.status", "cl")
        got = {
            (s, float(cl)): (None if r["g"] is None
                             else float(r["g"]))
            for (s, cl), r in by.items()
        }
        assert set(got) == set(LASTDIM_GOLD)
        for key, g in LASTDIM_GOLD.items():
            if g is None:
                assert got[key] is None, key
            else:
                assert got[key] == pytest.approx(g), key
        _assert_one_flat_with(await _dry_sql(engine, query), dialect)

    async def test_cross_model_ranked_producer_with_band_keeps_working(
        self, exec_backend,
    ):
        """Green pin — the target-rooted ranked producer whose grain carries
        the band row attach already hoists; the lift must not
        regress it."""
        dialect, engine = exec_backend
        query = q(dimensions=["orders.status", BAND],
                  metrics=[QueryMetric(metric="last(customers.spend)",
                                         alias="sl")],
                  to_many_handling="broadcast")
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.status", "band")
        for (s, b), r in by.items():
            assert float(r["sl"]) == pytest.approx(
                SPEND_LAST_BAND[(s, int(b))]
            )
        assert broadcast_warnings(resp)
        _assert_one_flat_with(await _dry_sql(engine, query), dialect)

    async def test_hidden_ranked_order_beside_filtered_local_keeps_working(
        self, exec_backend,
    ):
        """Green pin — a hidden ranked order-only aggregate beside a
        filtered-local measure renders one flat WITH today."""
        dialect, engine = exec_backend
        query = q(dimensions=["orders.status"],
                  metrics=[QueryMetric(metric="sum(orders.gold_amount)", alias="g")],
                  order=[OrderItem.model_validate({"field": "last(orders.amount)", "direction": "desc"})])
        resp = await engine.execute(query)
        got = [(r["orders.status"], float(r["g"])) for r in resp.data]
        assert got == [("new", 20.0), ("ok", 10.0)]
        _assert_one_flat_with(await _dry_sql(engine, query), dialect)


class TestNonFinalStage:
    """Producer-carrying non-final stages: values are correct today (pinned);
    the lift flattens the stage-internal WITH into the statement's one chain."""

    @staticmethod
    def _banded_stages(**stage2_kwargs) -> list:
        stage1 = q(name="banded", dimensions=["orders.region", BAND],
                   metrics=[QueryMetric(metric="sum(orders.amount)", alias="bt")])
        stage2 = SlayerQuery(population="banded",
                             extensions=[DatasetExtension(dataset="banded")],
                             **stage2_kwargs)
        return [stage1, stage2]

    async def test_row_attach_stage_executes_in_one_flat_with(
        self, exec_backend,
    ):
        dialect, engine = exec_backend
        stages = self._banded_stages(
            dimensions=["region"], filter=["banded.band == 1"],
            metrics=[QueryMetric(metric="sum(banded.bt)", alias="total")],
        )
        resp = await engine.execute(stages)
        by = rows_by(resp, "banded.region")
        for region, expected in BAND1_BY_REGION.items():
            assert float(by[(region,)]["total"]) == pytest.approx(expected)
        sql = await _dry_sql(engine, stages)
        _assert_one_flat_with(sql, dialect)
        assert cte_aliases(sql, "_cm_", dialect=dialect), (
            f"stage producer not hoisted to the top-level WITH:\n{sql}"
        )

    async def test_combined_attach_stage_executes_in_one_flat_with(
        self, exec_backend,
    ):
        dialect, engine = exec_backend
        stages = [
            q(name="s1", dimensions=["orders.region"],
              metrics=[QueryMetric(metric="sum(orders.amount)", alias="m"),
                        QueryMetric(metric="sum(orders.amount, partition_by=[])",
                                     alias="gt")]),
            SlayerQuery(population="s1", extensions=[DatasetExtension(dataset="s1")],
                        dimensions=["s1.region"],
                        metrics=[QueryMetric(metric="sum(s1.m)", alias="ms"),
                                  QueryMetric(metric="max(s1.gt)", alias="gts")]),
        ]
        resp = await engine.execute(stages)
        by = rows_by(resp, "s1.region")
        assert {k[0]: float(r["gts"]) for k, r in by.items()} == {
            "North": 117.0, "South": 117.0, None: 117.0,
        }
        sql = await _dry_sql(engine, stages)
        _assert_one_flat_with(sql, dialect)

    async def test_windowed_and_banded_stage_executes_in_one_flat_with(
        self, exec_backend,
    ):
        dialect, engine = exec_backend
        stages = [
            q(name="s1", dimensions=["orders.region", BAND],
              time_dimensions=month_td(),
              metrics=[QueryMetric(metric="sum(orders.amount)", alias="m"),
                        QueryMetric(metric="sum(orders.amount, window='1y')",
                                     alias="w")]),
            SlayerQuery(population="s1", extensions=[DatasetExtension(dataset="s1")],
                        dimensions=["s1.region"],
                        metrics=[QueryMetric(metric="max(s1.w)", alias="wm")]),
        ]
        resp = await engine.execute(stages)
        by = rows_by(resp, "s1.region")
        for region, expected in BAND_WMAX_BY_REGION.items():
            assert float(by[(region,)]["wm"]) == pytest.approx(expected)
        sql = await _dry_sql(engine, stages)
        _assert_one_flat_with(sql, dialect)


class TestNestingIsCardinalityNeutral:
    async def test_stage_with_and_without_nested_attach_agree(
        self, exec_backend,
    ):
        """Green pin — the same downstream stage over a stage-1 with and
        without the band attach returns the same rows and shared values."""
        _, engine = exec_backend

        def stages(with_band: bool) -> list:
            dims = ["region", BAND] if with_band else ["region"]
            return [
                q(name="s1", dimensions=dims,
                  metrics=[QueryMetric(metric="sum(orders.amount)", alias="bt")]),
                SlayerQuery(population="s1", extensions=[DatasetExtension(dataset="s1")],
                            dimensions=["s1.region"],
                            metrics=[QueryMetric(metric="sum(s1.bt)", alias="t")]),
            ]

        with_attach = await engine.execute(stages(True))
        without = await engine.execute(stages(False))
        key = lambda resp: {  # noqa: E731
            r["s1.region"]: float(r["t"]) for r in resp.data
        }
        assert key(with_attach) == key(without)


class TestLiftedGuardsLeaveNoResidue:
    """Spec scenario: the former CTE-body deferral errors survive nowhere in
    the package sources."""

    @staticmethod
    def _sources_containing(fragment: str) -> list:
        package_root = Path(slayer.__file__).parent
        needle = re.sub(r"[\s\"']+", "", fragment)
        return [
            str(p) for p in package_root.rglob("*.py")
            if needle in re.sub(r"[\s\"']+", "", p.read_text(encoding="utf-8"))
        ]

    @pytest.mark.parametrize("fragment", [
        ARM_ROW_CTE_BODY, ARM_COMBINED_CTE_BODY, ARM_REROOTED_RANKED,
    ])
    def test_guard_message_gone_from_sources(self, fragment: str) -> None:
        assert not self._sources_containing(fragment)
