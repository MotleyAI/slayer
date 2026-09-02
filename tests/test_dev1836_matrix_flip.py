"""DEV-1836 task 1.9 — the DEV-1837 matrix cells flip: row regroup attaches
coexist with cross-model measures, cross-model sources become legal in
computed dimensions, and intermediate-hop dimensions execute (design D4, F3).

Spec: openspec …/specs/queries/cross-model-aggregates — "Cross-model aggregates
compose in expressions and dimensions", "Intermediate-hop dimensions are
supported"; …/specs/queries/computed-dimensions — "Measure-dimension symmetry";
…/specs/queries/partitioned-aggregates — "Attachment preserves cardinality
structurally" (nested attaches inside target-rooted producers).

(The four strict-xfail ``…×cm`` cells in
tests/test_dev1837_dimension_measure_matrix.py XPASS on the lift and must be
moved to its supported table — that flip is enforced there, not here.)
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import slayer
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1836_fixtures import (
    AMOUNT_BY_BAND,
    AMOUNT_BY_CHANNEL,
    ModelMeasure,
    POP_TOTAL,
    SPEND_BAND,
    SPEND_BY_BAND,
    SPEND_TOTAL,
    broadcast_warnings,
    dev1836_models,
    make_exec_engine,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")

#: A local aggregation-derived (row attach) dimension: channel totals band.
LOCAL_BAND = {
    "expression": "CASE WHEN amount:sum(partition_by=channel) > 30 THEN 1 ELSE 0 END",
    "name": "band",
}
#: A bare partitioned aggregate as a dimension.
LOCAL_BARE = {"expression": "amount:sum(partition_by=channel)", "name": "ct"}


def _bundle() -> ResolvedSourceBundle:
    models = dev1836_models()
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=list(models[1:]),
    )


async def _dry_scope_closed(engine, query, dialect) -> None:
    dry = await engine.execute(query, dry_run=True)
    assert "__regroup__" not in dry.sql, f"placeholder leaked:\n{dry.sql}"
    assert_scope_closed(dry.sql, dialect=dialect)


class TestRowAttachWithCrossModelMeasure:
    """The former generator guard: row regroup attach × cross-model measure."""

    async def test_band_dim_with_cm_executes(self, exec_backend):
        dialect, engine = exec_backend
        query = q(dimensions=[LOCAL_BAND], measures=[M, CM])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.band")
        assert set(by) == {(1,), (0,)}
        assert float(by[(1,)]["orders.m"]) == pytest.approx(
            AMOUNT_BY_CHANNEL["web"],
        )
        assert float(by[(0,)]["orders.m"]) == pytest.approx(
            AMOUNT_BY_CHANNEL["app"],
        )
        # The band derives from host-local columns: unattributable → broadcast.
        for row in resp.data:
            assert float(row["orders.cm"]) == pytest.approx(SPEND_TOTAL)
        await _dry_scope_closed(engine, query, dialect)

    async def test_bare_dim_with_cm_executes(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(dimensions=[LOCAL_BARE], measures=[M, CM]))
        by = rows_by(resp, "orders.ct")
        assert set(by) == {(87.0,), (30.0,)}
        for row in resp.data:
            assert float(row["orders.cm"]) == pytest.approx(SPEND_TOTAL)

    async def test_adding_cm_keeps_the_band_grain(self, exec_backend):
        _, engine = exec_backend
        solo = await engine.execute(q(dimensions=[LOCAL_BAND], measures=[M]))
        both = await engine.execute(q(dimensions=[LOCAL_BAND], measures=[M, CM]))
        solo_by = rows_by(solo, "orders.band")
        both_by = rows_by(both, "orders.band")
        assert set(solo_by) == set(both_by)
        for key, row in both_by.items():
            assert float(row["orders.m"]) == pytest.approx(
                float(solo_by[key]["orders.m"]),
            )

    async def test_rank_dim_with_cm_executes(self, exec_backend):
        """Transform-root dimension family × cross-model measure."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=[{"expression": "rank(amount:sum(partition_by=channel))",
                         "name": "rr"}],
            measures=[M, CM],
        ))
        by = {int(k[0]): row for k, row in rows_by(resp, "orders.rr").items()}
        # Channel totals rank descending: web 87 → 1, app 30 → 2.
        assert set(by) == {1, 2}
        assert float(by[1]["orders.m"]) == pytest.approx(AMOUNT_BY_CHANNEL["web"])
        assert float(by[2]["orders.m"]) == pytest.approx(AMOUNT_BY_CHANNEL["app"])
        for row in resp.data:
            assert float(row["orders.cm"]) == pytest.approx(SPEND_TOTAL)

    async def test_mixed_dim_with_cm_executes(self, exec_backend):
        """Union-grain (mixed) dimension family × cross-model measure."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=[{
                "expression": ("rank(amount:sum(partition_by=channel) - "
                               "amount:sum(partition_by=status))"),
                "name": "mr",
            }],
            measures=[M, CM],
        ))
        by = {int(k[0]): row for k, row in rows_by(resp, "orders.mr").items()}
        # (channel−status) deltas at the union grain, ranked descending:
        # (web,ok)=30→1, (web,new)=27→2, (app,ok)=-27→3, (app,new)=-30→4.
        expected_m = {1: 47.0, 2: 40.0, 3: 10.0, 4: 20.0}
        assert set(by) == set(expected_m)
        for mr, expected in expected_m.items():
            assert float(by[mr]["orders.m"]) == pytest.approx(expected), mr
            assert float(by[mr]["orders.cm"]) == pytest.approx(SPEND_TOTAL), mr

    def test_cm_rides_the_regroup_primitive(self):
        """Plan structure: the migrated cross-model measure is a combined-phase
        regroup attach; the bespoke cross-model plan list is empty/gone."""
        planned = plan_query(
            query=q(dimensions=[LOCAL_BAND], measures=[M, CM]), bundle=_bundle(),
        )
        phases = {p.attach_phase for p in planned.regroup_attach_plans}
        assert phases == {"row", "combined"}


class TestCrossModelSourceInComputedDimension:
    """The former stage_planner guard: a cross-model aggregate source inside a
    computed dimension needs a target-rooted producer."""

    async def test_spend_band_dimension_executes(self, exec_backend):
        dialect, engine = exec_backend
        query = q(
            dimensions=[{"expression": SPEND_BAND, "name": "sband"}],
            measures=[M],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.sband")
        assert set(by) == {("hi",), ("lo",)}
        for band, expected in AMOUNT_BY_BAND.items():
            assert float(by[(band,)]["orders.m"]) == pytest.approx(expected), band
        await _dry_scope_closed(engine, query, dialect)

    async def test_spend_band_coexists_with_cm_measure(self, exec_backend):
        """The D4 load-bearing cell: the cross-model measure's producer groups
        by a computed dimension whose own producer nests inside it."""
        dialect, engine = exec_backend
        query = q(
            dimensions=[{"expression": SPEND_BAND, "name": "sband"}],
            measures=[M, CM],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.sband")
        assert set(by) == {("hi",), ("lo",)}
        for band in ("hi", "lo"):
            assert float(by[(band,)]["orders.m"]) == pytest.approx(
                AMOUNT_BY_BAND[band],
            ), band
            assert float(by[(band,)]["orders.cm"]) == pytest.approx(
                SPEND_BY_BAND[band],
            ), band
        await _dry_scope_closed(engine, query, dialect)

    def test_nested_attach_lives_inside_the_producer_plan(self):
        planned = plan_query(
            query=q(
                dimensions=[{"expression": SPEND_BAND, "name": "sband"}],
                measures=[M, CM],
            ),
            bundle=_bundle(),
        )

        def _all_nested(plans) -> list:
            out = []
            for p in plans:
                inner = p.producer_plan.regroup_attach_plans
                out.extend(inner)
                out.extend(_all_nested(inner))
            return out

        nested = _all_nested(planned.regroup_attach_plans)
        assert nested
        # D8 extended: every nested attach joins on its producer's complete
        # unique key, or is a provably single-row keyless attach.
        for p in nested:
            assert p.join_pairs or not p.partition_display, p


class TestIntermediateHopDimensions:
    """The former generator raise for shared-grain dims on intermediate hops."""

    async def test_intermediate_hop_dim_executes_and_broadcasts(self, exec_backend):
        """customers.tier lies on an intermediate hop of the pop aggregate's
        chain; a region's population is not attributable per customer, so the
        metric broadcasts with metadata — never a not-implemented error."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier"],
            measures=[M, ModelMeasure(formula="customers.regions.pop:sum",
                                      name="pop")],
        )
        resp = await engine.execute(query)
        assert set(rows_by(resp, "orders.customers.tier")) == {
            ("gold",), ("silver",), ("bronze",), (None,),
        }
        for row in resp.data:
            assert float(row["orders.pop"]) == pytest.approx(POP_TOTAL)
        (w,) = broadcast_warnings(resp)
        assert "pop" in w.measure
        await _dry_scope_closed(engine, query, dialect)


class TestLiftedGuardsLeaveNoResidue:
    """Scenario: the lifted guard messages are gone from the package sources."""

    LIFTED_FRAGMENTS = (
        "A row regroup attach (computed dimension) combined with a "
        "cross-model measure is not yet supported (DEV-1836).",
        "A cross-model aggregate source inside a computed dimension",
        "A partitioned aggregate whose producer itself needs a cross-model "
        "CTE is not yet supported (DEV-1836).",
        "shared-grain dimension on an intermediate hop",
        "Windowed cross-model aggregates",
    )

    @staticmethod
    def _normalized(text: str) -> str:
        return re.sub(r"\s+", " ", text.replace('"', "").replace("'", ""))

    def _sources_containing(self, fragment: str) -> list[Path]:
        root = Path(slayer.__file__).parent
        needle = self._normalized(fragment)
        return [
            path for path in sorted(root.rglob("*.py"))
            if needle in self._normalized(path.read_text(encoding="utf-8"))
        ]

    @pytest.mark.parametrize("fragment", LIFTED_FRAGMENTS)
    def test_lifted_message_absent_from_sources(self, fragment: str) -> None:
        offenders = self._sources_containing(fragment)
        assert not offenders, offenders
