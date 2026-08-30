"""DEV-1837 guard rework (design D5) — both directions.

Lifted direction: a row regroup attach (computed dimension) with a transform
measure renders in both chains; the old catch-all DEV-1824 arm is gone from the
source. Remaining deferrals: three narrowed arms with exact messages (windowed/
ranked → DEV-1835, cross-model → DEV-1836, CTE-body → DEV-1838) plus the
combined-attach CTE-body arm re-pointed to DEV-1838, and the two stale
stage_planner DEV-1824 refs re-pointed (D5).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import slayer
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import SQLGenerator

from tests._dev1837_fixtures import (
    BAND35,
    ModelMeasure,
    dev1837_models,
    gen,
    month_td,
    q,
)

BAND = {"expression": BAND35, "name": "band"}

ARM_WINDOWED_RANKED = (
    "A row regroup attach (computed dimension) combined with a windowed or "
    "ranked measure is not yet supported (DEV-1835)."
)
ARM_CROSS_MODEL = (
    "A row regroup attach (computed dimension) combined with a cross-model "
    "measure is not yet supported (DEV-1836)."
)
ARM_ROW_CTE_BODY = (
    "A row regroup attach (computed dimension) nested in a CTE body is not "
    "yet supported (DEV-1838)."
)
ARM_COMBINED_CTE_BODY = (
    "A partitioned-aggregate regroup attach nested in a CTE body is not yet "
    "supported (DEV-1838)."
)

#: The lifted DEV-1824 catch-all arm — must survive nowhere in the package.
OLD_ARM_FRAGMENT = (
    "combined with a cross-model / windowed / ranked / transform measure, "
    "or nested in a CTE body, is not yet supported (DEV-1824)"
)
#: The two stage_planner messages whose DEV-1824 refs D5 re-points.
STALE_PLANNER_FRAGMENTS = (
    "inside a computed dimension (e.g. 'customers.spend:sum(partition_by=...)') "
    "is not yet supported (DEV-1824)",
    "whose producer itself needs a cross-model or nested-regroup CTE "
    "is not yet supported (DEV-1824)",
)


def _normalized(text: str) -> str:
    """Source text with whitespace and quotes collapsed, so a string literal
    wrapped across source lines still matches its runtime message."""
    return re.sub(r"[\s\"']+", "", text)


def _sources_containing(fragment: str) -> list:
    package_root = Path(slayer.__file__).parent
    needle = _normalized(fragment)
    return [
        str(p) for p in package_root.rglob("*.py")
        if needle in _normalized(p.read_text())
    ]


def _bundle() -> ResolvedSourceBundle:
    models = dev1837_models()
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=list(models[1:]),
    )


class TestLiftedDirection:
    @pytest.mark.parametrize(
        "formula", ["cumsum(amount:sum)", "time_shift(amount:sum, -1)"],
    )
    async def test_band_with_transform_renders(self, formula: str) -> None:
        sql = await gen(q(
            dimensions=["region", BAND],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula=formula, name="x"),
            ],
        ))
        assert "__regroup__" not in sql, sql

    async def test_band_with_partitioned_and_transform_renders(self) -> None:
        """The cross-model chain's lifted direction (Codex F8 — behavioral
        negative alongside the source scan)."""
        sql = await gen(q(
            dimensions=["region", BAND],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
                ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
            ],
        ))
        assert "__regroup__" not in sql, sql

    def test_old_catch_all_arm_has_no_remaining_references(self) -> None:
        assert not _sources_containing(OLD_ARM_FRAGMENT)


class TestRemainingArmsExactMessages:
    async def test_windowed_measure_arm(self) -> None:
        with pytest.raises(NotImplementedError) as ei:
            await gen(q(
                dimensions=["region", BAND],
                time_dimensions=month_td(),
                measures=[
                    ModelMeasure(formula="amount:sum", name="m"),
                    ModelMeasure(formula="amount:sum(window='1y')", name="w"),
                ],
            ))
        assert str(ei.value) == ARM_WINDOWED_RANKED

    async def test_ranked_measure_arm(self) -> None:
        with pytest.raises(NotImplementedError) as ei:
            await gen(q(
                dimensions=["region", BAND],
                measures=[
                    ModelMeasure(formula="amount:sum", name="m"),
                    ModelMeasure(formula="amount:last", name="l"),
                ],
            ))
        assert str(ei.value) == ARM_WINDOWED_RANKED

    async def test_cross_model_measure_arm(self) -> None:
        with pytest.raises(NotImplementedError) as ei:
            await gen(q(
                dimensions=["region", BAND],
                measures=[
                    ModelMeasure(formula="amount:sum", name="m"),
                    ModelMeasure(formula="customers.spend:sum", name="cm"),
                ],
            ))
        assert str(ei.value) == ARM_CROSS_MODEL


class TestCteBodyArms:
    def test_row_attach_in_cte_body_arm(self) -> None:
        planned = plan_query(query=q(
            dimensions=["region", BAND],
            measures=[ModelMeasure(formula="amount:sum", name="m")],
        ), bundle=_bundle())
        generator = SQLGenerator(dialect="postgres")
        with pytest.raises(NotImplementedError) as ei:
            generator.generate_from_planned(
                planned, bundle=_bundle(), as_cte_body=True,
            )
        assert str(ei.value) == ARM_ROW_CTE_BODY

    def test_combined_attach_in_cte_body_arm(self) -> None:
        planned = plan_query(query=q(
            dimensions=["region"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
            ],
        ), bundle=_bundle())
        generator = SQLGenerator(dialect="postgres")
        with pytest.raises(NotImplementedError) as ei:
            generator.generate_from_planned(
                planned, bundle=_bundle(), as_cte_body=True,
            )
        assert str(ei.value) == ARM_COMBINED_CTE_BODY


class TestStalePlannerRefsRepointed:
    """D5 — only the two demonstrably stale stage_planner DEV-1824 refs move."""

    def test_stale_fragments_gone_from_source(self) -> None:
        offenders = [
            (path, frag)
            for frag in STALE_PLANNER_FRAGMENTS
            for path in _sources_containing(frag)
        ]
        assert not offenders, offenders

    async def test_cross_model_dim_source_points_at_stage_three(self) -> None:
        cband = (
            "CASE WHEN customers.spend:sum(partition_by=region) > 100 "
            "THEN 1 ELSE 0 END"
        )
        with pytest.raises(
            NotImplementedError,
            match=r"cross-model aggregate source inside a computed dimension",
        ) as ei:
            await gen(q(
                dimensions=["region", {"expression": cband, "name": "cband"}],
                measures=[ModelMeasure(formula="amount:sum", name="m")],
            ))
        assert "DEV-1836" in str(ei.value)
        assert "DEV-1824" not in str(ei.value)
