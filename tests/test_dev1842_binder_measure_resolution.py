"""DEV-1842 task 2.1 — migration of the pre-bind pass's unit suites.

``slayer/engine/measure_expansion.py`` (and its ``expand_model_measures`` /
``_ExpandCtx``) is deleted; all saved-measure resolution moves into the binder.
The eligibility matrix, cycle, and depth scenarios that
``test_measure_expansion.py`` / ``test_model_measure_expansion.py`` pinned at the
pass level are re-expressed here against the real binder path (through the
engine), importing nothing from the deleted module.

Positive bare-name resolution (root / arithmetic / transform / chained) is
already covered end-to-end by ``test_named_measures.py``; this suite pins the
NEGATIVE eligibility edges (a measure in an aggregation source / arg / kwarg is
NOT expanded), plus cycle-chain naming and the depth cap — the rules the unit
suites uniquely covered.
"""

from __future__ import annotations

import os
import pathlib
import re
from unittest import mock

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import (
    IllegalScopeReferenceError,
    MeasureCycleError,
    MeasureRecursionLimitError,
)
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.scope import StageColumn, StageSchema
from slayer.engine.binding import bind_expr
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.syntax import parse_expr

from tests._engine_helpers import _engine_generate


def _model(measures) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        measures=measures,
        default_time_dimension="created_at",
    )


async def _gen(model, formula, **kw) -> str:
    return await _engine_generate(
        query=SlayerQuery(source_model="orders",
                          measures=[{"formula": formula, "name": "x"}], **kw),
        model=model, dialect="duckdb", validate=False,
    )


_REV = ModelMeasure(name="rev", formula="amount:sum")
_CNT = ModelMeasure(name="cnt", formula="*:count")
_GRP = ModelMeasure(name="grp", formula="status")


class TestPositiveResolutionThroughBinder:
    """Every eligible operand position expands the bare measure — behaviour the
    deleted pass's matrix covered, now through the binder."""

    async def test_root_measure_equals_inline(self) -> None:
        model = _model([_REV])
        assert await _gen(model, "rev") == await _gen(_model([]), "amount:sum")

    async def test_arithmetic_operand_expanded(self) -> None:
        model = _model([_REV])
        assert await _gen(model, "rev + 1") == await _gen(_model([]), "amount:sum + 1")

    async def test_comparison_operand_expanded(self) -> None:
        model = _model([_REV])
        assert await _gen(model, "rev > 100") == await _gen(
            _model([]), "amount:sum > 100")

    async def test_unary_operand_expanded(self) -> None:
        model = _model([_REV])
        assert await _gen(model, "-rev") == await _gen(_model([]), "-amount:sum")

    async def test_boolean_operands_expanded(self) -> None:
        model = _model([_REV, _CNT])
        assert await _gen(model, "rev > 100 and cnt > 0") == await _gen(
            _model([]), "amount:sum > 100 and *:count > 0")

    async def test_scalar_arg_expanded(self) -> None:
        model = _model([_REV])
        assert await _gen(model, "nullif(rev, 0)") == await _gen(
            _model([]), "nullif(amount:sum, 0)")

    async def test_transform_input_expanded(self) -> None:
        model = _model([_REV])
        td = [{"dimension": "created_at", "granularity": "month"}]
        assert await _gen(model, "cumsum(rev)", time_dimensions=td) == await _gen(
            _model([]), "cumsum(amount:sum)", time_dimensions=td)


class TestAggregationScopeNotExpanded:
    """A saved-measure name inside an aggregation's source / arg / kwarg is
    column-level by contract and is NOT expanded — the binder rejects it."""

    async def test_agg_source_not_expanded(self) -> None:
        model = _model([_REV])
        with pytest.raises(ValueError) as ei:
            await _gen(model, "rev:sum")
        assert "saved measure" in str(ei.value).lower()

    async def test_agg_positional_arg_not_expanded(self) -> None:
        model = _model([_REV])
        with pytest.raises(ValueError) as ei:
            await _gen(model, "amount:last(rev)")
        assert "saved measure" in str(ei.value).lower()

    async def test_agg_kwarg_not_expanded(self) -> None:
        model = _model([_REV])
        with pytest.raises(ValueError) as ei:
            await _gen(model, "amount:weighted_avg(weight=rev)")
        assert "saved measure" in str(ei.value).lower()


class TestCycleAndDepthThroughBinder:
    async def test_direct_self_cycle(self) -> None:
        model = _model([ModelMeasure(name="aov", formula="aov + 1")])
        with pytest.raises((MeasureCycleError, ValueError)) as ei:
            await _gen(model, "aov")
        assert "aov" in str(ei.value)

    async def test_cycle_names_the_chain(self) -> None:
        model = _model([
            ModelMeasure(name="a", formula="b"),
            ModelMeasure(name="b", formula="a"),
        ])
        with pytest.raises((MeasureCycleError, ValueError)) as ei:
            await _gen(model, "a")
        message = str(ei.value).lower()
        assert "a" in message
        assert "b" in message
        assert "cycl" in message or "circular" in message

    async def test_depth_limit_exceeded(self) -> None:
        measures = [ModelMeasure(name="m5", formula="amount:sum")]
        for i in range(4, 0, -1):
            measures.append(ModelMeasure(name=f"m{i}", formula=f"m{i + 1}"))
        model = _model(measures)
        with mock.patch.dict(os.environ, {"SLAYER_MEASURE_EXPANSION_DEPTH": "2"}):
            with pytest.raises(MeasureRecursionLimitError) as ei:
                await _gen(model, "m1")
            assert ei.value.limit == 2

    async def test_within_depth_resolves(self) -> None:
        measures = [ModelMeasure(name="m5", formula="amount:sum")]
        for i in range(4, 0, -1):
            measures.append(ModelMeasure(name=f"m{i}", formula=f"m{i + 1}"))
        model = _model(measures)
        assert await _gen(model, "m1") == await _gen(_model([]), "amount:sum")


class TestTransformArgEligibilityDropped:
    """D2 — a transform's ``partition_by`` (and scalar args) DROP measure
    eligibility. This is a deliberate change from the deleted pass, which
    expanded transform kwargs. With ``status`` a real query dimension, the ONLY
    variable is whether ``grp`` (a measure whose formula is the bare dimension
    ``status``) expands: under D2 ``grp`` is not expanded in a transform's
    ``partition_by`` and the reference is rejected (the deleted pass substituted
    ``status`` and the query succeeded)."""

    async def test_transform_partition_by_measure_not_expanded(self) -> None:
        model = _model([_REV, _GRP])
        with pytest.raises(ValueError):
            await _gen(model, "rank(amount:sum, partition_by=grp)",
                       dimensions=["status"],
                       time_dimensions=[{"dimension": "created_at",
                                         "granularity": "month"}])


class TestStageSchemaScope:
    """A dotted saved-measure reference is illegal in a downstream ``StageSchema``
    scope — the measure feature never leaks into stage scopes (task 1.5)."""

    def test_dotted_ref_in_stage_schema_is_rejected(self) -> None:
        stage = StageSchema(
            relation_name="s",
            columns=[StageColumn(name="aov", sql_alias="aov", type=DataType.DOUBLE)],
        )
        parsed = parse_expr("customers.aov")
        bundle = ResolvedSourceBundle()
        with pytest.raises(IllegalScopeReferenceError):
            bind_expr(parsed, scope=stage, bundle=bundle)


class TestDeletedPassNotImported:
    """The migrated suite must not depend on the deleted module (task 2.1)."""

    def test_new_suites_do_not_import_measure_expansion(self) -> None:
        here = pathlib.Path(__file__).parent
        importer = re.compile(r"^\s*(from|import)\s+slayer\.engine\.measure_expansion",
                              re.MULTILINE)
        for path in list(here.glob("test_dev1842_*.py")) + [here / "_dev1842_fixtures.py"]:
            assert not importer.search(path.read_text()), (
                f"{path.name} imports the deleted pass module"
            )
