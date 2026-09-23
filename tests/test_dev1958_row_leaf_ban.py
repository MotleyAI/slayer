"""The grain-refining row-leaf rule is total over every transform op, the shift
family included (queries/transforms, "Transforms reject grain-refining row-level
leaves"); a projected grain key stays legal under the shift family."""

from __future__ import annotations

import re

import pytest

from slayer.core.formula import ALL_TRANSFORMS
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1846_fixtures import (
    ModelMeasure,
    SlayerQuery,
    dev1846_models,
    make_exec_engine,
    month_key,
    month_td,
)
from tests.test_law_guard_ratchet import DEFERRAL_CLASSIFIER

RULED_OPS = sorted(ALL_TRANSFORMS - {"first", "last"})
SHIFT_CASES = [
    ("time_shift", "time_shift(weight, -1)", "weight"),
    ("change", "change(weight)", "weight"),
    ("change_pct", "change_pct(weight)", "weight"),
    ("time_shift", "time_shift(hi_rev, -1)", "hi_rev"),
]


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "sales")
    return SlayerQuery(**kw)


def _bundle() -> ResolvedSourceBundle:
    models = dev1846_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _call(op: str, inner: str) -> str:
    return f"ntile({inner}, n=4)" if op == "ntile" else f"{op}({inner})"


def _assert_unified_message(msg: str, *, op: str, leaf: str) -> None:
    """Names the transform and the leaf, carries all three remedies, cites no issue."""
    assert op in msg, msg
    assert leaf in msg, msg
    assert re.search(r"(?i)row-level", msg), msg
    assert ":sum" in msg, msg
    assert "dimension" in msg, msg
    assert "source_queries" in msg, msg
    assert "DEV-" not in msg, msg
    assert not DEFERRAL_CLASSIFIER.search(msg), msg


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestShiftFamilyCovered:
    @pytest.mark.parametrize("op, formula, leaf", SHIFT_CASES)
    def test_rejected_at_plan_time(self, op, formula, leaf) -> None:
        query = _q(time_dimensions=month_td(),
                   measures=[ModelMeasure(formula=formula, name="t")])
        with pytest.raises(ValueError) as ei:
            plan_query(query=query, bundle=_bundle())
        _assert_unified_message(str(ei.value), op=op, leaf=leaf)

    @pytest.mark.parametrize("op, formula, leaf", SHIFT_CASES)
    async def test_rejected_via_engine(self, exec_engine, op, formula, leaf) -> None:
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(_q(
                time_dimensions=month_td(),
                measures=[ModelMeasure(formula=formula, name="t")]))
        _assert_unified_message(str(ei.value), op=op, leaf=leaf)


class TestOneMessageForEveryOp:
    @pytest.mark.parametrize("op", RULED_OPS)
    def test_bare_row_leaf_message(self, op) -> None:
        query = _q(time_dimensions=month_td(),
                   measures=[ModelMeasure(formula=_call(op, "weight"), name="t")])
        with pytest.raises(ValueError) as ei:
            plan_query(query=query, bundle=_bundle())
        _assert_unified_message(str(ei.value), op=op, leaf="weight")


class TestProjectedGrainKeyUnderShift:
    async def test_time_shift_over_projected_dimension(self, exec_engine) -> None:
        kw = {"dimensions": ["store"], "time_dimensions": month_td()}
        resp = await exec_engine.execute(_q(
            measures=[ModelMeasure(formula="time_shift(store, -1)", name="t")], **kw))
        plain = await exec_engine.execute(_q(
            measures=[ModelMeasure(formula="revenue:sum", name="r")], **kw))
        cells = {(r["sales.store"], month_key(r["sales.ordered_at"])) for r in resp.data}
        assert len(cells) == len(resp.data) == len(plain.data)
        for r in resp.data:
            assert r["sales.t"] in (None, r["sales.store"]), r
        assert any(r["sales.t"] is not None for r in resp.data)
