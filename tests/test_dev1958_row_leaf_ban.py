"""The grain-refining row-leaf rule is total over every transform op, the shift
family included (queries/transforms, "Transforms reject grain-refining row-level
leaves"); a projected grain key stays legal under the shift family."""

from __future__ import annotations

import re

import pytest

from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from slayer.core.query import QueryMetric

from tests._dev1846_fixtures import (
    SlayerQuery,
    dev1846_datasets,
    make_exec_engine,
    month_key,
    month_td,
)
from tests.test_law_guard_ratchet import DEFERRAL_CLASSIFIER
import slayer.core.enums

RULED_OPS = sorted(slayer.core.enums.ALL_TRANSFORMS - {"first", "last"})
SHIFT_CASES = [
    ("time_shift", "time_shift(sales.weight, -1)", "weight"),
    ("change", "change(sales.weight)", "weight"),
    ("change_pct", "change_pct(sales.weight)", "weight"),
    ("time_shift", "time_shift(sales.hi_rev, -1)", "hi_rev"),
]


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "sales")
    return SlayerQuery(**kw)


def _bundle() -> ResolvedSourceBundle:
    models = dev1846_datasets()
    return ResolvedSourceBundle(dialect="postgres", population=models[0], referenced_datasets=models[1:])


def _call(op: str, inner: str) -> str:
    if op == "ntile":
        return f"ntile({inner}, n=4)"
    if op in ("rank", "dense_rank"):
        return f"{op}({inner}, direction='desc')"
    return f"time_shift({inner}, -1)" if op == "time_shift" else f"{op}({inner})"


def _assert_unified_message(msg: str, *, op: str, leaf: str) -> None:
    """Names the transform and the leaf, carries all three remedies, cites no issue."""
    assert op in msg, msg
    assert leaf in msg, msg
    assert re.search(r"(?i)row-level", msg), msg
    assert f"{op}(sum({leaf}))" in msg, msg
    assert ":sum" not in msg, msg
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
                   metrics=[QueryMetric(metric=formula, alias="t")])
        bundle = _bundle()
        with pytest.raises(ValueError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_unified_message(str(ei.value), op=op, leaf=leaf)

    @pytest.mark.parametrize("op, formula, leaf", SHIFT_CASES)
    async def test_rejected_via_engine(self, exec_engine, op, formula, leaf) -> None:
        query = _q(time_dimensions=month_td(),
                   metrics=[QueryMetric(metric=formula, alias="t")])
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(query)
        _assert_unified_message(str(ei.value), op=op, leaf=leaf)


class TestOneMessageForEveryOp:
    @pytest.mark.parametrize("op", RULED_OPS)
    def test_bare_row_leaf_message(self, op) -> None:
        query = _q(time_dimensions=month_td(),
                   metrics=[QueryMetric(metric=_call(op, "weight"), alias="t")])
        bundle = _bundle()
        with pytest.raises(ValueError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_unified_message(str(ei.value), op=op, leaf="weight")


class TestProjectedGrainKeyUnderShift:
    async def test_time_shift_over_projected_dimension(self, exec_engine) -> None:
        kw = {"dimensions": ["sales.store"], "time_dimensions": month_td()}
        resp = await exec_engine.execute(_q(
            metrics=[QueryMetric(metric="time_shift(sales.store, -1)", alias="t")], **kw))
        plain = await exec_engine.execute(_q(
            metrics=[QueryMetric(metric="sum(sales.revenue)", alias="r")], **kw))
        cells = {(r["sales.store"], month_key(r["sales.ordered_at"])) for r in resp.data}
        assert len(cells) == len(resp.data) == len(plain.data)
        for r in resp.data:
            assert r["t"] in (None, r["sales.store"]), r
        assert any(r["t"] is not None for r in resp.data)

    async def test_predicate_over_projected_dimension(self, exec_engine) -> None:
        kw = {"dimensions": ["sales.store"], "time_dimensions": month_td()}
        resp = await exec_engine.execute(_q(
            metrics=[QueryMetric(metric="time_shift(sales.store in ('A', 'B'), -1)",
                                   alias="t")], **kw))
        plain = await exec_engine.execute(_q(
            metrics=[QueryMetric(metric="sum(sales.revenue)", alias="r")], **kw))
        assert len(resp.data) == len(plain.data)
        for r in resp.data:
            assert r["t"] in (None, r["sales.store"] in ("A", "B")), r
        assert any(r["t"] is not None for r in resp.data)

    async def test_time_shift_in_filter_position(self, exec_engine) -> None:
        kw = {"dimensions": ["sales.store"], "time_dimensions": month_td()}
        resp = await exec_engine.execute(_q(
            filter=["time_shift(sales.store, -1) = 'A'"],
            metrics=[QueryMetric(metric="sum(sales.revenue)", alias="r")], **kw))
        plain = await exec_engine.execute(_q(
            metrics=[QueryMetric(metric="sum(sales.revenue)", alias="r")], **kw))
        store_a = sorted(month_key(r["sales.ordered_at"]) for r in plain.data
                         if r["sales.store"] == "A")
        got = sorted((r["sales.store"], month_key(r["sales.ordered_at"])) for r in resp.data)
        assert got == [("A", m) for m in store_a[1:]], got
