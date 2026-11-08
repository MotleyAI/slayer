"""Multi-stage sibling edges: one extractor + one sorter for ordering, planning and emission.

Spec: openspec …/specs/queries/multi-stage — "Stages are ordered by every sibling reference".
"""

from __future__ import annotations

import itertools
import re
from typing import Any, Dict, List

import pytest

import slayer.engine.query_engine as query_engine_module
import slayer.sql.generator as generator_module
from slayer.core.query import SlayerQuery
from slayer.engine.plan import plan_stages
from slayer.engine.stage_ordering import topologically_order_stages
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.generator import generate_planned_stages
from slayer.sql.render.cte_assembly import CteEntry

from tests._dev1948_fixtures import (
    AMOUNT_BY_STATUS,
    AMOUNT_BY_TIER,
    BIG_SPENDER_AMOUNT_BY_STATUS,
    CUSTOMER_AMOUNT_BY_TIER,
    SPEND_BY_TIER,
    chain_list,
    customers_model,
    inline_join_list,
    make_dev1948_engine,
    orders_model,
    pop_check_list,
    producer_at_sibling_list,
    root_over_b,
    root_producer_list,
    rows_by,
    semi_join_list,
    shared_producer_list,
    stage_b,
    stage_c,
    stage_x,
    stored_join_collision_list,
    three_reads_list,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_dev1948_engine(request):
        yield engine


@pytest.fixture
async def sqlite_engine():
    class _Req:
        param = "sqlite"

    async for engine in make_dev1948_engine(_Req()):
        yield engine


class _Capture:
    """Spies on the engine's ``plan_stages`` / ``generate_planned_stages`` and the final ``WITH`` assembly."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.queries: List[SlayerQuery] = []
        self.planned: List[Any] = []
        self.bundle: Any = None
        self.dialect: str = ""
        self.final_entries: List[CteEntry] = []
        self._assemble_calls: List[List[CteEntry]] = []
        real_plan = query_engine_module.plan_stages
        real_gen = query_engine_module.generate_planned_stages
        real_assemble = generator_module.assemble_with_chain

        def plan_spy(*, queries, bundle):
            out = real_plan(queries=queries, bundle=bundle)
            self.queries, self.planned = list(queries), list(out)
            return out

        def assemble_spy(*, entries, final, external_names=frozenset()):
            self._assemble_calls.append(list(entries))
            return real_assemble(entries=entries, final=final, external_names=external_names)

        def gen_spy(planned_queries, **kw):
            self._assemble_calls.clear()
            self.bundle, self.dialect = kw["bundle"], kw["dialect"]
            sql = real_gen(planned_queries, **kw)
            self.final_entries = self._assemble_calls[-1] if self._assemble_calls else []
            return sql

        monkeypatch.setattr(query_engine_module, "plan_stages", plan_spy)
        monkeypatch.setattr(query_engine_module, "generate_planned_stages", gen_spy)
        monkeypatch.setattr(generator_module, "assemble_with_chain", assemble_spy)

    def display(self, name: str) -> str:
        """A relation's user spelling (stage identities are minted)."""
        return {
            p.stage_schema.relation_name: p.stage_schema.display_name for p in self.planned[:-1]
        }.get(name, name)

    def stage_names(self) -> List[str]:
        """Planned stage names (user spelling), root as ``<root>``."""
        return [p.stage_schema.display_name for p in self.planned[:-1]] + ["<root>"]

    def reads(self) -> Dict[str, List[str]]:
        return {n: [self.display(r) for r in p.stage_reads]
                for n, p in zip(self.stage_names(), self.planned)}

    def segments(self) -> Dict[str, List[CteEntry]]:
        """Final ``WITH`` entries (user-spelled) grouped per stage: its hoisted CTEs then its relation; root entries last."""
        relations = set(self.stage_names()[:-1])
        out: Dict[str, List[CteEntry]] = {}
        current: List[CteEntry] = []
        for entry in self.final_entries:
            entry = entry.model_copy(update={
                "name": self.display(entry.name),
                "depends_on": [self.display(d) for d in entry.depends_on],
            })
            current.append(entry)
            if entry.name in relations:
                out[entry.name] = current
                current = []
        out["<root>"] = current
        return out


async def _capture(engine, queries, monkeypatch) -> _Capture:
    cap = _Capture(monkeypatch)
    await engine.execute(queries, dry_run=True)
    return cap


# --------------------------------------------------------------------------- #
# 1.1 Execution — hand-computed values on SQLite + DuckDB.
# --------------------------------------------------------------------------- #
class TestExecution:
    @pytest.mark.parametrize(
        "order", list(itertools.permutations(["x", "c", "b"])), ids="-".join)
    async def test_chain_in_every_supply_order(self, exec_engine, order) -> None:
        by_name = {"x": stage_x(), "c": stage_c(), "b": stage_b()}
        resp = await exec_engine.execute([*(by_name[n] for n in order), root_over_b()])
        assert rows_by(resp.data, key="b.c__tr", value="b.total") == AMOUNT_BY_TIER

    async def test_inline_model_joins_a_sibling(self, exec_engine) -> None:
        resp = await exec_engine.execute(inline_join_list())
        assert rows_by(resp.data, key="b.c__tr", value="b.total") == AMOUNT_BY_TIER

    async def test_one_stage_reads_three_siblings(self, exec_engine) -> None:
        resp = await exec_engine.execute(three_reads_list())
        assert rows_by(resp.data, key="p.x__tier", value="p.total") == CUSTOMER_AMOUNT_BY_TIER

    async def test_semi_join_over_a_sibling(self, exec_engine) -> None:
        resp = await exec_engine.execute(semi_join_list())
        assert rows_by(resp.data, key="b.status", value="b.total") == BIG_SPENDER_AMOUNT_BY_STATUS

    async def test_producer_rooted_at_a_sibling(self, exec_engine) -> None:
        resp = await exec_engine.execute(producer_at_sibling_list())
        assert rows_by(resp.data, key="s.tr", value="s.total") == SPEND_BY_TIER

    async def test_shared_producer_across_stages(self, exec_engine) -> None:
        resp = await exec_engine.execute(shared_producer_list())
        assert rows_by(resp.data, key="s2.status", value="s2.total") == AMOUNT_BY_STATUS

    async def test_stored_join_resolves_to_the_model_not_a_sibling(self, exec_engine) -> None:
        resp = await exec_engine.execute(stored_join_collision_list())
        assert rows_by(resp.data, key="b.customers__tier", value="b.total") == AMOUNT_BY_TIER

    def test_nested_source_queries_rejected(self) -> None:
        outer = {"name": "outer", "source_queries": [
            {"source_model": "x", "dimensions": ["tier"], "measures": [{"formula": "total_spend:sum", "name": "s"}]}]}
        with pytest.raises(ValueError, match=r"(?is)outer.*named stages?|named stages?.*outer"):
            SlayerQuery.model_validate({
                "name": "n", "source_model": outer, "dimensions": ["tier"],
                "measures": [{"formula": "s:sum", "name": "s3"}]})


# --------------------------------------------------------------------------- #
# 1.2 plan_stages — canonical order + typed stage_reads.
# --------------------------------------------------------------------------- #
_READS_CASES = {
    "chain": (chain_list, {"x": [], "c": ["x"], "b": ["c"], "<root>": ["b"]}),
    "inline_join": (inline_join_list, {"x": [], "c": ["x"], "b": ["c"], "<root>": ["b"]}),
    "three_reads": (three_reads_list,
                    {"o": [], "x": [], "c": ["x"], "p": ["o", "x", "c"], "<root>": ["p"]}),
    "semi_join": (semi_join_list, {"x": [], "c": ["x"], "b": ["c"], "<root>": ["b"]}),
    "producer_at_sibling": (producer_at_sibling_list,
                            {"x": [], "c": ["x"], "s": ["c"], "<root>": ["s"]}),
    "root_producer": (root_producer_list, {"x": [], "c": ["x"], "<root>": ["c"]}),
    "shared_producer": (shared_producer_list, {"s1": [], "s2": [], "<root>": ["s2"]}),
}


class TestPlanStages:
    @pytest.mark.parametrize("case", list(_READS_CASES))
    async def test_order_and_stage_reads(self, sqlite_engine, monkeypatch, case) -> None:
        build, expected = _READS_CASES[case]
        cap = await _capture(sqlite_engine, build(), monkeypatch)
        canonical = [cap.display(q.name) for q in topologically_order_stages(cap.queries)[:-1]]
        assert cap.stage_names()[:-1] == canonical
        assert cap.reads() == expected

    async def test_chain_order_is_supply_order_invariant(self, sqlite_engine, monkeypatch) -> None:
        cap = await _capture(
            sqlite_engine, [stage_b(), stage_c(), stage_x(), root_over_b()], monkeypatch)
        assert cap.stage_names() == ["x", "c", "b", "<root>"]


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres", source_model=orders_model(), referenced_models=[customers_model()],
    )


def _q(name, source, **kw) -> SlayerQuery:
    kw.setdefault("dimensions", ["status"] if source == "orders" else [])
    kw.setdefault("measures", [{"formula": "amount:sum", "name": "amt"}]
                  if source == "orders" else [{"formula": "amt:sum", "name": "total"}])
    return SlayerQuery(name=name, source_model=source, **kw)


class TestPlanStagesDirect:
    def test_independent_stages_take_the_canonical_order(self) -> None:
        queries = [_q("zeta", "orders"), _q("alpha", "orders"), _q(None, "zeta")]
        planned = plan_stages(queries=queries, bundle=_bundle())
        schemas = [p.stage_schema for p in planned[:-1]]
        assert [s.relation_name if s else None for s in schemas] == ["alpha", "zeta"]
        assert [list(p.stage_reads) for p in planned] == [[], [], ["zeta"]]

    def test_named_stage_with_unnamed_root_accepted(self) -> None:
        planned = plan_stages(queries=[_q("a", "orders"), _q(None, "a")], bundle=_bundle())
        assert [list(p.stage_reads) for p in planned] == [[], ["a"]]

    def test_single_stage_has_no_reads(self) -> None:
        planned = plan_stages(queries=[_q(None, "orders")], bundle=_bundle())
        assert list(planned[0].stage_reads) == []

    def test_unnamed_non_root_rejected(self) -> None:
        queries, bundle = [_q(None, "orders"), _q("a", "orders"), _q(None, "a")], _bundle()
        with pytest.raises(ValueError, match="must have a 'name'"):
            plan_stages(queries=queries, bundle=bundle)

    def test_self_reference_rejected(self) -> None:
        queries, bundle = [_q("a", "a"), _q(None, "a")], _bundle()
        with pytest.raises(ValueError, match="Stage 'a' references itself"):
            plan_stages(queries=queries, bundle=bundle)

    def test_root_referenced_rejected(self) -> None:
        queries, bundle = [_q("a", "r"), _q("r", "orders")], _bundle()
        with pytest.raises(ValueError, match=r"final entry 'r'.*\['a'\]"):
            plan_stages(queries=queries, bundle=bundle)

    def test_cycle_rejected(self) -> None:
        queries, bundle = [_q("a", "b"), _q("b", "a"), _q(None, "a")], _bundle()
        with pytest.raises(ValueError, match=r"Cycle.*\['a', 'b'\]"):
            plan_stages(queries=queries, bundle=bundle)

    def test_duplicate_rejected(self) -> None:
        queries, bundle = [_q("a", "orders"), _q("a", "orders"), _q(None, "a")], _bundle()
        with pytest.raises(ValueError, match="Duplicate stage name 'a'"):
            plan_stages(queries=queries, bundle=bundle)


# --------------------------------------------------------------------------- #
# 1.3 Declared edges on the final WITH assembly.
# --------------------------------------------------------------------------- #
class TestDeclaredEdges:
    @pytest.mark.parametrize("case", list(_READS_CASES))
    async def test_every_entry_declares_its_statement_reads(
        self, sqlite_engine, monkeypatch, case,
    ) -> None:
        build, _ = _READS_CASES[case]
        cap = await _capture(sqlite_engine, build(), monkeypatch)
        reads = cap.reads()
        segments = cap.segments()
        for stage, entries in segments.items():
            stage_reads = set(reads[stage])
            for entry in entries:
                assert stage_reads <= set(entry.depends_on), (stage, entry.name, entry.depends_on)
            if stage == "<root>":
                continue
            relation, hoisted = entries[-1], entries[:-1]
            assert {h.name for h in hoisted} <= set(relation.depends_on), (stage, relation.depends_on)

    async def test_root_producer_entries_are_declared(self, sqlite_engine, monkeypatch) -> None:
        cap = await _capture(sqlite_engine, root_producer_list(), monkeypatch)
        root_entries = cap.segments()["<root>"]
        assert root_entries, "the root's producer CTEs are hoisted into the chain"
        assert all("c" in e.depends_on for e in root_entries)

    async def test_later_stage_body_reuse_declares_the_shared_producer(
        self, sqlite_engine, monkeypatch,
    ) -> None:
        cap = await _capture(sqlite_engine, shared_producer_list(), monkeypatch)
        segments = cap.segments()
        s1_hoisted = {e.name for e in segments["s1"][:-1]}
        assert s1_hoisted & set(segments["s2"][-1].depends_on), (
            s1_hoisted, segments["s2"][-1].depends_on)

    async def test_stage_consumer_is_popped_after_its_render(
        self, sqlite_engine, monkeypatch,
    ) -> None:
        cap = await _capture(sqlite_engine, pop_check_list(), monkeypatch)
        segments = cap.segments()
        s1_hoisted = {e.name for e in segments["s1"][:-1]}
        s2_hoisted = {e.name for e in segments["s2"][:-1]}
        s3_deps = set(segments["s3"][-1].depends_on)
        assert s1_hoisted & s3_deps, s3_deps
        assert s2_hoisted & s3_deps, s3_deps
        assert not s1_hoisted & set(segments["s2"][-1].depends_on)
        assert not s2_hoisted & set(segments["s1"][-1].depends_on)


# --------------------------------------------------------------------------- #
# 1.4 Dependency order is a checked precondition of the generator.
# --------------------------------------------------------------------------- #
class TestOrderPrecondition:
    async def test_reader_before_its_sibling_raises(self, sqlite_engine, monkeypatch) -> None:
        cap = await _capture(sqlite_engine, producer_at_sibling_list(), monkeypatch)
        x, c, s, root = cap.planned
        with pytest.raises(ValueError) as exc:
            generate_planned_stages([c, x, s, root], bundle=cap.bundle, dialect=cap.dialect)
        message = str(exc.value)
        assert re.search(r"['\"]c['\"]", message), message
        assert re.search(r"['\"]x['\"]", message), message
