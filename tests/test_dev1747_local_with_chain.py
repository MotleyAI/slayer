"""DEV-1747 D8 — the LOCAL transform chain's WITH clause, assembled as AST.

PR 3 (DEV-1746) built one WITH assembler (``slayer/sql/render/cte_assembly.py``)
and adopted it on the cross-model sites, deferring the LOCAL single-model
transform chain to this PR — its two near-identical sites still splice out of
f-strings::

    cte_clause = "WITH " + ",\\n".join(f"{name} AS (\\n{sql}\\n)" for ...)
    chain_sql  = f"{cte_clause}\\n{inner_sql}"

so the emitted order is whatever order the python list happened to be built in,
and each step reads its predecessor POSITIONALLY (``prev_cte = ctes[-1][0]``).
D8 moves both onto ``assemble_with_chain`` with DECLARED dependencies (step N
depends on step N-1), and — per PR 3's first hard-won lesson — keeps the bodies
as ``exp.Select`` from renderer to assembler rather than rendering to text and
re-parsing.

That lesson is the reason for :class:`TestNoTextRoundTrip` below. A dotted
public alias round-trips through text as a MULTI-PART reference on BigQuery
(``_base."orders_x.status"`` came back as ``` `_base___orders_x`.`status` ```),
and this path is full of dotted ``<relation>.<alias>`` names — so a parse seam
here would silently corrupt exactly the identifiers it carries.

Refs: DEV-1747 (D8), DEV-1746 handoff, DEV-1742 §5.6.
"""
from __future__ import annotations

import pytest
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from tests._dev1747_fixtures import (
    cte_body_names,
    dev1747_models,
    order_by_text,
    with_node_of,
)
from tests._engine_helpers import _engine_generate
from slayer.sql import generator
from slayer.sql.generator import SQLGenerator
from slayer.sql.render import cte_assembly
import inspect

#: A LOCAL (single-model) transform chain — no cross-model measure, so it takes
#: the f-string splice path rather than the cross-model one PR 3 already fixed.
_CHAIN_QUERY = SlayerQuery(
    source_model="orders",
    time_dimensions=[TimeDimension(
        dimension=ColumnRef(name="created_at"),
        granularity=TimeGranularity.MONTH,
    )],
    measures=[
        {"formula": "amount:sum", "name": "rev"},
        {"formula": "cumsum(amount:sum)", "name": "cs"},
    ],
)

#: Two chained transforms, so the chain has more than one step and dependency
#: ORDER is actually observable.
_MULTI_STEP_QUERY = SlayerQuery(
    source_model="orders",
    time_dimensions=[TimeDimension(
        dimension=ColumnRef(name="created_at"),
        granularity=TimeGranularity.MONTH,
    )],
    measures=[
        {"formula": "amount:sum", "name": "rev"},
        {"formula": "cumsum(amount:sum)", "name": "cs"},
        {"formula": "change(amount:sum)", "name": "ch"},
    ],
)


async def _sql(query: SlayerQuery, *, dialect: str = "postgres") -> str:
    models = dev1747_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:], dialect=dialect,
    )


def _assembler_spy(monkeypatch) -> list:
    """Record the ``CteEntry`` list handed to ``assemble_with_chain``.

    Patched as BOUND in ``slayer.sql.generator`` (which does ``from … import
    assemble_with_chain``), so patching the defining module would record
    nothing and every assertion downstream would pass vacuously. The vacuity
    guard is the ``assert entries`` in each caller.
    """

    seen: list = []
    original = cte_assembly.assemble_with_chain

    def _recording(*, entries, final):
        seen.extend(entries)
        return original(entries=entries, final=final)

    monkeypatch.setattr(generator, "assemble_with_chain", _recording)
    return seen


# ---------------------------------------------------------------------------
# Group 1 — assembled, and in dependency order
# ---------------------------------------------------------------------------
class TestWithChainAssembly:
    async def test_chain_emits_a_single_top_level_with(self) -> None:
        sql = await _sql(_CHAIN_QUERY)
        assert cte_body_names(sql), f"no WITH clause emitted:\n{sql}"

    async def test_each_step_is_emitted_after_the_step_it_reads(self) -> None:
        """The invariant the positional ``ctes[-1][0]`` encodes implicitly and
        the assembler makes explicit: a CTE must never be referenced before it
        is defined."""
        sql = await _sql(_MULTI_STEP_QUERY)
        with_node = with_node_of(sql, dialect="postgres")
        assert with_node is not None, f"no WITH clause:\n{sql}"
        defined: set[str] = set()
        for cte in with_node.expressions:
            for table in cte.this.find_all(exp.Table):
                name = table.name
                if name in {c.alias_or_name for c in with_node.expressions}:
                    assert name in defined, (
                        f"CTE {cte.alias_or_name!r} reads {name!r} before it is "
                        f"defined:\n{sql}"
                    )
            defined.add(cte.alias_or_name)

    async def test_cte_names_are_unique(self) -> None:
        sql = await _sql(_MULTI_STEP_QUERY)
        names = cte_body_names(sql)
        assert len(names) == len(set(names)), f"duplicate CTE names {names}:\n{sql}"

    async def test_chain_still_orders_and_paginates(self) -> None:
        """The chain's outer wrap is where ORDER BY / LIMIT land; D8 must not
        disturb that while replacing the splice."""
        sql = await _sql(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
            order=[OrderItem(column=ColumnRef(name="cs"), direction="desc")],
            limit=2,
        ))
        assert order_by_text(sql), f"chain lost its ORDER BY:\n{sql}"
        assert "LIMIT" in sql.upper() or "TOP" in sql.upper()


# ---------------------------------------------------------------------------
# Group 2 — no render-to-text-and-re-parse (PR 3's lesson 1)
# ---------------------------------------------------------------------------
class TestNoTextRoundTrip:
    def test_window_transform_renderer_returns_ast(self) -> None:
        """``_render_window_transform_sql`` must hand back AST, not a SQL
        string — a string body forces a parse at the assembler seam (D8)."""
        signature = inspect.signature(SQLGenerator._render_window_transform_sql)
        assert signature.return_annotation is not str, (
            "the window transform renderer must hand back AST so the local "
            "chain never round-trips through text (D8)"
        )

    async def test_local_chain_does_not_call_the_parse_seam(
        self, monkeypatch,
    ) -> None:
        """``_parse_cte_body`` is the documented seam PR 3 kept for the ONE
        input that arrives as a complete nested statement. The local chain
        builds its own bodies, so for THIS query it must not be called at all.

        A source-level ``count(...) <= 2`` would pass while permitting two live
        round-trips — the very thing D8 removes. A raising sentinel scoped to
        this render is the exact claim: zero.
        """

        def _boom(self, sql):  # noqa: ANN001 - signature mirrors the seam
            raise AssertionError(
                "the local transform chain routed a CTE body through the parse "
                "seam; keep bodies as exp.Select from renderer to assembler (D8)"
            )

        monkeypatch.setattr(SQLGenerator, "_parse_cte_body", _boom)
        await _sql(_MULTI_STEP_QUERY)

    async def test_chain_bodies_reach_the_assembler_as_ast(
        self, monkeypatch,
    ) -> None:
        """The positive half. Every entry the chain hands the assembler must
        already be a ``exp.Select`` — a string body would mean the renderer
        still emits text and something downstream re-parses it."""
        entries = _assembler_spy(monkeypatch)
        await _sql(_MULTI_STEP_QUERY)
        assert entries, (
            "the local chain never called assemble_with_chain — it is still "
            "splicing its WITH clause out of f-strings (D8)"
        )
        for entry in entries:
            assert isinstance(entry.query, exp.Select), (
                f"CTE {entry.name!r} reached the assembler as "
                f"{type(entry.query).__name__}, not exp.Select"
            )

    async def test_chain_declares_its_dependencies(self, monkeypatch) -> None:
        """D8's substantive change. Emitting in the right order is not the
        contract — DECLARING the dependency is, because the current code gets
        the order right by reading ``ctes[-1][0]`` positionally, which is
        correct only for as long as nothing ever inserts a step.

        A correct emission order therefore cannot distinguish "fixed" from
        "still positional"; the declared ``depends_on`` can.
        """
        entries = _assembler_spy(monkeypatch)
        await _sql(_MULTI_STEP_QUERY)
        assert entries, "assemble_with_chain was never called (D8)"
        declared = {entry.name: set(entry.depends_on) for entry in entries}
        chained = [name for name, deps in declared.items() if deps]
        assert chained, (
            f"no CTE declares a dependency, so the chain is still ordered "
            f"positionally: {declared}"
        )
        names = set(declared)
        for name, deps in declared.items():
            assert deps <= names, (
                f"CTE {name!r} declares dependencies outside the chain: "
                f"{deps - names}"
            )

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    async def test_dotted_aliases_survive_on_mangling_dialects(
        self, dialect: str,
    ) -> None:
        """The concrete corruption PR 3 hit: a dotted public alias re-parsed as
        a multi-part reference. This path carries dotted
        ``<relation>.<alias>`` names throughout, so it is the exposed one."""
        sql = await _sql(_MULTI_STEP_QUERY, dialect=dialect)
        assert "_base___" not in sql, (
            f"a dotted alias was re-read as a multi-part reference:\n{sql}"
        )


# ---------------------------------------------------------------------------
# Group 3 — semantics preserved across dialects
# ---------------------------------------------------------------------------
class TestChainSemanticsPreserved:
    @pytest.mark.parametrize(
        "dialect", ["postgres", "sqlite", "duckdb", "bigquery", "tsql"],
    )
    async def test_chain_parses_on_every_tier_one_dialect(
        self, dialect: str,
    ) -> None:
        sql = await _sql(_MULTI_STEP_QUERY, dialect=dialect)
        assert sql

    async def test_hidden_order_slot_survives_the_chain(self) -> None:
        """A hidden order-only slot has to be carried through every step; the
        carry lists are plan-ordered (B8, PR 3) and must stay that way."""
        sql = await _sql(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
            order=[OrderItem(column=ColumnRef(name="status"), direction="desc")],
        ))
        assert order_by_text(sql)

    async def test_post_phase_filter_still_wraps_the_chain(self) -> None:
        sql = await _sql(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
            filters=["cs > 5"],
        ))
        assert "WHERE" in sql.upper()
