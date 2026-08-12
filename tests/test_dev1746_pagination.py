"""DEV-1746 §5.9 — pagination through the dialect strategy (B3).

Today SLayer paginates three different ways depending on which render path a
query happens to take:

* the plain single-model path sets ``limit``/``offset`` on the sqlglot
  ``Select`` (correct everywhere — sqlglot transposes T-SQL's ``TOP`` /
  ``OFFSET … FETCH``),
* the transform-chain paths hand the detached nodes to ``emit_outer_wrap``
  (also correct — ``TsqlDialect`` re-attaches them to a ``Select``),
* and the **cross-model combined path appends raw text**::

      sql += f"\\nLIMIT {planned_query.limit}"

  which emits a literal ``LIMIT 10 OFFSET 5`` on SQL Server. So the *same*
  cross-model query is valid T-SQL when it carries a transform layer and
  invalid when it does not.

B3 routes every path through one dialect-strategy hook. The T-SQL rule is
specified rather than discovered: limit-only becomes ``TOP``; an offset without
an ``ORDER BY`` gets a deterministic ``ORDER BY (SELECT NULL)`` before
``OFFSET … ROWS``, because SQL Server rejects ``OFFSET`` without ordering.
sqlglot 30.11 happens to do this itself, but that is *its* behaviour, not our
contract — these tests pin the contract so a sqlglot upgrade that drops it
fails here instead of at a customer's database.

The matrix is {limit-only, offset-only, both} x {with, without ORDER BY} x
{tsql, bigquery, postgres, snowflake, sqlite} x {plain, outer-trim, combined},
plus SQLite execution for all three shapes (Codex D5 — pagination changes row
sets, so parse-only coverage is not enough).
"""

from __future__ import annotations

import os
import re
import tempfile
from typing import AsyncIterator, Optional

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect

from tests._dev1746_fixtures import (
    dev1746_models,
    make_sqlite_engine,
    outer_clause_sql,
    outer_statement,
    seed_dev1746_sqlite,
)
from tests._engine_helpers import _engine_generate

DIALECTS = ["tsql", "bigquery", "postgres", "snowflake", "sqlite"]

#: (limit, offset) — the three pagination combinations §5.9 names.
PAGINATION_COMBOS = [
    pytest.param(10, None, id="limit-only"),
    pytest.param(None, 5, id="offset-only"),
    pytest.param(10, 5, id="both"),
]

#: A standalone ``LIMIT`` keyword — not a substring of an identifier.
_BARE_LIMIT = re.compile(r"(?<![\w.\[\"`])LIMIT\s+\d+", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# Query shapes
# --------------------------------------------------------------------------- #
def _plain_query(
    *, limit: Optional[int], offset: Optional[int], ordered: bool,
) -> SlayerQuery:
    """Single-model aggregate — no isolation CTE, no hidden slot."""
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="amount:sum", name="revenue")],
        order=[OrderItem(column="amount:sum", direction="desc")] if ordered else [],
        limit=limit,
        offset=offset,
    )


def _outer_trim_query(
    *, limit: Optional[int], offset: Optional[int], ordered: bool,
) -> SlayerQuery:
    """Ordering by an aggregate that is NOT projected materialises a hidden
    slot, so the generator wraps the base SELECT to trim it — the outer-trim
    shape, whose pagination lands on the wrapper rather than the base."""
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="*:count", name="n")],
        order=[OrderItem(column="amount:sum", direction="desc")] if ordered else [],
        limit=limit,
        offset=offset,
    )


def _combined_query(
    *, limit: Optional[int], offset: Optional[int], ordered: bool,
) -> SlayerQuery:
    """Cross-model measure — the combined-SELECT path that appends raw text."""
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="customers.tier")],
        measures=[ModelMeasure(formula="customers.spend:sum", name="spend")],
        order=(
            [OrderItem(column="customers.spend:sum", direction="desc")]
            if ordered else []
        ),
        limit=limit,
        offset=offset,
    )


SHAPES = {
    "plain": _plain_query,
    "outer_trim": _outer_trim_query,
    "combined": _combined_query,
}


async def _gen_sql(query: SlayerQuery, *, dialect: str) -> str:
    models = dev1746_models()
    return await _engine_generate(
        query=query, model=models[0], dialect=dialect, extra_models=models[1:],
    )


@pytest.fixture
async def exec_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "dev1746.db")
        seed_dev1746_sqlite(db_path)
        yield await make_sqlite_engine(os.path.join(d, "store"), db_path)


# =========================================================================== #
# The §5.9 matrix.
# =========================================================================== #
class TestPaginationMatrix:

    @pytest.mark.parametrize("shape", sorted(SHAPES))
    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("limit,offset", PAGINATION_COMBOS)
    @pytest.mark.parametrize("ordered", [True, False], ids=["ordered", "unordered"])
    async def test_emitted_sql_parses_under_its_own_dialect(
        self, shape: str, dialect: str, limit, offset, ordered: bool,
    ) -> None:
        """Every cell of the matrix must emit SQL its own dialect can parse."""
        query = SHAPES[shape](limit=limit, offset=offset, ordered=ordered)
        sql = await _gen_sql(query, dialect=dialect)
        parsed = sqlglot.parse(sql, dialect=dialect)
        assert len(parsed) == 1, (
            f"[{dialect}/{shape}] did not parse to a single statement:\n{sql}"
        )

    @pytest.mark.parametrize("shape", sorted(SHAPES))
    @pytest.mark.parametrize("limit,offset", PAGINATION_COMBOS)
    @pytest.mark.parametrize("ordered", [True, False], ids=["ordered", "unordered"])
    async def test_tsql_never_emits_a_bare_limit_keyword(
        self, shape: str, limit, offset, ordered: bool,
    ) -> None:
        """NEW (B3): SQL Server has no ``LIMIT``. The combined shape emits one
        today; after B3 no shape does."""
        query = SHAPES[shape](limit=limit, offset=offset, ordered=ordered)
        sql = await _gen_sql(query, dialect="tsql")
        found = _BARE_LIMIT.search(sql)
        assert found is None, (
            f"[tsql/{shape}] emitted a literal {found.group(0)!r}, which SQL "
            f"Server rejects:\n{sql}"
        )

    @pytest.mark.parametrize("shape", sorted(SHAPES))
    @pytest.mark.parametrize("ordered", [True, False], ids=["ordered", "unordered"])
    async def test_tsql_limit_only_uses_top(
        self, shape: str, ordered: bool,
    ) -> None:
        """The specified T-SQL rule, part 1: a limit with no offset is ``TOP``.

        Asserted on the OUTER statement rendered without its CTEs — a global
        search could be satisfied by a ``TOP`` inside an inner scope.
        """
        query = SHAPES[shape](limit=10, offset=None, ordered=ordered)
        sql = await _gen_sql(query, dialect="tsql")
        outer = outer_clause_sql(sql, dialect="tsql")
        assert re.search(r"\bTOP\b", outer, re.IGNORECASE), (
            f"[tsql/{shape}] limit-only did not transpose to TOP on the outer "
            f"statement.\nouter: {outer}\n\nfull SQL:\n{sql}"
        )

    @pytest.mark.parametrize("shape", sorted(SHAPES))
    @pytest.mark.parametrize("limit,offset", [(None, 5), (10, 5)])
    async def test_tsql_offset_always_carries_an_order_by(
        self, shape: str, limit, offset,
    ) -> None:
        """The specified T-SQL rule, part 2: ``OFFSET`` requires an ``ORDER BY``
        **on the SELECT that carries the OFFSET**.

        The ordering is asserted on the outer statement's own AST node, not by
        searching the text: a window function's ``OVER (ORDER BY …)``, a hidden
        order slot, or an inner CTE would satisfy a global search while leaving
        the paginated SELECT unordered — precisely the SQL Server error this
        rule exists to prevent.
        """
        query = SHAPES[shape](limit=limit, offset=offset, ordered=False)
        sql = await _gen_sql(query, dialect="tsql")
        outer = outer_statement(sql, dialect="tsql")
        assert outer.args.get("order") is not None, (
            f"[tsql/{shape}] the paginated SELECT has no ORDER BY of its own — "
            f"SQL Server rejects OFFSET without ordering:\n{sql}"
        )
        rendered = outer_clause_sql(sql, dialect="tsql")
        assert re.search(r"\bOFFSET\s+\d+\s+ROWS?\b", rendered, re.IGNORECASE), (
            f"[tsql/{shape}] OFFSET did not transpose to `OFFSET n ROWS`:\n"
            f"{rendered}"
        )

    @pytest.mark.parametrize("shape", sorted(SHAPES))
    async def test_tsql_limit_with_offset_uses_fetch(self, shape: str) -> None:
        """With an offset present the limit becomes ``FETCH … ROWS ONLY``
        (``TOP`` cannot express a window)."""
        query = SHAPES[shape](limit=10, offset=5, ordered=True)
        sql = await _gen_sql(query, dialect="tsql")
        outer = outer_clause_sql(sql, dialect="tsql")
        assert re.search(r"\bFETCH\b.*\bROWS?\s+ONLY\b", outer, re.IGNORECASE | re.S), (
            f"[tsql/{shape}] limit+offset did not transpose to FETCH on the "
            f"outer statement:\n{outer}"
        )

    @pytest.mark.parametrize(
        "dialect", [d for d in DIALECTS if d != "tsql"],
    )
    @pytest.mark.parametrize("shape", sorted(SHAPES))
    async def test_non_tsql_dialects_keep_limit_offset(
        self, dialect: str, shape: str,
    ) -> None:
        """Regression guard: routing through the hook must not change the
        dialects that were already correct — and the bounds must land on the
        OUTER statement, not on some inner scope."""
        query = SHAPES[shape](limit=10, offset=5, ordered=True)
        sql = await _gen_sql(query, dialect=dialect)
        outer = outer_statement(sql, dialect=dialect)
        assert outer.args.get("limit") is not None, (
            f"[{dialect}/{shape}] the outer statement carries no LIMIT:\n{sql}"
        )
        assert outer.args.get("offset") is not None, (
            f"[{dialect}/{shape}] the outer statement carries no OFFSET:\n{sql}"
        )


# =========================================================================== #
# The hook itself.
# =========================================================================== #
class TestApplyPaginationHook:
    """``SqlDialect.apply_pagination`` is the single place pagination is
    expressed, so it gets direct coverage independent of any query shape."""

    @staticmethod
    def _select() -> exp.Select:
        return exp.Select().select(exp.column("a")).from_("t")

    def test_hook_exists_on_the_dialect_strategy(self) -> None:
        strategy = get_dialect("postgres")
        assert hasattr(strategy, "apply_pagination"), (
            "the pagination hook is missing — pagination still lives in the "
            "generator rather than the dialect strategy (P-H)."
        )

    def test_no_pagination_is_a_no_op(self) -> None:
        strategy = get_dialect("postgres")
        out = strategy.apply_pagination(self._select(), limit=None, offset=None)
        assert "LIMIT" not in out.sql(dialect="postgres").upper()
        assert "OFFSET" not in out.sql(dialect="postgres").upper()

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_hook_returns_a_select_that_renders_both_bounds(
        self, dialect: str,
    ) -> None:
        strategy = get_dialect(dialect)
        out = strategy.apply_pagination(self._select(), limit=10, offset=5)
        assert isinstance(out, exp.Select), (
            f"[{dialect}] the hook must return a Select — T-SQL's TOP/FETCH "
            f"transposition only fires when the nodes sit on a Select, never "
            f"on a free-standing Limit."
        )
        rendered = out.sql(dialect=dialect)
        # Structural, not substring: ``"10" in rendered`` matches any digits
        # anywhere — a column name, the other bound's digits, or a stray
        # literal — so it would pass even if one bound were dropped.
        assert out.args.get("limit") is not None, (
            f"[{dialect}] no LIMIT bound on the Select: {rendered!r}"
        )
        assert out.args.get("offset") is not None, (
            f"[{dialect}] no OFFSET bound on the Select: {rendered!r}"
        )

    def test_tsql_hook_injects_ordering_for_a_bare_offset(self) -> None:
        """The rule this PR specifies, at the unit level."""
        strategy = get_dialect("tsql")
        out = strategy.apply_pagination(self._select(), limit=None, offset=5)
        rendered = out.sql(dialect="tsql")
        assert re.search(r"\bORDER\s+BY\b", rendered, re.IGNORECASE), (
            f"tsql OFFSET emitted without ORDER BY: {rendered!r}"
        )
        assert _BARE_LIMIT.search(rendered) is None, rendered

    def test_tsql_hook_preserves_a_user_order_by(self) -> None:
        """The injected ordering is a fallback — never an override."""
        strategy = get_dialect("tsql")
        select = self._select().order_by("a")
        rendered = strategy.apply_pagination(
            select, limit=None, offset=5,
        ).sql(dialect="tsql")
        assert "ORDER BY" in rendered.upper(), rendered
        assert "SELECT NULL" not in rendered.upper(), (
            f"the user's ORDER BY was replaced by the fallback: {rendered!r}"
        )


# =========================================================================== #
# DEV-1783 item 2 — the OTHER outer wrap (emit_outer_wrap), which had no guard.
# =========================================================================== #
class TestEmitOuterWrapInjectsOrderingForOffset:
    """``emit_outer_wrap`` (transform-chain / cross-model combined outer wrap)
    must apply the same OFFSET-needs-ORDER-BY guard ``apply_pagination`` does —
    SQL Server rejects OFFSET without ORDER BY. The guard must cover the AST
    path AND the base-impl fallback taken for a non-``Select`` inner."""

    @staticmethod
    def _offset(n: int) -> exp.Offset:
        return exp.Offset(expression=exp.Literal.number(n))

    @staticmethod
    def _assert_bare_offset_guarded(out: str) -> None:
        """OFFSET is emitted, ordered by the synthesized no-op (a ``SELECT NULL``
        subquery — sqlglot renders it via its NULLS-ordering ``CASE`` form, not a
        literal ``ORDER BY (SELECT NULL)``), never a real column."""
        assert re.search(r"\bORDER\s+BY\b", out, re.IGNORECASE), (
            f"tsql outer-wrap emitted OFFSET without ORDER BY:\n{out}"
        )
        assert "OFFSET" in out.upper(), out
        assert re.search(r"SELECT\s+NULL", out, re.IGNORECASE), (
            f"OFFSET ordering is not the synthesized no-op:\n{out}"
        )

    def test_ast_path_injects_ordering_for_a_bare_offset(self) -> None:
        out = get_dialect("tsql").emit_outer_wrap(
            inner_sql="SELECT 1 AS a", public=["a"],
            order=None, limit=None, offset_arg=self._offset(5),
        )
        self._assert_bare_offset_guarded(out)

    def test_base_fallback_injects_ordering_for_a_bare_offset(self) -> None:
        """A UNION inner parses to a non-``Select`` and takes the base-impl
        fallback, which also appends OFFSET with no ORDER BY. The guard runs
        before branching, so the fallback receives the synthesized ordering."""
        out = get_dialect("tsql").emit_outer_wrap(
            inner_sql="SELECT 1 AS a UNION SELECT 2 AS a", public=["a"],
            order=None, limit=None, offset_arg=self._offset(5),
        )
        self._assert_bare_offset_guarded(out)

    def test_user_order_is_never_replaced(self) -> None:
        user_order = exp.Order(expressions=[
            exp.Ordered(this=exp.column("a", quoted=True)),
        ])
        out = get_dialect("tsql").emit_outer_wrap(
            inner_sql="SELECT 1 AS a", public=["a"],
            order=user_order, limit=None, offset_arg=self._offset(5),
        )
        assert "SELECT NULL" not in out.upper(), (
            f"the user's ORDER BY was replaced by the fallback ordering:\n{out}"
        )
        assert "OFFSET" in out.upper(), out
        assert re.search(r"\bORDER\s+BY\b.*\[a\]", out, re.IGNORECASE | re.DOTALL), (
            f"the user's ORDER BY column was dropped:\n{out}"
        )


# =========================================================================== #
# Execution — pagination changes row sets, so parse-only is not enough (D5).
# =========================================================================== #
class TestPaginationExecution:
    """Seeded groups: ``paid`` sums to 30.0, the NULL-status group to 12.0 —
    distinct, so ordering and slicing are unambiguous."""

    async def test_plain_shape_limit_and_offset(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        top = await exec_engine.execute(
            _plain_query(limit=1, offset=None, ordered=True),
        )
        assert [r["orders.status"] for r in top.data] == ["paid"], top.data
        assert top.data[0]["orders.revenue"] == pytest.approx(30.0), top.data

        second = await exec_engine.execute(
            _plain_query(limit=1, offset=1, ordered=True),
        )
        assert [r["orders.status"] for r in second.data] == [None], second.data
        assert second.data[0]["orders.revenue"] == pytest.approx(12.0), second.data

    async def test_outer_trim_shape_limit_and_offset(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """Pagination on the trim wrapper must slice the same ordering the
        hidden aggregate defines, and must not resurrect the hidden column."""
        top = await exec_engine.execute(
            _outer_trim_query(limit=1, offset=None, ordered=True),
        )
        assert [r["orders.status"] for r in top.data] == ["paid"], top.data
        assert all("amount_sum" not in k for k in top.data[0]), (
            f"the hidden order slot leaked into the response: {top.data[0]}"
        )

        second = await exec_engine.execute(
            _outer_trim_query(limit=1, offset=1, ordered=True),
        )
        assert [r["orders.status"] for r in second.data] == [None], second.data

    async def test_combined_shape_limit_and_offset(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """NEW (B3), EXECUTED: the cross-model combined shape paginates."""
        top = await exec_engine.execute(
            _combined_query(limit=1, offset=None, ordered=True),
        )
        assert len(top.data) == 1, f"LIMIT 1 returned {len(top.data)} rows:\n{top.data}"
        assert top.data[0]["orders.customers.tier"] == "gold", top.data
        assert top.data[0]["orders.spend"] == pytest.approx(1000.0), top.data

        second = await exec_engine.execute(
            _combined_query(limit=1, offset=1, ordered=True),
        )
        assert len(second.data) == 1, second.data
        assert second.data[0]["orders.customers.tier"] is None, second.data
        assert second.data[0]["orders.spend"] == pytest.approx(325.0), second.data

    async def test_offset_past_the_end_returns_no_rows(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        resp = await exec_engine.execute(
            _combined_query(limit=10, offset=50, ordered=True),
        )
        assert resp.data == [], resp.data

    async def test_limit_without_offset_on_the_combined_shape(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        resp = await exec_engine.execute(
            _combined_query(limit=2, offset=None, ordered=True),
        )
        assert len(resp.data) == 2, resp.data
