"""DEV-1746 §5.7 — the null-safe grain doctrine (B1 + B2).

Two ratified behaviour changes, one shared mechanism.

**B1 — the ``_wm_`` INNER grain goes null-safe.** A windowed measure's ``_wm_``
CTE joins its ``_src`` row subquery back to ``_base`` on the query grain. That
inner comparison is a plain ``=``, so a group whose dimension is NULL never
matches and silently receives NULL instead of its real windowed value. The outer
``_wm_`` join-back and the ``_cm_`` join-back are already null-safe, so today
SLayer disagrees with itself about NULL-grain semantics depending on which
isolation shape a measure lands in. Both ``_wm_`` comparison sites are asserted
here, as §5.7 requires.

**B2 — grain join-backs are built as AST, not by string re-parse.** The join-back
predicate is currently assembled by rendering both sides to pre-quoted strings
and re-parsing them (``_null_safe_join_pair_sql``). Public aliases are dotted
(``orders.customers.status``), and on a dialect that mangles dots at emission
(BigQuery ``___``, T-SQL brackets) the round-trip re-reads the dotted alias as a
multi-part *reference*, yielding a qualifier for a table that does not exist:

    ON `_base___orders___customers`.`status` IS NOT DISTINCT FROM ...
       ^^^^^^^^^^^^^^^^^^^^^^^^^^ not a table in scope

sjoin already avoids this by building the columns directly as AST; that
mechanism becomes the one shared builder. The corruption is latent rather than
theoretical: three entries in ``tests/golden/dev1745_sql_baseline.json`` record
``ScopeLeakError`` for exactly this shape, and this PR regenerates them.

Execution coverage is SQLite in-suite (the DuckDB counterpart lives in
``tests/integration/test_integration_dev1746.py``). What execution *cannot*
cover — the dotted-alias mangling itself — is asserted as emission per §5.13,
because neither SQLite nor DuckDB mangles dots.

"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.sql.render.joins import (
    build_grain_joinback_condition,
    grain_alias_column,
)
from slayer.sql.scope_check import assert_scope_closed

from tests._cross_model_chain import _gen
from tests._dev1746_fixtures import (
    NULL_STATUS_FEB_WINDOW,
    NULL_STATUS_JAN,
    PAID_FEB_WINDOW,
    PAID_JAN,
    joinback_on_predicate_for,
    make_sqlite_engine,
    seed_dev1746_sqlite,
    src_subquery_on_predicate,
)
from tests._engine_helpers import _norm

# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
async def exec_engine() -> AsyncIterator[SlayerQueryEngine]:
    """Engine over the seeded NULL-bearing SQLite corpus."""
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "dev1746.db")
        seed_dev1746_sqlite(db_path)
        yield await make_sqlite_engine(os.path.join(d, "store"), db_path)


def _windowed_query() -> SlayerQuery:
    """Windowed measure grouped by a NULLABLE dimension + a month bucket."""
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="amount:sum(window='90d')", name="rev_w")],
    )


def _windowed_chain_query() -> SlayerQuery:
    """The same shape against the shared orders_x chain (postgres-shaped)."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="amount:sum(window='90d')", name="rev_w")],
    )


def _cm_shared_grain_query() -> SlayerQuery:
    """Cross-model measure grouped by a grain shared with the target."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="customers_v2.status")],
        measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
    )


# =========================================================================== #
# B1 — the ``_wm_`` inner grain goes null-safe.
# =========================================================================== #
class TestB1WindowedInnerGrainNullSafe:
    """The inner ``_src`` join is the site that decides whether a NULL-dimension
    group receives a real windowed value."""

    @pytest.mark.parametrize(
        "dialect,expected_op",
        [
            ("postgres", "IS NOT DISTINCT FROM"),
            ("duckdb", "IS NOT DISTINCT FROM"),
            ("sqlite", " IS "),
        ],
    )
    async def test_inner_grain_equality_is_null_safe(
        self, dialect: str, expected_op: str,
    ) -> None:
        """NEW (B1): the inner grain comparison uses the dialect's null-safe
        equality, not a plain ``=``."""
        sql = await _gen(_windowed_chain_query(), dialect=dialect)
        on = _norm(src_subquery_on_predicate(sql, dialect=dialect))
        assert expected_op in on, (
            f"[{dialect}] the _wm_ inner grain join is not null-safe.\n"
            f"ON predicate: {on}\n\nfull SQL:\n{sql}"
        )
        # The grain member specifically — the time-range bounds legitimately
        # stay plain >= / <, so assert on the dimension comparison only.
        assert "_src._w_dim_0 =" not in on, (
            f"[{dialect}] the grain member still compares with a plain `=`, "
            f"so a NULL-dimension group cannot match.\nON predicate: {on}"
        )

    async def test_inner_grain_time_bounds_stay_plain_inequalities(self) -> None:
        """The window's time-range bounds are NOT grain equality and must keep
        their plain ``>=`` / ``<`` (a null-safe rewrite there would be wrong)."""
        sql = await _gen(_windowed_chain_query(), dialect="postgres")
        on = _norm(src_subquery_on_predicate(sql, dialect="postgres"))
        assert "_src._w_time >=" in on, f"lower bound changed shape:\n{on}"
        assert "_src._w_time <" in on, f"upper bound changed shape:\n{on}"

    async def test_outer_joinback_remains_null_safe(self) -> None:
        """§5.7 requires BOTH ``_wm_`` comparison sites be asserted. The outer
        join-back is already null-safe; this pins it so B1's inner-site change
        cannot regress it."""
        sql = await _gen(_windowed_chain_query(), dialect="postgres")
        on = _norm(joinback_on_predicate_for(sql, prefix="_wm_", dialect="postgres"))
        assert "IS NOT DISTINCT FROM" in on, (
            f"the _wm_ outer join-back lost its null-safe equality:\n{on}"
        )

    async def test_null_dimension_group_gets_its_real_windowed_value(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """NEW (B1), EXECUTED: the NULL-status group receives its real 90-day
        windowed sum instead of NULL.

        February's window reaches back 90 days and so covers January too:
        ``NULL`` → 5.0 + 7.0 = 12.0, ``paid`` → 10.0 + 20.0 = 30.0. The two
        differ from their single-month sums, so this cannot pass by accident.
        """
        resp = await exec_engine.execute(_windowed_query())
        by_group = {
            (r["orders.status"], str(r["orders.created_at"])[:7]): r["orders.rev_w"]
            for r in resp.data
        }
        feb_null = by_group.get((None, "2024-02"))
        assert feb_null is not None, (
            "the NULL-status group still receives NULL — the inner grain join "
            f"did not match on a NULL dimension.\nrows: {resp.data}"
        )
        assert feb_null == pytest.approx(NULL_STATUS_FEB_WINDOW), (
            f"NULL-status February window: expected {NULL_STATUS_FEB_WINDOW}, "
            f"got {feb_null}.\nrows: {resp.data}"
        )
        assert by_group.get((None, "2024-01")) == pytest.approx(NULL_STATUS_JAN), (
            f"NULL-status January window: expected {NULL_STATUS_JAN}.\n"
            f"rows: {resp.data}"
        )
        # Control: the non-NULL group was already correct and must stay correct.
        assert by_group.get(("paid", "2024-02")) == pytest.approx(PAID_FEB_WINDOW), (
            f"paid February window regressed: expected {PAID_FEB_WINDOW}.\n"
            f"rows: {resp.data}"
        )
        assert by_group.get(("paid", "2024-01")) == pytest.approx(PAID_JAN), (
            f"paid January window regressed: expected {PAID_JAN}.\nrows: {resp.data}"
        )

    async def test_null_group_count_unchanged_by_the_fix(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """B1 changes VALUES, never cardinality — the core invariant."""
        resp = await exec_engine.execute(_windowed_query())
        assert len(resp.data) == 4, (
            "expected one row per (status, month) group — 2 statuses x 2 months; "
            f"got {len(resp.data)}:\n{resp.data}"
        )


# =========================================================================== #
# B2 — join-backs built directly as AST (no string re-parse).
# =========================================================================== #
class TestB2JoinBackBuiltAsAst:
    """The dotted-alias corruption is invisible on dialects that do not mangle
    dots, so these assert on BigQuery and T-SQL specifically (§5.13)."""

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    async def test_cm_joinback_references_only_bound_tables(
        self, dialect: str,
    ) -> None:
        """NEW (B2): the join-back's qualifiers are the CTE aliases actually in
        scope (``_base`` and the ``_cm_*`` CTE) — not a mangled composite."""
        sql = await _gen(_cm_shared_grain_query(), dialect=dialect)
        on = joinback_on_predicate_for(sql, prefix="_cm_", dialect=dialect)
        qualifiers = {
            col.table for col in
            sqlglot.parse_one(on, dialect=dialect).find_all(exp.Column)
            if col.table
        }
        bound = {"_base"} | {
            n for n in qualifiers if n.startswith("_cm_") and "___" not in n
        }
        unbound = qualifiers - bound
        assert not unbound, (
            f"[{dialect}] the join-back references table(s) that do not exist: "
            f"{sorted(unbound)}. The dotted public alias was re-parsed as a "
            f"multi-part reference.\nON: {on}\n\nfull SQL:\n{sql}"
        )

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    async def test_cm_joinback_shape_is_scope_closed(self, dialect: str) -> None:
        """The scope validator is the belt that caught this in the golden
        baseline; it must now pass for these shapes."""
        sql = await _gen(_cm_shared_grain_query(), dialect=dialect)
        assert_scope_closed(sql, dialect=dialect)

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    async def test_wm_joinback_references_only_bound_tables(
        self, dialect: str,
    ) -> None:
        """B2 covers the ``_wm_`` join-back too — it shares the same builder."""
        sql = await _gen(_windowed_chain_query(), dialect=dialect)
        on = joinback_on_predicate_for(sql, prefix="_wm_", dialect=dialect)
        qualifiers = {
            col.table for col in
            sqlglot.parse_one(on, dialect=dialect).find_all(exp.Column)
            if col.table
        }
        unbound = {
            q for q in qualifiers
            if not (q == "_base" or (q.startswith("_wm_") and "___" not in q))
        }
        assert not unbound, (
            f"[{dialect}] the _wm_ join-back references non-existent table(s): "
            f"{sorted(unbound)}.\nON: {on}\n\nfull SQL:\n{sql}"
        )

    async def test_join_backs_no_longer_route_through_the_string_round_trip(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """NEW (B2), production-path proof: ``_null_safe_join_pair_sql`` is
        retained (P-J state 1) but must no longer be REACHED by either
        join-back. Poisoning it proves the call sites migrated — a grep cannot,
        because the function stays in the file.
        """
        from slayer.sql.generator import SQLGenerator

        def _poisoned(*args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError(
                "_null_safe_join_pair_sql was called — a grain join-back is "
                "still using the string re-parse round-trip (B2 incomplete)."
            )

        monkeypatch.setattr(
            SQLGenerator, "_null_safe_join_pair_sql", _poisoned, raising=True,
        )
        cm_sql = await _gen(_cm_shared_grain_query(), dialect="postgres")
        assert "LEFT JOIN _cm_" in cm_sql, cm_sql
        wm_sql = await _gen(_windowed_chain_query(), dialect="postgres")
        assert "LEFT JOIN _wm_" in wm_sql, wm_sql


# =========================================================================== #
# §5.7 explicit semantics — zero-column, composite, and type-coerced grains.
# =========================================================================== #
class TestGrainSemantics:

    async def test_zero_column_grain_emits_cross_join_and_no_on_clause(
        self,
    ) -> None:
        """A scalar CMA has an EMPTY grain: no join predicate exists to be
        null-safe, and the shape stays a CROSS JOIN. Unchanged by B1/B2 — pinned
        because the shared builder returns ``None`` for empty pairs and the
        caller must keep turning that into a CROSS JOIN."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query, dialect="postgres")
        assert "CROSS JOIN _cm_" in _norm(sql), (
            f"an empty-grain cross-model aggregate must CROSS JOIN:\n{sql}"
        )
        with pytest.raises(AssertionError):
            joinback_on_predicate_for(sql, prefix="_cm_", dialect="postgres")

    async def test_zero_column_grain_executes_to_the_scalar_value(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """The CROSS JOIN must not multiply rows: one row, the scalar total."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.spend:sum", name="total")],
        )
        resp = await exec_engine.execute(query)
        assert len(resp.data) == 1, f"scalar CMA must yield ONE row:\n{resp.data}"
        # 1000.0 + 250.0 + 75.0 over the three seeded customers.
        assert resp.data[0]["orders.total"] == pytest.approx(1325.0), resp.data

    async def test_composite_grain_conjoins_one_null_safe_pair_per_member(
        self,
    ) -> None:
        """A two-member grain yields two null-safe comparisons ANDed together —
        one per member, none of them a plain ``=``.

        Both members must be reachable from the TARGET: a host-local dimension
        is deliberately excluded from the shared grain (it cannot be re-derived
        inside the target-rooted CTE), so pairing one with a target dimension
        would yield a single-member grain and prove nothing.
        """
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[
                ColumnRef(name="customers_v2.status"),
                ColumnRef(name="customers_v2.ltv_x2"),
            ],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query, dialect="postgres")
        on = _norm(joinback_on_predicate_for(sql, prefix="_cm_", dialect="postgres"))
        assert on.count("IS NOT DISTINCT FROM") >= 2, (
            "a composite grain must emit one null-safe comparison per shared "
            f"member:\n{on}"
        )

    async def test_time_truncated_grain_member_is_null_safe(self) -> None:
        """A type-coerced grain member (a DATE_TRUNC bucket) joins back
        null-safely like any other."""
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.signup_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query, dialect="postgres")
        on = _norm(joinback_on_predicate_for(sql, prefix="_cm_", dialect="postgres"))
        assert "IS NOT DISTINCT FROM" in on, (
            f"a time-truncated grain member lost its null-safe join-back:\n{on}"
        )

    async def test_null_grain_cross_model_value_is_not_lost(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """EXECUTED regression: the ``_cm_`` NULL-grain group keeps its real
        aggregate (this join-back is already null-safe; B2 rebuilds how the
        predicate is constructed and must not change what it means)."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.tier")],
            measures=[ModelMeasure(formula="customers.spend:sum", name="spend")],
        )
        resp = await exec_engine.execute(query)
        by_tier = {r["orders.customers.tier"]: r["orders.spend"] for r in resp.data}
        assert None in by_tier, (
            f"the NULL-tier group vanished from the result:\n{resp.data}"
        )
        # customers 101 (250.0) and 102 (75.0) both have a NULL tier.
        assert by_tier[None] == pytest.approx(325.0), (
            f"NULL-tier group lost its aggregate: {by_tier}\nrows: {resp.data}"
        )


# =========================================================================== #
# The shared builder itself (Codex D2) — expression operands, quoting, dots.
# =========================================================================== #
class TestSharedGrainJoinBackBuilder:
    """Direct unit coverage of the one mechanism ``_cm_``/``_wm_``/sjoin share.

    The builder takes EXPRESSION operands (not alias strings) so a caller can
    hand it an already-resolved, already-cast reference; the alias-column helper
    covers today's three callers, which all compare projected aliases.
    """

    def test_builder_returns_none_for_an_empty_grain(self) -> None:
        """Zero-column grain → no predicate at all; the caller emits CROSS JOIN.
        Returning a truthy ``TRUE`` instead would silently turn every scalar CMA
        into an inner-join-shaped ON clause."""
        assert build_grain_joinback_condition(
            pairs=[], dialect=get_dialect("postgres"),
        ) is None

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "IS NOT DISTINCT FROM"),
            ("duckdb", "IS NOT DISTINCT FROM"),
            ("sqlite", " IS "),
            ("mysql", "<=>"),
            ("tsql", " OR "),
            ("snowflake", "IS NOT DISTINCT FROM"),
        ],
    )
    def test_builder_emits_the_dialect_null_safe_form(
        self, dialect: str, expected: str,
    ) -> None:
        """One builder, every dialect's own null-safe spelling — including the
        expanded ``a = b OR (a IS NULL AND b IS NULL)`` fallback on T-SQL."""
        strategy = get_dialect(dialect)
        left = grain_alias_column(alias="orders.status", table="_base")
        right = grain_alias_column(alias="orders.status", table="_cm_x")
        cond = build_grain_joinback_condition(
            pairs=[(left, right)], dialect=strategy,
        )
        assert cond is not None
        rendered = cond.sql(dialect=strategy.sqlglot_name)
        assert expected in rendered, (
            f"[{dialect}] expected {expected!r} in {rendered!r}"
        )

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql", "postgres", "mysql"])
    def test_dotted_alias_stays_one_identifier(self, dialect: str) -> None:
        """The B2 defect in miniature: a dotted PUBLIC ALIAS is one identifier,
        never a ``table.column`` reference. Built as AST it cannot decompose."""
        strategy = get_dialect(dialect)
        left = grain_alias_column(alias="orders.customers.status", table="_base")
        cond = build_grain_joinback_condition(
            pairs=[(left, grain_alias_column(
                alias="orders.customers.status", table="_cm_x"))],
            dialect=strategy,
        )
        assert cond is not None
        for col in cond.find_all(exp.Column):
            assert col.table in ("_base", "_cm_x"), (
                f"[{dialect}] qualifier {col.table!r} is not one of the two "
                f"CTE aliases — the dotted alias decomposed into a reference."
            )
            assert col.name == "orders.customers.status", (
                f"[{dialect}] the dotted alias was split: {col.name!r}"
            )

    def test_alias_containing_a_quote_is_not_injectable(self) -> None:
        """An embedded quote must survive as data inside one identifier."""
        weird = 'orders."evil'
        col = grain_alias_column(alias=weird, table="_base")
        assert col.name == weird, col.name
        rendered = col.sql(dialect="postgres")
        assert rendered.startswith('_base.'), rendered
        # Re-parsing must give back exactly one column with the same name.
        reparsed = sqlglot.parse_one(f"SELECT {rendered}", dialect="postgres")
        cols = list(reparsed.find_all(exp.Column))
        assert len(cols) == 1 and cols[0].name == weird, (
            f"identifier did not survive a round trip: {rendered!r} -> "
            f"{[c.name for c in cols]}"
        )

    def test_case_sensitive_alias_is_quoted(self) -> None:
        """Mixed-case aliases must stay quoted, or a case-folding dialect
        resolves them to a different column."""
        col = grain_alias_column(alias="Orders.Status", table="_base")
        rendered = col.sql(dialect="postgres")
        assert '"Orders.Status"' in rendered, rendered

    def test_composite_grain_ands_every_pair(self) -> None:
        strategy = get_dialect("postgres")
        pairs = [
            (grain_alias_column(alias="a", table="_base"),
             grain_alias_column(alias="a", table="_cm_x")),
            (grain_alias_column(alias="b", table="_base"),
             grain_alias_column(alias="b", table="_cm_x")),
        ]
        cond = build_grain_joinback_condition(pairs=pairs, dialect=strategy)
        assert cond is not None
        rendered = cond.sql(dialect="postgres")
        assert rendered.count("IS NOT DISTINCT FROM") == 2, rendered
        assert " AND " in rendered, rendered

    def test_builder_accepts_arbitrary_expression_operands(self) -> None:
        """Codex D2: the core API takes expressions, so a caller can compare a
        CAST or any resolved reference — not only a projected alias."""
        strategy = get_dialect("postgres")
        left = exp.cast(exp.column("x", table="_base"), "DATE")
        right = exp.column("y", table="_cm_x")
        cond = build_grain_joinback_condition(
            pairs=[(left, right)], dialect=strategy,
        )
        assert cond is not None
        rendered = cond.sql(dialect="postgres")
        assert "CAST(" in rendered and "IS NOT DISTINCT FROM" in rendered, rendered


# =========================================================================== #
# Dialect-emission coverage (§5.13) for what execution cannot reach.
# =========================================================================== #
class TestDialectEmission:

    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery", "tsql", "mysql"])
    async def test_joinback_sql_parses_under_its_own_dialect(
        self, dialect: str,
    ) -> None:
        sql = await _gen(_cm_shared_grain_query(), dialect=dialect)
        parsed = sqlglot.parse(sql, dialect=dialect)
        assert len(parsed) == 1, f"[{dialect}] did not parse to one statement:\n{sql}"

    async def test_snowflake_uses_a_native_null_safe_equality(self) -> None:
        """§5.13 names Snowflake null-safe equality specifically."""
        sql = await _gen(_cm_shared_grain_query(), dialect="snowflake")
        on = _norm(joinback_on_predicate_for(sql, prefix="_cm_", dialect="snowflake"))
        assert "IS NOT DISTINCT FROM" in on or "EQUAL_NULL" in on, (
            f"snowflake join-back is not null-safe:\n{on}"
        )

    async def test_tsql_uses_the_expanded_fallback(self) -> None:
        """T-SQL has no native null-safe operator: the expanded
        ``a = b OR (a IS NULL AND b IS NULL)`` must appear."""
        sql = await _gen(_cm_shared_grain_query(), dialect="tsql")
        on = _norm(joinback_on_predicate_for(sql, prefix="_cm_", dialect="tsql"))
        assert " OR " in on and "IS NULL" in on, (
            f"tsql join-back is missing the expanded null-safe form:\n{on}"
        )

    async def test_mysql_null_safe_operator_is_emitted(self) -> None:
        """MySQL's ``<=>`` comes from sqlglot's transposition of ``NullSafeEQ``
        rather than a dialect override — pinned so a future sqlglot change or a
        well-meaning 'fix' to the base docstring cannot silently drop it."""
        sql = await _gen(_cm_shared_grain_query(), dialect="mysql")
        on = _norm(joinback_on_predicate_for(sql, prefix="_cm_", dialect="mysql"))
        assert "<=>" in on, f"mysql join-back lost its null-safe operator:\n{on}"
