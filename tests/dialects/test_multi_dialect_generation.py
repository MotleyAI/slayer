"""Multi-dialect SQL generation end-to-end tests.

DEV-1542 cleanup: lifted from ``tests/test_sql_generator.py``. Each test
class drives ``SQLGenerator`` across multiple dialects and asserts on the
final emitted SQL — orthogonal to the per-dialect strategy-class tests in
``tests/dialects/test_<dialect>.py`` which pin emission from the
``SqlDialect`` strategy methods only.

``TestMultiDialectGeneration`` is the canonical Tier-2 matrix: when adding
a new dialect, append it to ``ALL_DIALECTS`` here.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.sql.generator import SQLGenerator

from tests.dialects.conftest import _generate, _norm


# DEV-1337: per-alias allowlists for "natively supports single-arg log10/log2"
# rendering. Outside these sets the current 2-arg LOG(base, x) form is kept.
# Explicit hand-written sets — these are the spec; deriving from the dialect
# flags would make the log-alias tests self-fulfilling.
_LOG10_NATIVE_DIALECTS = frozenset({
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse",
    "snowflake", "bigquery", "redshift",
    "trino", "presto", "databricks", "spark", "tsql",
})
_LOG2_NATIVE_DIALECTS = frozenset({
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse",
    "bigquery", "trino", "presto", "databricks", "spark",
})


class TestMultiDialectGeneration:
    """Test SQL generation across all supported dialects."""

    @pytest.fixture
    def orders_model(self) -> SlayerModel:
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),

                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                # Second numeric column so 2-arg stat aggregates
                # (corr(other=...) / covar_*(other=...)) have a valid LHS+RHS pair.
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
            ],
        )
        return model

    ALL_DIALECTS = [
        "postgres",
        "mysql",
        "sqlite",
        "clickhouse",
        "bigquery",
        "snowflake",
        "duckdb",
        "redshift",
        "trino",
        "presto",
        "databricks",
        "spark",
        "tsql",
        "oracle",
    ]

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    async def test_basic_query(self, dialect: str, orders_model: SlayerModel) -> None:
        """Basic aggregation query should generate valid SQL for every dialect."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(gen, query, orders_model)
        assert "COUNT(" in sql
        assert "SUM(" in sql

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    async def test_date_trunc(self, dialect: str, orders_model: SlayerModel) -> None:
        """DATE_TRUNC should produce valid output for every dialect."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
        )
        sql = await _generate(gen, query, orders_model)
        assert "COUNT(" in sql
        # Each dialect uses its own truncation function
        sql_upper = sql.upper()
        assert any(fn in sql_upper for fn in ["DATE_TRUNC", "STRFTIME", "TRUNC", "STR_TO_DATE", "DATETRUNC"])

    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "bigquery", "duckdb", "snowflake", "tsql"])
    async def test_date_trunc_casts_unknown_typed_time_dim(self, dialect: str) -> None:
        """A time-dimension whose ``sql`` is a bare literal (or any expression
        whose live type is ``unknown``) must be wrapped in ``CAST(... AS
        TIMESTAMP)`` before being passed to ``DATE_TRUNC``. Postgres has
        multiple overloads keyed on the second argument's type and rejects
        ``DATE_TRUNC('month', '2025-12-01')`` with ``AmbiguousFunctionError``.

        Bare column references stay unwrapped — their live DB type is known,
        and forcing a cast could strip ``TIMESTAMPTZ`` to ``TIMESTAMP``.
        """
        gen = SQLGenerator(dialect=dialect)
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="ts", sql="'2025-12-01'", type=DataType.TIMESTAMP),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
        )
        # Bare-literal time dim — must be cast.
        sql = await _generate(
            gen,
            SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="*:count")],
                time_dimensions=[TimeDimension(dimension=ColumnRef(name="ts"), granularity=TimeGranularity.MONTH)],
            ),
            model,
        )
        # sqlglot transpiles ``TIMESTAMP`` → ``DATETIME`` on MySQL / BigQuery,
        # so we don't assert the literal target-type spelling — only that the
        # literal is wrapped in a CAST.
        assert "CAST('2025-12-01' AS" in sql, sql
        # Bare-column time dim — must NOT be cast.
        sql = await _generate(
            gen,
            SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="*:count")],
                time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            ),
            model,
        )
        assert "CAST(" not in sql.upper(), sql

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    async def test_calendar_time_shift(self, dialect: str, orders_model: SlayerModel) -> None:
        """Calendar-based time_shift should produce dialect-appropriate date arithmetic in shifted CTE."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, -1, 'year')", name="rev_prev_year")],
        )
        sql = await _generate(gen, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        # Join should be simple equality (timestamp shift is inside the shifted CTE)
        # Dialect-specific date arithmetic should appear in the shifted CTE's SELECT/GROUP BY
        sql_upper = sql.upper()
        if dialect == "sqlite":
            assert "DATE(" in sql_upper
        elif dialect == "tsql":
            assert "DATEADD" in sql_upper
        else:
            assert "INTERVAL" in sql_upper

    @pytest.mark.parametrize("dialect", ["mysql", "clickhouse"])
    async def test_window_measure_multi_unit_interval_dialect_correct(
        self, dialect: str, orders_model: SlayerModel,
    ) -> None:
        """Multi-unit windows (e.g. '1y2m3d') must render as separate per-unit
        INTERVAL clauses on MySQL and ClickHouse — never as a single
        Postgres-shaped quoted multi-unit literal which neither dialect parses.

        Codex flagged this as a real correctness bug during PR #64 review:
        `_duration_interval_sql` had only two branches (SQLite + "Postgres-style"),
        and the latter emitted `INTERVAL '1 year 2 month 3 day'` for every
        non-SQLite dialect.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"),
                              granularity=TimeGranularity.DAY),
            ],
            measures=[ModelMeasure(formula="revenue:sum(window='1y2m3d')",
                                   name="rev_w")],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        norm = _norm(sql).upper()
        # The broken Postgres-shape multi-unit literal must NOT appear.
        assert "INTERVAL '1 YEAR 2 MONTH 3 DAY'" not in norm, (
            f"Multi-unit Postgres-shape INTERVAL literal is invalid on {dialect}.\n"
            f"sql:\n{sql}"
        )
        # Per-unit INTERVAL clauses must each be present (sqlglot transpiles
        # exp.Interval per dialect; MySQL + ClickHouse both render as
        # `INTERVAL N UNIT` for these AST nodes).
        for piece in ("INTERVAL 1 YEAR", "INTERVAL 2 MONTH", "INTERVAL 3 DAY"):
            assert piece in norm, (
                f"Expected dialect-correct '{piece}' in {dialect} output.\n"
                f"sql:\n{sql}"
            )

    @pytest.mark.parametrize("dialect", ["mysql", "clickhouse"])
    async def test_window_measure_single_unit_interval_dialect_correct(
        self, dialect: str, orders_model: SlayerModel,
    ) -> None:
        """Even single-unit windows must render unquoted on MySQL/ClickHouse.

        The pre-refactor code emits `INTERVAL '7 day'` for single-unit windows
        on every non-SQLite dialect, which is invalid MySQL syntax (MySQL wants
        `INTERVAL 7 DAY`). After the AST refactor, sqlglot's per-dialect
        transpiler emits the canonical form for each dialect.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"),
                              granularity=TimeGranularity.DAY),
            ],
            measures=[ModelMeasure(formula="revenue:sum(window='7d')",
                                   name="rev_w")],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        norm = _norm(sql).upper()
        assert "INTERVAL '7 DAY'" not in norm, (
            f"Quoted single-unit INTERVAL literal is invalid on {dialect}.\n"
            f"sql:\n{sql}"
        )
        assert "INTERVAL 7 DAY" in norm, (
            f"Expected dialect-correct 'INTERVAL 7 DAY' in {dialect} output.\n"
            f"sql:\n{sql}"
        )

    async def test_window_measure_tsql_uses_dateadd(
        self, orders_model: SlayerModel,
    ) -> None:
        """Window boundary on T-SQL must use DATEADD instead of INTERVAL (invalid T-SQL)."""
        gen = SQLGenerator(dialect="tsql")
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"),
                              granularity=TimeGranularity.DAY),
            ],
            measures=[ModelMeasure(formula="revenue:sum(window='7d')", name="rev_w")],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        norm = _norm(sql).upper()
        assert "DATEADD" in norm, f"Expected DATEADD in T-SQL window output:\n{sql}"
        assert "INTERVAL" not in norm, f"INTERVAL is invalid T-SQL syntax:\n{sql}"

    async def test_window_measure_tsql_multi_unit_uses_chained_dateadd(
        self, orders_model: SlayerModel,
    ) -> None:
        """Multi-unit window '1y2m3d' on T-SQL must use chained DATEADD calls, not INTERVAL."""
        gen = SQLGenerator(dialect="tsql")
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"),
                              granularity=TimeGranularity.DAY),
            ],
            measures=[ModelMeasure(formula="revenue:sum(window='1y2m3d')", name="rev_w")],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        norm = _norm(sql).upper()
        # Must use DATEADD, not INTERVAL literals
        assert "INTERVAL" not in norm, f"INTERVAL is invalid T-SQL syntax:\n{sql}"
        assert "DATEADD" in norm, f"Expected DATEADD in multi-unit T-SQL window:\n{sql}"

    # DEV-1317: cross-dialect stat-agg generation. The exact SQL shape per
    # Tier-1 dialect is pinned in TestStatAggsPerDialect; here we just confirm
    # the generator produces parseable SQL on every supported dialect.
    # Assertions check the function-call SHAPE (qualified column refs in the
    # arg slot), not just substring fragments — substrings like "STDDEV" or
    # "CORR" pass even when the aggregate has regressed because aliases
    # such as `revenue_stddev_samp` always contain the family name.

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    @pytest.mark.parametrize(
        "formula",
        [
            "revenue:stddev_samp",
            "revenue:stddev_pop",
            "revenue:var_samp",
            "revenue:var_pop",
        ],
    )
    async def test_one_arg_stat_agg_generation(
        self,
        dialect: str,
        formula: str,
        orders_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula=formula)],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        upper = sql.upper()
        assert "SELECT" in upper
        # The aggregate must wrap the resolved value column (orders.amount,
        # since the `revenue` Column has sql="amount") in its single-arg slot.
        assert "(ORDERS.AMOUNT)" in upper, (
            f"expected single-arg call (ORDERS.AMOUNT) in SQL for {formula!r} on {dialect}:\n{sql}"
        )

    # corr / covar_samp / covar_pop are implemented via variance-decomposition
    # formula on MySQL and T-SQL (neither has native two-arg functions), so those
    # dialects are filtered out of the direct two-arg call assertion matrix.
    @pytest.mark.parametrize(
        "dialect", [d for d in ALL_DIALECTS if d not in ("mysql", "tsql")],
    )
    @pytest.mark.parametrize(
        "formula",
        [
            "revenue:corr(other=quantity)",
            "revenue:covar_samp(other=quantity)",
            "revenue:covar_pop(other=quantity)",
        ],
    )
    async def test_two_arg_stat_agg_generation(
        self,
        dialect: str,
        formula: str,
        orders_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula=formula)],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        upper = sql.upper()
        assert "SELECT" in upper
        # Both legs (LHS column AND `other=` kwarg) must be qualified and
        # appear in the function-call's two-arg slot in that order. This
        # asymmetry vs the 1-arg test is what distinguishes the test bodies
        # for Sonar python:S4144 and pins the new `_resolve_agg_param` +
        # `_resolve_value_sql` qualification path for 2-arg stats.
        assert "(ORDERS.AMOUNT, ORDERS.QUANTITY)" in upper, (
            f"expected two-arg call (ORDERS.AMOUNT, ORDERS.QUANTITY) in SQL for {formula!r} "
            f"on {dialect}:\n{sql}"
        )

    @pytest.mark.parametrize("dialect", ["mysql", "tsql"])
    @pytest.mark.parametrize(
        "formula",
        [
            "revenue:corr(other=quantity)",
            "revenue:covar_samp(other=quantity)",
            "revenue:covar_pop(other=quantity)",
        ],
    )
    async def test_two_arg_stat_formula_dialects_generate_valid_sql(
        self,
        dialect: str,
        formula: str,
        orders_model: SlayerModel,
    ) -> None:
        """MySQL and T-SQL emit variance-decomposition formula instead of direct two-arg call."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula=formula)],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        assert "SELECT" in sql.upper()
        # The formula uses division (variance decomposition)
        assert "/" in sql

    @pytest.mark.parametrize(
        "formula", [
            "revenue:corr(other=quantity)",
            "revenue:covar_samp(other=quantity)",
            "revenue:covar_pop(other=quantity)",
        ],
    )
    async def test_two_arg_stat_agg_mysql_emits_formula_valid_sql(
        self, formula: str, orders_model: SlayerModel,
    ) -> None:
        """MySQL uses variance-decomposition formula for corr/covar_samp/covar_pop."""
        gen = SQLGenerator(dialect="mysql")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula=formula)],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        assert "SELECT" in sql.upper()
        # Formula uses division (variance decomposition)
        assert "/" in sql


class TestLogAliasPreservation:
    """DEV-1337 — user-written ``log10(x)`` / ``log2(x)`` must round-trip
    verbatim in emitted SQL on dialects that natively support those single-arg
    aliases. sqlglot's default behaviour normalises both into a generic
    ``Log(this=Literal(base), expression=arg)`` AST node and re-emits as
    ``LOG(base, x)`` for almost every dialect, which makes generated SQL
    diverge from the recipe formula text and (on dialects that lack 2-arg
    ``LOG``) can break a previously working call.
    """

    @pytest.fixture
    def log_model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                # Scalar log expressions inside Column.sql — the primary
                # path the issue surfaced through.
                Column(name="log_amount", sql="log10(amount)", type=DataType.DOUBLE),
                Column(name="log2_amount", sql="log2(amount)", type=DataType.DOUBLE),
                # Negative-control: a non-alias literal base. Must keep the
                # standard 2-arg LOG(base, x) form post-fix.
                Column(name="log3_amount", sql="log(3, amount)", type=DataType.DOUBLE),
                # ln(...) is a separate AST node (exp.Ln); the rewrite must
                # not touch it.
                Column(name="ln_amount", sql="ln(amount)", type=DataType.DOUBLE),
            ],
        )

    @pytest.mark.parametrize("dialect", TestMultiDialectGeneration.ALL_DIALECTS)
    async def test_log10_in_column_sql_is_preserved(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        upper_no_ws = "".join(sql.upper().split())
        if dialect in _LOG10_NATIVE_DIALECTS:
            assert "LOG10(AMOUNT)" in upper_no_ws, (
                f"{dialect}: expected literal LOG10(amount), got:\n{sql}"
            )
            # Must not have canonicalised to either arg-order 2-arg form.
            assert "LOG(10,AMOUNT)" not in upper_no_ws, (
                f"{dialect}: should not canonicalise to LOG(10, amount):\n{sql}"
            )
            assert "LOG(AMOUNT,10)" not in upper_no_ws, (
                f"{dialect}: should not canonicalise to LOG(amount, 10):\n{sql}"
            )
        else:
            # Fallback: current 2-arg LOG behaviour is preserved on dialects
            # without native single-arg log10 (oracle).
            assert "LOG(10,AMOUNT)" in upper_no_ws or "LOG(AMOUNT,10)" in upper_no_ws, (
                f"{dialect}: expected fallback LOG(base,x) form, got:\n{sql}"
            )

    @pytest.mark.parametrize("dialect", TestMultiDialectGeneration.ALL_DIALECTS)
    async def test_log2_in_column_sql_is_preserved(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log2_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        upper_no_ws = "".join(sql.upper().split())
        if dialect in _LOG2_NATIVE_DIALECTS:
            assert "LOG2(AMOUNT)" in upper_no_ws, (
                f"{dialect}: expected literal LOG2(amount), got:\n{sql}"
            )
            assert "LOG(2,AMOUNT)" not in upper_no_ws, (
                f"{dialect}: should not canonicalise to LOG(2, amount):\n{sql}"
            )
            assert "LOG(AMOUNT,2)" not in upper_no_ws, (
                f"{dialect}: should not canonicalise to LOG(amount, 2):\n{sql}"
            )
        else:
            # Fallback for tsql / oracle / redshift / snowflake (no native LOG2).
            assert "LOG(2,AMOUNT)" in upper_no_ws or "LOG(AMOUNT,2)" in upper_no_ws, (
                f"{dialect}: expected fallback LOG(base,x) form, got:\n{sql}"
            )

    @pytest.mark.parametrize("dialect", sorted(_LOG10_NATIVE_DIALECTS))
    async def test_log10_inside_filtered_column_survives_reparse(
        self, dialect: str,
    ) -> None:
        """A filtered Column (``Column.filter="..."``) wraps the resolved
        value in ``CASE WHEN ... THEN ... END`` and re-parses through
        sqlglot. The log-alias rewrite must survive that round-trip — a
        re-parse of ``LOG10(amount)`` would otherwise canonicalise back to
        a generic ``Log`` node and re-emit as ``LOG(10, amount)``.
        """
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(
                    name="log_completed_amount",
                    sql="log10(amount)",
                    filter="status = 'completed'",
                    type=DataType.DOUBLE,
                ),
            ],
        )
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log_completed_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=model)
        upper_no_ws = "".join(sql.upper().split())
        assert "LOG10(AMOUNT)" in upper_no_ws, (
            f"{dialect}: expected literal log10(amount) inside filtered "
            f"column wrapper, got:\n{sql}"
        )
        assert "LOG(10,AMOUNT)" not in upper_no_ws, (
            f"{dialect}: filtered-column re-parse must not re-canonicalise "
            f"to LOG(10, amount):\n{sql}"
        )

    @pytest.mark.parametrize("dialect", sorted(_LOG10_NATIVE_DIALECTS))
    async def test_log10_in_arithmetic_measure_is_preserved(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        """Arithmetic that mixes a log10 column-derived measure with COUNT(*).
        Pins that the rewrite survives the arithmetic enrichment path.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log_amount:max / *:count", name="ratio")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        upper_no_ws = "".join(sql.upper().split())
        assert "LOG10(AMOUNT)" in upper_no_ws, (
            f"{dialect}: expected log10(amount) inside arithmetic measure:\n{sql}"
        )
        assert "COUNT(" in sql.upper(), f"COUNT(*) leg missing on {dialect}:\n{sql}"

    @pytest.mark.parametrize("dialect", TestMultiDialectGeneration.ALL_DIALECTS)
    async def test_log_with_non_alias_base_unchanged(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        """Negative test: ``log(3, amount)`` (literal base ≠ 10/2) must keep
        the standard 2-arg form. The rewrite is scoped to bases 10 and 2 only.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log3_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        upper_no_ws = "".join(sql.upper().split())
        # Must NOT have invented a single-arg LOG3(...) function.
        assert "LOG3(" not in upper_no_ws, (
            f"{dialect}: must not invent LOG3() — only base 10 and 2 are aliased:\n{sql}"
        )
        # The 2-arg form must remain in some arg order.
        assert "LOG(3,AMOUNT)" in upper_no_ws or "LOG(AMOUNT,3)" in upper_no_ws, (
            f"{dialect}: expected 2-arg LOG(3, amount) preserved, got:\n{sql}"
        )

    @pytest.mark.parametrize("dialect", TestMultiDialectGeneration.ALL_DIALECTS)
    async def test_ln_unchanged(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        """``ln(x)`` lives under a separate sqlglot AST node (``exp.Ln``);
        the rewrite must not affect it.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="ln_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        # T-SQL has no LN — sqlglot transpiles to LOG(x). Every other dialect
        # keeps LN(...). We only assert the rewrite did not invent something.
        assert "LN10(" not in sql.upper()
        assert "LN2(" not in sql.upper()


class TestStringHygieneDialectTranslation:
    """DEV-1378: lowercase string-hygiene operators are pass-through to
    the emitted SQL string, then re-parsed by sqlglot under the target
    dialect at WHERE-assembly. sqlglot's per-dialect emitter chooses
    each dialect's preferred spelling. These tests pin the actual
    emitted SQL across SQLite / Postgres / MySQL / DuckDB / ClickHouse
    so a future sqlglot upgrade that changes the spelling is caught.
    """

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("sqlite", "LOWER(orders.status) = 'active'"),
            ("postgres", "LOWER(orders.status) = 'active'"),
            ("mysql", "LOWER(orders.status) = 'active'"),
            ("duckdb", "LOWER(orders.status) = 'active'"),
            ("clickhouse", "lower(orders.status) = 'active'"),
        ],
    )
    async def test_lower(self, orders_model: SlayerModel, dialect: str, expected: str) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["lower(status) = 'active'"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        assert expected in sql, f"{dialect}: {expected!r} not in {sql!r}"

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("sqlite", "INSTR(orders.status, ',')"),
            ("postgres", "POSITION(',' IN orders.status)"),
            ("mysql", "LOCATE(',', orders.status)"),
            ("duckdb", "STRPOS(orders.status, ',')"),
            ("clickhouse", "POSITION(orders.status, ',')"),
        ],
    )
    async def test_instr_translates_per_dialect(
        self, orders_model: SlayerModel, dialect: str, expected: str,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["instr(status, ',') > 0"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        assert expected in sql, f"{dialect}: {expected!r} not in {sql!r}"

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("sqlite", "SUBSTRING(orders.status, 1, 5)"),
            ("postgres", "SUBSTRING(orders.status FROM 1 FOR 5)"),
            ("mysql", "SUBSTRING(orders.status, 1, 5)"),
            ("duckdb", "SUBSTRING(orders.status, 1, 5)"),
            ("clickhouse", "substr(orders.status, 1, 5)"),
        ],
    )
    async def test_substr_translates_per_dialect(
        self, orders_model: SlayerModel, dialect: str, expected: str,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["substr(status, 1, 5) = 'abcde'"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        assert expected in sql, f"{dialect}: {expected!r} not in {sql!r}"

    @pytest.mark.parametrize(
        "dialect,expected_substring",
        [
            # SQLite normalises CONCAT(...) → a || b at emit time.
            ("sqlite", "orders.status || orders.status"),
            ("postgres", "CONCAT(orders.status, orders.status)"),
            ("mysql", "CONCAT(orders.status, orders.status)"),
            ("duckdb", "CONCAT(orders.status, orders.status)"),
            ("clickhouse", "CONCAT(orders.status, orders.status)"),
        ],
    )
    async def test_concat_via_pipe_pipe_translates_per_dialect(
        self, orders_model: SlayerModel, dialect: str, expected_substring: str,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status || status = 'foo'"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        assert expected_substring in sql, f"{dialect}: {expected_substring!r} not in {sql!r}"
