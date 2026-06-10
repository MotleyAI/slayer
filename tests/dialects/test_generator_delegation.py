"""SQLGenerator-surface tests exercising the per-dialect dispatch path.

DEV-1542 cleanup: lifted from ``tests/test_sql_generator.py``. The
strategy-class tests in ``tests/dialects/test_<dialect>.py`` pin emission
from ``<Dialect>().build_*`` with raw column-SQL inputs. This file pins
the additional wrapping that happens when ``SQLGenerator`` dispatches
through the dialect: ``_resolve_sql`` qualification, ``EnrichedMeasure``
filter wrapping, ``agg_kwargs`` parameter validation, model-level
default propagation, and full end-to-end query integration on T-SQL.
"""

from __future__ import annotations

import pytest
import sqlglot

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Aggregation, AggregationParam, Column, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.enriched import EnrichedMeasure
from slayer.sql.generator import SQLGenerator

from tests.dialects.conftest import _generate


class TestSqliteJsonExtractInGenerator:
    """DEV-1331: ``json_extract(col, '$.path')`` in ``Column.sql`` must not be
    rewritten to ``col -> '$.path'`` on SQLite — the operator returns the
    JSON-quoted form, silently breaking equality / CASE WHEN matches.
    """

    @pytest.fixture
    def model_with_json_dim(self) -> SlayerModel:
        return SlayerModel(
            name="users",
            sql_table="users",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="payload", sql="payload", type=DataType.TEXT),
                Column(
                    name="tier",
                    sql="json_extract(payload, '$.tier')",
                    type=DataType.TEXT,
                ),
                Column(
                    name="is_gold",
                    sql=(
                        "CASE LOWER(json_extract(payload, '$.tier')) "
                        "WHEN 'gold' THEN 1 ELSE 0 END"
                    ),
                    type=DataType.DOUBLE,
                ),
            ],
        )

    async def test_sqlite_column_sql_with_json_extract_dimension(
        self, model_with_json_dim: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="users",
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator=gen, query=query, model=model_with_json_dim)
        assert "JSON_EXTRACT(" in sql, f"missing JSON_EXTRACT in:\n{sql}"
        # The lossy ``payload -> '$.tier'`` form must not appear.
        assert "payload -> '$.tier'" not in sql, sql

    async def test_sqlite_column_sql_with_json_extract_in_case_when(
        self, model_with_json_dim: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="users",
            measures=[ModelMeasure(formula="is_gold:sum")],
        )
        sql = await _generate(generator=gen, query=query, model=model_with_json_dim)
        assert "JSON_EXTRACT(" in sql, sql
        assert "payload -> '$.tier'" not in sql, sql

    async def test_sqlite_inline_sql_subquery_with_json_extract(self) -> None:
        model = SlayerModel(
            name="users",
            sql=(
                "SELECT id, json_extract(payload, '$.tier') AS tier "
                "FROM raw_users"
            ),
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="tier", sql="tier", type=DataType.TEXT),
            ],
        )
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="users",
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator=gen, query=query, model=model)
        assert "JSON_EXTRACT(" in sql, sql
        assert "payload -> '$.tier'" not in sql, sql

    async def test_postgres_column_sql_with_json_extract_unchanged(
        self, model_with_json_dim: SlayerModel,
    ) -> None:
        """Regression guard: rewrite is SQLite-only; Postgres path is untouched.

        Postgres has no scalar-vs-JSON quoting bug for ``json_extract``;
        sqlglot transpiles it to ``JSON_EXTRACT_PATH(j, 'k')``. We just
        assert the generator produces *some* form of JSON extraction and
        does not crash.
        """
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="users",
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator=gen, query=query, model=model_with_json_dim)
        assert "JSON_EXTRACT" in sql.upper(), sql


class TestMedianPercentilePerDialect:
    """Per-dialect SQL emission for median and percentile aggregations.

    These pin the dialect-specific output of `_build_median` and
    `_build_percentile` and assert that MySQL raises ``NotImplementedError``
    (no native function, no Python-UDF mechanism).
    """

    def _measure(
        self,
        *,
        agg: str,
        agg_kwargs: dict[str, str] | None = None,
    ) -> EnrichedMeasure:
        return EnrichedMeasure(
            name="amount",
            sql="amount",
            model_name="orders",
            alias=f"amount_{agg}",
            aggregation=agg,
            agg_kwargs=agg_kwargs or {},
        )

    # --- median ------------------------------------------------------------

    def test_build_median_postgres(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        inner = sqlglot.parse_one("amount", dialect="postgres")
        sql = gen._build_median(inner).sql(dialect="postgres")
        assert sql == "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)"

    def test_build_median_sqlite_uses_udf_call(self) -> None:
        gen = SQLGenerator(dialect="sqlite")
        inner = sqlglot.parse_one("amount", dialect="sqlite")
        sql = gen._build_median(inner).sql(dialect="sqlite")
        # sqlglot rewrites MEDIAN(x) to PERCENTILE_CONT(x, 0.5) for SQLite,
        # which our percentile_cont UDF handles. SQLite UDF lookup is
        # case-insensitive.
        assert sql == "PERCENTILE_CONT(amount, 0.5)"

    def test_build_median_clickhouse_unchanged(self) -> None:
        gen = SQLGenerator(dialect="clickhouse")
        inner = sqlglot.parse_one("amount", dialect="clickhouse")
        sql = gen._build_median(inner).sql(dialect="clickhouse")
        # ClickHouse has native median(); sqlglot transpiles to its parametric form.
        assert sql == "quantile(0.5)(amount)"

    def test_build_median_duckdb(self) -> None:
        gen = SQLGenerator(dialect="duckdb")
        inner = sqlglot.parse_one("amount", dialect="duckdb")
        sql = gen._build_median(inner).sql(dialect="duckdb")
        # sqlglot translates PERCENTILE_CONT to DuckDB's QUANTILE_CONT.
        assert "QUANTILE_CONT" in sql or "PERCENTILE_CONT" in sql

    def test_build_median_mysql_raises(self) -> None:
        gen = SQLGenerator(dialect="mysql")
        inner = sqlglot.parse_one("amount", dialect="mysql")
        with pytest.raises(NotImplementedError, match="MySQL"):
            gen._build_median(inner)

    # --- percentile --------------------------------------------------------

    def test_build_percentile_postgres(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.95"})
        sql = gen._build_percentile(m).sql(dialect="postgres")
        assert sql == "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY orders.amount)"

    def test_build_percentile_sqlite(self) -> None:
        gen = SQLGenerator(dialect="sqlite")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.5"})
        sql = gen._build_percentile(m).sql(dialect="sqlite")
        assert sql == "PERCENTILE_CONT(orders.amount, 0.5)"

    def test_build_percentile_clickhouse_emits_quantile(self) -> None:
        gen = SQLGenerator(dialect="clickhouse")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.75"})
        sql = gen._build_percentile(m).sql(dialect="clickhouse")
        # ClickHouse parametric aggregate syntax.
        assert sql == "quantile(0.75)(orders.amount)"

    @pytest.mark.parametrize("p", ["0.05", "0.25", "0.5", "0.95"])
    def test_build_percentile_clickhouse_param_substitution(self, p: str) -> None:
        gen = SQLGenerator(dialect="clickhouse")
        m = self._measure(agg="percentile", agg_kwargs={"p": p})
        sql = gen._build_percentile(m).sql(dialect="clickhouse")
        assert sql == f"quantile({p})(orders.amount)"

    def test_build_percentile_duckdb(self) -> None:
        gen = SQLGenerator(dialect="duckdb")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.5"})
        sql = gen._build_percentile(m).sql(dialect="duckdb")
        # sqlglot rewrites the WITHIN GROUP form to DuckDB's QUANTILE_CONT.
        assert "QUANTILE_CONT" in sql
        # Qualified column.
        assert "orders.amount" in sql

    def test_build_percentile_mysql_raises(self) -> None:
        gen = SQLGenerator(dialect="mysql")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.5"})
        with pytest.raises(NotImplementedError, match="MySQL"):
            gen._build_percentile(m)

    def test_build_percentile_missing_p_raises(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(agg="percentile", agg_kwargs={})
        with pytest.raises(ValueError, match="requires parameter 'p'"):
            gen._build_percentile(m)

    def test_build_percentile_unsafe_p_rejected(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.5); DROP TABLE x; --"})
        with pytest.raises(ValueError, match="Unsafe value"):
            gen._build_percentile(m)

    def test_build_percentile_uses_model_level_default_p(self) -> None:
        """Model-level Aggregation(name='percentile', params=[p=...]) supplies the default."""
        gen = SQLGenerator(dialect="postgres")
        agg_def = Aggregation(
            name="percentile",
            params=[AggregationParam(name="p", sql="0.9")],
        )
        m = EnrichedMeasure(
            name="amount",
            sql="amount",
            model_name="orders",
            alias="amount_percentile",
            aggregation="percentile",
            agg_kwargs={},
            aggregation_def=agg_def,
        )
        sql = gen._build_percentile(m).sql(dialect="postgres")
        assert sql == "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY orders.amount)"

    def test_build_percentile_query_kwarg_overrides_model_default(self) -> None:
        """Query-time agg_kwargs win over the model-level default."""
        gen = SQLGenerator(dialect="postgres")
        agg_def = Aggregation(
            name="percentile",
            params=[AggregationParam(name="p", sql="0.9")],
        )
        m = EnrichedMeasure(
            name="amount",
            sql="amount",
            model_name="orders",
            alias="amount_percentile",
            aggregation="percentile",
            agg_kwargs={"p": "0.25"},
            aggregation_def=agg_def,
        )
        sql = gen._build_percentile(m).sql(dialect="postgres")
        assert sql == "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY orders.amount)"

    # --- A2: percentile p must be a numeric literal in [0, 1] -----------

    def test_build_percentile_rejects_non_literal_p(self) -> None:
        """`measure:percentile(p=quantity)` must fail at SQL-generation time
        with a clear validation error, not silently emit a column reference
        in PERCENTILE_CONT(p)'s direct-arg slot. Without this guard a
        non-literal `p` flows through `_resolve_agg_param` (which is
        identifier-friendly for the column-ref kwargs like `other=`), gets
        rendered as `orders.quantity`, and fails at the database with a
        dialect-specific error.
        """
        gen = SQLGenerator(dialect="postgres")
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={"p": "quantity"},
        )
        with pytest.raises(ValueError, match="numeric literal"):
            gen._build_percentile(m)

    def test_build_percentile_rejects_p_out_of_range(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={"p": "1.5"},
        )
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            gen._build_percentile(m)

    def test_build_percentile_rejects_p_negative(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={"p": "-0.1"},
        )
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            gen._build_percentile(m)

    def test_build_percentile_rejects_non_literal_p_via_model_default(self) -> None:
        """Model-level defaults bypass `_validate_agg_param_value` (trust
        model: model authors are trusted). The new numeric-literal check
        catches anything that's not a number even on that path — closes
        the gap where a malicious model author could put `p=pg_sleep(10)`
        as a default. Codex review #3 on PR #82.
        """
        gen = SQLGenerator(dialect="postgres")
        agg_def = Aggregation(
            name="percentile",
            params=[AggregationParam(name="p", sql="pg_sleep(10)")],
        )
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={}, aggregation_def=agg_def,
        )
        with pytest.raises(ValueError, match="numeric literal"):
            gen._build_percentile(m)


class TestStatAggsPerDialect:
    """Per-dialect SQL emission for the new statistical aggregations
    (DEV-1317): stddev_samp, stddev_pop, var_samp, var_pop, corr,
    covar_samp, covar_pop.

    These pin the observed SQL output for each dialect — including
    sqlglot's transpilation quirks (e.g., var_samp → VARIANCE on SQLite,
    var_pop → VARIANCE_POP on SQLite/MySQL) — so the SQLite UDF
    registration knows which names to alias. The expected outputs use
    fully model-qualified column references (``orders.amount`` etc.) —
    pinning the post-refactor invariant that all three dialect-aware
    builders go through ``_resolve_sql`` for the value column AND for
    the second column on two-arg stats, matching the standard
    sum/avg/min/max path.
    """

    def _measure(
        self,
        *,
        agg: str,
        agg_kwargs: dict[str, str] | None = None,
    ) -> EnrichedMeasure:
        return EnrichedMeasure(
            name="amount",
            sql="amount",
            model_name="orders",
            alias=f"amount_{agg}",
            aggregation=agg,
            agg_kwargs=agg_kwargs or {},
        )

    # --- stddev_samp -------------------------------------------------------

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "STDDEV_SAMP(orders.amount)"),
            ("duckdb", "STDDEV_SAMP(orders.amount)"),
            ("mysql", "STDDEV_SAMP(orders.amount)"),
            ("sqlite", "STDDEV_SAMP(orders.amount)"),
            # T-SQL: STDEV is the T-SQL name for sample standard deviation
            ("tsql", "STDEV(orders.amount)"),
        ],
    )
    def test_build_stddev_samp(self, dialect: str, expected: str) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg="stddev_samp")
        sql = gen._build_agg(measure=m)[0].sql(dialect=dialect)
        assert sql == expected

    # --- stddev_pop --------------------------------------------------------

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "STDDEV_POP(orders.amount)"),
            ("duckdb", "STDDEV_POP(orders.amount)"),
            ("mysql", "STDDEV_POP(orders.amount)"),
            ("sqlite", "STDDEV_POP(orders.amount)"),
            # T-SQL: STDEVP is the T-SQL name for population standard deviation
            ("tsql", "STDEVP(orders.amount)"),
        ],
    )
    def test_build_stddev_pop(self, dialect: str, expected: str) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg="stddev_pop")
        sql = gen._build_agg(measure=m)[0].sql(dialect=dialect)
        assert sql == expected

    # --- var_samp ----------------------------------------------------------

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "VAR_SAMP(orders.amount)"),
            # sqlglot rewrites VAR_SAMP → VARIANCE on SQLite/DuckDB; the
            # SQLite UDF is therefore registered under the alias `variance`
            # so generator output still resolves at runtime. MySQL is the
            # exception: sqlglot's MySQL dialect rewrites the same way, but
            # MySQL's ``VARIANCE`` is an alias for ``VAR_POP`` (population
            # variance), so the rewritten SQL would silently return the
            # wrong value. The generator emits ``VAR_SAMP`` directly on
            # MySQL via ``exp.Anonymous`` to bypass the transpile.
            ("duckdb", "VARIANCE(orders.amount)"),
            ("mysql", "VAR_SAMP(orders.amount)"),
            ("sqlite", "VARIANCE(orders.amount)"),
            # T-SQL: VAR is the T-SQL name for sample variance
            ("tsql", "VAR(orders.amount)"),
        ],
    )
    def test_build_var_samp(self, dialect: str, expected: str) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg="var_samp")
        sql = gen._build_agg(measure=m)[0].sql(dialect=dialect)
        assert sql == expected

    # --- var_pop -----------------------------------------------------------

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "VAR_POP(orders.amount)"),
            ("duckdb", "VAR_POP(orders.amount)"),
            # sqlglot rewrites VAR_POP → VARIANCE_POP on SQLite (handled by
            # a registered UDF alias). MySQL gets the same buggy rewrite,
            # but ``VARIANCE_POP`` is not a real MySQL function — the
            # generator emits ``VAR_POP`` directly via ``exp.Anonymous``.
            ("mysql", "VAR_POP(orders.amount)"),
            ("sqlite", "VARIANCE_POP(orders.amount)"),
            # T-SQL: VARP is the T-SQL name for population variance
            ("tsql", "VARP(orders.amount)"),
        ],
    )
    def test_build_var_pop(self, dialect: str, expected: str) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg="var_pop")
        sql = gen._build_agg(measure=m)[0].sql(dialect=dialect)
        assert sql == expected

    # --- corr (2-arg via `other=` kwarg) ----------------------------------

    # corr / covar_samp / covar_pop all share the 2-arg shape and the
    # `other=` kwarg parameter; parametrize once instead of repeating.
    @pytest.mark.parametrize(
        "agg,sql_fn",
        [
            ("corr", "CORR"),
            ("covar_samp", "COVAR_SAMP"),
            ("covar_pop", "COVAR_POP"),
        ],
    )
    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "sqlite"])
    def test_build_two_arg_stat_emits_two_arg_call(
        self, dialect: str, agg: str, sql_fn: str,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg=agg, agg_kwargs={"other": "quantity"})
        sql = gen._build_agg(measure=m)[0].sql(dialect=dialect)
        # Both legs go through _resolve_sql, so a bare `quantity` kwarg
        # qualifies under the LHS measure's model_name.
        assert sql == f"{sql_fn}(orders.amount, orders.quantity)"

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_clickhouse(self, agg: str) -> None:
        gen = SQLGenerator(dialect="clickhouse")
        m = self._measure(agg=agg, agg_kwargs={"other": "quantity"})
        sql = gen._build_agg(measure=m)[0].sql(dialect="clickhouse")
        # ClickHouse casing is its own thing; assert the call shape only.
        assert sql.lower() == f"{agg.lower()}(orders.amount, orders.quantity)"

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_mysql_emits_formula(self, agg: str) -> None:
        # MySQL has no native CORR / COVAR_SAMP / COVAR_POP but can express them
        # via the variance-decomposition formula: cov(x,y) = (var(x+y)-var(x)-var(y))/2
        gen = SQLGenerator(dialect="mysql")
        m = self._measure(agg=agg, agg_kwargs={"other": "quantity"})
        sql = gen._build_agg(measure=m)[0].sql(dialect="mysql")
        # Formula uses MySQL-compatible VAR_SAMP or VAR_POP (covar_pop uses population variance)
        assert "VAR_SAMP(" in sql or "VAR_POP(" in sql
        # Not a direct two-arg COVAR_SAMP/COVAR_POP/CORR call (those don't exist in MySQL)
        assert f"{agg.upper()}(" not in sql
        # Variance-decomposition uses division
        assert "/" in sql
        # Both columns are NULL-guarded against each other
        assert "CASE WHEN" in sql.upper()
        # MySQL may emit "IS NOT NULL" or "NOT ... IS NULL" (semantically equivalent)
        assert "IS NOT NULL" in sql.upper() or "IS NULL" in sql.upper()

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_tsql_emits_formula(self, agg: str) -> None:
        # T-SQL has no native CORR / COVAR_SAMP / COVAR_POP; use variance-decomposition
        gen = SQLGenerator(dialect="tsql")
        m = self._measure(agg=agg, agg_kwargs={"other": "quantity"})
        sql = gen._build_agg(measure=m)[0].sql(dialect="tsql")
        # Formula uses T-SQL VAR() or VARP() (covar_pop uses population variance)
        assert "VAR(" in sql or "VARP(" in sql
        # Not a direct two-arg call
        assert f"{agg.upper()}(" not in sql
        # Variance-decomposition uses division
        assert "/" in sql

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_mysql_missing_other_prioritises_param_error(
        self, agg: str,
    ) -> None:
        """When BOTH conditions hold (MySQL dialect AND missing `other=`
        kwarg), the missing-required-param error is more useful to the user
        than "MySQL not supported" — it points at the actual mistake. Codex
        review #5 on PR #82: the MySQL guard ran before `other=` resolution.
        """
        gen = SQLGenerator(dialect="mysql")
        m = self._measure(agg=agg, agg_kwargs={})
        with pytest.raises(ValueError, match=r"requires parameter 'other'"):
            gen._build_agg(measure=m)

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_missing_other_raises(self, agg: str) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(agg=agg, agg_kwargs={})
        with pytest.raises(ValueError, match=r"requires parameter 'other'|other="):
            gen._build_agg(measure=m)

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_unsafe_other_rejected(self, agg: str) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(
            agg=agg,
            agg_kwargs={"other": "quantity); DROP TABLE x; --"},
        )
        with pytest.raises(ValueError, match="Unsafe value"):
            gen._build_agg(measure=m)

    # --- filter wrapping ---------------------------------------------------

    def test_build_stddev_samp_with_filter_wraps_value(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = EnrichedMeasure(
            name="amount",
            sql="amount",
            model_name="orders",
            alias="amount_stddev_samp",
            aggregation="stddev_samp",
            agg_kwargs={},
            filter_sql="status = 'completed'",
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="postgres")
        # Filter wraps the qualified column reference.
        assert "CASE WHEN status = 'completed' THEN orders.amount END" in sql
        assert "STDDEV_SAMP" in sql

    def test_build_corr_with_filter_wraps_both_columns(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = EnrichedMeasure(
            name="amount",
            sql="amount",
            model_name="orders",
            alias="amount_corr",
            aggregation="corr",
            agg_kwargs={"other": "quantity"},
            filter_sql="status = 'completed'",
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="postgres")
        # Both legs of corr() must be wrapped in CASE WHEN so non-matching
        # rows contribute NULL pairs (which the aggregate skips entirely).
        assert sql.count("CASE WHEN status = 'completed'") == 2
        assert "CORR(" in sql
        # Both legs are also qualified.
        assert "orders.amount" in sql
        assert "orders.quantity" in sql


class TestTsqlDialect:
    """DEV-1520: T-SQL (SQL Server) dialect-specific SQL generation tests."""

    @pytest.fixture
    def gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="tsql")

    @pytest.fixture
    def orders_model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="dbo.orders",
            data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
            ],
        )

    # --- date trunc ---

    def test_build_date_trunc_month_emits_datetrunc(self, gen: SQLGenerator) -> None:
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_date_trunc(col, TimeGranularity.MONTH).sql(dialect="tsql")
        assert "DATETRUNC" in sql.upper()
        assert "MONTH" in sql.upper()

    def test_build_date_trunc_year(self, gen: SQLGenerator) -> None:
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_date_trunc(col, TimeGranularity.YEAR).sql(dialect="tsql")
        assert "DATETRUNC" in sql.upper()
        assert "YEAR" in sql.upper()

    def test_build_date_trunc_day(self, gen: SQLGenerator) -> None:
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_date_trunc(col, TimeGranularity.DAY).sql(dialect="tsql")
        assert "DATETRUNC" in sql.upper()
        assert "DAY" in sql.upper()

    def test_build_date_trunc_week_uses_iso_week(self, gen: SQLGenerator) -> None:
        """Week truncation must use ISO_WEEK for Monday-start (@@DATEFIRST-independent)."""
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_date_trunc(col, TimeGranularity.WEEK).sql(dialect="tsql")
        assert "ISO_WEEK" in sql.upper(), (
            f"T-SQL week truncation must use ISO_WEEK (not WEEK) to be "
            f"locale-independent. Got: {sql}"
        )
        # DATETRUNC(WEEK, ...) without ISO_ is locale-dependent — must not appear
        assert "DATETRUNC(WEEK" not in sql.upper().replace("ISO_WEEK", ""), (
            f"T-SQL week truncation must not use bare WEEK (@@DATEFIRST-dependent): {sql}"
        )

    def test_build_date_trunc_quarter(self, gen: SQLGenerator) -> None:
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_date_trunc(col, TimeGranularity.QUARTER).sql(dialect="tsql")
        assert "DATETRUNC" in sql.upper()
        assert "QUARTER" in sql.upper()

    def test_build_date_trunc_no_date_trunc_function(self, gen: SQLGenerator) -> None:
        """T-SQL uses DATETRUNC (no underscore), not DATE_TRUNC."""
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_date_trunc(col, TimeGranularity.MONTH).sql(dialect="tsql")
        assert "DATE_TRUNC" not in sql.upper(), f"T-SQL should use DATETRUNC, got: {sql}"

    # --- time offset ---

    def test_build_time_offset_year(self, gen: SQLGenerator) -> None:
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_time_offset_expr(col, -1, "year").sql(dialect="tsql")
        assert "DATEADD" in sql.upper()
        assert "YEAR" in sql.upper()

    def test_build_time_offset_month(self, gen: SQLGenerator) -> None:
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_time_offset_expr(col, -1, "month").sql(dialect="tsql")
        assert "DATEADD" in sql.upper()
        assert "MONTH" in sql.upper()

    def test_build_time_offset_positive(self, gen: SQLGenerator) -> None:
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_time_offset_expr(col, 3, "day").sql(dialect="tsql")
        assert "DATEADD" in sql.upper()
        assert "DAY" in sql.upper()
        assert "3" in sql
        assert "created_at" in sql
        assert "INTERVAL" not in sql.upper()

    @pytest.mark.parametrize("gran", ["year", "month", "day", "week"])
    def test_build_time_offset_no_interval_keyword(self, gen: SQLGenerator, gran: str) -> None:
        """T-SQL must never emit INTERVAL (invalid syntax) for time offsets."""
        col = sqlglot.parse_one("created_at", dialect="tsql")
        sql = gen._build_time_offset_expr(col, -1, gran).sql(dialect="tsql")
        assert "INTERVAL" not in sql.upper(), (
            f"INTERVAL is invalid T-SQL syntax for granularity {gran!r}: {sql}"
        )

    # --- median / percentile (unsupported) ---

    def test_build_median_tsql_raises(self, gen: SQLGenerator) -> None:
        """T-SQL PERCENTILE_CONT is window-only (requires OVER); unsupported as GROUP BY agg."""
        inner = sqlglot.parse_one("amount", dialect="tsql")
        with pytest.raises(NotImplementedError):
            gen._build_median(inner)

    def test_build_percentile_tsql_raises(self, gen: SQLGenerator) -> None:
        """T-SQL PERCENTILE_CONT is window-only (requires OVER); unsupported as GROUP BY agg."""
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={"p": "0.5"},
        )
        with pytest.raises(NotImplementedError):
            gen._build_percentile(m)

    # --- one-arg stat aggs ---

    @pytest.mark.parametrize("agg,expected_fn", [
        ("stddev_samp", "STDEV"),
        ("stddev_pop", "STDEVP"),
        ("var_samp", "VAR"),
        ("var_pop", "VARP"),
    ])
    def test_build_one_arg_stat_tsql(
        self, gen: SQLGenerator, agg: str, expected_fn: str,
    ) -> None:
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias=f"amount_{agg}", aggregation=agg, agg_kwargs={},
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="tsql")
        assert f"{expected_fn}(" in sql, f"Expected {expected_fn}() in {sql!r}"
        assert "orders.amount" in sql

    # --- two-arg stat aggs (variance-decomposition formula) ---

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_tsql_uses_var_function(
        self, gen: SQLGenerator, agg: str,
    ) -> None:
        """covar/corr on T-SQL must use T-SQL VAR() not Postgres VAR_SAMP()."""
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias=f"amount_{agg}", aggregation=agg,
            agg_kwargs={"other": "quantity"},
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="tsql")
        # covar_samp/corr use VAR(), covar_pop uses VARP() — both are valid T-SQL
        assert "VAR(" in sql or "VARP(" in sql, f"Expected VAR()/VARP() in formula, got: {sql}"
        # Must NOT use Postgres-style VAR_SAMP (invalid T-SQL function)
        assert "VAR_SAMP(" not in sql, f"VAR_SAMP is not a T-SQL function: {sql}"

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_tsql_no_direct_call(
        self, gen: SQLGenerator, agg: str,
    ) -> None:
        """T-SQL doesn't have COVAR_SAMP / COVAR_POP / CORR natively."""
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias=f"amount_{agg}", aggregation=agg,
            agg_kwargs={"other": "quantity"},
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="tsql")
        assert f"{agg.upper()}(" not in sql, (
            f"T-SQL should not emit a direct {agg.upper()}() call; use formula. Got: {sql}"
        )

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_tsql_contains_both_columns(
        self, gen: SQLGenerator, agg: str,
    ) -> None:
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias=f"amount_{agg}", aggregation=agg,
            agg_kwargs={"other": "quantity"},
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="tsql")
        assert "orders.amount" in sql
        assert "orders.quantity" in sql

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_tsql_uses_division(
        self, gen: SQLGenerator, agg: str,
    ) -> None:
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias=f"amount_{agg}", aggregation=agg,
            agg_kwargs={"other": "quantity"},
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="tsql")
        assert "/" in sql, f"Variance-decomposition formula must contain division: {sql}"

    @pytest.mark.parametrize("agg", ["covar_samp", "covar_pop"])
    def test_build_covar_tsql_null_guards_both_columns(
        self, gen: SQLGenerator, agg: str,
    ) -> None:
        """Both columns must be NULL-guarded against each other in the formula.

        For `covar_samp(x, y)`: x is guarded as `CASE WHEN y IS NOT NULL THEN x END`
        and y is guarded as `CASE WHEN x IS NOT NULL THEN y END`, so pairs where
        either column is NULL are excluded from the variance computation.
        """
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias=f"amount_{agg}", aggregation=agg,
            agg_kwargs={"other": "quantity"},
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="tsql")
        upper = sql.upper()
        # Both x-guarded-by-y and y-guarded-by-x CASE WHEN patterns must appear
        assert "CASE WHEN" in upper, f"Expected NULL guards (CASE WHEN) in formula: {sql}"
        # T-SQL may emit "IS NOT NULL" or "NOT ... IS NULL" (semantically equivalent)
        assert "IS NOT NULL" in upper or "IS NULL" in upper, (
            f"Expected IS NULL/IS NOT NULL guard in formula: {sql}"
        )

    def test_build_corr_tsql_uses_stdev_for_denominator(self, gen: SQLGenerator) -> None:
        """corr denominator uses STDEV (T-SQL stddev_samp) * STDEV."""
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias="amount_corr", aggregation="corr",
            agg_kwargs={"other": "quantity"},
        )
        sql = gen._build_agg(measure=m)[0].sql(dialect="tsql")
        assert "STDEV(" in sql, f"Expected STDEV() in corr denominator, got: {sql}"

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_tsql_missing_other_raises(
        self, gen: SQLGenerator, agg: str,
    ) -> None:
        m = EnrichedMeasure(
            name="amount", sql="amount", model_name="orders",
            alias=f"amount_{agg}", aggregation=agg, agg_kwargs={},
        )
        with pytest.raises(ValueError, match=r"requires parameter 'other'|other="):
            gen._build_agg(measure=m)

    # --- full query integration ---

    async def test_full_aggregation_query_valid_tsql(
        self, gen: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(gen, query, orders_model)
        assert "COUNT(" in sql
        assert "SUM(" in sql

    async def test_full_query_with_time_dim_valid_tsql(
        self, gen: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
        )
        sql = await _generate(gen, query, orders_model)
        assert "DATETRUNC" in sql.upper()
        assert "SUM(" in sql

    async def test_calendar_time_shift_tsql_uses_dateadd(
        self, gen: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="time_shift(revenue:sum, -1, 'year')", name="rev_prev_year"),
            ],
        )
        sql = await _generate(gen, query, orders_model)
        assert "shifted_" in sql
        assert "DATEADD" in sql.upper()
        assert "INTERVAL" not in sql.upper(), (
            f"INTERVAL is invalid T-SQL syntax; shifted CTE must use DATEADD:\n{sql}"
        )
