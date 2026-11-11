"""EXPLAIN as a typed statement set built by the dialect (spec: database/client-contract)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlglot import exp

from slayer.core.errors import ExplainUnsupportedError
from slayer.sql.dialects import (
    SQLGLOT_NAMES,
    BigqueryDialect,
    PostgresDialect,
    SqlDialect,
    TsqlDialect,
    get_dialect,
)
from slayer.sql.dialects.base import ExplainStatements

_SQL = "SELECT id FROM orders"

_SINGLE_STATEMENT = {
    "clickhouse": "EXPLAIN",
    "databricks": "EXPLAIN EXTENDED",
    "duckdb": "EXPLAIN ANALYZE",
    "mysql": "EXPLAIN FORMAT=JSON",
    "oracle": "EXPLAIN PLAN FOR",
    "postgres": "EXPLAIN ANALYZE",
    "presto": "EXPLAIN ANALYZE",
    "redshift": "EXPLAIN",
    "snowflake": "EXPLAIN USING JSON",
    "spark": "EXPLAIN EXTENDED",
    "sqlite": "EXPLAIN QUERY PLAN",
    "trino": "EXPLAIN ANALYZE",
}


class TestExplainStatementsModel:
    def test_session_statements_default_to_none(self) -> None:
        statements = ExplainStatements(statement=_SQL)
        assert statements.session_on is None
        assert statements.session_off is None

    def test_both_session_statements_are_accepted(self) -> None:
        statements = ExplainStatements(statement=_SQL, session_on="SET A ON", session_off="SET A OFF")
        assert (statements.session_on, statements.session_off) == ("SET A ON", "SET A OFF")

    @pytest.mark.parametrize("half", [{"session_on": "SET A ON"}, {"session_off": "SET A OFF"}])
    def test_one_session_statement_alone_is_refused(self, half: dict[str, str]) -> None:
        with pytest.raises(ValidationError):
            ExplainStatements.model_validate({"statement": _SQL, **half})

    def test_frozen(self) -> None:
        statements = ExplainStatements(statement=_SQL)
        with pytest.raises(ValidationError):
            statements.statement = "SELECT 2"  # pyright: ignore[reportAttributeAccessIssue]


class TestExplainStatementsPerDialect:
    @pytest.mark.parametrize(("name", "prefix"), sorted(_SINGLE_STATEMENT.items()))
    def test_single_statement_dialects_put_their_keywords_before_the_query(self, name: str, prefix: str) -> None:
        assert get_dialect(name).explain_statements(_SQL) == ExplainStatements(statement=f"{prefix} {_SQL}")

    def test_every_dialect_is_covered(self) -> None:
        assert set(SQLGLOT_NAMES) == {*_SINGLE_STATEMENT, "tsql", "bigquery"}

    def test_sql_server_runs_the_bare_query_between_showplan_switches(self) -> None:
        assert TsqlDialect().explain_statements(_SQL) == ExplainStatements(
            statement=_SQL, session_on="SET SHOWPLAN_ALL ON", session_off="SET SHOWPLAN_ALL OFF",
        )

    def test_sql_server_has_no_keyword_prefix(self) -> None:
        assert TsqlDialect().explain_prefix is None

    def test_bigquery_refuses(self) -> None:
        dialect = BigqueryDialect()
        with pytest.raises(ExplainUnsupportedError):
            dialect.explain_statements(_SQL)

    def test_the_refusal_is_a_value_error(self) -> None:
        assert issubclass(ExplainUnsupportedError, ValueError)

    def test_the_postfix_field_is_gone(self) -> None:
        assert "explain_postfix" not in SqlDialect.model_fields


class TestExplainSessionHook:
    @pytest.mark.parametrize(("enabled", "text"), [(True, "SET SHOWPLAN_ALL ON"), (False, "SET SHOWPLAN_ALL OFF")])
    def test_sql_server_returns_a_set_statement(self, enabled: bool, text: str) -> None:
        tree = TsqlDialect().explain_session(enabled)
        assert isinstance(tree, exp.Set)
        assert tree.sql(dialect="tsql") == text

    @pytest.mark.parametrize("enabled", [True, False])
    def test_the_default_has_no_session_mode(self, enabled: bool) -> None:
        assert SqlDialect().explain_session(enabled) is None
        assert PostgresDialect().explain_session(enabled) is None
