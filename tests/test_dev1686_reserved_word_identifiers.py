"""SLayer must quote SQL reserved words used as identifiers.

A dataset whose name is a reserved word (``grant``, ``order``, ``user``,
``group``, ``select``, ...) was unqueryable because the query builder derived
the table **alias** from the dataset name and emitted it — plus every column
**qualifier** — UNQUOTED. Postgres (and others) reject the bare reserved word.

Two defect layers, fixed by two mechanisms keyed off one ``SLAYER_RESERVED_KEYWORDS``:

* **AST-emit paths** (base FROM alias + qualifiers, cross-dataset CTEs, physical
  names): ``install_reserved_keywords()`` unions the set into every dialect
  generator's ``RESERVED_KEYWORDS`` so sqlglot's ``identifier_sql`` quotes them.
* **String-concatenation → re-parse paths** (``join_cond``, ``measure.filter_sql``,
  qualified WHERE, first/last ranked subquery): ``prequote_reserved_identifiers``
  token-quotes reserved qualifiers/leaves before the string is re-parsed.
  Plus two explicit ``AS <alias>`` sites (first/last join alias; query-backed
  short) that are not dot-adjacent.

Byte-level emission tests here; live execution is in the Postgres integration
suite.
"""

from __future__ import annotations


import contextlib
import importlib
import sqlite3

import duckdb
import pytest
import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import ParseError, TokenError

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import OrderItem, SlayerQuery, TimeDimension
from slayer.sql.dialects import _ALL_DIALECTS, SQLGLOT_NAMES
from slayer.storage.sqlite_conn import open_connection

# The feature under test.
from slayer.sql.reserved_keywords import (
    DIALECT_RESERVED_KEYWORDS,
    SLAYER_RESERVED_KEYWORDS,
    install_reserved_keywords,
    prequote_reserved_identifiers,
)

from tests._engine_helpers import _engine_generate
from tests._reserved_alias_probe import keyword_universe, unquoted_alias_failures


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return " ".join(s.split())


async def _gen(
    query: SlayerQuery,
    dataset: SlayerDataset,
    *,
    extra_datasets: list[SlayerDataset] | None = None,
    dialect: str = "postgres",
) -> str:
    """Generate SQL for ``query``/``dataset`` through the typed engine pipeline.

    ``extra_datasets`` registers the relationship targets ``dataset`` reaches, so relationship
    resolution has the same storage-backed context the legacy
    ``storage=``-flavoured helper used to provide."""
    return await _engine_generate(
        query=query, dataset=dataset, dialect=dialect, extra_datasets=extra_datasets,
    )


def _assert_parses(sql: str, dialect: str = "postgres") -> None:
    stmts = sqlglot.parse(sql, dialect=dialect)
    assert stmts, f"expected 1 parseable statement:\n{sql}"
    assert len(stmts) == 1, f"expected 1 parseable statement:\n{sql}"


# Dialects whose byte-level emission we assert (Tier-1). Each has a quote char.
_QUOTE_CHAR = {
    "postgres": '"', "sqlite": '"', "duckdb": '"', "snowflake": '"',
    "clickhouse": '"', "mysql": "`", "bigquery": "`", "tsql": "[",
}
_EMIT_DIALECTS = list(_QUOTE_CHAR)


def _q(name: str, dialect: str) -> str:
    return sqlglot.exp.Identifier(this=name, quoted=True).sql(dialect=dialect)


def _grant_dataset(*, with_join: bool = False, extra_cols=()) -> SlayerDataset:
    cols = [
        DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="namespace", sql="namespace", datatype=DataType.STRING),
        DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
        DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
        DatasetField(name="merchantId", sql="merchantId", datatype=DataType.INTEGER),
        *extra_cols,
    ]
    joins = [DatasetRelationship(to="merchant", from_columns=["merchantId"], to_columns=["id"])] if with_join else []
    return SlayerDataset(
        name="grant", sql_table='"Grant"', data_source="api",
        default_time_dimension="created_at", fields=cols, relationships=joins,
    )


def _merchant_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="merchant", sql_table='"Merchant"', data_source="api",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),
        ],
    )


# ---------------------------------------------------------------------------
# 1. Guard test — the mechanism is actually installed (tripwire for sqlglot bumps)
# ---------------------------------------------------------------------------

class TestReservedKeywordsInstalled:
    def test_every_dialect_generator_carries_the_set(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)  # idempotent
        for d in _ALL_DIALECTS:
            gen_cls = Dialect.get_or_raise(d.sqlglot_name).generator_class
            assert SLAYER_RESERVED_KEYWORDS <= set(gen_cls.RESERVED_KEYWORDS), (
                f"{d.sqlglot_name} generator missing reserved words"
            )
            assert "grant" in gen_cls.RESERVED_KEYWORDS

    def test_importing_dialects_package_installs_the_patch(self) -> None:
        # Importing slayer.sql.dialects alone must have run the installer.
        importlib.import_module("slayer.sql.dialects")
        pg = Dialect.get_or_raise("postgres").generator_class
        assert "grant" in pg.RESERVED_KEYWORDS

    def test_set_excludes_typeish_common_column_names(self) -> None:
        # These are non-reserved / type-ish and must NOT be quoted (churn guard).
        for w in ("date", "time", "timestamp", "name", "value", "count",
                  "sum", "id", "text", "number", "status", "revenue", "amount"):
            assert w not in SLAYER_RESERVED_KEYWORDS

    def test_set_includes_reported_and_common_reserved_words(self) -> None:
        for w in ("grant", "order", "user", "group", "select", "table",
                  "from", "where", "join", "having", "distinct",
                  # Completeness (Codex review): words that fail as a
                  # bare alias in Postgres parse must be in the set.
                  "between", "alter", "drop", "insert", "qualify", "xor",
                  "regexp", "revoke", "rollback"):
            assert w in SLAYER_RESERVED_KEYWORDS

    def test_set_covers_all_bare_alias_failures_in_keyword_universe(self) -> None:
        """Regression for the curation gap Codex found (`between`): every token
        in the sqlglot keyword universe that fails as a bare Postgres alias must
        be quoted by our set."""
        universe: set[str] = set()
        for name in ("postgres", "mysql", "duckdb", "bigquery", "redshift",
                     "trino", "tsql", "snowflake"):
            D = Dialect.get_or_raise(name)
            universe |= {k.lower() for k in getattr(D.tokenizer_class, "KEYWORDS", {})}
            universe |= {k.lower() for k in getattr(D.generator_class, "RESERVED_KEYWORDS", set())}
        universe = {w for w in universe if w.isidentifier()}

        def fails_as_alias(w: str) -> bool:
            try:
                sqlglot.parse_one(f"SELECT {w}.x AS a FROM t AS {w}", dialect="postgres")
                return False
            except (ParseError, TokenError):
                return True

        missing = sorted(w for w in universe if fails_as_alias(w) and w not in SLAYER_RESERVED_KEYWORDS)
        assert not missing, f"reserved words missing from SLAYER_RESERVED_KEYWORDS: {missing}"


class TestDialectReservedKeywords:
    @pytest.mark.parametrize("dialect", sorted(DIALECT_RESERVED_KEYWORDS))
    def test_dialect_words_emit_quoted_for_their_dialect(self, dialect: str) -> None:
        bare = [
            w for w in DIALECT_RESERVED_KEYWORDS[dialect]
            if exp.to_identifier(w).sql(dialect=dialect) != _q(w, dialect)
        ]
        assert not bare

    @pytest.mark.parametrize("word", ["sample", "trigger"])
    def test_snowflake_live_probed_words_are_quoted(self, word: str) -> None:
        assert exp.to_identifier(word).sql(dialect="snowflake") == f'"{word}"'

    def test_dialect_words_stay_bare_elsewhere(self) -> None:
        assert exp.to_identifier("index").sql(dialect="postgres") == "index"

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    def test_every_live_alias_failure_is_quoted(self, dialect: str) -> None:
        connection = open_connection(":memory:") if dialect == "sqlite" else contextlib.closing(duckdb.connect())
        with connection as con:
            con.execute("CREATE TABLE t (a INTEGER)")

            def fails(sql: str) -> bool:
                try:
                    con.execute(sql)
                except (sqlite3.Error, duckdb.Error):
                    return True
                return False

            assert unquoted_alias_failures(
                dialect=dialect, words=keyword_universe(), table="t", column="a", fails=fails,
            ) == []


# ---------------------------------------------------------------------------
# 2/3/4/5/6. Byte-level alias + qualifier quoting
# ---------------------------------------------------------------------------

class TestReservedAliasQuoting:
    @pytest.mark.parametrize("dialect", _EMIT_DIALECTS)
    async def test_grant_alias_and_qualifier_quoted(self, dialect: str) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({"population": "grant", "dimensions": ["namespace"], "metrics": ["count(*)"]})
        sql = _norm(await _gen(q, _grant_dataset(), dialect=dialect))
        qg = _q("grant", dialect)
        # FROM "Grant" AS "grant"  (table name already quoted; alias now quoted)
        assert f"AS {qg}" in sql, sql
        # qualifier "grant".namespace
        assert f"{qg}.namespace" in sql or f"{qg}.{_q('namespace', dialect)}" in sql, sql
        _assert_parses(sql, dialect)

    @pytest.mark.parametrize("name", ["order", "user", "group", "select", "table", "between"])
    async def test_other_reserved_model_names(self, name: str) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        model = SlayerDataset(
            name=name, sql_table=f'"{name.capitalize()}"', data_source="api",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="label", sql="label", datatype=DataType.STRING),
            ],
        )
        q = SlayerQuery.model_validate({"population": name, "dimensions": ["label"], "metrics": ["count(*)"]})
        sql = await _gen(q, model, dialect="postgres")
        assert f'AS "{name}"' in _norm(sql), sql
        assert f'"{name}".label' in _norm(sql), sql
        _assert_parses(sql)

    async def test_mixed_case_reserved_name_consistent(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        model = SlayerDataset(
            name="Order", sql_table='"Order"', data_source="api",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="label", sql="label", datatype=DataType.STRING),
            ],
        )
        q = SlayerQuery.model_validate({"population": "Order", "dimensions": ["label"], "metrics": ["count(*)"]})
        sql = _norm(await _gen(q, model, dialect="postgres"))
        # alias and qualifier both quoted identically -> they match
        assert 'AS "Order"' in sql, sql
        assert '"Order".label' in sql, sql
        _assert_parses(sql)

    async def test_non_reserved_names_stay_bare(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        model = SlayerDataset(
            name="orders", sql_table="orders", data_source="api",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        q = SlayerQuery.model_validate({"population": "orders", "dimensions": ["revenue"], "metrics": ["count(*)"]})
        sql = _norm(await _gen(q, model, dialect="postgres"))
        assert "AS orders" in sql, sql
        assert '"orders"' not in sql, sql
        _assert_parses(sql)

    async def test_physical_reserved_word_column_quoted(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        model = SlayerDataset(
            name="events", sql_table="events", data_source="api",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="order", sql="order", datatype=DataType.STRING),  # reserved column
            ],
        )
        q = SlayerQuery.model_validate({"population": "events", "dimensions": ["order"], "metrics": ["count(*)"]})
        sql = _norm(await _gen(q, model, dialect="postgres"))
        assert '"order"' in sql, sql
        _assert_parses(sql)


# ---------------------------------------------------------------------------
# 7-11. String-reparse paths on a reserved base dataset (typed engine pipeline)
# ---------------------------------------------------------------------------

class TestReservedStringReparsePaths:
    async def test_join_from_reserved_model(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["merchant.name"],
            "metrics": ["count(*)"],
        })
        sql = _norm(await _gen(
            q, _grant_dataset(with_join=True), extra_datasets=[_merchant_dataset()],
        ))
        assert 'ON "grant"."merchantId" = merchant.id' in sql, sql
        _assert_parses(sql)

    async def test_first_last_measure_on_reserved_model(self) -> None:
        """The ranked CTE's own scope quotes the reserved relation everywhere it
        names it — the FROM, the ranked value, and the ranking column.

        This used to assert ``"grant".*``: the ranked wrap re-exported the whole
        source relation. It projects a NAMED list now (P-B), so the star is gone
        and each reference is checked directly, which is the stronger claim —
        a star hides whether the individual refs were quoted at all."""
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["namespace"],
            "metrics": [{"metric": "last(amount)"}],
        })
        sql = _norm(await _gen(q, _grant_dataset(), extra_datasets=[_merchant_dataset()]))
        assert 'FROM "Grant" AS "grant"' in sql, sql
        assert '"grant".amount' in sql, sql
        assert 'ORDER BY CASE WHEN "grant".created_at IS NULL THEN 1 ELSE 0 END, "grant".created_at' in sql, sql
        _assert_parses(sql)

    async def test_where_filter_on_reserved_model(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["namespace"],
            "metrics": ["count(*)"],
            "filter": ["amount > 0"],
        })
        sql = _norm(await _gen(q, _grant_dataset(), extra_datasets=[_merchant_dataset()]))
        assert '"grant".amount' in sql, sql
        _assert_parses(sql)

    async def test_order_by_base_column_on_reserved_model(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["namespace"],
            "metrics": ["count(*)"],
            "order": [OrderItem.model_validate({"field": "namespace"})],
        })
        sql = await _gen(q, _grant_dataset(), extra_datasets=[_merchant_dataset()])
        _assert_parses(sql)

    async def test_time_shift_with_filter_on_reserved_model(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({
            "population": "grant",
            "time_dimensions": [TimeDimension(field="created_at", granularity="month")],
            "metrics": [{"metric": "change(sum(amount))"}],
            "filter": ["amount > 0"],
        })
        sql = await _gen(q, _grant_dataset(), extra_datasets=[_merchant_dataset()])
        _assert_parses(sql)

    async def test_where_filter_byte_level(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["namespace"],
            "metrics": ["count(*)"],
            "filter": ["amount > 0"],
        })
        sql = _norm(await _gen(q, _grant_dataset(), extra_datasets=[_merchant_dataset()]))
        assert '"grant".amount > 0' in sql, sql

    async def test_column_level_filter_case_when(self) -> None:
        """A ``DatasetField.filter`` metric emits ``SUM(CASE WHEN ... END)`` whose
        predicate is parsed via the ``measure.filter_sql`` sites — the reserved
        qualifier inside the CASE WHEN must be quoted."""
        install_reserved_keywords(SQLGLOT_NAMES)
        model = _grant_dataset(
            extra_cols=(DatasetField(name="big", sql="amount", datatype=DataType.FLOAT, filter="amount > 100"),),
        )
        q = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["namespace"],
            "metrics": [{"metric": "sum(big)"}],
        })
        sql = _norm(await _gen(q, model))
        assert 'CASE WHEN "grant".amount > 100' in sql, sql
        _assert_parses(sql)

    async def test_having_on_reserved_model(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["namespace"],
            "metrics": [{"metric": "sum(amount)"}],
            "filter": ["sum(amount) > 100"],
        })
        sql = _norm(await _gen(q, _grant_dataset(), extra_datasets=[_merchant_dataset()]))
        assert 'HAVING SUM("grant".amount) > 100' in sql, sql
        _assert_parses(sql)

    async def test_raw_row_distinct_dimension_values_false(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery(
            population="grant", dimensions=["namespace"],
            distinct_dimension_values=False,
        )
        sql = _norm(await _gen(q, _grant_dataset(), extra_datasets=[_merchant_dataset()]))
        assert 'FROM "Grant" AS "grant"' in sql, sql
        assert '"grant".namespace' in sql, sql
        _assert_parses(sql)


# ---------------------------------------------------------------------------
# 13. Reserved JOINED dataset reached from a NON-reserved root
# ---------------------------------------------------------------------------

def _orders_joined_to_grant() -> tuple[SlayerDataset, SlayerDataset]:
    """`orders` (non-reserved root) joined to the reserved `grant` dataset.
    Returns ``(orders, grant)`` — the root first, its relationship target second."""
    grant = SlayerDataset(
        name="grant", sql_table='"Grant"', data_source="api",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
        ],
        default_time_dimension="created_at",
    )
    orders = SlayerDataset(
        name="orders", sql_table="orders", data_source="api",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="grant_id", sql="grant_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
        ],
        default_time_dimension="created_at",
        relationships=[DatasetRelationship(to="grant", from_columns=["grant_id"], to_columns=["id"])],
    )
    return orders, grant


class TestReservedJoinedFromNonReservedRoot:
    async def test_reserved_join_alias_in_projection_and_on(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        orders, grant = _orders_joined_to_grant()
        q = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["grant.status"],
            "metrics": ["count(*)"],
        })
        sql = _norm(await _gen(q, orders, extra_datasets=[grant]))
        assert '"grant".status' in sql, sql
        _assert_parses(sql)

    async def test_where_on_reserved_joined_qualifier(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        orders, grant = _orders_joined_to_grant()
        q = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["grant.status"],
            "metrics": ["count(*)"],
            "filter": ["grant.status == 'active'"],
        })
        sql = _norm(await _gen(q, orders, extra_datasets=[grant]))
        # joined reserved qualifier quoted in BOTH the ON clause and the WHERE
        assert '"grant".status' in sql, sql
        assert "'active'" in sql, sql  # literal survived
        _assert_parses(sql)

    async def test_first_last_referencing_reserved_joined(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        orders, grant = _orders_joined_to_grant()
        q = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["grant.status"],
            "metrics": [{"metric": "last(amount)"}],
        })
        sql = _norm(await _gen(q, orders, extra_datasets=[grant]))
        # the reserved join alias appears quoted in the ranked subquery's join
        assert 'AS "grant"' in sql, sql
        _assert_parses(sql)


# ---------------------------------------------------------------------------
# 14. Token pre-quote helper — literal safety, parse-dialect, edge cases
# ---------------------------------------------------------------------------

class TestPrequoteHelper:
    def test_quotes_qualifier_and_leaf(self) -> None:
        assert prequote_reserved_identifiers("grant.x = y.id", dialect="postgres") == '"grant".x = y.id'
        assert prequote_reserved_identifiers("SELECT grant.* FROM t", dialect="postgres") == 'SELECT "grant".* FROM t'
        assert prequote_reserved_identifiers("orders.grant = 5", dialect="postgres") == 'orders."grant" = 5'

    def test_three_part_middle_reserved(self) -> None:
        assert prequote_reserved_identifiers("catalog.user.col = 1", dialect="postgres") == 'catalog."user".col = 1'

    @pytest.mark.parametrize("literal", [
        "grant.status = 'grant.id'",
        "grant.status = E'grant.id'",
        "grant.status = $$grant.id$$",
        "grant.status = 1 -- grant.id",
        "grant.status = 1 /* grant.id */",
    ])
    def test_literals_and_comments_preserved(self, literal: str) -> None:
        out = prequote_reserved_identifiers(literal, dialect="postgres")
        # the reserved token inside the literal/comment must NOT be quoted
        assert "'grant.id'" in out or "$$grant.id$$" in out or "grant.id" in out.split("--")[-1] or "grant.id */" in out
        # the real qualifier IS quoted
        assert '"grant".status' in out, out

    def test_already_quoted_untouched(self) -> None:
        assert prequote_reserved_identifiers('"grant.id" = 1', dialect="postgres") == '"grant.id" = 1'
        assert prequote_reserved_identifiers('"MixedCol".y = 2', dialect="postgres") == '"MixedCol".y = 2'

    def test_path_alias_untouched(self) -> None:
        s = "customers__regions.name = 'x'"
        assert prequote_reserved_identifiers(s, dialect="postgres") == s

    def test_output_alias_untouched(self) -> None:
        # AS "grant.id" is one quoted identifier token, not word + DOT
        s = 'SELECT x AS "grant.id" FROM t'
        assert prequote_reserved_identifiers(s, dialect="postgres") == s

    def test_quotes_for_parse_dialect_not_target(self) -> None:
        assert prequote_reserved_identifiers("grant.x = 1", dialect="mysql") == "`grant`.x = 1"
        assert prequote_reserved_identifiers("grant.x = 1", dialect="tsql") == "[grant].x = 1"

    @pytest.mark.parametrize("dialect", sorted(DIALECT_RESERVED_KEYWORDS))
    def test_quotes_dialect_words(self, dialect: str) -> None:
        bare = [
            w for w in DIALECT_RESERVED_KEYWORDS[dialect]
            if prequote_reserved_identifiers(f"1 + {w}.a + t.{w}", dialect=dialect)
            != f"1 + {_q(w, dialect)}.a + t.{_q(w, dialect)}"
        ]
        assert not bare

    def test_tokenizer_error_returns_unchanged(self) -> None:
        # An unterminated string literal makes sqlglot.tokenize raise; the helper
        # must swallow it and return the input verbatim (never make a
        # previously-parseable string fail).
        s = "grant.x = 'unterminated"
        assert prequote_reserved_identifiers(s, dialect="postgres") == s


# ---------------------------------------------------------------------------
# 15/16. Query-backed reserved short + derived DatasetField.sql referencing reserved relationship
# ---------------------------------------------------------------------------

def _orders_derived_grant_datasets() -> tuple[SlayerDataset, SlayerDataset]:
    """`orders` (non-reserved root) with a derived field ``bumped = grant.amount
    + 1`` joined to the reserved `grant` dataset. Returns ``(orders, grant)``."""
    grant = SlayerDataset(
        name="grant", sql_table='"Grant"', data_source="api",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
        ],
    )
    orders = SlayerDataset(
        name="orders", sql_table="orders", data_source="api",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="grant_id", sql="grant_id", datatype=DataType.INTEGER),
            DatasetField(name="bumped", sql="grant.amount + 1", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="grant", from_columns=["grant_id"], to_columns=["id"])],
    )
    return orders, grant


def _bumped_query() -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "orders", "dimensions": ["bumped"], "metrics": ["count(*)"]})


class TestReservedInComputedPaths:
    async def test_derived_column_referencing_reserved_joined_model(self) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        orders, grant = _orders_derived_grant_datasets()
        q = _bumped_query()
        sql = _norm(await _gen(q, orders, extra_datasets=[grant]))
        # The reserved joined dataset must be JOINED (not just referenced): a
        # quoted qualifier in the expanded derived-field SQL must still be
        # discovered by relationship-path resolution (Codex review).
        assert 'LEFT JOIN "Grant" AS "grant"' in sql, sql
        assert 'ON orders.grant_id = "grant".id' in sql, sql
        assert '"grant".amount' in sql, sql
        _assert_parses(sql)

    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "bigquery", "tsql", "sqlite", "duckdb"])
    async def test_derived_column_reserved_join_discovered_all_dialects(
        self, dialect: str,
    ) -> None:
        """The reserved qualifier is emitted with the dialect's quote char
        (`` `grant` `` on MySQL/BigQuery, ``[grant]`` on T-SQL); relationship-path
        discovery must still find it on every dialect (Codex review)."""
        install_reserved_keywords(SQLGLOT_NAMES)
        orders, grant = _orders_derived_grant_datasets()
        q = _bumped_query()
        sql = _norm(await _gen(q, orders, extra_datasets=[grant], dialect=dialect))
        assert "JOIN" in sql, f"[{dialect}] join dropped:\n{sql}"
        _assert_parses(sql, dialect)

    @pytest.mark.parametrize(("dialect", "word"), [("duckdb", "pivot"), ("sqlite", "index"), ("bigquery", "current_datetime")])
    async def test_derived_column_dialect_reserved_join_discovered(self, dialect: str, word: str) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        orders, grant = _orders_derived_grant_datasets()
        target = grant.model_copy(update={"name": word})
        orders = orders.model_copy(update={
            "fields": [c.model_copy(update={"sql": f"{word}.amount + 1"}) if c.name == "bumped" else c
                        for c in orders.fields],
            "relationships": [DatasetRelationship(to=word, from_columns=["grant_id"], to_columns=["id"])],
        })
        q = _bumped_query()
        sql = _norm(await _gen(q, orders, extra_datasets=[target], dialect=dialect))
        assert f"{_q(word, dialect)}.amount + 1" in sql, sql
        _assert_parses(sql, dialect)


# ---------------------------------------------------------------------------
# 17. Assemble-and-parse smoke across the query shapes on a reserved dataset
# ---------------------------------------------------------------------------

class TestGeneratedSqlAlwaysParses:
    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb", "mysql", "tsql"])
    async def test_standalone_reserved_parses_all_dialects(self, dialect: str) -> None:
        install_reserved_keywords(SQLGLOT_NAMES)
        q = SlayerQuery.model_validate({"population": "grant", "dimensions": ["namespace"], "metrics": ["count(*)"]})
        sql = await _gen(q, _grant_dataset(), dialect=dialect)
        _assert_parses(sql, dialect)
