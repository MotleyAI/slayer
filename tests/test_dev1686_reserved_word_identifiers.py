"""DEV-1686: SLayer must quote SQL reserved words used as identifiers.

A model whose name is a reserved word (``grant``, ``order``, ``user``,
``group``, ``select``, ...) was unqueryable because the query builder derived
the table **alias** from the model name and emitted it — plus every column
**qualifier** — UNQUOTED. Postgres (and others) reject the bare reserved word.

Two defect layers, fixed by two mechanisms keyed off one ``SLAYER_RESERVED_KEYWORDS``:

* **AST-emit paths** (base FROM alias + qualifiers, cross-model CTEs, physical
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


import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import OrderItem, SlayerQuery, TimeDimension
from slayer.engine.enrichment import enrich_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import _ALL_DIALECTS
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage

# The feature under test.
from slayer.sql.reserved_keywords import (
    SLAYER_RESERVED_KEYWORDS,
    install_reserved_keywords,
    prequote_reserved_identifiers,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return " ".join(s.split())


async def _noop_async(**kw):  # NOSONAR(S7503) — resolver-callback contract is async
    return None


async def _generate(query: SlayerQuery, model: SlayerModel, *, dialect: str = "postgres") -> str:
    enriched = await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
    )
    return SQLGenerator(dialect=dialect).generate(enriched=enriched)


async def _generate_via_engine(
    query: SlayerQuery, model: SlayerModel, storage: YAMLStorage, *, dialect: str = "postgres"
) -> str:
    """Enrich through a real engine+storage so joins resolve from storage."""
    engine = SlayerQueryEngine(storage=storage)
    enriched = await engine._enrich(query=query, model=model)
    return SQLGenerator(dialect=dialect).generate(enriched=enriched)


def _assert_parses(sql: str, dialect: str = "postgres") -> None:
    stmts = sqlglot.parse(sql, dialect=dialect)
    assert stmts and len(stmts) == 1, f"expected 1 parseable statement:\n{sql}"


# Dialects whose byte-level emission we assert (Tier-1). Each has a quote char.
_QUOTE_CHAR = {
    "postgres": '"', "sqlite": '"', "duckdb": '"', "snowflake": '"',
    "clickhouse": '"', "mysql": "`", "bigquery": "`", "tsql": "[",
}
_EMIT_DIALECTS = list(_QUOTE_CHAR)


def _q(name: str, dialect: str) -> str:
    return sqlglot.exp.Identifier(this=name, quoted=True).sql(dialect=dialect)


def _grant_model(*, with_join: bool = False, extra_cols=()) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.INT, primary_key=True),
        Column(name="namespace", sql="namespace", type=DataType.TEXT),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
        Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        Column(name="merchantId", sql="merchantId", type=DataType.INT),
        *extra_cols,
    ]
    joins = [ModelJoin(target_model="merchant", join_pairs=[["merchantId", "id"]])] if with_join else []
    return SlayerModel(
        name="grant", sql_table='"Grant"', data_source="api",
        default_time_dimension="created_at", columns=cols, joins=joins,
    )


def _merchant_model() -> SlayerModel:
    return SlayerModel(
        name="merchant", sql_table='"Merchant"', data_source="api",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    )


# ---------------------------------------------------------------------------
# 1. Guard test — the mechanism is actually installed (tripwire for sqlglot bumps)
# ---------------------------------------------------------------------------

class TestReservedKeywordsInstalled:
    def test_every_dialect_generator_carries_the_set(self) -> None:
        from sqlglot.dialects.dialect import Dialect
        install_reserved_keywords()  # idempotent
        for d in _ALL_DIALECTS:
            gen_cls = Dialect.get_or_raise(d.sqlglot_name).generator_class
            assert SLAYER_RESERVED_KEYWORDS <= set(gen_cls.RESERVED_KEYWORDS), (
                f"{d.sqlglot_name} generator missing reserved words"
            )
            assert "grant" in gen_cls.RESERVED_KEYWORDS

    def test_importing_dialects_package_installs_the_patch(self) -> None:
        # Importing slayer.sql.dialects alone must have run the installer.
        import importlib
        from sqlglot.dialects.dialect import Dialect
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
                  # DEV-1686 completeness (Codex review): words that fail as a
                  # bare alias in Postgres parse must be in the set.
                  "between", "alter", "drop", "insert", "qualify", "xor",
                  "regexp", "revoke", "rollback"):
            assert w in SLAYER_RESERVED_KEYWORDS

    def test_set_covers_all_bare_alias_failures_in_keyword_universe(self) -> None:
        """Regression for the curation gap Codex found (`between`): every token
        in the sqlglot keyword universe that fails as a bare Postgres alias must
        be quoted by our set."""
        from sqlglot.dialects.dialect import Dialect
        from sqlglot.errors import ParseError, TokenError

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


# ---------------------------------------------------------------------------
# 2/3/4/5/6. Byte-level alias + qualifier quoting
# ---------------------------------------------------------------------------

class TestReservedAliasQuoting:
    @pytest.mark.parametrize("dialect", _EMIT_DIALECTS)
    async def test_grant_alias_and_qualifier_quoted(self, dialect: str) -> None:
        install_reserved_keywords()
        q = SlayerQuery(source_model="grant", dimensions=["namespace"], measures=["*:count"])
        sql = _norm(await _generate(q, _grant_model(), dialect=dialect))
        qg = _q("grant", dialect)
        # FROM "Grant" AS "grant"  (table name already quoted; alias now quoted)
        assert f"AS {qg}" in sql, sql
        # qualifier "grant".namespace
        assert f"{qg}.namespace" in sql or f"{qg}.{_q('namespace', dialect)}" in sql, sql
        _assert_parses(sql, dialect)

    @pytest.mark.parametrize("name", ["order", "user", "group", "select", "table", "between"])
    async def test_other_reserved_model_names(self, name: str) -> None:
        install_reserved_keywords()
        model = SlayerModel(
            name=name, sql_table=f'"{name.capitalize()}"', data_source="api",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="label", sql="label", type=DataType.TEXT),
            ],
        )
        q = SlayerQuery(source_model=name, dimensions=["label"], measures=["*:count"])
        sql = await _generate(q, model, dialect="postgres")
        assert f'AS "{name}"' in _norm(sql), sql
        assert f'"{name}".label' in _norm(sql), sql
        _assert_parses(sql)

    async def test_mixed_case_reserved_name_consistent(self) -> None:
        install_reserved_keywords()
        model = SlayerModel(
            name="Order", sql_table='"Order"', data_source="api",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="label", sql="label", type=DataType.TEXT),
            ],
        )
        q = SlayerQuery(source_model="Order", dimensions=["label"], measures=["*:count"])
        sql = _norm(await _generate(q, model, dialect="postgres"))
        # alias and qualifier both quoted identically -> they match
        assert 'AS "Order"' in sql, sql
        assert '"Order".label' in sql, sql
        _assert_parses(sql)

    async def test_non_reserved_names_stay_bare(self) -> None:
        install_reserved_keywords()
        model = SlayerModel(
            name="orders", sql_table="orders", data_source="api",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        q = SlayerQuery(source_model="orders", dimensions=["revenue"], measures=["*:count"])
        sql = _norm(await _generate(q, model, dialect="postgres"))
        assert "AS orders" in sql and '"orders"' not in sql, sql
        _assert_parses(sql)

    async def test_physical_reserved_word_column_quoted(self) -> None:
        install_reserved_keywords()
        model = SlayerModel(
            name="events", sql_table="events", data_source="api",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="order", sql="order", type=DataType.TEXT),  # reserved column
            ],
        )
        q = SlayerQuery(source_model="events", dimensions=["order"], measures=["*:count"])
        sql = _norm(await _generate(q, model, dialect="postgres"))
        assert '"order"' in sql, sql
        _assert_parses(sql)


# ---------------------------------------------------------------------------
# 7-11. String-reparse paths on a reserved base model (engine+storage)
# ---------------------------------------------------------------------------

class TestReservedStringReparsePaths:
    async def _storage(self, tmp_path, *, with_join=True):
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_model(_merchant_model())
        await storage.save_model(_grant_model(with_join=with_join))
        return storage

    async def test_join_from_reserved_model(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = await self._storage(tmp_path)
        q = SlayerQuery(source_model="grant", dimensions=["merchant.name"], measures=["*:count"])
        sql = _norm(await _generate_via_engine(q, _grant_model(with_join=True), storage))
        assert 'ON "grant"."merchantId" = merchant.id' in sql, sql
        _assert_parses(sql)

    async def test_first_last_measure_on_reserved_model(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = await self._storage(tmp_path, with_join=False)
        q = SlayerQuery(
            source_model="grant", dimensions=["namespace"],
            measures=[{"formula": "amount:last"}],
        )
        sql = _norm(await _generate_via_engine(q, _grant_model(), storage))
        assert '"grant".*' in sql, sql
        _assert_parses(sql)

    async def test_where_filter_on_reserved_model(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = await self._storage(tmp_path, with_join=False)
        q = SlayerQuery(
            source_model="grant", dimensions=["namespace"],
            measures=["*:count"], filters=["amount > 0"],
        )
        sql = _norm(await _generate_via_engine(q, _grant_model(), storage))
        assert '"grant".amount' in sql, sql
        _assert_parses(sql)

    async def test_order_by_base_column_on_reserved_model(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = await self._storage(tmp_path, with_join=False)
        q = SlayerQuery(
            source_model="grant", dimensions=["namespace"],
            measures=["*:count"], order=[OrderItem(column="namespace")],
        )
        sql = await _generate_via_engine(q, _grant_model(), storage)
        _assert_parses(sql)

    async def test_time_shift_with_filter_on_reserved_model(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = await self._storage(tmp_path, with_join=False)
        q = SlayerQuery(
            source_model="grant",
            time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
            measures=[{"formula": "change(amount:sum)"}],
            filters=["amount > 0"],
        )
        sql = await _generate_via_engine(q, _grant_model(), storage)
        _assert_parses(sql)

    async def test_where_filter_byte_level(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = await self._storage(tmp_path, with_join=False)
        q = SlayerQuery(
            source_model="grant", dimensions=["namespace"],
            measures=["*:count"], filters=["amount > 0"],
        )
        sql = _norm(await _generate_via_engine(q, _grant_model(), storage))
        assert '"grant".amount > 0' in sql, sql

    async def test_column_level_filter_case_when(self, tmp_path) -> None:
        """A ``Column.filter`` measure emits ``SUM(CASE WHEN ... END)`` whose
        predicate is parsed via the ``measure.filter_sql`` sites — the reserved
        qualifier inside the CASE WHEN must be quoted."""
        install_reserved_keywords()
        model = _grant_model(
            extra_cols=(Column(name="big", sql="amount", type=DataType.DOUBLE, filter="amount > 100"),),
        )
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_model(model)
        q = SlayerQuery(
            source_model="grant", dimensions=["namespace"],
            measures=[{"formula": "big:sum"}],
        )
        sql = _norm(await _generate_via_engine(q, model, storage))
        assert 'CASE WHEN "grant".amount > 100' in sql, sql
        _assert_parses(sql)

    async def test_having_on_reserved_model(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = await self._storage(tmp_path, with_join=False)
        q = SlayerQuery(
            source_model="grant", dimensions=["namespace"],
            measures=[{"formula": "amount:sum"}], filters=["amount:sum > 100"],
        )
        sql = _norm(await _generate_via_engine(q, _grant_model(), storage))
        assert 'HAVING SUM("grant".amount) > 100' in sql, sql
        _assert_parses(sql)

    async def test_raw_row_distinct_dimension_values_false(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = await self._storage(tmp_path, with_join=False)
        q = SlayerQuery(
            source_model="grant", dimensions=["namespace"],
            distinct_dimension_values=False,
        )
        sql = _norm(await _generate_via_engine(q, _grant_model(), storage))
        assert 'FROM "Grant" AS "grant"' in sql, sql
        assert '"grant".namespace' in sql, sql
        _assert_parses(sql)


# ---------------------------------------------------------------------------
# 13. Reserved JOINED model reached from a NON-reserved root
# ---------------------------------------------------------------------------

class TestReservedJoinedFromNonReservedRoot:
    async def _storage(self, tmp_path):
        storage = YAMLStorage(base_dir=str(tmp_path))
        grant = SlayerModel(
            name="grant", sql_table='"Grant"', data_source="api",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            default_time_dimension="created_at",
        )
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="api",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="grant_id", sql="grant_id", type=DataType.INT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
            default_time_dimension="created_at",
            joins=[ModelJoin(target_model="grant", join_pairs=[["grant_id", "id"]])],
        )
        await storage.save_model(grant)
        await storage.save_model(orders)
        return storage, orders

    async def test_reserved_join_alias_in_projection_and_on(self, tmp_path) -> None:
        install_reserved_keywords()
        storage, orders = await self._storage(tmp_path)
        q = SlayerQuery(source_model="orders", dimensions=["grant.status"], measures=["*:count"])
        sql = _norm(await _generate_via_engine(q, orders, storage))
        assert '"grant".status' in sql, sql
        _assert_parses(sql)

    async def test_where_on_reserved_joined_qualifier(self, tmp_path) -> None:
        install_reserved_keywords()
        storage, orders = await self._storage(tmp_path)
        q = SlayerQuery(
            source_model="orders", dimensions=["grant.status"],
            measures=["*:count"], filters=["grant.status == 'active'"],
        )
        sql = _norm(await _generate_via_engine(q, orders, storage))
        # joined reserved qualifier quoted in BOTH the ON clause and the WHERE
        assert '"grant".status' in sql, sql
        assert "'active'" in sql, sql  # literal survived
        _assert_parses(sql)

    async def test_first_last_referencing_reserved_joined(self, tmp_path) -> None:
        install_reserved_keywords()
        storage, orders = await self._storage(tmp_path)
        q = SlayerQuery(
            source_model="orders", dimensions=["grant.status"],
            measures=[{"formula": "amount:last"}],
        )
        sql = _norm(await _generate_via_engine(q, orders, storage))
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

    def test_tokenizer_error_returns_unchanged(self) -> None:
        # An unterminated string literal makes sqlglot.tokenize raise; the helper
        # must swallow it and return the input verbatim (never make a
        # previously-parseable string fail).
        s = "grant.x = 'unterminated"
        assert prequote_reserved_identifiers(s, dialect="postgres") == s


# ---------------------------------------------------------------------------
# 15/16. Query-backed reserved short + derived Column.sql referencing reserved join
# ---------------------------------------------------------------------------

class TestReservedInComputedPaths:
    async def test_derived_column_referencing_reserved_joined_model(self, tmp_path) -> None:
        install_reserved_keywords()
        storage = YAMLStorage(base_dir=str(tmp_path))
        grant = SlayerModel(
            name="grant", sql_table='"Grant"', data_source="api",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="api",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="grant_id", sql="grant_id", type=DataType.INT),
                # derived column referencing the reserved joined model
                Column(name="bumped", sql="grant.amount + 1", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="grant", join_pairs=[["grant_id", "id"]])],
        )
        await storage.save_model(grant)
        await storage.save_model(orders)
        q = SlayerQuery(source_model="orders", dimensions=["bumped"], measures=["*:count"])
        sql = await _generate_via_engine(q, orders, storage)
        _assert_parses(sql)


# ---------------------------------------------------------------------------
# 17. Assemble-and-parse smoke across the query shapes on a reserved model
# ---------------------------------------------------------------------------

class TestGeneratedSqlAlwaysParses:
    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb", "mysql", "tsql"])
    async def test_standalone_reserved_parses_all_dialects(self, dialect: str) -> None:
        install_reserved_keywords()
        q = SlayerQuery(source_model="grant", dimensions=["namespace"], measures=["*:count"])
        sql = await _generate(q, _grant_model(), dialect=dialect)
        _assert_parses(sql, dialect)
