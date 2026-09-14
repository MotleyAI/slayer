"""Fit over-limit INTERNAL identifiers at emission.

The prior projection-alias pass fits only the plan-derived projection aliases;
internal CTE column aliases (canonical aggregate names, up to 305 bytes in ``lift/nested_attach``)
reach Postgres unfitted and are silently truncated at 63 bytes — two aliases
sharing their first 63 bytes collapse. The emission pass must scan the
assembled SQL for over-limit quoted identifiers (literals/comments masked),
fit each through the pure ``fit_identifier``, exempt user-authored
identifiers, and fail closed on anything unaccounted.
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import TypeVar

import pytest
import sqlglot
from sqlglot import exp

import slayer.sql._identifier_fit as fitmod
import slayer.sql.dialects as dialects_registry
from slayer.core.enums import DataType
from slayer.core.errors import IdentifierCollisionError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, ModelExtension, ModelMeasure, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql._identifier_fit import SqlLexis, fit_identifier, substitute_quoted
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.naming import encode_alias
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1739_fixtures import _engine_for, _seed_duckdb, _seed_sqlite, gen
from tests._dev1824_fixtures import BAND35, dev1824_models, q
from tests.test_dev1824_golden_sql import GOLDEN_PATH, _cases

# Dev-1891 machinery — absent until the implementation lands (collapse to plain
# imports then); guarded so the file collects and tests fail via _need instead.
try:
    from slayer.core.errors import IdentifierLengthError  # pyright: ignore[reportAttributeAccessIssue]
except ImportError:
    IdentifierLengthError = None
try:
    from slayer.sql._identifier_fit import find_overlimit_quoted  # pyright: ignore[reportAttributeAccessIssue]
except ImportError:
    find_overlimit_quoted = None
try:
    from slayer.sql._identifier_fit import overlimit_tokens  # pyright: ignore[reportAttributeAccessIssue]
except ImportError:
    overlimit_tokens = None

_T = TypeVar("_T")


def _need(feature: _T | None) -> _T:
    assert feature is not None, "dev-1891 identifier-fitting machinery not implemented yet"
    return feature


PG_LIMIT = 63
DS = "test"

# A single over-limit SLayer-shaped alias for the string-level unit tests.
LONG = "amount_sum_partition_by_" + "verbose_user_column_" * 3 + "region_city"  # 95 bytes
FIT_LONG = fit_identifier(name=LONG, limit=PG_LIMIT)

# Two over-limit names sharing head and tail within the 63-byte fit budget, so a
# forced-constant digest makes them fit to one identical form.
_TWIN_HEAD = "shared_head_" + "h" * 28
_TWIN_TAIL = "t" * 28 + "_shared_tail"
TWIN_A = f"{_TWIN_HEAD}_111_{_TWIN_TAIL}"
TWIN_B = f"{_TWIN_HEAD}_222_{_TWIN_TAIL}"

# Over-limit user-authored physical identifiers for the exemption tests.
PHYS_COL = "physical_metric_column_" + "x" * 56  # 79 bytes
PHYS_TBL = "physical_table_" + "y" * 60          # 75 bytes
LONG_COL_NAME = "user_metric_" + "z" * 60        # 72 bytes, a user COLUMN NAME

# The 63-byte-prefix collision pair: internal producer aliases of the two banded
# computed dimensions below; identical up to FORCED_LIMIT bytes.
CANON_REGION = "amount_sum_partition_by_region"
CANON_CITY = "amount_sum_partition_by_city"
FORCED_LIMIT = 24
RBAND = "CASE WHEN amount:sum(partition_by=region) > 55 THEN 1 ELSE 0 END"
COLLIDE_QUERY = q(
    dimensions=[
        "region",
        {"expression": RBAND, "name": "rband"},
        {"expression": BAND35, "name": "cband"},
    ],
    measures=[ModelMeasure(formula="amount:sum", name="s")],
)

_GOLDEN_CASES = _cases()
NESTED = _GOLDEN_CASES["lift/nested_attach"]
WINDOW_PARTITION = _GOLDEN_CASES["lift/window_partition"]


def _nbytes(s: str) -> int:
    return len(s.encode("utf-8"))


def _identifiers(sql: str, dialect: str) -> list[str]:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    return [n.this for n in tree.find_all(exp.Identifier) if isinstance(n.this, str)]


def _golden(key: str) -> str:
    return json.loads(GOLDEN_PATH.read_text())[key]


async def _canonical_internal_aliases() -> list[str]:
    """nested_attach's over-limit internal QUOTED aliases, read off the unbounded
    SQLite emission (canonical naming is dialect-independent). Excludes the bare
    CTE names — those are fitted at mint and out of dev-1891's scope."""
    sql = await gen(NESTED, dialect="sqlite")
    tree = sqlglot.parse_one(sql, dialect="sqlite")
    names = sorted({
        n.this for n in tree.find_all(exp.Identifier)
        if n.quoted and isinstance(n.this, str) and _nbytes(n.this) > PG_LIMIT
    })
    assert len(names) == 2, names  # the bare and the ``orders.``-prefixed alias
    return names


async def _dry_run(models: list[SlayerModel], query, *, dialect: str = "postgres"):
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name=DS, type=dialect))
        for m in models:
            await storage.save_model(m, _validate=False)
        resp = await SlayerQueryEngine(storage=storage).execute(query, dry_run=True)
        assert resp.sql is not None
        return resp


async def _dry_sql(models: list[SlayerModel], query, *, dialect: str = "postgres") -> str:
    resp = await _dry_run(models, query, dialect=dialect)
    assert resp.sql is not None
    return resp.sql


def _pg_rewrite(sql: str, **kw) -> str:
    return get_dialect("postgres").rewrite_emitted_sql(sql, **kw)


def _force_limit(monkeypatch: pytest.MonkeyPatch, name: str, limit: int) -> SqlDialect:
    patched = get_dialect(name).model_copy(update={"max_identifier_bytes": limit})
    monkeypatch.setitem(dialects_registry._BY_SQLGLOT_NAME, name, patched)
    for alias in patched.ds_type_aliases:
        monkeypatch.setitem(dialects_registry._BY_DS_TYPE, alias, patched)
    return patched


# The premise: the repro aliases really are over the limits, and the collision
# pair really shares its first FORCED_LIMIT bytes.


class TestPremise:
    async def test_nested_attach_aliases_exceed_every_bounded_tier1_limit(self) -> None:
        names = await _canonical_internal_aliases()
        prefixed = [n for n in names if n.startswith("orders.")]
        bare = [n for n in names if not n.startswith("orders.")]
        assert len(prefixed) == 1, names
        assert len(bare) == 1, names
        assert all(_nbytes(n) > 256 for n in names)              # duckdb-relevant
        assert _nbytes(bare[0]) > 128                            # tsql-relevant
        assert _nbytes(encode_alias(prefixed[0])) > 300          # bigquery post-mangle
        assert _nbytes(encode_alias(bare[0])) <= 300             # bigquery under budget

    def test_collision_pair_shares_its_first_limit_bytes(self) -> None:
        assert _nbytes(CANON_REGION) > FORCED_LIMIT
        assert _nbytes(CANON_CITY) > FORCED_LIMIT
        assert CANON_REGION.encode()[:FORCED_LIMIT] == CANON_CITY.encode()[:FORCED_LIMIT]


# Repro: nested_attach on bounded dialects (delta spec requirement 1, scenario 1).


class TestNestedAttachEmission:
    @pytest.mark.parametrize("dialect,limit", [("postgres", 63), ("duckdb", 256)])
    async def test_no_over_limit_identifier(self, dialect: str, limit: int) -> None:
        sql = await gen(NESTED, dialect=dialect)
        over = [n for n in _identifiers(sql, dialect) if _nbytes(n) > limit]
        assert not over, f"{over}\n{sql}"

    @pytest.mark.parametrize("dialect,limit", [("postgres", 63), ("duckdb", 256)])
    async def test_fitted_alias_at_definition_and_every_reference(
        self, dialect: str, limit: int,
    ) -> None:
        d = get_dialect(dialect)
        names = [n for n in await _canonical_internal_aliases() if _nbytes(n) > limit]
        assert names, "no alias exceeds this dialect's limit; test is vacuous"
        sql = await gen(NESTED, dialect=dialect)
        for name in names:
            assert name not in sql
            fitted = d.quote_identifier(fit_identifier(name=name, limit=limit))
            assert sql.count(fitted) >= 2, f"{fitted} not at definition+reference\n{sql}"

    async def test_repro_still_parses(self) -> None:
        sql = await gen(NESTED, dialect="postgres")
        assert len(sqlglot.parse(sql, dialect="postgres")) == 1

    async def test_query_backed_model_backing_sql_stays_fit_eligible(self) -> None:
        """SLayer-generated backing SQL is not a user surface: over-limit
        aliases inside an expanded query-backed model are fitted, not exempted."""
        names = await _canonical_internal_aliases()
        qb = SlayerModel(name="qb", source_queries=[NESTED], data_source=DS)
        query = SlayerQuery(
            source_model="qb",
            dimensions=[ColumnRef(name="band")],
            measures=[ModelMeasure(formula="s:sum", name="t")],
        )
        sql = await _dry_sql([*dev1824_models(), qb], query)
        over = [n for n in _identifiers(sql, "postgres") if _nbytes(n) > PG_LIMIT]
        assert not over, f"{over}\n{sql}"
        for name in names:
            assert name not in sql
            assert fit_identifier(name=name, limit=PG_LIMIT) in sql


# No churn (requirement 1, scenarios 2+3): under-limit queries byte-identical,
# unbounded dialects untouched — pinned against the blessed goldens.


class TestNoChurn:
    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb", "tsql", "bigquery"])
    async def test_under_limit_query_byte_identical(self, dialect: str) -> None:
        sql = await gen(WINDOW_PARTITION, dialect=dialect)
        assert sql == _golden(f"lift/window_partition::{dialect}")

    async def test_unbounded_sqlite_keeps_over_limit_aliases_byte_identical(self) -> None:
        sql = await gen(NESTED, dialect="sqlite")
        assert sql == _golden("lift/nested_attach::sqlite")

    async def test_unbounded_clickhouse_keeps_canonical_aliases(self) -> None:
        names = await _canonical_internal_aliases()
        sql = await gen(NESTED, dialect="clickhouse")
        for name in names:
            assert name in sql


# Distinct names stay distinct (requirement 2, scenario 1): forced low limit on
# an executing backend; the pair collides under plain truncation.


class TestForcedLimitExecution:
    @pytest.fixture(params=["sqlite", "duckdb"])
    def exec_dialect(self, request) -> str:
        if request.param == "duckdb":
            pytest.importorskip("duckdb")
        return request.param

    @staticmethod
    def _grouped(resp) -> dict:
        return {
            (r["orders.region"], r["orders.rband"], r["orders.cband"]): r["orders.s"]
            for r in resp.data
        }

    async def test_prefix_colliding_aliases_fit_distinct_and_results_match(
        self, exec_dialect: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db_path = str(tmp_path / f"data.{exec_dialect}")
        (_seed_sqlite if exec_dialect == "sqlite" else _seed_duckdb)(db_path)

        engine_free = await _engine_for(dialect=exec_dialect, db_path=db_path)
        sql_free = (await engine_free.execute(COLLIDE_QUERY, dry_run=True)).sql
        assert sql_free is not None
        quote = get_dialect(exec_dialect).quote_identifier
        assert quote(CANON_REGION) in sql_free, sql_free  # fixture still mints the pair
        assert quote(CANON_CITY) in sql_free, sql_free
        rows_free = self._grouped(await engine_free.execute(COLLIDE_QUERY))
        assert len(rows_free) == 4

        _force_limit(monkeypatch, exec_dialect, FORCED_LIMIT)
        engine_fit = await _engine_for(dialect=exec_dialect, db_path=db_path)
        sql_fit = (await engine_fit.execute(COLLIDE_QUERY, dry_run=True)).sql
        assert sql_fit is not None
        for canon in (CANON_REGION, CANON_CITY):
            assert quote(canon) not in sql_fit, sql_fit
        fit_region = fit_identifier(name=CANON_REGION, limit=FORCED_LIMIT)
        fit_city = fit_identifier(name=CANON_CITY, limit=FORCED_LIMIT)
        assert fit_region != fit_city
        assert fit_region.encode()[:FORCED_LIMIT] != fit_city.encode()[:FORCED_LIMIT]
        assert quote(fit_region) in sql_fit, sql_fit
        assert quote(fit_city) in sql_fit, sql_fit
        over = [n for n in _identifiers(sql_fit, exec_dialect) if _nbytes(n) > FORCED_LIMIT]
        assert not over, f"{over}\n{sql_fit}"

        assert self._grouped(await engine_fit.execute(COLLIDE_QUERY)) == rows_free


# User-authored identifiers pass through byte-identical (requirement 3), one
# test per extraction surface. Generation succeeding at all also pins that the
# backstop treats these tokens as exempt.


class TestUserSurfacesExempt:
    @staticmethod
    def _orders(**kw) -> SlayerModel:
        cols = [
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            *kw.pop("extra_columns", []),
        ]
        return SlayerModel(
            name=kw.pop("name", "orders"), data_source=DS,
            sql_table=kw.pop("sql_table", "orders"), columns=cols, **kw,
        )

    @staticmethod
    def _query(measure: str) -> SlayerQuery:
        return SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="region")],
            measures=[ModelMeasure(formula=measure, name="w")],
        )

    async def test_column_sql_survives(self) -> None:
        model = self._orders(extra_columns=[
            Column(name="wide", sql=f'"{PHYS_COL}"', type=DataType.DOUBLE),
        ])
        sql = await _dry_sql([model], self._query("wide:sum"))
        assert f'"{PHYS_COL}"' in sql

    async def test_column_filter_survives(self) -> None:
        model = self._orders(extra_columns=[
            Column(name="okwide", sql="amount", filter=f'"{PHYS_COL}" > 0', type=DataType.DOUBLE),
        ])
        sql = await _dry_sql([model], self._query("okwide:sum"))
        assert f'"{PHYS_COL}"' in sql

    async def test_model_filters_survive(self) -> None:
        model = self._orders(filters=[f'"{PHYS_COL}" IS NOT NULL'])
        sql = await _dry_sql([model], self._query("amount:sum"))
        assert f'"{PHYS_COL}"' in sql

    async def test_sql_table_survives(self) -> None:
        model = self._orders(name="wtab", sql_table=PHYS_TBL)
        query = SlayerQuery(
            source_model="wtab",
            dimensions=[ColumnRef(name="region")],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        sql = await _dry_sql([model], query)
        assert PHYS_TBL in sql

    async def test_extension_added_column_survives(self) -> None:
        ext = ModelExtension(
            source_name="orders",
            columns=[Column(name="extwide", sql=f'"{PHYS_COL}"', type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model=ext,
            dimensions=[ColumnRef(name="region")],
            measures=[ModelMeasure(formula="extwide:sum", name="w")],
        )
        sql = await _dry_sql([self._orders()], query)
        assert f'"{PHYS_COL}"' in sql

    async def test_non_root_named_stage_model_survives(self) -> None:
        wide_src = self._orders(name="wide_src", extra_columns=[
            Column(name="wide", sql=f'"{PHYS_COL}"', type=DataType.DOUBLE),
        ])
        inner = SlayerQuery(
            name="s1", source_model="wide_src",
            dimensions=[ColumnRef(name="region")],
            measures=[ModelMeasure(formula="wide:sum", name="w")],
        )
        outer = SlayerQuery(
            source_model="s1",
            dimensions=[ColumnRef(name="region")],
            measures=[ModelMeasure(formula="w:sum", name="tw")],
        )
        sql = await _dry_sql([wide_src], [inner, outer])
        assert f'"{PHYS_COL}"' in sql

    async def test_model_sql_survives(self) -> None:
        model = SlayerModel(
            name="sqlm", data_source=DS,
            sql=f'SELECT id, region, amount, "{PHYS_COL}" AS wide FROM base_orders',
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="wide", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="sqlm",
            dimensions=[ColumnRef(name="region")],
            measures=[ModelMeasure(formula="wide:sum", name="w")],
        )
        sql = await _dry_sql([model], query)
        assert f'"{PHYS_COL}"' in sql

    async def test_inline_model_survives(self) -> None:
        inline = self._orders(name="inline_orders", extra_columns=[
            Column(name="wide", sql=f'"{PHYS_COL}"', type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model=inline,
            dimensions=[ColumnRef(name="region")],
            measures=[ModelMeasure(formula="wide:sum", name="w")],
        )
        sql = await _dry_sql([], query)
        assert f'"{PHYS_COL}"' in sql

    async def test_referenced_model_column_survives(self) -> None:
        dims = SlayerModel(
            name="dims", data_source=DS, sql_table="dims",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="wide", sql=f'"{PHYS_COL}"', type=DataType.DOUBLE),
            ],
        )
        host = self._orders(
            extra_columns=[Column(name="dim_id", type=DataType.INT)],
            joins=[ModelJoin(target_model="dims", join_pairs=[["dim_id", "id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="region")],
            measures=[ModelMeasure(formula="dims.wide:sum", name="w")],
        )
        sql = await _dry_sql([host, dims], query)
        assert f'"{PHYS_COL}"' in sql

    async def test_column_name_is_exempt_but_its_projection_alias_is_fitted(self) -> None:
        """The bare user column NAME passes through; the SLayer-minted dotted
        alias built FROM it is a different spelling and stays fitted."""
        model = self._orders(extra_columns=[
            Column(name=LONG_COL_NAME, type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name=LONG_COL_NAME)],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        sql = await _dry_sql([model], query)
        assert LONG_COL_NAME in sql
        assert f'"orders.{LONG_COL_NAME}"' not in sql


# The emission scan itself, unit-level on the base rewrite hook.


class TestEmissionScan:
    def test_scan_fits_definition_and_references_without_an_alias_map(self) -> None:
        sql = f'SELECT t.x AS "{LONG}", t.y AS "short"\nFROM t\nGROUP BY\n  "{LONG}"'
        out = _pg_rewrite(sql)
        assert LONG not in out
        assert out.count(f'"{FIT_LONG}"') == 2, out
        assert '"short"' in out

    def test_under_limit_sql_is_byte_identical(self) -> None:
        sql = 'SELECT a AS "x", b AS "y" FROM t'
        assert _pg_rewrite(sql) == sql

    def test_unbounded_dialect_is_identity(self) -> None:
        sql = f'SELECT t.x AS "{LONG}" FROM t'
        assert get_dialect("sqlite").rewrite_emitted_sql(sql) == sql

    def test_exempt_name_passes_through(self) -> None:
        sql = f'SELECT t.x AS "{LONG}" FROM t GROUP BY "{LONG}"'
        assert _pg_rewrite(sql, exempt=frozenset({LONG})) == sql

    def test_exempt_wins_over_the_alias_map_on_a_spelling_tie(self) -> None:
        sql = f'SELECT t.x AS "{LONG}" FROM t'
        assert _pg_rewrite(sql, aliases=[LONG], exempt=frozenset({LONG})) == sql

    def test_byte_budget_counts_utf8_bytes(self) -> None:
        name = "ü" * 40  # 40 chars, 80 bytes
        fitted = fit_identifier(name=name, limit=PG_LIMIT)
        out = _pg_rewrite(f'SELECT t.x AS "{name}" FROM t GROUP BY "{name}"')
        assert name not in out
        assert out.count(f'"{fitted}"') == 2
        assert _nbytes(fitted) <= PG_LIMIT

    def test_candidate_containing_the_quote_char_is_skipped(self) -> None:
        tricky = 'evil""name_' + "e" * 60  # user text, not a SLayer-minted name
        sql = f'SELECT t.x AS "{tricky}" FROM t'
        assert _pg_rewrite(sql) == sql

    def test_forced_digest_collision_raises_naming_both_parties(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(fitmod, "_digest", lambda name: "deadbeef")
        sql = f'SELECT a AS "{TWIN_A}", b AS "{TWIN_B}" FROM t'
        with pytest.raises(IdentifierCollisionError) as exc:
            _pg_rewrite(sql)
        assert TWIN_A in str(exc.value)
        assert TWIN_B in str(exc.value)

    def test_fitted_form_colliding_with_an_existing_token_raises(self) -> None:
        sql = f'SELECT a AS "{LONG}", b AS "{FIT_LONG}" FROM t'
        with pytest.raises(IdentifierCollisionError) as exc:
            _pg_rewrite(sql)
        assert LONG in str(exc.value)
        assert FIT_LONG in str(exc.value)

    def test_identical_short_names_in_independent_scopes_pass(self) -> None:
        sql = (
            'WITH c1 AS (SELECT 1 AS "city"), c2 AS (SELECT 2 AS "city")\n'
            'SELECT c1."city", c2."city" FROM c1, c2'
        )
        assert _pg_rewrite(sql) == sql


# Fitting never rewrites literals or comments (requirement 4) — masking shared
# by the scan and by substitute_quoted (also hardening the projection-alias pass).


class TestMasking:
    def test_scan_leaves_string_literals(self) -> None:
        literal = f'\'ref: "{LONG}" inside\''
        sql = f'SELECT t.x AS "{LONG}", {literal} AS note FROM t'
        out = _pg_rewrite(sql)
        assert literal in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_scan_leaves_literals_with_doubled_quotes(self) -> None:
        literal = f'\'it\'\'s "{LONG}" quoted\''
        sql = f'SELECT t.x AS "{LONG}" FROM t WHERE note = {literal}'
        out = _pg_rewrite(sql)
        assert literal in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_scan_leaves_line_and_block_comments(self) -> None:
        sql = f'SELECT t.x AS "{LONG}" -- see "{LONG}"\nFROM t /* also "{LONG}" */'
        out = _pg_rewrite(sql)
        assert f'-- see "{LONG}"' in out
        assert f'/* also "{LONG}" */' in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_scan_leaves_dollar_quoted_bodies(self) -> None:
        body = f'$$ref "{LONG}"$$'
        sql = f'SELECT t.x AS "{LONG}", {body} AS doc FROM t'
        out = _pg_rewrite(sql)
        assert body in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_scan_leaves_escape_string_with_escaped_quote(self) -> None:
        """A backslash-escaped quote in an ``E'...'`` literal must not end the
        literal early and expose the trailing identifier to fitting."""
        literal = f"E'left \\' ref \"{LONG}\" right'"
        sql = f'SELECT t.x AS "{LONG}", {literal} AS note FROM t'
        out = _pg_rewrite(sql)
        assert literal in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_scan_leaves_tagged_dollar_quoted_bodies(self) -> None:
        body = f'$tag$ref "{LONG}"$tag$'
        sql = f'SELECT t.x AS "{LONG}", {body} AS doc FROM t'
        out = _pg_rewrite(sql)
        assert body in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_scan_leaves_nested_block_comments(self) -> None:
        comment = f'/* outer /* inner "{LONG}" */ still "{LONG}" */'
        sql = f'SELECT t.x AS "{LONG}" FROM t {comment}'
        out = _pg_rewrite(sql)
        assert comment in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_scan_leaves_multibyte_literal_content(self) -> None:
        """Masking is code-point aligned: a multibyte char inside a literal keeps
        offsets valid, so the literal survives and only the outside alias is fitted."""
        literal = f'\'café "{LONG}" note\''
        sql = f'SELECT t.x AS "{LONG}", {literal} AS n FROM t'
        out = _pg_rewrite(sql)
        assert literal in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_dollar_quote_requires_a_boundary_before_the_opener(self) -> None:
        """On a dollar-quoting dialect, a ``$`` right after an identifier char does
        not open a dollar-quote (body stays visible); at a boundary it does (masked)."""
        dq = SqlLexis(dollar_quotes=True)
        assert "body" in fitmod._mask_sql("a$$body$$", lexis=dq)
        assert "body" in fitmod._mask_sql("a$tag$body$tag$", lexis=dq)
        assert "body" not in fitmod._mask_sql(" $$body$$", lexis=dq)
        assert "body" not in fitmod._mask_sql(" $tag$body$tag$", lexis=dq)

    def test_mask_sql_ordinary_string_honours_backslash_escapes_when_flagged(self) -> None:
        """Ordinary ``'...'`` masking closes at ``\\'`` only with escapes OFF
        (Postgres); ON (MySQL) the escaped quote stays inside the masked body."""
        assert "tail" in fitmod._mask_sql("'a\\' tail'")
        assert "tail" not in fitmod._mask_sql("'a\\' tail'", lexis=SqlLexis(backslash_escapes=True))

    def test_scan_leaves_backslash_escaped_ordinary_string_on_mysql(self) -> None:
        """MySQL ordinary strings honour backslash escapes, so ``'..\\'..'`` masks
        its whole body — the trailing identifier must not be exposed to fitting."""
        d = get_dialect("mysql")
        assert d.backslash_escapes_strings
        assert d.max_identifier_bytes is not None
        quote = d.quote_identifier
        literal = f"'left \\' ref {quote(LONG)} right'"
        sql = f"SELECT t.x AS {quote(LONG)}, {literal} AS note FROM t"
        out = d.rewrite_emitted_sql(sql)
        assert literal in out
        assert out.count(quote(d.fit_alias(LONG))) == 1

    def test_substitute_quoted_leaves_literals(self) -> None:
        quote = get_dialect("postgres").quote_identifier
        literal = f'\'ref: "{LONG}"\''
        sql = f'SELECT {literal} AS lit, t.x AS "{LONG}" FROM t'
        out = substitute_quoted(sql, {LONG: FIT_LONG}, quote=quote)
        assert literal in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_substitute_quoted_leaves_comments(self) -> None:
        quote = get_dialect("postgres").quote_identifier
        sql = f'SELECT t.x AS "{LONG}" FROM t -- "{LONG}"'
        out = substitute_quoted(sql, {LONG: FIT_LONG}, quote=quote)
        assert out.endswith(f'-- "{LONG}"')
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_substitute_quoted_leaves_escaped_quote_identifier(self) -> None:
        """A whole-span rewrite must not clip the suffix of a ``"a""b"`` identifier
        that happens to end in a fitting target's spelling."""
        quote = get_dialect("postgres").quote_identifier
        sql = f'SELECT x AS "prefix""{LONG}", y AS "{LONG}" FROM t'
        out = substitute_quoted(sql, {LONG: FIT_LONG}, quote=quote)
        assert f'"prefix""{LONG}"' in out
        assert out.count(f'"{FIT_LONG}"') == 1

    def test_substitute_quoted_leaves_escape_string(self) -> None:
        """The shared mask keeps ``substitute_quoted`` off ``E'...'`` bodies whose
        escaped quote would otherwise expose a mapped identifier."""
        quote = get_dialect("postgres").quote_identifier
        literal = f"E'left \\' \"{LONG}\"'"
        sql = f'SELECT {literal} AS lit, t.x AS "{LONG}" FROM t'
        out = substitute_quoted(sql, {LONG: FIT_LONG}, quote=quote)
        assert literal in out
        assert out.count(f'"{FIT_LONG}"') == 1

    @pytest.mark.parametrize("weird", ["a'b", "a--b", "a/*b", "a$tag$b", 'a""b'])
    def test_delimiter_inside_a_quoted_identifier_does_not_corrupt_masking(
        self, weird: str,
    ) -> None:
        """A string/comment delimiter inside a quoted identifier is opaque: it must
        not hijack masking and corrupt fitting of a following over-limit alias."""
        ident = f'"{weird}"'
        sql = f'SELECT x AS {ident}, y AS "{LONG}" FROM t GROUP BY "{LONG}"'
        out = _pg_rewrite(sql)
        assert ident in out                     # the odd identifier survives verbatim
        assert out.count(f'"{FIT_LONG}"') == 2  # the over-limit alias still fits
        assert LONG not in out


# Masking is dialect-aware: comment nesting, dollar-quoting and
# ordinary-string backslash escapes are gated by sqlglot's tokenizer, so the masker
# never over-masks live SQL on a dialect whose grammar differs from Postgres.


class TestMaskingLexis:
    @pytest.mark.parametrize(
        "dialect,nested,backslash,dollar",
        [
            ("postgres", True, False, True),
            ("mysql", False, True, False),
            ("tsql", True, False, False),
            ("duckdb", True, False, True),
            ("bigquery", False, True, False),
            ("sqlite", False, False, False),
            ("clickhouse", True, True, True),
            ("snowflake", False, True, True),
        ],
    )
    def test_lexis_derived_from_sqlglot(
        self, dialect: str, nested: bool, backslash: bool, dollar: bool,
    ) -> None:
        lexis = get_dialect(dialect).identifier_masking_lexis
        assert lexis.nested_comments == nested
        assert lexis.backslash_escapes == backslash
        assert lexis.dollar_quotes == dollar

    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "tsql", "bigquery"])
    def test_lexis_identifier_quote_matches_emitter_anchor(self, dialect: str) -> None:
        d = get_dialect(dialect)
        assert d.identifier_masking_lexis.identifier_quote == d._identifier_quote_anchors()

    def test_non_nesting_dialect_closes_block_comment_at_first_terminator(self) -> None:
        """MySQL (no nesting) exposes text after the first ``*/``; a nesting dialect
        masks through to the outer close — universal nesting would fail open."""
        mysql = get_dialect("mysql").identifier_masking_lexis
        assert not mysql.nested_comments
        assert "live" in fitmod._mask_sql("/* a /* b */ live */", lexis=mysql)
        assert "live" not in fitmod._mask_sql(
            "/* a /* b */ live */", lexis=SqlLexis(nested_comments=True)
        )

    def test_dialect_without_dollar_quotes_leaves_them_as_live_sql(self) -> None:
        """MySQL has no dollar-quoting, so ``$tag$...$tag$`` stays live SQL rather
        than a masked literal (universal masking would hide identifiers inside)."""
        mysql = get_dialect("mysql").identifier_masking_lexis
        assert not mysql.dollar_quotes
        assert "body" in fitmod._mask_sql(" $tag$body$tag$", lexis=mysql)
        assert "body" not in fitmod._mask_sql(
            " $tag$body$tag$", lexis=SqlLexis(dollar_quotes=True)
        )


# The shared scan/extraction helpers (tasks 2.1).


class TestScanHelpers:
    def test_find_overlimit_quoted_masks_literals_and_comments(self) -> None:
        find = _need(find_overlimit_quoted)
        sql = (
            f'SELECT t.x AS "{LONG}", \'lit "{LONG}"\' AS note, t.y AS "short"\n'
            f'FROM t -- comment "{LONG}"'
        )
        found = set(find(sql, limit=PG_LIMIT, quote_open='"', quote_close='"'))
        assert found == {LONG}

    def test_find_overlimit_quoted_skips_quote_bearing_candidates(self) -> None:
        find = _need(find_overlimit_quoted)
        tricky = 'evil""name_' + "e" * 60
        sql = f'SELECT t.x AS "{tricky}" FROM t'
        assert set(find(sql, limit=PG_LIMIT, quote_open='"', quote_close='"')) == set()

    def test_find_overlimit_quoted_measures_bytes(self) -> None:
        find = _need(find_overlimit_quoted)
        name = "ü" * 40  # under 63 chars, over 63 bytes
        sql = f'SELECT t.x AS "{name}" FROM t'
        assert set(find(sql, limit=PG_LIMIT, quote_open='"', quote_close='"')) == {name}

    def test_find_overlimit_quoted_bracket_anchors(self) -> None:
        find = _need(find_overlimit_quoted)
        sql = f'SELECT t.x AS [{LONG}] FROM t'
        assert set(find(sql, limit=PG_LIMIT, quote_open="[", quote_close="]")) == {LONG}

    def test_overlimit_tokens_extracts_bare_and_quoted(self) -> None:
        tokens = _need(overlimit_tokens)
        text = f'SELECT "{LONG}" AS a, {PHYS_TBL}.amount AS b, short FROM {PHYS_TBL}'
        assert set(tokens(text, limit=PG_LIMIT)) == {LONG, PHYS_TBL}

    def test_overlimit_tokens_measures_bytes(self) -> None:
        tokens = _need(overlimit_tokens)
        name = "ü" * 40
        assert set(tokens(f"SELECT {name} FROM t", limit=PG_LIMIT)) == {name}

    def test_overlimit_tokens_ignores_long_numeric_literals(self) -> None:
        """A long number is not an unquoted identifier, so the backstop must not
        flag it; a digit-leading quoted identifier still counts."""
        tokens = _need(overlimit_tokens)
        assert set(tokens(f"SELECT amount > {'9' * 80} FROM t", limit=PG_LIMIT)) == set()
        quoted = "1" + "z" * 70
        assert set(tokens(f'SELECT "{quoted}" FROM t', limit=PG_LIMIT)) == {quoted}


# Surviving over-limit identifiers fail closed (requirement 5): the backstop is
# independent of the rewrite hook and of any test-harness env flag. The exempt
# counterpart (survivors that must NOT be rejected) is TestUserSurfacesExempt.


class TestBackstop:
    @pytest.mark.parametrize("scopes_env", [True, False])
    async def test_unfitted_survivor_is_rejected(
        self, scopes_env: bool, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        if scopes_env:
            monkeypatch.setenv("SLAYER_VALIDATE_SCOPES", "1")
        else:
            monkeypatch.delenv("SLAYER_VALIDATE_SCOPES", raising=False)
            assert os.environ.get("SLAYER_VALIDATE_SCOPES") is None
        error = _need(IdentifierLengthError)
        monkeypatch.setattr(SqlDialect, "rewrite_emitted_sql", lambda self, sql, **kw: sql)
        with pytest.raises(error) as exc:
            await gen(NESTED, dialect="postgres")
        assert "63" in str(exc.value)
        assert "amount_sum_partition_by" in str(exc.value)

    def test_bracket_array_syntax_is_not_flagged_on_a_non_bracket_dialect(self) -> None:
        """``[…]`` is array syntax on Postgres, not an identifier: a long array
        literal must not trip the backstop. The scan uses only the dialect's own
        identifier quote style, so ``[…]`` is inert here (unlike T-SQL below)."""
        d = get_dialect("postgres")
        arr = "ARRAY[" + ", ".join(str(i) for i in range(40)) + "]"  # >63-byte [...] span
        d.assert_no_overlimit_identifiers(f"SELECT {arr} AS a FROM t")  # must not raise

    def test_overlimit_bracket_identifier_is_still_flagged_on_tsql(self) -> None:
        """T-SQL quotes identifiers with ``[…]``, so an over-limit bracketed survivor
        there is still rejected — the fix narrows scanning by dialect, not blindly."""
        d = get_dialect("tsql")
        over = "z" * 130  # over T-SQL's 128-byte limit
        with pytest.raises(_need(IdentifierLengthError)):
            d.assert_no_overlimit_identifiers(f"SELECT 1 AS [{over}]")


# Fitting composes with dialect alias mangling (requirement 6): budgets sized
# against the post-mangle form, result keys stay canonical.


class TestManglingComposition:
    async def test_bigquery_fits_only_post_mangle_over_budget_names(self) -> None:
        names = await _canonical_internal_aliases()
        sql = await gen(NESTED, dialect="bigquery")
        over = [n for n in _identifiers(sql, "bigquery") if _nbytes(n) > 300]
        assert not over, f"{over}\n{sql}"
        bq = get_dialect("bigquery")
        fitted_names = [n for n in names if _nbytes(encode_alias(n)) > 300]
        assert fitted_names, "no alias exceeds the post-mangle budget; test is vacuous"
        for n in fitted_names:
            assert encode_alias(n) not in sql
            assert encode_alias(bq.fit_alias(n)) in sql
        # Under the post-mangle budget -> untouched, even though over 63 bytes.
        for n in [n for n in names if _nbytes(encode_alias(n)) <= 300]:
            assert encode_alias(n) in sql

    async def test_tsql_fits_and_mangles_within_budget(self) -> None:
        names = await _canonical_internal_aliases()
        sql = await gen(NESTED, dialect="tsql")
        over = [n for n in _identifiers(sql, "tsql") if _nbytes(n) > 128]
        assert not over, f"{over}\n{sql}"
        ts = get_dialect("tsql")
        for n in names:
            assert _nbytes(encode_alias(n)) > 128  # both legs need fitting on tsql
            assert encode_alias(n) not in sql
            assert encode_alias(ts.fit_alias(n)) in sql

    def test_dotted_alias_only_mangling_pushes_over_is_still_fitted(self) -> None:
        """Budget sized against the post-mangle form: a dotted alias within the raw
        limit but over it once ``.``→``___`` expands must still be fitted, or the
        mangle would push the final identifier over and trip the backstop."""
        bq = get_dialect("bigquery")
        name = "orders." + "a" * 292  # raw 299 <= 300; encode_alias -> 301 > 300
        assert _nbytes(name) <= 300 < _nbytes(encode_alias(name))
        out = bq.rewrite_emitted_sql(f"SELECT 1 AS {bq.quote_identifier(name)}")
        assert encode_alias(name) not in out
        assert encode_alias(bq.fit_alias(name)) in out
        assert not [n for n in _identifiers(out, "bigquery") if _nbytes(n) > 300]
        bq.assert_no_overlimit_identifiers(out)  # must not falsely reject

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    async def test_result_keys_stay_canonical_and_decode(self, dialect: str) -> None:
        """The emitted outer aliases must be exactly ``emit_alias`` of the
        canonical result keys — that identity is what result-key decoding
        rebuilds — and decode must return the canonical dotted names."""
        d = get_dialect(dialect)
        resp = await _dry_run(dev1824_models(), NESTED, dialect=dialect)
        assert resp.columns == ["orders.band", "orders.b2", "orders.s"]
        assert resp.sql is not None
        tree = sqlglot.parse_one(resp.sql, dialect=dialect)
        outer = next(iter(tree.find_all(exp.Select)))
        projected = set(outer.named_selects)
        assert {d.emit_alias(c) for c in resp.columns} <= projected, projected
        rows = [{d.emit_alias(c): 1 for c in resp.columns}]
        decoded = d.decode_result_keys(rows, aliases=list(resp.columns))
        assert list(decoded[0].keys()) == list(resp.columns)
