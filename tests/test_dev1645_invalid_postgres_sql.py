"""DEV-1645: SLayer compiler must not emit invalid Postgres SQL.

Two defect classes, both valid-on-SQLite / invalid-on-Postgres:

* **Flavor A — ORDER BY on an unprojected/renamed column.** The sort key was
  rendered as a single composite-quoted identifier ``"<model>.<col>"`` (the
  projection-alias convention), which resolves only when the column is a
  projected output alias. For a column that is not projected (renamed by a
  ``columns:`` transform, or an inner-stage grouping dim the outer stage
  dropped), that composite token is a nonexistent column -> ``UndefinedColumn``.
  Fix: emit the fallback sort key as a real split ``table.column`` reference.

* **Flavor B — mixed-case identifier not double-quoted.** Free-SQL identifiers
  referencing mixed-case columns/tables were emitted unquoted; Postgres folds
  them to lowercase and can't find them. Fix: quote any identifier containing
  an uppercase letter (universal, all dialects), applied at every construction
  site (``_parse``, ``_parse_predicate``, and direct ``exp.Column`` / table
  builders).

These are byte-level emission tests; execution against a real Postgres /
Snowflake lives in the integration suites.
"""

from __future__ import annotations

import pytest

import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.enrichment import enrich_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage


async def _noop_async(**kw):  # NOSONAR(S7503) — resolver-callback contract is async
    return None


def _norm(s: str) -> str:
    return " ".join(s.split())


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


# ----------------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------------

@pytest.fixture
def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
    )


@pytest.fixture
def accounts_model() -> SlayerModel:
    """Mirrors the ``fake_account_23`` repro: a derived boolean column whose SQL
    references a mixed-case physical column ``StateFlag``."""
    return SlayerModel(
        name="accounts",
        sql_table="public.accounts",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(
                name="is_inactive",
                type=DataType.BOOLEAN,
                sql="CASE WHEN StateFlag = 'Dormant' THEN TRUE ELSE FALSE END",
            ),
            Column(
                name="acct_form",
                type=DataType.TEXT,
                sql="acct_form",  # lowercase sibling — must stay unquoted
            ),
        ],
    )


# ============================================================================
# Flavor A — ORDER BY on an unprojected / renamed column
# ============================================================================

class TestFlavorAOrderByUnprojected:
    async def test_orderby_nonprojected_column_emits_split_not_composite(
        self, orders_model: SlayerModel
    ) -> None:
        """A raw-row query ordering by a base column that is not projected must
        emit a split ``orders.created_at`` reference, NOT the composite
        ``"orders.created_at"`` (a nonexistent output column on Postgres)."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="desc")],
        )
        sql = _norm(await _generate(query, orders_model))
        assert "ORDER BY orders.created_at DESC" in sql
        assert '"orders.created_at"' not in sql

    async def test_orderby_projected_alias_stays_composite_quoted(
        self, orders_model: SlayerModel
    ) -> None:
        """Regression guard: ordering by a projected measure alias keeps the
        whole-quoted composite ``"orders.rev"`` form (that IS the real output
        column name via ``AS "orders.rev"``)."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": "revenue:sum", "name": "rev"}],
            order=[OrderItem(column=ColumnRef(name="rev"), direction="desc")],
        )
        sql = _norm(await _generate(query, orders_model))
        assert 'ORDER BY "orders.rev" DESC' in sql

    async def test_orderby_nonprojected_mixed_case_column_split_and_quoted(
        self, orders_model: SlayerModel
    ) -> None:
        """Flavor A + B together: a non-projected mixed-case sort column emits a
        split reference whose column part is quoted: ``orders."CreatedAt"``."""
        model = orders_model.model_copy(deep=True)
        model.columns.append(Column(name="CreatedAt", sql="CreatedAt", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column=ColumnRef(name="CreatedAt"), direction="desc")],
        )
        sql = _norm(await _generate(query, model))
        assert 'ORDER BY orders."CreatedAt" DESC' in sql
        assert '"orders.CreatedAt"' not in sql

    async def test_orderby_split_key_keeps_asc_limit_offset(
        self, orders_model: SlayerModel
    ) -> None:
        """ASC direction plus LIMIT / OFFSET are still applied around the split
        fallback sort key."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
            limit=10,
            offset=5,
        )
        sql = _norm(await _generate(query, orders_model))
        assert "ORDER BY orders.created_at ASC" in sql
        assert "LIMIT 10" in sql
        assert "OFFSET 5" in sql
        assert '"orders.created_at"' not in sql

    async def test_orderby_projected_alias_combined_cte_path_unchanged(self, tmp_path) -> None:
        """Regression guard for the combined measure-CTE ORDER BY site
        (`_assemble_combined_sql`): ordering by a projected cross-model measure
        alias keeps the whole-quoted composite form."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        clusters = SlayerModel(
            name="clusters", sql_table="clusters", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="score", sql="score", type=DataType.DOUBLE),
            ],
        )
        await storage.save_model(clusters)
        accts = SlayerModel(
            name="accts", sql_table="accts", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="clu_ref", sql="clu_ref", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
            joins=[ModelJoin(target_model="clusters", join_pairs=[["clu_ref", "id"]])],
        )
        await storage.save_model(accts)
        query = SlayerQuery(
            source_model="accts",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": "clusters.score:sum", "name": "sc"}],
            order=[OrderItem(column=ColumnRef(name="sc"), direction="desc")],
        )
        sql = _norm(await _generate_via_engine(query, accts, storage))
        assert 'ORDER BY "accts.clusters.sc" DESC' in sql


# ============================================================================
# Flavor B — mixed-case identifier quoting
# ============================================================================

class TestFlavorBMixedCaseQuoting:
    async def test_column_sql_mixed_case_quoted(self, accounts_model: SlayerModel) -> None:
        """Mixed-case ident inside ``Column.sql`` (CASE WHEN) is quoted; a
        lowercase sibling column that IS projected stays unquoted."""
        query = SlayerQuery(
            source_model="accounts",
            dimensions=[ColumnRef(name="is_inactive"), ColumnRef(name="acct_form")],
            distinct_dimension_values=False,
        )
        sql = _norm(await _generate(query, accounts_model))
        assert 'CASE WHEN "StateFlag" = \'Dormant\'' in sql
        # lowercase sibling is emitted (projected) and stays unquoted
        assert "accounts.acct_form" in sql
        assert '"acct_form"' not in sql

    async def test_filter_predicate_path_mixed_case_quoted(
        self, accounts_model: SlayerModel
    ) -> None:
        """The headline ``fake_account_23`` repro: a filter referencing a
        derived column whose SQL holds a mixed-case ident flows through
        ``_parse_predicate`` and must emit the quoted form in the WHERE."""
        query = SlayerQuery(
            source_model="accounts",
            measures=[{"formula": "*:count", "name": "cnt"}],
            filters=["is_inactive == True"],
        )
        sql = _norm(await _generate(query, accounts_model))
        assert "WHERE" in sql
        assert '"StateFlag"' in sql

    async def test_model_filter_mixed_case_quoted(self) -> None:
        """A model-level always-applied filter with a mixed-case ident is quoted."""
        model = SlayerModel(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            filters=["StateFlag IS NOT NULL"],
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)],
        )
        query = SlayerQuery(source_model="accounts", measures=[{"formula": "*:count"}])
        sql = _norm(await _generate(query, model))
        assert "WHERE" in sql
        assert '"StateFlag"' in sql

    async def test_column_filter_mixed_case_quoted(self) -> None:
        """A ``Column.filter`` (CASE-WHEN at aggregation time) with a mixed-case
        ident is quoted."""
        model = SlayerModel(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(
                    name="amount",
                    sql="amount",
                    type=DataType.DOUBLE,
                    filter="StateFlag = 'Active'",
                ),
            ],
        )
        query = SlayerQuery(source_model="accounts", measures=[{"formula": "amount:sum"}])
        sql = _norm(await _generate(query, model))
        assert '"StateFlag"' in sql

    async def test_resolved_join_key_mixed_case_quoted(self, tmp_path) -> None:
        """A join whose join_pairs reference a mixed-case key (``CLSTR_PIN``)
        emits the quoted form in the ON clause (the ``fake_account_15`` repro)."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        cluster = SlayerModel(
            name="cluster_analysis",
            sql_table="cluster_analysis",
            data_source="test",
            columns=[
                Column(name="CLSTR_PIN", sql="CLSTR_PIN", type=DataType.DOUBLE, primary_key=True),
                Column(name="score", sql="score", type=DataType.DOUBLE),
            ],
        )
        await storage.save_model(cluster)
        accounts = SlayerModel(
            name="account_clusters",
            sql_table="account_clusters",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="clu_ref", sql="clu_ref", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="cluster_analysis", join_pairs=[["clu_ref", "CLSTR_PIN"]])],
        )
        await storage.save_model(accounts)
        # A cross-model dimension forces a visible LEFT JOIN ... ON <keys>,
        # matching the fake_account_15 shape (`ON ... = cluster_analysis.CLSTR_PIN`).
        query = SlayerQuery(
            source_model="account_clusters",
            dimensions=[ColumnRef(name="score", model="cluster_analysis")],
            distinct_dimension_values=False,
        )
        sql = await _generate_via_engine(query, accounts, storage)
        assert "LEFT JOIN" in sql and " ON " in sql
        assert '"CLSTR_PIN"' in _norm(sql)

    async def test_bare_mixed_case_dimension_quoted(self) -> None:
        """A dimension referencing a bare mixed-case column (``_resolve_sql``
        direct-construction path) quotes both qualifier-free column and any
        model qualifier consistently."""
        model = SlayerModel(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="StateFlag", sql="StateFlag", type=DataType.TEXT),
            ],
        )
        query = SlayerQuery(
            source_model="accounts",
            dimensions=[ColumnRef(name="StateFlag")],
            distinct_dimension_values=False,
        )
        sql = _norm(await _generate(query, model))
        assert '"StateFlag"' in sql

    async def test_first_last_ranked_mixed_case_dimension_quoted(self) -> None:
        """The first/last ranked-subquery path references group-by dimensions by
        bare name against the model.* subquery output; a mixed-case dimension
        must be quoted there (DEV-1645, Codex review of PR #224)."""
        model = SlayerModel(
            name="events",
            sql_table="public.events",
            data_source="test",
            default_time_dimension="ts",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="RegionCode", sql="RegionCode", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="ts", sql="ts", type=DataType.TIMESTAMP),
            ],
        )
        query = SlayerQuery(
            source_model="events",
            dimensions=[ColumnRef(name="RegionCode")],
            measures=[{"formula": "amount:last(ts)", "name": "latest"}],
        )
        sql = _norm(await _generate(query, model))
        # The OUTER group-by / projection reference (line 1376 path) must be
        # quoted so it matches the ranked subquery's model.* output column.
        assert 'GROUP BY "RegionCode"' in sql
        assert 'GROUP BY RegionCode' not in sql

    async def test_standard_agg_inner_mixed_case_column_quoted(self) -> None:
        """A standard aggregation over a bare mixed-case column quotes the inner
        column (the ``_resolve_sql`` / standard-agg inner construction path):
        ``SUM(accounts."MixedAmt")``."""
        model = SlayerModel(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="MixedAmt", sql="MixedAmt", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(source_model="accounts", measures=[{"formula": "MixedAmt:sum", "name": "s"}])
        sql = _norm(await _generate(query, model))
        assert 'SUM(accounts."MixedAmt")' in sql

    async def test_resolved_join_target_table_mixed_case_quoted(self, tmp_path) -> None:
        """A join whose TARGET model has a mixed-case physical ``sql_table`` emits
        a quoted table name in the LEFT JOIN."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        regions = SlayerModel(
            name="regions", sql_table="MyRegions", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
        )
        await storage.save_model(regions)
        cust = SlayerModel(
            name="cust", sql_table="cust", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
        await storage.save_model(cust)
        query = SlayerQuery(
            source_model="cust",
            dimensions=[ColumnRef(name="name", model="regions")],
            distinct_dimension_values=False,
        )
        sql = _norm(await _generate_via_engine(query, cust, storage))
        assert 'LEFT JOIN "MyRegions"' in sql

    async def test_physical_table_name_mixed_case_quoted(self) -> None:
        """A mixed-case physical ``sql_table`` is quoted in the FROM clause."""
        model = SlayerModel(
            name="accounts",
            sql_table="public.MyAccounts",
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)],
        )
        query = SlayerQuery(source_model="accounts", measures=[{"formula": "*:count"}])
        sql = _norm(await _generate(query, model))
        assert '"MyAccounts"' in sql

    async def test_lowercase_only_expression_unchanged(self, orders_model: SlayerModel) -> None:
        """No spurious quoting: an all-lowercase expression emits unchanged."""
        query = SlayerQuery(source_model="orders", measures=[{"formula": "revenue:sum"}])
        sql = _norm(await _generate(query, orders_model))
        assert "SUM(orders.amount)" in sql
        assert '"amount"' not in sql

    async def test_function_name_not_quoted(self) -> None:
        """A function call whose name is mixed-case-ish stays a function — only
        Identifier nodes are quoted, not Func/Anonymous names."""
        model = SlayerModel(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="j", sql="COALESCE(RawVal, 0)", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(source_model="accounts", measures=[{"formula": "j:sum"}])
        sql = _norm(await _generate(query, model))
        assert "COALESCE" in sql
        assert '"COALESCE"' not in sql
        assert '"RawVal"' in sql  # the mixed-case *argument* is quoted

    async def test_mixed_case_join_target_alias_consistent(self, tmp_path) -> None:
        """Def/ref consistency (Codex finding #4): when a join key is mixed-case,
        the ON reference and the projected join column resolve consistently — the
        emitted SQL parses and every quoted identifier is balanced."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        regions = SlayerModel(
            name="regions",
            sql_table="regions",
            data_source="test",
            columns=[
                Column(name="RegionId", sql="RegionId", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
        )
        await storage.save_model(regions)
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="RegionRef", sql="RegionRef", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["RegionRef", "RegionId"]])],
        )
        await storage.save_model(customers)
        query = SlayerQuery(
            source_model="customers",
            dimensions=[ColumnRef(name="name", model="regions")],
            distinct_dimension_values=False,
        )
        sql = await _generate_via_engine(query, customers, storage)
        # both mixed-case join keys quoted, and the SQL parses cleanly
        assert '"RegionRef"' in sql
        assert '"RegionId"' in sql
        sqlglot.parse_one(sql, dialect="postgres")


class TestMixedCaseHelperUnit:
    """Direct policy tests for the shared quoting helper / constructors."""

    def test_quote_mixed_case_identifiers_quotes_uppercase_only(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        tree = sqlglot.parse_one("CASE WHEN accounts.StateFlag = 'x' THEN 1 ELSE 0 END")
        out = tree.transform(gen._quote_mixed_case_identifiers).sql(dialect="postgres")
        assert '"StateFlag"' in out
        assert "accounts" in out and '"accounts"' not in out  # lowercase qualifier untouched

    def test_quote_mixed_case_idempotent_and_skips_prequoted(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        tree = sqlglot.parse_one('accounts."StateFlag" = 1')
        once = tree.transform(gen._quote_mixed_case_identifiers)
        twice = once.transform(gen._quote_mixed_case_identifiers)
        assert twice.sql(dialect="postgres").count('"StateFlag"') == 1  # no double-quoting

    def test_to_ident_quotes_mixed_case(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        assert gen._to_ident("StateFlag").sql(dialect="postgres") == '"StateFlag"'
        assert gen._to_ident("state_flag").sql(dialect="postgres") == "state_flag"

    def test_to_table_quotes_physical_name_not_alias(self) -> None:
        """Physical table name is quoted when mixed-case; the SLayer-internal
        alias is left unquoted (it folds consistently within the query)."""
        gen = SQLGenerator(dialect="postgres")
        out = gen._to_table("public.MyTable", alias="MyAlias").sql(dialect="postgres")
        assert '"MyTable"' in out
        assert '"MyAlias"' not in out
        assert "AS MyAlias" in out


# ============================================================================
# Multi-dialect: universal quoting with per-dialect quote characters
# ============================================================================

class TestMixedCaseQuotingMultiDialect:
    _QUOTE = {
        "postgres": ('"', '"'),
        "sqlite": ('"', '"'),
        "duckdb": ('"', '"'),
        "snowflake": ('"', '"'),
        "mysql": ("`", "`"),
        "tsql": ("[", "]"),
    }

    @pytest.mark.parametrize("dialect", list(_QUOTE))
    async def test_mixed_case_column_quoted_per_dialect(self, dialect: str) -> None:
        model = SlayerModel(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(
                    name="is_inactive",
                    type=DataType.BOOLEAN,
                    sql="CASE WHEN StateFlag = 'Dormant' THEN TRUE ELSE FALSE END",
                ),
            ],
        )
        query = SlayerQuery(
            source_model="accounts",
            dimensions=[ColumnRef(name="is_inactive")],
            distinct_dimension_values=False,
        )
        sql = _norm(await _generate(query, model, dialect=dialect))
        lq, rq = self._QUOTE[dialect]
        assert f"{lq}StateFlag{rq}" in sql
