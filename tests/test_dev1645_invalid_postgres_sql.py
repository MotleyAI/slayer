"""SLayer compiler must not emit invalid Postgres SQL.

Two defect classes, both valid-on-SQLite / invalid-on-Postgres:

* **Flavor A — ORDER BY on an unprojected/renamed column.** The sort key was
  rendered as a single composite-quoted identifier ``"<dataset>.<col>"`` (the
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

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import OrderItem, QueryDimension, SlayerQuery, TimeDimension
from slayer.sql.generator import SQLGenerator

from tests._engine_helpers import _engine_generate


def _norm(s: str) -> str:
    return " ".join(s.split())


def _orders_customers_regions() -> tuple[SlayerDataset, list[SlayerDataset]]:
    """orders → customers → regions two-hop relationship graph. Returns
    ``(orders_root, [customers, regions])`` — the root dataset plus the relationship
    targets to register alongside it. Shared by the joined-ORDER-BY reject +
    deferred xfail tests."""
    regions = SlayerDataset(
        name="regions", sql_table="regions", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),
        ],
    )
    customers = SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", sql="region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )
    orders = SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",  # lets first/last + cumsum tests reuse this graph
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )
    return orders, [customers, regions]


# ----------------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------------

@pytest.fixture
def orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
            DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT),
        ],
    )


@pytest.fixture
def accounts_dataset() -> SlayerDataset:
    """Mirrors the ``fake_account_23`` repro: a derived boolean field whose SQL
    references a mixed-case physical column ``StateFlag``."""
    return SlayerDataset(
        name="accounts",
        sql_table="public.accounts",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(
                name="is_inactive",
                datatype=DataType.BOOLEAN,
                expression="CASE WHEN StateFlag = 'Dormant' THEN TRUE ELSE FALSE END",
            ),
            DatasetField(
                name="acct_form",
                datatype=DataType.STRING,
                sql="acct_form",  # lowercase sibling — must stay unquoted
            ),
        ],
    )


# ============================================================================
# Flavor A — ORDER BY on an unprojected / renamed column
# ============================================================================

class TestFlavorAOrderByUnprojected:
    async def test_orderby_nonprojected_column_emits_split_not_composite(
        self, orders_dataset: SlayerDataset
    ) -> None:
        """A raw-row query ordering by a base field that is not projected must
        emit a split ``orders.created_at`` reference, NOT the composite
        ``"orders.created_at"`` (a nonexistent output column on Postgres)."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            distinct_dimension_values=False,
            order=[OrderItem(field="orders.created_at", direction="desc")],
        )
        sql = _norm(await _engine_generate(query=query, dataset=orders_dataset))
        assert "ORDER BY orders.created_at DESC" in sql
        assert '"orders.created_at"' not in sql

    async def test_orderby_projected_alias_stays_composite_quoted(
        self, orders_dataset: SlayerDataset
    ) -> None:
        """Regression guard: ordering by a projected metric alias keeps the
        whole-quoted composite ``"orders.rev"`` form (that IS the real output
        column name via ``AS "orders.rev"``)."""
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": [QueryDimension(field="orders.status")],
            "metrics": [{"metric": "sum(orders.revenue)", "alias": "rev"}],
            "order": [OrderItem(field="rev", direction="desc")],
        })
        sql = _norm(await _engine_generate(query=query, dataset=orders_dataset))
        assert 'ORDER BY "rev" DESC' in sql

    async def test_orderby_nonprojected_mixed_case_column_split_and_quoted(
        self, orders_dataset: SlayerDataset
    ) -> None:
        """Flavor A + B together: a non-projected mixed-case sort field emits a
        split reference whose column part is quoted: ``orders."CreatedAt"``."""
        model = orders_dataset.model_copy(deep=True)
        model.fields.append(DatasetField(name="CreatedAt", sql="CreatedAt", datatype=DataType.DATETIME))
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            distinct_dimension_values=False,
            order=[OrderItem(field="orders.CreatedAt", direction="desc")],
        )
        sql = _norm(await _engine_generate(query=query, dataset=model))
        assert 'ORDER BY orders."CreatedAt" DESC' in sql
        assert '"orders.CreatedAt"' not in sql

    async def test_orderby_split_key_keeps_asc_limit_offset(
        self, orders_dataset: SlayerDataset
    ) -> None:
        """ASC direction plus LIMIT / OFFSET are still applied around the split
        fallback sort key."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            distinct_dimension_values=False,
            order=[OrderItem(field="orders.created_at", direction="asc")],
            limit=10,
            offset=5,
        )
        sql = _norm(await _engine_generate(query=query, dataset=orders_dataset))
        assert "ORDER BY orders.created_at ASC" in sql
        assert "LIMIT 10" in sql
        assert "OFFSET 5" in sql
        assert '"orders.created_at"' not in sql

    async def test_orderby_projected_alias_combined_cte_path_unchanged(self) -> None:
        """Regression guard for the combined metric-CTE ORDER BY site
        (`_assemble_combined_sql`): ordering by a projected cross-dataset metric
        alias keeps the whole-quoted composite form."""
        clusters = SlayerDataset(
            name="clusters", sql_table="clusters", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="score", sql="score", datatype=DataType.FLOAT),
            ],
        )
        accts = SlayerDataset(
            name="accts", sql_table="accts", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="clu_ref", sql="clu_ref", datatype=DataType.FLOAT),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
            ],
            relationships=[DatasetRelationship(to="clusters", from_columns=["clu_ref"], to_columns=["id"])],
        )
        query = SlayerQuery.model_validate({
            "population": "accts",
            "dimensions": [QueryDimension(field="accts.status")],
            "metrics": [{"metric": "sum(clusters.score)", "alias": "sc"}],
            "order": [OrderItem(field="sc", direction="desc")],
            "to_many_handling": "broadcast",
        })
        sql = _norm(await _engine_generate(
            query=query, dataset=accts, extra_datasets=[clusters],
        ))
        # The typed pipeline sorts on the cross-dataset CTE's own canonical
        # output column, QUALIFIED by the CTE that emits it. It used to be the
        # bare name, which resolved only by falling through to an input column
        # of the FROM — legal on Postgres, not everywhere, and ambiguous the
        # moment two scopes project the same name. What this test guards is
        # the WHOLE-QUOTED composite form at the combined-CTE ORDER BY site —
        # a split ``_cm_x.clusters.score_sum`` names a column that does
        # not exist. The producer is target-rooted, so its inner alias is the
        # CANONICAL ``clusters.score_sum`` (not host-prefixed).
        assert (
            'ORDER BY _cm_accts__clusters__score_sum."score_sum" DESC'
        ) in sql, sql
        assert "ORDER BY accts." not in sql, sql
        assert '"score_sum"' in sql, sql

    async def test_orderby_unprojected_joined_column_resolves_host_rooted(
        self,
    ) -> None:
        """An unprojected multi-hop joined ORDER BY binds to no FROM table; a
        HOST-rooted CTE computes the per-group extreme over the crossed relationship and
        the outer query orders on that CTE's column, so nothing is unbound."""
        orders, joined = _orders_customers_regions()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            "order": [OrderItem(field="customers.regions.name", direction="desc")],
        })
        sql = _norm(await _engine_generate(
            query=query, dataset=orders, extra_datasets=joined,
        ))
        # The sort key's join lives in the CTE, never in the host base — that
        # containment is what keeps ``SUM(orders.amount)`` unmultiplied.
        base = sql.split("_cm_")[0]
        assert "JOIN" not in base.upper(), sql
        assert "MAX(customers__regions.name)" in sql, sql
        assert "ORDER BY _cm_" in sql, sql

    async def test_orderby_joined_column_resolves_when_filter_pulls_join_in(
        self,
    ) -> None:
        """The same resolution when a filter already pulled the relationship into the
        host base. The CTE re-applies the filter rather than depending on the
        base's copy, so the two scopes agree on which rows the extreme is over."""
        orders, joined = _orders_customers_regions()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            "filter": ["customers.regions.name == 'US'"],
            "order": [OrderItem(field="customers.regions.name", direction="desc")],
        })
        sql = _norm(await _engine_generate(
            query=query, dataset=orders, extra_datasets=joined,
        ))
        assert sql.count("customers__regions.name = 'US'") == 2, sql
        assert "ORDER BY _cm_" in sql, sql

    async def test_orderby_joined_column_resolves_in_cte_wrapped_scope(
        self,
    ) -> None:
        """The CTE-wrapped (windowed-metric) path. The sort key's CTE groups on
        the query grain and joins back NULL-safely, so the outer ORDER BY names a
        column the wrapper actually projects."""
        orders, joined = _orders_customers_regions()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "time_dimensions": [TimeDimension(field="orders.created_at", granularity=TimeGranularity.MONTH)],
            "metrics": [{"metric": "cumsum(sum(orders.amount))", "alias": "cs"}],  # windowed -> combined CTE path
            "filter": ["customers.regions.name == 'US'"],  # pulls the relationship into resolved_joins
            "order": [OrderItem(field="customers.regions.name", direction="desc")],
        })
        sql = _norm(await _engine_generate(
            query=query, dataset=orders, extra_datasets=joined,
        ))
        assert "IS NOT DISTINCT FROM" in sql, sql
        assert 'ORDER BY "customers.regions.name_max_host" DESC' in sql, sql

    async def test_orderby_joined_column_resolves_in_first_last_ranked_scope(
        self,
    ) -> None:
        """The first/last path: the ranked value is its own ``_cm_`` producer, and
        the joined sort key takes the same host MAX wrap as beside ``sum`` —
        never a ``_base`` GROUP BY key, which would split the one-row result."""
        orders, joined = _orders_customers_regions()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "last(orders.amount, orders.created_at)", "alias": "latest"}],
            "filter": ["customers.regions.name == 'US'"],
            "order": [OrderItem(field="customers.regions.name", direction="desc")],
        })
        sql = _norm(await _engine_generate(
            query=query, dataset=orders, extra_datasets=joined,
        ))
        assert "_cm_" in sql, sql
        assert "MAX(customers__regions.name)" in sql, sql
        assert '"customers.regions.name_max_host" DESC' in sql, sql
        assert "GROUP BY customers__regions.name" not in sql, sql


# ============================================================================
# Flavor A — joined / cross-dataset ORDER BY resolution (was DEFERRED)
# ============================================================================

class TestFlavorAJoinedOrderByResolves:
    """Ordering by an unprojected JOINED field in a GROUPED query.

    These three cases were ``xfail(strict=True)`` aspiring to a BARE split
    reference, ``ORDER BY customers__regions.name``. That aspiration was wrong
    for a grouped query — the field is not in GROUP BY, so a bare reference is
    invalid SQL on every Tier-1 dialect. The shape below is what the value
    actually requires: computed per group in its own scope, then joined back."""

    async def test_joined_orderby_with_filter_in_scope_resolves(self) -> None:
        orders, joined = _orders_customers_regions()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            "filter": ["customers.regions.name == 'US'"],
            "order": [OrderItem(field="customers.regions.name", direction="desc")],
        })
        sql = _norm(await _engine_generate(
            query=query, dataset=orders, extra_datasets=joined,
        ))
        assert "MAX(customers__regions.name)" in sql, sql

    async def test_joined_orderby_without_filter_pulls_join_into_the_cte(
        self,
    ) -> None:
        """With nothing else referencing the relationship, it is pulled into the sort
        key's CTE — and only there."""
        orders, joined = _orders_customers_regions()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            "order": [OrderItem(field="customers.regions.name", direction="desc")],
        })
        sql = _norm(await _engine_generate(
            query=query, dataset=orders, extra_datasets=joined,
        ))
        assert "MAX(customers__regions.name)" in sql, sql
        assert "JOIN" not in sql.split("_cm_")[0].upper(), sql

    async def test_joined_orderby_in_cte_wrapped_scope_resolves(self) -> None:
        orders, joined = _orders_customers_regions()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "time_dimensions": [TimeDimension(field="orders.created_at", granularity=TimeGranularity.MONTH)],
            "metrics": [{"metric": "cumsum(sum(orders.amount))", "alias": "cs"}],
            "filter": ["customers.regions.name == 'US'"],
            "order": [OrderItem(field="customers.regions.name", direction="desc")],
        })
        sql = _norm(await _engine_generate(
            query=query, dataset=orders, extra_datasets=joined,
        ))
        assert 'ORDER BY "customers.regions.name_max_host" DESC' in sql, sql


# ============================================================================
# Flavor B — mixed-case identifier quoting
# ============================================================================

class TestFlavorBMixedCaseQuoting:
    async def test_column_sql_mixed_case_quoted(self, accounts_dataset: SlayerDataset) -> None:
        """Mixed-case ident inside ``DatasetField.sql`` (CASE WHEN) is quoted; a
        lowercase sibling field that IS projected stays unquoted.

        The typed pipeline additionally anchors the expanded reference at the
        scope root (Law 1), so the quoted ident arrives relation-qualified as
        ``accounts."StateFlag"``. The defect this pins is the QUOTING — an
        unquoted ``StateFlag`` folds to lowercase on Postgres and raises
        UndefinedColumn — so the qualifier is incidental and the assertion
        matches the quoted ident rather than a bare-prefix shape.
        """
        query = SlayerQuery(
            population="accounts",
            dimensions=[QueryDimension(field="accounts.is_inactive"), QueryDimension(field="accounts.acct_form")],
            distinct_dimension_values=False,
        )
        sql = _norm(await _engine_generate(query=query, dataset=accounts_dataset))
        assert 'CASE WHEN accounts."StateFlag" = \'Dormant\'' in sql, sql
        # The bare unquoted form must never appear.
        assert "CASE WHEN accounts.StateFlag" not in sql, sql
        # lowercase sibling is emitted (projected) and stays unquoted
        assert "accounts.acct_form" in sql
        assert '"acct_form"' not in sql

    async def test_filter_predicate_path_mixed_case_quoted(
        self, accounts_dataset: SlayerDataset
    ) -> None:
        """The headline ``fake_account_23`` repro: a filter referencing a
        derived field whose SQL holds a mixed-case ident flows through
        ``_parse_predicate`` and must emit the quoted form in the WHERE."""
        query = SlayerQuery.model_validate({
            "population": "accounts",
            "metrics": [{"metric": "count(accounts.*)", "alias": "cnt"}],
            "filter": ["accounts.is_inactive == True"],
        })
        sql = _norm(await _engine_generate(query=query, dataset=accounts_dataset))
        assert "WHERE" in sql
        assert '"StateFlag"' in sql

    async def test_model_filter_mixed_case_quoted(self) -> None:
        """A dataset-level always-applied filter with a mixed-case ident is quoted."""
        model = SlayerDataset(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            filters=["StateFlag IS NOT NULL"],
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True)],
        )
        query = SlayerQuery.model_validate({"population": "accounts", "metrics": [{"metric": "count(accounts.*)"}]})
        sql = _norm(await _engine_generate(query=query, dataset=model))
        assert "WHERE" in sql
        assert '"StateFlag"' in sql

    async def test_column_filter_mixed_case_quoted(self) -> None:
        """A ``DatasetField.filter`` (CASE-WHEN at aggregation time) with a mixed-case
        ident is quoted."""
        model = SlayerDataset(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(
                    name="amount",
                    sql="amount",
                    datatype=DataType.FLOAT,
                    filter="StateFlag = 'Active'",
                ),
            ],
        )
        query = SlayerQuery.model_validate({"population": "accounts", "metrics": [{"metric": "sum(accounts.amount)"}]})
        sql = _norm(await _engine_generate(query=query, dataset=model))
        assert '"StateFlag"' in sql

    async def test_resolved_join_key_mixed_case_quoted(self) -> None:
        """A relationship whose join_pairs reference a mixed-case key (``CLSTR_PIN``)
        emits the quoted form in the ON clause (the ``fake_account_15`` repro)."""
        cluster = SlayerDataset(
            name="cluster_analysis",
            sql_table="cluster_analysis",
            data_source="test",
            fields=[
                DatasetField(name="CLSTR_PIN", sql="CLSTR_PIN", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="score", sql="score", datatype=DataType.FLOAT),
            ],
        )
        accounts = SlayerDataset(
            name="account_clusters",
            sql_table="account_clusters",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="clu_ref", sql="clu_ref", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="cluster_analysis", from_columns=["clu_ref"], to_columns=["CLSTR_PIN"])],
        )
        # A cross-dataset dimension forces a visible LEFT JOIN ... ON <keys>,
        # matching the fake_account_15 shape (`ON ... = cluster_analysis.CLSTR_PIN`).
        query = SlayerQuery(
            population="account_clusters",
            dimensions=[QueryDimension(field="cluster_analysis.score")],
            distinct_dimension_values=False,
        )
        sql = await _engine_generate(
            query=query, dataset=accounts, extra_datasets=[cluster],
        )
        assert "LEFT JOIN" in sql
        assert " ON " in sql
        assert '"CLSTR_PIN"' in _norm(sql)

    async def test_bare_mixed_case_dimension_quoted(self) -> None:
        """A dimension referencing a bare mixed-case field (``_resolve_sql``
        direct-construction path) quotes both qualifier-free column and any
        dataset qualifier consistently."""
        model = SlayerDataset(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="StateFlag", sql="StateFlag", datatype=DataType.STRING),
            ],
        )
        query = SlayerQuery(
            population="accounts",
            dimensions=[QueryDimension(field="accounts.StateFlag")],
            distinct_dimension_values=False,
        )
        sql = _norm(await _engine_generate(query=query, dataset=model))
        assert '"StateFlag"' in sql

    async def test_first_last_ranked_mixed_case_dimension_quoted(self) -> None:
        """The first/last ranked-subquery path references group-by dimensions by
        bare name against the model.* subquery output; a mixed-case dimension
        must be quoted there."""
        model = SlayerDataset(
            name="events",
            sql_table="public.events",
            data_source="test",
            default_time_dimension="ts",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="RegionCode", sql="RegionCode", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="ts", sql="ts", datatype=DataType.DATETIME),
            ],
        )
        query = SlayerQuery.model_validate({
            "population": "events",
            "dimensions": [QueryDimension(field="events.RegionCode")],
            "metrics": [{"metric": "last(events.amount, events.ts)", "alias": "latest"}],
        })
        sql = _norm(await _engine_generate(query=query, dataset=model))
        # The OUTER group-by / projection reference must be quoted so it
        # matches the ranked subquery's ``model.*`` output column. The typed
        # pipeline emits it relation-qualified (``events."RegionCode"``);
        # unquoted, Postgres folds it to ``regioncode`` -> UndefinedColumn.
        assert 'GROUP BY events."RegionCode"' in sql, sql
        assert "GROUP BY events.RegionCode" not in sql, sql
        # ...and the projection must agree with it.
        assert 'events."RegionCode" AS "events.RegionCode"' in sql, sql

    async def test_standard_agg_inner_mixed_case_column_quoted(self) -> None:
        """A standard aggregation over a bare mixed-case field quotes the inner
        column (the ``_resolve_sql`` / standard-agg inner construction path):
        ``SUM(accounts."MixedAmt")``."""
        model = SlayerDataset(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="MixedAmt", sql="MixedAmt", datatype=DataType.FLOAT),
            ],
        )
        query = SlayerQuery.model_validate({
            "population": "accounts",
            "metrics": [{"metric": "sum(accounts.MixedAmt)", "alias": "s"}],
        })
        sql = _norm(await _engine_generate(query=query, dataset=model))
        assert 'SUM(accounts."MixedAmt")' in sql

    async def test_resolved_join_target_table_mixed_case_quoted(self) -> None:
        """A relationship whose TARGET dataset has a mixed-case physical ``sql_table`` emits
        a quoted table name in the LEFT JOIN."""
        regions = SlayerDataset(
            name="regions", sql_table="MyRegions", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", sql="name", datatype=DataType.STRING),
            ],
        )
        cust = SlayerDataset(
            name="cust", sql_table="cust", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region_id", sql="region_id", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
        )
        query = SlayerQuery(
            population="cust",
            dimensions=[QueryDimension(field="regions.name")],
            distinct_dimension_values=False,
        )
        sql = _norm(await _engine_generate(
            query=query, dataset=cust, extra_datasets=[regions],
        ))
        assert 'LEFT JOIN "MyRegions"' in sql

    async def test_physical_table_name_mixed_case_quoted(self) -> None:
        """A mixed-case physical ``sql_table`` is quoted in the FROM clause."""
        model = SlayerDataset(
            name="accounts",
            sql_table="public.MyAccounts",
            data_source="test",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True)],
        )
        query = SlayerQuery.model_validate({"population": "accounts", "metrics": [{"metric": "count(accounts.*)"}]})
        sql = _norm(await _engine_generate(query=query, dataset=model))
        assert '"MyAccounts"' in sql

    async def test_lowercase_only_expression_unchanged(self, orders_dataset: SlayerDataset) -> None:
        """No spurious quoting: an all-lowercase expression emits unchanged."""
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.revenue)"}]})
        sql = _norm(await _engine_generate(query=query, dataset=orders_dataset))
        assert "SUM(orders.amount)" in sql
        assert '"amount"' not in sql

    async def test_function_name_not_quoted(self) -> None:
        """A function call whose name is mixed-case-ish stays a function — only
        Identifier nodes are quoted, not Func/Anonymous names."""
        model = SlayerDataset(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="j", expression="COALESCE(RawVal, 0)", datatype=DataType.FLOAT),
            ],
        )
        query = SlayerQuery.model_validate({"population": "accounts", "metrics": [{"metric": "sum(accounts.j)"}]})
        sql = _norm(await _engine_generate(query=query, dataset=model))
        assert "COALESCE" in sql
        assert '"COALESCE"' not in sql
        assert '"RawVal"' in sql  # the mixed-case *argument* is quoted

    async def test_mixed_case_join_target_alias_consistent(self) -> None:
        """Def/ref consistency (Codex finding #4): when a join key is mixed-case,
        the ON reference and the projected join column resolve consistently — the
        emitted SQL parses and every quoted identifier is balanced."""
        regions = SlayerDataset(
            name="regions",
            sql_table="regions",
            data_source="test",
            fields=[
                DatasetField(name="RegionId", sql="RegionId", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", sql="name", datatype=DataType.STRING),
            ],
        )
        customers = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="RegionRef", sql="RegionRef", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="regions", from_columns=["RegionRef"], to_columns=["RegionId"])],
        )
        query = SlayerQuery(
            population="customers",
            dimensions=[QueryDimension(field="regions.name")],
            distinct_dimension_values=False,
        )
        sql = await _engine_generate(
            query=query, dataset=customers, extra_datasets=[regions],
        )
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
        assert "accounts" in out
        assert '"accounts"' not in out  # lowercase qualifier untouched

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
        model = SlayerDataset(
            name="accounts",
            sql_table="public.accounts",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(
                    name="is_inactive",
                    datatype=DataType.BOOLEAN,
                    expression=ExpressionObject(dialects=[DialectEntry(
                        dialect=dialect.upper(), expression="CASE WHEN StateFlag = 'Dormant' THEN TRUE ELSE FALSE END")]),
                ),
            ],
        )
        query = SlayerQuery(
            population="accounts",
            dimensions=[QueryDimension(field="accounts.is_inactive")],
            distinct_dimension_values=False,
        )
        sql = _norm(await _engine_generate(query=query, dataset=model, dialect=dialect))
        lq, rq = self._QUOTE[dialect]
        assert f"{lq}StateFlag{rq}" in sql
