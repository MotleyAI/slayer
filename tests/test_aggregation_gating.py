"""Query-time aggregation gating, with focus on cross-model resolution.

Cross-model measures (``customers.id:sum``) must apply the same gate stack
as local measures: PK rule, type-default eligibility, and `allowed_aggregations`
whitelist. Today the cross-model path checks only ``NUMERIC_ONLY_AGGREGATIONS``
against string columns, silently passing PK/type/whitelist violations through.
"""

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def _customers_model(extra_columns=None, extra_aggregations=None) -> SlayerModel:
    columns = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="name", sql="name", type=DataType.TEXT),
    ]
    if extra_columns:
        columns.extend(extra_columns)
    return SlayerModel(
        name="customers",
        sql_table="public.customers",
        data_source="test",
        columns=columns,
        aggregations=extra_aggregations or [],
    )


async def _generate_sql(
    *,
    orders: SlayerModel,
    customers: SlayerModel,
    measures: list,
    dialect: str = "postgres",
) -> str:
    """Run a real engine + SQL generator and return the SQL string."""
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_model(orders)
        await storage.save_model(customers)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(source_model="orders", measures=measures)
        enriched = await engine._enrich(query=query, model=orders, named_queries={})
        return SQLGenerator(dialect=dialect).generate(enriched=enriched)


def _orders_with_status() -> SlayerModel:
    """Orders model carrying a local TEXT column (``status``) and a numeric
    ``rating`` column — for the DEV-1576 §3 error-split tests."""
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="rating", sql="rating", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


class TestCrossModelGating:
    async def test_cross_model_pk_aggregation_rejected(self) -> None:
        """``customers.id:sum`` — PK column, sum is forbidden by the PK rule."""
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            await _generate_sql(
                orders=_orders_model(),
                customers=_customers_model(),
                measures=[{"formula": "customers.id:sum", "name": "result"}],
            )

    async def test_cross_model_pk_count_allowed(self) -> None:
        """PK + ``count`` is fine."""
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=_customers_model(),
            measures=[{"formula": "customers.id:count", "name": "result"}],
        )
        assert "COUNT" in sql.upper()

    async def test_cross_model_string_sum_rejected(self) -> None:
        """``customers.name:sum`` — string + sum is forbidden by type defaults."""
        with pytest.raises(ValueError, match="not applicable|string"):
            await _generate_sql(
                orders=_orders_model(),
                customers=_customers_model(),
                measures=[{"formula": "customers.name:sum", "name": "result"}],
            )

    async def test_cross_model_string_min_allowed(self) -> None:
        """``customers.name:min`` is allowed (string min/max is type-default-eligible)."""
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=_customers_model(),
            measures=[{"formula": "customers.name:min", "name": "result"}],
        )
        assert "MIN" in sql.upper()

    async def test_cross_model_whitelist_enforced(self) -> None:
        """A whitelist on the joined column restricts further than type defaults.
        ``rating`` is NUMBER (sum is type-eligible) but the whitelist is ``["avg"]``,
        so ``customers.rating:sum`` must raise.
        """
        customers = _customers_model(
            extra_columns=[
                Column(
                    name="rating",
                    sql="rating",
                    type=DataType.DOUBLE,
                    allowed_aggregations=["avg"],
                ),
            ]
        )
        with pytest.raises(ValueError, match="not allowed|allowed_aggregations|whitelist"):
            await _generate_sql(
                orders=_orders_model(),
                customers=customers,
                measures=[{"formula": "customers.rating:sum", "name": "result"}],
            )

    async def test_cross_model_whitelist_match_allowed(self) -> None:
        """A whitelist match works."""
        customers = _customers_model(
            extra_columns=[
                Column(
                    name="rating",
                    sql="rating",
                    type=DataType.DOUBLE,
                    allowed_aggregations=["avg"],
                ),
            ]
        )
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=customers,
            measures=[{"formula": "customers.rating:avg", "name": "result"}],
        )
        assert "AVG" in sql.upper()

    async def test_cross_model_unknown_aggregation_rejected(self) -> None:
        """Unknown aggregation name raises with the same message style as local."""
        with pytest.raises(ValueError, match="bogus_agg|not.*aggregation"):
            await _generate_sql(
                orders=_orders_model(),
                customers=_customers_model(),
                measures=[{"formula": "customers.name:bogus_agg", "name": "result"}],
            )

    async def test_cross_model_custom_aggregation_allowed(self) -> None:
        """A custom aggregation defined on the joined model bypasses
        type-default eligibility (the formula determines applicability).
        """
        customers = _customers_model(
            extra_aggregations=[
                Aggregation(
                    name="name_concat",
                    formula="STRING_AGG({value}, ',')",
                ),
            ]
        )
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=customers,
            measures=[{"formula": "customers.name:name_concat", "name": "result"}],
        )
        assert "STRING_AGG" in sql.upper()


class TestStatAggregationEligibility:
    """The new statistical aggregations (DEV-1317) must follow the same
    eligibility rules as other built-ins: numeric-only types, PK columns
    rejected, missing required `other=` for `corr` raises a clear error.
    """

    @pytest.fixture
    def numeric_orders(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

    # Postgres preserves canonical names (no VAR_SAMP→VARIANCE rewrite —
    # that's a SQLite/MySQL/DuckDB sqlglot quirk), so we can pin the exact
    # function-call shape here. `_generate_sql` is hard-coded to Postgres.
    @pytest.mark.parametrize(
        "agg,fn",
        [
            ("stddev_samp", "STDDEV_SAMP"),
            ("stddev_pop", "STDDEV_POP"),
            ("var_samp", "VAR_SAMP"),
            ("var_pop", "VAR_POP"),
        ],
    )
    async def test_numeric_column_accepts_stat_agg(
        self, agg: str, fn: str, numeric_orders: SlayerModel,
    ) -> None:
        sql = await _generate_sql(
            orders=numeric_orders,
            customers=_customers_model(),
            measures=[{"formula": f"amount:{agg}", "name": "result"}],
        )
        # Pin the function-call shape: family name immediately followed by
        # the qualified value column. The earlier "( in sql" check passed
        # for any SELECT and didn't prove the aggregate survived enrichment
        # (Codex #6 / CodeRabbit nitpick on PR #82).
        assert f"{fn}(orders.amount)" in sql

    @pytest.mark.parametrize(
        "agg,sql_fn",
        [
            ("corr", "CORR"),
            ("covar_samp", "COVAR_SAMP"),
            ("covar_pop", "COVAR_POP"),
        ],
    )
    async def test_numeric_two_arg_stat_with_other_kwarg_accepted(
        self,
        agg: str,
        sql_fn: str,
        numeric_orders: SlayerModel,
    ) -> None:
        sql = await _generate_sql(
            orders=numeric_orders,
            customers=_customers_model(),
            measures=[
                {"formula": f"amount:{agg}(other=quantity)", "name": "result"}
            ],
        )
        # Both legs must be qualified and appear in the function call's
        # two-arg slot in canonical Postgres-style order.
        assert f"{sql_fn}(orders.amount, orders.quantity)" in sql

    @pytest.mark.parametrize(
        "agg",
        ["stddev_samp", "stddev_pop", "var_samp", "var_pop"],
    )
    async def test_string_column_rejects_stat_agg(
        self, agg: str, numeric_orders: SlayerModel,
    ) -> None:
        with pytest.raises(ValueError, match="not applicable|string|numeric"):
            await _generate_sql(
                orders=numeric_orders,
                customers=_customers_model(),
                measures=[{"formula": f"status:{agg}", "name": "result"}],
            )

    @pytest.mark.parametrize(
        "agg",
        ["stddev_samp", "stddev_pop", "var_samp", "var_pop"],
    )
    async def test_pk_column_rejects_stat_agg(
        self, agg: str, numeric_orders: SlayerModel,
    ) -> None:
        # PK columns are restricted to count/count_distinct regardless of type.
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            await _generate_sql(
                orders=numeric_orders,
                customers=_customers_model(),
                measures=[{"formula": f"id:{agg}", "name": "result"}],
            )

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    async def test_string_column_rejects_two_arg_stat(
        self, agg: str, numeric_orders: SlayerModel,
    ) -> None:
        """A string LHS must be rejected for the 2-arg stats too — closes the
        coverage gap CodeRabbit flagged: the unary-stat parametrization
        already covered string LHS, but `corr`/`covar_samp`/`covar_pop`
        with `other=` slipped past it.
        """
        with pytest.raises(ValueError, match="not applicable|string|numeric"):
            await _generate_sql(
                orders=numeric_orders,
                customers=_customers_model(),
                measures=[
                    {"formula": f"status:{agg}(other=quantity)", "name": "result"}
                ],
            )

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    async def test_pk_column_rejects_two_arg_stat(
        self, agg: str, numeric_orders: SlayerModel,
    ) -> None:
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            await _generate_sql(
                orders=numeric_orders,
                customers=_customers_model(),
                measures=[
                    {"formula": f"id:{agg}(other=quantity)", "name": "result"}
                ],
            )

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    async def test_two_arg_stat_missing_other_raises(
        self, agg: str, numeric_orders: SlayerModel,
    ) -> None:
        # Missing required `other=` parameter must raise with a clear message
        # naming the parameter, mirroring weighted_avg's missing-`weight=`
        # behaviour.
        with pytest.raises(ValueError, match=r"requires parameter 'other'|other="):
            await _generate_sql(
                orders=numeric_orders,
                customers=_customers_model(),
                measures=[{"formula": f"amount:{agg}", "name": "result"}],
            )


class TestCrossModelColumnFilter:
    """Codex Major 2: a Column.filter on a joined column must apply when
    that column is referenced cross-model (e.g. ``customers.completed_rev:sum``).
    """

    async def test_cross_model_column_filter_applied(self) -> None:
        """When a joined-model column has ``filter``, it should appear inside
        the aggregation as a CASE-WHEN — same as for local measures.
        """
        customers = _customers_model(
            extra_columns=[
                Column(
                    name="completed_rev",
                    sql="amount",
                    type=DataType.DOUBLE,
                    filter="status = 'completed'",
                ),
            ]
        )
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=customers,
            measures=[
                {"formula": "customers.completed_rev:sum", "name": "result"}
            ],
        )
        assert "CASE" in sql.upper()
        assert "completed" in sql.lower()


class TestDev1576UnknownVsDisallowed:
    """DEV-1576 §3 — split 'unknown aggregation name' from 'not allowed for
    this column type'. Local (non-cross-model) path only.
    """

    async def test_unknown_name_raises_unknown_aggregation(self) -> None:
        with pytest.raises(ValueError, match=r"Unknown aggregation 'bogus'"):
            await _generate_sql(
                orders=_orders_with_status(),
                customers=_customers_model(),
                measures=[{"formula": "amount:bogus", "name": "result"}],
            )

    async def test_unknown_name_lists_known_aggregations(self) -> None:
        with pytest.raises(ValueError, match=r"Known:"):
            await _generate_sql(
                orders=_orders_with_status(),
                customers=_customers_model(),
                measures=[{"formula": "amount:bogus", "name": "result"}],
            )

    async def test_unknown_name_suggests_close_match(self) -> None:
        # ``stdev`` is a near-miss for stddev_samp/stddev_pop. It is NOT an
        # alias (so §1 leaves it), and enrichment should offer a suggestion.
        with pytest.raises(ValueError, match=r"Did you mean 'stddev_(samp|pop)'"):
            await _generate_sql(
                orders=_orders_with_status(),
                customers=_customers_model(),
                measures=[{"formula": "amount:stdev", "name": "result"}],
            )

    async def test_type_disallowed_keeps_not_applicable_wording(self) -> None:
        # ``sum`` is a KNOWN aggregation but not eligible for a TEXT column —
        # keep the existing 'not applicable to <TYPE> column' message.
        with pytest.raises(ValueError, match=r"not applicable to TEXT column"):
            await _generate_sql(
                orders=_orders_with_status(),
                customers=_customers_model(),
                measures=[{"formula": "status:sum", "name": "result"}],
            )

    async def test_type_disallowed_is_not_reported_as_unknown(self) -> None:
        with pytest.raises(ValueError) as exc:
            await _generate_sql(
                orders=_orders_with_status(),
                customers=_customers_model(),
                measures=[{"formula": "status:sum", "name": "result"}],
            )
        assert "Unknown aggregation" not in str(exc.value)

    async def test_star_unknown_keeps_star_message(self) -> None:
        # ``*`` only supports count — keep the dedicated star message rather
        # than the generic 'Unknown aggregation' wording.
        with pytest.raises(ValueError, match=r"\*:count"):
            await _generate_sql(
                orders=_orders_with_status(),
                customers=_customers_model(),
                measures=[{"formula": "*:bogus", "name": "result"}],
            )

    async def test_custom_agg_known_but_disallowed_keeps_not_allowed(self) -> None:
        # A custom aggregation known model-wide but absent from a column's
        # allowed_aggregations whitelist → 'not allowed for column', NOT
        # 'Unknown aggregation'.
        orders = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(
                    name="rating",
                    sql="rating",
                    type=DataType.DOUBLE,
                    allowed_aggregations=["avg"],
                ),
            ],
            aggregations=[Aggregation(name="myagg", formula="SUM({value}) * 2")],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        with pytest.raises(ValueError) as exc:
            await _generate_sql(
                orders=orders,
                customers=_customers_model(),
                measures=[{"formula": "rating:myagg", "name": "result"}],
            )
        assert "Unknown aggregation" not in str(exc.value)
        assert "not allowed" in str(exc.value)

    async def test_custom_agg_allowed_when_no_whitelist(self) -> None:
        # Guard: the unknown-name check must not break a legitimate custom
        # aggregation on a column with no whitelist.
        orders = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            aggregations=[Aggregation(name="myagg", formula="SUM({value}) * 2")],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        sql = await _generate_sql(
            orders=orders,
            customers=_customers_model(),
            measures=[{"formula": "amount:myagg", "name": "result"}],
        )
        assert "SUM" in sql.upper()

    async def test_healed_alias_does_not_trigger_unknown(self) -> None:
        # ``countd`` heals to count_distinct in §1 — it must reach SQL, not
        # the §3 'Unknown aggregation' error.
        sql = await _generate_sql(
            orders=_orders_with_status(),
            customers=_customers_model(),
            measures=[{"formula": "amount:countd", "name": "result"}],
        )
        assert "COUNT(DISTINCT" in sql.upper()


class TestDev1576RoundAbsGeneration:
    """DEV-1576 §2 — round()/abs() compile in a formula; Postgres needs a
    numeric CAST for 2-arg round (round(double precision, int) doesn't
    exist), while SQLite/DuckDB round natively over DOUBLE.
    """

    async def test_round_two_args_postgres_casts_to_numeric(self) -> None:
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=_customers_model(),
            measures=[{"formula": "round(amount:sum, 2)", "name": "r"}],
            dialect="postgres",
        )
        up = sql.upper()
        assert "ROUND(CAST(" in up
        assert "AS DECIMAL" in up or "AS NUMERIC" in up

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_round_two_args_non_postgres_no_cast(self, dialect: str) -> None:
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=_customers_model(),
            measures=[{"formula": "round(amount:sum, 2)", "name": "r"}],
            dialect=dialect,
        )
        up = sql.upper()
        assert "ROUND(" in up
        # No numeric cast injected — these backends round DOUBLE natively.
        assert "ROUND(CAST(" not in up

    async def test_round_one_arg_postgres_no_cast(self) -> None:
        # 1-arg round(double precision) exists on Postgres — no cast needed.
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=_customers_model(),
            measures=[{"formula": "round(amount:sum)", "name": "r"}],
            dialect="postgres",
        )
        up = sql.upper()
        assert "ROUND(" in up
        assert "ROUND(CAST(" not in up

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb"])
    async def test_abs_generates_unchanged(self, dialect: str) -> None:
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=_customers_model(),
            measures=[{"formula": "abs(amount:sum)", "name": "a"}],
            dialect=dialect,
        )
        assert "ABS(" in sql.upper()


class TestDev1576UnknownNamePrecedence:
    """DEV-1576 §3 — the unknown-name guard fires BEFORE the PK / whitelist /
    type gates, and the suggestion list reflects model-wide custom aggs."""

    def _orders_pk_whitelist_custom(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(
                    name="rating",
                    sql="rating",
                    type=DataType.DOUBLE,
                    allowed_aggregations=["avg"],
                ),
            ],
            aggregations=[Aggregation(name="myagg", formula="SUM({value}) * 2")],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

    @pytest.mark.parametrize("column", ["rating", "id", "status"])
    async def test_unknown_name_beats_pk_whitelist_and_type_gates(
        self, column: str,
    ) -> None:
        # rating has a whitelist, id is PK, status is TEXT — for an unknown
        # aggregation name all three must surface 'Unknown aggregation', not
        # the whitelist / PK / type-applicability message.
        with pytest.raises(ValueError, match=r"Unknown aggregation 'bogus'"):
            await _generate_sql(
                orders=self._orders_pk_whitelist_custom(),
                customers=_customers_model(),
                measures=[{"formula": f"{column}:bogus", "name": "result"}],
            )

    async def test_known_list_includes_custom_aggregations(self) -> None:
        with pytest.raises(ValueError) as exc:
            await _generate_sql(
                orders=self._orders_pk_whitelist_custom(),
                customers=_customers_model(),
                measures=[{"formula": "amount:bogus", "name": "result"}],
            )
        assert "myagg" in str(exc.value)

    async def test_no_did_you_mean_for_poor_match(self) -> None:
        with pytest.raises(ValueError) as exc:
            await _generate_sql(
                orders=self._orders_pk_whitelist_custom(),
                customers=_customers_model(),
                measures=[{"formula": "amount:zzzzz", "name": "result"}],
            )
        assert "Did you mean" not in str(exc.value)


class TestDev1576CustomAggPrecedence:
    """DEV-1576 (Codex): a model custom aggregation named like an alias key
    (countd/stddev/var/...) or a builtin casing must take precedence over
    alias healing — the heal must not silently shadow it."""

    async def test_custom_agg_named_like_alias_takes_precedence(self) -> None:
        orders = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            aggregations=[
                Aggregation(name="countd", formula="COUNT(DISTINCT {value}) + 1"),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        sql = await _generate_sql(
            orders=orders,
            customers=_customers_model(),
            measures=[{"formula": "amount:countd", "name": "result"}],
        )
        # The custom formula (… + 1) must win, not the healed builtin
        # count_distinct (plain COUNT(DISTINCT …)).
        assert "+ 1" in sql

    async def test_alias_heals_when_no_colliding_custom_agg(self) -> None:
        # Same query on a model WITHOUT a colliding custom agg still heals to
        # the builtin count_distinct.
        sql = await _generate_sql(
            orders=_orders_model(),
            customers=_customers_model(),
            measures=[{"formula": "amount:countd", "name": "result"}],
        )
        assert "COUNT(DISTINCT" in sql.upper()
        assert "+ 1" not in sql
