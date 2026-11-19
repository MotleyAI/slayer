"""Query-time aggregation gating, with focus on cross-model resolution.

Cross-model measures (``sum(customers.id)``) must apply the same gate stack
as local measures: PK rule, type-default eligibility, and `allowed_aggregations`
whitelist. Today the cross-model path checks only ``NUMERIC_ONLY_AGGREGATIONS``
against string columns, silently passing PK/type/whitelist violations through.
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery

from tests._engine_helpers import _engine_generate


def _orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )


def _customers_dataset(extra_columns=None, extra_aggregations=None) -> SlayerDataset:
    columns = [
        DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="name", sql="name", datatype=DataType.STRING),
    ]
    if extra_columns:
        columns.extend(extra_columns)
    return SlayerDataset(
        name="customers",
        sql_table="public.customers",
        data_source="test",
        fields=columns,
        aggregations=extra_aggregations or [],
    )


async def _generate_sql(
    *,
    orders: SlayerDataset,
    customers: SlayerDataset,
    metrics: list,
    dialect: str = "postgres",
) -> str:
    """Run the typed engine pipeline through the shared ``_engine_generate``
    helper and return the SQL string. Mirrors the original local helper but
    reuses the centralised storage/datasource/dry-run/assert-valid pipeline
    (CodeRabbit PR #153 nitpick on this file)."""
    query = SlayerQuery(population=orders.name, metrics=metrics)
    return await _engine_generate(
        query=query, dataset=orders, extra_datasets=[customers], dialect=dialect,
    )


def _orders_with_status() -> SlayerDataset:
    """Orders model carrying a local TEXT column (``status``) and a numeric
    ``rating`` column — for the DEV-1576 §3 error-split tests."""
    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="rating", sql="rating", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )


class TestCrossModelGating:
    async def test_cross_model_pk_aggregation_rejected(self) -> None:
        """``sum(customers.id)`` — PK column, sum is forbidden by the PK rule."""
        orders = _orders_dataset()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "sum(customers.id)", "alias": "result"}],
            )

    async def test_cross_model_pk_count_allowed(self) -> None:
        """PK + ``count`` is fine."""
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=_customers_dataset(),
            metrics=[{"metric": "count(customers.id)", "alias": "result"}],
        )
        assert "COUNT" in sql.upper()

    async def test_cross_model_string_sum_rejected(self) -> None:
        """``sum(customers.name)`` — string + sum is forbidden by type defaults."""
        orders = _orders_dataset()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match="not applicable|string"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "sum(customers.name)", "alias": "result"}],
            )

    async def test_cross_model_string_min_allowed(self) -> None:
        """``min(customers.name)`` is allowed (string min/max is type-default-eligible)."""
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=_customers_dataset(),
            metrics=[{"metric": "min(customers.name)", "alias": "result"}],
        )
        assert "MIN" in sql.upper()

    async def test_cross_model_whitelist_enforced(self) -> None:
        """A whitelist on the joined column restricts further than type defaults.
        ``rating`` is NUMBER (sum is type-eligible) but the whitelist is ``["avg"]``,
        so ``sum(customers.rating)`` must raise.
        """
        customers = _customers_dataset(
            extra_columns=[
                DatasetField(
                    name="rating",
                    sql="rating",
                    datatype=DataType.FLOAT,
                    allowed_aggregations=["avg"],
                ),
            ]
        )
        orders = _orders_dataset()
        with pytest.raises(ValueError, match="not allowed|allowed_aggregations|whitelist"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "sum(customers.rating)", "alias": "result"}],
            )

    async def test_cross_model_whitelist_match_allowed(self) -> None:
        """A whitelist match works."""
        customers = _customers_dataset(
            extra_columns=[
                DatasetField(
                    name="rating",
                    sql="rating",
                    datatype=DataType.FLOAT,
                    allowed_aggregations=["avg"],
                ),
            ]
        )
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=customers,
            metrics=[{"metric": "avg(customers.rating)", "alias": "result"}],
        )
        assert "AVG" in sql.upper()

    async def test_cross_model_unknown_aggregation_rejected(self) -> None:
        """Unknown function name raises with the same message style as local."""
        orders = _orders_dataset()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match="bogus_agg|not.*aggregation"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "bogus_agg(customers.name)", "alias": "result"}],
            )

    async def test_cross_model_custom_aggregation_allowed(self) -> None:
        """A custom aggregation defined on the joined model bypasses
        type-default eligibility (the formula determines applicability).
        """
        customers = _customers_dataset(
            extra_aggregations=[
                Aggregation(
                    name="name_concat",
                    expression="STRING_AGG({value}, ',')",
                ),
            ]
        )
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=customers,
            metrics=[{"metric": "name_concat(customers.name)", "alias": "result"}],
        )
        assert "STRING_AGG" in sql.upper()


class TestStatAggregationEligibility:
    """The new statistical aggregations (DEV-1317) must follow the same
    eligibility rules as other built-ins: numeric-only types, PK columns
    rejected, missing required `other=` for `corr` raises a clear error.
    """

    @pytest.fixture
    def numeric_orders(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="quantity", sql="quantity", datatype=DataType.FLOAT),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
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
        self, agg: str, fn: str, numeric_orders: SlayerDataset,
    ) -> None:
        sql = await _generate_sql(
            orders=numeric_orders,
            customers=_customers_dataset(),
            metrics=[{"metric": f"{agg}(amount)", "alias": "result"}],
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
        numeric_orders: SlayerDataset,
    ) -> None:
        sql = await _generate_sql(
            orders=numeric_orders,
            customers=_customers_dataset(),
            metrics=[
                {"metric": f"{agg}(amount, other=quantity)", "alias": "result"}
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
        self, agg: str, numeric_orders: SlayerDataset,
    ) -> None:
        customers = _customers_dataset()
        with pytest.raises(ValueError, match="not applicable|string|numeric"):
            await _generate_sql(
                orders=numeric_orders,
                customers=customers,
                metrics=[{"metric": f"{agg}(status)", "alias": "result"}],
            )

    @pytest.mark.parametrize(
        "agg",
        ["stddev_samp", "stddev_pop", "var_samp", "var_pop"],
    )
    async def test_pk_column_rejects_stat_agg(
        self, agg: str, numeric_orders: SlayerDataset,
    ) -> None:
        # PK columns are restricted to count/count_distinct regardless of type.
        customers = _customers_dataset()
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            await _generate_sql(
                orders=numeric_orders,
                customers=customers,
                metrics=[{"metric": f"{agg}(id)", "alias": "result"}],
            )

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    async def test_string_column_rejects_two_arg_stat(
        self, agg: str, numeric_orders: SlayerDataset,
    ) -> None:
        """A string LHS must be rejected for the 2-arg stats too — closes the
        coverage gap CodeRabbit flagged: the unary-stat parametrization
        already covered string LHS, but `corr`/`covar_samp`/`covar_pop`
        with `other=` slipped past it.
        """
        customers = _customers_dataset()
        with pytest.raises(ValueError, match="not applicable|string|numeric"):
            await _generate_sql(
                orders=numeric_orders,
                customers=customers,
                metrics=[
                    {"metric": f"{agg}(status, other=quantity)", "alias": "result"}
                ],
            )

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    async def test_pk_column_rejects_two_arg_stat(
        self, agg: str, numeric_orders: SlayerDataset,
    ) -> None:
        customers = _customers_dataset()
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            await _generate_sql(
                orders=numeric_orders,
                customers=customers,
                metrics=[
                    {"metric": f"{agg}(id, other=quantity)", "alias": "result"}
                ],
            )

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    async def test_two_arg_stat_missing_other_raises(
        self, agg: str, numeric_orders: SlayerDataset,
    ) -> None:
        # Missing required `other=` parameter must raise with a clear message
        # naming the parameter, mirroring weighted_avg's missing-`weight=`
        # behaviour.
        customers = _customers_dataset()
        with pytest.raises(ValueError, match=r"requires parameter 'other'|other="):
            await _generate_sql(
                orders=numeric_orders,
                customers=customers,
                metrics=[{"metric": f"{agg}(amount)", "alias": "result"}],
            )


class TestCrossModelColumnFilter:
    """Codex Major 2: a Column.filter on a joined column must apply when
    that column is referenced cross-model (e.g. ``sum(customers.completed_rev)``).
    """

    async def test_cross_model_column_filter_applied(self) -> None:
        """When a joined-model column has ``filter``, it should appear inside
        the aggregation as a CASE-WHEN — same as for local measures.
        """
        customers = _customers_dataset(
            extra_columns=[
                DatasetField(
                    name="completed_rev",
                    sql="amount",
                    datatype=DataType.FLOAT,
                    filter="status = 'completed'",
                ),
            ]
        )
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=customers,
            metrics=[
                {"metric": "sum(customers.completed_rev)", "alias": "result"}
            ],
        )
        assert "CASE" in sql.upper()
        assert "completed" in sql.lower()


class TestDev1576UnknownVsDisallowed:
    """DEV-1576 §3 — split 'unknown aggregation name' from 'not allowed for
    this column type'. Local (non-cross-model) path only.
    """

    async def test_unknown_name_raises_unknown_aggregation(self) -> None:
        orders = _orders_with_status()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match=r"Unknown function 'bogus'"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "bogus(orders.amount)", "alias": "result"}],
            )

    async def test_unknown_name_lists_known_aggregations(self) -> None:
        orders = _orders_with_status()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match=r"Known:"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "bogus(orders.amount)", "alias": "result"}],
            )

    async def test_unknown_name_suggests_close_match(self) -> None:
        # ``stdev`` is a near-miss for stddev_samp/stddev_pop. It is NOT an
        # alias (so §1 leaves it), and enrichment should offer a suggestion.
        orders = _orders_with_status()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match=r"Did you mean 'stddev_(samp|pop)'"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "stdev(orders.amount)", "alias": "result"}],
            )

    async def test_type_disallowed_keeps_not_applicable_wording(self) -> None:
        # ``sum`` is a KNOWN aggregation but not eligible for a TEXT column —
        # keep the existing 'not applicable to <TYPE> column' message.
        orders = _orders_with_status()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match=r"not applicable to String field"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "sum(orders.status)", "alias": "result"}],
            )

    async def test_type_disallowed_is_not_reported_as_unknown(self) -> None:
        orders = _orders_with_status()
        customers = _customers_dataset()
        with pytest.raises(ValueError) as exc:
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "sum(orders.status)", "alias": "result"}],
            )
        assert "Unknown function" not in str(exc.value)

    async def test_star_unknown_gets_standard_message(self) -> None:
        # DEV-1826: the binder validates the aggregation NAME globally before
        # any per-column gate, so ``bogus(*)`` gets the standard unknown-
        # aggregation error instead of escaping to SQL generation (where it
        # used to surface as the star-specific message).
        orders = _orders_with_status()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match=r"Unknown function 'bogus'"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "bogus(orders.*)", "alias": "result"}],
            )

    async def test_custom_agg_known_but_disallowed_keeps_not_allowed(self) -> None:
        # A custom aggregation known model-wide but absent from a column's
        # allowed_aggregations whitelist → 'not allowed for column', NOT
        # 'Unknown function'.
        orders = SlayerDataset(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(
                    name="rating",
                    sql="rating",
                    datatype=DataType.FLOAT,
                    allowed_aggregations=["avg"],
                ),
            ],
            aggregations=[Aggregation(name="myagg", expression="SUM({value}) * 2")],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
        customers = _customers_dataset()
        with pytest.raises(ValueError) as exc:
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "myagg(orders.rating)", "alias": "result"}],
            )
        assert "Unknown function" not in str(exc.value)
        assert "not allowed" in str(exc.value)

    async def test_custom_agg_allowed_when_no_whitelist(self) -> None:
        # Guard: the unknown-name check must not break a legitimate custom
        # aggregation on a column with no whitelist.
        orders = SlayerDataset(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
            aggregations=[Aggregation(name="myagg", expression="SUM({value}) * 2")],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
        sql = await _generate_sql(
            orders=orders,
            customers=_customers_dataset(),
            metrics=[{"metric": "myagg(orders.amount)", "alias": "result"}],
        )
        assert "SUM" in sql.upper()

    async def test_healed_alias_does_not_trigger_unknown(self) -> None:
        # ``countd`` heals to count_distinct in §1 — it must reach SQL, not
        # the §3 'Unknown function' error.
        sql = await _generate_sql(
            orders=_orders_with_status(),
            customers=_customers_dataset(),
            metrics=[{"metric": "countd(orders.amount)", "alias": "result"}],
        )
        assert "COUNT(DISTINCT" in sql.upper()

    @pytest.mark.parametrize(
        "raw,sql_fn", [("countd", "COUNT(DISTINCT"), ("stddev", "STDDEV")],
    )
    async def test_healed_alias_in_filter_reaches_having(
        self, raw: str, sql_fn: str,
    ) -> None:
        # Binding-time healing must also fire on the filter path (DEV-1833
        # migrated filters onto the typed parser): an aliased aggregation in a
        # HAVING predicate heals just like a measure.
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}],
            "filter": [f"{raw}(amount) > 5"],
        })
        sql = await _engine_generate(
            query=query, dataset=_orders_with_status(),
            extra_datasets=[_customers_dataset()], dialect="postgres",
        )
        up = sql.upper()
        assert "HAVING" in up
        assert sql_fn in up


class TestDev1576RoundAbsGeneration:
    """DEV-1576 §2 — round()/abs() compile in a formula; Postgres needs a
    numeric CAST for 2-arg round (round(double precision, int) doesn't
    exist), while SQLite/DuckDB round natively over DOUBLE.
    """

    async def test_round_two_args_postgres_casts_to_numeric(self) -> None:
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=_customers_dataset(),
            metrics=[{"metric": "round(sum(orders.amount), 2)", "alias": "r"}],
            dialect="postgres",
        )
        up = sql.upper()
        assert "ROUND(CAST(" in up
        assert "AS DECIMAL" in up or "AS NUMERIC" in up

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
    async def test_round_two_args_non_postgres_no_cast(self, dialect: str) -> None:
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=_customers_dataset(),
            metrics=[{"metric": "round(sum(orders.amount), 2)", "alias": "r"}],
            dialect=dialect,
        )
        up = sql.upper()
        assert "ROUND(" in up
        # No numeric cast injected — these backends round DOUBLE natively.
        assert "ROUND(CAST(" not in up

    async def test_round_one_arg_postgres_no_cast(self) -> None:
        # 1-arg round(double precision) exists on Postgres — no cast needed.
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=_customers_dataset(),
            metrics=[{"metric": "round(sum(orders.amount))", "alias": "r"}],
            dialect="postgres",
        )
        up = sql.upper()
        assert "ROUND(" in up
        assert "ROUND(CAST(" not in up

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb"])
    async def test_abs_generates_unchanged(self, dialect: str) -> None:
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=_customers_dataset(),
            metrics=[{"metric": "abs(sum(orders.amount))", "alias": "a"}],
            dialect=dialect,
        )
        assert "ABS(" in sql.upper()


class TestDev1576UnknownNamePrecedence:
    """DEV-1576 §3 — the unknown-name guard fires BEFORE the PK / whitelist /
    type gates, and the suggestion list reflects model-wide custom aggs."""

    def _orders_pk_whitelist_custom(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(
                    name="rating",
                    sql="rating",
                    datatype=DataType.FLOAT,
                    allowed_aggregations=["avg"],
                ),
            ],
            aggregations=[Aggregation(name="myagg", expression="SUM({value}) * 2")],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )

    @pytest.mark.parametrize("column", ["rating", "id", "status"])
    async def test_unknown_name_beats_pk_whitelist_and_type_gates(
        self, column: str,
    ) -> None:
        # rating has a whitelist, id is PK, status is TEXT — for an unknown
        # aggregation name all three must surface 'Unknown function', not
        # the whitelist / PK / type-applicability message.
        orders = self._orders_pk_whitelist_custom()
        customers = _customers_dataset()
        with pytest.raises(ValueError, match=r"Unknown function 'bogus'"):
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": f"bogus({column})", "alias": "result"}],
            )

    async def test_known_list_includes_custom_aggregations(self) -> None:
        orders = self._orders_pk_whitelist_custom()
        customers = _customers_dataset()
        with pytest.raises(ValueError) as exc:
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "bogus(orders.amount)", "alias": "result"}],
            )
        assert "myagg" in str(exc.value)

    async def test_no_did_you_mean_for_poor_match(self) -> None:
        orders = self._orders_pk_whitelist_custom()
        customers = _customers_dataset()
        with pytest.raises(ValueError) as exc:
            await _generate_sql(
                orders=orders,
                customers=customers,
                metrics=[{"metric": "zzzzz(orders.amount)", "alias": "result"}],
            )
        assert "Did you mean" not in str(exc.value)


class TestDev1576CustomAggPrecedence:
    """DEV-1576 (Codex): a model custom aggregation named like an alias key
    (countd/stddev/var/...) or a builtin casing must take precedence over
    alias healing — the heal must not silently shadow it."""

    async def test_custom_agg_named_like_alias_takes_precedence(self) -> None:
        orders = SlayerDataset(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
            aggregations=[
                Aggregation(name="countd", expression="COUNT(DISTINCT {value}) + 1"),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
        sql = await _generate_sql(
            orders=orders,
            customers=_customers_dataset(),
            metrics=[{"metric": "countd(orders.amount)", "alias": "result"}],
        )
        # The custom formula (… + 1) must win, not the healed builtin
        # count_distinct (plain COUNT(DISTINCT …)).
        assert "+ 1" in sql

    async def test_alias_heals_when_no_colliding_custom_agg(self) -> None:
        # Same query on a model WITHOUT a colliding custom agg still heals to
        # the builtin count_distinct.
        sql = await _generate_sql(
            orders=_orders_dataset(),
            customers=_customers_dataset(),
            metrics=[{"metric": "countd(orders.amount)", "alias": "result"}],
        )
        assert "COUNT(DISTINCT" in sql.upper()
        assert "+ 1" not in sql
