"""Query-time aggregation gating, with focus on cross-model resolution.

Cross-model measures (``customers.id:sum``) must apply the same gate stack
as local measures: PK rule, type-default eligibility, and `allowed_aggregations`
whitelist. Today the cross-model path checks only ``NUMERIC_ONLY_AGGREGATIONS``
against string columns, silently passing PK/type/whitelist violations through.
"""

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _assert_valid_sql


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
) -> str:
    """Run the typed engine pipeline and return the SQL string."""
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_datasource(
            DatasourceConfig(name=orders.data_source, type="postgres")
        )
        await storage.save_model(orders)
        await storage.save_model(customers)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(source_model="orders", measures=measures)
        response = await engine.execute(query, dry_run=True)
        sql = response.sql
        assert sql is not None, "engine.execute(dry_run=True) returned no SQL"
        _assert_valid_sql(sql, dialect="postgres")
        return sql


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
