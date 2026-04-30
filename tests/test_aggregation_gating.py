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
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Column(name="amount", sql="amount", type=DataType.NUMBER),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def _customers_model(extra_columns=None, extra_aggregations=None) -> SlayerModel:
    columns = [
        Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
        Column(name="name", sql="name", type=DataType.STRING),
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
    """Run a real engine + SQL generator and return the SQL string."""
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_model(orders)
        await storage.save_model(customers)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(source_model="orders", measures=measures)
        enriched = await engine._enrich(query=query, model=orders, named_queries={})
        return SQLGenerator(dialect="postgres").generate(enriched=enriched)


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
                    type=DataType.NUMBER,
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
                    type=DataType.NUMBER,
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
