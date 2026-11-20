"""Result keys do not change with the canonical spelling (spec: aggregations/functional-form › Result keys are unchanged).

Every key below was recorded on ``main`` before the colon spelling was removed.
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.query import QueryDimension, SlayerQuery
from tests._dev1920_fixtures import FISCAL_QUARTER, StoreFactory, make_store, run  # noqa: F401 — fixture

MONTH = {"field": "orders.created_at", "granularity": "month"}

#: (unnamed metric, its result key).
MEASURE_KEYS = [
    ("sum(orders.amount)", "orders.amount_sum"),
    ("count(orders.*)", "orders._count"),
    ("sum(orders.amount) / count(orders.*)", "orders.amount_sum_count"),
    ("sum(orders.amount) FILTER (WHERE orders.status = 'paid')", "orders.amount_sum_filter_where_status_paid"),
    ('sum(orders."amount")', "orders.amount_sum"),
    ("sum(customers.regions.population)", "customers.regions.population_sum"),
    ("count(customers.*)", "customers._count"),
    ("count(customers.name)", "customers.name_count"),
    ("percentile(orders.price, p=0.9)", "orders.price_percentile_p_0_9"),
    ("percentile(orders.price, 0.9)", "orders.price_percentile_p_0_9"),
    ("last(orders.balance, orders.updated_at)", "orders.balance_last_updated_at"),
    ("weighted_avg(orders.price, orders.amount)", "orders.price_weighted_avg_weight_amount"),
    ("count_distinct(orders.customer_id)", "orders.customer_id_count_distinct"),
    ("sum(orders.amount::INT)", "orders.cast_amount_as_int_sum"),
    ("sum(CAST(orders.amount AS INT))", "orders.cast_amount_as_int_sum"),
    ("sum(orders.amount) * {k}", "orders.amount_sum_k"),
    ("sum(orders.amount) / max(orders.price) + min(orders.balance)", "orders.amount_sum_price_max_balance_min"),
    ("avg(orders.price) - min(orders.price)", "orders.price_avg_price_min"),
    # 48 characters: the longest name kept whole.
    ("sum(orders.amount) + sum(orders.price) + sum(orders.balance) + count(orders.*) + max(orders.price)",
     "orders.amount_sum_price_sum_balance_sum_count_price_max"),
    # Over 48 characters: shortened with a hash of the canonical text.
    ("sum(orders.amount) + sum(orders.price) + sum(orders.balance) + count(orders.*) + max(orders.price) + min(orders.price)",
     "orders.amount_sum_price_sum_balance_4ffaf4a2_rice_min"),
    ("sum(orders.amount) FILTER (WHERE orders.status = 'paid' AND orders.region = 'North')",
     "orders.amount_sum_filter_where_stat_5595a4a5_on_north"),
]

#: (query fields, the result keys in order).
QUERY_KEYS = [
    pytest.param({"metrics": ["cumsum(sum(orders.amount))"], "time_dimensions": [MONTH]},
                 ["orders.created_at", "orders.cumsum_amount_sum"], id="transform"),
    pytest.param({"metrics": ["sum(orders.amount, window='30d')"], "time_dimensions": [MONTH]},
                 ["orders.created_at", "orders.amount_sum_window_30d"], id="window-kwarg"),
    pytest.param({"metrics": ["sum(orders.amount)"], "dimensions": ["sum(orders.amount, partition_by=orders.region)"]},
                 ["orders.amount_sum_partition_by_region", "orders.amount_sum"], id="partitioned-dimension"),
    pytest.param({"metrics": ["sum(orders.amount)"], "dimensions": ["upper(orders.status)"]},
                 ["orders.upper_status", "orders.amount_sum"], id="scalar-dimension"),
    pytest.param({"metrics": ["sum(orders.amount)"], "dimensions": ["orders.status || '-' || orders.region"]},
                 ["orders.concat_concat_status_region", "orders.amount_sum"], id="concat-dimension"),
    pytest.param({"metrics": ["sum(orders.amount)"],
                  "dimensions": ["case when sum(orders.amount, partition_by=orders.customer_id) > 50 then 'high' else 'other' end"]},
                 ["orders.iif_amount_sum_partition_by_002eb04b_gh_other", "orders.amount_sum"],
                 id="hashed-dimension"),
    pytest.param({"metrics": ["sum(orders.amount)"], "dimensions": ["date_trunc('month', orders.created_at)"]},
                 ["orders.created_at", "orders.amount_sum"], id="date-trunc-dimension"),
    pytest.param({"metrics": ["sum(orders.amount)"], "time_dimensions": ["date_trunc('month', orders.created_at)"]},
                 ["orders.created_at", "orders.amount_sum"], id="date-trunc-time-dimension"),
    pytest.param({"metrics": ["sum(orders.amount)"], "dimensions": ["date_trunc('fiscal_quarter', orders.created_at)"]},
                 ["orders.created_at", "orders.amount_sum"], id="custom-granularity-dimension"),
]

#: (unnamed computed-dimension expression, its auto-name).
DIMENSION_NAMES = [
    ("sum(orders.amount, partition_by=orders.region)", "amount_sum_partition_by_region"),
    ("count(*, partition_by=region)", "count_partition_by_region"),
    ("case when sum(orders.amount, partition_by=orders.customer_id) > 50 then 'high' else 'other' end",
     "iif_amount_sum_partition_by_002eb04b_gh_other"),
    ("case when sum(amount, partition_by=customer_id) > 50 and count(*, partition_by=customer_id) > 1 "
     "then 'high' else 'other' end", "iif_amount_sum_partition_by_48b198e2_gh_other"),
]


async def _keys(factory: StoreFactory, fields: dict[str, Any]) -> list[str]:
    storage = await factory(granularities=[FISCAL_QUARTER])
    resp = await run(storage, {"population": "orders", **fields})
    return resp.columns


@pytest.mark.parametrize(("measure", "key"), MEASURE_KEYS)
async def test_measure_key(make_store: StoreFactory, measure: str, key: str) -> None:  # noqa: F811
    assert await _keys(make_store, {"metrics": [measure], "variables": {"k": 2}}) == [key]


@pytest.mark.parametrize(("fields", "keys"), QUERY_KEYS)
async def test_query_keys(make_store: StoreFactory, fields: dict, keys: list[str]) -> None:  # noqa: F811
    assert await _keys(make_store, fields) == keys


@pytest.mark.parametrize(("expression", "name"), DIMENSION_NAMES)
def test_computed_dimension_auto_name(expression: str, name: str) -> None:
    assert QueryDimension(field=expression).name == name
    [dim] = SlayerQuery.model_validate({"population": "orders", "dimensions": [expression]}).dimensions or []
    assert dim.ref is None
    assert dim.name == name
