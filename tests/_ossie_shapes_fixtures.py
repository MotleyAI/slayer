"""The no-result-change battery for Ossie's query shapes: each case in its 1.x spelling and its Ossie forms.

The 1.x spellings below are deliberate: keep this file out of any respelling pass.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from slayer.core.models import SlayerDataset
from slayer.engine.query_engine import SlayerResponse
from slayer.storage.base import StorageBackend
from tests._engine_helpers import owned_query_engine
from tests._ossie_vocabulary_fixtures import fresh, shop_customers, shop_orders, shop_store



class ShapeCase(BaseModel):
    """A query (or refinement) in its 1.x spelling, if one exists, and each of its Ossie forms."""

    x1: Any = None
    ossie: list[Any]
    ordered: bool = False


#: Queries over ``tests._ossie_vocabulary_fixtures.vocab_engine``.
QUERIES: dict[str, ShapeCase] = {
    "named_and_unnamed_metrics": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"],
            "measures": ["sum(amount)", {"formula": "avg(amount)", "name": "avg_amount"}, "revenue", "count(*)"]},
        ossie=[{"population": "orders", "dimensions": ["orders.status"],
                "metrics": ["sum(orders.amount)", {"metric": "avg(orders.amount)", "alias": "avg_amount"}, {"metric": "orders.revenue"},
                            "count(orders.*)"]}],
    ),
    "reference_dimensions": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status", {"name": "name", "model": "customers"}],
            "measures": ["count(*)"]},
        ossie=[
            {"population": "orders", "dimensions": ["orders.status", "customers.name"], "metrics": ["count(orders.*)"]},
            {"population": "orders", "dimensions": [{"field": "orders.status"}, {"field": "customers.name"}],
             "metrics": ["count(orders.*)"]},
        ],
    ),
    "aliased_reference_dimension": ShapeCase(
        x1={"source_model": "orders", "dimensions": [{"expression": "customers.name", "name": "buyer_name"}],
            "measures": ["count(*)"]},
        ossie=[{"population": "orders", "dimensions": [{"field": "customers.name", "alias": "buyer_name"}],
                "metrics": ["count(orders.*)"]}],
    ),
    "alias_equal_to_default": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"]},
        ossie=[{"population": "orders", "dimensions": [{"field": "orders.status", "alias": "status"}],
                "metrics": ["count(orders.*)"]}],
    ),
    "expression_dimensions": ShapeCase(
        x1={"source_model": "orders",
            "dimensions": [{"expression": "upper(status)", "name": "status_uc"}, {"expression": "lower(status)"}],
            "measures": ["count(*)"]},
        ossie=[
            {"population": "orders", "dimensions": [{"field": "upper(orders.status)", "alias": "status_uc"}, "lower(orders.status)"],
             "metrics": ["count(orders.*)"]},
            {"population": "orders",
             "dimensions": [{"field": "upper(orders.status)", "alias": "status_uc"}, {"field": "lower(orders.status)"}],
             "metrics": ["count(orders.*)"]},
        ],
    ),
    "time_dimension": ShapeCase(
        x1={"source_model": "orders", "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
            "measures": ["count(*)"]},
        ossie=[{"population": "orders", "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
                "metrics": ["count(orders.*)"]}],
    ),
    "aliased_time_dimension": ShapeCase(
        ossie=[{"population": "orders",
                "time_dimensions": [{"field": "orders.created_at", "granularity": "month", "alias": "m"}],
                "metrics": ["count(orders.*)"]}],
    ),
    "order_by_reference_and_expression": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status", {"name": "name", "model": "customers"}],
            "measures": ["sum(amount)"],
            "order": [{"column": "customers.name", "direction": "desc"}, {"column": "sum(amount)"},
                      {"column": "status"}]},
        ossie=[{"population": "orders", "dimensions": ["orders.status", "customers.name"], "metrics": ["sum(orders.amount)"],
                "order": [{"field": "customers.name", "direction": "desc"}, {"field": "sum(orders.amount)"},
                          {"field": "orders.status"}]}],
        ordered=True,
    ),
    "multi_stage": ShapeCase(
        x1=[
            {"name": "per_customer", "source_model": "orders", "dimensions": ["customer_id"],
             "measures": [{"formula": "sum(amount)", "name": "revenue_total"}, "count(*)"],
             "filters": ["amount > 5"]},
            {"source_model": "per_customer",
             "measures": [{"formula": "avg(revenue_total)", "name": "avg_revenue"}, "max(revenue_total)"],
             "order": [{"column": "avg_revenue", "direction": "desc"}]},
        ],
        ossie=[[
            {"name": "per_customer", "population": "orders", "dimensions": ["orders.customer_id"],
             "metrics": [{"metric": "sum(orders.amount)", "alias": "revenue_total"}, "count(orders.*)"],
             "filter": "orders.amount > 5"},
            {"population": "per_customer",
             "metrics": [{"metric": "avg(per_customer.revenue_total)", "alias": "avg_revenue"}, "max(per_customer.revenue_total)"],
             "order": [{"field": "avg_revenue", "direction": "desc"}]},
        ]],
    ),
    "single_filter": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"], "filters": ["amount > 15"]},
        ossie=[
            {"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"], "filter": "orders.amount > 15"},
            {"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"], "filter": ["orders.amount > 15"]},
        ],
    ),
    "filter_list": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"],
            "filters": ["amount > 15", "status = 'ok'"]},
        ossie=[{"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"],
                "filter": ["orders.amount > 15", "orders.status = 'ok'"]}],
    ),
    "no_filter": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"], "filters": []},
        ossie=[
            {"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"]},
            {"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"], "filter": None},
            {"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"], "filter": []},
        ],
    ),
}

#: Refinements of ``tests._ossie_vocabulary_fixtures.SAVED_QUERY`` (filter ``status = 'paid'``).
REFINEMENTS: dict[str, ShapeCase] = {
    "aliased_metric": ShapeCase(
        x1={"measures": [{"formula": "count(*)", "name": "n"}]},
        ossie=[{"metrics": [{"metric": "count(orders.*)", "alias": "n"}]}],
    ),
    "added_filter": ShapeCase(
        x1={"filters": ["region = 'US'"]},
        ossie=[{"filter": "orders.region = 'US'"}, {"filter": ["orders.region = 'US'"]}],
    ),
    "repeated_filter": ShapeCase(
        x1={"filters": ["status = 'paid'"]},
        ossie=[{"filter": "orders.status = 'paid'"}, {"filter": ["orders.status = 'paid'"]}],
    ),
    "aliased_dimension": ShapeCase(
        x1={"dimensions": [{"expression": "region", "name": "r"}]},
        ossie=[{"dimensions": [{"field": "orders.region", "alias": "r"}]}],
    ),
}


async def shop_engine_store(tmp_path: Path) -> StorageBackend:
    """A YAML ``shop`` store holding ``customers`` and ``orders``."""
    storage = await shop_store(backend="yaml", base=str(tmp_path))
    await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
    await storage.save_dataset(SlayerDataset.model_validate(shop_orders()))
    return storage


async def execute(storage: StorageBackend, query: Any, **kwargs: Any) -> SlayerResponse:
    async with owned_query_engine(storage=storage) as engine:
        return await engine.execute(fresh(query), **kwargs)


def sorted_rows(response: SlayerResponse) -> list[tuple]:
    return sorted(tuple(str(r[k]) for k in response.columns) for r in response.data)


def shape_normal(response: SlayerResponse, *, ordered: bool) -> dict[str, Any]:
    """Result keys in order; rows in returned order when ``ordered``, else sorted."""
    keys = list(response.columns)
    rows = [json.dumps([r[k] for k in keys], default=str) for r in response.data]
    return {"keys": keys, "rows": [json.loads(r) for r in (rows if ordered else sorted(rows))]}
