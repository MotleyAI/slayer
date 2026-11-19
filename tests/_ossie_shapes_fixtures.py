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

GOLDEN_PATH = Path(__file__).parent / "fixtures" / "ossie_shapes" / "golden.json"


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
        ossie=[{"population": "orders", "dimensions": ["status"],
                "metrics": ["sum(amount)", {"metric": "avg(amount)", "alias": "avg_amount"}, {"metric": "revenue"},
                            "count(*)"]}],
    ),
    "reference_dimensions": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status", {"name": "name", "model": "customers"}],
            "measures": ["count(*)"]},
        ossie=[
            {"population": "orders", "dimensions": ["status", "customers.name"], "metrics": ["count(*)"]},
            {"population": "orders", "dimensions": [{"field": "status"}, {"field": "customers.name"}],
             "metrics": ["count(*)"]},
        ],
    ),
    "aliased_reference_dimension": ShapeCase(
        x1={"source_model": "orders", "dimensions": [{"expression": "customers.name", "name": "buyer_name"}],
            "measures": ["count(*)"]},
        ossie=[{"population": "orders", "dimensions": [{"field": "customers.name", "alias": "buyer_name"}],
                "metrics": ["count(*)"]}],
    ),
    "alias_equal_to_default": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"]},
        ossie=[{"population": "orders", "dimensions": [{"field": "status", "alias": "status"}],
                "metrics": ["count(*)"]}],
    ),
    "expression_dimensions": ShapeCase(
        x1={"source_model": "orders",
            "dimensions": [{"expression": "upper(status)", "name": "status_uc"}, {"expression": "lower(status)"}],
            "measures": ["count(*)"]},
        ossie=[
            {"population": "orders", "dimensions": [{"field": "upper(status)", "alias": "status_uc"}, "lower(status)"],
             "metrics": ["count(*)"]},
            {"population": "orders",
             "dimensions": [{"field": "upper(status)", "alias": "status_uc"}, {"field": "lower(status)"}],
             "metrics": ["count(*)"]},
        ],
    ),
    "time_dimension": ShapeCase(
        x1={"source_model": "orders", "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
            "measures": ["count(*)"]},
        ossie=[{"population": "orders", "time_dimensions": [{"field": "created_at", "granularity": "month"}],
                "metrics": ["count(*)"]}],
    ),
    "aliased_time_dimension": ShapeCase(
        ossie=[{"population": "orders",
                "time_dimensions": [{"field": "created_at", "granularity": "month", "alias": "m"}],
                "metrics": ["count(*)"]}],
    ),
    "order_by_reference_and_expression": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status", {"name": "name", "model": "customers"}],
            "measures": ["sum(amount)"],
            "order": [{"column": "customers.name", "direction": "desc"}, {"column": "sum(amount)"},
                      {"column": "status"}]},
        ossie=[{"population": "orders", "dimensions": ["status", "customers.name"], "metrics": ["sum(amount)"],
                "order": [{"field": "customers.name", "direction": "desc"}, {"field": "sum(amount)"},
                          {"field": "status"}]}],
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
            {"name": "per_customer", "population": "orders", "dimensions": ["customer_id"],
             "metrics": [{"metric": "sum(amount)", "alias": "revenue_total"}, "count(*)"],
             "filter": "amount > 5"},
            {"population": "per_customer",
             "metrics": [{"metric": "avg(revenue_total)", "alias": "avg_revenue"}, "max(revenue_total)"],
             "order": [{"field": "avg_revenue", "direction": "desc"}]},
        ]],
    ),
    "single_filter": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"], "filters": ["amount > 15"]},
        ossie=[
            {"population": "orders", "dimensions": ["status"], "metrics": ["count(*)"], "filter": "amount > 15"},
            {"population": "orders", "dimensions": ["status"], "metrics": ["count(*)"], "filter": ["amount > 15"]},
        ],
    ),
    "filter_list": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"],
            "filters": ["amount > 15", "status = 'ok'"]},
        ossie=[{"population": "orders", "dimensions": ["status"], "metrics": ["count(*)"],
                "filter": ["amount > 15", "status = 'ok'"]}],
    ),
    "no_filter": ShapeCase(
        x1={"source_model": "orders", "dimensions": ["status"], "measures": ["count(*)"], "filters": []},
        ossie=[
            {"population": "orders", "dimensions": ["status"], "metrics": ["count(*)"]},
            {"population": "orders", "dimensions": ["status"], "metrics": ["count(*)"], "filter": None},
            {"population": "orders", "dimensions": ["status"], "metrics": ["count(*)"], "filter": []},
        ],
    ),
}

#: Refinements of ``tests._ossie_vocabulary_fixtures.SAVED_QUERY`` (filter ``status = 'paid'``).
REFINEMENTS: dict[str, ShapeCase] = {
    "aliased_metric": ShapeCase(
        x1={"measures": [{"formula": "count(*)", "name": "n"}]},
        ossie=[{"metrics": [{"metric": "count(*)", "alias": "n"}]}],
    ),
    "added_filter": ShapeCase(
        x1={"filters": ["region = 'US'"]},
        ossie=[{"filter": "region = 'US'"}, {"filter": ["region = 'US'"]}],
    ),
    "repeated_filter": ShapeCase(
        x1={"filters": ["status = 'paid'"]},
        ossie=[{"filter": "status = 'paid'"}, {"filter": ["status = 'paid'"]}],
    ),
    "aliased_dimension": ShapeCase(
        x1={"dimensions": [{"expression": "region", "name": "r"}]},
        ossie=[{"dimensions": [{"field": "region", "alias": "r"}]}],
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


def shape_golden(name: str, backend: str) -> dict[str, Any]:
    return json.loads(GOLDEN_PATH.read_text())[name][backend]
