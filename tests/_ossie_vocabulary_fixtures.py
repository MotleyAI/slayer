"""Datasets, queries and engines for the Ossie-vocabulary tests, each in the pre-2.0 and the Ossie spelling.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import copy
import json
import os
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any, Literal

import pytest
import yaml

from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.base import StorageBackend
from tests._dev1737_fixtures import CUSTOMERS, ORDERS, seed_backend
from tests._engine_helpers import seeded_exec_engine
from tests._saved_query_refinement_fixtures import seed_duckdb, seed_sqlite
from tests._stored_upgrade_fixtures import DS, raw_store, seed_shop, stored_bytes

Spelling = Literal["old", "new"]
SPELLINGS: tuple[Spelling, ...] = ("old", "new")
BACKENDS = ("sqlite", "duckdb")

_ORDER_FIELDS: list[tuple[str, str]] = [
    ("id", "INT"), ("customer_id", "INT"), ("order_date", "DATE"), ("created_at", "TIMESTAMP"),
    ("shipped_at", "TIMESTAMP"), ("amount", "DOUBLE"), ("status", "TEXT"), ("sla_days", "DOUBLE"),
    ("is_paid", "BOOLEAN"), ("raw_dt", "TEXT"),
]
_CUSTOMER_FIELDS: list[tuple[str, str]] = [("id", "INT"), ("name", "TEXT"), ("signup_date", "DATE")]


#: Pre-2.0 datatype -> Ossie datatype, for fields without a ``db_type``.
_OSSIE_TYPES = {"INT": "Integer", "TEXT": "String", "DATE": "Date", "TIMESTAMP": "DateTime", "DOUBLE": "Float",
                "BOOLEAN": "Boolean"}


def _type_key(spelling: Spelling) -> str:
    return "type" if spelling == "old" else "datatype"


def _type_value(value: str, spelling: Spelling) -> str:
    return value if spelling == "old" else _OSSIE_TYPES[value]


def _fields(spec: list[tuple[str, str]], *, spelling: Spelling, pk: str = "id") -> list[dict[str, Any]]:
    key = _type_key(spelling)
    return [{"name": n, key: _type_value(t, spelling), **({"primary_key": True} if n == pk else {})} for n, t in spec]


def orders_doc(spelling: Spelling) -> dict[str, Any]:
    """``orders`` with a metric, a custom aggregation and a relationship to ``customers``."""
    if spelling == "old":
        return {
            "name": "orders", "sql_table": "orders", "data_source": "test", "default_time_dimension": "created_at",
            "columns": _fields(_ORDER_FIELDS, spelling="old"),
            "measures": [{"name": "revenue", "formula": "sum(amount)", "type": "DOUBLE"}],
            "aggregations": [{"name": "doubled", "formula": "SUM({value}) * 2"}],
            "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
        }
    return {
        "name": "orders", "sql_table": "orders", "data_source": "test", "default_time_dimension": "created_at",
        "fields": _fields(_ORDER_FIELDS, spelling="new"),
        "metrics": [{"name": "revenue", "expression": "sum(amount)", "datatype": "Float"}],
        "aggregations": [{"name": "doubled", "expression": "SUM({value}) * 2"}],
        "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}],
    }


def customers_doc(spelling: Spelling) -> dict[str, Any]:
    key = "columns" if spelling == "old" else "fields"
    return {"name": "customers", "sql_table": "customers", "data_source": "test",
            key: _fields(_CUSTOMER_FIELDS, spelling=spelling)}


#: Golden queries: name -> (pre-2.0 spelling, Ossie spelling).
GOLDEN_QUERIES: dict[str, tuple[Any, Any]] = {
    "single_stage": (
        {
            "source_model": "orders",
            "dimensions": ["customers.name", {"expression": "upper(status)", "name": "status_uc"}],
            "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
            "measures": ["sum(amount)", {"formula": "avg(amount)", "name": "avg_amount"}, "count(*)"],
            "filters": ["amount > 5"],
            "order": [{"column": "customers.name"}],
        },
        {
            "population": "orders",
            "dimensions": ["customers.name", {"field": "upper(orders.status)", "alias": "status_uc"}],
            "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
            "metrics": ["sum(orders.amount)", {"metric": "avg(orders.amount)", "alias": "avg_amount"}, "count(orders.*)"],
            "filter": ["orders.amount > 5"],
            "order": [{"field": "customers.name"}],
        },
    ),
    "dataset_qualified_dimension": (
        {"source_model": "orders", "dimensions": [{"name": "name", "model": "customers"}],
         "measures": ["sum(amount)"]},
        {"population": "orders", "dimensions": ["customers.name"], "metrics": ["sum(orders.amount)"]},
    ),
    "saved_metric_and_aggregation": (
        {"source_model": "orders", "dimensions": ["status"], "measures": ["revenue", "doubled(amount)"]},
        {"population": "orders", "dimensions": ["orders.status"], "metrics": ["orders.revenue", "doubled(orders.amount)"]},
    ),
    "root_less": (
        {"dimensions": ["customers.name"], "measures": ["sum(orders.amount)"], "filters": ["orders.status = 'ok'"]},
        {"dimensions": ["customers.name"], "metrics": ["sum(orders.amount)"], "filter": ["orders.status = 'ok'"]},
    ),
    "inverse": (
        {"source_model": "customers", "dimensions": ["name"], "measures": ["sum(orders.amount)", "count(*)"]},
        {"population": "customers", "dimensions": ["customers.name"], "metrics": ["sum(orders.amount)", "count(customers.*)"]},
    ),
    "extension": (
        {"source_model": "orders", "dimensions": ["buyer.name"], "measures": ["aov"],
         "extensions": [{"source_name": "orders",
                         "measures": [{"name": "aov", "formula": "sum(amount) / count(*)"}],
                         "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]],
                                    "name": "buyer"}]}]},
        {"population": "orders", "dimensions": ["orders.buyer.name"], "metrics": ["orders.aov"],
         "extensions": [{"dataset": "orders",
                         "metrics": [{"name": "aov", "expression": "sum(amount) / count(*)"}],
                         "relationships": [{"to": "customers", "from_columns": ["customer_id"],
                                            "to_columns": ["id"], "name": "buyer"}]}]},
    ),
    "multi_stage": (
        [
            {"name": "per_customer", "source_model": "orders", "dimensions": ["customer_id"],
             "measures": [{"formula": "sum(amount)", "name": "revenue_total"}], "filters": ["amount > 5"]},
            {"source_model": "per_customer",
             "measures": [{"formula": "avg(revenue_total)", "name": "avg_revenue"}, "count(*)"],
             "order": [{"column": "avg_revenue", "direction": "desc"}]},
        ],
        [
            {"name": "per_customer", "population": "orders", "dimensions": ["orders.customer_id"],
             "metrics": [{"metric": "sum(orders.amount)", "alias": "revenue_total"}], "filter": ["orders.amount > 5"]},
            {"population": "per_customer",
             "metrics": [{"metric": "avg(per_customer.revenue_total)", "alias": "avg_revenue"}, "count(per_customer.*)"],
             "order": [{"field": "avg_revenue", "direction": "desc"}]},
        ],
    ),
}

SAVED_QUERY = "monthly_revenue"
#: The saved query behind the refinements, per spelling.
MONTHLY_REVENUE: dict[Spelling, dict[str, Any]] = {
    "old": {
        "source_model": "orders",
        "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}],
        "measures": [{"formula": "sum(amount)", "name": "revenue"}],
        "filters": ["status = 'paid'"],
    },
    "new": {
        "population": "orders",
        "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}],
        "metrics": [{"metric": "sum(orders.amount)", "alias": "revenue"}],
        "filter": ["orders.status = 'paid'"],
    },
}
#: Refinements of ``monthly_revenue``: name -> (pre-2.0 spelling, Ossie spelling).
REFINEMENTS: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
    "repeated_metric": (
        {"measures": [{"formula": "sum(amount)", "name": "revenue"}]},
        {"metrics": [{"metric": "sum(orders.amount)", "alias": "revenue"}]},
    ),
    "added_filter": ({"filters": ["region = 'US'"]}, {"filter": ["orders.region = 'US'"]}),
}


def refine_orders_doc(spelling: Spelling) -> dict[str, Any]:
    key = "columns" if spelling == "old" else "fields"
    tkey = _type_key(spelling)
    spec = [("id", "INT"), ("customer_id", "TEXT"), ("region", "TEXT"), ("status", "TEXT"),
            ("amount", "DOUBLE"), ("ordered_at", "DATE")]
    return {"name": "orders", "data_source": "test", "sql_table": "orders", "default_time_dimension": "ordered_at",
            key: [{"name": n, tkey: _type_value(t, spelling), **({"primary_key": True} if n == "id" else {})}
                  for n, t in spec]}


def fresh(value: Any) -> Any:
    """A deep copy, so a converting validator never sees a shared literal twice."""
    return copy.deepcopy(value)


def normal(response: SlayerResponse) -> dict[str, Any]:
    """Result keys in order and rows order-independent."""
    keys = list(response.columns)
    rows = sorted(json.dumps([r[k] for k in keys], default=str) for r in response.data)
    return {"keys": keys, "rows": [json.loads(r) for r in rows]}


def dataset_of(doc: dict[str, Any]) -> SlayerDataset:
    return SlayerDataset.model_validate(fresh(doc))


@asynccontextmanager
async def vocab_engine(backend: str, *, spelling: Spelling = "new") -> AsyncGenerator[SlayerQueryEngine]:
    """The ``orders`` → ``customers`` engine, its datasets declared in ``spelling``."""
    if backend == "duckdb":
        pytest.importorskip("duckdb")
    datasets = [dataset_of(orders_doc(spelling)), dataset_of(customers_doc(spelling))]
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [ORDERS, CUSTOMERS]), datasets=datasets,
    ) as (engine, _db):
        yield engine


@asynccontextmanager
async def refine_vocab_engine(backend: str, *, spelling: Spelling = "new") -> AsyncGenerator[SlayerQueryEngine]:
    """The refinement ``orders`` engine plus the saved query ``monthly_revenue``, both in ``spelling``."""
    if backend == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if backend == "duckdb" else seed_sqlite
    async with seeded_exec_engine(
        dialect=backend, seed=seed, datasets=[dataset_of(refine_orders_doc(spelling))],
    ) as (engine, _db):
        await engine.create_dataset_from_query(query=fresh(MONTHLY_REVENUE[spelling]), name=SAVED_QUERY)
        yield engine


# --- The ``shop`` store (``tests._stored_upgrade_fixtures`` tables) -------------------------------------------

_SHOP_ORDER_FIELDS = [("order_id", "INT"), ("customer_id", "INT"), ("status", "TEXT"),
                      ("ordered_at", "TIMESTAMP"), ("amount", "DOUBLE")]
_SHOP_CUSTOMER_FIELDS = [("id", "INT"), ("name", "TEXT"), ("region", "TEXT"), ("signed_up_at", "TIMESTAMP")]


def shop_orders(spelling: Spelling = "new", **extra: Any) -> dict[str, Any]:
    """The ``shop`` orders dataset with a metric and a relationship to ``customers``."""
    fields = _fields(_SHOP_ORDER_FIELDS, spelling=spelling, pk="order_id")
    if spelling == "old":
        return {"name": "orders", "sql_table": "orders", "data_source": DS, "columns": fields,
                "measures": [{"name": "revenue", "formula": "sum(amount)", "type": "DOUBLE"}],
                "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]],
                           "cardinality": "many_to_one"}],
                **extra}
    return {"name": "orders", "sql_table": "orders", "data_source": DS, "fields": fields,
            "metrics": [{"name": "revenue", "expression": "sum(amount)", "datatype": "Float"}],
            "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"],
                               "cardinality": "many_to_one"}],
            **extra}


def shop_customers(spelling: Spelling = "new", **extra: Any) -> dict[str, Any]:
    key = "columns" if spelling == "old" else "fields"
    return {"name": "customers", "sql_table": "customers", "data_source": DS,
            key: _fields(_SHOP_CUSTOMER_FIELDS, spelling=spelling), **extra}


#: ``sum(amount)`` by ``customers.region`` over the ``shop`` tables, per spelling.
SHOP_REVENUE_BY_REGION: dict[Spelling, dict[str, Any]] = {
    "old": {"source_model": "orders", "dimensions": ["customers.region"], "measures": ["revenue"]},
    "new": {"population": "orders", "dimensions": ["customers.region"], "metrics": ["orders.revenue"]},
}


async def shop_store(
    *, backend: str, base: str, datasets: Sequence[dict[str, Any]] = (), memories: Sequence[dict[str, Any]] = (),
) -> StorageBackend:
    """A YAML or SQLite store with datasource ``shop`` over a seeded SQLite file, ``datasets`` and ``memories``
    written verbatim."""
    os.makedirs(base, exist_ok=True)
    db = os.path.join(base, "shop.db")
    seed_shop(db, dialect="sqlite")
    return await raw_store(backend=backend, base=base, models=datasets, memories=memories,
                           datasource=DatasourceConfig(name=DS, type="sqlite", database=db))


def stored_document(*, backend: str, base: str, name: str) -> dict[str, Any]:
    """The persisted document of dataset ``name`` in datasource ``shop``, parsed."""
    text = stored_bytes(backend=backend, base=base, name=name)
    return yaml.safe_load(text) if backend == "yaml" else json.loads(text)
