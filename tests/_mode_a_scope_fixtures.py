"""Shared ScopeFrame / Mode-A model fixtures for the DEV-1745 and DEV-1752
packs.

Underscore-prefixed (like ``tests/_engine_helpers.py``) so pytest skips it during
collection while ``from tests._mode_a_scope_fixtures import ...`` still works.
The ``orders → customers → regions`` models and the ``_scope`` / ``_sql_of``
helpers are the common substrate both modules build their ScopeFrame assertions
on; keeping one copy here keeps the two files from duplicating them.
"""
from __future__ import annotations

from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import ScopeFrame


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="population", type=DataType.DOUBLE),
            Column(name="pop_x2", sql="population * 2", type=DataType.DOUBLE),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="balance", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="doubled", sql="amount * 2", type=DataType.DOUBLE),
            # a reserved-ish name that would shadow a statement keyword
            Column(name="select", type=DataType.TEXT),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
    )


def _scope(dialect: str = "postgres") -> ScopeFrame:
    host = _orders()
    alloc = AliasAllocator()
    bundle = ResolvedSourceBundle(
        source_model=host, referenced_models=[host, _customers(), _regions()],
    )
    return ScopeFrame(
        scope_id=alloc.next_scope_id(host.name),
        root_model=host, root_relation=host.name,
        bundle=bundle, dialect=get_dialect(dialect), allocator=alloc,
    )


def _sql_of(node: exp.Expression, dialect: str = "postgres") -> str:
    return node.sql(dialect=dialect)
