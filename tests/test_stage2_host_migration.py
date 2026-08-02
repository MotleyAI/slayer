"""DEV-1706 Stage 2 — host-base migration characterization guards.

These pin behavior that the ScopeFrame rewire MUST preserve byte-for-byte. They
pass BEFORE the migration (they describe current behavior) and must keep passing
AFTER it — they are the behavior-preservation gate, not feature tests.

1. Join-order byte parity (Codex H3): a query mixing several join-registration
   carriers (joined dim, WHERE filter, Column.filter, derived agg source) plus a
   shared prefix must emit its LEFT JOINs in the same first-seen order after the
   collectors are replaced by the single resolver. The resolver's ``join_paths``
   OrderedSet must reproduce the collectors' insertion order, not just membership.

2. The cross-model host-rooted placeholder (generator.py:5123) LIMIT-1 semantics
   (Codex H4 / D-J): a host-local filter crossing a join must still gate the whole
   result and collapse the host to a single row, with its join in scope.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.sql.scope_check import assert_scope_closed
from tests._engine_helpers import _engine_generate


def _ordered_join_aliases(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Joined-table aliases in emission (document) order — the observable join
    order ``_build_from_and_joins`` produces from the ordered join-path set."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    out: list[str] = []
    for join in tree.find_all(exp.Join):
        target = join.this
        if isinstance(target, exp.Table):
            out.append(target.alias_or_name)
    return out


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="population", sql="population", type=DataType.DOUBLE),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            Column(name="balance", sql="balance", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _warehouses() -> SlayerModel:
    return SlayerModel(
        name="warehouses", sql_table="warehouses", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


class TestJoinOrderParity:
    async def test_diamond_mixed_carriers_emit_deterministic_join_order(self) -> None:
        # A DIAMOND (orders → customers → regions AND orders → warehouses →
        # regions) reaches ``regions`` via two distinct path aliases
        # (customers__regions vs warehouses__regions). Carriers stay in the HOST
        # BASE (no DEV-1503 isolation): a joined dim (customers), a multi-hop WHERE
        # filter (customers→regions), and a derived agg SOURCE crossing the
        # warehouses branch. First-seen registration order across categories —
        # dims → WHERE filters → agg sources — must be reproduced byte-for-byte:
        # customers, customers__regions, warehouses, warehouses__regions.
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="warehouse_id", sql="warehouse_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="wh_pop", sql="warehouses__regions.population",
                       type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
                ModelJoin(target_model="warehouses", join_pairs=[["warehouse_id", "id"]]),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.region_id")],    # joined dim
            measures=[ModelMeasure(formula="wh_pop:sum")],         # derived source (warehouses)
            filters=["customers.regions.population > 100"],        # WHERE (customers→regions)
        )
        sql = await _engine_generate(
            query=query, model=orders,
            extra_models=[_customers(), _warehouses(), _regions()],
        )
        assert "_cm_" not in sql  # stayed in the host base
        order = _ordered_join_aliases(sql)
        # Each alias appears exactly once; every prefix precedes its extension;
        # the diamond's two paths to ``regions`` keep distinct aliases.
        assert order == [
            "customers", "customers__regions",
            "warehouses", "warehouses__regions",
        ]
        assert_scope_closed(sql)


class TestPlaceholderLimitOne:
    async def test_host_local_filter_keeps_limit_1(self) -> None:
        # A cross-model measure (customers.balance:sum) drives the _cm_ path; a
        # host-local ROW filter (amount > 100, no join, not routed to the CTE)
        # forces the host-rooted placeholder (gen:5123) to fire with LIMIT 1 so
        # the CROSS JOIN to the scalar _cm_ does not duplicate the aggregate,
        # while WHERE still gates the whole result (D-J LIMIT-1 semantics).
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.balance:sum")],
            filters=["amount > 100"],
        )
        sql = await _engine_generate(
            query=query, model=orders, extra_models=[_customers(), _regions()],
        )
        assert "LIMIT 1" in sql                       # placeholder collapses host to one row
        assert "amount > 100" in sql or "amount > 100" in sql.replace('"', "")
        assert_scope_closed(sql)
