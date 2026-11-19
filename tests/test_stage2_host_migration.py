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

import re

import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery
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


def _cte_body(sql: str, name_pattern: str) -> str:
    """Extract one CTE body by ``<name> AS (`` + balanced-paren walk (mirror
    of tests/test_sql_generator.py::_extract_cte_body)."""
    m = re.search(rf"({name_pattern})\s+AS\s*\(", sql)
    assert m, f"No CTE matching {name_pattern!r} in:\n{sql}"
    start = sql.index("(", m.start()) + 1
    depth, i = 1, start
    while i < len(sql):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[start:i]
        i += 1
    raise AssertionError(f"Unbalanced parens in:\n{sql}")


def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions", sql_table="regions", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="population", sql="population", datatype=DataType.FLOAT),
        ],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", sql="region_id", datatype=DataType.FLOAT),
            DatasetField(name="balance", sql="balance", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def _warehouses() -> SlayerDataset:
    return SlayerDataset(
        name="warehouses", sql_table="warehouses", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", sql="region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


class TestJoinOrderParity:
    async def test_diamond_mixed_carriers_emit_deterministic_join_order(self) -> None:
        # A DIAMOND (orders → customers → regions AND orders → warehouses →
        # regions) reaches ``regions`` via two distinct path aliases
        # (customers__regions vs warehouses__regions). DEV-1709: the derived
        # agg SOURCE crossing the warehouses branch now ISOLATES host-rooted;
        # the row-level carriers (joined dim + multi-hop WHERE filter) stay in
        # the host ``_base``. Deterministic first-seen join order applies PER
        # SCOPE: the base emits customers, customers__regions; the isolation
        # CTE (which inherits the host dims + ROW filter and adds the agg
        # source) emits customers, customers__regions, warehouses,
        # warehouses__regions.
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="warehouse_id", sql="warehouse_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="wh_pop", sql="warehouses.regions.population",
                       datatype=DataType.FLOAT),
            ],
            relationships=[
                DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
                DatasetRelationship(to="warehouses", from_columns=["warehouse_id"], to_columns=["id"]),
            ],
        )
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.region_id")],    # joined dim
            metrics=[QueryMetric(metric="sum(orders.wh_pop)")],         # derived source (warehouses)
            filter=["customers.regions.population > 100"],        # WHERE (customers→regions)
        )
        sql = await _engine_generate(
            query=query, dataset=orders,
            extra_datasets=[_customers(), _warehouses(), _regions()],
        )
        assert "_cm_" in sql, f"crossing agg source must isolate:\n{sql}"
        # Each alias appears exactly once per scope; every prefix precedes its
        # extension; the diamond's two paths to ``regions`` keep distinct
        # aliases.
        cm_order = _ordered_join_aliases(_cte_body(sql, r"_cm_\w+"))
        assert cm_order == [
            "customers", "customers__regions",
            "warehouses", "warehouses__regions",
        ]
        base_order = _ordered_join_aliases(_cte_body(sql, r"_base"))
        assert base_order == ["customers", "customers__regions"]
        assert_scope_closed(sql)


class TestPlaceholderLimitOne:
    async def test_host_local_filter_keeps_limit_1(self) -> None:
        # A cross-model measure (customers.balance:sum) drives the _cm_ path; a
        # host-local ROW filter (amount > 100, no join, not routed to the CTE)
        # forces the host-rooted placeholder (gen:5123) to fire with LIMIT 1 so
        # the CROSS JOIN to the scalar _cm_ does not duplicate the aggregate,
        # while WHERE still gates the whole result (D-J LIMIT-1 semantics).
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
            relationships=[
                DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            ],
        )
        query = SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric="sum(customers.balance)")],
            filter=["orders.amount > 100"],
        )
        sql = await _engine_generate(
            query=query, dataset=orders, extra_datasets=[_customers(), _regions()],
        )
        assert "LIMIT 1" in sql                       # placeholder collapses host to one row
        assert "amount > 100" in sql or "amount > 100" in sql.replace('"', "")
        assert_scope_closed(sql)

    async def test_host_local_crossing_filter_pulls_join_into_placeholder(self) -> None:
        # D-J placeholder join registration (Round 6 / Codex): the cross-model
        # measure (customers.balance:sum) empties the host base, and a host-local
        # ROW filter that CROSSES a *different* join (shippers.code = 'X' — not
        # reachable from the customers CTE root, so not routed there) must pull
        # its LEFT JOIN into the placeholder FROM through the SAME single resolver
        # the main host base uses (_resolve_where_filter_joins_via_scope). Without
        # the join, the placeholder WHERE references an unbound alias. LIMIT 1
        # still collapses the host to a single row.
        shippers = SlayerDataset(
            name="shippers", sql_table="shippers", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="code", sql="code", datatype=DataType.STRING),
            ],
        )
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="shipper_id", sql="shipper_id", datatype=DataType.FLOAT),
            ],
            relationships=[
                DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
                DatasetRelationship(to="shippers", from_columns=["shipper_id"], to_columns=["id"]),
            ],
        )
        query = SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric="sum(customers.balance)")],
            filter=["shippers.code = 'X'"],
        )
        sql = await _engine_generate(
            query=query, dataset=orders,
            extra_datasets=[_customers(), _regions(), shippers],
        )
        # Scope the assertions to the placeholder SELECT itself (the one-row
        # ``_placeholder`` scan with LIMIT 1) so a regression that moves the join
        # or predicate to another query block can't pass (CodeRabbit).
        tree = sqlglot.parse_one(sql, dialect="postgres")
        placeholder = next(
            (
                s for s in tree.find_all(exp.Select)
                if s.args.get("limit") is not None
                and any(p.alias_or_name == "_placeholder" for p in s.expressions)
            ),
            None,
        )
        assert placeholder is not None, f"no placeholder SELECT found in:\n{sql}"
        ph = " ".join(placeholder.sql(dialect="postgres").split()).replace('"', "")
        assert "LIMIT 1" in ph                          # placeholder collapses host
        # the crossing filter's LEFT JOIN + predicate live in the SAME placeholder.
        assert "LEFT JOIN shippers AS shippers" in ph
        assert "shippers.code = 'X'" in ph
        assert_scope_closed(sql)


class TestTypedJoinKeyMixedCaseQuoted:
    async def test_mixed_case_join_key_quoted_in_typed_on_clause(self) -> None:
        # DEV-1645 regression guard on the TYPED pipeline: ``_build_from_and_joins``
        # builds the LEFT JOIN ON keys as AST (not via the legacy string-parse
        # path), so a mixed-case physical join key (``CustId``) must be quoted
        # there too — otherwise a case-folding backend resolves ``custid`` and
        # the query fails at execution (only the Postgres integration suite,
        # which runs the typed engine, caught this; the DEV-1645 unit test uses
        # the legacy ``generate(enriched=)`` path and did not).
        customers = SlayerDataset(
            name="customers", sql_table="customers", data_source="test",
            fields=[
                DatasetField(name="CustId", sql="CustId", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="balance", sql="balance", datatype=DataType.FLOAT),
            ],
        )
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="cust_ref", sql="cust_ref", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["cust_ref"], to_columns=["CustId"])],
        )
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.balance")],
            distinct_dimension_values=False,
        )
        sql = await _engine_generate(
            query=query, dataset=orders, extra_datasets=[customers],
        )
        norm = " ".join(sql.split())
        assert "LEFT JOIN" in norm
        assert " ON " in norm
        # The mixed-case key is quoted on the target side of the ON; the lowercase
        # host key stays bare.
        assert 'customers."CustId"' in norm
        assert "orders.cust_ref" in norm
        assert_scope_closed(sql)
