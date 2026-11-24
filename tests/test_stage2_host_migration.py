"""Host-base migration characterization guards.

These pin behavior that the ScopeFrame rewire MUST preserve byte-for-byte. They
pass BEFORE the migration (they describe current behavior) and must keep passing
AFTER it — they are the behavior-preservation gate, not feature tests.

1. Join-order byte parity (Codex H3): a query mixing several join-registration
   carriers (joined dim, WHERE filter, DatasetField.filter, derived agg source) plus a
   shared prefix must emit its LEFT JOINs in the same first-seen order after the
   collectors are replaced by the single resolver. The resolver's ``join_paths``
   OrderedSet must reproduce the collectors' insertion order, not just membership.

2. The cross-dataset host-rooted placeholder (generator.py:5123) LIMIT-1 semantics
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
from tests._engine_helpers import _engine_generate as _engine_generate_untrusted
from tests._trusted import trusting, trusting_all


async def _engine_generate(*, query: SlayerQuery, dataset: SlayerDataset, extra_datasets: list[SlayerDataset]) -> str:
    # These pins describe the inline-join lowering of trusted hops.
    return await _engine_generate_untrusted(
        query=query, dataset=trusting(dataset), extra_datasets=trusting_all(extra_datasets),
    )


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
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="population", expression="population", datatype=DataType.FLOAT),
        ],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
            DatasetField(name="balance", expression="balance", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def _warehouses() -> SlayerDataset:
    return SlayerDataset(
        name="warehouses", sql_table="warehouses", data_source="test",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


class TestJoinOrderParity:
    async def test_diamond_mixed_carriers_emit_deterministic_join_order(self) -> None:
        # A DIAMOND (orders → customers → regions AND orders → warehouses →
        # regions) reaches ``regions`` via two distinct path aliases
        # (customers__regions vs warehouses__regions). The derived
        # agg SOURCE crossing the warehouses branch ISOLATES host-rooted;
        # the row-level carriers (joined dim + multi-hop WHERE filter) stay in
        # the host ``_base``. Deterministic first-seen join order applies PER
        # SCOPE: the base emits customers, customers__regions; the isolation
        # CTE (which inherits the host dims + ROW filter and adds the agg
        # source) emits customers, customers__regions, warehouses,
        # warehouses__regions.
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="warehouse_id", expression="warehouse_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="wh_pop", expression="warehouses.regions.population",
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


def _exists_text(sql: str) -> str:
    """Every EXISTS restriction of ``sql``, normalized to one quote-free line."""
    tree = sqlglot.parse_one(sql, dialect="postgres")
    return " ".join(" ".join(e.sql(dialect="postgres").split()).replace('"', "") for e in tree.find_all(exp.Exists))


class TestUnitHostFilters:
    async def test_host_local_filter_restricts_by_association(self) -> None:
        # The unit row always exists (Law 10); the host filter restricts the cross-dataset aggregate by association.
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
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
        assert "LIMIT 1" not in sql
        assert "amount > 100" in _exists_text(sql)
        assert_scope_closed(sql)

    async def test_host_local_crossing_filter_joins_inside_the_restriction(self) -> None:
        # The unit row always exists (Law 10); a host filter crossing another join restricts the cross-dataset
        # aggregate by association, its join inside that restriction.
        shippers = SlayerDataset(
            name="shippers", sql_table="shippers", data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="code", expression="code", datatype=DataType.STRING),
            ],
        )
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="shipper_id", expression="shipper_id", datatype=DataType.FLOAT),
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
        restriction = _exists_text(sql)
        assert "LIMIT 1" not in sql
        assert "JOIN shippers AS orders__shippers" in restriction
        assert "code = 'X'" in restriction
        assert_scope_closed(sql)


class TestTypedJoinKeyMixedCaseQuoted:
    async def test_mixed_case_join_key_quoted_in_typed_on_clause(self) -> None:
        # Mixed-case quoting regression guard on the TYPED pipeline: ``_build_from_and_joins``
        # builds the LEFT JOIN ON keys as AST (not via the legacy string-parse
        # path), so a mixed-case physical join key (``CustId``) must be quoted
        # there too — otherwise a case-folding backend resolves ``custid`` and
        # the query fails at execution (only the Postgres integration suite,
        # which runs the typed engine, caught this; the original unit test uses
        # the legacy ``generate(enriched=)`` path and did not).
        customers = SlayerDataset(
            name="customers", sql_table="customers", data_source="test",
            fields=[
                DatasetField(name="CustId", expression="CustId", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="balance", expression="balance", datatype=DataType.FLOAT),
            ],
        )
        orders = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="cust_ref", expression="cust_ref", datatype=DataType.FLOAT),
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
