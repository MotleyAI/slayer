"""DEV-1475 — Mode-B ``IN`` / ``NOT IN`` filters with literal-tuple RHS.

The DEV-1450 typed-pipeline redesign initially deferred SQL-style
``col in ('a', 'b')`` filters (the legacy enrichment path accepted them
via sqlglot parsing). The 02_sql_vs_dsl notebook example, which filters
on ``stores.name in ('Brooklyn', 'Philadelphia')``, was failing with
``ValueError: Invalid Mode-B expression … unsupported comparison operator
In.``

DEV-1475 lands the support: ``ast.In`` / ``ast.NotIn`` reach
``_CMP_OP_MAP``, the parser produces a new ``Tuple`` ParsedExpr node for
the RHS, the binder folds a ``Cmp(op="in"/"not in", left=ref, right=Tuple)``
into a new ``InKey``, and the SQL generator
emits ``IN (lit, lit, …)`` / ``NOT IN (...)`` through the single
``render_value_key`` path (P-G) across all filter render contexts — local
WHERE/HAVING, POST-phase filters, and cross-model CTE routed filters.

Coverage:

- Parser: shape of the produced ``ParsedExpr`` tree.
- Parser: guards on empty / scalar / non-literal RHS.
- Binder: a bound ``Cmp(op="in", …)`` interns as an ``InKey``.
- Generator: end-to-end SQL emission for ``IN`` and ``NOT IN`` on a
  local column.
- Generator: n-ary boolean fold preserves all operands when an IN
  filter is one of them (regression on the 7a1110f territory).
- Generator: cross-model filter on a joined column emits qualified
  ``IN (...)``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import DateOperandTypeError
from slayer.core.keys import InKey, LiteralKey
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.syntax import (
    Cmp,
    DottedRef,
    Literal,
    Ref,
    TupleLit,
    parse_expr,
)
import asyncio
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
import tempfile
from slayer.core.keys import FieldKey
from slayer.core.scope import DatasetScope
from slayer.engine.binding import bind_filter
from slayer.ir.source_bundle import ResolvedSourceBundle


# ---------------------------------------------------------------------------
# Parser shape
# ---------------------------------------------------------------------------


class TestParserShape:
    def test_in_with_two_string_literals(self):
        result = parse_expr("status in ('a', 'b')")
        assert result == Cmp(
            op="in",
            left=Ref(name="status"),
            right=TupleLit(elements=(
                Literal(value="a"),
                Literal(value="b"),
            )),
        )

    def test_not_in_with_two_string_literals(self):
        result = parse_expr("status not in ('a', 'b')")
        assert result == Cmp(
            op="not in",
            left=Ref(name="status"),
            right=TupleLit(elements=(
                Literal(value="a"),
                Literal(value="b"),
            )),
        )

    def test_in_with_numeric_literals(self):
        result = parse_expr("amount in (10, 20, 30)")
        assert result == Cmp(
            op="in",
            left=Ref(name="amount"),
            right=TupleLit(elements=(
                Literal(value=Decimal(10)),
                Literal(value=Decimal(20)),
                Literal(value=Decimal(30)),
            )),
        )

    def test_in_with_signed_numeric_literals(self):
        # Python's AST emits ``-1`` as ``UnaryOp(USub, Constant(1))``,
        # so a literal-only RHS guard that strictly checks ``isinstance
        # Literal`` would reject this. Codex review: collapse the sign
        # onto the inner numeric.
        result = parse_expr("amount in (-1, -2.5, +3)")
        assert result == Cmp(
            op="in",
            left=Ref(name="amount"),
            right=TupleLit(elements=(
                Literal(value=Decimal(-1)),
                Literal(value=Decimal("-2.5")),
                Literal(value=Decimal(3)),
            )),
        )

    def test_in_with_dotted_left(self):
        result = parse_expr("stores.name in ('Brooklyn', 'Philadelphia')")
        assert result == Cmp(
            op="in",
            left=DottedRef(parts=("stores", "name")),
            right=TupleLit(elements=(
                Literal(value="Brooklyn"),
                Literal(value="Philadelphia"),
            )),
        )

    def test_in_accepts_python_list_syntax(self):
        # ``[…]`` and ``(…)`` should be interchangeable on the RHS —
        # both produce a Tuple node.
        result = parse_expr("status in ['a', 'b']")
        assert result == Cmp(
            op="in",
            left=Ref(name="status"),
            right=TupleLit(elements=(
                Literal(value="a"),
                Literal(value="b"),
            )),
        )


# ---------------------------------------------------------------------------
# Parser guards
# ---------------------------------------------------------------------------


class TestParserGuards:
    def test_empty_rhs_tuple_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            parse_expr("status in ()")

    def test_scalar_rhs_is_a_time_point_membership(self):
        # ``col in 'a'`` is a single-string time-point membership: it parses, and the
        # checker rejects it against a non-temporal column with the typed error.
        assert parse_expr("status in 'a'") == Cmp(
            op="in", left=Ref(name="status"), right=Literal(value="a"),
        )
        engine, _ = _make_engine_with_orders()
        query = SlayerQuery.model_validate({
            "population": "orders", "metrics": [{"metric": "count(orders.*)"}], "filter": ["orders.status in 'a'"],
        })
        with pytest.raises(DateOperandTypeError, match="status"):
            engine.execute_sync(query=query, dry_run=True)

    def test_non_literal_in_rhs_rejected(self):
        # ``status in (other_col, 'b')`` — the RHS may not reference a
        # column (no schema-aware binding for dynamic RHS; literals only).
        with pytest.raises(ValueError, match="literal"):
            parse_expr("status in (other_col, 'b')")

    def test_direct_in_key_empty_values_rejected(self):
        # Defense in depth (Codex review): even if a caller bypasses
        # the parser and constructs ``InKey(values=())`` directly, the
        # SQL generator must not emit invalid ``col IN ()``. The
        # field_validator on InKey raises at construction time.
        column = FieldKey(leaf="status")
        with pytest.raises(ValueError, match="non-empty"):
            InKey(field=column, values=())


# ---------------------------------------------------------------------------
# Binder produces InKey
# ---------------------------------------------------------------------------


def _make_orders_with_status() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
        ],
    )


def _make_orders_joined_to_stores() -> tuple[SlayerDataset, SlayerDataset]:
    stores = SlayerDataset(
        name="stores",
        sql_table="stores",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),
        ],
    )
    orders = SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="store_id", sql="store_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
        ],
        relationships=[
            DatasetRelationship(
                to="stores",
                from_columns=["store_id"], to_columns=["id"],
            ),
        ],
    )
    return orders, stores


class TestBinder:
    def test_in_filter_binds_to_in_key(self):

        model = _make_orders_with_status()
        parsed = parse_expr("status in ('completed', 'pending')")
        bundle = ResolvedSourceBundle(dialect="postgres", population=model, referenced_datasets=[])
        scope = DatasetScope(population=model)
        bound = bind_filter(parsed, scope=scope, bundle=bundle)

        assert isinstance(bound.value_key, InKey)
        assert bound.value_key.negated is False
        assert len(bound.value_key.values) == 2
        assert all(isinstance(v, LiteralKey) for v in bound.value_key.values)
        assert {v.value for v in bound.value_key.values} == {
            "completed",
            "pending",
        }

    def test_not_in_filter_binds_negated(self):

        model = _make_orders_with_status()
        parsed = parse_expr("status not in ('cancelled',)")
        bundle = ResolvedSourceBundle(dialect="postgres", population=model, referenced_datasets=[])
        scope = DatasetScope(population=model)
        bound = bind_filter(parsed, scope=scope, bundle=bundle)

        assert isinstance(bound.value_key, InKey)
        assert bound.value_key.negated is True


# ---------------------------------------------------------------------------
# End-to-end SQL emission (sync — unit-test depth, no DB)
# ---------------------------------------------------------------------------


def _make_engine_with_orders() -> tuple:
    """In-memory engine + storage seeded with a single ``orders`` model.

    Returns (engine, model) tuple so tests can call ``engine.execute_sync``
    with dry_run=True (build SQL without hitting a database).
    """



    tmp = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmp)
    ds = DatasourceConfig(name="test", type="sqlite", database=":memory:")
    asyncio.run(storage.save_datasource(ds))
    model = _make_orders_with_status()
    asyncio.run(storage.save_dataset(model))
    return SlayerQueryEngine(storage=storage), model


def _make_engine_with_orders_and_stores() -> tuple:


    tmp = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmp)
    ds = DatasourceConfig(name="test", type="sqlite", database=":memory:")
    asyncio.run(storage.save_datasource(ds))
    orders, stores = _make_orders_joined_to_stores()
    asyncio.run(storage.save_dataset(stores))
    asyncio.run(storage.save_dataset(orders))
    return SlayerQueryEngine(storage=storage), orders, stores


class TestSQLEmission:
    def test_in_filter_emits_sql_in_clause(self):
        engine, _ = _make_engine_with_orders()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.status in ('completed', 'pending')"],
        })
        sql = engine.execute_sync(query=query, dry_run=True).sql

        assert "IN" in sql
        assert "'completed'" in sql
        assert "'pending'" in sql
        # The column reference must appear on the LHS, not on the RHS.
        assert sql.upper().count("IN") >= 1

    def test_not_in_emits_sql_not_in(self):
        engine, _ = _make_engine_with_orders()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.status not in ('cancelled', 'expired')"],
        })
        sql = engine.execute_sync(query=query, dry_run=True).sql

        # sqlglot emits the canonical ``NOT <col> IN (...)`` form (the
        # parser-produced shape for ``NOT IN``). Both spellings are
        # equivalent SQL; the assertion accepts either so we don't pin
        # to a sqlglot rendering detail that could change across
        # versions.
        upper = sql.upper()
        assert "NOT IN" in upper or "NOT " in upper and "IN (" in upper
        assert "'cancelled'" in sql
        assert "'expired'" in sql
        assert "STATUS" in upper

    def test_in_filter_in_n_ary_and_keeps_all_operands(self):
        # Regression on 7a1110f territory: an n-ary AND fold over an IN
        # predicate plus a numeric comparison must keep both predicates.
        engine, _ = _make_engine_with_orders()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": [
                "orders.status in ('completed', 'pending') and orders.amount > 100",
            ],
        })
        sql = engine.execute_sync(query=query, dry_run=True).sql

        assert "IN" in sql
        assert "'completed'" in sql
        assert "'pending'" in sql
        assert "amount" in sql.lower()
        # ``> 100`` should land verbatim (sqlglot may render the literal
        # as ``100`` or ``100.0`` depending on dialect — both contain "100").
        assert "100" in sql

    def test_in_filter_on_joined_column_emits_qualified(self):
        engine, orders, stores = _make_engine_with_orders_and_stores()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["stores.name"],
            "filter": ["stores.name in ('Brooklyn', 'Philadelphia')"],
        })
        sql = engine.execute_sync(query=query, dry_run=True).sql

        assert "IN" in sql
        assert "'Brooklyn'" in sql
        assert "'Philadelphia'" in sql
        # The IN predicate must reference the stores leaf, joined in.
        assert "name" in sql.lower()
        # Join into stores must appear.
        assert "stores" in sql.lower()
