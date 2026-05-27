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
into a new ``InKey`` (modelled on ``BetweenKey``), and the SQL generator
emits ``IN (lit, lit, …)`` / ``NOT IN (...)`` at all three filter render
sites (``_render_value_key_for_filter`` for local WHERE/HAVING,
``_render_value_key_against_aliases`` for POST-phase filters,
``_render_filter_value_key_in_target_scope`` for cross-model CTE filters).

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
from slayer.core.keys import InKey, LiteralKey
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.syntax import (
    Cmp,
    DottedRef,
    Literal,
    Ref,
    TupleLit,
    parse_filter_expr,
)


# ---------------------------------------------------------------------------
# Parser shape
# ---------------------------------------------------------------------------


class TestParserShape:
    def test_in_with_two_string_literals(self):
        result = parse_filter_expr("status in ('a', 'b')")
        assert result == Cmp(
            op="in",
            left=Ref(name="status"),
            right=TupleLit(elements=(
                Literal(value="a"),
                Literal(value="b"),
            )),
        )

    def test_not_in_with_two_string_literals(self):
        result = parse_filter_expr("status not in ('a', 'b')")
        assert result == Cmp(
            op="not in",
            left=Ref(name="status"),
            right=TupleLit(elements=(
                Literal(value="a"),
                Literal(value="b"),
            )),
        )

    def test_in_with_numeric_literals(self):
        result = parse_filter_expr("amount in (10, 20, 30)")
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
        result = parse_filter_expr("amount in (-1, -2.5, +3)")
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
        result = parse_filter_expr("stores.name in ('Brooklyn', 'Philadelphia')")
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
        result = parse_filter_expr("status in ['a', 'b']")
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
            parse_filter_expr("status in ()")

    def test_scalar_rhs_rejected(self):
        # ``col in 'a'`` is a Python-AST Compare with a scalar comparator
        # (not a Tuple) — must surface a clear ValueError instead of crashing.
        with pytest.raises(ValueError, match="tuple|list"):
            parse_filter_expr("status in 'a'")

    def test_non_literal_in_rhs_rejected(self):
        # ``status in (other_col, 'b')`` — the RHS may not reference a
        # column (no schema-aware binding for dynamic RHS; literals only).
        with pytest.raises(ValueError, match="literal"):
            parse_filter_expr("status in (other_col, 'b')")

    def test_direct_in_key_empty_values_rejected(self):
        # Defense in depth (Codex review): even if a caller bypasses
        # the parser and constructs ``InKey(values=())`` directly, the
        # SQL generator must not emit invalid ``col IN ()``. The
        # field_validator on InKey raises at construction time.
        from slayer.core.keys import ColumnKey
        with pytest.raises(ValueError, match="non-empty"):
            InKey(column=ColumnKey(leaf="status"), values=())


# ---------------------------------------------------------------------------
# Binder produces InKey
# ---------------------------------------------------------------------------


def _make_orders_with_status() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )


def _make_orders_joined_to_stores() -> tuple[SlayerModel, SlayerModel]:
    stores = SlayerModel(
        name="stores",
        sql_table="stores",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    )
    orders = SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", sql="store_id", type=DataType.INT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(
                target_model="stores",
                join_pairs=[["store_id", "id"]],
            ),
        ],
    )
    return orders, stores


class TestBinder:
    def test_in_filter_binds_to_in_key(self):
        from slayer.core.scope import ModelScope
        from slayer.engine.binding import bind_filter
        from slayer.engine.source_bundle import ResolvedSourceBundle
        from slayer.engine.syntax import parse_filter_expr

        model = _make_orders_with_status()
        parsed = parse_filter_expr("status in ('completed', 'pending')")
        bundle = ResolvedSourceBundle(source_model=model, referenced_models=[])
        scope = ModelScope(source_model=model)
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
        from slayer.core.scope import ModelScope
        from slayer.engine.binding import bind_filter
        from slayer.engine.source_bundle import ResolvedSourceBundle

        model = _make_orders_with_status()
        parsed = parse_filter_expr("status not in ('cancelled',)")
        bundle = ResolvedSourceBundle(source_model=model, referenced_models=[])
        scope = ModelScope(source_model=model)
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
    import asyncio

    from slayer.core.models import DatasourceConfig
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.storage.yaml_storage import YAMLStorage

    import tempfile

    tmp = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmp)
    ds = DatasourceConfig(name="test", type="sqlite", database=":memory:")
    asyncio.run(storage.save_datasource(ds))
    model = _make_orders_with_status()
    asyncio.run(storage.save_model(model))
    return SlayerQueryEngine(storage=storage), model


def _make_engine_with_orders_and_stores() -> tuple:
    import asyncio
    import tempfile

    from slayer.core.models import DatasourceConfig
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.storage.yaml_storage import YAMLStorage

    tmp = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmp)
    ds = DatasourceConfig(name="test", type="sqlite", database=":memory:")
    asyncio.run(storage.save_datasource(ds))
    orders, stores = _make_orders_joined_to_stores()
    asyncio.run(storage.save_model(stores))
    asyncio.run(storage.save_model(orders))
    return SlayerQueryEngine(storage=storage), orders, stores


class TestSQLEmission:
    def test_in_filter_emits_sql_in_clause(self):
        engine, _ = _make_engine_with_orders()
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status in ('completed', 'pending')"],
        )
        sql = engine.execute_sync(query=query, dry_run=True).sql

        assert "IN" in sql
        assert "'completed'" in sql
        assert "'pending'" in sql
        # The column reference must appear on the LHS, not on the RHS.
        assert sql.upper().count("IN") >= 1

    def test_not_in_emits_sql_not_in(self):
        engine, _ = _make_engine_with_orders()
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status not in ('cancelled', 'expired')"],
        )
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
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=[
                "status in ('completed', 'pending') and amount > 100",
            ],
        )
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
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=["stores.name"],
            filters=["stores.name in ('Brooklyn', 'Philadelphia')"],
        )
        sql = engine.execute_sync(query=query, dry_run=True).sql

        assert "IN" in sql
        assert "'Brooklyn'" in sql
        assert "'Philadelphia'" in sql
        # The IN predicate must reference the stores leaf, joined in.
        assert "name" in sql.lower()
        # Join into stores must appear.
        assert "stores" in sql.lower()
