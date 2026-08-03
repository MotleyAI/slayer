"""DEV-1706 Stage 2 — ``ScopeFrame`` + single resolver (``slayer/sql/scope.py``).

Law 1 (anchored rendering): every ref enters a scope through ``resolve``, which
expands derived refs, anchors them at the scope root or a ``__``-path join alias,
and REGISTERS every crossed join path into ``scope.join_paths`` as a side effect.

Law 2 (projection boundaries): a ``resolve(ref, consumer=<scope>)`` call
materialises the value as a ``_val_<n>`` projection in the PRODUCING scope
(edge-aware — Codex F5/M3) and returns a bare alias for the consumer. Stage 2's
host base never crosses a boundary, so this branch has no generated-SQL path
yet — these direct tests are its only coverage (Codex M4, D-D).

Fails at import until ``slayer/sql/scope.py`` exists — intended red state.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.keys import ColumnKey, ColumnSqlKey
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.generator import SQLGenerator
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import Materialization, ScopeFrame
from slayer.sql.scope_check import assert_scope_closed


# --------------------------------------------------------------------------- #
# Models — the orders → customers → regions chain (+ a reserved-word variant).
# --------------------------------------------------------------------------- #
def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="population", sql="population", type=DataType.DOUBLE),
            Column(name="weight", sql="weight", type=DataType.DOUBLE),
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


def _orders(*, extra=None) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
        Column(name="balance", sql="balance", type=DataType.DOUBLE),
    ]
    cols += extra or []
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test", columns=cols,
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def _reserved_host() -> SlayerModel:
    # Model NAME is a reserved word ("order"); joins to another reserved name
    # ("user"). DEV-1686: aliases/qualifiers for these must be QUOTED.
    return SlayerModel(
        name="order", sql_table="orders_tbl", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="user_id", sql="user_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="user", join_pairs=[["user_id", "id"]])],
    )


def _reserved_target() -> SlayerModel:
    return SlayerModel(
        name="user", sql_table="users_tbl", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="grade", sql="grade", type=DataType.DOUBLE),
        ],
    )


def _scope(host: SlayerModel, *others: SlayerModel, dialect: str = "postgres",
           allocator: AliasAllocator | None = None) -> ScopeFrame:
    alloc = allocator or AliasAllocator()
    bundle = ResolvedSourceBundle(source_model=host, referenced_models=[host, *others])
    return ScopeFrame(
        scope_id=alloc.next_scope_id(host.name),
        root_model=host, root_relation=host.name,
        bundle=bundle, dialect=get_dialect(dialect), allocator=alloc,
    )


# --------------------------------------------------------------------------- #
# Law 1 — anchored rendering + join registration.
# --------------------------------------------------------------------------- #
class TestResolveRegistersJoins:
    def test_local_columnkey_anchors_to_root_no_join(self) -> None:
        scope = _scope(_orders(), _customers(), _regions())
        expr = scope.resolve(ColumnKey(leaf="amount"))
        assert expr.sql(dialect="postgres") == "orders.amount"
        assert scope.join_paths.as_list() == []

    def test_single_hop_joined_columnkey_registers_path(self) -> None:
        scope = _scope(_orders(), _customers(), _regions())
        expr = scope.resolve(ColumnKey(path=("customers",), leaf="region_id"))
        assert expr.sql(dialect="postgres") == "customers.region_id"
        assert scope.join_paths.as_list() == [("customers",)]

    def test_multi_hop_columnkey_registers_every_prefix(self) -> None:
        scope = _scope(_orders(), _customers(), _regions())
        expr = scope.resolve(ColumnKey(path=("customers", "regions"), leaf="name"))
        assert expr.sql(dialect="postgres") == "customers__regions.name"
        assert scope.join_paths.as_list() == [("customers",), ("customers", "regions")]

    def test_derived_columnsqlkey_crossing_registers_prefixes(self) -> None:
        orders = _orders(extra=[
            Column(name="region_weight", sql="customers__regions.weight",
                   type=DataType.DOUBLE),
        ])
        scope = _scope(orders, _customers(), _regions())
        expr = scope.resolve(ColumnSqlKey(model="orders", column_name="region_weight"))
        assert "customers__regions.weight" in expr.sql(dialect="postgres")
        assert scope.join_paths.as_list() == [("customers",), ("customers", "regions")]

    def test_registration_dedupes_and_preserves_order(self) -> None:
        scope = _scope(_orders(), _customers(), _regions())
        scope.resolve(ColumnKey(path=("customers", "regions"), leaf="name"))
        scope.resolve(ColumnKey(path=("customers",), leaf="balance"))  # already seen
        # ``customers`` was registered first (as a prefix); dedup keeps one copy
        # in first-seen order.
        assert scope.join_paths.as_list() == [("customers",), ("customers", "regions")]


# --------------------------------------------------------------------------- #
# DEV-1686 — reserved-identifier quoting, compositional (Codex M5/H1).
# --------------------------------------------------------------------------- #
class TestResolveReservedQuoting:
    def test_reserved_root_qualifier_quoted(self) -> None:
        scope = _scope(_reserved_host(), _reserved_target())
        expr = scope.resolve(ColumnKey(leaf="amount"))
        assert '"order"' in expr.sql(dialect="postgres")

    def test_reserved_join_alias_quoted_and_registered(self) -> None:
        scope = _scope(_reserved_host(), _reserved_target())
        expr = scope.resolve(ColumnKey(path=("user",), leaf="grade"))
        assert '"user"' in expr.sql(dialect="postgres")
        assert scope.join_paths.as_list() == [("user",)]

    def test_resolver_alias_matches_build_from_and_joins(self) -> None:
        # D-K / Codex H1: the alias the resolver anchors a reserved-word join
        # path to MUST equal the alias ``_build_from_and_joins`` emits — else the
        # cached expr references a table the FROM never binds.
        host, target = _reserved_host(), _reserved_target()
        scope = _scope(host, target)
        expr = scope.resolve(ColumnKey(path=("user",), leaf="grade"))
        resolver_alias = expr.find(exp.Column).table  # unquoted identifier name

        gen = SQLGenerator(dialect="postgres")
        _from, joins = gen._build_from_and_joins(
            source_model=host, source_relation="order",
            joined_paths=[("user",)], bundle=scope.bundle,
        )
        emitted_aliases = {
            j.alias_or_name for (j, _on, _jt) in joins if isinstance(j, exp.Table)
        }
        assert resolver_alias in emitted_aliases

    def test_resolver_alias_matches_build_from_and_joins_multi_hop(self) -> None:
        # H1 across a multi-hop path: both the resolver and _build_from_and_joins
        # must choose the structural ``customers__regions`` alias.
        scope = _scope(_orders(), _customers(), _regions())
        expr = scope.resolve(
            ColumnKey(path=("customers", "regions"), leaf="population"))
        resolver_alias = expr.find(exp.Column).table
        gen = SQLGenerator(dialect="postgres")
        _from, joins = gen._build_from_and_joins(
            source_model=_orders(), source_relation="orders",
            joined_paths=[("customers",), ("customers", "regions")],
            bundle=scope.bundle,
        )
        emitted = {j.alias_or_name for (j, _o, _t) in joins if isinstance(j, exp.Table)}
        assert resolver_alias == "customers__regions"
        assert resolver_alias in emitted

    def test_derived_columnsqlkey_reserved_qualifier_quoted(self) -> None:
        # A derived ColumnSqlKey whose sql crosses to a reserved-word join target
        # ("user") must emit the quoted qualifier and register the join (the
        # "reserved prequoted expansion" path, not just a bare ColumnKey).
        host = SlayerModel(
            name="order", sql_table="orders_tbl", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="user_id", sql="user_id", type=DataType.DOUBLE),
                Column(name="user_grade", sql="user.grade", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="user", join_pairs=[["user_id", "id"]])],
        )
        scope = _scope(host, _reserved_target())
        expr = scope.resolve(ColumnSqlKey(model="order", column_name="user_grade"))
        assert '"user"' in expr.sql(dialect="postgres")
        assert scope.join_paths.as_list() == [("user",)]


# --------------------------------------------------------------------------- #
# DEV-1539 — predicate outer-parens / precedence (Codex M5).
# --------------------------------------------------------------------------- #
class TestResolvePredicateParens:
    @staticmethod
    def _derived_orders():
        return _orders(extra=[
            Column(name="total", sql="amount + balance", type=DataType.DOUBLE),
        ])

    def test_derived_expansion_parenthesised_preserves_precedence(self) -> None:
        scope = _scope(self._derived_orders(), _customers(), _regions())
        sql = scope.resolve_predicate_sql("total * 2 > 0")
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        mul = parsed.find(exp.Mul)
        assert mul is not None, f"no multiplication in {sql!r}"
        # The multiplied operand must be a parenthesised group so precedence is
        # ``(amount + balance) * 2`` — NOT ``amount + balance * 2``.
        assert isinstance(mul.left, exp.Paren), (
            f"expansion not parenthesised — precedence broken: {sql!r}"
        )

    @pytest.mark.parametrize("predicate", [
        "total > 0",                       # bare comparison
        "amount > 1 AND total > 0",        # AND connector
        "amount > 1 OR total > 0",         # OR connector
        "NOT (total > 0)",                 # NOT
        "total BETWEEN 1 AND 5",           # BETWEEN
        "total IN (1, 2, 3)",              # IN
    ])
    def test_multiterm_derived_expansion_is_parenthesised(self, predicate: str) -> None:
        # Wherever the multi-term derived ``total`` is substituted into a
        # predicate, its expansion (amount + balance) must be wrapped in parens so
        # precedence can never leak — robust to qualification form (re-parse and
        # assert the Add node's parent is a Paren).
        scope = _scope(self._derived_orders(), _customers(), _regions())
        sql = scope.resolve_predicate_sql(predicate)
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        add = parsed.find(exp.Add)
        assert add is not None, f"expansion missing in {sql!r}"
        assert isinstance(add.parent, exp.Paren), (
            f"multi-term expansion not parenthesised in {predicate!r}: {sql!r}"
        )


# --------------------------------------------------------------------------- #
# Law 2 — projection-boundary materialisation (edge-aware).
# --------------------------------------------------------------------------- #
class TestMaterialization:
    def _producer_consumer(self):
        alloc = AliasAllocator()
        producer = _scope(_orders(), _customers(), _regions(), allocator=alloc)
        consumer = _scope(_orders(), _customers(), _regions(), allocator=alloc)
        return producer, consumer

    def test_consumer_gets_bare_val_alias(self) -> None:
        producer, consumer = self._producer_consumer()
        out = producer.resolve(
            ColumnKey(path=("customers", "regions"), leaf="population"),
            consumer=consumer,
        )
        assert out.sql(dialect="postgres") == "_val_0"
        assert len(producer.materializations) == 1
        mat = producer.materializations[0]
        assert isinstance(mat, Materialization)
        assert mat.alias == "_val_0"
        assert "customers__regions.population" in mat.expr.sql(dialect="postgres")
        # Registration still happens in the producer (Law 1 before Law 2).
        assert producer.join_paths.as_list() == [
            ("customers",), ("customers", "regions"),
        ]

    def test_dedup_reuses_val_for_equal_expr(self) -> None:
        producer, consumer = self._producer_consumer()
        ref = ColumnKey(path=("customers",), leaf="balance")
        a = producer.resolve(ref, consumer=consumer)
        b = producer.resolve(ref, consumer=consumer)
        assert a.sql() == b.sql() == "_val_0"
        assert len(producer.materializations) == 1  # F6 dedup
        # Deduped, but the two returned alias refs must be DISTINCT objects, so a
        # consumer attaching one into its tree can't corrupt the other (Codex M1).
        assert a is not b

    def test_distinct_val_for_distinct_expr(self) -> None:
        producer, consumer = self._producer_consumer()
        a = producer.resolve(ColumnKey(path=("customers",), leaf="balance"),
                             consumer=consumer)
        b = producer.resolve(ColumnKey(path=("customers", "regions"), leaf="population"),
                             consumer=consumer)
        assert a.sql() == "_val_0"
        assert b.sql() == "_val_1"
        assert len(producer.materializations) == 2

    def test_dedup_key_distinguishes_producing_scope(self) -> None:
        alloc = AliasAllocator()
        p1 = _scope(_orders(), _customers(), _regions(), allocator=alloc)
        p2 = _scope(_orders(), _customers(), _regions(), allocator=alloc)
        consumer = _scope(_orders(), _customers(), _regions(), allocator=alloc)
        ref = ColumnKey(path=("customers",), leaf="balance")
        m1 = p1.resolve(ref, consumer=consumer)
        m2 = p2.resolve(ref, consumer=consumer)
        # Same expr text, different producing scope → NOT deduped (F6 key
        # includes producing-scope identity).
        assert m1.sql() != m2.sql()
        assert p1.materializations[0].dedup_key != p2.materializations[0].dedup_key

    def test_may_inline_is_false(self) -> None:
        producer, _ = self._producer_consumer()
        assert producer.may_inline((("customers",),)) is False

    def test_apply_materializations_projects_val(self) -> None:
        producer, consumer = self._producer_consumer()
        producer.resolve(
            ColumnKey(path=("customers", "regions"), leaf="population"),
            consumer=consumer,
        )
        select = exp.Select().select(exp.column("id", table="orders")).from_("orders")
        producer.apply_materializations(select)
        proj = select.sql(dialect="postgres")
        assert "AS _val_0" in proj
        assert "customers__regions.population" in proj

    def test_apply_materializations_projects_copies_not_template(self) -> None:
        # Codex M1 / D-L: the cached template must stay parent-less, so a reused
        # expr can never be corrupted by in-place sqlglot parent-pointer mutation.
        producer, consumer = self._producer_consumer()
        producer.resolve(ColumnKey(path=("customers",), leaf="balance"),
                        consumer=consumer)
        template = producer.materializations[0].expr
        s1 = exp.Select().select(exp.column("id", table="orders")).from_("orders")
        s2 = exp.Select().select(exp.column("id", table="orders")).from_("orders")
        producer.apply_materializations(s1)
        producer.apply_materializations(s2)
        assert template.parent is None  # projected a copy, not the template

    def test_two_scope_sql_is_scope_closed(self) -> None:
        # Codex M4: prove the Law-2 wiring end to end — the producer projects
        # ``<template> AS _val_0`` over a FROM that binds the crossed joins, and
        # an outer scope referencing ``sub._val_0`` is scope-closed.
        producer, consumer = self._producer_consumer()
        out = producer.resolve(
            ColumnKey(path=("customers", "regions"), leaf="population"),
            consumer=consumer,
        )
        gen = SQLGenerator(dialect="postgres")
        from_clause, joins = gen._build_from_and_joins(
            source_model=producer.root_model, source_relation="orders",
            joined_paths=producer.join_paths.as_list(), bundle=producer.bundle,
        )
        inner = exp.Select().select(exp.column("id", table="orders")).from_(from_clause)
        for j, on, jt in joins:
            inner = inner.join(j, on=on, join_type=jt)
        producer.apply_materializations(inner)
        # The producing scope must actually project ``<template> AS _val_0`` — the
        # outer ``sub._val_0`` reference is only closed because the inner exports it.
        assert "AS _val_0" in inner.sql(dialect="postgres")
        outer = (
            exp.Select()
            .select(exp.column(out.this if hasattr(out, "this") else "_val_0", table="sub"))
            .from_(exp.Subquery(this=inner, alias=exp.TableAlias(this=exp.to_identifier("sub"))))
        )
        assert_scope_closed(outer.sql(dialect="postgres"), dialect="postgres")


# --------------------------------------------------------------------------- #
# DEV-1711 Stage 7 — path'd ColumnSqlKey (derived column ON a joined model).
# The shifted-CTE partition/time resolver hands ``resolve`` a ColumnSqlKey whose
# ``path`` is non-empty (e.g. ``stores.tier`` where ``tier = upper(name)`` is
# derived on the joined ``stores`` model). ``_anchor`` must expand it rooted at
# the ``__``-path alias (``is_root=False``) and register the join chain — today
# it ignores ``path`` and mis-anchors at the host relation.
# --------------------------------------------------------------------------- #
class TestResolvePathedColumnSqlKey:
    @staticmethod
    def _customers_with_tier():
        # ``tier = upper(balance)`` — a LOCAL derived column on the joined
        # customers model (its inner ref stays on customers).
        return SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
                Column(name="balance", sql="balance", type=DataType.DOUBLE),
                Column(name="tier", sql="upper(balance)", type=DataType.TEXT),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )

    def test_single_hop_derived_anchors_at_path_alias(self) -> None:
        scope = _scope(_orders(), self._customers_with_tier(), _regions())
        expr = scope.resolve(
            ColumnSqlKey(path=("customers",), model="customers", column_name="tier"))
        rendered = expr.sql(dialect="postgres")
        # The derived column is EXPANDED (the ``upper(...)`` wrapper is preserved,
        # not dropped to the bare column) AND its inner ref qualifies under the
        # JOINED alias, not the host — ``UPPER(customers.balance)``, never
        # ``UPPER(orders.balance)``.
        assert "UPPER(customers.balance)" in rendered, rendered
        assert "orders.balance" not in rendered, rendered
        # The join it lives behind is registered (Law 1).
        assert scope.join_paths.as_list() == [("customers",)]

    def test_single_hop_derived_registers_only_its_join(self) -> None:
        scope = _scope(_orders(), self._customers_with_tier(), _regions())
        scope.resolve(
            ColumnSqlKey(path=("customers",), model="customers", column_name="tier"))
        # ``regions`` is NOT referenced by ``tier`` — only the ``customers`` hop
        # is pulled.
        assert ("customers", "regions") not in scope.join_paths.as_list()

    def test_joined_derived_crossing_further_join_registers_prefixes(self) -> None:
        # A derived column on the joined model whose sql crosses a FURTHER join
        # (``deep = regions.population`` on customers) anchors the further ref at
        # the full ``customers__regions`` path (DEV-1701 shape) and registers
        # both prefixes.
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
                Column(name="deep", sql="regions.population", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
        scope = _scope(_orders(), customers, _regions())
        expr = scope.resolve(
            ColumnSqlKey(path=("customers",), model="customers", column_name="deep"))
        rendered = expr.sql(dialect="postgres")
        assert "customers__regions.population" in rendered, rendered
        assert scope.join_paths.as_list() == [
            ("customers",), ("customers", "regions"),
        ]


# --------------------------------------------------------------------------- #
# Cached-expr ownership (Codex M1 / D-L).
# --------------------------------------------------------------------------- #
class TestResolveOwnership:
    def test_resolve_returns_fresh_object_each_call(self) -> None:
        scope = _scope(_orders(), _customers(), _regions())
        ref = ColumnKey(path=("customers",), leaf="region_id")
        r1 = scope.resolve(ref)
        r2 = scope.resolve(ref)
        assert r1 is not r2  # callers can mutate/attach without cross-talk
