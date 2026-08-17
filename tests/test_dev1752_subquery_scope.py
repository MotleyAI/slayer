"""DEV-1752 — Mode-A expansion must not qualify subquery columns against the
outer root.

A subquery's own columns belong to the subquery's scope, not the outer model's.
Before the fix, ``_process_column_node_sync`` qualified every column node against
the outer root BEFORE the root-scope gate, so ``amount IN (SELECT amount FROM
other_tbl)`` became ``orders.amount IN (SELECT orders.amount FROM other_tbl)`` —
silently rebinding the inner ``amount`` to the wrong table.

Contract: a Mode-A subquery must be self-contained (scope-closed). A bare name
binds to the subquery's own FROM; the expander leaves it alone. Correlation to
the outer model is NOT a supported Mode-A feature — an explicit outer reference
(``orders.col``) is a scope leak that ``assert_scope_closed`` rejects (the only
legal correlated ref in generated SQL is the RLS session-policy EXISTS). The
expander is scope-aware and does not rebind such refs; the scope-closure guard,
not the expander, is what forbids correlation.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.column_expansion import collect_root_scope_joined_paths
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import ScopeFrame
from slayer.sql.scope_check import ScopeLeakError
from slayer.storage.yaml_storage import YAMLStorage


# --------------------------------------------------------------------------- #
# ScopeFrame fixtures (mirror tests/test_dev1745_mode_a_door.py)
# --------------------------------------------------------------------------- #
def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="population", type=DataType.DOUBLE),
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


def _inner_select_sql(rendered: str, dialect: str = "postgres") -> str:
    """The rendered SQL of the first nested SELECT (the subquery body)."""
    tree = sqlglot.parse_one(rendered, dialect=dialect)
    inner = tree.find(exp.Select)
    assert inner is not None, f"no subquery SELECT found in:\n{rendered}"
    return inner.sql(dialect=dialect)


# --------------------------------------------------------------------------- #
# Scope-awareness: a subquery's own columns stay in the subquery's scope
# --------------------------------------------------------------------------- #
class TestSubqueryColumnsStayLocal:

    def test_non_correlated_in_subquery_keeps_inner_bare(self) -> None:
        out = _sql_of(
            _scope().enter_predicate("amount IN (SELECT amount FROM other_tbl)")
        )
        inner = _inner_select_sql(out)
        assert "orders" not in inner, (
            f"inner subquery column was qualified against the outer root: {out}"
        )
        # The OUTER amount still qualifies to the root.
        assert "orders.amount IN" in out, out

    def test_scalar_subquery_keeps_inner_bare(self) -> None:
        out = _sql_of(
            _scope().enter_predicate("status = (SELECT status FROM lookup LIMIT 1)")
        )
        inner = _inner_select_sql(out)
        assert "orders" not in inner, out
        assert "orders.status =" in out, out

    def test_inner_name_colliding_with_root_column_stays_bare(self) -> None:
        # ``amount`` IS a real orders column; the fix must be scope-driven, not
        # name-driven — the inner ``amount`` still must not bind to orders.
        out = _sql_of(
            _scope().enter_predicate(
                "amount IN (SELECT amount FROM other_tbl WHERE other_tbl.k = 1)"
            )
        )
        inner = _inner_select_sql(out)
        assert "orders" not in inner, out

    def test_expression_surface_keeps_inner_bare(self) -> None:
        # Not predicate-only: a Column.sql scalar containing a subquery behaves
        # the same through enter_expression.
        out = _sql_of(
            _scope().enter_expression("(SELECT amount FROM other_tbl LIMIT 1)")
        )
        inner = _inner_select_sql(out)
        assert "orders" not in inner, out


# --------------------------------------------------------------------------- #
# Correlation contract: bare = local; an explicit outer ref is a scope leak
# --------------------------------------------------------------------------- #
class TestCorrelationContract:

    def test_expander_does_not_rebind_explicit_outer_ref(self) -> None:
        # The expander is scope-aware: it leaves an explicit outer ref untouched
        # (it does NOT invent a rebind). Whether that ref is legal is decided
        # downstream by assert_scope_closed, not here — see
        # TestExecution.test_correlated_subquery_is_rejected.
        out = _sql_of(
            _scope().enter_predicate(
                "EXISTS (SELECT 1 FROM line_items "
                "WHERE line_items.order_id = orders.id)"
            )
        )
        inner = _inner_select_sql(out)
        assert "orders.id" in inner, out
        assert "line_items.order_id" in inner, out

    def test_bare_reference_in_subquery_stays_bare(self) -> None:
        # Documented limitation: a BARE name inside a subquery binds to the
        # subquery's own FROM (local), never the outer root. Pin it so the
        # contract is asserted, not accidental.
        out = _sql_of(
            _scope().enter_predicate(
                "EXISTS (SELECT 1 FROM line_items WHERE line_items.amount = amount)"
            )
        )
        inner = _inner_select_sql(out)
        assert "orders.amount" not in inner, (
            f"bare inner reference was rebound to the outer root: {out}"
        )


# --------------------------------------------------------------------------- #
# Root-scope refs are unaffected (guard against over-gating)
# --------------------------------------------------------------------------- #
class TestRootScopeUnaffected:

    def test_top_level_bare_column_still_qualifies(self) -> None:
        out = _sql_of(_scope().enter_predicate("amount > 1"))
        assert "orders.amount > 1" in out, out

    def test_top_level_derived_column_still_inlines(self) -> None:
        out = _sql_of(_scope().enter_predicate("doubled > 1"))
        # ``doubled`` derives to ``amount * 2`` and inlines, qualified to root.
        assert "orders.amount * 2" in out, out


# --------------------------------------------------------------------------- #
# Join-path discovery stays root-only (a subquery ref registers no join)
# --------------------------------------------------------------------------- #
class TestJoinDiscoveryRootOnly:

    def test_subquery_join_target_ref_registers_no_join(self) -> None:
        scope = _scope()
        scope.enter_predicate(
            "id IN (SELECT x FROM other WHERE other.k = customers.balance)"
        )
        assert scope.join_paths.as_list() == [], (
            "a join-target-looking ref INSIDE a subquery must not register a "
            f"root join path: {scope.join_paths.as_list()}"
        )

    def test_collect_paths_ignores_subquery_scope(self) -> None:
        parsed = sqlglot.parse_one(
            "id IN (SELECT x FROM other WHERE other.k = customers.balance)",
            dialect="postgres",
        )
        host = _orders()
        bundle = ResolvedSourceBundle(
            source_model=host, referenced_models=[host, _customers(), _regions()],
        )
        paths = collect_root_scope_joined_paths(
            parsed=parsed, source_model=host,
            source_relation=host.name, bundle=bundle,
        )
        assert paths == [], paths

    def test_collect_paths_ignores_two_hop_alias_in_subquery(self) -> None:
        # A resolvable TWO-hop alias inside a subquery must register NEITHER
        # prefix — path-prefix collection is root-only.
        parsed = sqlglot.parse_one(
            "id IN (SELECT x FROM other "
            "WHERE other.k = customers__regions.population)",
            dialect="postgres",
        )
        host = _orders()
        bundle = ResolvedSourceBundle(
            source_model=host, referenced_models=[host, _customers(), _regions()],
        )
        paths = collect_root_scope_joined_paths(
            parsed=parsed, source_model=host,
            source_relation=host.name, bundle=bundle,
        )
        assert paths == [], paths


# --------------------------------------------------------------------------- #
# Execution: real SQLite differential
# --------------------------------------------------------------------------- #
def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    try:
        con.executescript(
            """
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                amount REAL,
                status TEXT
            );
            CREATE TABLE other_tbl (amount REAL);
            CREATE TABLE line_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER,
                note TEXT
            );
            """
        )
        con.executemany(
            "INSERT INTO orders VALUES (?,?,?,?)",
            [(1, 100, 10.0, "a"), (2, 100, 20.0, "b"),
             (3, 101, 30.0, "c"), (4, 101, 40.0, "d")],
        )
        # Only 10.0 and 30.0 are present -> filter must keep exactly orders 1 & 3.
        con.executemany("INSERT INTO other_tbl VALUES (?)", [(10.0,), (30.0,)])
        # line_items reference orders 1 & 3 only; line_items.id is deliberately
        # disjoint from orders.id so a wrong (local) bind of an outer ref would
        # change the answer.
        con.executemany(
            "INSERT INTO line_items VALUES (?,?,?)",
            [(501, 1, "x"), (502, 3, "y")],
        )
        con.commit()
    finally:
        con.close()


async def _engine_with_filter(base_dir: str, db_path: str, *, model_filter: str) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=base_dir)
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=db_path),
    )
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        filters=[model_filter],
    )
    await storage.save_model(orders)
    return SlayerQueryEngine(storage=storage)


def _ids(resp) -> set:
    return {int(r["orders.id"]) for r in resp.data}


class TestExecution:

    async def test_non_correlated_subquery_filters_correctly(self) -> None:
        """Differential: before the fix the buggy ``SELECT orders.amount FROM
        other_tbl`` correlates to the outer row (always-true) and returns ALL
        orders; after the fix the inner ``amount`` binds to ``other_tbl`` and
        only orders 1 & 3 match."""
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "corpus.db")
            _seed_sqlite(db)
            engine = await _engine_with_filter(
                os.path.join(d, "store"), db,
                model_filter="amount IN (SELECT amount FROM other_tbl)",
            )
            resp = await engine.execute(
                SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="id")]),
            )
            assert _ids(resp) == {1, 3}, resp.data

    async def test_correlated_subquery_is_rejected(self) -> None:
        """Contract: a correlated Mode-A subquery references the outer relation,
        which SLayer forbids — the scope-closure guard rejects it. Correlation is
        not a supported Mode-A feature; subqueries must be self-contained. (The
        fix leaves the outer ref untouched; assert_scope_closed is what rejects
        it.)"""
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "corpus.db")
            _seed_sqlite(db)
            engine = await _engine_with_filter(
                os.path.join(d, "store"), db,
                model_filter=(
                    "EXISTS (SELECT 1 FROM line_items "
                    "WHERE line_items.order_id = orders.id)"
                ),
            )
            with pytest.raises(ScopeLeakError):
                await engine.execute(
                    SlayerQuery(
                        source_model="orders", dimensions=[ColumnRef(name="id")],
                    ),
                )
