"""DEV-1745 (W1) — the single Mode-A entry point on ``ScopeFrame`` (P-A).

Every fragment of free SQL enters a SELECT scope through ONE door that
prequotes, expands, registers the joins it crosses, and returns an AST. Join
discovery is a side effect of resolution, never a separate pass.

Two public surfaces over one implementation, differing ONLY in the parse helper:

* ``enter_predicate`` — for ``Column.filter`` and ``SlayerModel.filters``,
  which are boolean predicates. Keeps the ``SELECT 1 WHERE ...`` statement-
  keyword guard.
* ``enter_expression`` — for ``Column.sql``, a scalar expression.

The surface's grammar is a STATIC property of the field, not a runtime choice:
no content-based dispatch, no "try expression then predicate" retry. Either
would re-introduce render-time re-classification (a P-D violation).

Deliberately NOT part of the door: a separate qualification pass.
``expand_derived_refs_sync`` already qualifies, and qualifies correctly —
against the OWNING model's canonical alias. It leaves a node alone when the
alias path does not resolve, because that is an opaque CTE / subquery
reference; a blanket pass against ``root_relation`` would corrupt exactly
those. See ``TestOpaqueReferencesSurvive``.

Removed by this work: ``include_dotted_derived`` (no caller ever passed it, and
its documented ``False`` case was unwired) and the three swallow-all
``except Exception`` lanes — including the one in ``_filter_join_paths._scan``
that silently contributed ZERO join paths, turning an unparseable fragment into
missing joins rather than an error.
"""

from __future__ import annotations

import inspect

import pytest
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import AliasAllocator
from slayer.sql.scope import ScopeFrame


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
# The door exists and has exactly two surfaces
# --------------------------------------------------------------------------- #
class TestDoorSurface:

    def test_enter_predicate_exists(self) -> None:
        assert hasattr(ScopeFrame, "enter_predicate"), (
            "ScopeFrame must expose the single Mode-A predicate entry point"
        )

    def test_enter_expression_exists(self) -> None:
        assert hasattr(ScopeFrame, "enter_expression"), (
            "ScopeFrame must expose the single Mode-A expression entry point"
        )

    def test_no_include_dotted_derived_flag_anywhere(self) -> None:
        """The flag is deleted, not threaded through the new door."""
        for name in ("enter_predicate", "enter_expression"):
            fn = getattr(ScopeFrame, name, None)
            if fn is None:
                pytest.fail(f"ScopeFrame.{name} missing")
            params = set(inspect.signature(fn).parameters)
            assert "include_dotted_derived" not in params
            assert "include_dotted" not in params


# --------------------------------------------------------------------------- #
# Qualification comes from expansion — the door adds no second pass
# --------------------------------------------------------------------------- #
class TestQualification:

    def test_bare_root_column_is_qualified(self) -> None:
        out = _sql_of(_scope().enter_predicate("status = 'x'"))
        assert "orders.status" in out, out

    def test_quoted_bare_column_is_qualified(self) -> None:
        """Assert an actual qualified column, not the two tokens appearing
        independently somewhere in the string."""
        node = _scope().enter_predicate('"status" = \'x\'')
        cols = [
            c for c in node.find_all(exp.Column)
            if c.name == "status"
        ]
        assert cols, _sql_of(node)
        assert all(c.table == "orders" for c in cols), (
            f"quoted bare column was not qualified against the root: "
            f"{_sql_of(node)}"
        )

    def test_joined_derived_column_resolves_against_its_owning_model(self) -> None:
        """A derived column ON customers whose sql names a bare customers
        column must expand to ``customers.balance`` — never ``orders.balance``.
        This is what separates "expansion qualified it, against the owning
        model" from "something re-qualified it against the scope root"."""
        customers = _customers()
        customers.columns.append(
            Column(name="balance_x2", sql="balance * 2", type=DataType.DOUBLE),
        )
        host = _orders()
        # a same-named column on the ROOT, so a root-anchored pass is visible
        host.columns.append(
            Column(name="balance", type=DataType.DOUBLE),
        )
        alloc = AliasAllocator()
        bundle = ResolvedSourceBundle(
            source_model=host, referenced_models=[host, customers, _regions()],
        )
        scope = ScopeFrame(
            scope_id=alloc.next_scope_id(host.name),
            root_model=host, root_relation=host.name,
            bundle=bundle, dialect=get_dialect("postgres"), allocator=alloc,
        )
        out = _sql_of(scope.enter_expression("customers.balance_x2"))
        assert "customers.balance" in out, out
        assert "orders.balance" not in out, (
            f"derived sql was re-qualified against the scope root instead of "
            f"its owning model: {out}"
        )

    def test_local_derived_column_is_inlined_and_qualified(self) -> None:
        out = _sql_of(_scope().enter_expression("doubled"))
        assert "doubled" not in out, f"derived name leaked: {out}"
        assert "orders.amount" in out, out

    def test_already_qualified_join_path_is_left_alone(self) -> None:
        out = _sql_of(_scope().enter_predicate("customers.balance > 1"))
        assert "customers.balance" in out, out


class TestOpaqueReferencesSurvive:
    """A blanket root-relation qualification pass would corrupt these.
    ``_walk_path_to_target_sync`` deliberately leaves an unresolvable alias
    untouched — it is an opaque CTE / subquery reference, not an error."""

    def test_unresolvable_alias_is_not_requalified_to_root(self) -> None:
        out = _sql_of(_scope().enter_predicate("some_cte.flag = 1"))
        assert "orders.some_cte" not in out, (
            f"opaque reference was re-qualified against the scope root: {out}"
        )
        assert "some_cte.flag" in out, out

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Known defect, tracked separately and deliberately NOT fixed in "
            "this PR: qualification in _process_column_node_sync happens "
            "BEFORE the root_scope_ids gate, so it is not scope-aware. Only "
            "derived INLINING is gated. A column inside a subquery with its "
            "own FROM is therefore qualified against the OUTER root: "
            "'amount IN (SELECT amount FROM other_tbl)' becomes "
            "'orders.amount IN (SELECT orders.amount FROM other_tbl)', "
            "silently rebinding the inner reference to the wrong table."
        ),
    )
    def test_subquery_column_is_not_qualified_against_outer_root(self) -> None:
        out = _sql_of(
            _scope().enter_predicate("amount IN (SELECT amount FROM other_tbl)")
        )
        inner = out.split("SELECT", 1)[1]
        assert "orders.amount" not in inner, (
            f"a column inside a subquery was bound to the outer root: {out}"
        )


# --------------------------------------------------------------------------- #
# Join registration is a side effect of entering the scope (P-A)
# --------------------------------------------------------------------------- #
class TestJoinRegistration:

    def test_predicate_crossing_one_hop_registers_it(self) -> None:
        scope = _scope()
        scope.enter_predicate("customers.balance > 1")
        assert ("customers",) in scope.join_paths.as_list()

    def test_predicate_crossing_two_hops_registers_every_prefix(self) -> None:
        scope = _scope()
        scope.enter_predicate("customers__regions.population > 1")
        paths = scope.join_paths.as_list()
        assert ("customers",) in paths
        assert ("customers", "regions") in paths

    def test_expression_crossing_registers_too(self) -> None:
        scope = _scope()
        scope.enter_expression("customers__regions.population")
        paths = scope.join_paths.as_list()
        # every PREFIX is required, not just the deepest hop — the FROM builder
        # needs the intermediate join to reach the last one
        assert ("customers",) in paths, paths
        assert ("customers", "regions") in paths, paths

    def test_local_predicate_registers_nothing(self) -> None:
        scope = _scope()
        scope.enter_predicate("amount > 1")
        assert scope.join_paths.as_list() == []

    def test_dual_scan_keeps_paths_that_expansion_removes(self) -> None:
        """The pre-expansion scan is load-bearing (the DEV-1494 contract).

        ``customers.flag_const`` is a DERIVED column on customers whose sql is
        the constant ``1``, so expansion rewrites the reference to ``1`` and the
        join disappears from the expanded AST entirely. The join is only
        discoverable by scanning BEFORE expansion.

        The crossing ref must be the ONLY one in the predicate — with any other
        surviving ``customers.*`` reference, an implementation that scans only
        after expansion would still find the path and this test would pass
        while proving nothing.
        """
        customers = _customers()
        customers.columns.append(
            Column(name="flag_const", sql="1", type=DataType.INT),
        )
        host = _orders()
        alloc = AliasAllocator()
        bundle = ResolvedSourceBundle(
            source_model=host, referenced_models=[host, customers, _regions()],
        )
        scope = ScopeFrame(
            scope_id=alloc.next_scope_id(host.name),
            root_model=host, root_relation=host.name,
            bundle=bundle, dialect=get_dialect("postgres"), allocator=alloc,
        )
        node = scope.enter_predicate("customers.flag_const = 1")
        # the reference really does vanish from the expanded AST ...
        assert "customers" not in _sql_of(node), _sql_of(node)
        # ... and the join is still registered
        assert ("customers",) in scope.join_paths.as_list(), (
            "the pre-expansion scan did not run: a crossing ref that inlines "
            "to a constant lost its join path"
        )


# --------------------------------------------------------------------------- #
# Failure is loud (D1) — no regex fallback, no raw passthrough, no silent
# zero-join-paths
# --------------------------------------------------------------------------- #
class TestParseFailureRaises:

    def test_unparseable_predicate_raises(self) -> None:
        from slayer.core.errors import SlayerError

        scope = _scope()
        with pytest.raises(SlayerError):
            scope.enter_predicate("this is ( not sql")

    def test_unparseable_expression_raises(self) -> None:
        from slayer.core.errors import SlayerError

        scope = _scope()
        with pytest.raises(SlayerError):
            scope.enter_expression("SELECT ((( FROM")

    def test_error_carries_the_original_fragment(self) -> None:
        from slayer.core.errors import SlayerError

        fragment = "this is ( not sql"
        scope = _scope()
        with pytest.raises(SlayerError) as excinfo:
            scope.enter_predicate(fragment)
        assert fragment in str(excinfo.value), (
            "the error must name the offending fragment"
        )

    def test_unparseable_never_silently_drops_joins(self) -> None:
        """The old ``_filter_join_paths._scan`` swallowed the parse error and
        contributed zero paths — missing joins instead of a failure."""
        from slayer.core.errors import SlayerError

        scope = _scope()
        with pytest.raises(SlayerError):
            scope.enter_predicate("customers.balance > ( not sql")
        # and it certainly must not have quietly succeeded with no joins
        assert scope.join_paths.as_list() == []


# --------------------------------------------------------------------------- #
# Predicate vs expression grammar
# --------------------------------------------------------------------------- #
class TestSurfaceGrammar:

    def test_predicate_with_statement_keyword_column_parses(self) -> None:
        """``select`` is a column name here. The predicate parser's
        ``SELECT 1 WHERE ...`` guard exists so this is not read as a
        statement."""
        out = _sql_of(_scope().enter_predicate("\"select\" = 'x'"))
        assert "select" in out.lower(), out

    def test_expression_returns_a_non_boolean_node(self) -> None:
        node = _scope().enter_expression("amount * 2")
        assert isinstance(node, exp.Expression)
        assert not isinstance(node, (exp.EQ, exp.GT, exp.LT, exp.And, exp.Or))

    def test_predicate_returns_a_boolean_node(self) -> None:
        node = _scope().enter_predicate("amount > 2")
        assert isinstance(node, exp.GT)
