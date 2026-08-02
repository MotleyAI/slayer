"""DEV-1706 Stage 2 — minimal alias allocator (``slayer/sql/naming.py``).

The allocator is per-generation and collision-safe. It mints ``_val_<n>``
materialisation aliases (Law 2) and CTE names, seeded from every name already
in scope so it can never collide with a user column, a ``__``-path join alias,
or a reserved public alias. It also hands out generation-local ``ScopeFrame``
ids (D-F). Stage 9 grows this module into the full naming module; Stage 2 only
needs the collision-safe primitive (subsumes DEV-1692's collision check).

These tests fail at import until ``slayer/sql/naming.py`` exists — that is the
intended "feature missing" red state (Step 4, tests-first).
"""

from __future__ import annotations

from slayer.sql.naming import AliasAllocator


class TestAllocateCollisionWalk:
    def test_first_use_returns_preferred(self) -> None:
        a = AliasAllocator()
        assert a.allocate("region") == "region"

    def test_repeat_walks_numeric_suffix(self) -> None:
        a = AliasAllocator()
        assert a.allocate("x") == "x"
        assert a.allocate("x") == "x_2"
        assert a.allocate("x") == "x_3"

    def test_reserved_name_forces_rename(self) -> None:
        a = AliasAllocator()
        a.reserve("y")
        # ``y`` is taken by a real column/alias — the allocator must skip it.
        assert a.allocate("y") == "y_2"

    def test_reserve_multiple(self) -> None:
        a = AliasAllocator()
        a.reserve("a", "b", "a_2")
        assert a.allocate("a") == "a_3"


class TestAllocateVal:
    def test_sequence(self) -> None:
        a = AliasAllocator()
        assert a.allocate_val() == "_val_0"
        assert a.allocate_val() == "_val_1"
        assert a.allocate_val() == "_val_2"

    def test_skips_reserved(self) -> None:
        a = AliasAllocator()
        a.reserve("_val_0")
        # A user column literally named ``_val_0`` must not be shadowed.
        assert a.allocate_val() == "_val_1"

    def test_never_collides_with_user_names(self) -> None:
        a = AliasAllocator()
        a.reserve("_val_1", "_val_3")
        handed = {a.allocate_val() for _ in range(4)}
        assert handed.isdisjoint({"_val_1", "_val_3"})
        assert len(handed) == 4  # all distinct

    def test_val_and_allocate_share_namespace(self) -> None:
        # An ``allocate`` result and an ``allocate_val`` result never collide.
        a = AliasAllocator()
        assert a.allocate("_val_0") == "_val_0"
        assert a.allocate_val() == "_val_1"


class TestAllocateCte:
    def test_dedupes(self) -> None:
        a = AliasAllocator()
        assert a.allocate_cte("shifted_amount") == "shifted_amount"
        assert a.allocate_cte("shifted_amount") == "shifted_amount_2"

    def test_cte_avoids_reserved(self) -> None:
        a = AliasAllocator()
        a.reserve("_cm_customers_balance_sum")
        assert (
            a.allocate_cte("_cm_customers_balance_sum")
            == "_cm_customers_balance_sum_2"
        )


class TestNextScopeId:
    def test_monotonic_and_root_prefixed(self) -> None:
        a = AliasAllocator()
        assert a.next_scope_id("orders") == "orders#0"
        assert a.next_scope_id("customers") == "customers#1"
        assert a.next_scope_id("orders") == "orders#2"

    def test_ids_are_unique_across_roots(self) -> None:
        a = AliasAllocator()
        ids = {a.next_scope_id("orders") for _ in range(5)}
        assert len(ids) == 5
