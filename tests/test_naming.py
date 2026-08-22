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

import subprocess

import pytest

from slayer.sql.naming import AliasAllocator


def test_naming_imports_cold_without_dialects_cycle() -> None:
    """DEV-1817 regression: importing ``slayer.sql.naming`` FIRST (before
    ``slayer.sql.dialects``) must not fail on a naming <-> dialects.base import
    cycle. Runs in a fresh interpreter so the import order is genuinely cold —
    the in-process suite imports dialects early and would mask the cycle."""
    result = subprocess.run(
        ["poetry", "run", "python", "-c", "import slayer.sql.naming"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


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

    def test_cross_family_names_reserved_then_allocated(self) -> None:
        """Codex F3: the DEV-1692 fix reserves every deterministic CTE name
        (``_cm_`` / ``_wm_`` / user CTEs) up front, then ALLOCATES
        the transform families (``shifted_`` / ``sjoin_``) around them. A
        transform CTE whose preferred name collides with a reserved name from
        another family (or a user model/CTE literally named ``shifted_x``)
        must rename, not shadow."""
        a = AliasAllocator()
        # A user/other-family CTE already claims the transform's preferred name.
        a.reserve("shifted_x", "_cm_shifted_x")
        assert a.allocate_cte("shifted_x") == "shifted_x_2"
        # And the walk keeps advancing past additional reserved variants.
        a.reserve("shifted_x_2")
        assert a.allocate_cte("shifted_x") == "shifted_x_3"


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


# ===========================================================================
# DEV-1713 Stage 9 — full naming module.
#
# The functions exercised below do not exist yet; these tests are the Step-4
# "feature missing" red state. They pin the single-owner contract for every
# result-key / flat-name / alias-mangle / CTE-collision decision.
# ===========================================================================


class TestResultKey:
    """``result_key`` builds the DOTTED user-facing FINAL-stage key from
    STRUCTURED parts (source_relation + path hops + leaf). It is the D3
    (DEV-1495 bug 1) fix's single owner of the dotted form."""

    def test_local_leaf(self) -> None:
        from slayer.sql.naming import result_key

        assert result_key(source_relation="orders", leaf="revenue_sum") == (
            "orders.revenue_sum"
        )

    def test_star_count_leading_underscore_leaf(self) -> None:
        from slayer.sql.naming import result_key

        assert result_key(source_relation="orders", leaf="_count") == "orders._count"

    def test_single_hop_joined(self) -> None:
        from slayer.sql.naming import result_key

        assert result_key(
            source_relation="orders", path=("customers",), leaf="revenue",
        ) == "orders.customers.revenue"

    def test_multi_hop_joined(self) -> None:
        from slayer.sql.naming import result_key

        assert result_key(
            source_relation="orders",
            path=("customers", "regions"),
            leaf="name",
        ) == "orders.customers.regions.name"

    def test_dot_in_leaf_rejected(self) -> None:
        """Hop information must arrive through ``path`` — a dotted ``leaf``
        would double-encode ownership (Codex F5). Use
        ``result_key_from_alias`` for an already-canonical dotted alias."""
        from slayer.sql.naming import result_key

        with pytest.raises(ValueError):
            result_key(source_relation="orders", leaf="customers.revenue")

    def test_dot_in_source_relation_is_passed_through(self) -> None:
        # The source relation is an opaque token passed through verbatim even
        # when it contains a dot; only ``leaf`` is validated dot-free.
        from slayer.sql.naming import result_key

        assert result_key(
            source_relation="sales.orders", path=(), leaf="n",
        ) == "sales.orders.n"


class TestResultKeyFromAlias:
    """``result_key_from_alias`` is the deliberately-named second entry
    point for an ALREADY-canonical relative alias that may embed hop dots
    (cross-model measure aliases built by ``_canonical_alias_for_formula``)."""

    def test_bare_alias(self) -> None:
        from slayer.sql.naming import result_key_from_alias

        assert result_key_from_alias(
            source_relation="orders", alias="revenue_sum",
        ) == "orders.revenue_sum"

    def test_hop_embedded_alias(self) -> None:
        from slayer.sql.naming import result_key_from_alias

        assert result_key_from_alias(
            source_relation="orders", alias="customers.revenue_sum",
        ) == "orders.customers.revenue_sum"

    def test_parametric_kwarg_suffix_preserved(self) -> None:
        from slayer.sql.naming import result_key_from_alias

        assert result_key_from_alias(
            source_relation="orders",
            alias="customers.regions.population_percentile_p_0_5",
        ) == "orders.customers.regions.population_percentile_p_0_5"


class TestFlatName:
    """``flat_name`` is the single owner of the ``__``-flatten rule used by
    INNER stages of a multi-stage DAG (the StageSchema bind contract)."""

    def test_flattens_dots(self) -> None:
        from slayer.sql.naming import flat_name

        assert flat_name("customers.revenue_sum") == "customers__revenue_sum"

    def test_multi_hop(self) -> None:
        from slayer.sql.naming import flat_name

        assert flat_name("customers.regions.name") == "customers__regions__name"

    def test_no_dot_is_identity(self) -> None:
        from slayer.sql.naming import flat_name

        assert flat_name("revenue_sum") == "revenue_sum"

    def test_dot_vs_underscore_do_not_collide(self) -> None:
        """Aliases differing only in dot-vs-underscore placement flatten to
        distinct names — the dot→``__`` rule is injective on the pair, so a
        CTE name minted from either can never collide."""
        from slayer.sql.naming import flat_name

        assert flat_name("a.b_c") == "a__b_c"
        assert flat_name("a_b.c") == "a_b__c"
        assert flat_name("a.b_c") != flat_name("a_b.c")

    def test_strip_relation_prefix(self) -> None:
        from slayer.sql.naming import flat_name

        assert flat_name(
            "orders.customers.revenue", strip_relation="orders",
        ) == "customers__revenue"

    def test_strip_relation_then_flatten_local(self) -> None:
        from slayer.sql.naming import flat_name

        assert flat_name("orders.count", strip_relation="orders") == "count"

    def test_strip_relation_matches_exact_prefix_only(self) -> None:
        """``strip_relation='orders'`` strips ``orders.`` — NOT the char
        prefix of a sibling like ``orders_archive`` (which shares no dot
        boundary)."""
        from slayer.sql.naming import flat_name

        assert flat_name(
            "orders_archive.x", strip_relation="orders",
        ) == "orders_archive__x"

    def test_non_injective_collision_pair(self) -> None:
        """Codex F10: ``flat_name`` is intentionally non-injective — a dotted
        joined dim (``customers.region``) and a literal ``__`` column
        (``customers__region``) flatten to the SAME downstream name. The
        stage-schema collision guard (``stage_planner._emit_stage_schema``)
        compares ``flat_name`` outputs, so delegation must preserve this
        collapse for the guard to keep firing."""
        from slayer.sql.naming import flat_name

        assert flat_name("customers.region") == "customers__region"
        assert flat_name("customers__region") == "customers__region"


class TestManglingRelocatedToNaming:
    """The BigQuery/T-SQL dotted-alias bijection now lives in the naming
    module (D-a). Importing it from ``slayer.sql.naming`` is the relocation
    contract; ``slayer/sql/dialects/_alias_mangle.py`` is deleted."""

    def test_encode_decode_importable_from_naming(self) -> None:
        from slayer.sql.naming import decode_alias, encode_alias

        assert encode_alias("orders.customers.revenue") == (
            "orders___customers___revenue"
        )
        assert decode_alias("orders___customers___revenue") == (
            "orders.customers.revenue"
        )

    @pytest.mark.parametrize(
        "original",
        [
            "orders.id",
            "orders._count",
            "orders.products.category",
            "orders.my___metric",
            "a.b.c___d",
            "orders.customers.regions.population_sum",
        ],
    )
    def test_round_trip(self, original: str) -> None:
        from slayer.sql.naming import decode_alias, encode_alias

        assert decode_alias(encode_alias(original)) == original

    def test_old_alias_mangle_module_is_deleted(self) -> None:
        """Codex F8/relocation: the old ``dialects/_alias_mangle`` module is
        gone — importing it must fail so no consumer can drift back to it."""
        import importlib

        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("slayer.sql.dialects._alias_mangle")

    def test_dialects_import_bijection_from_naming(self) -> None:
        """BigQuery and T-SQL dialects consume the bijection from the naming
        module (the single owner), not a private dialect-package copy. DEV-1817
        moved the shared mangling into ``DottedAliasManglingMixin`` (base), so
        the bijection is consumed there rather than in each dialect module."""
        import slayer.sql.dialects.base as base
        from slayer.sql.naming import decode_alias, encode_alias

        assert base.encode_alias is encode_alias
        assert base.decode_alias is decode_alias

    def test_distinct_aliases_stay_distinct_after_encode(self) -> None:
        """Codex F8: reversibility is not enough — distinct logical aliases
        must not collapse to one wire identifier after mangling (a false
        collision would corrupt result-key decode)."""
        from slayer.sql.naming import encode_alias

        aliases = [
            "orders.my.metric",       # two hops
            "orders.my___metric",     # one hop, user ``___`` in leaf
            "orders.my_metric",       # one hop, single underscore
            "orders.rev",
            "orders.rev_2",           # allocator suffix shape
            "orders.customers.rev",
        ]
        encoded = [encode_alias(a) for a in aliases]
        assert len(set(encoded)) == len(aliases)


class TestAssertUniqueCteNames:
    """``assert_unique_cte_names`` is the DEV-1692 belt: it fails loudly on
    a duplicate CTE name WITHIN a single ``WITH`` scope, while allowing the
    same name to recur across independent nested ``WITH`` scopes (Codex F2)."""

    def test_unique_names_ok(self) -> None:
        from slayer.sql.naming import assert_unique_cte_names

        assert_unique_cte_names(
            "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) "
            "SELECT x, y FROM a, b",
        )

    def test_duplicate_in_same_with_raises(self) -> None:
        from slayer.sql.naming import assert_unique_cte_names

        with pytest.raises(ValueError):
            assert_unique_cte_names(
                "WITH a AS (SELECT 1 AS x), a AS (SELECT 2 AS y) SELECT x FROM a",
            )

    def test_nested_scope_name_reuse_ok(self) -> None:
        """A name legally recurs in a separate nested ``WITH`` scope — a
        whole-statement dup check would wrongly reject this valid SQL."""
        from slayer.sql.naming import assert_unique_cte_names

        assert_unique_cte_names(
            "WITH a AS (SELECT 1 AS x) "
            "SELECT sub.y FROM ("
            "  WITH a AS (SELECT 2 AS y) SELECT y FROM a"
            ") AS sub",
        )


class TestQuotingRelocatedToNaming:
    """The DEV-1645 mixed-case identifier-quoting policy is now owned by the
    naming module (D-b); the generator keeps thin delegators (existing
    ``TestMixedCaseHelperUnit`` pins those). Cross-dialect quote-character
    behaviour is covered by ``test_mixed_case_column_quoted_per_dialect``."""

    def test_quote_mixed_case_identifiers_importable(self) -> None:
        import sqlglot

        from slayer.sql.naming import quote_mixed_case_identifiers

        tree = sqlglot.parse_one("accounts.StateFlag = 'x'")
        out = tree.transform(quote_mixed_case_identifiers).sql(dialect="postgres")
        assert '"StateFlag"' in out
        assert '"accounts"' not in out  # lowercase qualifier untouched

    def test_maybe_quote_ident_importable(self) -> None:
        import sqlglot

        from slayer.sql.naming import maybe_quote_ident

        ident = sqlglot.to_identifier("MixedCase")
        maybe_quote_ident(ident)
        assert ident.quoted is True
        lower = sqlglot.to_identifier("lower")
        maybe_quote_ident(lower)
        assert lower.quoted is False


class TestFlatNameReproducesLegacy:
    """The legacy virtual-model flatteners (``_alias_to_short`` /
    ``_alias_to_short_local``) delegate to ``flat_name`` (byte-identical
    output). Those are closures, so this pins the reproduction property
    directly: ``flat_name`` with the first path segment as ``strip_relation``
    equals the legacy ``alias.split('.', 1)[-1].replace('.', '__')``."""

    @staticmethod
    def _legacy(alias: str) -> str:
        stripped = alias.split(".", 1)[-1] if "." in alias else alias
        return stripped.replace(".", "__")

    @pytest.mark.parametrize(
        "alias",
        [
            "orders.customers.regions.name",
            "orders.count",
            "orders._count",
            "orders.revenue_sum",
            "orders.customers.revenue_sum",
            "plain_no_dot",
        ],
    )
    def test_matches_legacy_semantics(self, alias: str) -> None:
        from slayer.sql.naming import flat_name

        strip = alias.split(".", 1)[0] if "." in alias else None
        assert flat_name(alias, strip_relation=strip) == self._legacy(alias)
