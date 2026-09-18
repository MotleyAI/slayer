"""DEV-1832 task 1.2 — the one source-anchor accessor and its AST guard (D1).

``source_anchor_path`` / ``source_leaf_paths`` (``slayer.core.keys``) answer
"where does this aggregation source live" for every source shape. The AST guard
fails on any remaining direct ``<expr>.source.path`` read in ``slayer/engine`` /
``slayer/sql`` outside the accessor — enumeration is the sweep's first task, so
the guard is RED on the current tree and green once every site routes through
the accessor.
"""

from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

import slayer.core.keys as keys
import slayer.engine
import slayer.sql
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    ScalarCallKey,
    StarKey,
)


def _col(path=(), leaf="amount") -> ColumnKey:
    return ColumnKey(path=tuple(path), leaf=leaf)


class TestSourceAnchorPath:
    """``source_anchor_path`` = the longest common prefix of the source's
    row-level leaves' paths; attached constituents are opaque."""

    def test_bare_column_yields_its_own_path(self):
        assert keys.source_anchor_path(_col(leaf="amount")) == ()
        assert keys.source_anchor_path(_col(("customers",), "discount")) == ("customers",)

    def test_star_yields_its_own_path(self):
        assert keys.source_anchor_path(StarKey(path=("customers",))) == ("customers",)

    def test_literal_only_yields_empty(self):
        assert keys.source_anchor_path(LiteralKey(value=Decimal(1))) == ()

    def test_host_mixed_expression_anchors_at_root(self):
        # amount (host) - customers.discount → common prefix is ().
        src = ArithmeticKey(op="-", operands=(
            _col(leaf="amount"), _col(("customers",), "discount")))
        assert keys.source_anchor_path(src) == ()

    def test_target_only_expression_anchors_at_target(self):
        # customers.spend - customers.regions.pop → common prefix ("customers",).
        src = ArithmeticKey(op="-", operands=(
            _col(("customers",), "spend"),
            _col(("customers", "regions"), "pop")))
        assert keys.source_anchor_path(src) == ("customers",)

    def test_two_branch_expression_anchors_at_common_ancestor(self):
        # customers.spend - stores.rent → branches diverge → common prefix ().
        src = ArithmeticKey(op="-", operands=(
            _col(("customers",), "spend"), _col(("stores",), "rent")))
        assert keys.source_anchor_path(src) == ()

    def test_scalar_call_leaves_are_walked(self):
        src = ScalarCallKey(name="upper", args=(_col(("customers",), "email"),))
        assert keys.source_anchor_path(src) == ("customers",)

    def test_attached_constituent_is_opaque(self):
        # quantity (row leaf) * sum(unit_price) (attached) → anchor is the row
        # leaf's path; the aggregate constituent contributes no source leaf.
        attached = AggregateKey(source=_col(leaf="unit_price"), agg="sum")
        src = ArithmeticKey(op="*", operands=(_col(leaf="quantity"), attached))
        assert keys.source_anchor_path(src) == ()
        assert set(keys.source_leaf_paths(src)) == {()}


class TestSourceLeafPaths:
    def test_expression_leaf_paths(self):
        src = ArithmeticKey(op="-", operands=(
            _col(("customers",), "spend"),
            _col(("customers", "regions"), "pop")))
        assert set(keys.source_leaf_paths(src)) == {("customers",), ("customers", "regions")}

    def test_parallel_named_edges_yield_distinct_paths(self):
        # Two named joins to the same target model carry distinct path segments
        # (the join name), so their leaves are distinct paths, anchored at ().
        src = ArithmeticKey(op="-", operands=(
            _col(("opener",), "score"), _col(("closer",), "score")))
        assert set(keys.source_leaf_paths(src)) == {("opener",), ("closer",)}
        assert keys.source_anchor_path(src) == ()

    def test_literal_only_has_no_leaves(self):
        assert list(keys.source_leaf_paths(LiteralKey(value=Decimal(1)))) == []


# --------------------------------------------------------------------------- #
# AST guard — no direct ``.source.path`` read survives in engine / sql.
# --------------------------------------------------------------------------- #
def _is_source_path_read(node: ast.AST) -> bool:
    """``<expr>.source.path`` attribute chain."""
    return (isinstance(node, ast.Attribute) and node.attr == "path"
            and isinstance(node.value, ast.Attribute) and node.value.attr == "source")


def _is_getattr_ref(func: ast.AST) -> bool:
    """The builtin ``getattr`` — bare or qualified ``builtins.getattr`` — but not
    an arbitrary ``helper.getattr`` (that isn't necessarily the builtin)."""
    if isinstance(func, ast.Name):
        return func.id == "getattr"
    return (isinstance(func, ast.Attribute) and func.attr == "getattr"
            and isinstance(func.value, ast.Name) and func.value.id == "builtins")


def _is_getattr_source_path(node: ast.AST) -> bool:
    """``getattr(<expr>.source, "path", ...)`` or ``builtins.getattr(...)``."""
    if not (isinstance(node, ast.Call) and _is_getattr_ref(node.func)
            and len(node.args) >= 2):
        return False
    target, attr = node.args[0], node.args[1]
    return (isinstance(target, ast.Attribute) and target.attr == "source"
            and isinstance(attr, ast.Constant) and attr.value == "path")


def _is_key_host_path_source(node: ast.AST) -> bool:
    """``key_host_path(<expr>.source)`` — a disguised source-path read: an
    expression source has no ``.path``, so it silently answers the root."""
    if not isinstance(node, ast.Call):
        return False
    is_key_host_path = (
        isinstance(node.func, ast.Name) and node.func.id == "key_host_path"
    ) or (
        isinstance(node.func, ast.Attribute) and node.func.attr == "key_host_path"
    )
    return (is_key_host_path and len(node.args) >= 1
            and isinstance(node.args[0], ast.Attribute)
            and node.args[0].attr == "source")


def _source_path_reads() -> list[str]:
    sites: list[str] = []
    scanned = 0
    for package in (slayer.engine, slayer.sql):
        file = package.__file__
        assert file is not None
        for path in sorted(Path(file).parent.rglob("*.py")):
            scanned += 1
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if (_is_source_path_read(node) or _is_getattr_source_path(node)
                        or _is_key_host_path_source(node)):
                    sites.append(f"{path}:{getattr(node, 'lineno', 0)}")
    assert scanned > 0, "no engine/sql modules scanned — guard is dead"
    return sites


class TestNoDirectSourcePathRead:
    def test_no_source_path_read_outside_the_accessor(self):
        sites = _source_path_reads()
        assert sites == [], (
            "Direct `<expr>.source.path` reads (incl. `key_host_path(<expr>.source)`) "
            "must route through `source_anchor_path` / `source_leaf_paths`:\n"
            + "\n".join(sites))

    def _call(self, src: str) -> ast.AST:
        return ast.parse(src, mode="eval").body

    def test_guard_catches_bare_and_qualified_getattr(self):
        assert _is_getattr_source_path(self._call('getattr(x.source, "path", ())'))
        assert _is_getattr_source_path(self._call('builtins.getattr(x.source, "path", ())'))

    def test_guard_ignores_arbitrary_dot_getattr(self):
        assert not _is_getattr_source_path(self._call('helper.getattr(x.source, "path", ())'))
