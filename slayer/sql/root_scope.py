"""The root scope of a native SQL fragment, and sibling references in it written as paths."""

from __future__ import annotations

from collections.abc import Callable, Collection

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression
from sqlglot.optimizer.scope import ScopeType, traverse_scope

__all__ = ["qualified_siblings", "respelled_columns", "respelled_qualifier", "root_scope_field_ids"]


def root_scope_field_ids(*, parsed: Expression) -> set[int]:
    """Return the ``id()`` set of ``exp.Column`` nodes that lexically belong
    to the root scope of ``parsed``.

    A row definition is contractually a scalar expression, not a SELECT. To re-use
    sqlglot's scope analysis we wrap ``parsed`` in a synthetic
    ``SELECT <parsed> AS _`` so it has a real root scope. The wrapper is
    used only for scope traversal; the original ``parsed`` AST is unchanged.

    A Column is "root-scope" iff its innermost scope-defining ancestor is
    the wrapper itself. Anything nested under a ``Subquery``, CTE, set
    operation (``Union`` / ``Except`` / ``Intersect``), ``Values``, or
    other scope-producing construct returns a non-root ScopeType and is
    skipped from derived-field inlining.

    ``Window`` / ``OVER`` is NOT a new scope: columns inside
    ``PARTITION BY`` / ``ORDER BY`` remain root-scope.
    """
    if not isinstance(parsed, Expression):
        return set()
    wrapper = exp.Select(expressions=[exp.Alias(this=parsed.copy(), alias="_")])
    scope_node_ids: dict[int, ScopeType] = {}
    for scope in traverse_scope(wrapper):
        scope_node_ids[id(scope.expression)] = scope.scope_type
    bound = _comprehension_bound_ids(parsed)
    if not scope_node_ids:
        # No SELECTs in the fragment at all — every column is root-scope.
        return {id(c) for c in parsed.find_all(exp.Column)} - bound
    # Re-walk the WRAPPER (which holds copies) — but we need ids from the
    # ORIGINAL parsed tree. Pair them up positionally: find_all yields
    # nodes in document order on both wrapper.this[0] and parsed.
    wrapper_cols = list(wrapper.find_all(exp.Column))
    parsed_cols = list(parsed.find_all(exp.Column))
    if len(wrapper_cols) != len(parsed_cols):
        # Fail closed: if the positional pairing between the wrapper copy
        # and the original tree ever drifts, treat NO column as root-scope.
        # Since the root-scope gate now runs BEFORE qualification, an empty
        # set leaves every bare column unqualified (not just un-inlined) —
        # conservative: the compile-time guard still catches cycles, and no
        # column is spliced across scopes. This branch is unreachable today;
        # the wrapper just wraps a deep copy and ``find_all`` walks in
        # document order.
        return set()
    root_ids: set[int] = set()
    for w_col, p_col in zip(wrapper_cols, parsed_cols):
        node: exp.Expr | None = w_col.parent
        scope_type: ScopeType | None = None
        while node is not None:
            if id(node) in scope_node_ids:
                scope_type = scope_node_ids[id(node)]
                break
            node = node.parent
        if scope_type == ScopeType.ROOT:
            root_ids.add(id(p_col))
    return root_ids - bound


def _comprehension_bound_ids(parsed: exp.Expr) -> set[int]:
    """Ids of the names a list comprehension (``[e FOR e IN xs IF p]``) binds, wherever they appear in it."""
    bound: set[int] = set()
    for node in parsed.find_all(exp.Comprehension):
        var = node.expression
        if not isinstance(var, exp.Column):
            continue
        for part in (node.this, var, node.args.get("condition")):
            if isinstance(part, exp.Expr):
                bound |= {id(c) for c in part.find_all(exp.Column) if not c.table and c.name == var.name}
    return bound


def _root_columns(text: str, *, dialect: str | None) -> list[exp.Column] | None:
    try:
        parsed = sqlglot.parse_one(text, dialect=dialect)
    except (SqlglotError, ValueError):
        return None
    root = root_scope_field_ids(parsed=parsed)  # pyright: ignore[reportArgumentType]  # parse_one returns an Expr
    return [c for c in parsed.find_all(exp.Column) if id(c) in root]


def _span(ident: object) -> tuple[int, int] | None:
    if not isinstance(ident, exp.Identifier):
        return None
    start, end = ident.meta.get("start"), ident.meta.get("end")
    return (start, end + 1) if isinstance(start, int) and isinstance(end, int) else None


def _spliced(text: str, spans: list[tuple[int, int]], fn: Callable[[str], str]) -> str:
    for start, end in sorted(spans, reverse=True):
        text = f"{text[:start]}{fn(text[start:end])}{text[end:]}"
    return text


def respelled_columns(text: str, *, respell: Callable[[str], str | None], dialect: str | None) -> str:
    """``text`` verbatim except each root-scope bare reference ``respell`` maps (by name) is written as it returns;
    text the dialect cannot parse is returned unchanged."""
    columns = _root_columns(text, dialect=dialect)
    if columns is None:
        return text
    found = [(s, new) for c in columns if not c.table and (new := respell(c.name)) is not None
             and (s := _span(c.this)) is not None]
    for (start, end), new in sorted(found, reverse=True):
        text = f"{text[:start]}{new}{text[end:]}"
    return text


def qualified_siblings(text: str, *, dataset: str, siblings: Collection[str], dialect: str | None) -> str:
    """``text`` verbatim except each root-scope bare reference to one of ``siblings`` is written ``<dataset>.<name>``;
    text the dialect cannot parse is returned unchanged."""
    columns = _root_columns(text, dialect=dialect) if siblings else None
    if columns is None:
        return text
    spans = [s for c in columns if not c.table and c.name in siblings and (s := _span(c.this)) is not None]
    return _spliced(text, spans, lambda written: f"{dataset}.{written}")


def respelled_qualifier(text: str, *, old: str, new: str, dialect: str | None) -> str:
    """``text`` verbatim except each root-scope two-part reference qualified by ``old`` (any case) is qualified by
    ``new``; text the dialect cannot parse is returned unchanged."""
    columns = _root_columns(text, dialect=dialect)
    if columns is None:
        return text
    spans = [s for c in columns if not c.args.get("db") and c.table.casefold() == old.casefold()
             and (s := _span(c.args.get("table"))) is not None]
    return _spliced(text, spans, lambda _: new)
