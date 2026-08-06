"""DEV-1745 (W4 / mechanism contract 5.3) — per-filter structural reachability.

Cross-model routing asks one question of every host filter: can this predicate
be evaluated inside a CTE rooted at ``target_path``? The answer is structural —
it depends on which join paths the filter's dependencies are anchored at — so
it is computed HERE, at plan time, and ``classify_host_filter`` reads it.

What this replaces: a flat model-NAME membership test (``cm in target_path``)
used for derived columns. It got two shapes wrong. A model reachable on a
SIBLING branch counted as reachable, because its name appeared in the target
path even though no prefix of the path led to it. And a host-model derived
column whose ``Column.sql`` crossed INTO the target counted as host-local,
because only the declaring model's name was consulted, never the SQL.

One rule for every key kind: a dependency is reachable iff its anchored join
path is a PREFIX of ``target_path`` (``path == target_path[:len(path)]``).
Reachability is an ALL-DEPENDENCIES predicate — any unreachable dependency
drops the filter.

Storage (D9). The summary lives per-filter on ``PlannedQuery``. NOT on
``ColumnSqlKey``: that key is interned and ``_reroot_path_ref`` re-anchors it
with ``model_copy(update={"path": ...})``, which carries any extra field
through rerooting stale. NOT on ``ValueSlot``: ``filter_referenced_slot_ids``
silently skips keys with no interned slot, and a derived column referenced only
inside a filter is exactly such a key — plus slots are copied wholesale into
nested plans, which would import the PARENT's coordinate system.

Invariant: every summary is expressed in the coordinate system of the
``PlannedQuery`` that owns it, and is recomputed per plan, never copied.
"""

from __future__ import annotations

from decimal import Decimal
from typing import List, Tuple

from sqlglot import exp

from slayer.core.errors import SlayerError
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
    SqlExprKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
)
from slayer.engine.column_expansion import collect_root_scope_joined_paths
from slayer.engine.column_filter_paths import (
    _expand_derived_refs_any_dialect,
    _parse_filter_sql_any_dialect,
)

Path = Tuple[str, ...]


class UnhandledValueKindError(SlayerError, TypeError):
    """A ValueKey kind the reachability scan does not know how to walk.

    Fails CLOSED, mirroring the total-visitor discipline the ValueKey renderer
    uses: a new key kind that silently contributed no paths would read as
    "crosses nothing", and a filter depending on it would propagate into a CTE
    that cannot evaluate it.
    """

    def __init__(self, key: object) -> None:
        self.key_type = type(key).__name__
        super().__init__(
            f"UnhandledValueKindError: reachability scan has no rule for key "
            f"kind {self.key_type!r}. Add an explicit arm — a silent empty "
            f"result would route the filter as if it crossed nothing."
        )


def _prefixes(path: Path) -> List[Path]:
    """Every non-empty prefix of ``path``.

    The FROM builder needs each intermediate join to reach the last one, and
    reachability is judged per hop, so a two-hop reference contributes both
    ``("a",)`` and ``("a", "b")``.
    """
    return [tuple(path[: i + 1]) for i in range(len(path))]


def _expanded_derived_ast(
    *, key: ColumnSqlKey, anchor_model, anchor_relation: str, bundle,
):
    """The parsed, expanded AST of a derived column's ``Column.sql``.

    Expanded with the same anchoring convention ``ScopeFrame`` uses — at the
    ``__``-path alias with ``is_root=False`` when the column lives on a joined
    model — so the refs inside come out already prefixed by the key's own path
    and an anchor-rooted scan resolves them without further adjustment.
    """
    model = (
        anchor_model if key.model == getattr(anchor_model, "name", None)
        else bundle.get_referenced_model(key.model)
    )
    if model is None:
        return None
    col = next((c for c in model.columns if c.name == key.column_name), None)
    if col is None or not col.sql:
        return None

    alias_path = "__".join(key.path) if key.path else anchor_relation
    expanded = _expand_derived_refs_any_dialect(
        sql=col.sql, model=model, alias_path=alias_path, bundle=bundle,
    )
    return _parse_filter_sql_any_dialect(expanded or col.sql)


def _derived_sql_paths(
    *, key: ColumnSqlKey, anchor_model, anchor_relation: str, bundle,
) -> List[Path]:
    """Join paths the expansion of a derived column's ``Column.sql`` crosses."""
    parsed = _expanded_derived_ast(
        key=key, anchor_model=anchor_model,
        anchor_relation=anchor_relation, bundle=bundle,
    )
    if parsed is None:
        return []
    return list(collect_root_scope_joined_paths(
        parsed=parsed,
        source_model=anchor_model,
        source_relation=anchor_relation,
        bundle=bundle,
    ))


def _derived_sql_touches_anchor(
    *, key: ColumnSqlKey, anchor_model, anchor_relation: str, bundle,
) -> bool:
    """Whether a derived column's expansion references the ANCHOR relation.

    Tested directly rather than inferred from "it crossed nothing". A derived
    column can do BOTH: ``amount * customers.rate`` crosses into ``customers``
    AND depends on the host-local ``amount``. Treating a non-empty crossed set
    as proof of non-locality would propagate that filter into a
    ``customers``-rooted CTE, where ``orders.amount`` is not bound.

    Expansion qualifies host-local refs to ``anchor_relation``, so those are
    exactly the columns carrying that table (or, defensively, none at all).
    """
    parsed = _expanded_derived_ast(
        key=key, anchor_model=anchor_model,
        anchor_relation=anchor_relation, bundle=bundle,
    )
    if parsed is None:
        # Nothing resolvable to inspect — a bare column name on the anchor.
        return True
    for col in parsed.find_all(exp.Column):
        table = col.args.get("table")
        if table is None or table.name == anchor_relation:
            return True
    return False


# Values a key tree can carry INLINE — plain data, not references, so they
# cannot cross a join. ``Decimal`` is load-bearing: ``AggregateKey.args`` /
# ``kwargs`` and ``ScalarCallKey.args`` normalise numeric literals to it, so a
# parametric aggregate like ``price:percentile(p=0.9)`` puts a Decimal in the
# tree. Omitting it made the fail-closed visitor reject a legitimate key.
_INLINE_SCALARS = (str, int, float, bool, Decimal)

# Leaf kinds: they carry references but no child keys.
_LEAF_KINDS = (LiteralKey, StarKey, SqlExprKey, ColumnKey, ColumnSqlKey)


def _child_keys(node, *, descend_aggregates: bool = True) -> Tuple:
    """The child keys of a composite node, in a STABLE order.

    One dispatch shared by both visitors, so a new key kind is handled — or
    rejected — identically by each. Fails CLOSED on an unknown kind: a silent
    empty result would read as "crosses nothing" and route a filter into a
    scope that cannot evaluate it.

    ``partition_keys`` is a frozenset, whose iteration order varies between
    runs; sorted here because the discovered paths drive JOIN emission order,
    and non-deterministic SQL is its own bug.

    ``descend_aggregates=False`` stops at an aggregate: for host-locality, an
    aggregate is routed by WHERE it is computed, not by its inputs.
    """
    if isinstance(node, _LEAF_KINDS) or isinstance(node, _INLINE_SCALARS):
        return ()
    if isinstance(node, TimeTruncKey):
        return (node.column,)
    if isinstance(node, AggregateKey):
        if not descend_aggregates:
            return ()
        return (
            node.source,
            *node.args,
            *(v for _name, v in node.kwargs),
            node.column_filter_key,
        )
    if isinstance(node, TransformKey):
        return (
            node.input,
            *sorted(node.partition_keys, key=repr),
            node.time_key,
        )
    if isinstance(node, ArithmeticKey):
        return tuple(node.operands)
    if isinstance(node, ScalarCallKey):
        return tuple(node.args)
    if isinstance(node, InKey):
        return (node.column, *node.values)
    if isinstance(node, BetweenKey):
        return (node.column, node.low, node.high)
    raise UnhandledValueKindError(node)


def compute_key_join_paths(
    *, key, anchor_model, anchor_relation: str, bundle,
) -> Tuple[Path, ...]:
    """Every join path ``key``'s dependency tree crosses, anchored at
    ``anchor_relation``.

    Recursive over the WHOLE key tree, not just the top node: a crossing
    reference buried under arithmetic or inside an aggregate's kwargs is still
    a dependency the destination scope has to satisfy. Returns an
    insertion-ordered, de-duplicated tuple; empty means the key is evaluable
    wherever the anchor is.
    """
    seen: "dict[Path, None]" = {}

    def _add(path: Path) -> None:
        if path:
            seen.setdefault(tuple(path), None)

    def _walk(node) -> None:
        if node is None:
            return
        if isinstance(node, (ColumnKey, ColumnSqlKey)):
            for p in _prefixes(node.path):
                _add(p)
        if isinstance(node, ColumnSqlKey):
            for p in _derived_sql_paths(
                key=node, anchor_model=anchor_model,
                anchor_relation=anchor_relation, bundle=bundle,
            ):
                _add(p)
        elif isinstance(node, SqlExprKey):
            for p in node.referenced_join_paths:
                for pre in _prefixes(tuple(p)):
                    _add(pre)
        for child in _child_keys(node):
            _walk(child)

    _walk(key)
    return tuple(seen)


def key_has_host_local_ref(
    *, key, anchor_model, anchor_relation: str, bundle,
) -> bool:
    """Whether ``key`` depends on anything anchored AT the host root.

    A host-local dependency cannot be evaluated inside a CTE rooted elsewhere,
    so a filter carrying one stays at the host even when its other dependencies
    are reachable. Distinguished from "crosses nothing" deliberately: a derived
    column declared on the host whose ``Column.sql`` reaches INTO the target
    has an empty anchored path but is NOT host-local — inside the target's
    scope its expansion resolves.
    """

    def _is_local(node) -> bool:
        if isinstance(node, ColumnKey):
            return not node.path
        if isinstance(node, ColumnSqlKey):
            return not node.path and _derived_sql_touches_anchor(
                key=node, anchor_model=anchor_model,
                anchor_relation=anchor_relation, bundle=bundle,
            )
        return False

    def _walk(node) -> bool:
        if node is None:
            return False
        if _is_local(node):
            return True
        return any(
            _walk(child)
            for child in _child_keys(node, descend_aggregates=False)
        )

    return _walk(key)


def path_is_reachable(*, path: Path, target_path: Path) -> bool:
    """The ONE reachability rule, for every key kind.

    ``path`` is reachable from a CTE rooted at ``target_path`` iff it is a
    prefix of it. A path DEEPER than the target is not available (the target's
    scope stops there); a SIBLING branch that happens to share a model name is
    not available either, which is precisely what the old flat membership test
    got wrong.
    """
    return tuple(path) == tuple(target_path[: len(path)])


def recompute_filter_reachability(planned_query, *, bundle) -> List:
    """Recompute every filter's summary from scratch, anchored at
    ``planned_query``'s OWN root.

    Used to verify the coordinate-system invariant: a plan's stored summary
    must equal this. If a parent had copied its summary into a nested plan, the
    stored value would still be anchored at the parent root and the two would
    differ.
    """
    from slayer.engine.planned import FilterReachability

    anchor_model = planned_query.render_source_model or bundle.source_model
    anchor_relation = planned_query.source_relation
    out: List = []
    for fp in planned_query.filters_by_phase:
        if fp.expression is None:
            continue
        out.append(FilterReachability(
            filter_id=fp.id,
            crossed_join_paths=compute_key_join_paths(
                key=fp.expression.value_key,
                anchor_model=anchor_model,
                anchor_relation=anchor_relation,
                bundle=bundle,
            ),
            has_host_local_ref=key_has_host_local_ref(
                key=fp.expression.value_key,
                anchor_model=anchor_model,
                anchor_relation=anchor_relation,
                bundle=bundle,
            ),
        ))
    return out


def filter_reachability_for(planned_query) -> List:
    """The summary ``planned_query`` CARRIES — read, never recomputed.

    The accessor exists so consumers cannot accidentally recompute against a
    different anchor and get a summary in the wrong coordinate system.
    """
    return list(planned_query.filter_reachability)
