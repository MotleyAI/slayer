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

from typing import List, Optional, Tuple

from sqlglot import exp

from slayer.core.keys import ColumnKey, ColumnSqlKey
from slayer.engine.reference_closure import (
    UnhandledValueKindError,  # re-exported
    _child_keys,
    _expand_derived_refs_any_dialect,
    _parse_filter_sql_any_dialect,
    key_closure,
)
from slayer.ir.planned import FilterReachability

__all__ = [
    "UnhandledValueKindError",
    "compute_key_join_paths",
    "filter_reachability_for",
    "key_has_host_local_ref",
    "path_is_reachable",
    "recompute_filter_reachability",
]

Path = Tuple[str, ...]


def _expanded_derived_ast(
    *, key: ColumnSqlKey, anchor_model, anchor_relation: str, bundle,
    cache: "Optional[dict]" = None,
):
    """The parsed, expanded AST of a derived column's ``Column.sql``.

    Expanded with the same anchoring convention ``ScopeFrame`` uses — at the
    ``__``-path alias with ``is_root=False`` when the column lives on a joined
    model — so the refs inside come out already prefixed by the key's own path
    and an anchor-rooted scan resolves them without further adjustment.

    Memoised through the caller-supplied ``cache``. Both visitors ask for the
    same key's expansion — ``_derived_sql_paths`` for the crossed set and
    ``_derived_sql_touches_anchor`` for host-locality — and both run for every
    filter on every plan, while ``_expand_derived_refs_any_dialect`` itself
    re-parses per hop of a derived-of-derived chain.

    The cache is passed IN rather than held module-level on purpose. A global
    keyed by ``id(bundle)`` would be unsound: CPython reuses ids once an object
    is collected, so a fresh bundle could be handed a dead one's entry. A dict
    owned by one plan-level call cannot outlive the bundle it was built for.
    """
    if cache is None:
        return _expanded_derived_ast_uncached(
            key=key, anchor_model=anchor_model,
            anchor_relation=anchor_relation, bundle=bundle,
        )
    cache_key = (key.model, key.column_name, key.path, anchor_relation)
    if cache_key not in cache:
        cache[cache_key] = _expanded_derived_ast_uncached(
            key=key, anchor_model=anchor_model,
            anchor_relation=anchor_relation, bundle=bundle,
        )
    return cache[cache_key]


def _expanded_derived_ast_uncached(
    *, key: ColumnSqlKey, anchor_model, anchor_relation: str, bundle,
):
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


def _derived_sql_touches_anchor(
    *, key: ColumnSqlKey, anchor_model, anchor_relation: str, bundle,
    cache: "Optional[dict]" = None,
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
        anchor_relation=anchor_relation, bundle=bundle, cache=cache,
    )
    if parsed is None:
        # Nothing resolvable to inspect — a bare column name on the anchor.
        return True
    for col in parsed.find_all(exp.Column):
        table = col.args.get("table")
        if table is None or table.name == anchor_relation:
            return True
    return False


def compute_key_join_paths(
    *, key, anchor_model, anchor_relation: str, bundle,
    cache: "Optional[dict]" = None,
) -> Tuple[Path, ...]:
    """Every join path ``key``'s dependency tree crosses, anchored at
    ``anchor_relation`` — delegates to the one dependency closure (DEV-1900).

    Best-effort for filter routing: an unanalysable derived dependency (closure
    ``None``) coerces to ``()`` here; the population guard and input safety take
    the tri-state closure directly and fail closed on ``None``.
    """
    return key_closure(
        key=key, anchor_model=anchor_model, anchor_relation=anchor_relation,
        bundle=bundle, cache=cache,
    ) or ()


def key_has_host_local_ref(
    *, key, anchor_model, anchor_relation: str, bundle,
    cache: "Optional[dict]" = None,
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
                anchor_relation=anchor_relation, bundle=bundle, cache=cache,
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


def path_is_reachable(
    *,
    path: Path,
    target_path: Path,
    reachable_paths: "Optional[frozenset]" = None,
) -> bool:
    """The ONE reachability rule, for every key kind.

    A FORWARD-path CTE selects from the bare target and carries only the
    host→target hops, so ``path`` is reachable iff it is a PREFIX of
    ``target_path``. A path deeper than the target is not available (the
    target's scope stops there); a sibling branch that happens to share a model
    name is not available either, which is precisely what the old flat
    membership test got wrong.

    A RE-ROOTED CTE (DEV-1747 D6) is planned against the TARGET as its own
    root, so the target's WHOLE join graph is in scope and the prefix test no
    longer describes it: a host-side sibling branch can be reachable from the
    target by a different route entirely. That question is about the model
    graph, not about string prefixes, so the caller — which holds the bundle —
    walks it and passes the answer in as ``reachable_paths``. Membership then
    replaces the prefix test outright.
    """
    if reachable_paths is not None:
        return tuple(path) in reachable_paths
    return tuple(path) == tuple(target_path[: len(path)])


def recompute_filter_reachability(planned_query, *, bundle) -> List:
    """Recompute every mask's summary from scratch, anchored at
    ``planned_query``'s OWN root.

    Used to verify the coordinate-system invariant: a plan's stored summary
    must equal this. If a parent had copied its summary into a nested plan, the
    stored value would still be anchored at the parent root and the two would
    differ.
    """
    anchor_model = planned_query.render_source_model or bundle.source_model
    anchor_relation = planned_query.source_relation
    slots_by_id = {
        s.id: s
        for s in (
            *planned_query.row_slots,
            *planned_query.aggregate_slots,
            *planned_query.combined_expression_slots,
        )
    }
    cache: dict = {}
    out: List = []
    for mask in planned_query.masks:
        key = slots_by_id[mask.slot_id].key
        out.append(FilterReachability(
            filter_id=mask.slot_id,
            crossed_join_paths=compute_key_join_paths(
                key=key,
                anchor_model=anchor_model,
                anchor_relation=anchor_relation,
                bundle=bundle,
                cache=cache,
            ),
            has_host_local_ref=key_has_host_local_ref(
                key=key,
                anchor_model=anchor_model,
                anchor_relation=anchor_relation,
                bundle=bundle,
                cache=cache,
            ),
        ))
    return out


def filter_reachability_for(planned_query) -> List:
    """The summary ``planned_query`` CARRIES — read, never recomputed.

    The accessor exists so consumers cannot accidentally recompute against a
    different anchor and get a summary in the wrong coordinate system.
    """
    return list(planned_query.filter_reachability)
