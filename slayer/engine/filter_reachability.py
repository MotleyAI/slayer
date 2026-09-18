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


def _resolve_key_column(*, key: ColumnSqlKey, anchor_model, bundle):
    """The (model, column) a ``ColumnSqlKey`` names, or ``(None, None)``."""
    model = (
        anchor_model if key.model == getattr(anchor_model, "name", None)
        else bundle.get_referenced_model(key.model)
    )
    if model is None:
        return None, None
    return model, next(
        (c for c in model.columns if c.name == key.column_name), None,
    )


def _expanded_fragment_ast_uncached(
    *, fragment: str, model, key_path: Path, anchor_relation: str, bundle,
):
    alias_path = "__".join(key_path) if key_path else anchor_relation
    expanded = _expand_derived_refs_any_dialect(
        sql=fragment, model=model, alias_path=alias_path, bundle=bundle,
    )
    return _parse_filter_sql_any_dialect(expanded or fragment)


def _expanded_fragment_ast(
    *, fragment: str, model, column_name: str, key_path: Path,
    anchor_relation: str, bundle, cache: "Optional[dict]" = None,
):
    """A definition fragment (value or filter) expanded + parsed at the column's
    own anchoring convention, so its refs come out already path-prefixed.

    Memoised through the caller-supplied plan-scoped ``cache`` — the fragment
    string is part of the key, so a column's value and filter never collide. The
    cache is passed IN, never module-global: a global keyed by ``id(bundle)`` is
    unsound (CPython reuses collected ids), so it must not outlive its bundle.
    """
    if cache is None:
        return _expanded_fragment_ast_uncached(
            fragment=fragment, model=model, key_path=key_path,
            anchor_relation=anchor_relation, bundle=bundle,
        )
    cache_key = (model.name, column_name, key_path, anchor_relation, fragment)
    if cache_key not in cache:
        cache[cache_key] = _expanded_fragment_ast_uncached(
            fragment=fragment, model=model, key_path=key_path,
            anchor_relation=anchor_relation, bundle=bundle,
        )
    return cache[cache_key]


def _fragment_touches_anchor(parsed, *, anchor_relation: str) -> bool:
    """Whether a parsed fragment carries a column anchored at ``anchor_relation``
    (or unqualified). ``None`` (unanalysable) counts as touching — fail safe."""
    if parsed is None:
        return True
    for col in parsed.find_all(exp.Column):
        table = col.args.get("table")
        if table is None or table.name == anchor_relation:
            return True
    return False


def _derived_def_touches_anchor(
    *, key: ColumnSqlKey, anchor_model, anchor_relation: str, bundle,
    cache: "Optional[dict]" = None,
) -> bool:
    """Whether a derived column's definition — its ``Column.sql`` VALUE or its
    ``Column.filter`` (DEV-1832) — references the ANCHOR relation.

    Both fragments are definition inputs (mirrors ``reference_closure``'s
    ``_definition_fragments``): a column whose value crosses INTO a joined model
    but whose filter reads a host-local column is STILL host-local — its mask
    cannot move off the host, where that column is bound. Tested per fragment,
    not inferred from the crossed set: ``amount * customers.rate`` crosses into
    ``customers`` yet depends on host-local ``amount``.

    A column with no explicit ``sql`` masks its OWN physical column, so the value
    fragment falls back to ``col.name`` — an implicit reference at the column's
    own path (host-local for a ``path=()`` column), never dropped.
    """
    model, col = _resolve_key_column(
        key=key, anchor_model=anchor_model, bundle=bundle,
    )
    if model is None or col is None:
        return True  # nothing resolvable — a bare column name on the anchor
    fragments = [frag for frag in (col.sql or col.name, col.filter) if frag]
    if not fragments:
        return True
    return any(
        _fragment_touches_anchor(
            _expanded_fragment_ast(
                fragment=frag, model=model, column_name=key.column_name,
                key_path=tuple(key.path), anchor_relation=anchor_relation,
                bundle=bundle, cache=cache,
            ),
            anchor_relation=anchor_relation,
        )
        for frag in fragments
    )


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
            return not node.path and _derived_def_touches_anchor(
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
