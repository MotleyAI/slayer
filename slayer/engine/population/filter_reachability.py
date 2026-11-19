"""Per-filter structural summary: whether a host filter depends on anything anchored at the host root.

The summary lives per-filter on ``PlannedQuery`` — not on ``FieldSqlKey`` (interned and
re-anchored by ``model_copy``, which would carry the field through rerooting stale) and not
on ``ValueSlot`` (a filter-only derived key has no interned slot, and slots are copied into
nested plans). Every summary is expressed in the coordinate system of the ``PlannedQuery``
that owns it, and is recomputed per plan, never copied.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from sqlglot import exp

from slayer.core.keys import FieldKey, FieldSqlKey
from slayer.core.models import DatasetField, SlayerDataset
from slayer.engine.reference_closure import (
    UnhandledValueKindError,  # re-exported
    _child_keys,
    _expand_derived_refs_any_dialect,
    parse_fragment_any_dialect,
)
from slayer.ir.planned import FilterReachability, PlannedQuery
from slayer.ir.source_bundle import ResolvedSourceBundle

__all__ = [
    "UnhandledValueKindError",
    "filter_reachability_for",
    "key_has_host_local_ref",
    "recompute_filter_reachability",
]

Path = Tuple[str, ...]


def _resolve_key_field(
    *, key: FieldSqlKey, anchor_dataset: SlayerDataset | None, bundle: ResolvedSourceBundle,
) -> tuple[SlayerDataset | None, DatasetField | None]:
    """The (dataset, field) a ``FieldSqlKey`` names, or ``(None, None)``."""
    dataset = (
        anchor_dataset if key.dataset == getattr(anchor_dataset, "name", None)
        else bundle.get_referenced_dataset(key.dataset)
    )
    if dataset is None:
        return None, None
    return dataset, next(
        (c for c in dataset.fields if c.name == key.field_name), None,
    )


def _expanded_fragment_ast_uncached(
    *, fragment: str, dataset, key_path: Path, anchor_relation: str, bundle,
):
    alias_path = "__".join(key_path) if key_path else anchor_relation
    expanded = _expand_derived_refs_any_dialect(
        sql=fragment, dataset=dataset, alias_path=alias_path, bundle=bundle,
    )
    return parse_fragment_any_dialect(expanded or fragment)


def _expanded_fragment_ast(
    *, fragment: str, dataset, field_name: str, key_path: Path,
    anchor_relation: str, bundle, cache: "Optional[dict]" = None,
):
    """A definition fragment (value or filter) expanded + parsed at the field's
    own anchoring convention, so its refs come out already path-prefixed.

    Memoised through the caller-supplied plan-scoped ``cache`` — the fragment
    string is part of the key, so a field's value and filter never collide. The
    cache is passed IN, never module-global: a global keyed by ``id(bundle)`` is
    unsound (CPython reuses collected ids), so it must not outlive its bundle.
    """
    if cache is None:
        return _expanded_fragment_ast_uncached(
            fragment=fragment, dataset=dataset, key_path=key_path,
            anchor_relation=anchor_relation, bundle=bundle,
        )
    cache_key = (dataset.name, field_name, key_path, anchor_relation, fragment)
    if cache_key not in cache:
        cache[cache_key] = _expanded_fragment_ast_uncached(
            fragment=fragment, dataset=dataset, key_path=key_path,
            anchor_relation=anchor_relation, bundle=bundle,
        )
    return cache[cache_key]


def _fragment_touches_anchor(parsed, *, anchor_relation: str) -> bool:
    """Whether a parsed fragment carries a field anchored at ``anchor_relation``
    (or unqualified). ``None`` (unanalysable) counts as touching — fail safe."""
    if parsed is None:
        return True
    for col in parsed.find_all(exp.Column):
        table = col.args.get("table")
        if table is None or table.name == anchor_relation:
            return True
    return False


def _derived_def_touches_anchor(
    *, key: FieldSqlKey, anchor_dataset, anchor_relation: str, bundle,
    cache: "Optional[dict]" = None,
) -> bool:
    """Whether a derived field's definition — its ``DatasetField.sql`` VALUE or its
    ``DatasetField.filter`` — references the ANCHOR relation.

    Both fragments are definition inputs (mirrors ``reference_closure``'s
    ``_definition_fragments``): a field whose value crosses INTO a joined dataset
    but whose filter reads a host-local field is STILL host-local — its mask
    cannot move off the host, where that field is bound. Tested per fragment,
    not inferred from the crossed set: ``amount * customers.rate`` crosses into
    ``customers`` yet depends on host-local ``amount``.

    A field with no explicit ``sql`` masks its OWN physical column, so the value
    fragment falls back to ``col.name`` — an implicit reference at the field's
    own path (host-local for a ``path=()`` field), never dropped.
    """
    dataset, col = _resolve_key_field(
        key=key, anchor_dataset=anchor_dataset, bundle=bundle,
    )
    if dataset is None or col is None:
        return True  # nothing resolvable — a bare field name on the anchor
    fragments = [frag for frag in (col.sql or col.name, col.filter) if frag]
    if not fragments:
        return True
    return any(
        _fragment_touches_anchor(
            _expanded_fragment_ast(
                fragment=frag, dataset=dataset, field_name=key.field_name,
                key_path=tuple(key.path), anchor_relation=anchor_relation,
                bundle=bundle, cache=cache,
            ),
            anchor_relation=anchor_relation,
        )
        for frag in fragments
    )


def key_has_host_local_ref(
    *, key, anchor_dataset, anchor_relation: str, bundle,
    cache: "Optional[dict]" = None,
) -> bool:
    """Whether ``key`` depends on anything anchored AT the host root.

    A host-local dependency cannot be evaluated inside a CTE rooted elsewhere,
    so a filter carrying one stays at the host even when its other dependencies
    are reachable. Distinguished from "crosses nothing" deliberately: a derived
    field declared on the host whose ``DatasetField.sql`` reaches INTO the target
    has an empty anchored path but is NOT host-local — inside the target's
    scope its expansion resolves.
    """

    def _is_local(node) -> bool:
        if isinstance(node, FieldKey):
            return not node.path
        if isinstance(node, FieldSqlKey):
            return not node.path and _derived_def_touches_anchor(
                key=node, anchor_dataset=anchor_dataset,
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


def recompute_filter_reachability(planned_query: PlannedQuery, *, bundle: ResolvedSourceBundle) -> List:
    """Recompute every mask's summary from scratch, anchored at
    ``planned_query``'s OWN root.

    Used to verify the coordinate-system invariant: a plan's stored summary
    must equal this. If a parent had copied its summary into a nested plan, the
    stored value would still be anchored at the parent root and the two would
    differ.
    """
    anchor_dataset = planned_query.render_source_dataset or bundle.population
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
            has_host_local_ref=key_has_host_local_ref(
                key=key,
                anchor_dataset=anchor_dataset,
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
