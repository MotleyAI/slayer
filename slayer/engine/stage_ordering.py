"""Kahn topo-sort and name localization for stored / runtime stage lists.

The last stage stays root / sink. Cycles, self-references, duplicate names, and
a root referenced by another stage raise ``ValueError`` naming the offending
stage; forward references are reordered, not rejected. Sibling refs are walked
through ``source_model``, inline models' and extensions' ``joins[].target_model``,
and extensions' ``source_name``.
"""
from __future__ import annotations

from typing import AbstractSet, Any, Dict, List, Optional, Set, Tuple

from slayer.core.models import ModelJoin
from slayer.core.models import SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery, SourceSpec, extension_model_names
from slayer.core.scope import StageDisplay

#: Minted identity prefixes; model names never carry them (``__slayer_`` is reserved).
USER_STAGE_PREFIX = "__slayer_stage_"
SPLICED_STAGE_PREFIX = "__slayer_qb__"


def stage_sibling_reads(*, query: SlayerQuery, siblings: AbstractSet[str]) -> Set[str]:
    """Sibling names in ``siblings`` that ``query`` references; purely structural, never raises."""
    spec = query.source_model
    names = [spec] if isinstance(spec, str) else [j.target_model for j in spec.joins] if spec is not None else []
    return {n for n in (*names, *extension_model_names(query)) if n in siblings}


def stage_identity(*, name: str, model: Optional[str] = None) -> str:
    """The minted identity of stage ``name`` (of stored query-backed ``model``, if spliced)."""
    if model is None:
        return f"{USER_STAGE_PREFIX}{name}"
    return f"{SPLICED_STAGE_PREFIX}{model}__{name}"


def _respell_joins(joins: List[ModelJoin], rename: Dict[str, str]) -> List[ModelJoin]:
    return [
        j.model_copy(update={"target_model": rename[j.target_model]})
        if j.target_model in rename else j
        for j in joins
    ]


def _respell_spec(spec: SourceSpec | None, rename: Dict[str, str]) -> SourceSpec | None:
    """``spec`` with every query-written sibling reference respelled through ``rename``."""
    if isinstance(spec, str):
        return rename.get(spec, spec)
    if isinstance(spec, SlayerModel) and any(j.target_model in rename for j in spec.joins):
        return spec.model_copy(update={"joins": _respell_joins(list(spec.joins), rename)})
    return spec


def _respell_extension(extension: ModelExtension, rename: Dict[str, str]) -> ModelExtension:
    update: Dict[str, Any] = {}
    if extension.source_name in rename:
        update["source_name"] = rename[extension.source_name]
    if any(j.target_model in rename for j in extension.joins or []):
        update["joins"] = _respell_joins(list(extension.joins or []), rename)
    return extension.model_copy(update=update) if update else extension


def localize_stages(
    queries: List[SlayerQuery], *, model: Optional[str] = None,
) -> "Tuple[List[SlayerQuery], Dict[str, StageDisplay]]":
    """Give every named non-root stage a minted identity and respell the references the
    list itself writes (sources, extended models, inline and extension join targets); stored
    definitions are untouched. Under ``model`` (a spliced stored query-backed model)
    the final stage takes the model's name. Returns the stages and ``{identity: display}``."""
    if not queries:
        return [], {}
    rest, root = queries[:-1], queries[-1]
    rename = {q.name: stage_identity(name=q.name, model=model) for q in rest if q.name}
    displays = {ident: StageDisplay(name=name, model=model) for name, ident in rename.items()}
    out: List[SlayerQuery] = []
    for q in queries:
        update: Dict[str, Any] = {"source_model": _respell_spec(q.source_model, rename)}
        if q.extensions:
            update["extensions"] = [_respell_extension(e, rename) for e in q.extensions]
        if q is not root and q.name:
            update["name"] = rename[q.name]
        elif model is not None:
            update["name"] = model
            displays[model] = StageDisplay(name=model, model=model)
        out.append(q.model_copy(update=update))
    return out, displays


def _index_query_list_by_name(rest: List[Any], root: Any) -> Dict[str, Any]:
    """Build ``{name: query}`` for the non-final entries. Validates that
    every non-final stage has a unique non-empty name and that the
    root's name (if any) doesn't collide with a sibling.
    """
    rest_by_name: Dict[str, Any] = {}
    for q in rest:
        if not q.name:
            raise ValueError(
                "Every non-final entry in a query list must have a "
                "'name' (siblings reference each other by name)."
            )
        if q.name in rest_by_name:
            raise ValueError(f"Duplicate stage name '{q.name}' in query list.")
        rest_by_name[q.name] = q
    if root.name and root.name in rest_by_name:
        raise ValueError(
            f"Stage name '{root.name}' is duplicated: the final entry "
            f"shares a name with an earlier entry."
        )
    return rest_by_name


def _validate_query_list_invariants(
    queries: List[Any],
    rest: List[Any],
    root: Any,
    sibling_names: Set[str],
) -> None:
    """Reject self-references and any sibling that depends on the root.

    Self-references are caught for every entry (including the root).
    Root-as-sink: no non-final stage may reference the root by name.
    """
    for q in queries:
        if q.name and q.name in stage_sibling_reads(query=q, siblings={q.name} | sibling_names):
            raise ValueError(
                f"Stage '{q.name}' references itself — self-references "
                f"are not allowed."
            )
    if root.name:
        referrers = sorted(
            q.name for q in rest if root.name in stage_sibling_reads(query=q, siblings={root.name})
        )
        if referrers:
            raise ValueError(
                f"The final entry '{root.name}' is the DAG root and must "
                f"not be referenced by other stages. Referenced by: "
                f"{referrers}."
            )


def _build_dependency_graph(
    rest_by_name: Dict[str, Any],
    sibling_names: Set[str],
) -> "tuple[Dict[str, int], Dict[str, List[str]]]":
    """Build the (in_degree, dependents) adjacency for Kahn's algorithm."""
    in_degree: Dict[str, int] = dict.fromkeys(rest_by_name, 0)
    dependents: Dict[str, List[str]] = {name: [] for name in rest_by_name}
    for name, q in rest_by_name.items():
        for prereq in stage_sibling_reads(query=q, siblings=sibling_names):
            dependents[prereq].append(name)
            in_degree[name] += 1
    return in_degree, dependents


def _kahn_sort(
    in_degree: Dict[str, int],
    dependents: Dict[str, List[str]],
) -> List[str]:
    """Topologically sort by Kahn's algorithm. Cycle → ``ValueError``.

    The frontier is kept sorted for deterministic output order across runs.
    """
    frontier: List[str] = sorted(n for n, d in in_degree.items() if d == 0)
    sorted_names: List[str] = []
    while frontier:
        n = frontier.pop(0)
        sorted_names.append(n)
        unlocked: List[str] = []
        for dep in dependents[n]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                unlocked.append(dep)
        frontier.extend(sorted(unlocked))
    if len(sorted_names) < len(in_degree):
        cycle = sorted(set(in_degree) - set(sorted_names))
        raise ValueError(
            f"Cycle in query list: stages {cycle} form a cyclic "
            f"dependency. The reference graph must be acyclic."
        )
    return sorted_names


def topologically_order_stages(queries: List[Any]) -> List[Any]:
    """Re-order a query list so every stage appears after the siblings it
    references via ``source_model`` or ``joins[].target_model``.

    The final entry is the entry point / DAG root: it stays last. Only
    the non-final entries are reordered. Stages that aren't reachable
    from the root are accepted as utility sub-queries — they flow
    through the sort like any other node.

    Raises ``ValueError`` on: missing ``name`` on a non-final entry;
    duplicate stage names; self-references; the root being depended on
    by any other stage; or a cycle among non-final stages.
    """
    if len(queries) <= 1:
        return list(queries)
    rest = list(queries[:-1])
    root = queries[-1]
    rest_by_name = _index_query_list_by_name(rest, root)
    sibling_names: Set[str] = set(rest_by_name)
    _validate_query_list_invariants(queries, rest, root, sibling_names)
    in_degree, dependents = _build_dependency_graph(rest_by_name, sibling_names)
    sorted_names = _kahn_sort(in_degree, dependents)
    return [rest_by_name[n] for n in sorted_names] + [root]


__all__ = [
    "SPLICED_STAGE_PREFIX",
    "USER_STAGE_PREFIX",
    "localize_stages",
    "stage_identity",
    "stage_sibling_reads",
    "topologically_order_stages",
]
