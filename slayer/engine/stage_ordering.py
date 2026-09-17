"""Kahn topo-sort for stored / runtime ``source_queries`` stage lists.

The last stage stays root / sink. Cycles, self-references, duplicate names, and
a root referenced by another stage raise ``ValueError`` naming the offending
stage; forward references are reordered, not rejected. Sibling refs are walked
through inline ``SlayerModel`` / ``ModelExtension`` specs (including nested
``source_queries``) and ``joins[].target_model``.
"""
from __future__ import annotations

from typing import Any, Dict, List, Set

from slayer.core.query import ModelExtension, SlayerQuery, SourceSpec


def _extract_sibling_refs(query: SlayerQuery, against: Set[str]) -> Set[str]:
    """Sibling names in ``against`` that ``query`` references; purely structural, never raises."""
    out: Set[str] = set()
    _walk_spec(query.source_model, against, out)
    return out


def _walk_spec(spec: SourceSpec | None, against: Set[str], out: Set[str]) -> None:
    """Recursively collect sibling refs from a ``source_model`` spec."""
    if spec is None:
        return
    if isinstance(spec, str):
        if spec in against:
            out.add(spec)
        return
    if isinstance(spec, ModelExtension):
        if spec.source_name in against:
            out.add(spec.source_name)
        joins = spec.joins or []
    else:
        joins = spec.joins
        for inner in spec.source_queries or []:
            _walk_spec(inner.source_model, against, out)
    for j in joins:
        if j.target_model in against:
            out.add(j.target_model)


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
        if q.name and q.name in _extract_sibling_refs(q, {q.name} | sibling_names):
            raise ValueError(
                f"Stage '{q.name}' references itself — self-references "
                f"are not allowed."
            )
    if root.name:
        referrers = sorted(
            q.name for q in rest if root.name in _extract_sibling_refs(q, {root.name})
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
        for prereq in _extract_sibling_refs(q, sibling_names):
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


__all__ = ["topologically_order_stages"]
