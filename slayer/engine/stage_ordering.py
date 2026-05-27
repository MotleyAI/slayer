"""DEV-1452 Stage B — Kahn topo-sort for stored / runtime ``source_queries``
stage lists.

Extracted from ``SlayerQueryEngine._topologically_order_queries`` so the
migrated ``_expand_query_backed_model`` / ``_validate_and_populate_cache``
can validate stored ``source_queries`` with the same fault-tolerance
contract the runtime ``execute(query=list[...])`` path uses. Decisions
#1 + E of the Stage B plan:

* Last stage stays root / sink. Cycles, self-references, duplicate names,
  root referenced by another stage, and forward references all raise
  ``ValueError`` with messages that name the offending stage.
* Sibling refs are walked recursively through inline ``SlayerModel``
  (typed or dict), ``ModelExtension`` (typed or dict), and ``ModelJoin``
  shapes inside ``joins[].target_model``. An inline-nested
  ``source_queries`` list contributes edges from the enclosing stage to
  any sibling referenced inside.

The classmethod shim on ``SlayerQueryEngine`` delegates here so existing
call sites (``execute(query=list[...])`` at query_engine.py:469) remain
unchanged.
"""
from __future__ import annotations

from typing import Any, Dict, List, Set


def _extract_sibling_refs(query: Any, against: Set[str]) -> Set[str]:
    """Collect every sibling name referenced by ``query`` that appears in
    ``against``. Walks ``source_model`` (string, typed ``SlayerModel`` /
    ``ModelExtension``, or dict) plus any ``joins[].target_model``.

    When ``source_model`` is an inline ``SlayerModel`` carrying its own
    ``source_queries``, recurses into each inner stage so a sibling name
    hidden inside a nested stage still surfaces as an edge from the
    enclosing stage. Same recursion through ``ModelExtension`` shapes.

    The traversal is purely structural — it never touches storage and
    never raises on a missing-sibling reference (the caller validates
    edges against ``against``).
    """
    out: Set[str] = set()
    _walk_spec(query.source_model, against, out)
    return out


def _walk_spec(spec: Any, against: Set[str], out: Set[str]) -> None:  # NOSONAR(S3776) — recursive walker over five ``source_model`` shapes (str / dict-ModelExtension / dict-inline-SlayerModel / typed ModelExtension / typed SlayerModel) plus nested ``source_queries`` recursion. Splitting fragments the per-shape contract; the recursion + isinstance dispatch IS the function.
    """Recursively collect sibling refs from a ``source_model`` spec.

    Handled shapes:
    * ``str`` — bare sibling name.
    * ``dict`` — disambiguates by key shape:
      - ``source_name`` present → ``ModelExtension`` form.
      - ``source_queries`` present → inline ``SlayerModel`` form.
      - otherwise treated as inline ``SlayerModel``.
    * Typed ``SlayerModel`` — walk ``joins[].target_model`` AND every
      inner stage's ``source_model`` in ``source_queries``.
    * Typed ``ModelExtension`` — walk ``source_name`` and ``joins``.
    """
    if spec is None:
        return
    if isinstance(spec, str):
        if spec in against:
            out.add(spec)
        return
    if isinstance(spec, dict):
        if "source_name" in spec:
            src = spec.get("source_name")
            if isinstance(src, str) and src in against:
                out.add(src)
            for j in spec.get("joins") or []:
                tgt = (
                    j.get("target_model") if isinstance(j, dict)
                    else getattr(j, "target_model", None)
                )
                if isinstance(tgt, str) and tgt in against:
                    out.add(tgt)
            return
        # Inline SlayerModel-as-dict (presence of ``source_queries`` is
        # the discriminator from a typed-shape dict; also handles the
        # legitimate no-source_queries inline-model dict by inspecting
        # ``joins`` directly).
        for j in spec.get("joins") or []:
            tgt = (
                j.get("target_model") if isinstance(j, dict)
                else getattr(j, "target_model", None)
            )
            if isinstance(tgt, str) and tgt in against:
                out.add(tgt)
        for inner_q in spec.get("source_queries") or []:
            inner_spec = (
                inner_q.get("source_model")
                if isinstance(inner_q, dict)
                else getattr(inner_q, "source_model", None)
            )
            _walk_spec(inner_spec, against, out)
        return
    # Typed shapes — ModelExtension vs SlayerModel discriminated by the
    # presence of ``source_name`` (only ModelExtension has it).
    src = getattr(spec, "source_name", None)
    if isinstance(src, str) and src in against:
        out.add(src)
    for j in getattr(spec, "joins", None) or []:
        tgt = getattr(j, "target_model", None)
        if isinstance(tgt, str) and tgt in against:
            out.add(tgt)
    # Inline SlayerModel may itself carry ``source_queries``; recurse so
    # references hidden inside any nested stage's ``source_model``
    # surface as edges from the enclosing stage.
    for inner_q in getattr(spec, "source_queries", None) or []:
        inner_spec = getattr(inner_q, "source_model", None)
        _walk_spec(inner_spec, against, out)


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
