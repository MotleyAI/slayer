"""Kahn topo-sort and name localization for stored / runtime stage lists.

The last stage stays root / sink. Cycles, self-references, duplicate names, and
a root referenced by another stage raise ``ValueError`` naming the offending
stage; forward references are reordered, not rejected. A stage reads the siblings
it names: its ``population``, its extensions' datasets and relationship targets,
and any dataset a reference in its expressions names.
"""
from __future__ import annotations

from typing import AbstractSet, Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from slayer.core.names import match_name
from slayer.core.models import DatasetRelationship, SlayerDataset
from slayer.core.relationship_walker import OrientedRelationship, matching_edges, neighbors, unobserved
from slayer.core.query import DatasetExtension, SlayerQuery, SourceSpec, extension_dataset_names
from slayer.core.scope import StageDisplay
from slayer.engine.extensions import query_reference_parts

#: Minted identity prefixes; dataset names never carry them (``__slayer_`` is reserved).
USER_STAGE_PREFIX = "__slayer_stage_"
SPLICED_STAGE_PREFIX = "__slayer_qb__"


def stage_sibling_reads(
    *, query: SlayerQuery, siblings: AbstractSet[str], displays: Optional[Mapping[str, StageDisplay]] = None,
    population_edges: Sequence[OrientedRelationship] = (), expressions: bool = True,
) -> Set[str]:
    """The siblings ``query`` names: its population, its extensions' datasets, and a dataset any reference in its
    expressions names (by identity, or by the display name of a sibling in its own list), except a first segment
    spelling a relationship of its population (``population_edges``) or of its extensions; never raises.
    ``expressions=False`` reads only the population and extensions."""
    spec = query.population
    names = [spec] if spec is not None else []
    found = {match_name(name=n, declared=siblings, quoted=False) for n in (*names, *extension_dataset_names(query))}
    written = _written_names(query=query, siblings=siblings, displays=displays or {})
    own = _extension_relationship_spellings(query)
    for parts in query_reference_parts(query) if expressions else ():
        # Only a reference's head (after a root qualifier) names a dataset; later segments are relationship hops.
        head = parts[1:] if _root_qualifier(parts, query=query, displays=displays or {}) else parts
        if len(head) < 2 or _spells_relationship(head[0], edges=population_edges, own=own):
            continue
        hit = match_name(name=head[0], declared=written, quoted=False)
        if hit is not None:
            found.add(written[hit])
    return {f for f in found if f is not None}


def _root_qualifier(parts: Sequence[str], *, query: SlayerQuery, displays: Mapping[str, StageDisplay]) -> bool:
    spec = query.population
    if len(parts) < 3 or not isinstance(spec, str):
        return False
    spellings = [spec, *([displays[spec].name] if spec in displays else [])]
    return match_name(name=parts[0], declared=spellings, quoted=False) is not None


def _extension_relationship_spellings(query: SlayerQuery) -> List[str]:
    return [n for e in query.extensions or [] for j in e.relationships or [] for n in (j.name, j.to) if n]


def _spells_relationship(head: str, *, edges: Sequence[OrientedRelationship], own: List[str]) -> bool:
    return bool(matching_edges(list(edges), head)) or match_name(name=head, declared=own, quoted=False) is not None


def population_edges(query: SlayerQuery, datasets_by_name: Mapping[str, SlayerDataset]) -> List[OrientedRelationship]:
    """The one-hop relationships of ``query``'s stored population (none for a sibling or an inferred population)."""
    spec = query.population
    dataset = datasets_by_name.get(spec) if isinstance(spec, str) else None
    if dataset is None:
        return []
    with unobserved():
        return neighbors(dataset=dataset, datasets_by_name=dict(datasets_by_name))


def _written_names(
    *, query: SlayerQuery, siblings: AbstractSet[str], displays: Mapping[str, StageDisplay],
) -> Dict[str, str]:
    """How ``query``'s expressions may write each sibling → its identity."""
    own = displays.get(query.name) if query.name else None
    group = own.dataset if own is not None else None
    # A stage's spelling shadows a same-named dataset identity.
    out = {s: s for s in siblings}
    out.update({d.name: ident for ident, d in displays.items() if ident in siblings and d.dataset == group})
    return out


def stage_identity(*, name: str, dataset: Optional[str] = None) -> str:
    """The minted identity of stage ``name`` (of stored query-backed ``dataset``, if spliced)."""
    if dataset is None:
        return f"{USER_STAGE_PREFIX}{name}"
    return f"{SPLICED_STAGE_PREFIX}{dataset}__{name}"


def _respelled(name: str, rename: Dict[str, str]) -> str:
    found = match_name(name=name, declared=rename, quoted=False)
    return name if found is None else rename[found]


def _respell_relationships(relationships: List[DatasetRelationship], rename: Dict[str, str]) -> List[DatasetRelationship]:
    return [j.model_copy(update={"to": _respelled(j.to, rename)}) for j in relationships]


def _respell_spec(spec: SourceSpec | None, rename: Dict[str, str]) -> SourceSpec | None:
    """``spec`` respelled through ``rename``."""
    return _respelled(spec, rename) if spec is not None else None


def _respell_extension(extension: DatasetExtension, rename: Dict[str, str]) -> DatasetExtension:
    update: Dict[str, Any] = {}
    if _respelled(extension.dataset, rename) != extension.dataset:
        update["dataset"] = _respelled(extension.dataset, rename)
    if any(_respelled(j.to, rename) != j.to for j in extension.relationships or []):
        update["relationships"] = _respell_relationships(list(extension.relationships or []), rename)
    return extension.model_copy(update=update) if update else extension


def localize_stages(
    queries: List[SlayerQuery], *, dataset: Optional[str] = None,
) -> "Tuple[List[SlayerQuery], Dict[str, StageDisplay]]":
    """Give every named non-root stage a minted identity and respell the references the
    list itself writes (sources, extended datasets, extension relationship targets); stored
    definitions are untouched. Under ``dataset`` (a spliced stored query-backed dataset)
    the final stage takes the dataset's name. Returns the stages and ``{identity: display}``."""
    if not queries:
        return [], {}
    rest, root = queries[:-1], queries[-1]
    rename = {q.name: stage_identity(name=q.name, dataset=dataset) for q in rest if q.name}
    displays = {ident: StageDisplay(name=name, dataset=dataset) for name, ident in rename.items()}
    out: List[SlayerQuery] = []
    for q in queries:
        update: Dict[str, Any] = {"population": _respell_spec(q.population, rename)}
        if q.extensions:
            update["extensions"] = [_respell_extension(e, rename) for e in q.extensions]
        if q is not root and q.name:
            update["name"] = rename[q.name]
        elif dataset is not None:
            update["name"] = dataset
            displays[dataset] = StageDisplay(name=dataset, dataset=dataset)
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
        if match_name(name=q.name, declared=rest_by_name, quoted=False) is not None:
            raise ValueError(f"Duplicate stage name '{q.name}' in query list (stage names match in any case).")
        rest_by_name[q.name] = q
    if root.name and match_name(name=root.name, declared=rest_by_name, quoted=False) is not None:
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
    displays: Mapping[str, StageDisplay],
    datasets_by_name: Mapping[str, SlayerDataset],
    expressions: bool,
) -> None:
    """Reject self-references and any sibling that depends on the root.

    Self-references are caught for every entry (including the root).
    Root-as-sink: no non-final stage may reference the root by name.
    """
    for q in queries:
        if q.name and q.name in stage_sibling_reads(query=q, siblings={q.name} | sibling_names, displays=displays,
                                                    population_edges=population_edges(q, datasets_by_name), expressions=expressions):
            raise ValueError(
                f"Stage '{_spelled(q.name, displays)}' references itself — self-references "
                f"are not allowed."
            )
    if root.name:
        referrers = sorted(
            _spelled(q.name, displays) for q in rest if root.name in stage_sibling_reads(
                query=q, siblings={root.name}, displays=displays, population_edges=population_edges(q, datasets_by_name), expressions=expressions)
        )
        if referrers:
            raise ValueError(
                f"The final entry '{_spelled(root.name, displays)}' is the DAG root and must "
                f"not be referenced by other stages. Referenced by: "
                f"{referrers}."
            )


def _build_dependency_graph(
    rest_by_name: Dict[str, Any],
    sibling_names: Set[str],
    displays: Mapping[str, StageDisplay],
    datasets_by_name: Mapping[str, SlayerDataset],
    expressions: bool,
) -> "tuple[Dict[str, int], Dict[str, List[str]]]":
    """Build the (in_degree, dependents) adjacency for Kahn's algorithm."""
    in_degree: Dict[str, int] = dict.fromkeys(rest_by_name, 0)
    dependents: Dict[str, List[str]] = {name: [] for name in rest_by_name}
    for name, q in rest_by_name.items():
        for prereq in stage_sibling_reads(query=q, siblings=sibling_names, displays=displays,
                                          population_edges=population_edges(q, datasets_by_name), expressions=expressions):
            dependents[prereq].append(name)
            in_degree[name] += 1
    return in_degree, dependents


def _spelled(name: str, displays: Mapping[str, StageDisplay]) -> str:
    """A stage's name as its query list writes it, never its minted identity."""
    return displays[name].name if name in displays else name


def _kahn_sort(
    in_degree: Dict[str, int],
    dependents: Dict[str, List[str]],
    displays: Mapping[str, StageDisplay],
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
        cycle = sorted(_spelled(n, displays) for n in set(in_degree) - set(sorted_names))
        raise ValueError(
            f"Cycle in query list: stages {cycle} form a cyclic "
            f"dependency. The reference graph must be acyclic."
        )
    return sorted_names


def topologically_order_stages(
    queries: List[Any], *, displays: Optional[Mapping[str, StageDisplay]] = None,
    datasets_by_name: Optional[Mapping[str, SlayerDataset]] = None, expressions: bool = True,
) -> List[Any]:
    """Re-order a query list so every stage appears after the siblings it
    reads (``stage_sibling_reads``; ``displays`` maps localized identities to their spellings; ``datasets_by_name``
    supplies the stored populations whose relationships a reference's first segment may spell; ``expressions=False``
    orders by populations and extensions only, for a pre-check without datasets).

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
    datasets = datasets_by_name or {}
    _validate_query_list_invariants(queries, rest, root, sibling_names, displays or {}, datasets, expressions)
    in_degree, dependents = _build_dependency_graph(rest_by_name, sibling_names, displays or {}, datasets, expressions)
    sorted_names = _kahn_sort(in_degree, dependents, displays or {})
    return [rest_by_name[n] for n in sorted_names] + [root]


__all__ = [
    "SPLICED_STAGE_PREFIX",
    "USER_STAGE_PREFIX",
    "localize_stages",
    "stage_identity",
    "population_edges",
    "stage_sibling_reads",
    "topologically_order_stages",
]
