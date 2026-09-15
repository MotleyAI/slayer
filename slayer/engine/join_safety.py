"""Join-arity safety: a hop is *provably many-to-one* iff, on its traversal
orientation, its target-side columns cover a declared PK/unique set or its
oriented cardinality is ``many_to_one``/``one_to_one``. Unknown = unsafe.
Both orientations of every declared edge participate; proof is per orientation
(DEV-1853), so one direction may be provably to-one while the other fans out."""

from __future__ import annotations

import re
from typing import Callable, Dict, List, Optional, Sequence, Tuple, TypeVar, Union

from pydantic import BaseModel

from slayer.core.enums import JoinCardinality, RANKED_AGGREGATIONS, invert_cardinality
from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.join_walker import OrientedJoin, resolve_hop, walk
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    Grain,
    StarKey,
    TimeTruncKey,
    ValueKey,
    reroot_value_key,
    substitute_value_keys,
    walk_value_keys,
    window_kwarg_of,
)
from slayer.core.models import ModelJoin, SlayerModel
from slayer.core.scope import ModelScope, StageSchema
from slayer.engine.aggregate_input_paths import compute_aggregate_input_join_paths
from slayer.engine.elaborate_env import check_partition_key_attributable
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.cardinality import (
    CardinalityVerdict,
    JoinCardinalityReport,
    is_key_set_unique,
)

__all__ = [
    "provably_to_one",
    "safe_reachable",
    "may_inline_crossing_inputs",
    "audit_join_safety",
    "JoinSafetyFinding",
]

#: An oriented hop, or a declared join read in its declared orientation — both
#: expose ``cardinality`` and target-side ``join_pairs``, which is all the proof
#: predicate reads.
OrientedLike = Union[OrientedJoin, ModelJoin]


def may_inline_crossing_inputs(crossed_paths: Sequence[tuple]) -> bool:  # NOSONAR(S1172) — crossed_paths is the documented DEV-1688 seam; the cardinality-aware decision reads it, hardcoded False until then.
    """Whether a crossing-input local aggregate may stay inline in the host base.
    Hardcoded ``False`` (always a producer); the DEV-1688 seam that will flip."""
    return False


#: A bare-identifier ``Column.sql`` rename carries the column's uniqueness.
_BARE_IDENT_RE = re.compile(r"[A-Za-z_]\w*")


def _physical_name(column) -> str:
    """Physical spelling: a bare-identifier ``sql`` rename, else the model name."""
    sql = (column.sql or "").strip()
    return sql if sql and _BARE_IDENT_RE.fullmatch(sql) else column.name


def _unique_key_sets(model: SlayerModel) -> list[list[str]]:
    # PHYSICAL spelling: composite PK as one set, then each solo-unique singleton.
    sets: list[list[str]] = []
    pk = [_physical_name(c) for c in model.columns if c.primary_key]
    if pk:
        sets.append(pk)
    for c in model.columns:
        if c.unique:
            sets.append([_physical_name(c)])
    return sets


def provably_to_one(*, edge: OrientedLike, target_model: SlayerModel) -> bool:
    """Is ``edge`` provably many-to-one onto ``target_model`` in its orientation?
    True iff the oriented cardinality is m:1/1:1, or the traversal-target columns
    fully cover a unique key-set of ``target_model`` (PHYSICAL spelling)."""
    if edge.cardinality in (JoinCardinality.MANY_TO_ONE, JoinCardinality.ONE_TO_ONE):
        return True
    by_name = {c.name: c for c in target_model.columns}
    target_cols = [
        _physical_name(by_name[pair[1]]) if pair[1] in by_name else pair[1]
        for pair in edge.join_pairs
    ]
    return is_key_set_unique(
        key_columns=target_cols, unique_key_sets=_unique_key_sets(target_model)
    )


def safe_reachable(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> bool:
    """Is ``path`` a reachable chain of provably many-to-one hops from ``root``?
    Any declared edge traverses in either orientation (DEV-1853); an unresolvable
    or revisiting hop fails the walk. Empty path is safe. An ambiguous hop raises."""
    chain = walk(root=root, path=path, models_by_name=models_by_name)
    if chain is None:
        return False
    for edge in chain:
        target_model = models_by_name.get(edge.target_model)
        if target_model is None or not provably_to_one(
            edge=edge, target_model=target_model
        ):
            return False
    return True


class JoinSafetyFinding(BaseModel):
    """One audit record per declared edge: the provability of both orientations
    plus, when the declared (forward) orientation is unproven, the broadcast
    consequence and remedies. Detection contradictions are separate findings."""

    data_source: str
    model: str
    target_model: str
    message: str
    severity: str = "warning"
    forward_provably_to_one: bool = False
    reverse_provably_to_one: bool = False
    contradiction: bool = False


_UNPROVEN_REMEDY = (
    "declare `cardinality` many_to_one/one_to_one, declare a covering unique "
    "key on the target, or run cardinality detection"
)


def audit_join_safety(
    *,
    models: Sequence[SlayerModel],
    detection: Optional[JoinCardinalityReport] = None,
) -> list[JoinSafetyFinding]:
    """One finding per declared edge, recording both orientations' provability.
    The forward-unproven edges carry the broadcast consequence and remedies;
    data-contradicted declarations are appended as separate error findings."""
    # Joins resolve within the parent model's datasource — no cross-datasource shadowing.
    models_by_key = {(m.data_source, m.name): m for m in models}
    findings: list[JoinSafetyFinding] = []
    for model in models:
        for join in model.joins:
            target = models_by_key.get((model.data_source, join.target_model))
            if target is not None:
                findings.append(_edge_finding(model=model, join=join, target=target))
    if detection is not None:
        findings.extend(
            _contradiction_finding(finding)
            for finding in detection.findings
            if finding.verdict is CardinalityVerdict.CONTRADICTS_HARD
        )
    return findings


def _edge_finding(
    *, model: SlayerModel, join, target: SlayerModel
) -> JoinSafetyFinding:
    """Provability of one declared edge in both orientations. Orients THIS
    declaration directly (never searches by pairs — parallel edges may share
    them)."""
    fwd_edge = OrientedJoin(
        source_model=model.name, target_model=join.target_model,
        join_pairs=[list(p) for p in join.join_pairs],
        join_type=join.join_type, cardinality=join.cardinality,
        name=join.name, declaring_model=model.name,
    )
    rev_edge = OrientedJoin(
        source_model=join.target_model, target_model=model.name,
        join_pairs=[[p[1], p[0]] for p in join.join_pairs],
        join_type=join.join_type,
        cardinality=invert_cardinality(join.cardinality),
        name=join.name, declaring_model=model.name,
    )
    fwd_ok = provably_to_one(edge=fwd_edge, target_model=target)
    rev_ok = provably_to_one(edge=rev_edge, target_model=model)
    if fwd_ok:
        message = (
            f"Join {model.name} → {join.target_model} is provably "
            f"to-one; the reverse hop "
            f"{join.target_model} → {model.name} "
            f"{'is provably to-one' if rev_ok else 'fans out'}."
        )
    else:
        message = (
            f"Join {model.name} → {join.target_model} is unproven: "
            f"metrics crossing it will broadcast rather than join "
            f"through. Remedies: {_UNPROVEN_REMEDY}."
        )
    return JoinSafetyFinding(
        data_source=model.data_source,
        model=model.name,
        target_model=join.target_model,
        message=message,
        severity="warning" if not fwd_ok else "info",
        forward_provably_to_one=fwd_ok,
        reverse_provably_to_one=rev_ok,
    )


def _contradiction_finding(finding) -> JoinSafetyFinding:
    return JoinSafetyFinding(
        data_source=finding.data_source,
        model=finding.model,
        target_model=finding.target_model,
        message=(
            f"Join {finding.model} → {finding.target_model} declares "
            f"{finding.stored}, but cardinality detection contradicts "
            f"it (observed {finding.detected})."
        ),
        severity="error",
        contradiction=True,
    )


# ---------------------------------------------------------------------------
# Attributability / re-rooting analysis shared by binding, elaboration and compilation
# ---------------------------------------------------------------------------



def key_host_path(key: ValueKey) -> Tuple[str, ...]:
    if isinstance(key, TimeTruncKey):
        return tuple(getattr(key.column, "path", ()) or ())
    return tuple(getattr(key, "path", ()) or ())


def _back_token(
    *, root_model: SlayerModel, host_name: str, target_path: Tuple[str, ...],
    models_by_name: Dict[str, SlayerModel],
) -> str:
    """The token that traverses from the aggregate's root back to the host.

    An edge-name hop is direction-agnostic, so when the last target-path token
    is a named edge it also names the reverse hop and resolves unambiguously
    (the bare host model name can be ambiguous across parallel edges). Falls
    back to the host model name otherwise (DEV-1853 D5)."""
    if target_path:
        last = target_path[-1]
        if last != host_name:
            try:
                edge = resolve_hop(
                    current=root_model, token=last,
                    models_by_name=models_by_name,
                )
            except AmbiguousJoinPathError:
                edge = None
            if edge is not None and edge.target_model == host_name:
                return last
    return host_name


def attributable_from_root(
    *, host_path: Tuple[str, ...], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: Optional[str] = None,
) -> bool:
    """Is a host-coordinate path attributable from the aggregate's root over provably many-to-one hops only?"""
    tp, hp = tuple(target_path), tuple(host_path)
    if hp[: len(tp)] == tp:
        return safe_reachable(
            root=root_model, path=hp[len(tp):], models_by_name=models_by_name,
        )
    if host_name is None or (tp and host_name == tp[0]):
        return False
    if hp and safe_reachable(root=root_model, path=hp, models_by_name=models_by_name):
        return True
    back = _back_token(
        root_model=root_model, host_name=host_name, target_path=tp,
        models_by_name=models_by_name,
    )
    return safe_reachable(
        root=root_model, path=(back, *hp), models_by_name=models_by_name,
    )


def _reroot_leaf_via_host(
    r: ValueKey, *, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: str,
) -> Optional[ValueKey]:
    if not isinstance(r, (ColumnKey, ColumnSqlKey, StarKey, TimeTruncKey)):
        return None
    hp = key_host_path(r)
    if hp[: len(target_path)] == target_path:
        return None  # reroot_value_key strips the prefix
    if target_path and host_name == target_path[0]:
        return None
    back = _back_token(
        root_model=root_model, host_name=host_name, target_path=target_path,
        models_by_name=models_by_name,
    )
    via_host = (back, *hp)
    if not safe_reachable(
        root=root_model, path=via_host, models_by_name=models_by_name,
    ) and hp and safe_reachable(
        root=root_model, path=hp, models_by_name=models_by_name,
    ):
        return None  # resolved through the root's own join to the sibling
    if isinstance(r, TimeTruncKey):
        return r.model_copy(update={
            "column": r.column.model_copy(update={"path": via_host}),
        })
    return r.model_copy(update={"path": via_host})


_RerootableT = TypeVar("_RerootableT", bound=ValueKey)


def reroot_from_root(
    key: _RerootableT, *, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: str,
) -> _RerootableT:
    """Re-anchor a host-coordinate key into the root's coordinates, per leaf, by the same rules ``attributable_from_root`` proves safety with."""
    tp = tuple(target_path)
    mapping: Dict[ValueKey, ValueKey] = {}
    for r in walk_value_keys(key):
        # The walk also yields TimeTruncKey.column, but substitute matches the
        # whole TimeTruncKey pre-order, so the inner-column entry is inert.
        rerooted = _reroot_leaf_via_host(
            r, target_path=tp, root_model=root_model,
            models_by_name=models_by_name, host_name=host_name,
        )
        if rerooted is not None:
            mapping[r] = rerooted
    # Strip the target prefix from under-target refs FIRST; off-side refs
    # (the via-host mapping) never start with the target prefix so they survive
    # unchanged, then get substituted. Doing it the other way round would let a
    # direction-agnostic edge-name back-token (== the target token) be stripped.
    key = reroot_value_key(key, target_path=tp)
    if mapping:
        key = substitute_value_keys(key, mapping)
    return key


UNREACHABLE_NO_PATH = "unreachable from the aggregate's root (no join path from it)"


def _hop_walk_reason(
    *, root_model: SlayerModel, path: Tuple[str, ...],
    models_by_name: Dict[str, SlayerModel],
) -> Optional[str]:
    """Walk ``path`` from ``root_model`` (bidirectional, DEV-1853): the
    fanning/unproven-hop reason if the path resolves but a hop is not provably
    many-to-one, else ``None`` (an unresolvable/ambiguous path is unreachable)."""
    try:
        chain = walk(root=root_model, path=path, models_by_name=models_by_name)
    except AmbiguousJoinPathError:
        return None
    if chain is None:
        return None
    for i, edge in enumerate(chain):
        tgt = models_by_name.get(edge.target_model)
        if tgt is None:
            return None
        if not provably_to_one(edge=edge, target_model=tgt):
            token = path[i] if i < len(path) else edge.target_model
            return f"crosses a fanning or unproven join hop to {token}"
    return None


def broadcast_reason(
    *, host_path: Tuple[str, ...], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: Optional[str] = None,
) -> str:
    """Why a dimension broadcasts: crosses an unproven/fanning join hop (forward
    from the root, or back through the reverse hop), or unreachable if no path."""
    tp, hp = tuple(target_path), tuple(host_path)
    if hp[: len(tp)] == tp:
        reason = _hop_walk_reason(
            root_model=root_model, path=hp[len(tp):], models_by_name=models_by_name,
        )
        return reason or UNREACHABLE_NO_PATH
    # Off the forward path: reachable only back through the reverse (fanning) hop?
    if host_name is not None and not (tp and host_name == tp[0]):
        back = _back_token(
            root_model=root_model, host_name=host_name, target_path=tp,
            models_by_name=models_by_name,
        )
        reason = _hop_walk_reason(
            root_model=root_model, path=(back, *hp), models_by_name=models_by_name,
        )
        if reason is not None:
            return reason
    return UNREACHABLE_NO_PATH


def assert_partition_key_attributable(
    *, key: ValueKey, pk: ValueKey, label: str,
    scope: Union[ModelScope, StageSchema], bundle: ResolvedSourceBundle,
) -> None:
    """Resolve a partition key's attributability from the aggregate's root; the checker raises on an unproven/fanning hop."""
    hp = key_host_path(pk)
    if not hp:
        return  # a local column — no join to cross
    host_m = scope.source_model if isinstance(scope, ModelScope) else None
    if host_m is None:
        return
    agg_target = (
        tuple(getattr(key.source, "path", ()) or ())
        if isinstance(key, AggregateKey) else ()
    )
    models_by_name = {m.name: m for m in bundle.referenced_models}
    root = walk_key_path(model=host_m, path=agg_target, bundle=bundle) or host_m
    host_name = host_m.name if agg_target else None
    attributable = attributable_from_root(
        host_path=hp, target_path=agg_target, root_model=root,
        models_by_name=models_by_name, host_name=host_name,
    )
    reason = None if attributable else broadcast_reason(
        host_path=hp, target_path=agg_target, root_model=root,
        models_by_name=models_by_name, host_name=host_name,
    )
    check_partition_key_attributable(
        label=label, pk=pk, attributable=attributable, reason=reason,
    )


def shared_join_key_reroot(
    *, key: ValueKey, target_path: Tuple[str, ...], host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
) -> Optional[ValueKey]:
    """A host-local dimension that IS a source-side join column of the single hop to the root: return the root's target-side ColumnKey, else ``None``."""
    if not isinstance(key, ColumnKey) or key_host_path(key) or len(target_path) != 1:
        return None
    try:
        edge = resolve_hop(
            current=host_model, token=target_path[0],
            models_by_name=models_by_name,
        )
    except AmbiguousJoinPathError:
        return None
    if edge is None:
        return None
    for src, tgt in edge.join_pairs:
        if src == key.leaf:
            return key.model_copy(update={"leaf": tgt, "path": ()})
    return None


def grain_member_attributable(
    *, key: ValueKey, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
) -> bool:
    """Is a grain member attributable from the aggregate's root? (Every column/aggregate it references must be.)"""
    saw = False
    for r in walk_value_keys(key):
        if isinstance(r, AggregateKey):
            saw = True
            if not attributable_from_root(
                host_path=tuple(getattr(r.source, "path", ()) or ()), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
            ):
                return False
        elif isinstance(r, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey)):
            saw = True
            if not attributable_from_root(
                host_path=key_host_path(r), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
                host_name=host_name,
            ):
                return False
    return saw


def _grain_leaf_name(key: ValueKey) -> Optional[str]:
    """The physical leaf a column-ish grain member / dimension names, else None."""
    if isinstance(key, ColumnKey):
        return key.leaf
    if isinstance(key, ColumnSqlKey):
        return key.column_name
    return None


def grain_determines(
    *, key: ValueKey, grain: Grain, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
) -> bool:
    """Does a dataset grain determine ``key`` (Axiom 1)? True iff ``key``
    is a grain member, an aggregate whose ``partition_by=`` grain ⊆ the grain (a
    cell of the same dataset), or a column reached over provably to-one hops from
    a model the grain pins — pinned by that model's unique key lying in the grain
    at its path (an entity-key seed) or by the host-side join columns of the hop
    into it (a foreign-key seed: the fixed key pins the to-one target row)."""
    if key in grain:
        return True
    if isinstance(key, AggregateKey):
        # Recursive: each partition key must itself be determined — a member, a
        # to-one column, or a nested aggregate whose grain is determined; an
        # expression key only as an exact member (DEV-1859 decision 12).
        return key.partition_keys is not None and all(
            grain_determines(
                key=pk, grain=grain, host_model=host_model,
                models_by_name=models_by_name,
            )
            for pk in key.partition_keys)
    if not isinstance(key, (ColumnKey, ColumnSqlKey)):
        return False
    return _column_grain_determined(
        key=key, grain=grain, host_model=host_model, models_by_name=models_by_name,
    )


def _physical_grain_leaves(*, grain: Grain, at: Tuple[str, ...], model: SlayerModel) -> set:
    """The grain's leaves at path ``at``, spelled physically for ``model`` — so a
    logical ``ColumnKey.leaf`` matches a ``_unique_key_sets`` entry carrying a
    bare-identifier ``Column.sql`` rename."""
    by_name = {c.name: _physical_name(c) for c in model.columns}
    return {
        by_name.get(leaf, leaf)
        for g in grain
        if key_host_path(g) == at and (leaf := _grain_leaf_name(g)) is not None
    }


def _entity_seeded(*, grain: Grain, model: SlayerModel, at: Tuple[str, ...]) -> bool:
    """The grain pins ``model`` at ``at`` iff its leaves there cover a unique key
    set (both physical spelling)."""
    here = _physical_grain_leaves(grain=grain, at=at, model=model)
    return any(ks and set(ks) <= here for ks in _unique_key_sets(model))


def _hop_pins(
    *, edge, src_model: Optional[SlayerModel], tgt: SlayerModel, grain: Grain,
    path: Tuple[str, ...], i: int, pinned_before: bool,
) -> bool:
    """The to-one target of ``edge`` stays pinned iff its own entity key is seeded,
    its host-side FK columns are grain members, or the source was pinned and the hop
    is provably to-one. ``join_pairs`` source columns are PHYSICAL names, matched
    against the grain's physical leaves at the source (same normalization the
    entity-key seed uses, so a renamed FK still seeds)."""
    to_one = provably_to_one(edge=edge, target_model=tgt)
    src_leaves = (
        _physical_grain_leaves(grain=grain, at=path[:i], model=src_model)
        if src_model is not None else set()
    )
    fk_seed = to_one and bool(edge.join_pairs) and all(
        src in src_leaves for src, _ in edge.join_pairs)
    return (
        _entity_seeded(grain=grain, model=tgt, at=path[: i + 1])
        or fk_seed
        or (pinned_before and to_one)
    )


def _column_grain_determined(
    *, key: ValueKey, grain: Grain, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
) -> bool:
    """The column arm of ``grain_determines``: pinned from the host over provably
    to-one hops, each hop reseeded by an entity or FK key in the grain."""
    path = key_host_path(key)
    try:
        chain = walk(root=host_model, path=path, models_by_name=models_by_name)
    except AmbiguousJoinPathError:
        return False
    if path and chain is None:
        return False
    pinned = _entity_seeded(grain=grain, model=host_model, at=())
    for i, e in enumerate(chain or []):
        tgt = models_by_name.get(e.target_model)
        if tgt is None:
            return False
        src_model = host_model if i == 0 else models_by_name.get(e.source_model)
        pinned = _hop_pins(
            edge=e, src_model=src_model, tgt=tgt, grain=grain,
            path=path, i=i, pinned_before=pinned,
        )
    return pinned


def local_crossing_input_paths(
    *, key: AggregateKey, bundle: ResolvedSourceBundle,
    host_model: SlayerModel, include_source: bool = True,
) -> List[Tuple[str, ...]]:
    out: List[Tuple[str, ...]] = []
    if key.column_filter_key is not None:
        for p in key.column_filter_key.referenced_join_paths:
            if p not in out:
                out.append(tuple(p))
    for p in compute_aggregate_input_join_paths(
        key=key,
        anchor_model=host_model,
        anchor_relation=host_model.name,
        bundle=bundle,
        include_source=include_source,
    ):
        if p not in out:
            out.append(tuple(p))
    return out


def crossing_local_root_predicate(
    *, scope: Union[ModelScope, StageSchema], bundle: ResolvedSourceBundle,
) -> Callable[[ValueKey], bool]:
    """Predicate for a LOCAL plain aggregate whose inputs cross a join (desugars onto a HOST-rooted producer); windowed / ranked roots excluded."""
    host_model = scope.source_model if isinstance(scope, ModelScope) else None

    def _pred(k: ValueKey) -> bool:
        return (
            isinstance(k, AggregateKey)
            and k.partition_keys is None
            and not getattr(k.source, "path", ())
            and window_kwarg_of(k) is None
            and k.agg not in RANKED_AGGREGATIONS
            and host_model is not None
            and _crosses(k, host=host_model)
        )

    def _crosses(k: AggregateKey, *, host: SlayerModel) -> bool:
        crossed = local_crossing_input_paths(
            key=k, bundle=bundle, host_model=host,
        )
        return bool(crossed) and not may_inline_crossing_inputs(crossed)

    return _pred
