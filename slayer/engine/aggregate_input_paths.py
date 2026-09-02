"""Plan-time crossing-input discovery: which join paths do an aggregate's inputs
cross? The widened Law-3 trigger isolates a LOCAL aggregate into a host-rooted CTE
when ANY explicit input crosses. Structural refs contribute as-is; derived
``Column.sql`` / template-fragment kwargs / non-overridden default fragments are
parsed and walked with the Law-1 scanner. ``column_filter_key`` is not re-scanned;
unparseable fragments and scalar values contribute nothing."""

from __future__ import annotations

from typing import List, Optional, Tuple, Union

from slayer.core.keys import AggregateKey, ColumnKey, ColumnSqlKey, StarKey
from slayer.core.models import SlayerModel
from slayer.engine.column_filter_paths import compute_column_filter_join_paths
from slayer.engine.source_bundle import ResolvedSourceBundle

_PathList = List[Tuple[str, ...]]
_StructuralRef = Union[ColumnKey, ColumnSqlKey, StarKey]


def _add_path_prefixes(path: Tuple[str, ...], out: _PathList) -> None:
    """Emit every prefix of ``path`` once — same prefix semantics as the Law-1 scanner."""
    for i in range(1, len(path) + 1):
        prefix = tuple(path[:i])
        if prefix not in out:
            out.append(prefix)


def _scan_sql_fragment(
    sql: str,
    *,
    anchor_model: SlayerModel,
    anchor_relation: str,
    bundle: ResolvedSourceBundle,
    out: _PathList,
) -> None:
    """Scan a free-SQL fragment for crossed join paths (unparseable → nothing)."""
    for path in compute_column_filter_join_paths(
        canonical_sql=sql,
        anchor_model=anchor_model,
        anchor_relation=anchor_relation,
        bundle=bundle,
    ):
        if path not in out:
            out.append(path)


def _collect_ref_paths(
    ref: object,
    *,
    anchor_model: SlayerModel,
    anchor_relation: str,
    bundle: ResolvedSourceBundle,
    out: _PathList,
) -> None:
    """Crossed paths of one embedded ref; scalars contribute nothing, strings scan as free SQL."""
    if isinstance(ref, (ColumnKey, StarKey)):
        _add_path_prefixes(tuple(getattr(ref, "path", ()) or ()), out)
        return
    if isinstance(ref, ColumnSqlKey):
        if ref.path:
            # Structural crossing; further crossing inside the target's Column.sql is the CTE's concern.
            _add_path_prefixes(tuple(ref.path), out)
            return
        col = next(
            (c for c in anchor_model.columns if c.name == ref.column_name),
            None,
        )
        if col is not None and col.sql:
            _scan_sql_fragment(
                col.sql,
                anchor_model=anchor_model,
                anchor_relation=anchor_relation,
                bundle=bundle,
                out=out,
            )
        return
    if isinstance(ref, str):
        _scan_sql_fragment(
            ref,
            anchor_model=anchor_model,
            anchor_relation=anchor_relation,
            bundle=bundle,
            out=out,
        )


def _collect_default_fragment_paths(
    key: AggregateKey,
    *,
    anchor_model: SlayerModel,
    anchor_relation: str,
    bundle: ResolvedSourceBundle,
    out: _PathList,
) -> None:
    """Scan ``key.agg``'s model-default ``AggregationParam.sql`` fragments, skipping kwarg-overridden params."""
    agg_def = next(
        (a for a in (anchor_model.aggregations or []) if a.name == key.agg),
        None,
    )
    if agg_def is None:
        return
    overridden = {name for name, _ in key.kwargs}
    for param in agg_def.params or []:
        param_sql: Optional[str] = getattr(param, "sql", None)
        if param.name in overridden or not param_sql:
            continue
        _scan_sql_fragment(
            param_sql,
            anchor_model=anchor_model,
            anchor_relation=anchor_relation,
            bundle=bundle,
            out=out,
        )


def compute_aggregate_input_join_paths(
    *,
    key: AggregateKey,
    anchor_model: Optional[SlayerModel],
    anchor_relation: str,
    bundle: ResolvedSourceBundle,
    include_source: bool = True,
) -> Tuple[Tuple[str, ...], ...]:
    """Ordered, de-duplicated join-path prefixes crossed by the aggregate's explicit
    inputs; ``()`` when purely local. ``column_filter_key`` crossing is excluded
    (read ``referenced_join_paths``). ``include_source=False`` drops the SOURCE's own
    crossings — reading through a join is legal; only filter refs and args gate."""
    if anchor_model is None:
        return ()
    out: _PathList = []
    refs: List[object] = [
        *([key.source] if include_source else []),
        *key.args,
        *(v for _, v in key.kwargs),
    ]
    for ref in refs:
        _collect_ref_paths(
            ref,
            anchor_model=anchor_model,
            anchor_relation=anchor_relation,
            bundle=bundle,
            out=out,
        )
    _collect_default_fragment_paths(
        key,
        anchor_model=anchor_model,
        anchor_relation=anchor_relation,
        bundle=bundle,
        out=out,
    )
    return tuple(out)
