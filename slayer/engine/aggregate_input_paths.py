"""DEV-1709 (Stage 5) — plan-time crossing-input discovery for aggregates.

The widened Law-3 trigger isolates a LOCAL aggregate into a host-rooted CTE
when ANY of its explicit inputs crosses a join. This module answers "which
join paths do the aggregate's inputs cross?" for every input kind:

* **source** — a structural ``source.path`` contributes as-is; a derived
  ``ColumnSqlKey`` with ``path == ()`` has its ``Column.sql`` expanded and
  scanned with the shared Law-1 scanner (via
  ``compute_column_filter_join_paths``, the same parse → expand → walk
  pipeline the ``Column.filter`` trigger half uses).
* **positional args** (covers the explicit first/last time arg) — same
  structural + derived-sql treatment.
* **kwargs** — column-valued kwargs same as args; template-fragment STRING
  kwargs (user-supplied values for custom-aggregation params) are parsed
  with the dialect-fallback chain and scanned. Model-default
  ``AggregationParam.sql`` fragments of the custom aggregation named by
  ``key.agg`` are scanned too — but only for params NOT overridden by a
  user kwarg (an overridden default never renders).
* **``column_filter_key`` is deliberately NOT re-scanned** — the trigger
  reads its bind-time ``SqlExprKey.referenced_join_paths`` directly
  (DEV-1503, unchanged).

Defensive fallbacks mirror ``column_filter_paths.py``: an unparseable
fragment contributes nothing (parity with the ``Column.filter`` scan —
pre-Stage-5 behavior is preserved for fragments the dialect fallback chain
cannot parse; a documented D1 carve-out, not an endorsement), and scalar /
duration / literal kwarg values contribute nothing.
"""

from __future__ import annotations

from typing import List, Optional, Tuple, Union

from slayer.core.keys import AggregateKey, ColumnKey, ColumnSqlKey, StarKey
from slayer.core.models import SlayerModel
from slayer.engine.column_filter_paths import compute_column_filter_join_paths
from slayer.engine.source_bundle import ResolvedSourceBundle

_PathList = List[Tuple[str, ...]]
_StructuralRef = Union[ColumnKey, ColumnSqlKey, StarKey]


def _add_path_prefixes(path: Tuple[str, ...], out: _PathList) -> None:
    """Emit every prefix of ``path`` once (``("a", "b")`` → ``("a",)`` AND
    ``("a", "b")``) — same prefix semantics as the Law-1 scanner."""
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
    """Scan a free-SQL fragment (derived ``Column.sql`` or a template
    fragment) for crossed join paths, reusing the filter-side pipeline
    (dialect-fallback parse → anchor-derived expansion → root-scope walk).
    Unparseable fragments contribute nothing."""
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
    """Crossed paths of one embedded reference (source / arg / kwarg value).

    Scalars (Decimal / int / float / None) contribute nothing; strings are
    template fragments and get the free-SQL scan.
    """
    if isinstance(ref, (ColumnKey, StarKey)):
        _add_path_prefixes(tuple(getattr(ref, "path", ()) or ()), out)
        return
    if isinstance(ref, ColumnSqlKey):
        if ref.path:
            # Structural crossing; any FURTHER crossing inside the target's
            # own Column.sql is the target-rooted CTE's concern (Stage 4).
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
    """Scan the model-default ``AggregationParam.sql`` fragments of the
    custom aggregation named by ``key.agg`` — skipping params a user kwarg
    overrides (the default never renders for those)."""
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
    """Ordered, de-duplicated tuple of join-path prefixes crossed by the
    aggregate's explicit inputs (source, positional args, kwargs, and
    non-overridden custom-aggregation default fragments).

    ``()`` for a purely-local aggregate. ``column_filter_key`` crossing is
    intentionally excluded — read ``referenced_join_paths`` on the key.
    ``include_source=False`` drops the SOURCE's own crossings (DEV-1838 D5:
    a source that reads through a join consumes the target's values
    per-match, which is legal; only filter references and arguments gate).
    """
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
