"""Ranking-key resolution for the ``first`` / ``last`` route (P-C / P-D).

A ranked aggregate needs its own ROW ORDERING, so under P-C it compiles to a
ranked-kernel producer (DEV-1838 D4) rooted where its rows live and joined back
on the query grain. The one decision this module owns is WHICH column the
ranking runs over — settled at plan time so the renderer never re-derives it.

The ranking column used to be resolved at render time, twice: once for the host
base's ranked wrap and once, by a different precedence, inside the cross-model
CTE. They agreed by accident on the cases anyone had tried. One resolver with
an explicit per-scope precedence replaces both:

* **host-rooted** — an explicit positional time arg, else the first
  ``DATE``/``TIMESTAMP`` row dimension, else the first time dimension's RAW
  column (never the truncated bucket: ranking within a month by the month ties
  every row in it), else the model's ``default_time_dimension``.
* **target-rooted** — the same explicit arg, re-anchored in the target's
  coordinates, else the TARGET model's ``default_time_dimension``. Host
  dimensions are deliberately not candidates: the CTE ranks target rows, and a
  host column is not in scope there.

Both ends of the precedence raise rather than fall through, with the message the
scope's users already see.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

from slayer.core.enums import DataType
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    TimeTruncKey,
    ValueKey,
    reroot_value_key,
)
from slayer.core.models import SlayerModel
from slayer.engine.planned import (
    SlotId,
    ValueSlot,
)
from slayer.engine.source_bundle import ResolvedSourceBundle

__all__ = [
    "RANKED_AGGREGATIONS",
    "explicit_ranking_time_arg",
    "ordered_row_keys",
    "resolve_ranking_time_key",
]

#: The aggregations that rank. Named once so the classifier, the planner and
#: the renderer cannot drift on what "a ranked aggregate" is.
RANKED_AGGREGATIONS = ("first", "last")

_TEMPORAL_TYPES = (DataType.DATE, DataType.TIMESTAMP)


def explicit_ranking_time_arg(key: AggregateKey) -> Optional[ValueKey]:
    """The explicit positional ranking-time arg of a ``first`` / ``last``, or
    ``None``.

    The FIRST positional arg iff it is a column ref; ``None`` for anything else
    — first/last never takes a leading non-column positional, so a different
    shape means the caller passed something this aggregation does not read.
    """
    if key.agg not in RANKED_AGGREGATIONS:
        return None
    for arg in key.args:
        return arg if isinstance(arg, (ColumnKey, ColumnSqlKey)) else None
    return None


def _ranking_key_name(key: ValueKey) -> str:
    """The user-facing name of a ranking-time key, for an error message."""
    if isinstance(key, ColumnKey):
        return ".".join((*key.path, key.leaf))
    if isinstance(key, ColumnSqlKey):
        return ".".join((*key.path, key.column_name))
    return type(key).__name__


def _resolves_on(*, key: ValueKey, model: SlayerModel) -> bool:
    """Whether ``key`` names something reachable FROM ``model``.

    A shallow check on purpose: a local leaf must be a column of the model, and
    a path-bearing one must start at a model this one joins to. Walking the
    whole chain is the renderer's job and it raises its own errors; this exists
    so the common mistake — naming a HOST column as a TARGET-rooted ranking key
    — is caught where the plan is made rather than at the database.
    """
    if isinstance(key, ColumnKey):
        leaf, path = key.leaf, key.path
    elif isinstance(key, ColumnSqlKey):
        leaf, path = key.column_name, key.path
    else:
        return True
    if path:
        return any(j.target_model == path[0] for j in (model.joins or []))
    return any(c.name == leaf for c in model.columns)


def _temporal_row_dimension_key(
    *,
    row_keys: Sequence[ValueKey],
    source_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> Optional[ValueKey]:
    """The first row dimension whose declared type is ``DATE``/``TIMESTAMP``."""
    for key in row_keys:
        if not isinstance(key, ColumnKey):
            continue
        model: Optional[SlayerModel] = source_model
        for hop in key.path:
            model = bundle.get_referenced_model(hop)
            if model is None:
                break
        if model is None:
            continue
        col = next((c for c in model.columns if c.name == key.leaf), None)
        if col is not None and col.type in _TEMPORAL_TYPES:
            return key
    return None


def _time_dimension_raw_column(
    *, row_keys: Sequence[ValueKey],
) -> Optional[ValueKey]:
    """The first time dimension's RAW column, un-truncated."""
    for key in row_keys:
        if isinstance(key, TimeTruncKey):
            return key.column
    return None


def resolve_ranking_time_key(
    *,
    key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    row_keys: Sequence[ValueKey] = (),
    target_path: Tuple[str, ...] = (),
) -> ValueKey:
    """The column a ranked aggregate's ``ROW_NUMBER`` orders by, in the RANKED
    scope's coordinates.

    ``row_keys`` are the host's row-dimension keys in render order; they are
    candidates only for a HOST-rooted plan (``target_path`` empty). A
    target-rooted CTE ranks the target's rows, so a host dimension is not in
    scope there and the precedence goes straight from the explicit arg to the
    target's own default.
    """
    arg = explicit_ranking_time_arg(key)
    if arg is not None:
        # The arg arrives in the HOST's coordinates; a target-rooted plan needs
        # it in the target's, which is the same re-anchoring the aggregate
        # source itself undergoes — done in lockstep so the ORDER BY and the
        # value cannot end up rooted at different relations.
        rerooted = reroot_value_key(arg, target_path=target_path)
        if target_path and not _resolves_on(key=rerooted, model=root_model):
            # A HOST column as the ranking key of a TARGET-rooted CTE. The rows
            # being ranked are the target's, and a host column is not one of
            # their attributes — the relationship runs the other way, usually
            # one-to-many, so there is no single host value per target row to
            # rank by. This used to emit ``ORDER BY <target>.<host column>``,
            # a reference to a column that does not exist, and fail at the
            # database with no indication of which measure caused it.
            raise ValueError(
                f"first/last ranking column "
                f"{_ranking_key_name(rerooted)!r} is not resolvable on model "
                f"{root_model.name!r}, where a cross-model first/last ranks "
                f"its rows. Name a column of {root_model.name!r} (or one it "
                f"joins to), or drop the argument to rank by its "
                f"default_time_dimension."
            )
        return rerooted

    if not target_path:
        temporal = _temporal_row_dimension_key(
            row_keys=row_keys, source_model=root_model, bundle=bundle,
        )
        if temporal is not None:
            return temporal
        raw = _time_dimension_raw_column(row_keys=row_keys)
        if raw is not None:
            return raw
        if root_model.default_time_dimension:
            return ColumnKey(path=(), leaf=root_model.default_time_dimension)
        raise ValueError(
            "first/last aggregation requires a ranking time column "
            "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
            "model's default_time_dimension); none is resolvable for "
            f"model {root_model.name!r}."
        )

    if root_model.default_time_dimension:
        return ColumnKey(path=(), leaf=root_model.default_time_dimension)
    raise ValueError(
        f"first/last aggregation requires a ranking time column "
        f"(an explicit positional time arg, or the target "
        f"model's default_time_dimension); none is resolvable "
        f"for cross-model aggregate on target "
        f"{root_model.name!r}."
    )


def ordered_row_keys(
    *, row_slots: Sequence[ValueSlot], public_projection: Sequence[SlotId],
) -> List[ValueKey]:
    """Row-dimension keys in the order the base SELECT renders them.

    Publicly projected slots first, in projection order, then the rest. That is
    the order the superseded render-time resolver walked, and the ranking-column
    precedence is order-sensitive — two temporal dimensions rank by whichever
    comes first.
    """
    by_id = {s.id: s for s in row_slots}
    seen: set = set()
    ordered: List[ValueKey] = []
    for sid in public_projection:
        slot = by_id.get(sid)
        if slot is not None and sid not in seen:
            seen.add(sid)
            ordered.append(slot.key)
    for slot in row_slots:
        if slot.id not in seen:
            seen.add(slot.id)
            ordered.append(slot.key)
    return ordered




