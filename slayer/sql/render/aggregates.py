"""One registry table for aggregation rendering.

``_build_agg`` reached its builders five different ways, so adding an
aggregation meant knowing which one to touch. Here each is one
:class:`AggEntry`: ``dispatch`` names the mechanism that renders it (the
generator still owns the builders, which need model columns and dialect hooks),
and ``window_class`` replaces a silent ``else AVG`` catch-all.
"""

from __future__ import annotations

from typing import Dict, Literal, Optional, Type

from pydantic import BaseModel, ConfigDict
from sqlglot import exp

from slayer.core.enums import BUILTIN_AGGREGATIONS

# Which mechanism renders an aggregation. Retained as data so the generator's
# dispatch is a table lookup rather than five stacked conditionals.
DISPATCH_SIMPLE = "simple"        # direct sqlglot node: COUNT/SUM/AVG/MIN/MAX
DISPATCH_RANKED = "ranked"        # first/last — needs the ranked-subquery state
DISPATCH_STAT = "stat"            # stddev/var/corr/covar — dialect UDF split
DISPATCH_DIALECT_HOOK = "dialect_hook"  # percentile/median/approx-distinct
DISPATCH_DISTINCT = "distinct"    # COUNT(DISTINCT ...)
DISPATCH_FORMULA = "formula"      # {value}/{param} template substitution

# Closed set of dispatch names (mirrors the DISPATCH_* constants above). An
# entry built with a value outside this set fails at import, not at render.
DispatchKind = Literal[
    "simple", "ranked", "stat", "dialect_hook", "distinct", "formula",
]


class AggEntry(BaseModel):
    """How one aggregation renders."""

    model_config = ConfigDict(
        frozen=True, arbitrary_types_allowed=True, extra="forbid",
    )

    name: str
    dispatch: DispatchKind
    # The sqlglot class for the simple path, when there is one.
    node_class: Optional[Type[exp.Expression]] = None
    # Set only for aggregations that can carry their own window frame.
    window_class: Optional[Type[exp.Expression]] = None

    @property
    def windowable(self) -> bool:
        return self.window_class is not None


def _entry(*, name: str, dispatch: DispatchKind, **kw) -> AggEntry:
    return AggEntry(name=name, dispatch=dispatch, **kw)


AGG_REGISTRY: Dict[str, AggEntry] = {
    e.name: e
    for e in (
        # Only sum and avg carry a window frame — the same pair the stage
        # planner gates windowed measures on.
        _entry(name="sum", dispatch=DISPATCH_SIMPLE, node_class=exp.Sum, window_class=exp.Sum),
        _entry(name="avg", dispatch=DISPATCH_SIMPLE, node_class=exp.Avg, window_class=exp.Avg),
        _entry(name="count", dispatch=DISPATCH_SIMPLE, node_class=exp.Count),
        _entry(name="min", dispatch=DISPATCH_SIMPLE, node_class=exp.Min),
        _entry(name="max", dispatch=DISPATCH_SIMPLE, node_class=exp.Max),
        _entry(name="count_distinct", dispatch=DISPATCH_DISTINCT, node_class=exp.Count),
        _entry(name="count_distinct_approx", dispatch=DISPATCH_DIALECT_HOOK),
        _entry(name="first", dispatch=DISPATCH_RANKED),
        _entry(name="last", dispatch=DISPATCH_RANKED),
        _entry(name="median", dispatch=DISPATCH_DIALECT_HOOK),
        _entry(name="percentile", dispatch=DISPATCH_DIALECT_HOOK),
        _entry(name="weighted_avg", dispatch=DISPATCH_FORMULA),
        _entry(name="stddev_samp", dispatch=DISPATCH_STAT),
        _entry(name="stddev_pop", dispatch=DISPATCH_STAT),
        _entry(name="var_samp", dispatch=DISPATCH_STAT),
        _entry(name="var_pop", dispatch=DISPATCH_STAT),
        _entry(name="corr", dispatch=DISPATCH_STAT),
        _entry(name="covar_samp", dispatch=DISPATCH_STAT),
        _entry(name="covar_pop", dispatch=DISPATCH_STAT),
    )
}

# Every built-in must be in the table, or a lookup would fall through to the
# custom-formula path and render a built-in as if it were user-defined.
# Checked BOTH ways: a missing built-in would fall through to the custom-formula
# path, and a registry key that is NOT a built-in (a typo such as sumn) would
# make is_builtin_agg accept it and route it away from that path.
_registered = set(AGG_REGISTRY)
_missing = set(BUILTIN_AGGREGATIONS) - _registered
_unknown = _registered - set(BUILTIN_AGGREGATIONS)
if _missing or _unknown:  # pragma: no cover — import-time invariant
    raise RuntimeError(
        f"Aggregation registry disagrees with BUILTIN_AGGREGATIONS: "
        f"missing={sorted(_missing)}, unknown={sorted(_unknown)}",
    )


def resolve_agg_entry(name: str) -> AggEntry:
    """Return the registry entry for a BUILT-IN aggregation.

    Raises ``ValueError`` for anything else. Custom model-level aggregations
    are deliberately not registered: they carry their own ``formula`` and take
    the template path, so callers check :func:`is_builtin_agg` first.
    """
    entry = AGG_REGISTRY.get(name)
    if entry is None:
        raise ValueError(
            f"Unknown aggregation {name!r}. Built-ins: "
            f"{sorted(AGG_REGISTRY)}; anything else must be defined as a "
            f"model-level aggregation with a formula.",
        )
    return entry


def is_builtin_agg(name: str) -> bool:
    return name in AGG_REGISTRY


def window_agg_class(name: str) -> Type[exp.Expression]:
    """The sqlglot class for a WINDOWED aggregate.

    Raises ``ValueError`` when the aggregation cannot carry a window frame.
    The windowed render path previously read ``exp.Sum if agg == "sum" else
    exp.Avg``, silently rendering every other aggregation as AVG.
    """
    entry = resolve_agg_entry(name)
    if entry.window_class is None:
        raise ValueError(
            f"Aggregation {name!r} cannot be windowed; only "
            f"{sorted(n for n, e in AGG_REGISTRY.items() if e.windowable)} "
            f"carry their own window frame.",
        )
    return entry.window_class
