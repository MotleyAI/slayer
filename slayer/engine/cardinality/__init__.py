"""Join cardinality and association."""

from typing import TYPE_CHECKING

from slayer.core.deprecation import raise_moved

#: Public names of the 1.x ``slayer.engine.cardinality`` module, now in ``inference``.
_MOVED = frozenset({
    "declares_solo_unique", "classify_cardinality", "infer_structural_cardinality", "CardinalityVerdict",
    "SideStats", "JoinCardinalityFinding", "JoinCardinalityReport", "compute_verdict",
})

if not TYPE_CHECKING:  # stale imports of a moved name stay static errors

    def __getattr__(name: str) -> object:
        if name in _MOVED:
            raise_moved(f"slayer.engine.cardinality.{name}", "slayer.engine.cardinality.inference", removal="3.0")
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
