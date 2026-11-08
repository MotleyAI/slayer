"""Row supply: which rows a query has."""

from typing import TYPE_CHECKING

from slayer.core.deprecation import raise_moved

#: Public names of the 1.x ``slayer.engine.population`` module, now in ``inference``.
_MOVED = frozenset({
    "PopulationChoice", "determination_items", "probe_item", "determined_path", "ambiguity_resolved_path",
    "item_blockers", "CandidateVerdict", "evaluate_candidates", "select_from_verdicts", "spine_factor_out",
    "infer_spine_population", "spine_datasource", "infer_population",
})

if not TYPE_CHECKING:  # stale imports of a moved name stay static errors

    def __getattr__(name: str) -> object:
        if name in _MOVED:
            raise_moved(f"slayer.engine.population.{name}", "slayer.engine.population.inference", removal="3.0")
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
