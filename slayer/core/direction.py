"""Ordering direction: the shared synonym table and the rank-family ``direction=`` rule."""

from typing import Any

from slayer.core.errors import TransformArgumentError

DIRECTION_NORMALIZE = {
    "asc": "asc",
    "ascending": "asc",
    "desc": "desc",
    "descending": "desc",
}

#: Rank-family transforms whose ordering direction is a required ``direction=`` keyword.
DIRECTED_RANK_TRANSFORMS = frozenset({"rank", "dense_rank"})
#: Rank-family transforms that always order ascending and take no ``direction=``.
ASCENDING_RANK_TRANSFORMS = frozenset({"ntile", "percent_rank"})

_ACCEPTED = "asc, desc, ascending or descending (any case)"


def normalize_direction(value: Any) -> str | None:
    """``asc`` / ``desc`` for a recognised direction word, else ``None``."""
    if not isinstance(value, str):
        return None
    return DIRECTION_NORMALIZE.get(value.strip().lower())


def with_direction_kwarg(*, op: str, accepted: frozenset[str]) -> frozenset[str]:
    """``accepted`` plus ``direction`` when ``op`` takes one, for error listings."""
    return accepted | {"direction"} if op in DIRECTED_RANK_TRANSFORMS else accepted


def rank_direction(*, op: str, given: bool, value: Any = None) -> str | None:
    """Validate a rank-family call's ``direction=``; ``value`` is the literal, or any non-``str`` when not a string literal."""
    if op in ASCENDING_RANK_TRANSFORMS:
        if given:
            raise TransformArgumentError(
                summary=f"Transform '{op}' always orders ascending and takes no direction= keyword.",
                suggestion=f"drop direction=; {op} gives the lowest value the lowest result.",
            )
        return None
    if op not in DIRECTED_RANK_TRANSFORMS:
        return None
    if not given:
        raise TransformArgumentError(
            summary=f"Transform '{op}' needs an ordering direction.",
            suggestion=(
                f"pass direction='asc' (lowest first) or direction='desc' (highest first), "
                f"e.g. {op}(sum(amount), direction='desc')."
            ),
        )
    normalized = normalize_direction(value)
    if normalized is None:
        got = repr(value) if isinstance(value, str) else "a value that is not a string literal"
        raise TransformArgumentError(
            summary=f"Transform '{op}' direction must be {_ACCEPTED}; got {got}.",
            suggestion=f"write {op}(..., direction='asc') or {op}(..., direction='desc').",
        )
    return normalized
