"""The grain of an aggregate — its dimension-key set — as a first-class value type.

Grains form a lattice under inclusion: the union grain is the join, a coarser grain
is a subgrain of a finer one, and broadcast is the coarse->fine coercion. ``Grain``
gives that reasoning one home instead of re-deriving it over raw ``frozenset``s.

Convention: set-like dunders (``|`` / ``-`` / ``<=`` / ``<`` / ``in`` / ``len`` /
``iter``) carry the mechanical set algebra; the named predicates (``union`` /
``is_subgrain_of`` / ``is_strict_subgrain_of`` / ``broadcasts_into``) spell out the
lattice reading and are preferred at admission sites where direction matters.

Operand contract: comparisons and ``union`` accept a ``Grain`` only; ``__or__`` /
``__sub__`` also accept any ``AbstractSet[ValueKey]``; every set-producing op returns a
``Grain``. Anything else yields ``NotImplemented`` (so ``grain <= frozenset(...)``
raises ``TypeError``), and ``__eq__`` is ``Grain``-only — a ``Grain`` never equals a raw
frozenset, so mixed-representation drift fails loudly instead of surviving silently.
"""

from __future__ import annotations

from typing import AbstractSet, ClassVar, Iterable, Iterator

from pydantic import BaseModel, ConfigDict

from slayer.core.keys import ValueKey


class Grain(BaseModel):
    """A frozen, hashable dimension-key set with lattice operations."""

    model_config = ConfigDict(frozen=True)

    keys: frozenset[ValueKey] = frozenset()

    EMPTY: ClassVar["Grain"]

    @classmethod
    def of(cls, keys: Iterable[ValueKey]) -> "Grain":
        return cls(keys=frozenset(keys))

    @property
    def is_empty(self) -> bool:
        return not self.keys

    def union(self, other: "Grain") -> "Grain":
        """The join: the grain containing every key of both."""
        return Grain(keys=self.keys | other.keys)

    def is_subgrain_of(self, other: "Grain") -> bool:
        """``self`` is coarser than or equal to ``other`` (reflexive)."""
        return self.keys <= other.keys

    def is_strict_subgrain_of(self, other: "Grain") -> bool:
        """``self`` is strictly coarser than ``other`` (irreflexive)."""
        return self.keys < other.keys

    def broadcasts_into(self, finer: "Grain") -> bool:
        """A coarse value coerces up to ``finer`` iff ``self`` is a subgrain of it.

        The reverse (fine->coarse) is not a broadcast; it needs a second-order
        aggregation — accumulate within the coarser grain first, then combine.
        """
        return self.is_subgrain_of(finer)

    def __contains__(self, key: object) -> bool:
        return key in self.keys

    def __iter__(self) -> Iterator[ValueKey]:  # type: ignore[override]
        return iter(self.keys)

    def __len__(self) -> int:
        return len(self.keys)

    def __bool__(self) -> bool:
        return bool(self.keys)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys == other.keys
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.keys)

    def __le__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys <= other.keys
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys < other.keys
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys >= other.keys
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys > other.keys
        return NotImplemented

    def __or__(self, other: object) -> "Grain":
        if isinstance(other, Grain):
            return Grain(keys=self.keys | other.keys)
        if isinstance(other, AbstractSet):
            return Grain(keys=self.keys | frozenset(other))
        return NotImplemented

    def __sub__(self, other: object) -> "Grain":
        if isinstance(other, Grain):
            return Grain(keys=self.keys - other.keys)
        if isinstance(other, AbstractSet):
            return Grain(keys=self.keys - frozenset(other))
        return NotImplemented


Grain.EMPTY = Grain(keys=frozenset())
