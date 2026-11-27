"""A relationship's Ossie orientation: which side is its many side, the ``from`` of an Ossie relationship."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Optional

from slayer.core.enums import RelationshipCardinality

__all__ = ["ossie_from"]

#: ``keyed(dataset, fields)``: whether ``fields`` are a key of ``dataset``.
Keyed = Callable[[str, Sequence[str]], bool]


def ossie_from(
    *, declaring: str, to: str, cardinality: Optional[RelationshipCardinality], declaring_keys: Sequence[str],
    to_keys: Sequence[str], keyed: Keyed, document_from: Optional[str] = None,
) -> Optional[str]:
    """The many side of a relationship declared on ``declaring`` to ``to``, as Ossie export orients it: a document's
    own ``from``, else the declaring side of a many-to-one or one-to-one relationship, the target of a one-to-many
    one, else the side whose keys the other side's keys are; ``None`` for many-to-many or undeterminable."""
    if document_from is not None:
        return document_from
    if cardinality in (RelationshipCardinality.MANY_TO_ONE, RelationshipCardinality.ONE_TO_ONE):
        return declaring
    if cardinality is RelationshipCardinality.ONE_TO_MANY:
        return to
    if cardinality is RelationshipCardinality.MANY_TO_MANY:
        return None
    if keyed(to, to_keys):
        return declaring
    if keyed(declaring, declaring_keys):
        return to
    return None
