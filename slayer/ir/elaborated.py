"""The elaborator's output: a typing environment over the bound query (D2).

Not a parallel tree — every top-level expression (measure, dimension, filter
conjunct, order target) gets one ``ExpressionEntry`` (position verdict, home,
resolved grain, broadcast insertions at grain-union points), and every
aggregate/transform occurrence gets one term, memoized by key identity in
``terms``. Compilers read semantic decisions here; identity walks stay on keys.
"""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field as PydanticField

from slayer.core.keys import Grain, ValueKey
from slayer.ir.terms import Aggregate, Broadcast, DatasetT, Transform

PositionVerdict = Literal["field", "measure"]

Term = Union[Aggregate, Transform]


class ExpressionEntry(BaseModel):
    """One top-level expression's typing: what it is, where it lives, at what grain."""

    model_config = ConfigDict(frozen=True)

    verdict: PositionVerdict
    home: Optional[DatasetT] = None
    grain: Grain = PydanticField(default_factory=lambda: Grain.EMPTY)
    broadcasts: Tuple[Broadcast, ...] = ()


class ElaborationSource(BaseModel):
    """Opaque handles to what was elaborated, so a compiler can consume the
    environment without re-deriving its inputs. Transitional: dies when the
    orchestrator passes compile inputs explicitly (DEV-1871 group 16)."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    query: Any = None
    bundle: Any = None
    scope: Any = None
    stage_schemas: Any = None
    prebound: Any = None


class ElaboratedQuery(BaseModel):
    """The typing environment for one bound query stage.

    Equality is semantic — the ``source`` carrier is excluded.
    """

    model_config = ConfigDict(frozen=True)

    dimensions: Tuple[ExpressionEntry, ...] = ()
    measures: Tuple[ExpressionEntry, ...] = ()
    filters: Tuple[ExpressionEntry, ...] = ()
    order: Tuple[ExpressionEntry, ...] = ()
    terms: Dict[ValueKey, Term] = PydanticField(default_factory=dict)
    source: Optional[ElaborationSource] = PydanticField(default=None, repr=False)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ElaboratedQuery):
            return NotImplemented
        return (
            self.dimensions == other.dimensions
            and self.measures == other.measures
            and self.filters == other.filters
            and self.order == other.order
            and self.terms == other.terms
        )

    __hash__ = None  # type: ignore[assignment] — dict-valued field
