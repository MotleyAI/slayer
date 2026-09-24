"""The elaborator's output: a typing environment over the bound query (D2).

Not a parallel tree — every top-level expression (measure, dimension, filter
conjunct, order target) gets one ``ExpressionEntry`` (position verdict, home,
resolved grain, broadcast insertions at grain-union points), and every
aggregate/transform occurrence gets one term, memoized by key identity in
``terms``. Compilers read semantic decisions here; identity walks stay on keys.
"""

from __future__ import annotations

from typing import Dict, Literal, NamedTuple, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field as PydanticField

from slayer.core.keys import Grain, ValueKey
from slayer.core.query import SlayerQuery
from slayer.core.scope import ModelScope, StageSchema
from slayer.ir.planned import MaskTyping
from slayer.ir.prebound import PreboundQuery, StrictQueryCarrier
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.ir.terms import Aggregate, Broadcast, DatasetT, Transform

PositionVerdict = Literal["field", "measure"]


class ConjunctTyping(NamedTuple):
    """One position expression's typing: field/measure + stratum (0 = base-row population)."""

    typing: MaskTyping
    stratum: int

Term = Union[Aggregate, Transform]


class ExpressionEntry(BaseModel):
    """One top-level expression's typing: what it is, where it lives, at what grain."""

    model_config = ConfigDict(frozen=True)

    verdict: PositionVerdict
    home: Optional[DatasetT] = None
    grain: Grain = PydanticField(default_factory=lambda: Grain.EMPTY)
    broadcasts: Tuple[Broadcast, ...] = ()


class ElaboratedQuery(BaseModel):
    """The typing environment for one bound query: an entry per top-level
    expression and a term per aggregate / transform occurrence.

    Equality is semantic — only the typing surface participates.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    dimensions: Tuple[ExpressionEntry, ...] = ()
    measures: Tuple[ExpressionEntry, ...] = ()
    filters: Tuple[ExpressionEntry, ...] = ()
    order: Tuple[ExpressionEntry, ...] = ()
    terms: Dict[ValueKey, Term] = PydanticField(default_factory=dict)

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


class _CompileInputs(ElaboratedQuery):
    """The typed bound query an environment types — the compiler's inputs."""

    scope: Union[ModelScope, StageSchema] = PydanticField(repr=False)
    bundle: ResolvedSourceBundle = PydanticField(repr=False)
    stage_schemas: Dict[str, StageSchema] = PydanticField(
        default_factory=dict, repr=False,
    )
    prebound: PreboundQuery = PydanticField(repr=False)
    filter_typings: Tuple[ConjunctTyping, ...] = PydanticField(default=(), repr=False)


class ElaboratedStage(_CompileInputs):
    """One user-authored query stage's environment."""

    query: Optional[SlayerQuery] = PydanticField(default=None, repr=False)


class ElaboratedProducer(_CompileInputs):
    """A compiler-synthesized producer's environment."""

    query: StrictQueryCarrier = PydanticField(repr=False)
