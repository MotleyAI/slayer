"""Typed terms of the query algebra (DEV-1871, design D1–D3).

Terms annotate keys, never replace them: each adds only what the key lacks —
the resolved home dataset and the resolved total grain — and ``Aggregate.recipe``
is a reference to the interned ``AggregateKey``. ``Dataset`` is the
aggregate-is-a-dataset axiom as subtyping; it describes an input relation and
never renders one (``slayer/sql`` keeps sole Node-building authority).
"""

from __future__ import annotations

from typing import Protocol, Tuple, Union, runtime_checkable

from pydantic import BaseModel, model_validator

from slayer.core.formula import TIME_TRANSFORMS
from slayer.core.keys import AggregateKey, Grain, TransformKey, ValueKey


@runtime_checkable
class Dataset(Protocol):
    """A relation a compiler may take as a producer's input: identified and
    described by ``kind``, never self-rendering."""

    @property
    def kind(self) -> str: ...


class _Term(BaseModel, frozen=True):
    pass


class ModelDataset(_Term, frozen=True):
    """Model-backed dataset, identified by (datasource, model name)."""

    data_source: str
    model_name: str

    @property
    def kind(self) -> str:
        return "model"


class StageDataset(_Term, frozen=True):
    """Stage-backed dataset (a prior pipeline stage's output), by stage name."""

    stage_name: str

    @property
    def kind(self) -> str:
        return "stage"


#: Concrete datasets an ``Aggregate`` may aggregate over, or a ``Field`` live on.
DatasetT = Union[ModelDataset, StageDataset, "Aggregate"]


class Aggregate(_Term, frozen=True):
    """An aggregation of ``home`` at a total ``grain`` — itself a ``Dataset``.

    ``home_path`` is the resolved home dataset (Axiom 2) as a join path relative
    to the environment's host: the deepest dataset that determines every input;
    ``()`` is the host itself. Resolved once in the elaborator (``engine/home``).
    """

    home: DatasetT
    recipe: AggregateKey
    grain: Grain
    home_path: Tuple[str, ...] = ()

    @property
    def kind(self) -> str:
        return "aggregate"


class Transform(_Term, frozen=True):
    """A window/temporal operator over an ``Aggregate``; grain-preserving.

    A time-ordered op (``TIME_TRANSFORMS``) must carry its axis at construction.
    """

    input: Aggregate
    recipe: TransformKey

    @model_validator(mode="after")
    def _require_time_axis(self) -> "Transform":
        if self.recipe.op in TIME_TRANSFORMS and self.recipe.time_key is None:
            raise ValueError(
                f"Transform {self.recipe.op!r} requires a time axis "
                f"(recipe.time_key is unset).",
            )
        return self

    @property
    def grain(self) -> Grain:
        return self.input.grain


class Broadcast(_Term, frozen=True):
    """Explicit coarse→fine coercion of ``source`` into the finer grain ``into``;
    the reverse direction needs a re-aggregation and is rejected."""

    source: Aggregate
    into: Grain

    @model_validator(mode="after")
    def _require_broadcast_direction(self) -> "Broadcast":
        if not self.source.grain.broadcasts_into(self.into):
            raise ValueError(
                f"Broadcast requires the source grain to be a subgrain of the "
                f"target: {sorted(map(repr, self.source.grain))} does not "
                f"broadcast into {sorted(map(repr, self.into))}.",
            )
        return self

    @property
    def grain(self) -> Grain:
        return self.into


class Field(_Term, frozen=True):
    """A row-level value on a dataset: the key, with its home made explicit."""

    home: DatasetT
    key: ValueKey


Aggregate.model_rebuild()
Field.model_rebuild()
