"""Typed terms of the query algebra (DEV-1871, design D1–D3).

Terms annotate keys, never replace them: each adds only what the key lacks —
the resolved home dataset and the resolved total grain — and ``Aggregate.recipe``
is a reference to the interned ``AggregateKey``. ``Dataset`` is the
aggregate-is-a-dataset axiom as subtyping; it describes an input relation and
never renders one (``slayer/sql`` keeps sole Node-building authority).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Dict, Literal, Protocol, Tuple, Union, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field as PydanticField, model_validator

from slayer.core.keys import AggregateKey, Grain, TransformKey, ValueKey, is_reaggregation_key, source_anchor_path
import slayer.core.enums


@runtime_checkable
class Dataset(Protocol):
    """A relation a compiler may take as a producer's input: identified and
    described by ``kind``, never self-rendering."""

    @property
    def kind(self) -> str: ...


class _Term(BaseModel, frozen=True):
    pass


class StoredDataset(_Term, frozen=True):
    """Backed by a stored dataset, identified by (datasource, dataset name)."""

    data_source: str
    dataset_name: str

    @property
    def kind(self) -> str:
        return "dataset"


class StageDataset(_Term, frozen=True):
    """Stage-backed dataset (a prior pipeline stage's output), by stage name."""

    stage_name: str

    @property
    def kind(self) -> str:
        return "stage"


#: Where an aggregation is computed: the host, a dataset below it, its operand dataset (second-order), or nowhere
#: resolved (no dataset determines its inputs, or the query has no dataset host).
HomeKind = Literal["host", "physical_target", "operand_dataset", "unresolved"]


class AggregateHome(BaseModel, frozen=True):
    """An aggregation's home (semantics Axiom 2): its kind and path relative to the host."""

    kind: HomeKind
    path: Tuple[str, ...] = ()


class AggregateHomes(BaseModel, frozen=True):
    """Every aggregate's home in one elaborated environment."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    by_key: Dict[AggregateKey, AggregateHome] = PydanticField(default_factory=dict)

    def of(self, key: AggregateKey) -> AggregateHome:
        home = self.by_key.get(key)
        if home is None:
            raise RuntimeError(f"Aggregate {key!r} has no resolved home in this environment.")
        return home

    def target_rooted(self, key: ValueKey) -> bool:
        """``key`` is computed by a target-rooted producer: at its home below the host; any other home keeps its
        source anchor's routing (a host home's producer roots at the host, an unresolved one's input check names
        the offending hop)."""
        if not isinstance(key, AggregateKey) or key.locus == "host" or is_reaggregation_key(key):
            return False
        return self.of(key).kind == "physical_target" or bool(source_anchor_path(key.source))

    def carried(self, rebuilt: Mapping[ValueKey, ValueKey]) -> "AggregateHomes":
        """These homes, each rebuilt aggregate keeping its original's."""
        extra = {
            new: self.of(old) for old, new in rebuilt.items()
            if isinstance(old, AggregateKey) and isinstance(new, AggregateKey)
        }
        return self.model_copy(update={"by_key": {**self.by_key, **extra}})


#: Concrete datasets an ``Aggregate`` may aggregate over, or a ``Field`` live on.
DatasetT = Union[StoredDataset, StageDataset, "Aggregate"]


class Aggregate(_Term, frozen=True):
    """An aggregation of ``home`` at a total ``grain`` — itself a ``Dataset``.

    ``home_path`` is the resolved home dataset (Axiom 2) as a relationship path relative
    to the environment's host: the deepest dataset that determines every input;
    ``()`` is the host itself; ``home_kind`` classifies it. Resolved once in the elaborator
    (``engine/population/home``).
    """

    home: DatasetT
    recipe: AggregateKey
    grain: Grain
    home_path: Tuple[str, ...] = ()
    home_kind: HomeKind = "host"

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
        if self.recipe.op in slayer.core.enums.TIME_TRANSFORMS and self.recipe.time_key is None:
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
