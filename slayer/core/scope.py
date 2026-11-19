"""Typed scope and stage-schema for the new pipeline.

``DatasetScope`` is the one binding scope: dotted refs walk the relationship graph
rooted at ``population``; a stage is read through the dataset its ``StageSchema`` defines.

``StageField`` is the typed projection element (P6): explicit ``name``
(downstream bind name), ``sql_alias`` (emitted SQL identifier),
``public_alias`` (result-key piece), plus the per-field metadata that
downstream stages need.

``DatasetScope.population`` is ``Optional`` from day one so a future anchor-less
mode is a type-additive change. The binder asserts ``population is not None``
at use sites — the type-level optionality is the extension point.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, TypeVar

from pydantic import BaseModel, ConfigDict, model_validator

from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.errors import AmbiguousReferenceError, UnknownReferenceError
from slayer.core.format import NumberFormat
from slayer.core.granularity import CustomGranularity, GranularitySpec
from slayer.core.models import DatasetField, DatasetRelationship, RelationshipOrigin, SlayerDataset
from slayer.core.names import match_name
from slayer.core.relationship_walker import OrientedRelationship

_T = TypeVar("_T")


class PassThrough(BaseModel):
    """What an untransformed row-level stage output reads: ``field`` at the end of ``path`` (its hops, from the stage's population)."""

    model_config = ConfigDict(frozen=True)

    path: Tuple[OrientedRelationship, ...] = ()
    field: str


class InheritedEdge(BaseModel):
    """An edge a stage inherits on pass-through keys, oriented from the stage; ``to`` is the target's identity."""

    model_config = ConfigDict(frozen=True)

    to: str
    from_columns: Tuple[str, ...]
    to_columns: Tuple[str, ...]
    name: Optional[str] = None
    join_type: JoinType = JoinType.LEFT
    cardinality: Optional[RelationshipCardinality] = None
    origin: RelationshipOrigin

    def as_relationship(self) -> DatasetRelationship:
        return DatasetRelationship(
            to=self.to, from_columns=list(self.from_columns), to_columns=list(self.to_columns), name=self.name,
            join_type=self.join_type, cardinality=self.cardinality,
        ).as_inherited(self.origin)


class StageField(BaseModel):
    """Typed projection element for one stage (P6).

    ``name`` is the downstream bind name — flat (e.g.
    ``robot_details__modelseriesval`` or ``rev``). ``sql_alias`` is the
    identifier emitted in the stage's SELECT projection (usually equal
    to ``name``, but the typed split lets the planner reserve hidden
    or alias-bearing forms without coupling them). ``public_alias`` is
    the result-key piece returned to the user — set only for non-hidden
    fields.

    ``format`` is the typed ``NumberFormat``
    inherited from the source ``DatasetMetric`` / ``DatasetField`` or computed
    by ``_infer_aggregated_format``. ``description`` propagates the source
    field's documentation through the typed plan.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    sql_alias: str
    public_alias: Optional[str] = None
    datatype: Optional[DataType] = None
    element_type: Optional[DataType] = None
    # Set only for a field an upstream stage bucketed (a ``TimeTruncKey`` slot);
    # ``None`` means "not truncated", so any granularity re-binds.
    granularity: CustomGranularity | GranularitySpec | None = None
    label: Optional[str] = None
    format: Optional[NumberFormat] = None
    hidden: bool = False
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    sampled: Optional[str] = None
    provenance: Optional[str] = None
    #: ``name`` under each non-canonical spelling of its path (stale-spelling slack).
    respellings: Tuple[str, ...] = ()
    #: Set only for an untransformed public row-level output.
    pass_through: Optional[PassThrough] = None


class StageDisplay(BaseModel):
    """A stage's user-facing spelling: its name in its own query list, plus the
    stored query-backed dataset it was spliced from (``None`` for a user stage)."""

    model_config = ConfigDict(frozen=True)

    name: str
    dataset: Optional[str] = None

    @property
    def label(self) -> str:
        if self.dataset is None:
            return f"stage {self.name!r}"
        if self.name == self.dataset:
            return f"dataset {self.dataset!r}"
        return f"stage {self.name!r} of dataset {self.dataset!r}"


class StageSchema(BaseModel):
    """The typed projection of one query stage (P6); later stages bind against the dataset it defines.

    ``relation_name`` is the stage's internal identity: its CTE name.
    ``display`` is its user-facing spelling (``None`` → ``relation_name``).
    ``sql`` is the emitted text of the stage's SELECT — populated by the
    planner; left ``None`` until rendering. ``grain`` names the fields the
    stage is unique on (its dimension positions); ``None`` for a raw-rows stage.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    relation_name: str
    display: Optional[StageDisplay] = None
    sql: Optional[str] = None
    fields: List[StageField]
    grain: Optional[List[str]] = None
    # A spliced query-backed dataset's final stage keeps its source's default time dimension.
    default_time_dimension: Optional[str] = None
    inherited_edges: Tuple[InheritedEdge, ...] = ()
    # The stage's rows restrict its population: a filter, or a restricted population.
    restricted: bool = False
    # Why inherited edges could not be derived (inspect only); ``None`` when they were.
    inherited_edges_error: Optional[str] = None
    #: Two public fields flattening to one name ``(flat name, first key, second key)``: refused once the stage is read.
    flatten_collision: Optional[Tuple[str, str, str]] = None

    @property
    def display_name(self) -> str:
        return self.display.name if self.display is not None else self.relation_name

    @property
    def label(self) -> str:
        return (self.display or StageDisplay(name=self.relation_name)).label

    @model_validator(mode="after")
    def _grain_names_fields(self) -> "StageSchema":
        names = {c.name for c in self.fields}
        missing = [g for g in self.grain or [] if g not in names]
        if missing:
            raise ValueError(
                f"Stage {self.display_name!r} grain names unknown fields {missing}."
            )
        return self

    def __getitem__(self, name: str) -> StageField:
        for c in self.fields:
            if c.name == name:
                return c
        raise KeyError(
            f"No field named {name!r} in stage {self.display_name!r}."
        )

    def get(self, name: str, *, quoted: bool = False) -> Optional[StageField]:
        found = match_name(name=name, declared=[c.name for c in self.fields], quoted=quoted)
        return None if found is None else next(c for c in self.fields if c.name == found)

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and self.get(name) is not None

    def resolve_flat_name(self, name: str, *, quoted: bool = False) -> Optional[StageField]:
        """Stage-boundary lookup: the name ``match_name`` reaches, else a unique stale respelling (recorded)."""
        return resolve_flat_name(name=name, entries=self._flat_entries(), quoted=quoted)

    def peek_flat_name(self, name: str, *, quoted: bool = False) -> Optional[StageField]:
        """``resolve_flat_name`` that records nothing."""
        return peek_flat_name(name=name, entries=self._flat_entries(), quoted=quoted)

    def _flat_entries(self) -> List[Tuple[str, Tuple[str, ...], StageField]]:
        return [(c.name, c.respellings, c) for c in self.fields]


class DatasetScope(BaseModel):
    """Scope for binding Mode-B refs against a dataset with relationships (P5).

    Dotted refs walk the relationship graph rooted at ``population``;
    ``__``-bearing refs are flat-only and reject unless they exact-match
    a field literally named that way on the dataset.

    I2: ``population`` is ``Optional`` from day one. The binder
    asserts ``population is not None`` at use sites so behavior is
    unchanged. A future anchor-less mode uses ``population=None`` and
    a different binder branch (DatasourceScope-style binding). Keeping
    the type optional avoids a breaking change later.
    """

    population: Optional[SlayerDataset] = None
    #: A query's own scope: each reference is anchored at its first segment (``queries/references``); otherwise
    #: references read from ``population`` (a stored definition's dataset-local mode).
    anchored: bool = False


def root_spelling(scope: DatasetScope) -> Optional[str]:
    """The name a reference writes for the scope's root: its dataset's spelling."""
    return scope.population.spelling if scope.population is not None else None


def anchored_scope(scope: DatasetScope) -> DatasetScope:
    """``scope`` as a query's own scope, its references anchored at their first segment."""
    return scope if scope.anchored else scope.model_copy(update={"anchored": True})


def is_anchored(scope: DatasetScope) -> bool:
    return scope.anchored


def host_dataset_name(scope: DatasetScope) -> str:
    """The host relation's display name for alias-collision reporting."""
    return root_spelling(scope) or "(stage)"


def resolve_generated_field(
    *, dataset: SlayerDataset, name: str, location: Optional[str] = None, quoted: bool = False,
) -> Optional[DatasetField]:
    """``resolve_flat_name`` over ``dataset``'s fields; only generated query-backed
    fields carry respellings, so authored fields stay exact. A stale name
    matching several fields fails closed (never read as a physical column)."""
    col = resolve_flat_name(
        name=name, entries=[(c.name, c.respellings, c) for c in dataset.fields],
        location=location, quoted=quoted,
    )
    if col is None:
        matches = [c.name for c in dataset.fields if name in c.respellings]
        if len(matches) > 1:
            raise UnknownReferenceError(
                name=name, scope_kind="SlayerModel",
                scope_summary=f"dataset {dataset.name!r}: stale spelling of {matches}",
            )
    return col


def resolve_flat_name(
    *, name: str, entries: Sequence[Tuple[str, Tuple[str, ...], _T]],
    location: Optional[str] = None, quoted: bool = False,
) -> Optional[_T]:
    """The entry ``match_name`` reaches, else the one entry whose respellings contain it
    (recorded as a stale spelling); several or none → ``None``."""
    hit = _flat_hit(name=name, entries=entries, quoted=quoted)
    if hit is None:
        return None
    stale, entry = hit
    if stale is not None:
        record_stale_spelling(original=name, normalized=stale, location=location)
    return entry


def peek_flat_name(
    *, name: str, entries: Sequence[Tuple[str, Tuple[str, ...], _T]], quoted: bool = False,
) -> Optional[_T]:
    """``resolve_flat_name`` that records nothing and never raises (an ambiguous name reaches nothing)."""
    try:
        hit = _flat_hit(name=name, entries=entries, quoted=quoted)
    except AmbiguousReferenceError:
        return None
    return None if hit is None else hit[1]


def peek_dataset_field(*, dataset: SlayerDataset, name: str, quoted: bool = False) -> Optional[DatasetField]:
    """``peek_flat_name`` over ``dataset``'s fields."""
    return peek_flat_name(name=name, entries=[(c.name, c.respellings, c) for c in dataset.fields], quoted=quoted)


def _flat_hit(
    *, name: str, entries: Sequence[Tuple[str, Tuple[str, ...], _T]], quoted: bool,
) -> Optional[Tuple[Optional[str], _T]]:
    """``(None, entry)`` for the entry ``match_name`` reaches, else ``(canonical, entry)`` for a unique respelling."""
    found = match_name(name=name, declared=[n for n, _, _ in entries], quoted=quoted)
    for entry_name, _, entry in entries:
        if entry_name == found:
            return None, entry
    matches = [(n, e) for n, respellings, e in entries if name in respellings]
    return matches[0] if len(matches) == 1 else None


class StaleSpelling(BaseModel):
    """One stale flat name bound to its canonical field at ``location``."""

    model_config = ConfigDict(frozen=True)

    location: str
    original: str
    normalized: str


_stale_sink: ContextVar[Optional[Dict[StaleSpelling, None]]] = ContextVar(
    "slayer_stale_spellings", default=None,
)
_stale_stage: ContextVar[Optional[str]] = ContextVar("slayer_stale_stage", default=None)
_stale_position: ContextVar[Optional[str]] = ContextVar(
    "slayer_stale_position", default=None,
)


@contextmanager
def collect_stale_spellings() -> Generator[List[StaleSpelling], None, None]:
    """Collect stale spellings recorded in the block, deduplicated, into the yielded list."""
    sink: Dict[StaleSpelling, None] = {}
    out: List[StaleSpelling] = []
    token = _stale_sink.set(sink)
    try:
        yield out
    finally:
        _stale_sink.reset(token)
        out.extend(sink)


@contextmanager
def stale_spelling_stage(stage: Optional[str]) -> Generator[None, None, None]:
    token = _stale_stage.set(stage)
    try:
        yield
    finally:
        _stale_stage.reset(token)


@contextmanager
def stale_spelling_position(position: Optional[str]) -> Generator[None, None, None]:
    """Referencing position for records in the block; ``None`` suppresses recording."""
    token = _stale_position.set(position)
    try:
        yield
    finally:
        _stale_position.reset(token)


def record_stale_spelling(
    *, original: str, normalized: str, location: Optional[str] = None,
) -> None:
    """Record at ``location``, else the active position (no position → dropped)."""
    sink = _stale_sink.get()
    if sink is None:
        return
    if location is None:
        position = _stale_position.get()
        if position is None:
            return
        stage = _stale_stage.get()
        location = f"{stage}.{position}" if stage else position
    sink[StaleSpelling(location=location, original=original, normalized=normalized)] = None
