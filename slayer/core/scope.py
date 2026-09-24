"""Stage 2 (DEV-1450) — typed scope and stage-schema for the new pipeline.

Two scope kinds, never confused (P5):

- ``ModelScope``: joins exist; dotted refs walk the join graph rooted at
  ``source_model``. ``__`` in a Mode-B ref is an error unless it exact-
  matches a column literally named that way (legacy persisted query-backed
  columns).
- ``StageSchema``: flat namespace; dots are not join syntax;
  ``__``-bearing identifiers are flat names.

``StageColumn`` is the typed projection element (P6): explicit ``name``
(downstream bind name), ``sql_alias`` (emitted SQL identifier),
``public_alias`` (result-key piece), plus the per-column metadata that
downstream stages need.

Per I2 of the DEV-1450 execution plan, ``ModelScope.source_model`` is
``Optional`` from day one so a future anchor-less mode is a type-additive
change. DEV-1450's binder will assert ``source_model is not None`` at
use sites — the type-level optionality is the extension point.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, TypeVar

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import UnknownReferenceError
from slayer.core.format import NumberFormat
from slayer.core.models import Column, SlayerModel

_T = TypeVar("_T")


class StageColumn(BaseModel):
    """Typed projection element for one stage (P6).

    ``name`` is the downstream bind name — flat (e.g.
    ``robot_details__modelseriesval`` or ``rev``). ``sql_alias`` is the
    identifier emitted in the stage's SELECT projection (usually equal
    to ``name``, but the typed split lets the planner reserve hidden
    or alias-bearing forms without coupling them). ``public_alias`` is
    the result-key piece returned to the user — set only for non-hidden
    columns.

    ``format`` (DEV-1452 Stage B decision #8) is the typed ``NumberFormat``
    inherited from the source ``ModelMeasure`` / ``Column`` or computed
    by ``_infer_aggregated_format``. ``description`` propagates the source
    column's documentation through the typed plan.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    sql_alias: str
    public_alias: Optional[str] = None
    type: Optional[DataType] = None
    # Set only for a column an upstream stage bucketed (a ``TimeTruncKey`` slot);
    # ``None`` means "not truncated", so any granularity re-binds (DEV-1471).
    granularity: Optional[TimeGranularity] = None
    label: Optional[str] = None
    format: Optional[NumberFormat] = None
    hidden: bool = False
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    sampled: Optional[str] = None
    provenance: Optional[str] = None
    #: ``name`` under each non-canonical spelling of its path (stale-spelling slack).
    respellings: Tuple[str, ...] = ()


class StageSchema(BaseModel):
    """The typed projection of one query stage (P6).

    Downstream stages bind against this as a flat namespace (P5). They
    never re-walk the upstream join graph through a StageSchema — the
    only legal refs are entries in ``columns``.

    ``relation_name`` is the SQL identifier used when this stage is
    referenced from a downstream stage (CTE name or subquery alias).
    ``sql`` is the emitted text of the stage's SELECT — populated by the
    planner; left ``None`` until rendering.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    relation_name: str
    sql: Optional[str] = None
    columns: List[StageColumn]

    def __getitem__(self, name: str) -> StageColumn:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(
            f"No column named {name!r} in stage {self.relation_name!r}."
        )

    def get(self, name: str) -> Optional[StageColumn]:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and self.get(name) is not None

    def resolve_flat_name(self, name: str) -> Optional[StageColumn]:
        """Stage-boundary lookup: exact name, else a unique stale respelling (recorded)."""
        return resolve_flat_name(
            name, [(c.name, c.respellings, c) for c in self.columns],
        )


class ModelScope(BaseModel):
    """Scope for binding Mode-B refs against a model with joins (P5).

    Dotted refs walk the join graph rooted at ``source_model``;
    ``__``-bearing refs are flat-only and reject unless they exact-match
    a column literally named that way on the model.

    I2: ``source_model`` is ``Optional`` from day one. DEV-1450's binder
    asserts ``source_model is not None`` at use sites so behavior is
    unchanged. A future anchor-less mode uses ``source_model=None`` and
    a different binder branch (DatasourceScope-style binding). Keeping
    the type optional avoids a breaking change later.
    """

    source_model: Optional[SlayerModel] = None


def host_model_name(scope) -> str:
    """The host relation's display name for alias-collision reporting."""
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        return scope.source_model.name
    if isinstance(scope, StageSchema):
        return scope.relation_name
    return "(stage)"


def resolve_generated_column(
    model: SlayerModel, name: str, *, location: Optional[str] = None,
) -> Optional[Column]:
    """``resolve_flat_name`` over ``model``'s columns; only generated query-backed
    columns carry respellings, so authored columns stay exact. A stale name
    matching several columns fails closed (never read as a physical column)."""
    col = resolve_flat_name(
        name, [(c.name, c.respellings, c) for c in model.columns], location=location,
    )
    if col is None:
        matches = [c.name for c in model.columns if name in c.respellings]
        if len(matches) > 1:
            raise UnknownReferenceError(
                name=name, scope_kind="SlayerModel",
                scope_summary=f"model {model.name!r}: stale spelling of {matches}",
            )
    return col


def resolve_flat_name(
    name: str, entries: Sequence[Tuple[str, Tuple[str, ...], _T]],
    *, location: Optional[str] = None,
) -> Optional[_T]:
    """The entry named ``name``, else the one entry whose respellings contain it
    (recorded as a stale spelling); several or none → ``None``."""
    for entry_name, _, entry in entries:
        if entry_name == name:
            return entry
    matches = [(n, e) for n, respellings, e in entries if name in respellings]
    if len(matches) != 1:
        return None
    canonical, entry = matches[0]
    record_stale_spelling(original=name, normalized=canonical, location=location)
    return entry


class StaleSpelling(BaseModel):
    """One stale flat name bound to its canonical column at ``location``."""

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
def collect_stale_spellings() -> Iterator[List[StaleSpelling]]:
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
def stale_spelling_stage(stage: Optional[str]) -> Iterator[None]:
    token = _stale_stage.set(stage)
    try:
        yield
    finally:
        _stale_stage.reset(token)


@contextmanager
def stale_spelling_position(position: Optional[str]) -> Iterator[None]:
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
