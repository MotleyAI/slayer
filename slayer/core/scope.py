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

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat
from slayer.core.models import SlayerModel


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
    label: Optional[str] = None
    format: Optional[NumberFormat] = None
    hidden: bool = False
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    sampled: Optional[str] = None
    provenance: Optional[str] = None


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
