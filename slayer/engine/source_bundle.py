"""Stage 2 (DEV-1450) — ResolvedSourceBundle: eagerly resolved query inputs (P11).

The orchestrator builds this once at the top of execute; the binder reads
from it purely. No ContextVar machinery, no callback re-resolution — the
binder is provably scope-only because everything it needs is in the bundle.

Contents (per DEV-1450 spec):
- Source model (the host of the query).
- All other referenced models (joined targets, sibling stage hosts).
- Inline ``ModelExtension`` overlays (extra columns / measures / joins).
- Named query siblings (raw ``SlayerQuery``s; the stage planner compiles
  each to its own ``StageSchema`` as siblings are traversed in
  topological order).
- ``query_variables`` (merged precedence: runtime > stage > outer > model).
- Datasource hint (the ``data_source=`` kwarg that wins over the priority
  list).

Per I2 of the DEV-1450 execution plan, ``source_model`` is ``Optional``
from day one. DEV-1450's binder asserts ``source_model is not None``;
the type-level optionality is the extension point for a future
anchor-less mode.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.models import SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery


class ResolvedSourceBundle(BaseModel):
    """Eagerly resolved inputs to one query execution (P11)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_model: Optional[SlayerModel] = None
    referenced_models: List[SlayerModel] = Field(default_factory=list)
    inline_extensions: List[ModelExtension] = Field(default_factory=list)
    named_queries: Dict[str, SlayerQuery] = Field(default_factory=dict)
    query_variables: Dict[str, Any] = Field(default_factory=dict)
    datasource_hint: Optional[str] = None

    def get_referenced_model(self, name: str) -> Optional[SlayerModel]:
        """Linear lookup by name. The list is small (handful of joined
        models per query), so the O(n) scan is fine.
        """
        for m in self.referenced_models:
            if m.name == name:
                return m
        return None
