"""Compilers of well-typed queries (DEV-1871 D8): planning became compilation.

``compile_query`` is the public seam over an elaborated stage; it cannot bind or
type — those are ``elaborate_query``'s, and a shape error surviving into
compilation is a bug. Synthesized producers compile through ``compile_synthesized``.
"""

from __future__ import annotations

from typing import Dict, Hashable, Optional

from slayer.engine.compile.stages import compile_stage
from slayer.ir.elaborated import ElaboratedStage
from slayer.ir.planned import PlannedQuery


def compile_query(
    *,
    elaborated: ElaboratedStage,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Compile an elaborated stage to a ``PlannedQuery``."""
    if not isinstance(elaborated, ElaboratedStage) or elaborated.query is None:
        raise ValueError(
            "compile_query needs an environment produced by elaborate_query "
            "(its compile inputs are unset).",
        )
    return compile_stage(elaborated, producer_registry=producer_registry)
