"""Compilers of well-typed queries: planning became compilation.

``compile_query`` is the public seam over an elaborated stage; it cannot bind or
type — those are ``elaborate_query``'s, and a shape error surviving into
compilation is a bug. Synthesized producers compile through ``compile_synthesized``.
"""

from __future__ import annotations

from typing import Dict, Hashable, Optional

from slayer.core.keys import TimePointCmpKey, walk_value_keys
from slayer.engine.compile.stages import compile_stage
from slayer.ir.elaborated import ElaboratedStage
from slayer.ir.planned import PlannedQuery
from slayer.ir.prebound import PreboundQuery


def _has_unresolved_time_point(prebound: PreboundQuery) -> bool:
    roots = [
        *(dm.bound.value_key for dm in prebound.declared_measures),
        *(bf.value_key for bf in prebound.bound_filters),
        *(spec.bound.value_key for spec in prebound.order_specs),
    ]
    return any(isinstance(k, TimePointCmpKey) for root in roots for k in walk_value_keys(root))


def compile_query(
    *,
    elaborated: ElaboratedStage,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Compile an elaborated stage to a ``PlannedQuery``."""
    if not isinstance(elaborated, ElaboratedStage):
        raise ValueError(
            "compile_query needs an environment produced by elaborate_query "
            "(its compile inputs are unset).",
        )
    if _has_unresolved_time_point(elaborated.prebound):
        raise RuntimeError("An unresolved time-point comparison reached compilation.")
    return compile_stage(elaborated, producer_registry=producer_registry)
