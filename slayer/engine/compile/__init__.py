"""Compilers of well-typed queries (DEV-1871 D8): planning became compilation.

``compile_query`` is the public seam over an elaborated environment; it cannot
bind or type — those are ``elaborate_query``'s, and a shape error surviving
into compilation is a bug.
"""

from __future__ import annotations

from typing import Dict, Hashable, Optional

from slayer.engine.compile.stages import compile_prebound
from slayer.ir.elaborated import ElaboratedQuery
from slayer.ir.planned import PlannedQuery


def compile_query(
    *,
    elaborated: ElaboratedQuery,
    disable_host_rooted_isolation: bool = False,
    enable_producer_regroups: bool = False,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Compile an elaborated environment to a ``PlannedQuery``."""
    if (
        elaborated.query is None or elaborated.prebound is None
        or elaborated.bundle is None or elaborated.scope is None
    ):
        raise ValueError(
            "compile_query needs an environment produced by elaborate_query "
            "(its compile inputs are unset).",
        )
    return compile_prebound(
        query=elaborated.query,
        bundle=elaborated.bundle,
        scope=elaborated.scope,
        stage_schemas=dict(elaborated.stage_schemas),
        prebound=elaborated.prebound,
        filter_typings=list(elaborated.filter_typings),
        env=elaborated,
        disable_host_rooted_isolation=disable_host_rooted_isolation,
        enable_producer_regroups=enable_producer_regroups,
        producer_registry=producer_registry,
    )
