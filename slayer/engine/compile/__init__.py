"""Compilers of well-typed queries (DEV-1871 D8): planning became compilation.

``compile_query`` is the public seam over an elaborated environment;
``bind_query_inputs`` / ``plan_query`` (in ``stages``) remain as compatibility
wrappers until every caller routes through elaborate→compile.
"""

from __future__ import annotations

from slayer.engine.compile.stages import plan_query
from slayer.ir.elaborated import ElaboratedQuery
from slayer.ir.planned import PlannedQuery


def compile_query(*, elaborated: ElaboratedQuery, **plan_kwargs) -> PlannedQuery:
    """Compile an elaborated environment to a ``PlannedQuery``."""
    src = elaborated.source
    if src is None or src.bundle is None:
        raise ValueError(
            "compile_query needs an environment produced by elaborate_query "
            "(its source carrier is unset).",
        )
    return plan_query(
        query=src.query,
        bundle=src.bundle,
        scope=src.scope,
        stage_schemas=src.stage_schemas,
        prebound=src.prebound,
        **plan_kwargs,
    )
