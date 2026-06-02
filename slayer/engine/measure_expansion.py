"""Stage 7b.2 (DEV-1450) — pre-bind ModelMeasure expansion.

Pre-bind AST -> AST rewrite of a ``ParsedExpr`` tree: every ``Ref(name=X)``
whose ``X`` resolves to a ``ModelMeasure`` (on the model or in
``extra_measures``) is replaced with the recursively-expanded
``ParsedExpr`` produced by ``parse_expr(measure.formula)``.

Why pre-bind: the binder (``slayer.engine.binding``) raises
``UnknownReferenceError`` for bare measure names because measures are not
columns; running expansion before the binder sees the tree turns those
refs into binder-resolvable column / aggregation nodes.

Eligibility matrix:

* Eligible positions: root; ``Arith`` / ``UnaryOp`` / ``Cmp`` / ``BoolOp``
  operands; ``ScalarCall.args``; ``TransformCall.input`` / args /
  kwarg values.
* Not eligible: ``DottedRef`` (cross-model dotted paths resolve through
  the join graph, not through measure expansion); ``AggCall`` in any
  position (``source`` / ``args`` / ``kwargs`` are column-level by
  contract); function-name slots on ``TransformCall.op`` /
  ``ScalarCall.name`` (those are strings, not ``Ref`` nodes, and would
  never match the dispatch even if a measure with the same name
  existed); ``Literal`` / ``StarSource``; ``SlayerQuery.order`` entries
  (the caller does not pass order entries through this function — order
  resolves declared slot names only at the planner layer).

Recursion controls:

* Depth limit configurable via ``SLAYER_MEASURE_EXPANSION_DEPTH`` env
  var (default ``32``). An explicit ``depth_limit=`` kwarg wins. Exceeded
  -> :class:`MeasureRecursionLimitError`.
* Per-chain cycle detection. A measure transitively referencing itself
  raises :class:`MeasureCycleError` with the offending chain attached.

Purity: input ``ParsedExpr`` nodes are frozen Pydantic models; the
function returns a fresh tree.

Dormant in this commit. Stage 7b.6 (BoundExpr unification) and stage
7b.15 (engine cutover) wire this into the engine pipeline.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Sequence, Tuple, get_args

from slayer.core.errors import MeasureCycleError, MeasureRecursionLimitError
from slayer.core.models import ModelMeasure, SlayerModel
from slayer.engine.syntax import (
    AggCall,
    Arith,
    BoolOp,
    Cmp,
    DottedRef,
    Literal,
    ParsedExpr,
    Ref,
    ScalarCall,
    StarSource,
    TransformCall,
    UnaryOp,
    parse_expr,
)

_DEFAULT_DEPTH = 32
_DEPTH_ENV_VAR = "SLAYER_MEASURE_EXPANSION_DEPTH"

# Authoritative ParsedExpr node-type tuple, derived from the union in
# slayer.engine.syntax so new node types added there are automatically
# walked by `_maybe_walk` without a silent skip.
_PARSED_EXPR_TYPES: Tuple[type, ...] = get_args(ParsedExpr)


def expand_model_measures(
    *,
    expr: ParsedExpr,
    model: SlayerModel,
    extra_measures: Sequence[ModelMeasure] = (),
    depth_limit: Optional[int] = None,
) -> ParsedExpr:
    """Walk ``expr`` and replace bare measure refs with their formula AST.

    See module docstring for the eligibility matrix and recursion rules.
    Raises ``ValueError`` if ``depth_limit`` is not a positive integer.
    """
    if depth_limit is not None and depth_limit < 1:
        raise ValueError(
            f"depth_limit must be a positive integer, got {depth_limit!r}."
        )
    measures = _collect_named_measures(model=model, extras=extra_measures)
    limit = depth_limit if depth_limit is not None else _env_depth_limit()
    parse_cache: Dict[str, ParsedExpr] = {}
    return _walk(
        expr,
        measures=measures,
        depth_limit=limit,
        chain=(),
        parse_cache=parse_cache,
    )


def _collect_named_measures(
    *,
    model: SlayerModel,
    extras: Sequence[ModelMeasure],
) -> Dict[str, ModelMeasure]:
    """Build a ``name -> ModelMeasure`` map. ``extras`` shadow model
    measures with the same name. Unnamed measures are not addressable
    via bare ref and are skipped.
    """
    out: Dict[str, ModelMeasure] = {}
    for m in model.measures:
        if m.name:
            out[m.name] = m
    for m in extras:
        if m.name:
            out[m.name] = m
    return out


def _env_depth_limit() -> int:
    raw = os.environ.get(_DEPTH_ENV_VAR)
    if raw is None:
        return _DEFAULT_DEPTH
    try:
        return max(1, int(raw))
    except ValueError:
        return _DEFAULT_DEPTH


def _walk(
    node: ParsedExpr,
    *,
    measures: Dict[str, ModelMeasure],
    depth_limit: int,
    chain: Tuple[str, ...],
    parse_cache: Dict[str, ParsedExpr],
) -> ParsedExpr:
    if isinstance(node, Ref):
        return _expand_ref(
            node=node,
            measures=measures,
            depth_limit=depth_limit,
            chain=chain,
            parse_cache=parse_cache,
        )
    if isinstance(node, (DottedRef, StarSource, Literal, AggCall)):
        return node
    if isinstance(node, TransformCall):
        return _walk_transform_call(
            node=node,
            measures=measures,
            depth_limit=depth_limit,
            chain=chain,
            parse_cache=parse_cache,
        )
    if isinstance(node, ScalarCall):
        return _walk_scalar_call(
            node=node,
            measures=measures,
            depth_limit=depth_limit,
            chain=chain,
            parse_cache=parse_cache,
        )
    if isinstance(node, Arith):
        return node.model_copy(
            update={
                "left": _maybe_walk(
                    node.left,
                    measures=measures,
                    depth_limit=depth_limit,
                    chain=chain,
                    parse_cache=parse_cache,
                ),
                "right": _maybe_walk(
                    node.right,
                    measures=measures,
                    depth_limit=depth_limit,
                    chain=chain,
                    parse_cache=parse_cache,
                ),
            }
        )
    if isinstance(node, UnaryOp):
        return node.model_copy(
            update={
                "operand": _maybe_walk(
                    node.operand,
                    measures=measures,
                    depth_limit=depth_limit,
                    chain=chain,
                    parse_cache=parse_cache,
                ),
            }
        )
    if isinstance(node, Cmp):
        return node.model_copy(
            update={
                "left": _maybe_walk(
                    node.left,
                    measures=measures,
                    depth_limit=depth_limit,
                    chain=chain,
                    parse_cache=parse_cache,
                ),
                "right": _maybe_walk(
                    node.right,
                    measures=measures,
                    depth_limit=depth_limit,
                    chain=chain,
                    parse_cache=parse_cache,
                ),
            }
        )
    if isinstance(node, BoolOp):
        return node.model_copy(
            update={
                "operands": tuple(
                    _maybe_walk(
                        o,
                        measures=measures,
                        depth_limit=depth_limit,
                        chain=chain,
                        parse_cache=parse_cache,
                    )
                    for o in node.operands
                )
            }
        )
    # Unknown node type: leave alone. This branch is unreachable for
    # well-formed ParsedExpr but keeps the helper total.
    return node


def _expand_ref(
    *,
    node: Ref,
    measures: Dict[str, ModelMeasure],
    depth_limit: int,
    chain: Tuple[str, ...],
    parse_cache: Dict[str, ParsedExpr],
) -> ParsedExpr:
    if node.name not in measures:
        return node
    if node.name in chain:
        raise MeasureCycleError(chain=list(chain) + [node.name])
    new_chain = chain + (node.name,)
    if len(new_chain) > depth_limit:
        raise MeasureRecursionLimitError(
            chain=list(new_chain), limit=depth_limit
        )
    cached = parse_cache.get(node.name)
    if cached is None:
        cached = parse_expr(measures[node.name].formula)
        parse_cache[node.name] = cached
    return _walk(
        cached,
        measures=measures,
        depth_limit=depth_limit,
        chain=new_chain,
        parse_cache=parse_cache,
    )


def _walk_transform_call(
    *,
    node: TransformCall,
    measures: Dict[str, ModelMeasure],
    depth_limit: int,
    chain: Tuple[str, ...],
    parse_cache: Dict[str, ParsedExpr],
) -> TransformCall:
    new_input = _maybe_walk(
        node.input,
        measures=measures,
        depth_limit=depth_limit,
        chain=chain,
        parse_cache=parse_cache,
    )
    new_args = tuple(
        _maybe_walk(
            a,
            measures=measures,
            depth_limit=depth_limit,
            chain=chain,
            parse_cache=parse_cache,
        )
        for a in node.args
    )
    new_kwargs = tuple(
        (
            k,
            _maybe_walk(
                v,
                measures=measures,
                depth_limit=depth_limit,
                chain=chain,
                parse_cache=parse_cache,
            ),
        )
        for k, v in node.kwargs
    )
    return node.model_copy(
        update={"input": new_input, "args": new_args, "kwargs": new_kwargs}
    )


def _walk_scalar_call(
    *,
    node: ScalarCall,
    measures: Dict[str, ModelMeasure],
    depth_limit: int,
    chain: Tuple[str, ...],
    parse_cache: Dict[str, ParsedExpr],
) -> ScalarCall:
    new_args = tuple(
        _maybe_walk(
            a,
            measures=measures,
            depth_limit=depth_limit,
            chain=chain,
            parse_cache=parse_cache,
        )
        for a in node.args
    )
    return node.model_copy(update={"args": new_args})


def _maybe_walk(
    v: Any,
    *,
    measures: Dict[str, ModelMeasure],
    depth_limit: int,
    chain: Tuple[str, ...],
    parse_cache: Dict[str, ParsedExpr],
) -> Any:
    """Walk ``v`` only if it is a ParsedExpr node; pass scalars through
    unchanged. AggCall args/kwargs and the like contain scalars
    (Decimal / str / bool) that should not be touched.
    """
    if isinstance(v, _PARSED_EXPR_TYPES):
        return _walk(
            v,
            measures=measures,
            depth_limit=depth_limit,
            chain=chain,
            parse_cache=parse_cache,
        )
    return v


__all__ = ["expand_model_measures"]
