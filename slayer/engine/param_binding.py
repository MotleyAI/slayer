"""The one analysis of aggregation parameter text: definition defaults and explicit
string fragments bind here, once, onto the aggregate's key."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression
from sqlglot.expressions.ddl import DDL
from sqlglot.expressions.dml import DML

from slayer.core.enums import BUILTIN_AGGREGATION_PARAM_ORDER
from slayer.core.errors import (
    AggregationArgumentError,
    CircularJoinPathError,
    UnanalyzableAggregationParameterError,
    UnknownReferenceError,
    UnresolvableDimensionJoinError,
)
from slayer.core.join_walker import cancelling_revisit, terminal_model, walk_cancelling
from slayer.core.keys import (
    ColumnKey,
    ColumnSqlKey,
    SqlFragmentKey,
    ValueKey,
    normalize_scalar,
    source_anchor_path,
)
from slayer.core.models import (
    VALUE_PLACEHOLDER,
    WINDOW_PARAM,
    Aggregation,
    SlayerModel,
    rendered_formula,
    reserved_window_param_message,
)
from slayer.core.scope import resolve_generated_column
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.column_expansion import (
    raise_if_legacy_split_alias,
    reference_sites,
    root_scope_column_ids,
)
from slayer.sql.dialects import get_dialect
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.sql.sql_template import SqlTemplateError, sql_template

Path = Tuple[str, ...]
ColumnRef = Union[ColumnKey, ColumnSqlKey]
#: Resolves a reference's dotted parts in the query frame, as its unquoted twin binds.
QueryRefResolver = Callable[[Tuple[str, ...]], ValueKey]

_UNANALYSABLE_KINDS: Tuple[Tuple[Tuple[type, ...], str], ...] = (
    ((exp.AggFunc,), "an aggregate"),
    ((exp.Window,), "a window function"),
    ((exp.Query, exp.Subquery), "a subquery"),
    ((exp.Alias,), "an alias"),
    ((exp.Placeholder, exp.Parameter, exp.SessionParameter), "a bind parameter"),
    ((exp.Star, exp.Command, DDL, DML), "a non-expression"),
)


def _unanalysable_kind(node: object) -> Optional[str]:
    return next((kind for types, kind in _UNANALYSABLE_KINDS if isinstance(node, types)), None)


def _fresh_sentinel(text: str) -> str:
    """A reference-placeholder pattern that occurs nowhere in ``text``."""
    salt = 0
    while f"__slayer{salt}_r" in text:
        salt += 1
    return f"__slayer{salt}_r{{}}__"


def agg_owner(*, source: ValueKey, bundle: ResolvedSourceBundle) -> Optional[SlayerModel]:
    """The model declaring ``source``'s aggregation: the host walked along the source
    anchor; ``None`` with no host or an unresolvable hop."""
    host = bundle.source_model
    if host is None:
        return None
    return terminal_model(
        root=host, path=source_anchor_path(source), models_by_name=bundle.models_by_name,
    )


def bind_aggregation_params(
    *,
    agg: str,
    source: ValueKey,
    kwargs: Tuple[Tuple[str, Any], ...],
    bundle: ResolvedSourceBundle,
    resolve_query_ref: QueryRefResolver,
) -> Tuple[Tuple[str, Any], ...]:
    """``kwargs`` with every parameter ``agg`` reads bound to a value: a string naming a
    read parameter binds in the query frame, a missing one binds its definition default
    in the owner's frame; any other string (``window='90d'``) stays a marker."""
    owner = agg_owner(source=source, bundle=bundle)
    definition = _definition(owner=owner, agg=agg)
    if definition is not None and (
        any(p.name == WINDOW_PARAM for p in definition.params)
        or WINDOW_PARAM in _reads(agg=agg, definition=definition, bundle=bundle)
    ):
        raise AggregationArgumentError(reserved_window_param_message(agg))
    reads = _reads(agg=agg, definition=definition, bundle=bundle) - {VALUE_PLACEHOLDER}
    ctx = _Ctx(agg=agg, owner=owner, source=source, bundle=bundle)
    out: List[Tuple[str, Any]] = [
        (name, _bind_text(text=value, name=name, ctx=ctx, resolve=resolve_query_ref))
        if isinstance(value, str) and name in reads else (name, value)
        for name, value in kwargs
    ]
    given = {name for name, _ in kwargs}
    defaults = {p.name: p.sql for p in definition.params} if definition is not None else {}
    for name in sorted(reads - given):
        if name not in defaults:
            raise SqlTemplateError(
                f"Aggregation '{agg}' requires parameter '{name}'. "
                f"Set it in the model's aggregation definition or at query time "
                f"(e.g., 'measure:{agg}({name}=column)')."
            )
        out.append((name, _bind_text(
            text=defaults[name], name=name, ctx=ctx, resolve=ctx.resolve_default)))
    return tuple(out)


def _definition(*, owner: Optional[SlayerModel], agg: str) -> Optional[Aggregation]:
    if owner is None:
        return None
    return next((a for a in (owner.aggregations or []) if a.name == agg), None)


def _reads(
    *, agg: str, definition: Optional[Aggregation], bundle: ResolvedSourceBundle,
) -> frozenset[str]:
    """The parameters ``agg`` reads, its formula parsed (an invalid one fails here)."""
    formula = rendered_formula(agg=agg, definition=definition)
    if formula is None:
        return frozenset(BUILTIN_AGGREGATION_PARAM_ORDER.get(agg, ()))
    try:
        return sql_template(text=formula, dialect=bundle.dialect).placeholder_names
    except SqlTemplateError as e:
        raise SqlTemplateError(f"Aggregation '{agg}': {e}") from e


class _Ctx:
    """The fixed inputs of one aggregate's parameter binding."""

    def __init__(
        self, *, agg: str, owner: Optional[SlayerModel], source: ValueKey,
        bundle: ResolvedSourceBundle,
    ) -> None:
        self.agg = agg
        self.owner = owner
        self.owner_path: Path = source_anchor_path(source)
        self.bundle = bundle
        self.root = bundle.source_model

    def model_name(self) -> Optional[str]:
        return self.owner.name if self.owner is not None else None

    def resolve_default(self, parts: Tuple[str, ...]) -> ValueKey:
        """A default's reference, owner-first with cancellation, else the query root."""
        assert self.root is not None  # a default exists only with an owner
        quals, leaf = parts[:-1], parts[-1]
        abs_path = resolve_default_path(
            qualifiers=quals, leaf=leaf, root_model=self.root,
            owner_path=self.owner_path, root_path=(), bundle=self.bundle,
        )
        return _column_key(
            root=self.root, path=abs_path, leaf=leaf, spelled=".".join(parts),
            bundle=self.bundle,
        )


def _bind_text(
    *, text: str, name: str, ctx: _Ctx, resolve: QueryRefResolver,
) -> object:
    """Parameter text → a column key, a scalar, or a ``SqlFragmentKey``."""
    tree = _parse(text=text, name=name, ctx=ctx)
    scalar = _scalar(tree)
    if scalar is not _NOT_SCALAR:
        return scalar
    sites = reference_sites(tree, root_scope_column_ids(parsed=tree))
    keys = [resolve((*quals, leaf)) for _node, quals, leaf in sites]
    for key in keys:
        if not isinstance(key, (ColumnKey, ColumnSqlKey)):
            raise _unanalysable(text=text, name=name, ctx=ctx, reason="it names a measure, not a column")
    if len(sites) == 1 and sites[0][0] is tree:
        return keys[0]
    sentinel = _fresh_sentinel(text)
    refs: Dict[ValueKey, int] = {}
    for (node, _q, _l), key in zip(sites, keys):
        index = refs.setdefault(key, len(refs))
        node.replace(exp.column(sentinel.format(index)))
    template = tree.sql(dialect=_sqlglot_name(ctx.bundle))
    for index in range(len(refs)):
        template = template.replace(sentinel.format(index), f"{{r{index}}}")
    return SqlFragmentKey(template=template, refs=tuple(refs))  # type: ignore[arg-type]


def _parse(*, text: str, name: str, ctx: _Ctx) -> Expression:
    dialect = _sqlglot_name(ctx.bundle)
    try:
        tree = cast(Expression, sqlglot.parse_one(
            prequote_reserved_identifiers(text, dialect=dialect), dialect=dialect))
    except (SqlglotError, ValueError) as e:
        raise _unanalysable(text=text, name=name, ctx=ctx, reason="it does not parse") from e
    for node in tree.walk():
        kind = _unanalysable_kind(node)
        if kind is not None:
            raise _unanalysable(text=text, name=name, ctx=ctx, reason=f"it contains {kind}")
    while isinstance(tree, exp.Paren):
        tree = tree.this
    return tree


def _unanalysable(*, text: str, name: str, ctx: _Ctx, reason: str) -> UnanalyzableAggregationParameterError:
    return UnanalyzableAggregationParameterError(
        model=ctx.model_name(), aggregation=ctx.agg, parameter=name, text=text, reason=reason,
    )


_NOT_SCALAR = object()


def _scalar(tree: Expression) -> object:
    """A numeric / boolean literal (optionally signed or parenthesised) as the scalar
    its explicit spelling binds, else ``_NOT_SCALAR``."""
    if isinstance(tree, exp.Boolean):
        return bool(tree.this)
    node, negative = tree, False
    while isinstance(node, (exp.Paren, exp.Neg)):
        negative ^= isinstance(node, exp.Neg)
        node = node.this
    if not isinstance(node, exp.Literal) or node.is_string:
        return _NOT_SCALAR
    try:
        value = Decimal(node.this)
    except InvalidOperation:
        return _NOT_SCALAR
    if not value.is_finite():
        return _NOT_SCALAR
    return normalize_scalar(-value if negative else value)


def _sqlglot_name(bundle: ResolvedSourceBundle) -> str:
    try:
        return get_dialect(bundle.dialect).sqlglot_name
    except KeyError:
        return bundle.dialect


def _column_key(
    *, root: SlayerModel, path: Path, leaf: str, spelled: str, bundle: ResolvedSourceBundle,
) -> ColumnRef:
    terminal = walk_key_path(model=root, path=path, bundle=bundle)
    if terminal is None:
        raise UnresolvableDimensionJoinError(
            reference=spelled, root_model=root.name,
            reason="its join path does not resolve from the query root.",
        )
    col = resolve_generated_column(model=terminal, name=leaf)
    if col is None:
        raise UnknownReferenceError(
            name=spelled, scope_kind="ModelScope",
            scope_summary=f"model {terminal.name!r} columns: {[c.name for c in terminal.columns]}",
            suggestion=None,
        )
    if col.needs_expansion:
        return ColumnSqlKey(path=path, model=terminal.name, column_name=col.name)
    return ColumnKey(path=path, leaf=col.name)


def resolve_default_path(
    *, qualifiers: Path, leaf: str, root_model: SlayerModel, owner_path: Path,
    root_path: Path, bundle: ResolvedSourceBundle,
) -> Path:
    """The absolute path of a default reference: owner-first with cancellation, else
    the query root; unreachable from both, or circular from the root, fails closed."""
    mbn = bundle.models_by_name
    abs_path = resolve_default_qualifier_path(
        qualifiers=qualifiers, leaf=leaf, root_model=root_model,
        owner_path=owner_path, models_by_name=mbn,
    )
    if abs_path is None:
        abs_path = resolve_default_qualifier_path(
            qualifiers=qualifiers, leaf=leaf, root_model=root_model,
            owner_path=root_path, models_by_name=mbn,
        )
    if abs_path is None:
        _raise_if_circular(
            qualifiers=qualifiers, leaf=leaf, root_model=root_model,
            owner_path=root_path, models_by_name=mbn,
        )
        raise UnresolvableDimensionJoinError(
            reference=".".join((*qualifiers, leaf)), root_model=root_model.name,
            reason="not reachable forward from the owning model or the query root.",
        )
    return abs_path


def resolve_default_qualifier_path(
    *,
    qualifiers: Path,
    leaf: str,
    root_model: SlayerModel,
    owner_path: Path,
    models_by_name: Dict[str, SlayerModel],
) -> Optional[Path]:
    """Owner-first resolution of a default's qualifier chain with reverse-hop
    cancellation, from ``root_model`` walked along ``owner_path``: the absolute path, or
    ``None`` on a clean first-token miss (the caller retries at the root). A revisit
    through an edge name fails closed with the circular-join error, and a chain whose
    first token resolves but a later one misses as unresolvable."""
    quals = tuple(qualifiers)
    if not quals:
        return tuple(owner_path)
    resolved = walk_cancelling(
        root=root_model, owner_path=owner_path, tokens=quals, models_by_name=models_by_name,
    )
    if resolved is not None:
        return resolved
    _raise_if_circular(
        qualifiers=quals, leaf=leaf, root_model=root_model,
        owner_path=owner_path, models_by_name=models_by_name,
    )
    if walk_cancelling(
        root=root_model, owner_path=owner_path, tokens=quals[:1], models_by_name=models_by_name,
    ) is None:
        if len(quals) == 1:
            raise_if_legacy_split_alias(
                qualifier=quals[0], leaf=leaf, models_by_name=models_by_name,
                source_model=terminal_model(
                    root=root_model, path=owner_path, models_by_name=models_by_name,
                ) or root_model,
            )
        return None
    failing = next(
        (quals[i - 1] for i in range(2, len(quals) + 1)
         if walk_cancelling(root=root_model, owner_path=owner_path,
                            tokens=quals[:i], models_by_name=models_by_name) is None),
        quals[-1],
    )
    raise UnresolvableDimensionJoinError(
        reference=".".join((*quals, leaf)), root_model=root_model.name,
        reason=f"'{failing}' is not a joined model on the preceding hop.",
    )


def _raise_if_circular(
    *, qualifiers: Path, leaf: str, root_model: SlayerModel, owner_path: Path,
    models_by_name: Dict[str, SlayerModel],
) -> None:
    revisit = cancelling_revisit(
        root=root_model, owner_path=owner_path, tokens=qualifiers,
        models_by_name=models_by_name,
    )
    if revisit is not None:
        hop, revisited, via = revisit
        raise CircularJoinPathError(
            reference=".".join((*qualifiers, leaf)), root_model=root_model.name,
            revisited=revisited, hop=hop, via=via,
        )


def resolve_default_reference_paths(
    *, parsed: Expression, owner_path: Path,
    root_model: SlayerModel, root_path: Path, bundle: ResolvedSourceBundle,
) -> List[Tuple[Path, str]]:
    """``(absolute path, leaf)`` per root-scope reference of a parsed default, each
    resolved in its own frame (reference-site order)."""
    return [
        (resolve_default_path(
            qualifiers=quals, leaf=leaf, root_model=root_model,
            owner_path=owner_path, root_path=root_path, bundle=bundle,
        ), leaf)
        for _node, quals, leaf in reference_sites(parsed, root_scope_column_ids(parsed=parsed))
    ]
