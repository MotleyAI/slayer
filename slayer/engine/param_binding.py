"""The one analysis of aggregation parameter text: definition defaults and explicit
string fragments bind here, once, onto the aggregate's key."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.enums import BUILTIN_AGGREGATION_PARAM_ORDER, PERCENTILE_AGGREGATIONS
from slayer.core.errors import (
    AggregationArgumentError,
    CircularRelationshipPathError,
    UnanalyzableAggregationParameterError,
    UnknownReferenceError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.relationship_walker import (
    aggregation_owner,
    cancelling_revisit,
    resolved_terminal,
    walk_cancelling,
    walk_terminal,
)
from slayer.core.keys import (
    FieldKey,
    FieldSqlKey,
    SqlFragmentKey,
    ValueKey,
    normalize_scalar,
    source_anchor_path,
)
from slayer.core.models import (
    VALUE_PLACEHOLDER,
    WINDOW_PARAM,
    Aggregation,
    SlayerDataset,
    aggregation_definition,
    rendered_expression,
    reserved_window_param_message,
)
from slayer.core.scope import resolve_generated_field
from slayer.core.syntax import BoundVariable, Literal, NamedArg, Placeholder, UnaryOp, canonical_metric_text
from slayer.engine.population.anchors import anchor_route, find_anchor
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.field_expansion import (
    raise_if_legacy_split_alias,
    reference_sites,
    root_scope_field_ids,
)
from slayer.sql.dialects import get_dialect
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.sql.sql_template import SqlTemplateError, sql_template

Path = Tuple[str, ...]


def check_percentile_fraction(*, agg: str, args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...]) -> None:
    """Refuse a percentile's ``p`` (``args`` after the value, or named) that is not one numeric literal in [0, 1]."""
    if agg not in PERCENTILE_AGGREGATIONS:
        return
    positional = [a for a in args if not isinstance(a, NamedArg)]
    named = [v for k, v in kwargs if k.lower() == "p"] + [
        a.value for a in args if isinstance(a, NamedArg) and a.name.lower() == "p"]
    if positional and named:
        raise AggregationArgumentError(f"Aggregation '{agg}' got parameter 'p' both positionally and by name.")
    value = next(iter([*positional, *named]), None)
    if value is not None and not _is_fraction(value):
        raise AggregationArgumentError(
            f"Aggregation '{agg}' parameter 'p' must be a numeric literal in [0, 1]; "
            f"got {canonical_metric_text(value)!r}."
        )


def _is_fraction(node: Any) -> bool:
    """A numeric literal in [0, 1]; a variable is judged once bound."""
    negative = isinstance(node, UnaryOp) and node.op == "-"
    node = node.operand if negative else node
    if isinstance(node, (Placeholder, BoundVariable)):
        return True
    if not isinstance(node, Literal) or isinstance(node.value, bool) or not isinstance(node.value, (int, Decimal)):
        return False
    value = -Decimal(node.value) if negative else Decimal(node.value)
    return value.is_finite() and 0 <= value <= 1
FieldRef = Union[FieldKey, FieldSqlKey]
#: Binds a query-side string argument as its unquoted spelling; raises ``QuotedArgumentRefused``.
QuotedArgumentBinder = Callable[[str], Any]


class QuotedArgumentRefused(ValueError):
    """A query-side string argument that is no Mode-B row expression; the message is the reason."""

#: Named refusals, checked first: these are expressions to sqlglot but not row values.
_UNANALYSABLE_KINDS: Tuple[Tuple[Tuple[type, ...], str], ...] = (
    ((exp.AggFunc,), "an aggregate"),
    ((exp.Window,), "a window function"),
    ((exp.Query, exp.Subquery), "a subquery"),
    ((exp.Placeholder, exp.Parameter, exp.SessionParameter), "a bind parameter"),
    ((exp.Explode, exp.Unnest, exp.Inline, exp.Flatten, exp.GenerateSeries, exp.NextValueFor),
     "a set-returning or sequence function"),
)
#: Every node of row-expression text is an expression or one of these helper nodes.
_ROW_HELPER_NODES: Tuple[type, ...] = (
    exp.Identifier, exp.Var, exp.DataType, exp.DataTypeParam, exp.Interval, exp.AtTimeZone,
    exp.JSONPath, exp.JSONPathPart, exp.JSONKeyValue, exp.JSONValue, exp.Tuple, exp.Lambda,
    exp.Slice, exp.ColumnDef,
)


def _non_row_reason(node: object) -> Optional[str]:
    kind = next((kind for types, kind in _UNANALYSABLE_KINDS if isinstance(node, types)), None)
    if kind is not None:
        return f"it contains {kind}"
    if isinstance(node, (exp.Condition, *_ROW_HELPER_NODES)):
        return None
    return "it is not a row expression"


def _fresh_sentinel(text: str) -> str:
    """A reference-placeholder pattern that occurs nowhere in ``text``."""
    salt = 0
    while f"__slayer{salt}_r" in text:
        salt += 1
    return f"__slayer{salt}_r{{}}__"


def bind_aggregation_params(
    *,
    agg: str,
    source: ValueKey,
    kwargs: Tuple[Tuple[str, Any], ...],
    bundle: ResolvedSourceBundle,
    bind_quoted: QuotedArgumentBinder,
) -> Tuple[Tuple[str, Any], ...]:
    """``kwargs`` with every parameter ``agg`` reads bound to a value: a string naming a
    read parameter is Mode B, bound by ``bind_quoted``; a missing one binds its definition
    default (Mode A) in the owner's frame; any other string (``window='90d'``) stays a marker."""
    owner = aggregation_owner(
        root=bundle.population, source=source, datasets_by_name=bundle.datasets_by_name,
    )
    definition = aggregation_definition(owner=owner, agg=agg)
    if definition is not None and (
        any(p.name == WINDOW_PARAM for p in definition.params)
        or WINDOW_PARAM in _reads(agg=agg, definition=definition, bundle=bundle)
    ):
        raise AggregationArgumentError(reserved_window_param_message(agg))
    reads = _reads(agg=agg, definition=definition, bundle=bundle) - {VALUE_PLACEHOLDER}
    ctx = _Ctx(agg=agg, owner=owner, source=source, bundle=bundle)
    out: List[Tuple[str, Any]] = [
        (name, _bind_quoted(text=value, name=name, ctx=ctx, bind=bind_quoted))
        if isinstance(value, str) and name in reads else (name, value)
        for name, value in kwargs
    ]
    given = {name for name, _ in kwargs}
    defaults = {p.name: p.sql for p in definition.params} if definition is not None else {}
    for name in sorted(reads - given):
        if name not in defaults:
            raise SqlTemplateError(
                f"Aggregation '{agg}' requires parameter '{name}'. "
                f"Set it in the dataset's aggregation definition or at query time "
                f"(e.g., '{agg}(metric, {name}=field)')."
            )
        out.append((name, _bind_text(
            text=defaults[name], name=name, ctx=ctx, resolve=ctx.resolve_default)))
    return tuple(out)


def _reads(
    *, agg: str, definition: Optional[Aggregation], bundle: ResolvedSourceBundle,
) -> frozenset[str]:
    """The parameters ``agg`` reads, its expression parsed (an invalid one fails here)."""
    expression = rendered_expression(agg=agg, definition=definition)
    if expression is None:
        return frozenset(BUILTIN_AGGREGATION_PARAM_ORDER.get(agg, ()))
    try:
        return sql_template(text=expression, dialect=bundle.dialect).placeholder_names
    except SqlTemplateError as e:
        raise SqlTemplateError(f"Aggregation '{agg}': {e}") from e


class _Ctx:
    """The fixed inputs of one aggregate's parameter binding."""

    def __init__(
        self, *, agg: str, owner: Optional[SlayerDataset], source: ValueKey,
        bundle: ResolvedSourceBundle,
    ) -> None:
        self.agg = agg
        self.owner = owner
        self.owner_path: Path = source_anchor_path(source)
        self.bundle = bundle
        self.root = bundle.population

    def dataset_name(self) -> Optional[str]:
        return self.owner.name if self.owner is not None else None

    def resolve_default(self, parts: Tuple[str, ...]) -> ValueKey:
        """A default's reference, owner-first with cancellation, else the query root."""
        assert self.root is not None  # a default exists only with an owner
        quals, leaf = parts[:-1], parts[-1]
        abs_path = resolve_default_path(
            qualifiers=quals, leaf=leaf, root_dataset=self.root,
            owner_path=self.owner_path, root_path=(), bundle=self.bundle,
        )
        return _field_key(
            root=self.root, path=abs_path, leaf=leaf, spelled=".".join(parts),
            bundle=self.bundle,
        )


def _bind_quoted(*, text: str, name: str, ctx: _Ctx, bind: QuotedArgumentBinder) -> object:
    """A query-side string argument bound as its unquoted Mode-B spelling; a string literal stays a literal."""
    try:
        value = bind(text)
    except QuotedArgumentRefused as e:
        raise _unanalysable(text=text, name=name, ctx=ctx, reason=str(e)) from e
    if isinstance(value, str):
        return SqlFragmentKey(template=exp.Literal.string(value).sql(dialect=_sqlglot_name(ctx.bundle)), refs=())
    return value


def _bind_text(
    *, text: str, name: str, ctx: _Ctx, resolve: Callable[[Tuple[str, ...]], ValueKey],
) -> object:
    """A definition default's Mode-A text → a field key, a scalar, or a ``SqlFragmentKey``."""
    tree = _parse(text=text, name=name, ctx=ctx)
    scalar = _scalar(tree)
    if scalar is not _NOT_SCALAR:
        return scalar
    sites = reference_sites(tree, root_scope_field_ids(parsed=tree))
    keys = [resolve((*quals, leaf)) for _node, quals, leaf in sites]
    for key in keys:
        if not isinstance(key, (FieldKey, FieldSqlKey)):
            raise _unanalysable(text=text, name=name, ctx=ctx, reason="it names a metric, not a field")
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
        reason = _non_row_reason(node)
        if reason is not None:
            raise _unanalysable(text=text, name=name, ctx=ctx, reason=reason)
    while isinstance(tree, exp.Paren):
        tree = tree.this
    return tree


def _unanalysable(*, text: str, name: str, ctx: _Ctx, reason: str) -> UnanalyzableAggregationParameterError:
    return UnanalyzableAggregationParameterError(
        dataset=ctx.dataset_name(), aggregation=ctx.agg, parameter=name, text=text, reason=reason,
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


def _field_key(
    *, root: SlayerDataset, path: Path, leaf: str, spelled: str, bundle: ResolvedSourceBundle,
) -> FieldRef:
    terminal = walk_key_path(dataset=root, path=path, bundle=bundle)
    if terminal is None:
        raise UnresolvableDimensionRelationshipError(
            reference=spelled, root_dataset=root.spelling,
            reason="its relationship path does not resolve from the query root.",
        )
    col = resolve_generated_field(dataset=terminal, name=leaf)
    if col is None:
        raise UnknownReferenceError(
            name=spelled, scope_kind="DatasetScope",
            scope_summary=f"dataset {terminal.name!r} fields: {[c.name for c in terminal.fields]}",
            suggestion=None,
        )
    if col.needs_expansion:
        return FieldSqlKey(path=path, dataset=terminal.name, field_name=col.name)
    return FieldKey(path=path, leaf=col.name)


def resolve_default_path(
    *, qualifiers: Path, leaf: str, root_dataset: SlayerDataset, owner_path: Path,
    root_path: Path, bundle: ResolvedSourceBundle,
) -> Path:
    """The absolute path of a default reference: owner-first with cancellation, else
    the query root; unreachable from both, or circular from the root, fails closed."""
    mbn = bundle.datasets_by_name
    abs_path = resolve_default_qualifier_path(
        qualifiers=qualifiers, leaf=leaf, root_dataset=root_dataset,
        owner_path=owner_path, datasets_by_name=mbn,
    )
    if abs_path is None:
        abs_path = resolve_default_qualifier_path(
            qualifiers=qualifiers, leaf=leaf, root_dataset=root_dataset,
            owner_path=root_path, datasets_by_name=mbn,
        )
    if abs_path is None:
        _raise_if_circular(
            qualifiers=qualifiers, leaf=leaf, root_dataset=root_dataset,
            owner_path=root_path, datasets_by_name=mbn,
        )
        abs_path = _anchored_default_path(qualifiers=qualifiers, leaf=leaf, reader=root_dataset, datasets_by_name=mbn)
    if abs_path is None:
        raise UnresolvableDimensionRelationshipError(
            reference=".".join((*qualifiers, leaf)), root_dataset=root_dataset.spelling,
            reason="not reachable forward from the owning dataset or the query root.",
        )
    return abs_path


def _anchored_default_path(
    *, qualifiers: Path, leaf: str, reader: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[Path]:
    """A default's qualifier read as a query reference (Principle 1): anchored at its first segment, reached from
    ``reader`` by its only to-one path, else its only route; ``None`` when it names no dataset (an ambiguous route
    raises)."""
    if not qualifiers:
        return None
    anchor = find_anchor(qualifiers[0], quoted=False, reader=reader, datasets_by_name=datasets_by_name)
    if anchor is None:
        return None
    reached = anchor_route(reader=reader, anchor=anchor, datasets_by_name=datasets_by_name,
                           rest=(*qualifiers[1:], leaf))
    walked = walk_terminal(root=anchor, path=qualifiers[1:], datasets_by_name={**datasets_by_name, reader.name: reader})
    return None if reached.unrelated or walked is None else (*reached.route, *walked[1])


def resolve_default_qualifier_path(
    *,
    qualifiers: Path,
    leaf: str,
    root_dataset: SlayerDataset,
    owner_path: Path,
    datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[Path]:
    """Owner-first resolution of a default's qualifier chain with reverse-hop
    cancellation, from ``root_dataset`` walked along ``owner_path``: the absolute path, or
    ``None`` on a clean first-token miss (the caller retries at the root). A revisit
    through an edge name fails closed with the circular-relationship-path error, and a chain whose
    first token resolves but a later one misses as unresolvable."""
    quals = tuple(qualifiers)
    if not quals:
        return tuple(owner_path)
    resolved = walk_cancelling(
        root=root_dataset, owner_path=owner_path, tokens=quals, datasets_by_name=datasets_by_name,
    )
    if resolved is not None:
        return resolved
    _raise_if_circular(
        qualifiers=quals, leaf=leaf, root_dataset=root_dataset,
        owner_path=owner_path, datasets_by_name=datasets_by_name,
    )
    if walk_cancelling(
        root=root_dataset, owner_path=owner_path, tokens=quals[:1], datasets_by_name=datasets_by_name,
    ) is None:
        if len(quals) == 1:
            raise_if_legacy_split_alias(
                qualifier=quals[0], leaf=leaf, datasets_by_name=datasets_by_name,
                source_dataset=resolved_terminal(
                    root=root_dataset, path=owner_path, datasets_by_name=datasets_by_name,
                ) or root_dataset,
            )
        return None
    failing = next(
        (quals[i - 1] for i in range(2, len(quals) + 1)
         if walk_cancelling(root=root_dataset, owner_path=owner_path,
                            tokens=quals[:i], datasets_by_name=datasets_by_name) is None),
        quals[-1],
    )
    raise UnresolvableDimensionRelationshipError(
        reference=".".join((*quals, leaf)), root_dataset=root_dataset.spelling,
        reason=f"'{failing}' is not a joined dataset on the preceding hop.",
    )


def _raise_if_circular(
    *, qualifiers: Path, leaf: str, root_dataset: SlayerDataset, owner_path: Path,
    datasets_by_name: Dict[str, SlayerDataset],
) -> None:
    revisit = cancelling_revisit(
        root=root_dataset, owner_path=owner_path, tokens=qualifiers,
        datasets_by_name=datasets_by_name,
    )
    if revisit is not None:
        hop, revisited, via = revisit
        raise CircularRelationshipPathError(
            reference=".".join((*qualifiers, leaf)), root_dataset=root_dataset.spelling,
            revisited=revisited, hop=hop, via=via,
        )


def default_reference_sites(definition: Aggregation) -> List[Tuple[Path, str]]:
    """``(qualifiers, leaf)`` of each root-scope reference in ``definition``'s parameter defaults; an unparseable default has none."""
    out: List[Tuple[Path, str]] = []
    for param in definition.params:
        if not param.sql:
            continue
        try:
            parsed = cast(Expression, sqlglot.parse_one(
                prequote_reserved_identifiers(param.sql, dialect="postgres"), dialect="postgres"))
        except (SqlglotError, ValueError):
            continue
        out.extend((tuple(quals), leaf) for _node, quals, leaf in reference_sites(parsed, root_scope_field_ids(parsed=parsed)))
    return out


def resolve_default_reference_paths(
    *, parsed: Expression, owner_path: Path,
    root_dataset: SlayerDataset, root_path: Path, bundle: ResolvedSourceBundle,
) -> List[Tuple[Path, str]]:
    """``(absolute path, leaf)`` per root-scope reference of a parsed default, each
    resolved in its own frame (reference-site order)."""
    return [
        (resolve_default_path(
            qualifiers=quals, leaf=leaf, root_dataset=root_dataset,
            owner_path=owner_path, root_path=root_path, bundle=bundle,
        ), leaf)
        for _node, quals, leaf in reference_sites(parsed, root_scope_field_ids(parsed=parsed))
    ]
