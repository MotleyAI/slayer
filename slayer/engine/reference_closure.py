"""DEV-1900 — the one dependency closure (engine.arc42 principle 10).

A reference's *dependency closure* is its own join path plus every join path the
definition of any derived column it names crosses, recursively. Every planner
predicate that classifies a reference's row-level data dependencies — input
safety, attributability, grain determination, filter disposition — consumes this
closure, so a derived reference behaves exactly like a structural one in every
position. The closure is tri-state: ``None`` when a definition cannot be
analysed (no dialect parses it, or the expansion fails), and every consumer
treats ``None`` as unsafe, never as "crosses nothing".

Absorbs the former ``aggregate_input_paths`` and ``column_filter_paths`` modules
and the key-tree ``_child_keys`` dispatch that lived in ``filter_reachability``.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import List, NamedTuple, Optional, Tuple

import sqlglot
from sqlglot import exp

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    source_anchor_path,
    source_row_leaves,
)
from slayer.core.errors import SlayerError, UnresolvableDimensionJoinError
from slayer.core.models import AggregationParam, SlayerModel
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.column_expansion import (
    ColumnCycleError,
    collect_root_scope_reference_columns,
    expand_derived_refs_sync,
    is_trivial_base,
    resolve_default_qualifier_path,
    resolve_default_reference_paths,
)

Path = Tuple[str, ...]

_BARE_IDENT_RE = re.compile(r"^[A-Za-z_]\w*$")
_DOTTED_PATH_RE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+$")

# The planner doesn't carry the datasource dialect, so a backend-specific
# ``Column.sql`` / filter (MySQL backticks, T-SQL brackets, ClickHouse fns) could
# fail the Postgres parse and silently return no paths. Try each dialect; a
# fragment only fails closed when EVERY dialect rejects it.
_PLANNER_PARSE_DIALECT_CHAIN: Tuple[Optional[str], ...] = (
    "postgres", None, "mysql", "clickhouse", "bigquery", "tsql",
)


def _parse_filter_sql_any_dialect(sql: str) -> Optional[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage] — sqlglot ships no __all__
    """First successful parse across the dialect chain, else ``None``."""
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        try:
            return sqlglot.parse_one(sql, dialect=dialect)  # pyright: ignore[reportReturnType] — parse_one's Expr TypeVar
        except Exception:
            continue
    return None


def _expand_derived_refs_any_dialect(
    *, sql: str, model: SlayerModel, alias_path: str, bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """First successful ``expand_derived_refs_sync`` across the chain, else ``None``."""
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        if dialect is None:
            continue
        try:
            expanded = expand_derived_refs_sync(
                sql=sql, model=model, alias_path=alias_path,
                models_by_name=bundle.models_by_name, dialect=dialect,
            )
        except Exception:
            continue
        if expanded:
            return expanded
    return None


class _OrderedSink:
    """Insertion-ordered de-duplicated path collector for the expansion sink."""

    def __init__(self) -> None:
        self._seen: "dict[Path, None]" = {}

    def add(self, path: Path, /) -> None:
        self._seen.setdefault(tuple(path), None)

    @property
    def paths(self) -> Tuple[Path, ...]:
        return tuple(self._seen)


def fragment_closure(
    *, sql: Optional[str], model: SlayerModel, owner_path: Path,
    anchor_relation: str, bundle: ResolvedSourceBundle,
    cache: "Optional[dict]" = None,
) -> Optional[Tuple[Path, ...]]:
    """The root-relative join-path prefixes a free-SQL fragment crosses, derived
    definitions expanded recursively. ``owner_path`` is the fragment owner's
    root-relative path (prefixed onto every crossing). ``()`` = analysed and
    local; ``None`` = no dialect could analyse it (fail closed). A cyclic
    definition raises ``ColumnCycleError``. ``cache`` (optional, plan-scoped)
    memoises the dialect parse + derived expansion, the loop's expensive part,
    keyed by ``(model, sql, owner_path, anchor_relation)``."""
    if not sql:
        return ()
    ck = ("fragment_closure", model.name, sql, tuple(owner_path), anchor_relation)
    if cache is not None and ck in cache:
        return cache[ck]
    result: Optional[Tuple[Path, ...]] = None
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        if dialect is None:
            continue  # expand_derived_refs_sync requires a dialect string
        sink = _OrderedSink()
        try:
            expanded = expand_derived_refs_sync(
                sql=sql, model=model, alias_path=anchor_relation,
                owner_path=tuple(owner_path), models_by_name=bundle.models_by_name,
                dialect=dialect, crossed_paths=sink,
            )
        except ColumnCycleError:
            raise
        except Exception:
            continue
        if expanded is not None:
            result = sink.paths
            break
    if cache is not None:
        cache[ck] = result
    return result


def _prefixes(path: Path) -> List[Path]:
    """Every non-empty prefix of ``path`` (reachability is judged per hop)."""
    return [tuple(path[: i + 1]) for i in range(len(path))]


# Values a key tree carries INLINE — plain data, never references.
_INLINE_SCALARS = (str, int, float, bool, Decimal)
_LEAF_KINDS = (LiteralKey, StarKey, ColumnKey, ColumnSqlKey)


class UnhandledValueKindError(SlayerError, TypeError):
    """A ValueKey kind the closure walk has no rule for. Fails closed: a silent
    empty result would read as 'crosses nothing'."""

    def __init__(self, key: object) -> None:
        self.key_type = type(key).__name__
        super().__init__(
            f"UnhandledValueKindError: dependency-closure walk has no rule for "
            f"key kind {self.key_type!r}. Add an explicit arm — a silent empty "
            f"result would route the reference as if it crossed nothing."
        )


def _child_keys(node, *, descend_aggregates: bool = True) -> List:
    """The child keys of a composite node, in a STABLE order (moved from
    ``filter_reachability``). Fails closed on an unknown kind. A ``Column.filter``
    rides its source ``ColumnSqlKey``, whose closure walks both value and filter.
    ``descend_aggregates=False`` stops at an aggregate (routed by where it is
    computed, not by its inputs)."""
    if isinstance(node, _LEAF_KINDS) or isinstance(node, _INLINE_SCALARS):
        return []
    if isinstance(node, TimeTruncKey):
        return [node.column]
    if isinstance(node, AggregateKey):
        if not descend_aggregates:
            return []
        return [node.source, *node.args, *(v for _name, v in node.kwargs)]
    if isinstance(node, TransformKey):
        return [node.input, *sorted(node.partition_keys, key=repr), node.time_key]
    if isinstance(node, ArithmeticKey):
        return list(node.operands)
    if isinstance(node, ScalarCallKey):
        return list(node.args)
    if isinstance(node, InKey):
        return [node.column, *node.values]
    if isinstance(node, BetweenKey):
        return [node.column, node.low, node.high]
    raise UnhandledValueKindError(node)


def _terminal_model(
    node: ColumnSqlKey, *, anchor_model: SlayerModel, bundle: ResolvedSourceBundle,
) -> Optional[SlayerModel]:
    """The model a ``ColumnSqlKey`` names, falling back to the anchor for its own name."""
    terminal = bundle.models_by_name.get(node.model)
    if terminal is None and node.model == anchor_model.name:
        return anchor_model
    return terminal


def _definition_fragments(col) -> List[str]:
    """The Mode-A fragments a column's definition reads: its non-trivial ``sql`` and its ``filter``."""
    return [
        sql for sql in (
            col.sql if col.sql is not None and not is_trivial_base(column=col) else None,
            col.filter,
        )
        if sql
    ]


def _column_key_closure(
    node, *, anchor_model: SlayerModel, anchor_relation: str,
    bundle: ResolvedSourceBundle, cache: "Optional[dict]" = None,
) -> Optional[List[Path]]:
    """Own path prefixes of a ``ColumnSqlKey``/``ColumnKey`` plus the fragment
    closure of its definition — the value's ``sql`` (when non-trivial) AND its
    ``Column.filter`` (DEV-1832), each anchored at the column's owner. Tri-state
    ``None`` propagates (a dependency that cannot be analysed)."""
    path = tuple(getattr(node, "path", ()) or ())
    out: List[Path] = list(_prefixes(path))
    if not isinstance(node, ColumnSqlKey):
        return out
    terminal = _terminal_model(node, anchor_model=anchor_model, bundle=bundle)
    col = terminal.get_column(node.column_name) if terminal is not None else None
    if terminal is None or col is None:
        return out
    owner_relation = "__".join(path) if path else anchor_relation
    for sql in _definition_fragments(col):
        frag = fragment_closure(
            sql=sql, model=terminal, owner_path=path,
            anchor_relation=owner_relation, bundle=bundle, cache=cache,
        )
        if frag is None:
            return None
        out.extend(frag)
    return out


def key_closure(
    *, key, anchor_model: SlayerModel, anchor_relation: str,
    bundle: ResolvedSourceBundle, cache: "Optional[dict]" = None,
) -> Optional[Tuple[Path, ...]]:
    """Every join path ``key``'s dependency tree crosses, anchored at
    ``anchor_relation`` — its structural paths plus, recursively, every path the
    definition of a derived column it names crosses. Insertion-ordered,
    de-duplicated; ``()`` = local; ``None`` = a dependency could not be analysed
    (fail closed). A cyclic derived definition raises ``ColumnCycleError``."""
    seen: "dict[Path, None]" = {}

    def _add(path: Path) -> None:
        if path:
            seen.setdefault(tuple(path), None)

    def _walk(node) -> bool:
        if node is None:
            return True
        leafs = _leaf_closure(node, anchor_model=anchor_model,
                              anchor_relation=anchor_relation, bundle=bundle,
                              cache=cache)
        if leafs is None:
            return False
        for path in leafs:
            _add(path)
        for child in _child_keys(node):
            if not _walk(child):
                return False
        return True

    if not _walk(key):
        return None
    return tuple(seen)


def _leaf_closure(
    node, *, anchor_model: SlayerModel, anchor_relation: str,
    bundle: ResolvedSourceBundle, cache: "Optional[dict]" = None,
) -> Optional[List[Path]]:
    """Paths a node contributes ITSELF (not via children). ``None`` = unanalysable."""
    if isinstance(node, ColumnSqlKey):
        return _column_key_closure(
            node, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle, cache=cache,
        )
    if isinstance(node, ColumnKey):
        return _prefixes(tuple(node.path))
    if isinstance(node, str):
        frag = fragment_closure(
            sql=node, model=anchor_model, owner_path=(),
            anchor_relation=anchor_relation, bundle=bundle, cache=cache,
        )
        return None if frag is None else list(frag)
    return []


def compute_expr_reference_columns(
    *, canonical_sql: Optional[str], anchor_model: SlayerModel,
    anchor_relation: str, bundle: ResolvedSourceBundle,
) -> Optional[Tuple[Tuple[Optional[Path], str], ...]]:
    """Root-scope column refs of an expression fragment as ``(join path, leaf)``:
    ``()`` path = anchor-local, non-empty = a resolved walk, ``None`` path =
    opaque. ``()`` overall = analysed and column-free; ``None`` overall = the
    fragment could not be analysed (callers fail closed on both ``None`` shapes)."""
    if not canonical_sql:
        return ()
    parsed = _parse_filter_sql_any_dialect(canonical_sql)
    if parsed is None:
        return None
    try:
        return tuple(collect_root_scope_reference_columns(
            parsed=parsed, source_model=anchor_model,
            source_relation=anchor_relation, bundle=bundle,
        ))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Aggregation-parameter defaults (moved from compile/stages).
# ---------------------------------------------------------------------------


class ParamSpec(NamedTuple):
    """A resolved aggregation parameter that references data: a bound ``key``
    (column / aggregate) or an ``expr_sql`` expression default whose referenced
    columns are ``expr_refs`` (``None`` = an unresolvable qualifier, fails
    closed). Literal params never become a ``ParamSpec``."""

    name: str
    key: Optional[ValueKey]
    expr_sql: Optional[str]
    expr_refs: Tuple[Optional[ValueKey], ...] = ()


def column_default_key(
    *, path: Path, leaf: str, base: Optional[SlayerModel],
) -> ValueKey:
    """A ``ColumnSqlKey`` when ``leaf`` names a derived or filtered column on
    ``base`` (so its definition expands and its crossings close), else a plain
    ``ColumnKey``."""
    if base is not None:
        col = next((c for c in (base.columns or []) if c.name == leaf), None)
        if col is not None and col.needs_expansion:
            return ColumnSqlKey(path=path, model=base.name, column_name=leaf)
    return ColumnKey(path=path, leaf=leaf)


def _forward_valid(
    *, root_model: Optional[SlayerModel], path: Path,
    bundle: Optional[ResolvedSourceBundle],
) -> bool:
    """A resolved default path is forward-valid iff it is empty (frame-local) or
    walks forward from the frame root without stepping back to an ancestor —
    ``walk_key_path``'s revisit guard blocks a reverse hop to the root/ancestor,
    so an ancestor reference (resolved bidirectionally) reads as non-forward and
    falls back to the root."""
    if not path:
        return True
    return root_model is not None and bundle is not None and walk_key_path(
        model=root_model, path=path, bundle=bundle,
    ) is not None


def _dotted_key_legacy(
    *, parts: List[str], owner_path: Path, owner_model: Optional[SlayerModel],
    bundle: Optional[ResolvedSourceBundle],
) -> ValueKey:
    """Owner-frame-only dotted resolution (no root fallback) — the behavior when
    no root frame is supplied (typing / reaggregation callers)."""
    if owner_model is not None and parts[0] == owner_model.name:
        parts = parts[1:]  # a leading owner-model qualifier is a self-reference
    if len(parts) == 1:
        return column_default_key(path=tuple(owner_path), leaf=parts[0], base=owner_model)
    terminal = (
        walk_key_path(model=owner_model, path=tuple(parts[:-1]), bundle=bundle)
        if owner_model is not None and bundle is not None else None
    )
    return column_default_key(
        path=tuple(owner_path) + tuple(parts[:-1]), leaf=parts[-1], base=terminal,
    )


def _frame_dotted_key(
    *, chain: Tuple[str, ...], leaf: str, frame_model: SlayerModel,
    frame_path: Path, query_root: SlayerModel, bundle: ResolvedSourceBundle,
) -> Optional[ValueKey]:
    """Resolve a dotted default's qualifier chain in one frame, returning the key
    when it resolves FORWARD in the query tree (``()`` = frame-local) and ``None``
    on a clean miss — including a chain that only resolves via a reverse hop to an
    ancestor (its absolute path fails the forward walk from ``query_root``). Raises
    on an ambiguous or partially-broken chain (fail closed)."""
    resolved = resolve_default_qualifier_path(
        qualifiers=chain, leaf=leaf, frame_model=frame_model,
        models_by_name=bundle.models_by_name,
    )
    if resolved is None:
        return None
    abs_path = tuple(frame_path) + resolved
    if not _forward_valid(root_model=query_root, path=abs_path, bundle=bundle):
        return None
    return column_default_key(
        path=abs_path, leaf=leaf,
        base=walk_key_path(model=frame_model, path=resolved, bundle=bundle)
        if resolved else frame_model,
    )


def default_param_value_key(
    *, sql: str, owner_path: Path, owner_model: Optional[SlayerModel] = None,
    bundle: Optional[ResolvedSourceBundle] = None,
    root_model: Optional[SlayerModel] = None, root_path: Path = (),
) -> Optional[ValueKey]:
    """A bare-identifier or dotted-path definition default → a structured key (a
    ``ColumnSqlKey`` when the named column is derived); an expression or literal
    default → ``None``. A bare default is owner-local. With a ``root_model`` frame
    a dotted default resolves owner-first — forward from the owning model — falling
    back to the query root when the owner cannot reach it forward (a leading
    root-model name self-strips to root-local), and failing closed on an ambiguous
    or partially-broken chain or one unreachable from both frames. Without a root
    frame it keeps the owner-only behavior (typing / reaggregation callers)."""
    text = sql.strip()
    if _BARE_IDENT_RE.match(text):
        return column_default_key(path=tuple(owner_path), leaf=text, base=owner_model)
    if not _DOTTED_PATH_RE.match(text):
        return None
    parts = text.split(".")
    if root_model is None or bundle is None:
        return _dotted_key_legacy(
            parts=parts, owner_path=owner_path, owner_model=owner_model, bundle=bundle,
        )
    chain, leaf = tuple(parts[:-1]), parts[-1]
    if owner_model is not None:
        owner_key = _frame_dotted_key(
            chain=chain, leaf=leaf, frame_model=owner_model,
            frame_path=owner_path, query_root=root_model, bundle=bundle,
        )
        if owner_key is not None:
            return owner_key
    root_key = _frame_dotted_key(
        chain=chain, leaf=leaf, frame_model=root_model,
        frame_path=root_path, query_root=root_model, bundle=bundle,
    )
    if root_key is not None:
        return root_key
    raise UnresolvableDimensionJoinError(
        reference=text, root_model=root_model.name,
        reason="not reachable forward from the owning model or the query root.",
    )


def expr_default_ref_keys(
    *, sql: str, owner_model: Optional[SlayerModel], owner_path: Path,
    bundle: Optional[ResolvedSourceBundle],
    root_model: Optional[SlayerModel] = None, root_path: Path = (),
) -> List[Optional[ValueKey]]:
    """Parse-based column refs of an expression default, as keys. Each reference
    resolves owner-first (forward from the owning model); with a ``root_model``
    frame a reference the owner cannot reach forward is retried root-relative,
    per reference (so ``spend + orders.amount`` resolves ``spend`` owner-local and
    ``orders.amount`` root-local). ``None`` entries — an unresolvable qualifier, or
    an unanalysable fragment — fail closed at typing."""
    if owner_model is None or bundle is None:
        return []
    if root_model is None:  # legacy: owner-frame resolution only (typing / reaggregation)
        owner_refs = compute_expr_reference_columns(
            canonical_sql=sql, anchor_model=owner_model,
            anchor_relation=owner_model.name, bundle=bundle,
        )
        if owner_refs is None:
            return [None]
        return [
            column_default_key(
                path=tuple(owner_path) + path, leaf=leaf,
                base=walk_key_path(model=owner_model, path=path, bundle=bundle),
            )
            if path is not None else None
            for path, leaf in owner_refs
        ]
    parsed = _parse_filter_sql_any_dialect(sql)
    if parsed is None:
        return [None]
    # STRICT per reference: an ambiguous / partially-broken owner qualifier raises
    # here (fail closed), never silently re-anchors at the root.
    abs_refs = resolve_default_reference_paths(
        parsed=parsed, owner_model=owner_model, owner_path=owner_path,
        root_model=root_model, root_path=root_path, bundle=bundle,
    )
    return [
        column_default_key(
            path=abs, leaf=leaf,
            base=walk_key_path(model=root_model, path=abs, bundle=bundle),
        )
        if abs is not None else None
        for abs, leaf in abs_refs
    ]


def resolve_aggregation_params(
    *, agg: AggregateKey, owner_model: Optional[SlayerModel], owner_path: Path,
    bundle: Optional[ResolvedSourceBundle] = None,
    root_model: Optional[SlayerModel] = None, root_path: Path = (),
) -> List[ParamSpec]:
    """Every aggregation parameter that references data — explicit non-scalar
    kwargs and non-overridden definition defaults resolved on the owner
    (terminal of the source path), with an optional query-root frame for
    owner-unreachable qualifiers. Literal params are omitted."""
    explicit = {name for name, _ in agg.kwargs}
    out: List[ParamSpec] = [
        ParamSpec(name=name, key=v, expr_sql=None)
        for name, v in agg.kwargs
        if isinstance(v, (ColumnKey, ColumnSqlKey, AggregateKey))
    ]
    agg_def = next(
        (a for a in (owner_model.aggregations or []) if a.name == agg.agg), None,
    ) if owner_model is not None else None
    if agg_def is not None and owner_model is not None:
        out.extend(
            spec for p in agg_def.params if p.name not in explicit
            and (spec := _default_param_spec(
                p=p, owner_model=owner_model, owner_path=owner_path, bundle=bundle,
                root_model=root_model, root_path=root_path,
            )) is not None
        )
    return out


def _default_param_spec(
    *, p: AggregationParam, owner_model: SlayerModel, owner_path: Path,
    bundle: Optional[ResolvedSourceBundle],
    root_model: Optional[SlayerModel] = None, root_path: Path = (),
) -> Optional[ParamSpec]:
    """A non-overridden definition default → its ``ParamSpec`` (a bound key, or a
    lifted expression with its referenced columns), or ``None`` when it rides the
    plain kwarg/default machinery unchanged."""
    vk = default_param_value_key(
        sql=p.sql, owner_path=owner_path, owner_model=owner_model, bundle=bundle,
        root_model=root_model, root_path=root_path,
    )
    if vk is not None:
        return ParamSpec(name=p.name, key=vk, expr_sql=None)
    refs = expr_default_ref_keys(
        sql=p.sql, owner_model=owner_model, owner_path=owner_path, bundle=bundle,
        root_model=root_model, root_path=root_path,
    )
    if refs:
        return ParamSpec(name=p.name, key=None, expr_sql=p.sql, expr_refs=tuple(refs))
    return None


# ---------------------------------------------------------------------------
# Aggregate-input closure (replaces compute_aggregate_input_join_paths).
# ---------------------------------------------------------------------------


def _explicit_input_refs(*, key: AggregateKey, include_source: bool) -> List[object]:
    """An aggregate's directly-named input refs: its source (when
    ``include_source``), positional args, and keyword-arg values."""
    return [
        *([key.source] if include_source else []),
        *key.args,
        *(v for _, v in key.kwargs),
    ]


def _default_param_specs(
    *, key: AggregateKey, anchor_model: SlayerModel, bundle: ResolvedSourceBundle,
) -> List[ParamSpec]:
    """Resolved default parameters NOT overridden by an explicit kwarg. The
    definition is looked up on its DECLARING model — the re-rooted source anchor
    (mirroring the parameter-typing caller) — so a fanning default resolves on
    the declaring model even when the home widened away from it; a default the
    owner cannot reach forward falls back to the aggregate's root frame."""
    explicit = {name for name, _ in key.kwargs}
    source_path = source_anchor_path(key.source)
    owner_model = walk_key_path(
        model=anchor_model, path=source_path, bundle=bundle,
    ) or anchor_model
    return [
        spec
        for spec in resolve_aggregation_params(
            agg=key, owner_model=owner_model, owner_path=source_path, bundle=bundle,
            root_model=anchor_model, root_path=(),
        )
        if spec.name not in explicit
    ]


def _refs_closure(
    *, refs: List[object], descend_aggregates: bool, anchor_model: SlayerModel,
    anchor_relation: str, bundle: ResolvedSourceBundle,
) -> Optional[List[Path]]:
    """The combined closure of a list of input refs. ``None`` when any non-string
    ref is unanalysable. A raw, unparseable template-fragment STRING contributes
    nothing (the pre-existing defensive fallback; a malformed SQL fragment is the
    renderer's gate) — only a named DERIVED COLUMN fails closed."""
    out: List[Path] = []
    for ref in refs:
        if isinstance(ref, AggregateKey) and not descend_aggregates:
            continue  # opaque: its inputs belong to its own producer
        c = key_closure(
            key=ref, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if c is None:
            if isinstance(ref, str):
                continue
            return None
        out.extend(c)
    return out


def _default_params_closure(
    *, key: AggregateKey, anchor_model: SlayerModel, anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Optional[List[Path]]:
    """The combined closure of every non-overridden default parameter (``None``
    fails closed)."""
    out: List[Path] = []
    for spec in _default_param_specs(key=key, anchor_model=anchor_model, bundle=bundle):
        c = _param_spec_closure(
            spec, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if c is None:
            return None
        out.extend(c)
    return out


def _merge_paths(*, seen: "dict[Path, None]", part: Optional[List[Path]]) -> bool:
    """Merge one component's paths into ``seen`` (deduped, non-empty only).
    ``False`` when the component is unanalysable (``None``) — the caller then
    fails closed WITHOUT evaluating the rest (preserving the short circuit)."""
    if part is None:
        return False
    for p in part:
        if p:
            seen.setdefault(tuple(p), None)
    return True


def aggregate_input_closure(
    *, key: AggregateKey, anchor_model: Optional[SlayerModel],
    anchor_relation: str, bundle: ResolvedSourceBundle,
    include_source: bool = True, descend_aggregates: bool = False,
) -> Optional[Tuple[Path, ...]]:
    """The dependency closure of an aggregate's inputs — its source (when
    ``include_source``), positional/keyword arguments, and non-overridden
    definition defaults — recursively through derived definitions. A source
    column's ``Column.filter`` rides its ``ColumnSqlKey`` source (DEV-1832), so
    the source closure covers it. ``None`` when any dependency cannot be analysed
    (fail closed, short-circuiting on the first unanalysable component); ``()``
    when purely local. Safety mode (``descend_aggregates=False``) treats an
    ``AggregateKey``-valued input as opaque — its inputs belong to its own
    producer; discovery mode descends into it."""
    if anchor_model is None:
        return ()
    seen: "dict[Path, None]" = {}
    if not _merge_paths(seen=seen, part=_refs_closure(
        refs=_explicit_input_refs(key=key, include_source=include_source),
        descend_aggregates=descend_aggregates, anchor_model=anchor_model,
        anchor_relation=anchor_relation, bundle=bundle,
    )):
        return None
    if not _merge_paths(seen=seen, part=_default_params_closure(
        key=key, anchor_model=anchor_model, anchor_relation=anchor_relation,
        bundle=bundle,
    )):
        return None
    return tuple(seen)


def _unanalyzable_derived_name(
    *, ref: object, anchor_model: SlayerModel, anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """``ref``'s column name when its definition — derived ``sql`` OR
    ``Column.filter`` (DEV-1832) — is one no dialect can analyse, else ``None``."""
    if not isinstance(ref, ColumnSqlKey):
        return None
    path = tuple(ref.path or ())
    terminal = bundle.models_by_name.get(ref.model)
    if terminal is None and ref.model == anchor_model.name:
        terminal = anchor_model
    col = terminal.get_column(ref.column_name) if terminal is not None else None
    if terminal is None or col is None:
        return None
    owner_relation = "__".join(path) if path else anchor_relation
    for sql in (
        col.sql if col.sql is not None and not is_trivial_base(column=col) else None,
        col.filter,
    ):
        if sql and fragment_closure(
            sql=sql, model=terminal, owner_path=path,
            anchor_relation=owner_relation, bundle=bundle,
        ) is None:
            return ref.column_name
    return None


def first_unanalyzable_input_column(
    *, key: AggregateKey, anchor_model: Optional[SlayerModel],
    anchor_relation: str, bundle: ResolvedSourceBundle, include_source: bool = True,
) -> Optional[str]:
    """The column name of the first aggregate input whose derived definition no
    dialect can analyse (best-effort diagnostic naming the
    ``check_input_dependencies_analyzable`` error — the safety DECISION is the
    closure's tri-state), else ``None``."""
    if anchor_model is None:
        return None
    refs: List[object] = _explicit_input_refs(key=key, include_source=include_source)
    for spec in _default_param_specs(key=key, anchor_model=anchor_model, bundle=bundle):
        if spec.key is not None:
            refs.append(spec.key)
        refs.extend(r for r in spec.expr_refs if r is not None)
    for ref in refs:
        name = _unanalyzable_derived_name(
            ref=ref, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if name is not None:
            return name
    return None


def source_row_leaf_closure(
    *, key: AggregateKey, anchor_model: Optional[SlayerModel],
    anchor_relation: str, bundle: ResolvedSourceBundle,
) -> Optional[Tuple[Path, ...]]:
    """The dependency closure of an aggregate SOURCE's own ROW-level leaves, with
    attached constituents opaque (Axiom 2.3) — unlike ``aggregate_input_closure``,
    which descends a nested aggregate inside an expression source. ``None`` when a
    leaf's derived definition cannot be analysed (fail closed); ``()`` = local."""
    if anchor_model is None:
        return ()
    seen: "dict[Path, None]" = {}
    for leaf in source_row_leaves(key.source):
        c = key_closure(
            key=leaf, anchor_model=anchor_model,
            anchor_relation=anchor_relation, bundle=bundle,
        )
        if c is None:
            return None
        for p in c:
            if p:
                seen.setdefault(tuple(p), None)
    return tuple(seen)


def first_unanalyzable_source_row_leaf(
    *, key: AggregateKey, anchor_model: Optional[SlayerModel],
    anchor_relation: str, bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """Column name of the first source ROW leaf whose derived definition no dialect
    can analyse (diagnostic for ``check_input_dependencies_analyzable``)."""
    if anchor_model is None:
        return None
    for leaf in source_row_leaves(key.source):
        name = _unanalyzable_derived_name(
            ref=leaf, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if name is not None:
            return name
    return None


def first_unanalyzable_filter_column(
    *, key, anchor_model: Optional[SlayerModel], anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """Column name of the first reference in a filter conjunct whose derived
    definition no dialect can analyse (diagnostic for
    ``check_filter_dependencies_analyzable``), else ``None``."""
    if anchor_model is None:
        return None
    stack: List[object] = [key]
    while stack:
        node = stack.pop()
        name = _unanalyzable_derived_name(
            ref=node, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if name is not None:
            return name
        stack.extend(_child_keys(node))
    return None


def _param_spec_closure(
    spec: ParamSpec, *, anchor_model: SlayerModel, anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Optional[List[Path]]:
    """Closure of one resolved default parameter — its bound key, or every ref of
    an expression default (a ``None`` ref fails closed)."""
    if spec.key is not None:
        if isinstance(spec.key, AggregateKey):
            return []  # opaque: its own producer
        c = key_closure(
            key=spec.key, anchor_model=anchor_model,
            anchor_relation=anchor_relation, bundle=bundle,
        )
        return None if c is None else list(c)
    out: List[Path] = []
    for ref in spec.expr_refs:
        if ref is None:
            return None
        c = key_closure(
            key=ref, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if c is None:
            return None
        out.extend(c)
    return out
