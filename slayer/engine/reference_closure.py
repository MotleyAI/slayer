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
    SqlExprKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
)
from slayer.core.errors import SlayerError
from slayer.core.models import AggregationParam, SlayerModel
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.column_expansion import (
    ColumnCycleError,
    collect_root_scope_reference_columns,
    expand_derived_refs_sync,
    is_trivial_base,
)

Path = Tuple[str, ...]

import re

_BARE_IDENT_RE = re.compile(r"^[A-Za-z_]\w*$")
_DOTTED_PATH_RE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+$")

# The planner doesn't carry the datasource dialect, so a backend-specific
# ``Column.sql`` / filter (MySQL backticks, T-SQL brackets, ClickHouse fns) could
# fail the Postgres parse and silently return no paths. Try each dialect; a
# fragment only fails closed when EVERY dialect rejects it.
_PLANNER_PARSE_DIALECT_CHAIN: Tuple[Optional[str], ...] = (
    "postgres", None, "mysql", "clickhouse", "bigquery", "tsql",
)


def _parse_filter_sql_any_dialect(sql: str) -> Optional[exp.Expression]:
    """First successful parse across the dialect chain, else ``None``."""
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        try:
            return sqlglot.parse_one(sql, dialect=dialect)
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
) -> Optional[Tuple[Path, ...]]:
    """The root-relative join-path prefixes a free-SQL fragment crosses, derived
    definitions expanded recursively. ``owner_path`` is the fragment owner's
    root-relative path (prefixed onto every crossing). ``()`` = analysed and
    local; ``None`` = no dialect could analyse it (fail closed). A cyclic
    definition raises ``ColumnCycleError``."""
    if not sql:
        return ()
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
            return sink.paths
    return None


def _prefixes(path: Path) -> List[Path]:
    """Every non-empty prefix of ``path`` (reachability is judged per hop)."""
    return [tuple(path[: i + 1]) for i in range(len(path))]


# Values a key tree carries INLINE — plain data, never references.
_INLINE_SCALARS = (str, int, float, bool, Decimal)
_LEAF_KINDS = (LiteralKey, StarKey, SqlExprKey, ColumnKey, ColumnSqlKey)


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
    ``filter_reachability``). Fails closed on an unknown kind. ``column_filter_key``
    is never a plain child — its paths are owner-relative and re-anchored by the
    leaf arm. ``descend_aggregates=False`` stops at an aggregate (routed by where
    it is computed, not by its inputs)."""
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


def _derived_column(model: Optional[SlayerModel], leaf: str):
    """The non-trivial derived ``Column`` named ``leaf`` on ``model``, else ``None``."""
    if model is None:
        return None
    col = model.get_column(leaf)
    if col is None or not col.sql or is_trivial_base(column=col):
        return None
    return col


def _column_key_closure(
    node, *, anchor_model: SlayerModel, anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Optional[List[Path]]:
    """Own path prefixes of a ``ColumnSqlKey``/``ColumnKey`` plus, when its
    terminal column is derived and non-trivial, the fragment closure of its
    definition (tri-state ``None`` propagates)."""
    path = tuple(getattr(node, "path", ()) or ())
    out: List[Path] = list(_prefixes(path))
    if isinstance(node, ColumnSqlKey):
        terminal = bundle.models_by_name.get(node.model)
        if terminal is None and node.model == anchor_model.name:
            terminal = anchor_model
        col = _derived_column(terminal, node.column_name)
        if col is not None:
            frag = fragment_closure(
                sql=col.sql, model=terminal, owner_path=path,
                anchor_relation="__".join(path) if path else anchor_relation,
                bundle=bundle,
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
                              anchor_relation=anchor_relation, bundle=bundle)
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
    bundle: ResolvedSourceBundle,
) -> Optional[List[Path]]:
    """Paths a node contributes ITSELF (not via children). ``None`` = unanalysable."""
    if isinstance(node, AggregateKey) and node.column_filter_key is not None:
        source_path = tuple(getattr(node.source, "path", ()) or ())
        return [
            pre
            for p in node.column_filter_key.referenced_join_paths
            for pre in _prefixes(source_path + tuple(p))
        ]
    if isinstance(node, ColumnSqlKey):
        return _column_key_closure(
            node, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
    if isinstance(node, ColumnKey):
        return _prefixes(tuple(node.path))
    if isinstance(node, str):
        frag = fragment_closure(
            sql=node, model=anchor_model, owner_path=(),
            anchor_relation=anchor_relation, bundle=bundle,
        )
        return None if frag is None else list(frag)
    if isinstance(node, SqlExprKey):
        return [pre for p in node.referenced_join_paths for pre in _prefixes(tuple(p))]
    return []


# ---------------------------------------------------------------------------
# Column-filter / expression fragment discovery (was column_filter_paths).
# ---------------------------------------------------------------------------


def compute_column_filter_join_paths(
    *, canonical_sql: Optional[str], anchor_model: SlayerModel,
    anchor_relation: str, bundle: ResolvedSourceBundle,
) -> Tuple[Path, ...]:
    """The ordered join-path prefixes a ``Column.filter`` predicate crosses, its
    derived references (anchor-local or on a joined model) expanded recursively.
    ``()`` for same-model / empty / unanalysable (best-effort; the input-safety
    consumer takes the tri-state closure directly)."""
    frag = fragment_closure(
        sql=canonical_sql, model=anchor_model, owner_path=(),
        anchor_relation=anchor_relation, bundle=bundle,
    )
    return frag or ()


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
    """A ``ColumnSqlKey`` when ``leaf`` names a derived column on ``base`` (so its
    ``Column.sql`` expands), else a plain ``ColumnKey``."""
    if base is not None:
        col = next((c for c in (base.columns or []) if c.name == leaf), None)
        if col is not None and col.sql:
            return ColumnSqlKey(path=path, model=base.name, column_name=leaf)
    return ColumnKey(path=path, leaf=leaf)


def default_param_value_key(
    *, sql: str, owner_path: Path, owner_model: Optional[SlayerModel] = None,
    bundle: Optional[ResolvedSourceBundle] = None,
) -> Optional[ValueKey]:
    """A bare-identifier or dotted-path definition default → a structured key in
    the owner's coordinates (a ``ColumnSqlKey`` when the named column is derived);
    an expression or literal default → ``None``."""
    text = sql.strip()
    if _BARE_IDENT_RE.match(text):
        return column_default_key(path=tuple(owner_path), leaf=text, base=owner_model)
    if _DOTTED_PATH_RE.match(text):
        parts = text.split(".")
        if owner_model is not None and parts[0] == owner_model.name:
            parts = parts[1:]  # a leading owner-model qualifier is a self-reference
        if len(parts) == 1:
            return column_default_key(
                path=tuple(owner_path), leaf=parts[0], base=owner_model,
            )
        terminal = (
            walk_key_path(model=owner_model, path=tuple(parts[:-1]), bundle=bundle)
            if owner_model is not None and bundle is not None else None
        )
        return column_default_key(
            path=tuple(owner_path) + tuple(parts[:-1]), leaf=parts[-1], base=terminal,
        )
    return None


def expr_default_ref_keys(
    *, sql: str, owner_model: Optional[SlayerModel], owner_path: Path,
    bundle: Optional[ResolvedSourceBundle],
) -> List[Optional[ValueKey]]:
    """Parse-based column refs of an expression default, as keys in the host's
    coordinates. ``None`` entries — an unresolvable qualifier, or an unanalysable
    fragment — fail closed at typing."""
    if owner_model is None or bundle is None:
        return []
    refs = compute_expr_reference_columns(
        canonical_sql=sql, anchor_model=owner_model,
        anchor_relation=owner_model.name, bundle=bundle,
    )
    if refs is None:
        return [None]
    return [
        column_default_key(
            path=tuple(owner_path) + path, leaf=leaf,
            base=walk_key_path(model=owner_model, path=path, bundle=bundle),
        )
        if path is not None else None
        for path, leaf in refs
    ]


def resolve_aggregation_params(
    *, agg: AggregateKey, owner_model: Optional[SlayerModel], owner_path: Path,
    bundle: Optional[ResolvedSourceBundle] = None,
) -> List[ParamSpec]:
    """Every aggregation parameter that references data — explicit non-scalar
    kwargs and non-overridden definition defaults resolved on the owner
    (terminal of the source path). Literal params are omitted."""
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
            )) is not None
        )
    return out


def _default_param_spec(
    *, p: AggregationParam, owner_model: SlayerModel, owner_path: Path,
    bundle: Optional[ResolvedSourceBundle],
) -> Optional[ParamSpec]:
    """A non-overridden definition default → its ``ParamSpec`` (a bound key, or a
    lifted expression with its referenced columns), or ``None`` when it rides the
    plain kwarg/default machinery unchanged."""
    vk = default_param_value_key(
        sql=p.sql, owner_path=owner_path, owner_model=owner_model, bundle=bundle,
    )
    if vk is not None:
        return ParamSpec(name=p.name, key=vk, expr_sql=None)
    refs = expr_default_ref_keys(
        sql=p.sql, owner_model=owner_model, owner_path=owner_path, bundle=bundle,
    )
    if refs:
        return ParamSpec(name=p.name, key=None, expr_sql=p.sql, expr_refs=tuple(refs))
    return None


# ---------------------------------------------------------------------------
# Aggregate-input closure (replaces compute_aggregate_input_join_paths).
# ---------------------------------------------------------------------------


def aggregate_input_closure(
    *, key: AggregateKey, anchor_model: Optional[SlayerModel],
    anchor_relation: str, bundle: ResolvedSourceBundle,
    include_source: bool = True, descend_aggregates: bool = False,
) -> Optional[Tuple[Path, ...]]:
    """The dependency closure of an aggregate's inputs — its source (when
    ``include_source``), positional/keyword arguments, a measure-level column
    filter, and non-overridden definition defaults — recursively through derived
    definitions. ``None`` when any dependency cannot be analysed (fail closed);
    ``()`` when purely local. Safety mode (``descend_aggregates=False``) treats an
    ``AggregateKey``-valued input as opaque — its inputs belong to its own
    producer; discovery mode descends into it."""
    if anchor_model is None:
        return ()
    seen: "dict[Path, None]" = {}

    def _add(paths) -> None:
        for p in paths:
            if p:
                seen.setdefault(tuple(p), None)

    if key.column_filter_key is not None:
        # The filter's crossed paths are stamped OWNER-relative on the key at bind
        # time (already derived-expanded via compute_column_filter_join_paths, and
        # tolerant of internal ``__`` aliases the strict expander would reject);
        # re-anchor by prefixing the source path, as filter_reachability does.
        source_path = tuple(getattr(key.source, "path", ()) or ())
        for p in key.column_filter_key.referenced_join_paths:
            _add(_prefixes(source_path + tuple(p)))

    refs: List[object] = [
        *([key.source] if include_source else []),
        *key.args,
        *(v for _, v in key.kwargs),
    ]
    for ref in refs:
        if isinstance(ref, AggregateKey) and not descend_aggregates:
            continue
        c = key_closure(
            key=ref, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if c is None:
            return None
        _add(c)

    # Non-overridden definition defaults, resolved in the aggregate's ROOT frame
    # (the anchor) so a default naming the root's own model resolves locally after
    # the home rule widens the home, rather than via a reverse hop; explicit kwargs
    # already rode the refs loop above.
    explicit = {name for name, _ in key.kwargs}
    for spec in resolve_aggregation_params(
        agg=key, owner_model=anchor_model, owner_path=(), bundle=bundle,
    ):
        if spec.name in explicit:
            continue
        c = _param_spec_closure(
            spec, anchor_model=anchor_model, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if c is None:
            return None
        _add(c)
    return tuple(seen)


def first_unanalyzable_input_column(
    *, key: AggregateKey, anchor_model: Optional[SlayerModel],
    anchor_relation: str, bundle: ResolvedSourceBundle, include_source: bool = True,
) -> Optional[str]:
    """The column name of the first aggregate input whose derived definition no
    dialect can analyse (names the ``check_input_dependencies_analyzable`` error),
    else ``None``."""
    if anchor_model is None:
        return None
    refs: List[object] = [
        *([key.source] if include_source else []),
        *key.args,
        *(v for _, v in key.kwargs),
    ]
    explicit = {name for name, _ in key.kwargs}
    for spec in resolve_aggregation_params(
        agg=key, owner_model=anchor_model, owner_path=(), bundle=bundle,
    ):
        if spec.name in explicit:
            continue
        if spec.key is not None:
            refs.append(spec.key)
        refs.extend(r for r in spec.expr_refs if r is not None)
    for ref in refs:
        if not isinstance(ref, ColumnSqlKey):
            continue
        path = tuple(ref.path or ())
        terminal = bundle.models_by_name.get(ref.model)
        if terminal is None and ref.model == anchor_model.name:
            terminal = anchor_model
        col = _derived_column(terminal, ref.column_name)
        if col is not None and fragment_closure(
            sql=col.sql, model=terminal, owner_path=path,
            anchor_relation="__".join(path) if path else anchor_relation,
            bundle=bundle,
        ) is None:
            return ref.column_name
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
