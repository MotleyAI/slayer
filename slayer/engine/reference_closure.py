"""The one dependency closure (python.engine.arc42.md principle 10).

A reference's *dependency closure* is its own relationship path plus every relationship path the
definition of any derived field it names crosses, recursively. Every planner
predicate that classifies a reference's row-level data dependencies — input
safety, attributability, grain determination, filter disposition — consumes this
closure, so a derived reference behaves exactly like a structural one in every
position. The closure is tri-state: ``None`` when a definition cannot be
analysed (no dialect parses it, or the expansion fails), and every consumer
treats ``None`` as unsafe, never as "crosses nothing".
"""

from __future__ import annotations

from decimal import Decimal
from typing import List, Optional, Tuple

import sqlglot
from sqlglot import exp

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    CastKey,
    ListSetKey,
    FieldKey,
    FieldSqlKey,
    InKey,
    ArrayLiteralKey,
    LiteralKey,
    ScalarCallKey,
    SqlFragmentKey,
    StarKey,
    TimePointCmpKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    source_row_leaves,
)
from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    CircularRelationshipPathError,
    SlayerError,
    UnreachableDefinitionError,
)
from slayer.core.relationship_walker import canonical_path, walk
from slayer.core.models import DatasetField, SlayerDataset
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.field_expansion import (
    DatasetFieldCycleError,
    expand_derived_refs_sync,
    is_trivial_base,
    reference_sites,
    root_scope_field_ids,
)

Path = Tuple[str, ...]


# The planner doesn't carry the datasource dialect, so a backend-specific
# ``DatasetField.sql`` / filter (MySQL backticks, T-SQL brackets, ClickHouse fns) could
# fail the Postgres parse and silently return no paths. Try each dialect; a
# fragment only fails closed when EVERY dialect rejects it.
_PLANNER_PARSE_DIALECT_CHAIN: Tuple[Optional[str], ...] = (
    "postgres", None, "mysql", "clickhouse", "bigquery", "tsql",
)


def parse_fragment_any_dialect(sql: str) -> Optional[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage] — sqlglot ships no __all__
    """First successful parse across the dialect chain, else ``None``."""
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        try:
            return sqlglot.parse_one(sql, dialect=dialect)  # pyright: ignore[reportReturnType] — parse_one's Expr TypeVar
        except Exception:
            continue
    return None


def _expand_derived_refs_any_dialect(
    *, sql: str, dataset: SlayerDataset, alias_path: str, bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """First successful ``expand_derived_refs_sync`` across the chain, else ``None``."""
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        if dialect is None:
            continue
        try:
            expanded = expand_derived_refs_sync(
                sql=sql, dataset=dataset, alias_path=alias_path,
                datasets_by_name=bundle.datasets_by_name, dialect=dialect,
            )
        except (DatasetFieldCycleError, CircularRelationshipPathError, UnreachableDefinitionError):
            raise
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
    *, sql: Optional[str], dataset: SlayerDataset, owner_path: Path,
    anchor_relation: str, bundle: ResolvedSourceBundle,
    cache: "Optional[dict]" = None,
) -> Optional[Tuple[Path, ...]]:
    """The root-relative relationship-path prefixes a free-SQL fragment crosses, derived
    definitions expanded recursively. ``owner_path`` is the fragment owner's
    root-relative path (prefixed onto every crossing). ``()`` = analysed and
    local; ``None`` = no dialect could analyse it (fail closed). A cyclic
    definition raises ``DatasetFieldCycleError``. ``cache`` (optional, plan-scoped)
    memoises the dialect parse + derived expansion, the loop's expensive part,
    keyed by ``(dataset, sql, owner_path, anchor_relation)``."""
    if not sql:
        return ()
    ck = ("fragment_closure", dataset.name, sql, tuple(owner_path), anchor_relation)
    if cache is not None and ck in cache:
        return cache[ck]
    result: Optional[Tuple[Path, ...]] = None
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        if dialect is None:
            continue  # expand_derived_refs_sync requires a dialect string
        sink = _OrderedSink()
        try:
            expanded = expand_derived_refs_sync(
                sql=sql, dataset=dataset, alias_path=anchor_relation,
                owner_path=tuple(owner_path), datasets_by_name=bundle.datasets_by_name,
                dialect=dialect, crossed_paths=sink,
            )
        except (DatasetFieldCycleError, CircularRelationshipPathError, UnreachableDefinitionError):
            raise
        except Exception:
            continue
        if expanded is not None:
            result = sink.paths
            break
    if cache is not None:
        cache[ck] = result
    return result


#: Strict fragment nodes (NULL in, NULL out): a field, arithmetic, a binary
#: comparison, a cast — never a function call, CASE, IS, AND/OR or a row tuple.
_NULL_PROPAGATING_NODES = (
    exp.Column, exp.Dot, exp.Identifier, exp.Literal, exp.Null, exp.Boolean,
    exp.Neg, exp.Paren, exp.Not, exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod,
    exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Like, exp.ILike,
    exp.Cast, exp.TryCast, exp.DataType, exp.DataTypeParam,
)


def _node_propagates_null(node: object) -> bool:
    """IN / BETWEEN are OR / AND over their left operand: strict only when it is
    field-valued (``1 IN (value, 1)`` is TRUE under a NULL value) and, for IN,
    the list is non-empty (``NULL IN ()`` is FALSE on SQLite)."""
    if isinstance(node, (exp.In, exp.Between)):
        lhs_columned = any(isinstance(n, exp.Column) for n in node.this.walk())
        return lhs_columned and (not isinstance(node, exp.In) or bool(node.expressions))
    return isinstance(node, _NULL_PROPAGATING_NODES)


def fragment_null_propagates(
    *, field: DatasetField, dataset: SlayerDataset, anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> bool:
    """Is a derived field NULL whenever its owner row is? True for
    a bare field, arithmetic or comparison over ≥1 field once derived references
    are expanded; a function call, CASE, literal-only or unparseable definition
    is data-dependent."""
    if is_trivial_base(field=field):
        return True
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        if dialect is None:
            continue
        try:
            expanded = expand_derived_refs_sync(
                sql=field.sql, dataset=dataset, alias_path=anchor_relation,
                datasets_by_name=bundle.datasets_by_name, dialect=dialect,
            )
            nodes = list(sqlglot.parse_one(expanded or "", dialect=dialect).walk())
        except (DatasetFieldCycleError, CircularRelationshipPathError, UnreachableDefinitionError):
            raise
        except Exception:
            continue
        return any(isinstance(n, exp.Column) for n in nodes) and all(
            _node_propagates_null(n) for n in nodes
        )
    return False


def _prefixes(path: Path) -> List[Path]:
    """Every non-empty prefix of ``path`` (reachability is judged per hop)."""
    return [tuple(path[: i + 1]) for i in range(len(path))]


# Values a key tree carries INLINE — plain data, never references.
_INLINE_SCALARS = (str, int, float, bool, Decimal)
_LEAF_KINDS = (LiteralKey, ArrayLiteralKey, StarKey, FieldKey, FieldSqlKey)


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
    ``filter_reachability``). Fails closed on an unknown kind. A ``DatasetField.filter``
    rides its source ``FieldSqlKey``, whose closure walks both value and filter.
    ``descend_aggregates=False`` stops at an aggregate (routed by where it is
    computed, not by its inputs)."""
    if isinstance(node, _LEAF_KINDS) or isinstance(node, _INLINE_SCALARS):
        return []
    if isinstance(node, TimeTruncKey):
        return [node.source]
    if isinstance(node, AggregateKey):
        if not descend_aggregates:
            return []
        return [node.source, *node.args, *(v for _name, v in node.kwargs), *([node.filter] if node.filter else [])]
    if isinstance(node, TransformKey):
        return [node.input, *sorted(node.partition_keys, key=repr), node.time_key]
    if isinstance(node, ArithmeticKey):
        return list(node.operands)
    if isinstance(node, ScalarCallKey):
        return list(node.args)
    if isinstance(node, InKey):
        return [node.field, *node.values]
    if isinstance(node, TimePointCmpKey):
        return [node.operand]
    if isinstance(node, SqlFragmentKey):
        return list(node.refs)
    if isinstance(node, (CastKey, ListSetKey)):
        return [node.source]
    raise UnhandledValueKindError(node)


def _terminal_dataset(
    node: FieldSqlKey, *, anchor_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
) -> Optional[SlayerDataset]:
    """The dataset a ``FieldSqlKey`` names, falling back to the anchor for its own name."""
    terminal = bundle.datasets_by_name.get(node.dataset)
    if terminal is None and node.dataset == anchor_dataset.name:
        return anchor_dataset
    return terminal


def _definition_fragments(col) -> List[str]:
    """The Mode-A fragments a field's definition reads: its non-trivial ``sql`` and its ``filter``."""
    return [
        sql for sql in (
            col.sql if col.sql is not None and not is_trivial_base(field=col) else None,
            col.filter,
        )
        if sql
    ]


def _definition_ref(
    *, key: FieldSqlKey, owner: SlayerDataset, col: DatasetField, quals: Path, leaf: str,
    bundle: ResolvedSourceBundle,
) -> Optional[ValueKey]:
    """One definition reference as an anchor-relative field key, else ``None``."""
    hops = quals[1:] if quals[:1] == (owner.name,) else quals
    datasets = {**bundle.datasets_by_name, owner.name: owner}
    try:
        chain = walk(root=owner, path=hops, datasets_by_name=datasets)
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None
    if chain is None:
        return None
    target = datasets.get(chain[-1].to) if chain else owner
    path = (*key.path, *canonical_path(chain))
    if target is owner and leaf == col.name:
        return FieldKey(path=path, leaf=leaf)  # a filter reading its own physical column
    if target is None or target.get_field(leaf) is None:
        return None
    return field_default_key(path=path, leaf=leaf, base=target)


def definition_refs(
    *, key: FieldSqlKey, anchor_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
) -> Optional[List[ValueKey]]:
    """The field keys a derived field's value and ``DatasetField.filter`` read, at
    anchor-relative paths; ``None`` when the definition cannot be analysed."""
    owner = _terminal_dataset(key, anchor_dataset=anchor_dataset, bundle=bundle)
    col = owner.get_field(key.field_name) if owner is not None else None
    if owner is None or col is None:
        return None
    out: List[ValueKey] = (
        [FieldKey(path=key.path, leaf=col.name)] if is_trivial_base(field=col) else []
    )
    for sql in _definition_fragments(col):
        parsed = parse_fragment_any_dialect(sql)
        if parsed is None:
            return None
        for _node, quals, leaf in reference_sites(parsed, root_scope_field_ids(parsed=parsed)):
            ref = _definition_ref(
                key=key, owner=owner, col=col, quals=tuple(quals), leaf=leaf, bundle=bundle,
            )
            if ref is None:
                return None
            out.append(ref)
    return out


def _field_key_closure(
    node, *, anchor_dataset: SlayerDataset, anchor_relation: str,
    bundle: ResolvedSourceBundle, cache: "Optional[dict]" = None,
) -> Optional[List[Path]]:
    """Own path prefixes of a ``FieldSqlKey``/``FieldKey`` plus the fragment
    closure of its definition — the value's ``sql`` (when non-trivial) AND its
    ``DatasetField.filter``, each anchored at the field's owner. Tri-state
    ``None`` propagates (a dependency that cannot be analysed)."""
    path = tuple(getattr(node, "path", ()) or ())
    out: List[Path] = list(_prefixes(path))
    if not isinstance(node, FieldSqlKey):
        return out
    terminal = _terminal_dataset(node, anchor_dataset=anchor_dataset, bundle=bundle)
    col = terminal.get_field(node.field_name) if terminal is not None else None
    if terminal is None or col is None:
        return out
    owner_relation = "__".join(path) if path else anchor_relation
    for sql in _definition_fragments(col):
        try:
            frag = fragment_closure(
                sql=sql, dataset=terminal, owner_path=path,
                anchor_relation=owner_relation, bundle=bundle, cache=cache,
            )
        except UnreachableDefinitionError as exc:
            raise exc.labelled(f"field {col.name!r}") from None
        if frag is None:
            return None
        out.extend(frag)
    return out


def key_closure(
    *, key, anchor_dataset: SlayerDataset, anchor_relation: str,
    bundle: ResolvedSourceBundle, cache: "Optional[dict]" = None,
) -> Optional[Tuple[Path, ...]]:
    """Every relationship path ``key``'s dependency tree crosses, anchored at
    ``anchor_relation`` — its structural paths plus, recursively, every path the
    definition of a derived field it names crosses. Insertion-ordered,
    de-duplicated; ``()`` = local; ``None`` = a dependency could not be analysed
    (fail closed). A cyclic derived definition raises ``DatasetFieldCycleError``."""
    return _closure(
        key, anchor_dataset=anchor_dataset, anchor_relation=anchor_relation,
        bundle=bundle, cache=cache, opaque=(),
    )


def _closure(
    key, *, anchor_dataset: SlayerDataset, anchor_relation: str,
    bundle: ResolvedSourceBundle, cache: "Optional[dict]", opaque: Tuple[type, ...],
) -> Optional[Tuple[Path, ...]]:
    """``key_closure`` with nodes of the ``opaque`` kinds contributing nothing."""
    seen: "dict[Path, None]" = {}

    def _walk(node) -> bool:
        if node is None or isinstance(node, opaque):
            return True
        leafs = _leaf_closure(node, anchor_dataset=anchor_dataset,
                              anchor_relation=anchor_relation, bundle=bundle,
                              cache=cache)
        if leafs is None:
            return False
        for path in leafs:
            if path:
                seen.setdefault(tuple(path), None)
        return all(_walk(child) for child in _child_keys(node))

    if not _walk(key):
        return None
    return tuple(seen)


def _leaf_closure(
    node, *, anchor_dataset: SlayerDataset, anchor_relation: str,
    bundle: ResolvedSourceBundle, cache: "Optional[dict]" = None,
) -> Optional[List[Path]]:
    """Paths a node contributes ITSELF (not via children). ``None`` = unanalysable."""
    if isinstance(node, FieldSqlKey):
        return _field_key_closure(
            node, anchor_dataset=anchor_dataset, anchor_relation=anchor_relation,
            bundle=bundle, cache=cache,
        )
    if isinstance(node, FieldKey):
        return _prefixes(tuple(node.path))
    return []




def field_default_key(
    *, path: Path, leaf: str, base: Optional[SlayerDataset],
) -> ValueKey:
    """A ``FieldSqlKey`` when ``leaf`` names a derived or filtered field on
    ``base`` (so its definition expands and its crossings close), else a plain
    ``FieldKey``."""
    if base is not None:
        col = next((c for c in (base.fields or []) if c.name == leaf), None)
        if col is not None and col.needs_expansion:
            return FieldSqlKey(path=path, dataset=base.name, field_name=leaf)
    return FieldKey(path=path, leaf=leaf)


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


def _refs_closure(
    *, refs: List[object], anchor_dataset: SlayerDataset,
    anchor_relation: str, bundle: ResolvedSourceBundle,
) -> Optional[List[Path]]:
    """The combined closure of a list of input refs, attached constituents opaque
    (their inputs belong to their own producer, Axiom 2.3); ``None`` when any ref is
    unanalysable."""
    out: List[Path] = []
    for ref in refs:
        c = _closure(
            ref, anchor_dataset=anchor_dataset, anchor_relation=anchor_relation,
            bundle=bundle, cache=None, opaque=(AggregateKey, TransformKey),
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
    *, key: AggregateKey, anchor_dataset: Optional[SlayerDataset],
    anchor_relation: str, bundle: ResolvedSourceBundle,
    include_source: bool = True,
) -> Optional[Tuple[Path, ...]]:
    """The dependency closure of an aggregate's inputs — its source (when
    ``include_source``), positional/keyword arguments, and non-overridden
    definition defaults — recursively through derived definitions. A source
    field's ``DatasetField.filter`` rides its ``FieldSqlKey`` source, so
    the source closure covers it. ``None`` when any dependency cannot be analysed
    (fail closed, short-circuiting on the first unanalysable component); ``()``
    when purely local. Attached constituents (aggregates, transforms) are opaque:
    their inputs belong to their own producer."""
    if anchor_dataset is None:
        return ()
    seen: "dict[Path, None]" = {}
    if not _merge_paths(seen=seen, part=_refs_closure(
        refs=_explicit_input_refs(key=key, include_source=include_source),
        anchor_dataset=anchor_dataset,
        anchor_relation=anchor_relation, bundle=bundle,
    )):
        return None
    return tuple(seen)


def _unanalyzable_derived_name(
    *, ref: object, anchor_dataset: SlayerDataset, anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """``ref``'s field name when its definition — derived ``sql`` OR
    ``DatasetField.filter`` — is one no dialect can analyse, else ``None``."""
    if not isinstance(ref, FieldSqlKey):
        return None
    path = tuple(ref.path or ())
    terminal = bundle.datasets_by_name.get(ref.dataset)
    if terminal is None and ref.dataset == anchor_dataset.name:
        terminal = anchor_dataset
    col = terminal.get_field(ref.field_name) if terminal is not None else None
    if terminal is None or col is None:
        return None
    owner_relation = "__".join(path) if path else anchor_relation
    for sql in (
        col.sql if col.sql is not None and not is_trivial_base(field=col) else None,
        col.filter,
    ):
        if sql and fragment_closure(
            sql=sql, dataset=terminal, owner_path=path,
            anchor_relation=owner_relation, bundle=bundle,
        ) is None:
            return ref.field_name
    return None


def first_unanalyzable_input_field(
    *, key: AggregateKey, anchor_dataset: Optional[SlayerDataset],
    anchor_relation: str, bundle: ResolvedSourceBundle, include_source: bool = True,
) -> Optional[str]:
    """The field name of the first aggregate input whose derived definition no
    dialect can analyse (best-effort diagnostic naming the
    ``check_input_dependencies_analyzable`` error — the safety DECISION is the
    closure's tri-state), else ``None``."""
    if anchor_dataset is None:
        return None
    refs = [
        leaf for ref in _explicit_input_refs(key=key, include_source=include_source)
        for leaf in (ref.refs if isinstance(ref, SqlFragmentKey) else (ref,))
    ]
    for ref in refs:
        name = _unanalyzable_derived_name(
            ref=ref, anchor_dataset=anchor_dataset, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if name is not None:
            return name
    return None


def source_row_leaf_closure(
    *, key: AggregateKey, anchor_dataset: Optional[SlayerDataset],
    anchor_relation: str, bundle: ResolvedSourceBundle,
) -> Optional[Tuple[Path, ...]]:
    """The dependency closure of an aggregate SOURCE's own ROW-level leaves, with
    attached constituents opaque (Axiom 2.3) — unlike ``aggregate_input_closure``,
    which descends a nested aggregate inside an expression source. ``None`` when a
    leaf's derived definition cannot be analysed (fail closed); ``()`` = local."""
    if anchor_dataset is None:
        return ()
    seen: "dict[Path, None]" = {}
    for leaf in source_row_leaves(key.source):
        c = key_closure(
            key=leaf, anchor_dataset=anchor_dataset,
            anchor_relation=anchor_relation, bundle=bundle,
        )
        if c is None:
            return None
        for p in c:
            if p:
                seen.setdefault(tuple(p), None)
    return tuple(seen)


def first_unanalyzable_source_row_leaf(
    *, key: AggregateKey, anchor_dataset: Optional[SlayerDataset],
    anchor_relation: str, bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """Field name of the first source ROW leaf whose derived definition no dialect
    can analyse (diagnostic for ``check_input_dependencies_analyzable``)."""
    if anchor_dataset is None:
        return None
    for leaf in source_row_leaves(key.source):
        name = _unanalyzable_derived_name(
            ref=leaf, anchor_dataset=anchor_dataset, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if name is not None:
            return name
    return None


def first_unanalyzable_filter_field(
    *, key, anchor_dataset: Optional[SlayerDataset], anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """Field name of the first reference in a filter conjunct whose derived
    definition no dialect can analyse (diagnostic for
    ``check_filter_dependencies_analyzable``), else ``None``."""
    if anchor_dataset is None:
        return None
    stack: List[object] = [key]
    while stack:
        node = stack.pop()
        name = _unanalyzable_derived_name(
            ref=node, anchor_dataset=anchor_dataset, anchor_relation=anchor_relation,
            bundle=bundle,
        )
        if name is not None:
            return name
        stack.extend(_child_keys(node))
    return None
