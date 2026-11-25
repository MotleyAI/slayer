"""Expansion of row definitions (a field's native SQL, its ``filter``, a dataset's ``filters``).

Inside a row definition a bare name (or ``source.x``) reads the dataset's source column and a path from the dataset
(``orders.status``, ``customers.name``) reads a field. Source columns are qualified to their path alias; a field a path
reads is qualified when it is a bare base field and inlined, recursively, when it is derived or filtered. The expansion
runs during binding/planning, so the SQL generator never sees unresolved field references.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Dict, List, Optional, Protocol, Set, Tuple, cast

import sqlglot
from pydantic import BaseModel, ConfigDict, SkipValidation
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.expressions.core import Expression

from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    CircularRelationshipPathError,
    DatasetFieldCycleError,
    LegacyDunderAliasError,
    ModeASqlParseError,
    UnreachableDefinitionError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.relationship_search import unreachable_target
from slayer.core.relationship_walker import (
    canonical_path,
    follow,
    resolve_hop,
    walk_miss,
    walk_terminal,
)
from slayer.core.models import DatasetField, SlayerDataset, is_source_qualifier
from slayer.core.scope import resolve_generated_field
from slayer.core.syntax import drop_root_hop
from slayer.core.vocabulary import old_keywords
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.sql.root_scope import root_scope_field_ids
from slayer.sql.zoned_reads import STORED, VALUE, Context, ReadSite, field_for_column

__all__ = [
    "reads_source_column",
    "collect_root_scope_joined_paths",
    "expand_derived_refs_ast",
    "expand_field_definition_ast",
    "expand_field_definition_parts_sync",
    "expand_field_definition_sync",
    "expand_derived_refs_sync",
    "is_trivial_base",
    "raise_if_legacy_split_alias",
    "reference_sites",
    "resolve_ref_target",
    "root_scope_field_ids",
    "wrap_field_filter",
]

Path = Tuple[str, ...]



def is_trivial_base(*, field: DatasetField) -> bool:
    """A field is "trivial base" iff its definition is missing or is just its own
    bare name. These need no expansion — only re-qualification.
    """
    if field.definition_sql is None:
        return True
    sql = field.definition_sql.strip()
    # A double-quoted self-identity (``"legalEntityType"`` for a field named
    # ``legalEntityType``) is still a bare base reference — required to point
    # at a mixed-case physical column on case-folding dialects. Strip the
    # surrounding identifier quotes before comparing so it is not mistaken
    # for a derived expression (which would self-recurse into a false cycle).
    if len(sql) >= 2 and sql[0] == '"' and sql[-1] == '"':
        sql = sql[1:-1].replace('""', '"')
    return sql == field.name


class _PathSink(Protocol):
    """A duck-typed ordered collector of relationship-path tuples (``.add(path)``), so
    a caller can pass its own insertion-ordered set (``ScopeFrame.relationship_paths``)
    and receive the crossed paths in emission order."""

    def add(self, path: Tuple[str, ...], /) -> None: ...


#: The read path: a qualified column read at its site, as SLayer reads it there.
ReadHook = Callable[[exp.Column, ReadSite], Expression]


class _Sinks(BaseModel):
    """The collectors one expansion feeds — each crossed relationship-path prefix, each dataset path read — and the
    read path its column reads go through, in their context."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    crossed: SkipValidation[Optional[_PathSink]] = None
    reads: SkipValidation[Optional[_PathSink]] = None
    read: SkipValidation[Optional[ReadHook]] = None
    context: Context = VALUE


def _record(sink: Optional[_PathSink], path: Tuple[str, ...]) -> None:
    if sink is not None:
        sink.add(path)


def _read(
    node: Expression, *, sinks: _Sinks, dataset: SlayerDataset, field: Optional[DatasetField], path: Path,
) -> Expression:
    """``node`` (a qualified column read) through the read path, replaced in its tree."""
    if sinks.read is None or not isinstance(node, exp.Column):
        return node
    stub = exp.Placeholder()
    node.replace(stub)
    site = ReadSite(dataset=dataset, field=field, column=node.name, path=path, context=sinks.context, reads_naive=False)
    out = sinks.read(node, site)
    stub.replace(out)
    return out


# ---------------------------------------------------------------------------
# The shared Mode-A qualifier resolver.
#
# The dotted-canonical flip makes ``.`` the ONLY chain separator in Mode-A free
# SQL. A qualifier is EITHER an exact name (the host, or a directly-joined dataset
# — which MAY contain ``__``) OR a dotted chain of exact relationship hops. User input
# is never split on ``__``. The legacy ``customers__regions`` split-alias
# spelling — which does not name a real relationship target but whose naive split walks
# the graph — is a hard D2 error (:class:`LegacyDunderAliasError`).
# ---------------------------------------------------------------------------

DatasetsByName = Dict[str, SlayerDataset]


def _identifier_chain(node: Expression) -> Optional[List[str]]:
    """The full identifier chain of a pure-identifier reference.

    A ``Column`` (2–4 name parts) yields its ``parts``; a nested ``Dot`` (5+
    parts, ``aa.bb.cc.dd.v`` [C4]) is unwrapped recursively. Returns ``None``
    for anything that is not a pure identifier chain (a call, arithmetic, …).
    """
    if isinstance(node, exp.Column):
        parts = [p.name for p in node.parts]
        return parts or None
    if isinstance(node, exp.Dot):
        left = _identifier_chain(node.this)
        right = node.expression
        if left is None or not isinstance(right, exp.Identifier):
            return None
        return [*left, right.name]
    return None


def _dot_reference_sites(
    parsed: Expression, root_scope_ids: Set[int],
) -> Tuple[List[Tuple[Expression, Tuple[str, ...], str]], Set[int]]:
    """Root-scope references that parse as an outermost ``exp.Dot`` (5+-part
    refs), plus the ids of the ``Column`` nodes they consume so the plain-Column
    pass in :func:`reference_sites` does not double-count them."""
    sites: List[Tuple[Expression, Tuple[str, ...], str]] = []
    consumed: Set[int] = set()
    for dot in parsed.find_all(exp.Dot):
        chain = _identifier_chain(dot)
        if chain is None or len(chain) < 2:
            continue
        parent = dot.parent
        if isinstance(parent, exp.Dot) and _identifier_chain(parent) is not None:
            continue  # not the outermost dot
        inner = dot.find(exp.Column)
        if inner is None or id(inner) not in root_scope_ids:
            continue
        sites.append((dot, tuple(chain[:-1]), chain[-1]))
        for c in dot.find_all(exp.Column):
            consumed.add(id(c))
    return sites, consumed


def reference_sites(
    parsed: Expression, root_scope_ids: Set[int],
) -> List[Tuple[Expression, Tuple[str, ...], str]]:
    """Yield ``(node, qualifiers, leaf)`` for every root-scope reference.

    A 5+-part reference parses as an outer ``exp.Dot`` wrapping a ``Column``;
    a 2–4-part (or bare) reference is a plain ``Column``. Both are surfaced,
    with the wrapping ``Dot`` preferred so its inner ``Column`` is not double-
    counted.
    """
    sites, consumed = _dot_reference_sites(parsed, root_scope_ids)
    for col in parsed.find_all(exp.Column):
        if id(col) in consumed or id(col) not in root_scope_ids:
            continue
        parts = [p.name for p in col.parts]
        sites.append((col, tuple(parts[:-1]), parts[-1]))
    return sites


@old_keywords({"source_model": "source_dataset", "models_by_name": "datasets_by_name"})
def resolve_ref_target(
    *,
    qualifiers: Tuple[str, ...],
    source_dataset: SlayerDataset,
    datasets_by_name: DatasetsByName,
    quoted: Sequence[bool] = (),
) -> Optional[SlayerDataset]:
    """Resolve a Mode-A qualifier chain (``quoted`` flags each quoted hop) to its target dataset for best-effort
    save-time inspectors (schema-drift cascade, field-dependency cycle walk).

    Exact-name-first: an empty chain or the host's own name → the host; a single
    exact hop token (which MAY contain ``__``) or a dotted chain of exact hops
    → the terminal dataset, in either traversal direction. Never ``__``-splits —
    the legacy split-alias is a hard D2 error at the runtime / save-time door,
    so here it simply fails to resolve (best-effort skip). Returns ``None``
    when a hop is not a relationship / not reachable / ambiguous / revisiting.
    """
    quals = drop_root_hop(qualifiers, source_dataset.spelling, head_quoted=True)
    if not quals:
        return source_dataset
    flags = tuple(quoted)[len(qualifiers) - len(quals):]
    try:
        walked = walk_terminal(root=source_dataset, path=quals, datasets_by_name=datasets_by_name, quoted=flags)
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None
    return walked[0] if walked is not None else None


def _resolve_qualifiers(
    *,
    qualifiers: Tuple[str, ...],
    leaf: str,
    source_dataset: SlayerDataset,
    owner_alias: str,
    datasets_by_name: DatasetsByName,
    field: Optional[str] = None,
) -> Optional[Tuple[str, ...]]:
    """Classify a Mode-A qualifier chain.

    Returns:
      * ``()`` — the reference is anchored on the host / owner dataset;
      * a non-empty path tuple — the resolved canonical relationship path;
      * ``None`` — an opaque qualifier (CTE / subquery / physical
        ``schema.table.column``), left untouched.

    Raises :class:`LegacyDunderAliasError` (D2) for a split-alias spelling,
    :class:`UnresolvableDimensionRelationshipError` for a chain with a broken hop,
    :class:`AmbiguousRelationshipPathError` for an ambiguous hop, and
    :class:`CircularRelationshipPathError` (re-raised with the complete pre-strip
    ``reference`` and the ``field`` being expanded) for a revisiting hop — all
    fail closed.
    """
    root = owner_alias if tuple(qualifiers[:1]) == (owner_alias,) else source_dataset.spelling
    path = drop_root_hop(qualifiers, root, head_quoted=True)
    if not path:
        return ()
    # A definition is judged from its dataset as stored: it never sees a query frame's product hops.
    source_dataset = source_dataset.with_unrelated(frozenset()) if source_dataset.unrelated_datasets else source_dataset
    dataset = unreachable_target(root=source_dataset, token=path[0], datasets_by_name=datasets_by_name)
    if dataset is not None:
        raise UnreachableDefinitionError(
            dataset=source_dataset.name, reference=".".join((*qualifiers, leaf)), unreached=dataset,
            definition=f"field {field!r}" if field else None,
        )
    # A revisit surfaces either from the direct walk or from the legacy
    # ``a__b`` split-alias probe; re-raise both with the complete pre-strip
    # reference and the field being expanded.
    try:
        walked = walk_terminal(root=source_dataset, path=path, datasets_by_name=datasets_by_name)
        if walked is not None:
            return walked[1]
        if len(path) == 1:
            raise_if_legacy_split_alias(
                qualifier=path[0], leaf=leaf,
                source_dataset=source_dataset, datasets_by_name=datasets_by_name,
            )
            return None  # opaque single qualifier
        _raise_if_broken_relationship_walk(
            path=path, leaf=leaf,
            source_dataset=source_dataset, datasets_by_name=datasets_by_name,
        )
        return None
    except CircularRelationshipPathError as exc:
        raise CircularRelationshipPathError(
            reference=".".join((*qualifiers, leaf)),
            root_dataset=source_dataset.spelling,
            revisited=exc.revisited, hop=exc.hop, via=exc.via,
            field=field,
        ) from exc


def raise_if_legacy_split_alias(
    *,
    qualifier: str,
    leaf: str,
    source_dataset: SlayerDataset,
    datasets_by_name: DatasetsByName,
) -> None:
    """D2 door: a single opaque qualifier ``a__b`` that naive-splits into a real
    relationship walk is the legacy split-alias form — raise :class:`LegacyDunderAliasError`
    naming the dotted replacement. A qualifier without ``__`` (or whose split is
    not a relationship walk) is left opaque (no raise)."""
    if "__" not in qualifier:
        return
    naive = tuple(qualifier.split("__"))
    try:
        walkable = walk_terminal(root=source_dataset, path=naive, datasets_by_name=datasets_by_name) is not None
    except AmbiguousRelationshipPathError:
        walkable = True  # ambiguously walkable is still the legacy spelling
    if walkable:
        raise LegacyDunderAliasError(
            alias=qualifier, dotted=".".join((*naive, leaf)),
            dataset=source_dataset.name,
        )


def _raise_if_broken_relationship_walk(
    *,
    path: Tuple[str, ...],
    leaf: str,
    source_dataset: SlayerDataset,
    datasets_by_name: DatasetsByName,
) -> None:
    """Multi-part chain whose first hop IS a relationship hop but a later hop fails:
    raise :class:`UnresolvableDimensionRelationshipError` naming the failing hop. A chain
    whose first hop is NOT a relationship hop is opaque (physical
    ``schema.table.column`` — relationship-target-beats-schema precedence) and returns
    without raising."""
    datasets = {**datasets_by_name, source_dataset.name: datasets_by_name.get(source_dataset.name, source_dataset)}
    if resolve_hop(current=source_dataset, token=path[0], datasets_by_name=datasets) is None:
        return
    miss = walk_miss(root=source_dataset, path=path, datasets_by_name=datasets)
    if miss is None:
        return
    raise UnresolvableDimensionRelationshipError(
        reference=".".join((*path, leaf)),
        root_dataset=source_dataset.spelling,
        reason={
            "no_edge": f"'{miss.token}' is not a joined dataset on the preceding hop.",
            "unloaded": f"joined dataset '{miss.token}' is not in the resolved bundle.",
            "spine": f"'{miss.token}' would cross the time spine, which a path may end at but never pass through.",
        }[miss.kind],
    )


def _lenient_path(
    *,
    qualifiers: Tuple[str, ...],
    source_dataset: SlayerDataset,
    owner_alias: str,
    datasets_by_name: DatasetsByName,
) -> Optional[Tuple[str, ...]]:
    """LENIENT qualifier resolution for the relationship-path SCANNER.

    Unlike :func:`_resolve_qualifiers` (STRICT — the expansion door that raises
    D2), the scanner runs over BOTH raw user forms AND already-expanded output
    whose qualifiers are legitimate internal ``__`` join aliases. It must not
    D2 (or fail-closed) those, so it resolves a ``__`` qualifier by exact-match
    first and then by naive split, and simply SKIPS (returns ``None``) anything
    that does not fully walk — an opaque CTE / subquery / physical reference,
    or an ambiguous hop (the strict door raises on those).
    """
    quals = list(qualifiers)
    if quals and quals[0] in (owner_alias, source_dataset.name):
        quals = quals[1:]
    if not quals:
        return ()
    try:
        walked = walk_terminal(root=source_dataset, path=quals, datasets_by_name=datasets_by_name)
        if walked is not None:
            return walked[1]
        if len(quals) == 1 and "__" in quals[0]:
            # An internal join alias spells a bound, possibly composed, path.
            chain = follow(root=source_dataset, path=quals[0].split("__"),
                           datasets_by_name={**datasets_by_name, source_dataset.name: source_dataset})
            return canonical_path(chain) if chain is not None else None
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None
    return None


def collect_root_scope_joined_paths(
    *,
    parsed: Expression,
    source_dataset: SlayerDataset,
    source_relation: str,
    bundle: ResolvedSourceBundle,
) -> List[Tuple[str, ...]]:
    """Collect the ordered de-duplicated list of relationship-path prefixes a parsed
    SQL fragment references in its root scope.

    Each ROOT-scope reference that resolves as a relationship walk on ``source_dataset``
    contributes its prefixes (path ``("a", "b")`` yields ``("a",)`` AND
    ``("a", "b")``). Host-anchored and opaque references (CTE / subquery
    aliases, physical ``schema.table.column``) contribute nothing.

    LENIENT (:func:`_lenient_path`): this scanner runs over already-expanded
    fragments whose qualifiers are internal ``__`` join aliases, so it never
    raises D2 — that is the STRICT expansion door's job. Shared by the SQL
    generator (``SQLGenerator._joined_paths_in_sql``) and the planner-side
    field filter discovery so the two surfaces agree on what counts as
    "crosses a relationship."
    """
    root_ids = root_scope_field_ids(parsed=parsed)
    datasets_by_name = bundle.datasets_by_name
    seen: Set[Tuple[str, ...]] = set()
    ordered: List[Tuple[str, ...]] = []
    for _node, quals, _leaf in reference_sites(parsed, root_ids):
        if quals and reads_source_column(quals, dataset=source_dataset):
            continue
        path = _lenient_path(
            qualifiers=quals, source_dataset=source_dataset,
            owner_alias=source_relation,
            datasets_by_name=datasets_by_name,
        )
        if not path:
            continue  # host-anchored or opaque
        for i in range(1, len(path) + 1):
            prefix = path[:i]
            if prefix not in seen:
                seen.add(prefix)
                ordered.append(prefix)
    return ordered


# ---------------------------------------------------------------------------
# Synchronous expansion
# ---------------------------------------------------------------------------
#
# The generator runs synchronously over a ``ResolvedSourceBundle`` that has
# already loaded every referenced dataset (P11: storage consulted once, up
# front), so it expands derived refs through a *sync* dataset resolver.
#
# Nothing awaits dataset resolution here — the expansion is fully synchronous.

#: Maps a root-relative relationship-path tuple to its internal alias string. The
#: generator/scope pass the WP3 registry (``AliasAllocator.alias_for``) so a
#: chain leaf and a literal ``__``-named dataset never share an alias; ``None``
#: falls back to the byte-identical legacy ``"__".join`` spelling.
AliasResolver = Callable[[Tuple[str, ...]], str]


def _alias_for_path(
    full_path: Tuple[str, ...],
    *,
    alias_resolver: Optional[AliasResolver],
) -> str:
    """The internal qualifier alias for a ROOT-relative ``full_path``.

    ``alias_resolver`` (the WP3 registry) mints a collision-safe alias;
    ``None`` falls back to the byte-identical legacy ``"__".join`` spelling."""
    return alias_resolver(full_path) if alias_resolver else "__".join(full_path)


def _requalify(node: Expression, *, alias: str, leaf: str) -> Expression:
    """Re-qualify a reference ``node`` to ``alias.leaf``.

    For a plain ``Column`` the leaf identifier is preserved IN PLACE (only the
    table qualifier is reset and any db/catalog cleared) so a quoted leaf
    (``customers."spend"``) keeps its quoting. A nested ``Dot`` (5+ parts) reuses
    its tail ``Identifier`` so a quoted / mixed-case leaf (``a.b.c."Spend"``)
    keeps its quoting on case-folding dialects too (CR)."""
    if isinstance(node, exp.Column):
        node.set("table", exp.to_identifier(alias))
        node.set("db", None)
        node.set("catalog", None)
        if node.name != leaf:  # a respelled stale leaf
            node.set("this", exp.to_identifier(leaf))
        return node
    tail = (
        node.expression.copy()
        if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Identifier)
        and node.expression.name == leaf
        else exp.to_identifier(leaf)
    )
    replacement = exp.column(tail, table=alias)
    node.replace(replacement)
    return replacement


def _locate_site_target(
    *,
    path: Tuple[str, ...],
    full_path: Tuple[str, ...],
    dataset: SlayerDataset,
    alias_path: str,
    datasets_by_name: DatasetsByName,
    alias_resolver: Optional[AliasResolver],
    crossed_paths: Optional[_PathSink],
) -> Optional[Tuple[SlayerDataset, str]]:
    """``(target_dataset, canonical_alias)`` of a resolved site path, recording crossed relationships."""
    if not path:
        return dataset, alias_path
    walked = walk_terminal(root=dataset, path=path, datasets_by_name=datasets_by_name)
    if walked is None:
        return None
    if crossed_paths is not None:
        for i in range(1, len(full_path) + 1):
            crossed_paths.add(full_path[:i])
    return walked[0], _alias_for_path(full_path, alias_resolver=alias_resolver)


def reads_siblings_first(field: DatasetField) -> bool:
    """A deprecated field ``sql`` not yet converted keeps its pre-2.0 meaning: a bare name reads a sibling field."""
    return field.sql is not None and not field.resolved


def _expanding_sibling(dataset: SlayerDataset, *, leaf: str) -> bool:
    col = resolve_generated_field(dataset=dataset, name=leaf, quoted=True)
    return col is not None and col.needs_expansion


def reads_source_column(qualifiers: Tuple[str, ...], *, dataset: SlayerDataset) -> bool:
    """Whether a row-definition reference with ``qualifiers`` reads the dataset's source column: bare, or ``source.``
    when no field is named ``source``."""
    if not qualifiers:
        return True
    return len(qualifiers) == 1 and is_source_qualifier(qualifiers[0], dataset=dataset)


def _process_reference_site(
    *,
    node: Expression,
    qualifiers: Tuple[str, ...],
    leaf: str,
    dataset: SlayerDataset,
    alias_path: str,
    owner_path: Tuple[str, ...],
    datasets_by_name: DatasetsByName,
    dialect: str,
    visited: Tuple[Tuple[str, str], ...],
    alias_resolver: Optional[AliasResolver],
    sinks: _Sinks,
    site: Optional[Tuple[str, str]] = None,
    sibling_first: bool = False,
) -> Optional[Expression]:
    """Resolve one row-definition reference: a source column is qualified in place, a path's base field qualified, a
    path's derived field inlined, an opaque reference left untouched. ``sibling_first`` reads a bare name as the
    sibling field it names when that field needs expansion (a deprecated, unconverted field ``sql``).

    ``owner_path`` is the ROOT-relative path of ``dataset`` (empty at the scope
    root). A resolved ref's full root-relative path is ``owner_path + path``;
    its prefixes are added to ``sinks.crossed`` so the caller learns every relationship
    the fragment crosses WITHOUT re-scanning the (ambiguous) internal-alias
    output. Returns the replacement node when rewritten, else ``None``. ``sinks.reads`` receives the root-relative
    path of each dataset a field is read on.
    """
    sibling = sibling_first and not qualifiers and _expanding_sibling(dataset, leaf=leaf)
    if not sibling and reads_source_column(qualifiers, dataset=dataset):
        _record(sinks.reads, owner_path)
        return _read(_requalify(node=node, alias=alias_path, leaf=leaf), sinks=sinks, dataset=dataset,
                     field=field_for_column(dataset, leaf), path=owner_path)
    path = _resolve_qualifiers(
        qualifiers=qualifiers, leaf=leaf, source_dataset=dataset,
        owner_alias=alias_path, datasets_by_name=datasets_by_name,
        field=visited[-1][1] if visited else None,
    )
    if path is None:
        return None  # opaque — leave untouched
    full_path = owner_path + path
    located = _locate_site_target(
        path=path, full_path=full_path, dataset=dataset, alias_path=alias_path,
        datasets_by_name=datasets_by_name, alias_resolver=alias_resolver,
        crossed_paths=sinks.crossed,
    )
    if located is None:
        return None
    target_dataset, canonical_alias = located
    owner = visited[-1] if visited else site
    # Mode-A SQL names a field exactly.
    target_col = resolve_generated_field(
        dataset=target_dataset, name=leaf, location=".".join(owner) if owner else None, quoted=True,
    )
    if target_col is not None:
        leaf = target_col.name
    if target_col is None or not target_col.needs_expansion:
        _record(sinks.reads, full_path)
        return _read(_requalify(node=node, alias=canonical_alias, leaf=leaf), sinks=sinks, dataset=target_dataset,
                     field=target_col or field_for_column(target_dataset, leaf), path=full_path)
    key = (target_dataset.name, leaf)
    if key in visited:
        cycle_start = visited.index(key)
        cycle = (*visited[cycle_start:], key)
        raise DatasetFieldCycleError(cycle=list(cycle))
    expanded_ast = _expand_target_field(
        target_col=target_col, target_dataset=target_dataset,
        canonical_alias=canonical_alias, full_path=full_path,
        datasets_by_name=datasets_by_name, dialect=dialect, visited=(*visited, key),
        alias_resolver=alias_resolver, sinks=sinks,
    )
    if expanded_ast is None:
        return None
    replacement = exp.Paren(this=expanded_ast)
    node.replace(replacement)
    return replacement


def _expand_target_field(
    *,
    target_col: DatasetField,
    target_dataset: SlayerDataset,
    canonical_alias: str,
    full_path: Tuple[str, ...],
    datasets_by_name: DatasetsByName,
    dialect: str,
    visited: Tuple[Tuple[str, str], ...],
    alias_resolver: Optional[AliasResolver],
    sinks: _Sinks,
) -> Optional[Expression]:
    """The expanded definition of a derived / filtered target field: its value (the
    qualified bare field for a filtered physical one) masked by its filter, which reads stored values."""
    def _expand_child(sql: str, *, sibling_first: bool = False, context: Context) -> Optional[Expression]:
        return expand_derived_refs_ast(
            sql=sql, dataset=target_dataset, alias_path=canonical_alias,
            owner_path=full_path, datasets_by_name=datasets_by_name, dialect=dialect,
            visited=visited, alias_resolver=alias_resolver,
            crossed_paths=sinks.crossed, sibling_first=sibling_first,
            site=(target_dataset.name, target_col.name), read_paths=sinks.reads, read=sinks.read, context=context,
        )

    definition = target_col.definition_sql
    if definition is None or is_trivial_base(field=target_col):
        # The target's own spelling, not the (possibly unquoted) reference site,
        # so a quoted self-identity keeps its quoting on case-folding dialects.
        _record(sinks.reads, full_path)
        value: Optional[Expression] = _qualified_base_ast(
            field=target_col, dataset=target_dataset, alias_path=canonical_alias, dialect=dialect, path=full_path,
            sinks=sinks,
        )
    else:
        value = _expand_child(definition, sibling_first=reads_siblings_first(target_col), context=sinks.context)
    if value is None or not target_col.filter:
        return value
    predicate = _expand_child(target_col.filter, context=STORED)
    if predicate is None:
        predicate = cast(Expression, sqlglot.parse_one(target_col.filter, dialect=dialect))
    return wrap_field_filter_ast(value=value, predicate=predicate)


def expand_derived_refs_sync(
    *,
    sql: Optional[str],
    dataset: SlayerDataset,
    alias_path: str,
    datasets_by_name: DatasetsByName,
    dialect: str,
    visited: Optional[Tuple[Tuple[str, str], ...]] = None,
    owner_path: Tuple[str, ...] = (),
    alias_resolver: Optional[AliasResolver] = None,
    crossed_paths: Optional[_PathSink] = None,
    site: Optional[Tuple[str, str]] = None,
    sibling_first: bool = False,
    read_paths: Optional[_PathSink] = None,
) -> Optional[str]:
    """``expand_derived_refs_ast`` rendered, its reads as stored."""
    if not sql:
        return sql
    expanded = expand_derived_refs_ast(
        sql=sql, dataset=dataset, alias_path=alias_path, datasets_by_name=datasets_by_name, dialect=dialect,
        visited=visited, owner_path=owner_path, alias_resolver=alias_resolver, crossed_paths=crossed_paths, site=site,
        sibling_first=sibling_first, read_paths=read_paths,
    )
    return prequote_reserved_identifiers(sql, dialect=dialect) if expanded is None else expanded.sql(dialect=dialect)


def expand_derived_refs_ast(
    *,
    sql: Optional[str],
    dataset: SlayerDataset,
    alias_path: str,
    datasets_by_name: DatasetsByName,
    dialect: str,
    visited: Optional[Tuple[Tuple[str, str], ...]] = None,
    owner_path: Tuple[str, ...] = (),
    alias_resolver: Optional[AliasResolver] = None,
    crossed_paths: Optional[_PathSink] = None,
    site: Optional[Tuple[str, str]] = None,
    sibling_first: bool = False,
    read_paths: Optional[_PathSink] = None,
    read: Optional[ReadHook] = None,
    context: Context = VALUE,
) -> Optional[Expression]:
    """Expand a row definition: qualify every source-column read (a bare name, ``source.x``) and every base field a
    path reads to its internal alias, and inline every derived field a path reads.

    Recurses through chains (``A.ratio`` -> ``A.bar / B.foo_normalized`` -> …)
    with cycle detection via ``visited``, raising ``DatasetFieldCycleError`` on a
    self-referential chain and ``LegacyDunderAliasError`` (D2) on a legacy
    split-alias qualifier.

    ``datasets_by_name`` is the resolved dataset collection (typically built from
    ``bundle.referenced_models``) — hops resolve through the shared
    bidirectional walker. ``owner_path`` is the
    ROOT-relative path of ``dataset`` (empty at the scope root). ``alias_resolver``
    threads the WP3 join-alias registry so qualifier strings match
    the emitted JOIN aliases; ``None`` falls back to the legacy ``"__".join``
    spelling. When ``crossed_paths`` is supplied, every relationship-path prefix the
    fragment (recursively) crosses is added to it — the structural alternative
    to re-scanning the internal-alias output. ``site`` ``(dataset, field)`` names
    the definition being expanded, for stale-spelling warnings. Every qualified column read goes through ``read`` in
    ``context`` (a field filter inside reads in the stored context).
    """
    if not sql:
        return None
    visited = visited or ()
    # Reserved-word qualifiers (``grant.amount``) parse as an unsupported
    # ``Command`` unless quoted, so the reference would be invisible to the
    # resolver (no relationship path registered). Prequote first — idempotent, so callers
    # that already prequoted (the Mode-A door) are unaffected.
    sql = prequote_reserved_identifiers(sql, dialect=dialect)
    try:
        parsed = cast(Expression, sqlglot.parse_one(sql, dialect=dialect))
    except ParseError as exc:
        raise ModeASqlParseError(
            fragment=sql,
            location=f"definition of field {'.'.join(site)!r}" if site else f"a definition on dataset {dataset.name!r}",
            reason=str(exc).splitlines()[0] if str(exc) else None,
        ) from exc
    if isinstance(parsed, exp.Command):
        # A call spelled like a statement (``replace(...)`` on SQLite / MySQL) is read as an expression.
        where = sqlglot.parse_one(f"SELECT 1 WHERE {sql}", dialect=dialect).args.get("where")
        if where is None or where.this is None:
            return None
        parsed = cast(Expression, where.this)
        parsed.pop()
    root_scope_ids = root_scope_field_ids(parsed=parsed)
    sinks = _Sinks(crossed=crossed_paths, reads=read_paths, read=read, context=context)
    for node, quals, leaf in reference_sites(parsed, root_scope_ids):
        replacement = _process_reference_site(
            node=node,
            qualifiers=quals,
            leaf=leaf,
            dataset=dataset,
            alias_path=alias_path,
            owner_path=owner_path,
            datasets_by_name=datasets_by_name,
            dialect=dialect,
            visited=visited,
            alias_resolver=alias_resolver,
            sinks=sinks,
            site=site,
            sibling_first=sibling_first,
        )
        # ``node.replace`` mutates the node's PARENT. When the whole fragment is
        # a single reference — ``expression: orders.other_derived_col``, an alias of
        # another derived field — that reference IS ``parsed`` and has no
        # parent, so the replace is a silent no-op. Rebind the root here, or the
        # correctly-expanded SQL is computed and thrown away and the emitted
        # query references a derived field as though it were physical.
        if replacement is not None and node is parsed:
            parsed = replacement
    return parsed


def wrap_field_filter(*, value_sql: str, filter_sql: Optional[str]) -> str:
    """Desugar ``DatasetField.filter``: ``CASE WHEN <filter> THEN <value> END`` when a
    filter is set, else the bare value. Both fragments are already expanded (and
    derived expansions self-parenthesise), so no extra parens are added — the
    canonical single-field form stays ``SUM(CASE WHEN f THEN col END)``."""
    if not filter_sql:
        return value_sql
    return f"CASE WHEN {filter_sql} THEN {value_sql} END"


def wrap_field_filter_ast(*, value: Expression, predicate: Optional[Expression]) -> Expression:
    """``wrap_field_filter`` as AST."""
    if predicate is None:
        return value
    return exp.Case(ifs=[exp.If(this=predicate, true=value)])


def expand_field_definition_ast(
    *,
    field: DatasetField,
    dataset: SlayerDataset,
    alias_path: str,
    datasets_by_name: DatasetsByName,
    dialect: str,
    owner_path: Tuple[str, ...] = (),
    alias_resolver: Optional[AliasResolver] = None,
    crossed_paths: Optional[_PathSink] = None,
    read: Optional[ReadHook] = None,
    context: Context = VALUE,
) -> Expression:
    """A field's value masked by its filter as AST, its reads through ``read``: the value in ``context``, the filter
    in the stored context."""
    value, predicate = expand_field_definition_parts_ast(
        field=field, dataset=dataset, alias_path=alias_path, datasets_by_name=datasets_by_name, dialect=dialect,
        owner_path=owner_path, alias_resolver=alias_resolver, crossed_paths=crossed_paths, read=read, context=context,
    )
    return wrap_field_filter_ast(value=value, predicate=predicate)


def expand_field_definition_sync(
    *,
    field: DatasetField,
    dataset: SlayerDataset,
    alias_path: str,
    datasets_by_name: DatasetsByName,
    dialect: str,
    owner_path: Tuple[str, ...] = (),
    visited: Optional[Tuple[Tuple[str, str], ...]] = None,
    alias_resolver: Optional[AliasResolver] = None,
    crossed_paths: Optional[_PathSink] = None,
) -> str:
    """Expand a field's value SQL, wrapping it in ``CASE WHEN (<filter>) THEN
    (<value>) END`` when the field carries a ``DatasetField.filter``."""
    value, filter_sql = expand_field_definition_parts_sync(
        field=field, dataset=dataset, alias_path=alias_path,
        datasets_by_name=datasets_by_name, dialect=dialect, owner_path=owner_path,
        visited=visited, alias_resolver=alias_resolver, crossed_paths=crossed_paths,
    )
    return wrap_field_filter(value_sql=value, filter_sql=filter_sql)


def expand_field_definition_parts_sync(
    *,
    field: DatasetField,
    dataset: SlayerDataset,
    alias_path: str,
    datasets_by_name: DatasetsByName,
    dialect: str,
    owner_path: Tuple[str, ...] = (),
    visited: Optional[Tuple[Tuple[str, str], ...]] = None,
    alias_resolver: Optional[AliasResolver] = None,
    crossed_paths: Optional[_PathSink] = None,
) -> Tuple[str, Optional[str]]:
    """``expand_field_definition_parts_ast`` rendered, its reads as stored."""
    value, predicate = expand_field_definition_parts_ast(
        field=field, dataset=dataset, alias_path=alias_path, datasets_by_name=datasets_by_name, dialect=dialect,
        owner_path=owner_path, visited=visited, alias_resolver=alias_resolver, crossed_paths=crossed_paths,
    )
    return value.sql(dialect=dialect), (predicate.sql(dialect=dialect) if predicate is not None else None)


def expand_field_definition_parts_ast(
    *,
    field: DatasetField,
    dataset: SlayerDataset,
    alias_path: str,
    datasets_by_name: DatasetsByName,
    dialect: str,
    owner_path: Tuple[str, ...] = (),
    visited: Optional[Tuple[Tuple[str, str], ...]] = None,
    alias_resolver: Optional[AliasResolver] = None,
    crossed_paths: Optional[_PathSink] = None,
    read: Optional[ReadHook] = None,
    context: Context = VALUE,
) -> Tuple[Expression, Optional[Expression]]:
    """``(value, expanded filter or None)`` of a field definition — the one
    door for both the scope resolver and the generator's cast-wrapping renderer.
    Value and filter expand through the same derived-refs door at the same owner
    path, the value in ``context`` and the filter in the stored one; a filtered physical column's value is its
    qualified bare name (re-entering the reference site would mask it twice). The field is on ``visited``, so a
    definition reading its own field by path is a cycle."""
    key = (dataset.name, field.name)
    visited = (*(visited or ()), *(() if key in (visited or ()) else (key,)))

    def _expand(sql: str, *, sibling_first: bool = False, ctx: Context) -> Expression:
        out = expand_derived_refs_ast(
            sql=sql, dataset=dataset, alias_path=alias_path,
            datasets_by_name=datasets_by_name, dialect=dialect, owner_path=owner_path,
            visited=visited, alias_resolver=alias_resolver, crossed_paths=crossed_paths,
            site=key, sibling_first=sibling_first, read=read, context=ctx,
        )
        return out if out is not None else cast(Expression, sqlglot.parse_one(sql, dialect=dialect))

    definition = field.definition_sql
    if definition is None or is_trivial_base(field=field):
        value = _qualified_base_ast(field=field, dataset=dataset, alias_path=alias_path, dialect=dialect,
                                    path=owner_path, sinks=_Sinks(read=read, context=context))
    else:
        value = _expand(definition, sibling_first=reads_siblings_first(field), ctx=context)
    return value, (_expand(field.filter, ctx=STORED) if field.filter else None)


def _qualified_base_ast(
    *, field: DatasetField, dataset: SlayerDataset, alias_path: str, dialect: str, path: Path, sinks: _Sinks,
) -> Expression:
    """A trivial-base field as ``alias.leaf`` through the read path, parsed from its own spelling so a quoted
    self-identity or reserved name keeps its quoting."""
    raw = prequote_reserved_identifiers(field.definition_sql or field.name, dialect=dialect)
    node = exp.maybe_parse(raw, dialect=dialect)
    return _read(_requalify(node=node, alias=alias_path, leaf=field.name), sinks=sinks, dataset=dataset, field=field,
                 path=path)
