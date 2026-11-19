"""Recursive expansion of derived ``DatasetField.sql`` references.

A ``DatasetField.sql`` may reference any other field on the same
dataset or on a joined dataset — including fields that are themselves derived
(have their own ``sql`` expression rather than being a bare base-table
field). The query planner had been emitting such references verbatim, which
fails at execution because the joined table's underlying SQL knows nothing
about derived SLayer fields. This module walks the parsed AST of every
``DatasetField.sql`` we are about to embed in a query, recursively replaces each
``<table>.<col>`` reference whose target is a derived field with the
target's own SQL (qualified to the right path alias), and lets the bare
base-field references qualify to the canonical ``__``-delimited path
alias.

The expansion runs during binding/planning, so the SQL generator never sees
unresolved derived references.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Dict, FrozenSet, List, Optional, Protocol, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import ScopeType, traverse_scope

from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    CircularRelationshipPathError,
    DatasetFieldCycleError,
    LegacyDunderAliasError,
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
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.scope import resolve_generated_field
from slayer.core.syntax import drop_root_hop
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.reserved_keywords import prequote_reserved_identifiers

__all__ = [
    "collect_root_scope_joined_paths",
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
    """A field is "trivial base" iff its sql is missing or is just its own
    bare name. These need no expansion — only re-qualification.
    """
    if field.sql is None:
        return True
    sql = field.sql.strip()
    # A double-quoted self-identity (``"legalEntityType"`` for a field named
    # ``legalEntityType``) is still a bare base reference — required to point
    # at a mixed-case physical column on case-folding dialects. Strip the
    # surrounding identifier quotes before comparing so it is not mistaken
    # for a derived expression (which would self-recurse into a false cycle).
    if len(sql) >= 2 and sql[0] == '"' and sql[-1] == '"':
        sql = sql[1:-1].replace('""', '"')
    return sql == field.name


def root_scope_field_ids(*, parsed: exp.Expression) -> set[int]:
    """Return the ``id()`` set of ``exp.Column`` nodes that lexically belong
    to the root scope of ``parsed``.

    DatasetField.sql is contractually a scalar expression, not a SELECT. To re-use
    sqlglot's scope analysis we wrap ``parsed`` in a synthetic
    ``SELECT <parsed> AS _`` so it has a real root scope. The wrapper is
    used only for scope traversal; the original ``parsed`` AST is unchanged.

    A Column is "root-scope" iff its innermost scope-defining ancestor is
    the wrapper itself. Anything nested under a ``Subquery``, CTE, set
    operation (``Union`` / ``Except`` / ``Intersect``), ``Values``, or
    other scope-producing construct returns a non-root ScopeType and is
    skipped from derived-field inlining.

    ``Window`` / ``OVER`` is NOT a new scope: columns inside
    ``PARTITION BY`` / ``ORDER BY`` remain root-scope.
    """
    if not isinstance(parsed, exp.Expression):
        return set()
    wrapper = exp.Select(expressions=[exp.Alias(this=parsed.copy(), alias="_")])
    scope_node_ids: dict[int, ScopeType] = {}
    for scope in traverse_scope(wrapper):
        scope_node_ids[id(scope.expression)] = scope.scope_type
    bound = _comprehension_bound_ids(parsed)
    if not scope_node_ids:
        # No SELECTs in the fragment at all — every column is root-scope.
        return {id(c) for c in parsed.find_all(exp.Column)} - bound
    # Re-walk the WRAPPER (which holds copies) — but we need ids from the
    # ORIGINAL parsed tree. Pair them up positionally: find_all yields
    # nodes in document order on both wrapper.this[0] and parsed.
    wrapper_cols = list(wrapper.find_all(exp.Column))
    parsed_cols = list(parsed.find_all(exp.Column))
    if len(wrapper_cols) != len(parsed_cols):
        # Fail closed: if the positional pairing between the wrapper copy
        # and the original tree ever drifts, treat NO column as root-scope.
        # Since the root-scope gate now runs BEFORE qualification, an empty
        # set leaves every bare column unqualified (not just un-inlined) —
        # conservative: the compile-time guard still catches cycles, and no
        # column is spliced across scopes. This branch is unreachable today;
        # the wrapper just wraps a deep copy and ``find_all`` walks in
        # document order.
        return set()
    root_ids: set[int] = set()
    for w_col, p_col in zip(wrapper_cols, parsed_cols):
        node: exp.Expression | None = w_col.parent
        scope_type: ScopeType | None = None
        while node is not None:
            if id(node) in scope_node_ids:
                scope_type = scope_node_ids[id(node)]
                break
            node = node.parent
        if scope_type == ScopeType.ROOT:
            root_ids.add(id(p_col))
    return root_ids - bound


def _comprehension_bound_ids(parsed: exp.Expr) -> set[int]:
    """Ids of the names a list comprehension (``[e FOR e IN xs IF p]``) binds, wherever they appear in it."""
    bound: set[int] = set()
    for node in parsed.find_all(exp.Comprehension):
        var = node.expression
        if not isinstance(var, exp.Column):
            continue
        for part in (node.this, var, node.args.get("condition")):
            if isinstance(part, exp.Expr):
                bound |= {id(c) for c in part.find_all(exp.Column) if not c.table and c.name == var.name}
    return bound


class _PathSink(Protocol):
    """A duck-typed ordered collector of relationship-path tuples (``.add(path)``), so
    a caller can pass its own insertion-ordered set (``ScopeFrame.join_paths``)
    and receive the crossed paths in emission order."""

    def add(self, path: Tuple[str, ...], /) -> None: ...


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


def _identifier_chain(node: exp.Expression) -> Optional[List[str]]:
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
    parsed: exp.Expression, root_scope_ids: Set[int],
) -> Tuple[List[Tuple[exp.Expression, Tuple[str, ...], str]], Set[int]]:
    """Root-scope references that parse as an outermost ``exp.Dot`` (5+-part
    refs), plus the ids of the ``Column`` nodes they consume so the plain-Column
    pass in :func:`reference_sites` does not double-count them."""
    sites: List[Tuple[exp.Expression, Tuple[str, ...], str]] = []
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
    parsed: exp.Expression, root_scope_ids: Set[int],
) -> List[Tuple[exp.Expression, Tuple[str, ...], str]]:
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


def resolve_ref_target(
    *,
    qualifiers: Tuple[str, ...],
    source_dataset: SlayerDataset,
    datasets_by_name: DatasetsByName,
) -> Optional[SlayerDataset]:
    """Resolve a Mode-A qualifier chain to its target dataset for best-effort
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
    try:
        walked = walk_terminal(root=source_dataset, path=quals, datasets_by_name=datasets_by_name)
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
        _raise_if_broken_join_walk(
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


def _raise_if_broken_join_walk(
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
    parsed: exp.Expression,
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


def _requalify(node: exp.Expression, *, alias: str, leaf: str) -> exp.Expression:
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


def _process_reference_site(
    *,
    node: exp.Expression,
    qualifiers: Tuple[str, ...],
    leaf: str,
    dataset: SlayerDataset,
    alias_path: str,
    owner_path: Tuple[str, ...],
    datasets_by_name: DatasetsByName,
    dialect: str,
    visited: Tuple[Tuple[str, str], ...],
    alias_resolver: Optional[AliasResolver],
    crossed_paths: Optional[_PathSink],
    physical_read_exempt: FrozenSet[Tuple[str, str]] = frozenset(),
    site: Optional[Tuple[str, str]] = None,
) -> Optional[exp.Expression]:
    """Resolve one reference site: qualify a base field in place, inline a
    derived one, or leave an opaque reference untouched.

    ``owner_path`` is the ROOT-relative path of ``dataset`` (empty at the scope
    root). A resolved ref's full root-relative path is ``owner_path + path``;
    its prefixes are added to ``crossed_paths`` so the caller learns every relationship
    the fragment crosses WITHOUT re-scanning the (ambiguous) internal-alias
    output. Returns the replacement node when rewritten, else ``None``.
    """
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
        crossed_paths=crossed_paths,
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
    # An exempt field reads physically (its filter's self-reference reads the
    # bare field, never the masked value — matching the save-time cycle rule).
    if (
        target_col is None or not target_col.needs_expansion
        or (target_dataset.name, leaf) in physical_read_exempt
    ):
        return _requalify(node=node, alias=canonical_alias, leaf=leaf)
    key = (target_dataset.name, leaf)
    if key in visited:
        cycle_start = visited.index(key)
        cycle = (*visited[cycle_start:], key)
        raise DatasetFieldCycleError(cycle=list(cycle))
    # A field's own-filter exemption is LOCAL to that filter's fragment — it is
    # NOT inherited into a referenced field's definition, or a real cross-field
    # cycle (x.filter→y, y.sql→x) would be silently read as physical (Codex).
    value_sql = _expand_target_field(
        target_col=target_col, target_dataset=target_dataset,
        canonical_alias=canonical_alias, full_path=full_path,
        datasets_by_name=datasets_by_name, dialect=dialect, visited=(*visited, key),
        alias_resolver=alias_resolver, crossed_paths=crossed_paths,
    )
    if value_sql is None:
        return None
    expanded_ast = sqlglot.parse_one(value_sql, dialect=dialect)
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
    crossed_paths: Optional[_PathSink],
) -> Optional[str]:
    """The expanded SQL of a derived / filtered target field: its value (the
    qualified bare field for a filtered physical one) masked by its filter."""
    def _expand_child(
        sql: str, *, exempt: FrozenSet[Tuple[str, str]] = frozenset(),
    ) -> Optional[str]:
        return expand_derived_refs_sync(
            sql=sql, dataset=target_dataset, alias_path=canonical_alias,
            owner_path=full_path, datasets_by_name=datasets_by_name, dialect=dialect,
            visited=visited, alias_resolver=alias_resolver,
            crossed_paths=crossed_paths, physical_read_exempt=exempt,
        )

    if is_trivial_base(field=target_col):
        # The target's own spelling, not the (possibly unquoted) reference site,
        # so a quoted self-identity keeps its quoting on case-folding dialects.
        value_sql: Optional[str] = _qualified_base_sql(
            field=target_col, alias_path=canonical_alias, dialect=dialect,
        )
    else:
        assert target_col.sql is not None  # non-trivial base ⇒ real derived sql
        value_sql = _expand_child(target_col.sql)
    if value_sql is None or not target_col.filter:
        return value_sql
    # A trivial-base target's OWN filter reads the physical column, not the mask;
    # the exemption is local to this filter fragment (never inherited).
    filter_exempt = frozenset(
        {(target_dataset.name, target_col.name)} if is_trivial_base(field=target_col) else set()
    )
    filter_sql = _expand_child(target_col.filter, exempt=filter_exempt)
    return wrap_field_filter(
        value_sql=value_sql,
        filter_sql=filter_sql if filter_sql is not None else target_col.filter,
    )


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
    physical_read_exempt: FrozenSet[Tuple[str, str]] = frozenset(),
    site: Optional[Tuple[str, str]] = None,
) -> Optional[str]:
    """Inline every derived-field reference in ``sql`` to its definition and
    qualify every base reference to its internal alias.

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
    the definition being expanded, for stale-spelling warnings.
    """
    if not sql:
        return sql
    visited = visited or ()
    # Reserved-word qualifiers (``grant.amount``) parse as an unsupported
    # ``Command`` unless quoted, so the reference would be invisible to the
    # resolver (no join registered). Prequote first — idempotent, so callers
    # that already prequoted (the Mode-A door) are unaffected.
    sql = prequote_reserved_identifiers(sql, dialect=dialect)
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    root_scope_ids = root_scope_field_ids(parsed=parsed)
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
            crossed_paths=crossed_paths,
            physical_read_exempt=physical_read_exempt,
            site=site,
        )
        # ``node.replace`` mutates the node's PARENT. When the whole fragment is
        # a single reference — ``DatasetField.sql = "other_derived_col"``, an alias of
        # another derived field — that reference IS ``parsed`` and has no
        # parent, so the replace is a silent no-op. Rebind the root here, or the
        # correctly-expanded SQL is computed and thrown away and the emitted
        # query references a derived field as though it were physical.
        if replacement is not None and node is parsed:
            parsed = replacement
    return parsed.sql(dialect=dialect)


def wrap_field_filter(*, value_sql: str, filter_sql: Optional[str]) -> str:
    """Desugar ``DatasetField.filter``: ``CASE WHEN <filter> THEN <value> END`` when a
    filter is set, else the bare value. Both fragments are already expanded (and
    derived expansions self-parenthesise), so no extra parens are added — the
    canonical single-field form stays ``SUM(CASE WHEN f THEN col END)``."""
    if not filter_sql:
        return value_sql
    return f"CASE WHEN {filter_sql} THEN {value_sql} END"


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
    """``(value SQL, expanded filter SQL or None)`` of a field definition — the one
    door for both the scope resolver and the generator's cast-wrapping renderer.
    Value and filter expand through the same derived-refs door at the same owner
    path; a filtered physical column's value is its qualified bare name (re-entering
    the reference site would mask it twice)."""
    def _expand(
        sql: str, *, exempt: FrozenSet[Tuple[str, str]] = frozenset(),
    ) -> str:
        out = expand_derived_refs_sync(
            sql=sql, dataset=dataset, alias_path=alias_path,
            datasets_by_name=datasets_by_name, dialect=dialect, owner_path=owner_path,
            visited=visited, alias_resolver=alias_resolver, crossed_paths=crossed_paths,
            physical_read_exempt=exempt, site=(dataset.name, field.name),
        )
        return out if out is not None else sql

    if is_trivial_base(field=field):
        value = _qualified_base_sql(field=field, alias_path=alias_path, dialect=dialect)
        # This field's own filter reads the physical column, never its mask.
        filter_exempt = frozenset({(dataset.name, field.name)})
    else:
        assert field.sql is not None  # non-trivial base ⇒ real derived sql
        value = _expand(field.sql)
        filter_exempt = frozenset()
    return value, (_expand(field.filter, exempt=filter_exempt) if field.filter else None)


def _qualified_base_sql(*, field: DatasetField, alias_path: str, dialect: str) -> str:
    """A trivial-base field as ``alias.leaf``, parsed from its own spelling so a
    quoted self-identity or reserved name keeps its quoting."""
    raw = prequote_reserved_identifiers(field.sql or field.name, dialect=dialect)
    node = exp.maybe_parse(raw, dialect=dialect)
    return _requalify(node=node, alias=alias_path, leaf=field.name).sql(dialect=dialect)
