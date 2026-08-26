"""Recursive expansion of derived ``Column.sql`` references.

Closes DEV-1333. A ``Column.sql`` may reference any other column on the same
model or on a joined model — including columns that are themselves derived
(have their own ``sql`` expression rather than being a bare base-table
column). The query planner had been emitting such references verbatim, which
fails at execution because the joined table's underlying SQL knows nothing
about derived SLayer columns. This module walks the parsed AST of every
``Column.sql`` we are about to embed in a query, recursively replaces each
``<table>.<col>`` reference whose target is a derived column with the
target's own SQL (qualified to the right path alias), and lets the bare
base-column references qualify to the canonical ``__``-delimited path
alias.

The expansion runs during binding/planning, so the SQL generator never sees
unresolved derived references.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import List, Optional, Protocol, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import ScopeType, traverse_scope

from slayer.core.errors import (
    ColumnCycleError,
    LegacyDunderAliasError,
    UnresolvableDimensionJoinError,
)
from slayer.core.models import Column, SlayerModel
from slayer.sql.reserved_keywords import prequote_reserved_identifiers



def _is_trivial_base(*, column: Column) -> bool:
    """A column is "trivial base" iff its sql is missing or is just its own
    bare name. These need no expansion — only re-qualification.
    """
    if column.sql is None:
        return True
    sql = column.sql.strip()
    # A double-quoted self-identity (``"legalEntityType"`` for a column named
    # ``legalEntityType``) is still a bare base reference — required to point
    # at a mixed-case physical column on case-folding dialects. Strip the
    # surrounding identifier quotes before comparing so it is not mistaken
    # for a derived expression (which would self-recurse into a false cycle).
    if len(sql) >= 2 and sql[0] == '"' and sql[-1] == '"':
        sql = sql[1:-1].replace('""', '"')
    return sql == column.name


def _root_scope_column_ids(*, parsed: exp.Expression) -> set[int]:
    """Return the ``id()`` set of ``exp.Column`` nodes that lexically belong
    to the root scope of ``parsed`` (DEV-1410).

    Column.sql is contractually a scalar expression, not a SELECT. To re-use
    sqlglot's scope analysis we wrap ``parsed`` in a synthetic
    ``SELECT <parsed> AS _`` so it has a real root scope. The wrapper is
    used only for scope traversal; the original ``parsed`` AST is unchanged.

    A Column is "root-scope" iff its innermost scope-defining ancestor is
    the wrapper itself. Anything nested under a ``Subquery``, CTE, set
    operation (``Union`` / ``Except`` / ``Intersect``), ``Values``, or
    other scope-producing construct returns a non-root ScopeType and is
    skipped from derived-column inlining.

    ``Window`` / ``OVER`` is NOT a new scope: columns inside
    ``PARTITION BY`` / ``ORDER BY`` remain root-scope.
    """
    if not isinstance(parsed, exp.Expression):
        return set()
    wrapper = exp.Select(expressions=[exp.Alias(this=parsed.copy(), alias="_")])
    scope_node_ids: dict[int, ScopeType] = {}
    for scope in traverse_scope(wrapper):
        scope_node_ids[id(scope.expression)] = scope.scope_type
    if not scope_node_ids:
        # No SELECTs in the fragment at all — every column is root-scope.
        return {id(c) for c in parsed.find_all(exp.Column)}
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
    return root_ids


class _SyncBundle(Protocol):
    """Minimal contract for ``collect_root_scope_joined_paths``'s bundle —
    matches ``ResolvedSourceBundle.get_referenced_model``. Declared inline so
    this helper stays import-free of the engine layer.
    """

    def get_referenced_model(self, name: str) -> Optional[SlayerModel]: ...


class _PathSink(Protocol):
    """A duck-typed ordered collector of join-path tuples (``.add(path)``), so
    a caller can pass its own insertion-ordered set (``ScopeFrame.join_paths``)
    and receive the crossed paths in emission order."""

    def add(self, path: Tuple[str, ...], /) -> None: ...


# ---------------------------------------------------------------------------
# DEV-1743 — the shared Mode-A qualifier resolver.
#
# The dotted-canonical flip makes ``.`` the ONLY chain separator in Mode-A free
# SQL. A qualifier is EITHER an exact name (the host, or a directly-joined model
# — which MAY contain ``__``) OR a dotted chain of exact join hops. User input
# is never split on ``__``. The legacy ``customers__regions`` split-alias
# spelling — which does not name a real join target but whose naive split walks
# the graph — is a hard D2 error (:class:`LegacyDunderAliasError`).
# ---------------------------------------------------------------------------

SyncResolveModel = Callable[[str], Optional[SlayerModel]]


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


def _reference_sites(
    parsed: exp.Expression, root_scope_ids: Set[int],
) -> List[Tuple[exp.Expression, Tuple[str, ...], str]]:
    """Yield ``(node, qualifiers, leaf)`` for every root-scope reference.

    A 5+-part reference parses as an outer ``exp.Dot`` wrapping a ``Column``;
    a 2–4-part (or bare) reference is a plain ``Column``. Both are surfaced,
    with the wrapping ``Dot`` preferred so its inner ``Column`` is not double-
    counted.
    """
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
    for col in parsed.find_all(exp.Column):
        if id(col) in consumed or id(col) not in root_scope_ids:
            continue
        parts = [p.name for p in col.parts]
        sites.append((col, tuple(parts[:-1]), parts[-1]))
    return sites


def _walk_exact(
    hops: Tuple[str, ...],
    source_model: SlayerModel,
    resolve_model: SyncResolveModel,
) -> Optional[SlayerModel]:
    """Walk ``hops`` as a chain of EXACT join targets from ``source_model``.

    Returns the terminal model when every hop is a direct join (each hop may
    itself contain ``__`` — it is matched by exact name, never split), or
    ``None`` when any hop is not a join / not resolvable.
    """
    current = source_model
    for hop in hops:
        if not any(j.target_model == hop for j in current.joins):
            return None
        nxt = resolve_model(hop)
        if nxt is None:
            return None
        current = nxt
    return current


def resolve_ref_target(
    *,
    qualifiers: Tuple[str, ...],
    source_model: SlayerModel,
    resolve_model: SyncResolveModel,
) -> Optional[SlayerModel]:
    """Resolve a Mode-A qualifier chain to its target model for best-effort
    save-time inspectors (schema-drift cascade, column-dependency cycle walk).

    Exact-name-first: an empty chain or the host's own name → the host; a single
    exact join target (which MAY contain ``__``) or a dotted chain of exact hops
    → the terminal model. Never ``__``-splits — the legacy split-alias is a hard
    D2 error at the runtime / save-time door, so here it simply fails to resolve
    (best-effort skip). Returns ``None`` when a hop is not a direct join / not
    reachable.
    """
    quals = list(qualifiers)
    if quals and quals[0] == source_model.name:
        quals = quals[1:]
    if not quals:
        return source_model
    return _walk_exact(tuple(quals), source_model, resolve_model)


def _resolve_qualifiers(
    *,
    qualifiers: Tuple[str, ...],
    leaf: str,
    source_model: SlayerModel,
    owner_alias: str,
    resolve_model: SyncResolveModel,
) -> Optional[Tuple[str, ...]]:
    """Classify a Mode-A qualifier chain (DEV-1743).

    Returns:
      * ``()`` — the reference is anchored on the host / owner model;
      * a non-empty path tuple — a resolved join path (each hop exact);
      * ``None`` — an opaque qualifier (CTE / subquery / physical
        ``schema.table.column``), left untouched.

    Raises :class:`LegacyDunderAliasError` (D2) for a split-alias spelling and
    :class:`UnresolvableDimensionJoinError` for a chain with a broken hop.
    """
    quals = list(qualifiers)
    if quals and quals[0] in (owner_alias, source_model.name):
        quals = quals[1:]
    if not quals:
        return ()
    path = tuple(quals)
    if _walk_exact(path, source_model, resolve_model) is not None:
        return path
    if len(path) == 1:
        q = path[0]
        if "__" in q:
            naive = tuple(q.split("__"))
            if _walk_exact(naive, source_model, resolve_model) is not None:
                raise LegacyDunderAliasError(
                    alias=q, dotted=".".join((*naive, leaf)),
                    model=source_model.name,
                )
        return None  # opaque single qualifier
    # Multi-part chain. If the first hop IS a join target but a later hop
    # failed, name the failing hop; otherwise the whole chain is opaque
    # (physical schema.table.column — join-target-beats-schema precedence).
    if any(j.target_model == path[0] for j in source_model.joins):
        current = source_model
        for hop in path:
            if not any(j.target_model == hop for j in current.joins):
                raise UnresolvableDimensionJoinError(
                    reference=".".join((*path, leaf)),
                    root_model=source_model.name,
                    reason=f"'{hop}' is not a joined model on the preceding hop.",
                )
            current = resolve_model(hop)
            if current is None:
                raise UnresolvableDimensionJoinError(
                    reference=".".join((*path, leaf)),
                    root_model=source_model.name,
                    reason=f"joined model '{hop}' is not in the resolved bundle.",
                )
    return None


def _lenient_path(
    *,
    qualifiers: Tuple[str, ...],
    source_model: SlayerModel,
    owner_alias: str,
    resolve_model: SyncResolveModel,
) -> Optional[Tuple[str, ...]]:
    """LENIENT qualifier resolution for the join-path SCANNER.

    Unlike :func:`_resolve_qualifiers` (STRICT — the expansion door that raises
    D2), the scanner runs over BOTH raw user forms AND already-expanded output
    whose qualifiers are legitimate internal ``__`` join aliases. It must not
    D2 those, so it resolves a ``__`` qualifier by exact-match first and then by
    naive split, and simply SKIPS (returns ``None``) anything that does not
    fully walk — an opaque CTE / subquery / physical reference.
    """
    quals = list(qualifiers)
    if quals and quals[0] in (owner_alias, source_model.name):
        quals = quals[1:]
    if not quals:
        return ()
    if _walk_exact(tuple(quals), source_model, resolve_model) is not None:
        return tuple(quals)
    if len(quals) == 1 and "__" in quals[0]:
        naive = tuple(quals[0].split("__"))
        if _walk_exact(naive, source_model, resolve_model) is not None:
            return naive
    return None


def collect_root_scope_joined_paths(
    *,
    parsed: exp.Expression,
    source_model: SlayerModel,
    source_relation: str,
    bundle: _SyncBundle,
) -> List[Tuple[str, ...]]:
    """Collect the ordered de-duplicated list of join-path prefixes a parsed
    SQL fragment references in its root scope.

    Each ROOT-scope reference that resolves as a join walk on ``source_model``
    contributes its prefixes (path ``("a", "b")`` yields ``("a",)`` AND
    ``("a", "b")``). Host-anchored and opaque references (CTE / subquery
    aliases, physical ``schema.table.column``) contribute nothing.

    LENIENT (:func:`_lenient_path`): this scanner runs over already-expanded
    fragments whose qualifiers are internal ``__`` join aliases, so it never
    raises D2 — that is the STRICT expansion door's job. Shared by the SQL
    generator (``SQLGenerator._joined_paths_in_sql``) and the planner-side
    column filter discovery so the two surfaces agree on what counts as
    "crosses a join."
    """
    root_ids = _root_scope_column_ids(parsed=parsed)
    seen: Set[Tuple[str, ...]] = set()
    ordered: List[Tuple[str, ...]] = []
    for _node, quals, _leaf in _reference_sites(parsed, root_ids):
        path = _lenient_path(
            qualifiers=quals, source_model=source_model,
            owner_alias=source_relation,
            resolve_model=bundle.get_referenced_model,
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
# Synchronous expansion (DEV-1450 typed pipeline)
# ---------------------------------------------------------------------------
#
# The generator runs synchronously over a ``ResolvedSourceBundle`` that has
# already loaded every referenced model (P11: storage consulted once, up
# front), so it expands derived refs through a *sync* model resolver.
#
# Nothing awaits model resolution here — the expansion is fully synchronous.

#: Maps a root-relative join-path tuple to its internal alias string. The
#: generator/scope pass the WP3 registry (``AliasAllocator.alias_for``) so a
#: chain leaf and a literal ``__``-named model never share an alias; ``None``
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
    (``customers."spend"``) keeps its quoting. A nested ``Dot`` (5+ parts) is
    rebuilt from ``leaf`` — those deep chains never carry a quoted leaf in
    practice."""
    if isinstance(node, exp.Column):
        node.set("table", exp.to_identifier(alias))
        node.set("db", None)
        node.set("catalog", None)
        return node
    replacement = exp.column(leaf, table=alias)
    node.replace(replacement)
    return replacement


def _process_reference_site(
    *,
    node: exp.Expression,
    qualifiers: Tuple[str, ...],
    leaf: str,
    model: SlayerModel,
    alias_path: str,
    owner_path: Tuple[str, ...],
    resolve_model: SyncResolveModel,
    dialect: str,
    visited: Tuple[Tuple[str, str], ...],
    alias_resolver: Optional[AliasResolver],
    crossed_paths: Optional[_PathSink],
) -> Optional[exp.Expression]:
    """Resolve one reference site: qualify a base column in place, inline a
    derived one, or leave an opaque reference untouched.

    ``owner_path`` is the ROOT-relative path of ``model`` (empty at the scope
    root). A resolved ref's full root-relative path is ``owner_path + path``;
    its prefixes are added to ``crossed_paths`` so the caller learns every join
    the fragment crosses WITHOUT re-scanning the (ambiguous) internal-alias
    output. Returns the replacement node when rewritten, else ``None``.
    """
    path = _resolve_qualifiers(
        qualifiers=qualifiers, leaf=leaf, source_model=model,
        owner_alias=alias_path, resolve_model=resolve_model,
    )
    if path is None:
        return None  # opaque — leave untouched
    full_path = owner_path + path
    if not path:
        target_model: Optional[SlayerModel] = model
        canonical_alias = alias_path
    else:
        target_model = _walk_exact(path, model, resolve_model)
        if target_model is None:
            return None
        canonical_alias = _alias_for_path(
            full_path, alias_resolver=alias_resolver,
        )
        if crossed_paths is not None:
            for i in range(1, len(full_path) + 1):
                crossed_paths.add(full_path[:i])
    target_col = target_model.get_column(leaf)
    if target_col is None or _is_trivial_base(column=target_col):
        return _requalify(node, alias=canonical_alias, leaf=leaf)
    key = (target_model.name, leaf)
    if key in visited:
        cycle_start = visited.index(key)
        cycle = (*visited[cycle_start:], key)
        raise ColumnCycleError(cycle=list(cycle))
    expanded_sql = expand_derived_refs_sync(
        sql=target_col.sql,
        model=target_model,
        alias_path=canonical_alias,
        owner_path=full_path,
        resolve_model=resolve_model,
        dialect=dialect,
        visited=(*visited, key),
        alias_resolver=alias_resolver,
        crossed_paths=crossed_paths,
    )
    if expanded_sql is None:
        return None
    expanded_ast = sqlglot.parse_one(expanded_sql, dialect=dialect)
    replacement = exp.Paren(this=expanded_ast)
    node.replace(replacement)
    return replacement


def expand_derived_refs_sync(
    *,
    sql: Optional[str],
    model: SlayerModel,
    alias_path: str,
    resolve_model: SyncResolveModel,
    dialect: str,
    visited: Optional[Tuple[Tuple[str, str], ...]] = None,
    owner_path: Tuple[str, ...] = (),
    alias_resolver: Optional[AliasResolver] = None,
    crossed_paths: Optional[_PathSink] = None,
) -> Optional[str]:
    """Inline every derived-column reference in ``sql`` to its definition and
    qualify every base reference to its internal alias.

    Recurses through chains (``A.ratio`` -> ``A.bar / B.foo_normalized`` -> …)
    with cycle detection via ``visited``, raising ``ColumnCycleError`` on a
    self-referential chain and ``LegacyDunderAliasError`` (D2) on a legacy
    split-alias qualifier.

    ``resolve_model`` is a plain ``name -> Optional[SlayerModel]`` lookup
    (typically ``bundle.get_referenced_model``). ``owner_path`` is the
    ROOT-relative path of ``model`` (empty at the scope root). ``alias_resolver``
    (DEV-1743) threads the WP3 join-alias registry so qualifier strings match
    the emitted JOIN aliases; ``None`` falls back to the legacy ``"__".join``
    spelling. When ``crossed_paths`` is supplied, every join-path prefix the
    fragment (recursively) crosses is added to it — the structural alternative
    to re-scanning the internal-alias output.
    """
    if not sql:
        return sql
    visited = visited or ()
    # Reserved-word qualifiers (``grant.amount``) parse as an unsupported
    # ``Command`` unless quoted, so the reference would be invisible to the
    # resolver (no join registered). Prequote first — idempotent, so callers
    # that already prequoted (the Mode-A door) are unaffected (DEV-1686).
    sql = prequote_reserved_identifiers(sql, dialect=dialect)
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    root_scope_ids = _root_scope_column_ids(parsed=parsed)
    for node, quals, leaf in _reference_sites(parsed, root_scope_ids):
        replacement = _process_reference_site(
            node=node,
            qualifiers=quals,
            leaf=leaf,
            model=model,
            alias_path=alias_path,
            owner_path=owner_path,
            resolve_model=resolve_model,
            dialect=dialect,
            visited=visited,
            alias_resolver=alias_resolver,
            crossed_paths=crossed_paths,
        )
        # ``node.replace`` mutates the node's PARENT. When the whole fragment is
        # a single reference — ``Column.sql = "other_derived_col"``, an alias of
        # another derived column — that reference IS ``parsed`` and has no
        # parent, so the replace is a silent no-op. Rebind the root here, or the
        # correctly-expanded SQL is computed and thrown away and the emitted
        # query references a derived column as though it were physical.
        if replacement is not None and node is parsed:
            parsed = replacement
    return parsed.sql(dialect=dialect)
