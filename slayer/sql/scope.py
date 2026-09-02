"""DEV-1706 Stage 2 — ``ScopeFrame`` + the single resolver (Laws 1 & 2).

A query renders as a tree of SELECT scopes, each rooted at one relation. Every
expression enters a scope through :meth:`ScopeFrame.resolve`, which:

* **Law 1 (anchored rendering):** expands derived refs (reserved-word
  identifiers prequoted — DEV-1686; multi-term derived expansions parenthesised
  — DEV-1539), anchors every reference at the scope root or a ``__``-path join
  alias, and REGISTERS each crossed join path into ``join_paths`` in the same
  call. Discovery is a side effect of rendering — it can never be forgotten.
* **Law 2 (projection boundaries):** when a ``consumer`` scope is named, the
  value is materialised as a ``_val_<n>`` projection in THIS (producing) scope
  and a bare alias is returned for the consumer. Materialisations dedup by a
  scope-safe key (producing-scope id + anchored AST + dialect — Codex F6).

Stage 2 migrates the host base SELECT, which is a single scope with no
projection boundary, so the materialise branch is exercised only by direct unit
tests here; Stage 4 is its first generated-SQL consumer. The resolver reuses the
existing engine-layer expansion/scan helpers (D-G wrap-and-reuse).
"""

from __future__ import annotations

from typing import Callable, Dict, List, Literal, Optional, Tuple, Union

import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.errors import ParseError

from slayer.core.enums import DataType
from slayer.core.errors import ModeASqlParseError, UnknownReferenceError
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    LiteralKey,
    ScalarCallKey,
)
from slayer.core.models import SlayerModel
from slayer.engine.column_expansion import (
    collect_root_scope_joined_paths,
    expand_derived_refs_sync,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.naming import AliasAllocator
from slayer.sql.render.parse import parse_expression, parse_predicate
from slayer.sql.render.row_expr import render_row_expression
from slayer.sql.reserved_keywords import (
    install_reserved_keywords,
    prequote_reserved_identifiers,
)

# The resolver relies on sqlglot's reserved-word quoting on emit (DEV-1686).
install_reserved_keywords()

# The two Mode-A grammars. A static property of the surface being read, chosen
# by the call site — never sniffed from the text (see ``ScopeFrame._enter``).
_PREDICATE = "predicate"
_EXPRESSION = "expression"
_Grammar = Literal["predicate", "expression"]

# A ref that can enter a scope: structural column refs, derived columns, free
# Mode-A / predicate text, and (DEV-1826) row-level expression composites — an
# aggregate's same-model expression source anchors through the same door.
Ref = Union[ColumnKey, ColumnSqlKey, ArithmeticKey, ScalarCallKey, LiteralKey, str]


class _OrderedPathSet:
    """Insertion-ordered, de-duplicated set of ``__``-join-path tuples.

    Backed by a dict so membership is O(1) and iteration/`as_list` preserve
    first-seen order — the join emission order ``_build_from_and_joins`` reads.
    """

    def __init__(self) -> None:
        self._d: "dict[Tuple[str, ...], None]" = {}

    def add(self, path: Tuple[str, ...]) -> None:
        self._d.setdefault(path, None)

    def __contains__(self, path: object) -> bool:
        return path in self._d

    def __iter__(self):
        return iter(self._d)

    def __len__(self) -> int:
        return len(self._d)

    def as_list(self) -> List[Tuple[str, ...]]:
        return list(self._d)


class Materialization(BaseModel):
    """A Law-2 ``_val_<n>`` projection produced in a scope for a consumer."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    alias: str
    expr: exp.Expression  # anchored template, projected in the producing scope
    # (producing_scope_id, ast.sql(dialect=sqlglot_name), sqlglot_name) — Codex F6/M3.
    dedup_key: Tuple[str, str, str]


class ScopeFrame(BaseModel):
    """One SELECT scope rooted at ``root_relation`` (Laws 1 & 2)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    scope_id: str  # generation-local, ephemeral, never emitted (D-F / Codex L1)
    root_model: SlayerModel
    root_relation: str
    bundle: ResolvedSourceBundle
    dialect: SqlDialect
    allocator: AliasAllocator
    join_paths: _OrderedPathSet = Field(default_factory=_OrderedPathSet)
    materializations: List[Materialization] = Field(default_factory=list)
    # DEV-1825 — reserved-leaf placeholder → its rendered column on an attached
    # regroup producer CTE. Resolved by EXACT membership before ordinary column
    # anchoring; a prefixed leaf that misses this registry is fail-closed.
    attached_columns: Dict[ColumnKey, exp.Expression] = Field(default_factory=dict)

    # ---- Law 1 -------------------------------------------------------------
    def resolve(self, ref: Ref, *, consumer: "ScopeFrame | None" = None) -> exp.Expression:
        """Anchor ``ref`` in this scope, register the joins it crosses, and —
        when a ``consumer`` scope is named — materialise it and return the bare
        alias for the consumer.

        DEV-1743: ``_anchor`` registers the crossed join paths STRUCTURALLY
        (into ``self.join_paths``) as it resolves — the emitted AST is never
        re-scanned, because an internal ``__`` join alias is indistinguishable
        from a user ``__``-named model once serialized."""
        template = self._anchor(ref)
        return self._close(template, consumer=consumer)

    def _register_path_prefixes(self, path: Tuple[str, ...]) -> None:
        """Register every join-path prefix of ``path`` into this scope."""
        for i in range(1, len(path) + 1):
            self.join_paths.add(path[:i])

    # ---- The one Mode-A door (P-A) -----------------------------------------
    def enter_predicate(
        self,
        sql: str,
        *,
        consumer: "ScopeFrame | None" = None,
        location: Optional[str] = None,
    ) -> exp.Expression:
        """Enter a Mode-A boolean PREDICATE (``Column.filter``, model
        ``filters``) into this scope. See :meth:`_enter`."""
        return self._enter(
            sql, grammar=_PREDICATE, consumer=consumer, location=location,
        )

    def enter_expression(
        self,
        sql: str,
        *,
        consumer: "ScopeFrame | None" = None,
        location: Optional[str] = None,
    ) -> exp.Expression:
        """Enter a Mode-A scalar EXPRESSION (``Column.sql``) into this scope.
        See :meth:`_enter`."""
        return self._enter(
            sql, grammar=_EXPRESSION, consumer=consumer, location=location,
        )

    def _enter(
        self,
        sql: str,
        *,
        grammar: _Grammar,
        consumer: "ScopeFrame | None",
        location: Optional[str],
    ) -> exp.Expression:
        """The single implementation behind both Mode-A surfaces.

        One pass, in order:

        1. prequote reserved identifiers (DEV-1686),
        2. parse the PREQUOTED text and scan it for crossed join paths,
        3. expand derived refs, parse the EXPANDED text and scan that too,
        4. union both scans into ``join_paths``,
        5. Law 2 — materialise for a named ``consumer``, else return the AST.

        Both scans are load-bearing (the DEV-1494 dual-scan contract): a dotted
        ref whose derived column inlines to a constant vanishes from the
        expanded AST, so only the pre-expansion scan sees its join; and a bare
        derived ref only reveals the joins its expansion crosses AFTER
        expanding. Discovery is a side effect of entering — it cannot be
        forgotten by a caller.

        There is deliberately NO qualification step. ``expand_derived_refs_sync``
        already qualifies, against the OWNING model's canonical alias, and
        deliberately leaves a node alone when its alias path does not resolve —
        that is an opaque CTE / subquery reference. A blanket pass against
        ``root_relation`` would fire on exactly those and corrupt them.

        ``grammar`` is fixed by the call site, never by the content: the surface
        being read determines it (a ``Column.filter`` is always a predicate).
        Sniffing content or retrying the other grammar would put classification
        back into render time.
        """
        prequoted = prequote_reserved_identifiers(
            sql, dialect=self.dialect.sqlglot_name,
        )
        raw_ast = self._parse_mode_a(
            prequoted, grammar=grammar, fragment=sql, location=location,
        )
        # Scan the RAW (dotted) form for the join paths its references cross;
        # the expansion below additionally registers any joins revealed only by
        # inlining a derived column (the DEV-1494 dual-discovery contract), now
        # collected STRUCTURALLY via ``crossed_paths`` rather than by re-scanning
        # the internal-alias output (DEV-1743).
        self._register_join_paths(raw_ast)

        expanded = expand_derived_refs_sync(
            sql=prequoted,
            model=self.root_model,
            alias_path=self.root_relation,
            resolve_model=self.bundle.get_referenced_model,
            dialect=self.dialect.sqlglot_name,
            owner_path=(),
            alias_resolver=self._alias_resolver(),
            crossed_paths=self.join_paths,
        )
        if expanded is None or expanded == prequoted:
            final = raw_ast
        else:
            final = self._parse_mode_a(
                expanded, grammar=grammar, fragment=sql, location=location,
            )

        return self._close(final, consumer=consumer)

    def _parse_mode_a(
        self,
        text: str,
        *,
        grammar: _Grammar,
        fragment: str,
        location: Optional[str],
    ) -> exp.Expression:
        """Parse ``text`` under the surface's grammar, or RAISE (D1).

        Only sqlglot's ``ParseError`` is caught, and only to re-raise it as a
        typed SLayer error naming the ORIGINAL author text. Nothing falls back
        to the raw string and nothing degrades to "no join paths" — the two
        soft failures this replaces both turned a broken fragment into silently
        wrong SQL.
        """
        if grammar == _PREDICATE:
            parse = parse_predicate
        elif grammar == _EXPRESSION:
            parse = parse_expression
        else:  # pragma: no cover — the _Grammar Literal forbids other values
            raise ValueError(
                f"Unknown Mode-A grammar {grammar!r}; expected "
                f"{_PREDICATE!r} or {_EXPRESSION!r}.",
            )
        try:
            return parse(sql=text, target_dialect=self.dialect, prequote=False)
        except ParseError as exc:
            raise ModeASqlParseError(
                fragment=fragment,
                location=location or self._default_location(),
                reason=str(exc).splitlines()[0] if str(exc) else None,
            ) from exc

    def _default_location(self) -> str:
        return f"Mode-A SQL in scope rooted at model {self.root_model.name!r}"

    def _alias_resolver(self) -> Callable[[Tuple[str, ...]], str]:
        """The WP3 registry-backed alias resolver for this scope (DEV-1743):
        maps a root-relative join path to the internal JOIN alias the generator
        will emit, so ``expand_derived_refs_sync`` qualifies refs to a matching
        alias even when a chain leaf collides with a literal ``__``-named
        model."""
        return lambda path: self.allocator.alias_for(
            root=self.root_relation, path=path,
            limit=self.dialect.max_identifier_bytes,
        )

    def _register_join_paths(self, parsed: exp.Expression) -> None:
        """Law 1's side effect: every join path ``parsed`` crosses is recorded
        on this scope, so ``_build_from_and_joins`` emits the JOINs it needs."""
        for path in collect_root_scope_joined_paths(
            parsed=parsed,
            source_model=self.root_model,
            source_relation=self.root_relation,
            bundle=self.bundle,
        ):
            self.join_paths.add(path)

    def materialize_for(
        self, template: exp.Expression, *, consumer: "ScopeFrame",
    ) -> exp.Expression:
        """Law 2 for an expression the caller has ALREADY anchored.

        :meth:`resolve` anchors a ref and then closes it; this is the second
        half on its own, for a producer that built its template itself — a
        date-truncated grain, a value carrying its column's declared CAST. Those
        cannot be re-derived by anchoring a ref without changing the expression
        that gets projected, so they arrive here as AST.

        Same table, same dedup key, same aliases as :meth:`resolve`: there is
        one materialisation mechanism per scope, which is the point.
        """
        self._register_join_paths(template)
        return self._close(template, consumer=consumer)

    def _close(
        self, template: exp.Expression, *, consumer: "ScopeFrame | None",
    ) -> exp.Expression:
        """Law 2: materialise for a named consumer, else hand back a copy so a
        caller attaching this into its tree can never corrupt a value the scope
        (or another caller) also holds (D-L / M1)."""
        if consumer is not None and not self.may_inline(self.join_paths.as_list()):
            return exp.column(self._materialize(template))
        return template.copy()

    def resolve_predicate_sql(self, ref: Ref) -> Optional[str]:
        """Resolve a predicate ref to a SQL string for WHERE/HAVING builders."""
        expr = self.resolve(ref)
        return None if expr is None else expr.sql(dialect=self.dialect.sqlglot_name)

    def _anchor(self, ref: Ref) -> exp.Expression:  # NOSONAR(S3776) — flat dispatch over the Ref kinds (ColumnKey / ColumnSqlKey / str); each arm is independently simple and splitting would scatter the one-resolver-per-scope contract
        if isinstance(ref, ColumnKey):
            # DEV-1825: a regroup placeholder resolves from the attach registry
            # by EXACT membership; a reserved-prefix leaf that misses is
            # fail-closed — but ONLY when a regroup is active in this scope
            # (non-empty registry). With nothing attached, a leaf that merely
            # collides with the reserved prefix is an ordinary column: the
            # plan-time prefix fence runs only when a regroup is planned.
            attached = self.attached_columns.get(ref)
            if attached is not None:
                return attached.copy()
            if self.attached_columns and ref.leaf.startswith(REGROUP_LEAF_PREFIX):
                raise ValueError(
                    f"Regroup placeholder {ref.leaf!r} has no attached producer "
                    f"column in this scope. A __regroup__ leaf must resolve "
                    f"through the attach registry; reaching column anchoring "
                    f"means the placeholder escaped its producer's join scope.",
                )
            # DEV-1743: the allocator is the single join-alias authority
            # (dotted-canonical); it also registers the crossed path prefixes.
            alias = self.allocator.alias_for(
                root=self.root_relation, path=ref.path,
                limit=self.dialect.max_identifier_bytes,
            )
            self._register_path_prefixes(ref.path)
            return exp.Column(
                this=exp.to_identifier(ref.leaf),
                table=exp.to_identifier(alias),
            )
        if isinstance(ref, ColumnSqlKey):
            model = self._model_for(ref.model)
            col = next(
                (c for c in model.columns if c.name == ref.column_name), None,
            )
            if col is None:
                raw_sql = ref.column_name
            elif col.sql:
                raw_sql = col.sql
            else:
                raw_sql = col.name
            # DEV-1711: a derived column ON a JOINED model (``path`` non-empty,
            # e.g. ``stores.tier`` where ``tier`` lives on the joined ``stores``)
            # anchors at its ``__``-path alias (``owner_path=ref.path``) so a
            # bare inner ref (``name``) qualifies to ``stores.name`` and a
            # further-joined inner ref (``regions.population``) to the full
            # ``stores__regions`` path. A local derived column (empty path)
            # keeps anchoring at the scope root.
            owner_path = tuple(ref.path)
            if owner_path:
                alias_path = self.allocator.alias_for(
                    root=self.root_relation, path=owner_path,
                    limit=self.dialect.max_identifier_bytes,
                )
            else:
                alias_path = self.root_relation
            self._register_path_prefixes(owner_path)
            expanded = expand_derived_refs_sync(
                sql=raw_sql,
                model=model,
                alias_path=alias_path,
                resolve_model=self.bundle.get_referenced_model,
                dialect=self.dialect.sqlglot_name,
                owner_path=owner_path,
                alias_resolver=self._alias_resolver(),
                crossed_paths=self.join_paths,
            )
            return self._parse(expanded or raw_sql)
        if isinstance(ref, (ArithmeticKey, ScalarCallKey, LiteralKey)):
            # DEV-1826: an aggregate's row-level expression source — column
            # leaves anchor recursively through this scope, so join
            # registration and derived expansion apply per leaf.
            return render_row_expression(
                key=ref, dialect=self.dialect, resolve_column=self._anchor,
            )
        if isinstance(ref, str):
            prequoted = prequote_reserved_identifiers(
                ref, dialect=self.dialect.sqlglot_name,
            )
            expanded = expand_derived_refs_sync(
                sql=prequoted,
                model=self.root_model,
                alias_path=self.root_relation,
                resolve_model=self.bundle.get_referenced_model,
                dialect=self.dialect.sqlglot_name,
                owner_path=(),
                alias_resolver=self._alias_resolver(),
                crossed_paths=self.join_paths,
            )
            return self._parse(expanded or prequoted)
        raise NotImplementedError(
            f"ScopeFrame.resolve does not yet handle ref type {type(ref).__name__}",
        )

    def column_type(self, ref: Ref) -> Optional[DataType]:
        """The declared ``DataType`` of the column ``ref`` names, or ``None``
        when it is unknown (an anonymous free-SQL string, or a name absent from
        its model). Used by the filter-CAST policy (DEV-1763); the model lookup
        mirrors :meth:`_anchor` / :meth:`_model_for` so the type and the
        rendering agree on which model owns the column."""
        if isinstance(ref, ColumnSqlKey):
            model = self._model_for(ref.model)
            col = next(
                (c for c in model.columns if c.name == ref.column_name), None,
            )
            return col.type if col is not None else None
        if isinstance(ref, ColumnKey):
            if not ref.path:
                model = self.root_model
            else:
                model = self.bundle.get_referenced_model(ref.path[-1])
            if model is None:
                return None
            col = next((c for c in model.columns if c.name == ref.leaf), None)
            return col.type if col is not None else None
        return None

    def _model_for(self, name: str) -> SlayerModel:
        """Resolve a model name against the scope root, then the bundle.

        Unresolvable RAISES: the previous ``or self.root_model`` fallback
        expanded the ROOT model's derived SQL instead, turning a wiring bug into
        a wrong answer rather than a failure.
        """
        if name == self.root_model.name:
            return self.root_model
        model = self.bundle.get_referenced_model(name)
        if model is None:
            known = sorted(
                {self.root_model.name}
                | {m.name for m in self.bundle.referenced_models},
            )
            raise UnknownReferenceError(
                name=name,
                scope_kind="ScopeFrame",
                scope_summary=(
                    f"scope rooted at model {self.root_model.name!r}; "
                    f"models resolvable here: {known}"
                ),
                suggestion=(
                    "A ColumnSqlKey must name the model that owns the derived "
                    "column, and that model must be in the query's resolved "
                    "source bundle (reachable from the source model via joins)."
                ),
            )
        return model

    def _parse(self, sql: str) -> exp.Expression:
        return sqlglot.parse_one(sql, dialect=self.dialect.sqlglot_name)

    # ---- Law 2 -------------------------------------------------------------
    def may_inline(self, crossed_paths: List[Tuple[str, ...]]) -> bool:  # NOSONAR(S1172) — crossed_paths is the documented v1 API seam; the Stage-N inlining optimisation reads it, hardcoded False until then.
        """Whether a crossing value may be inlined back into the consumer scope
        instead of materialised. Hardcoded ``False`` in v1 (the seam Stage-N+
        optimisation grows into)."""
        return False

    def _materialize(self, template: exp.Expression) -> str:
        key = (
            self.scope_id,
            template.sql(dialect=self.dialect.sqlglot_name),
            self.dialect.sqlglot_name,
        )
        for m in self.materializations:
            if m.dedup_key == key:
                return m.alias
        alias = self.allocator.allocate_val()
        self.materializations.append(
            Materialization(alias=alias, expr=template, dedup_key=key),
        )
        return alias

    def apply_materializations(self, select: exp.Select) -> exp.Select:
        """Project each materialisation as ``<template> AS _val_<n>`` into
        ``select`` (in place). Projects a copy so the cached template stays
        parent-less and reusable (D-L)."""
        for m in self.materializations:
            select.select(m.expr.copy().as_(m.alias), copy=False)
        return select
