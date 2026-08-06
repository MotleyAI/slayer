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

from typing import List, Optional, Tuple, Union

import sqlglot
from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.errors import ParseError

from slayer.core.errors import ModeASqlParseError, UnknownReferenceError
from slayer.core.keys import ColumnKey, ColumnSqlKey
from slayer.core.models import SlayerModel
from slayer.engine.column_expansion import (
    collect_root_scope_joined_paths,
    expand_derived_refs_sync,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.naming import AliasAllocator
from slayer.sql.render.parse import parse_expression, parse_predicate
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

# A ref that can enter a scope. Stage 2 exercises structural column refs, derived
# columns, and free Mode-A / predicate text; later stages widen this union.
Ref = Union[ColumnKey, ColumnSqlKey, str]


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

    # ---- Law 1 -------------------------------------------------------------
    def resolve(self, ref: Ref, *, consumer: "ScopeFrame | None" = None) -> exp.Expression:
        """Anchor ``ref`` in this scope, register the joins it crosses, and —
        when a ``consumer`` scope is named — materialise it and return the bare
        alias for the consumer.
        """
        template = self._anchor(ref)
        self._register_join_paths(template)
        return self._close(template, consumer=consumer)

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
        grammar: str,
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
        self._register_join_paths(raw_ast)

        expanded = expand_derived_refs_sync(
            sql=prequoted,
            model=self.root_model,
            alias_path=self.root_relation,
            resolve_model=self.bundle.get_referenced_model,
            dialect=self.dialect.sqlglot_name,
            is_root=True,
        )
        if expanded is None or expanded == prequoted:
            final = raw_ast
        else:
            final = self._parse_mode_a(
                expanded, grammar=grammar, fragment=sql, location=location,
            )
            self._register_join_paths(final)

        return self._close(final, consumer=consumer)

    def _parse_mode_a(
        self,
        text: str,
        *,
        grammar: str,
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
        parse = parse_predicate if grammar == _PREDICATE else parse_expression
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

    def _anchor(self, ref: Ref) -> exp.Expression:
        if isinstance(ref, ColumnKey):
            alias = self.root_relation if not ref.path else "__".join(ref.path)
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
            # must anchor at the ``__``-path alias with ``is_root=False`` so a
            # bare inner ref (``name``) qualifies to ``stores.name`` — and a
            # further-joined inner ref (``regions.population``) to the full
            # ``stores__regions`` path (the DEV-1701 shape). A local derived
            # column (empty path) keeps anchoring at the scope root.
            if ref.path:
                alias_path = "__".join(ref.path)
                is_root = False
            else:
                alias_path = self.root_relation
                is_root = True
            expanded = expand_derived_refs_sync(
                sql=raw_sql,
                model=model,
                alias_path=alias_path,
                resolve_model=self.bundle.get_referenced_model,
                dialect=self.dialect.sqlglot_name,
                is_root=is_root,
            )
            return self._parse(expanded or raw_sql)
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
                is_root=True,
            )
            return self._parse(expanded or prequoted)
        raise NotImplementedError(
            f"ScopeFrame.resolve does not yet handle ref type {type(ref).__name__}",
        )

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
