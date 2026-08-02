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

from slayer.core.keys import ColumnKey, ColumnSqlKey
from slayer.core.models import SlayerModel
from slayer.engine.column_expansion import (
    collect_root_scope_joined_paths,
    expand_derived_refs_sync,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.naming import AliasAllocator
from slayer.sql.reserved_keywords import (
    install_reserved_keywords,
    prequote_reserved_identifiers,
)

# The resolver relies on sqlglot's reserved-word quoting on emit (DEV-1686).
install_reserved_keywords()

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
        for path in collect_root_scope_joined_paths(
            parsed=template,
            source_model=self.root_model,
            source_relation=self.root_relation,
            bundle=self.bundle,
        ):
            self.join_paths.add(path)

        if consumer is not None and not self.may_inline(self.join_paths.as_list()):
            alias = self._materialize(template)
            return exp.column(alias)
        # Return a copy so a caller attaching this into its tree can never
        # corrupt a value the scope (or another caller) also holds (D-L / M1).
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
            expanded = expand_derived_refs_sync(
                sql=raw_sql,
                model=model,
                alias_path=self.root_relation,
                resolve_model=self.bundle.get_referenced_model,
                dialect=self.dialect.sqlglot_name,
                is_root=True,
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
        if name == self.root_model.name:
            return self.root_model
        return self.bundle.get_referenced_model(name) or self.root_model

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
