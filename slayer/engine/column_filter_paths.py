"""DEV-1503 — planner-facing helper for ``Column.filter`` join-path discovery.

When the binder constructs a ``SqlExprKey`` for an ``AggregateKey.column_filter_key``,
it computes the typed set of non-anchor join paths the filter touches and stamps
them on the key as ``SqlExprKey.referenced_join_paths``. The DEV-1503 trigger
predicate then reads this typed field instead of re-parsing SQL text at plan
time — preserving the typed pipeline's "no string rewriting after parse" P7
boundary at the planner layer.

The actual rewriting for RENDERING (inlining derived refs inside the
``SUM(CASE WHEN <filter> THEN col END)`` wrapper) still lives in
``slayer/sql/generator.py`` (DEV-1494's ``_expand_column_filter_sql``). The
helper here is a parallel structural-analysis pass that returns paths only,
shared by the binder.

The discovery rules match the generator-side ones byte-for-byte:

* Same-model bare refs ("status") qualify to the anchor relation — no path.
* Cross-model dotted refs ("loss_payment.has_flag") contribute ``("loss_payment",)``.
* Multi-hop ``__``-delimited refs ("loss_payment__claim.state") contribute
  ``("loss_payment",)`` AND ``("loss_payment", "claim")``.
* Bare refs that name a non-trivial DERIVED column whose own ``Column.sql``
  crosses a join (``is_eu`` reaching ``customers.region``) expand to the
  derived sql and contribute the expansion's paths.
* Refs inside nested scopes (subqueries, set-op branches) are ignored —
  they belong to the inner rowset.
* Aliases that don't resolve as a join walk on the anchor model are
  silently skipped — they may be CTE / subquery aliases out of scope.
"""

from __future__ import annotations

from typing import Optional, Tuple

import sqlglot
from sqlglot import exp

from slayer.core.models import Column, SlayerModel
from slayer.engine.column_expansion import (
    _is_trivial_base,
    collect_root_scope_joined_paths,
    expand_derived_refs_sync,
)
from slayer.engine.source_bundle import ResolvedSourceBundle


# Fallback dialect chain (Codex round 7) — the planner doesn't carry the
# datasource's dialect, so a user-configured backend with dialect-specific
# syntax in ``Column.filter`` (MySQL backticks, T-SQL square brackets,
# ClickHouse-specific functions) could fail the Postgres parse and
# silently return no referenced join paths. The DEV-1503 trigger would
# then miss and the generator would render the filter inline in ``_base``,
# pulling the cross-model join into the host rowset. Try each dialect in
# order; only return ``()`` if ALL fail. The path discovery itself is
# dialect-agnostic (AST walking, no SQL emission); the dialect-aware
# re-parse for emission still happens on the generator side.
_PLANNER_PARSE_DIALECT_CHAIN: Tuple[Optional[str], ...] = (
    "postgres",
    None,         # sqlglot's permissive default — accepts ANSI SQL broadly
    "mysql",
    "clickhouse", # ClickHouse-only constructs (countIf, distinct identifier escapes) — CR PR #153 r3350000228
    "bigquery",
    "tsql",
)


def _parse_filter_sql_any_dialect(sql: str) -> Optional[exp.Expression]:
    """Parse ``sql`` trying each dialect in the fallback chain.

    Returns the first successful parse, or ``None`` when every dialect
    rejects the input — that fall-through preserves the catch-all the
    original ``except Exception`` provided for malicious / unparseable
    payloads (the generator's dialect-aware emission is the
    authoritative gate).
    """
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        try:
            return sqlglot.parse_one(sql, dialect=dialect)
        except Exception:
            continue
    return None


def _expand_derived_refs_any_dialect(
    *,
    sql: str,
    model: SlayerModel,
    alias_path: str,
    bundle: ResolvedSourceBundle,
) -> Optional[str]:
    """Run ``expand_derived_refs_sync`` against each dialect in the chain.

    ``expand_derived_refs_sync`` parses ``sql`` (and the derived columns'
    own ``sql`` fields) internally with the supplied dialect. If a
    derived column whose ``Column.sql`` uses dialect-specific syntax
    (MySQL backticks, BigQuery struct literals, etc.) tips over the
    Postgres parser, the expansion would silently drop join paths the
    DEV-1503 trigger needs (Codex round 9). Try the same chain
    ``_parse_filter_sql_any_dialect`` uses; return the first
    successful expansion or ``None`` if every dialect fails.
    """
    for dialect in _PLANNER_PARSE_DIALECT_CHAIN:
        if dialect is None:
            # ``expand_derived_refs_sync`` requires a dialect string.
            continue
        try:
            expanded = expand_derived_refs_sync(
                sql=sql,
                model=model,
                alias_path=alias_path,
                resolve_model=bundle.get_referenced_model,
                dialect=dialect,
            )
        except Exception:
            continue
        if expanded:
            return expanded
    return None


def _is_nontrivial_derived(model: SlayerModel, name: str) -> bool:
    """True iff ``name`` is a column on ``model`` whose ``Column.sql`` is a
    non-trivial expression (set, and not just a bare-identifier remap).

    Mirrors ``SQLGenerator._is_nontrivial_derived``; duplicated here to keep
    the planner-facing helper free of generator imports.
    """
    col: Optional[Column] = next(
        (c for c in model.columns if c.name == name), None,
    )
    return col is not None and col.sql is not None and not _is_trivial_base(
        column=col,
    )


def _is_anchor_local_col_ref(
    col: exp.Column, *, anchor_aliases: set,
) -> bool:
    """A column ref counts as "on the anchor" when it is bare (no ``table``)
    OR self-qualified to the anchor (``orders.is_eu`` where ``orders`` is
    the anchor relation / model name).
    """
    if not isinstance(col.this, exp.Identifier):
        return False
    tbl = col.args.get("table")
    if tbl is None:
        return True
    return tbl.name in anchor_aliases


def _expand_filter_sql_if_anchor_derived(
    *,
    parsed: exp.Expression,
    canonical_sql: str,
    anchor_model: SlayerModel,
    anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Optional[exp.Expression]:
    """Expand any non-trivial derived anchor-local refs in ``parsed``,
    re-parsing the expanded SQL. Returns the new AST, the original
    ``parsed`` when no derived ref was present, or ``None`` when the
    expansion produced unparseable output (caller should bail to ``()``).

    Mirrors the generator's ``_expand_column_filter_sql`` gate so the
    planner and the renderer surface the same set of crossed joins.
    """
    anchor_aliases = {anchor_relation, anchor_model.name}
    anchor_local_names = {
        col.this.name
        for col in parsed.find_all(exp.Column)
        if _is_anchor_local_col_ref(col, anchor_aliases=anchor_aliases)
    }
    has_derived = any(
        _is_nontrivial_derived(anchor_model, n) for n in anchor_local_names
    )
    if not has_derived:
        return parsed

    # Degenerate: the whole predicate IS a single derived column ref
    # (``filter="is_eu"`` or self-qualified ``filter="orders.is_eu"``).
    # ``expand_derived_refs_sync`` rewrites refs via in-place
    # ``col.replace`` which is a no-op on the AST root, so expand the
    # column's sql directly.
    if (
        isinstance(parsed, exp.Column)
        and _is_anchor_local_col_ref(parsed, anchor_aliases=anchor_aliases)
        and _is_nontrivial_derived(anchor_model, parsed.name)
    ):
        col = next(
            (c for c in anchor_model.columns if c.name == parsed.name), None,
        )
        if col is None or not col.sql:
            return None
        sql_to_expand = col.sql
    else:
        sql_to_expand = canonical_sql

    expanded = _expand_derived_refs_any_dialect(
        sql=sql_to_expand,
        model=anchor_model,
        alias_path=anchor_relation,
        bundle=bundle,
    )
    if not expanded:
        return parsed
    return _parse_filter_sql_any_dialect(expanded)


def compute_column_filter_join_paths(
    *,
    canonical_sql: Optional[str],
    anchor_model: SlayerModel,
    anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Tuple[Tuple[str, ...], ...]:
    """Return the ordered tuple of non-anchor join-path prefixes a
    ``Column.filter`` predicate touches after derived-ref expansion.

    ``anchor_model`` is the model the filter is bound against — for a
    filtered-local measure on the host (``AggregateKey.source.path == ()``),
    the anchor is the host; for a cross-model aggregate on a target column
    (``source.path == ("customers",)``), the anchor is ``customers``.

    Returns ``()`` for same-model filters (no cross-anchor joins), for an
    empty / unparseable canonical_sql, and as a defensive fallback if join
    resolution fails partway through.

    Multi-hop alias paths emit each prefix once (``loss_payment__claim``
    yields ``("loss_payment",)`` and ``("loss_payment", "claim")``).
    """
    if not canonical_sql:
        return ()
    parsed = _parse_filter_sql_any_dialect(canonical_sql)
    if parsed is None:
        return ()

    expanded = _expand_filter_sql_if_anchor_derived(
        parsed=parsed,
        canonical_sql=canonical_sql,
        anchor_model=anchor_model,
        anchor_relation=anchor_relation,
        bundle=bundle,
    )
    if expanded is None:
        return ()
    parsed = expanded

    # ``_walk_root_scope_paths`` exercises sqlglot's scope analyser, which
    # can raise ``TypeError`` etc. on unusual / malicious payloads (SQL
    # injection attempts like ``status = 'x' UNION SELECT * FROM users``).
    # The dialect-aware generator path is the authoritative gate for those —
    # it raises ``ParseError`` / ``ValueError`` at SQL emission. The planner
    # discovery here is a best-effort structural pass; swallow any internal
    # parser failure so it can't shadow the generator's rejection.
    try:
        return _walk_root_scope_paths(
            parsed=parsed,
            anchor_model=anchor_model,
            anchor_relation=anchor_relation,
            bundle=bundle,
        )
    except Exception:
        return ()


def _walk_root_scope_paths(
    *,
    parsed: exp.Expression,
    anchor_model: SlayerModel,
    anchor_relation: str,
    bundle: ResolvedSourceBundle,
) -> Tuple[Tuple[str, ...], ...]:
    """Collect every root-scope ``<alias>.<col>`` whose ``alias`` resolves as
    a join walk on ``anchor_model``, returning the ordered tuple of path
    prefixes (de-duplicated).

    Thin shim over the shared ``collect_root_scope_joined_paths`` helper so
    the planner and ``SQLGenerator._joined_paths_in_sql`` agree on what
    counts as "crosses a join."
    """
    return tuple(collect_root_scope_joined_paths(
        parsed=parsed,
        source_model=anchor_model,
        source_relation=anchor_relation,
        bundle=bundle,
    ))
