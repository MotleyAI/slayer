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
    _root_scope_column_ids,
    expand_derived_refs_sync,
)
from slayer.engine.source_bundle import ResolvedSourceBundle


# Planner-side default dialect for parsing column filter SQL. The path
# discovery is dialect-agnostic (AST walking, no SQL emission); the dialect
# matters only for the initial parse, and Postgres parses the supported
# Mode-A predicate shapes cleanly. The dialect-aware re-parse for emission
# still happens on the generator side.
_PLANNER_PARSE_DIALECT = "postgres"


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
    try:
        parsed = sqlglot.parse_one(canonical_sql, dialect=_PLANNER_PARSE_DIALECT)
    except Exception:
        return ()

    # If the predicate names any non-trivial DERIVED column on the anchor,
    # inline-expand it before scanning. This is the same gate the generator's
    # ``_expand_column_filter_sql`` uses for rendering — applied here to
    # discovery so paths the derived expansion introduces are surfaced.
    bare_names = {
        col.this.name
        for col in parsed.find_all(exp.Column)
        if col.args.get("table") is None and isinstance(col.this, exp.Identifier)
    }
    has_derived = any(
        _is_nontrivial_derived(anchor_model, n) for n in bare_names
    )
    if has_derived:
        # Degenerate: the whole predicate IS a bare derived column ref
        # (``filter="is_eu"``). ``expand_derived_refs_sync`` rewrites refs
        # via in-place ``col.replace`` which is a no-op on the AST root,
        # so expand the column's sql directly.
        if (
            isinstance(parsed, exp.Column)
            and parsed.args.get("table") is None
            and _is_nontrivial_derived(anchor_model, parsed.name)
        ):
            col = next(
                (c for c in anchor_model.columns if c.name == parsed.name), None,
            )
            if col is None or not col.sql:
                return ()
            expanded = expand_derived_refs_sync(
                sql=col.sql,
                model=anchor_model,
                alias_path=anchor_relation,
                resolve_model=bundle.get_referenced_model,
                dialect=_PLANNER_PARSE_DIALECT,
            )
        else:
            expanded = expand_derived_refs_sync(
                sql=canonical_sql,
                model=anchor_model,
                alias_path=anchor_relation,
                resolve_model=bundle.get_referenced_model,
                dialect=_PLANNER_PARSE_DIALECT,
            )
        if expanded:
            try:
                parsed = sqlglot.parse_one(expanded, dialect=_PLANNER_PARSE_DIALECT)
            except Exception:
                return ()

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

    Mirrors ``SQLGenerator._joined_paths_in_sql`` so the planner and the
    generator agree on which refs count as "crosses a join."
    """
    root_ids = _root_scope_column_ids(parsed=parsed)
    seen: set = set()
    ordered: list = []
    for col in parsed.find_all(exp.Column):
        tbl = col.args.get("table")
        if tbl is None or col.args.get("db") or col.args.get("catalog"):
            continue
        if id(col) not in root_ids:
            continue
        alias = tbl.name
        if alias in (anchor_relation, anchor_model.name):
            continue
        segments = alias.split("__")
        current: SlayerModel = anchor_model
        resolved = True
        for seg in segments:
            join = next(
                (j for j in current.joins if j.target_model == seg), None,
            )
            if join is None:
                resolved = False
                break
            nxt = bundle.get_referenced_model(seg)
            if nxt is None:
                resolved = False
                break
            current = nxt
        if not resolved:
            continue
        for i in range(1, len(segments) + 1):
            prefix = tuple(segments[:i])
            if prefix not in seen:
                seen.add(prefix)
                ordered.append(prefix)
    return tuple(ordered)
