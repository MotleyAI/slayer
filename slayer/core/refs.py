"""Shared identifier and aggregation-suffix definitions for SLayer references.

DEV-1369 consolidates the identifier-shape regexes and aggregation-suffix
parsing that previously lived in four different files (``formula.py``,
``dbt/converter.py``, ``engine/enrichment.py``, ``memories/resolver.py``).
Keeping a single source of truth prevents the four copies from drifting
out of sync.

This module is intentionally side-effect-free and depends on nothing from
``slayer.core.models`` / ``slayer.core.query`` so it can be imported from
those modules' validators without circular import risk.
"""
from __future__ import annotations

import re
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Identifier shapes
# ---------------------------------------------------------------------------

# A bare SQL identifier (no dots, no double underscores constraint at this
# level — the dunder restriction is applied selectively at user-input time).
IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# An identifier or a dotted path of identifiers, e.g. ``revenue``,
# ``customers.revenue``, ``a.b.c.d``. Used to scan formula text for
# reference candidates.
IDENT_OR_PATH_RE = re.compile(r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*")

# A string that is exactly a chain of ``.``-joined identifiers and nothing
# else. Distinguishes ``customers.regions.name`` from a SQL fragment that
# happens to contain a dot.
DOTTED_IDENT_REF_RE = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)+$")

# Aggregation colon syntax, e.g. ``revenue:sum``, ``*:count``,
# ``customers.revenue:weighted_avg(weight=quantity)``. Group 1 is the
# measure name (``*``, identifier, or dotted path, optionally
# ``path.*``); group 2 is the aggregation name; group 3 is the optional
# ``(...)`` arglist.
AGG_REF_RE = re.compile(
    r"(\*|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*(?:\.\*)?)"  # measure / *
    r":"
    r"([a-zA-Z_]\w*)"
    r"(\([^)]*\))?"
)


# ---------------------------------------------------------------------------
# Aggregation-suffix utilities
# ---------------------------------------------------------------------------

_NON_IDENT_RE = re.compile(r"\W+")


def agg_signature_suffix(
    agg_args: Optional[List[str]],
    agg_kwargs: Optional[dict],
) -> str:
    """Build a deterministic identifier suffix from aggregation args/kwargs.

    Returns the empty string when both are empty so unparameterized
    aggregations keep their existing canonical names. For parameterized
    variants (``last(created_at)`` vs ``last(updated_at)``,
    ``percentile(p=0.5)`` vs ``percentile(p=0.95)``,
    ``sum(window='90d')`` vs ``sum(window='30d')``) the suffix
    differentiates them so they don't collapse onto a single hidden alias.
    """
    args = agg_args or []
    kwargs = agg_kwargs or {}
    if not args and not kwargs:
        return ""
    parts: List[str] = []
    for a in args:
        sanitized = _NON_IDENT_RE.sub("_", str(a)).strip("_")
        if sanitized:
            parts.append(sanitized)
    for k in sorted(kwargs.keys()):
        sk = _NON_IDENT_RE.sub("_", str(k)).strip("_")
        sv = _NON_IDENT_RE.sub("_", str(kwargs[k])).strip("_")
        if sk:
            parts.append(sk)
        if sv:
            parts.append(sv)
    return "_" + "_".join(parts) if parts else ""


def canonical_agg_name(
    measure_name: str,
    aggregation_name: str,
    agg_args: Optional[List[str]] = None,
    agg_kwargs: Optional[dict] = None,
) -> str:
    """Canonical hidden-column name for an aggregated measure ref.

    ``revenue:sum`` → ``revenue_sum``; ``*:count`` → ``_count``;
    ``revenue:percentile(p=0.5)`` → ``revenue_percentile_p_0_5``;
    ``revenue:sum(window='90d')`` → ``revenue_sum_window_90d``.
    """
    suffix = agg_signature_suffix(agg_args, agg_kwargs)
    if measure_name == "*":
        return f"_{aggregation_name}{suffix}"
    return f"{measure_name}_{aggregation_name}{suffix}"


def strip_agg_suffix(raw: str) -> Tuple[str, Optional[str]]:
    """Return ``(prefix, agg_name)`` after stripping a trailing ``:agg``
    or ``:agg(...)``.

    ``agg`` may be parametric: ``revenue:weighted_avg(weight=qty)`` →
    ``("revenue", "weighted_avg")``. The args themselves are discarded
    since the aggregation is not an independent entity.

    Locates the *outermost* colon (one not inside parentheses) so a
    parametric aggregation like ``revenue:last(created_at)`` doesn't
    fool the scan.
    """
    depth = 0
    for i, ch in enumerate(raw):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == ":" and depth == 0:
            prefix = raw[:i]
            tail = raw[i + 1:]
            agg = tail.split("(", 1)[0]
            return prefix, agg
    return raw, None


# ---------------------------------------------------------------------------
# User-input validation
# ---------------------------------------------------------------------------


def reject_user_dunder(value: str, *, context: str) -> str:
    """Reject ``__`` in user-supplied query / DSL input.

    The double-underscore separator is reserved for internal join-path
    aliases (``customers__regions``) and for virtual-model column names
    produced by ``_query_as_model`` (``stores__name``). User-authored
    queries and ModelMeasure formulas must use single-dot DSL paths
    (``customers.regions.name``).

    SQL-mode fields (``Column.sql``, ``Column.filter``,
    ``SlayerModel.filters``) intentionally do NOT call this validator —
    they accept ``__`` as legitimate join-alias syntax.

    Returns the value unchanged on success; raises ``ValueError`` on a
    violation. The message names the offending context so multi-field
    validators surface a helpful error.
    """
    if "__" in value:
        raise ValueError(
            f"{context} contains a reserved double-underscore (`__`) in "
            f"{value!r}. `__` is reserved for internal join-path aliases "
            f"in generated SQL — use single-dot DSL paths "
            f"(e.g. `customers.region`) in queries and ModelMeasure formulas."
        )
    return value
