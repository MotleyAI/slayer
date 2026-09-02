"""Shared identifier and aggregation-suffix definitions for SLayer references (single source
of truth). Depends only on ``slayer.core.keys`` so model/query validators avoid circular imports."""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from slayer.core.keys import ColumnKey, ColumnSqlKey, TimeTruncKey

# Identifier shapes

# A bare SQL identifier; the dunder restriction is applied at user-input time.
IDENTIFIER_RE = re.compile(r"^[a-zA-Z_]\w*$")

# An identifier or dotted path; used to scan formula text for reference candidates.
IDENT_OR_PATH_RE = re.compile(r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*")

# Exactly a chain of ``.``-joined identifiers — distinguishes a dotted ref from a
# SQL fragment that merely contains a dot.
DOTTED_IDENT_REF_RE = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)+$")

# Aggregation colon syntax (``revenue:sum``, ``*:count``). Group 1 measure name,
# group 2 aggregation name, group 3 optional ``(...)`` arglist.
AGG_REF_RE = re.compile(
    r"(\*|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*(?:\.\*)?)"  # measure / *
    r":"
    r"([a-zA-Z_]\w*)"
    r"(\([^)]*\))?"
)


# Aggregation-suffix utilities

_NON_IDENT_RE = re.compile(r"\W+")


def agg_signature_suffix(
    agg_args: list[str] | None,
    agg_kwargs: dict | None,
) -> str:
    """Deterministic identifier suffix from aggregation args/kwargs (empty when both empty); differentiates parametric variants (``percentile(p=0.5)`` vs ``(p=0.95)``)."""
    args = agg_args or []
    kwargs = agg_kwargs or {}
    if not args and not kwargs:
        return ""
    parts: list[str] = []
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


def _partition_key_display(key: Any) -> str:
    if isinstance(key, TimeTruncKey):
        key = key.column
    if isinstance(key, ColumnKey):
        parts = [*key.path, key.leaf]
    elif isinstance(key, ColumnSqlKey):
        parts = [*key.path, key.column_name]
    else:
        parts = [str(key)]
    return _NON_IDENT_RE.sub("_", "_".join(parts)).strip("_")


def partition_by_suffix(partition_keys) -> str:
    """Deterministic identifier suffix for ``partition_keys``: ``None`` -> empty; empty frozenset (grand total) -> ``_partition_by``; non-empty -> ``_partition_by`` + sorted displays."""
    if partition_keys is None:
        return ""
    displays = sorted(_partition_key_display(k) for k in partition_keys)
    return "_partition_by" + "".join(f"_{d}" for d in displays)


def _decimal_to_plain_str(value: Decimal) -> str:
    """``value`` as a plain-decimal string, no scientific notation: ``str(Decimal("1E-7"))`` yields ``"1E-7"``, which the generator's ``_SAFE_AGG_PARAM_RE`` allowlist rejects."""
    s = f"{value:f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def agg_kwarg_canonical_str(value: Any) -> str:
    """Canonicalize an AggregateKey kwarg/arg value to the SQL-string form the generator's
    ``_validate_agg_param_value`` accepts (so a ``ColumnKey`` never leaks as Pydantic repr).
    ``bool``/``None`` raise (kept distinct from numerics to fail loudly); ``ColumnKey`` → ``[path.]leaf``."""
    if isinstance(value, bool):
        # bool is-a int, must check first.
        raise TypeError(
            f"AggregateKey kwarg cannot be bool: {value!r}",
        )
    if value is None:
        raise TypeError("AggregateKey kwarg cannot be None")
    if isinstance(value, Decimal):
        return _decimal_to_plain_str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        # Decimal(str(float)) preserves human-readable text (matches planner normalize_scalar).
        return _decimal_to_plain_str(Decimal(str(value)))
    if isinstance(value, str):
        return value
    if isinstance(value, ColumnKey):
        if value.path:
            return ".".join(value.path) + "." + value.leaf
        return value.leaf
    if isinstance(value, ColumnSqlKey):
        if value.path:
            return ".".join(value.path) + "." + value.column_name
        return value.column_name
    raise TypeError(
        f"AggregateKey kwarg value of type {type(value).__name__!r} "
        f"is not supported: {value!r}",
    )


def canonical_agg_name(
    measure_name: str,
    aggregation_name: str,
    agg_args: list[str] | None = None,
    agg_kwargs: dict | None = None,
) -> str:
    """Canonical hidden-column name for an aggregated measure ref (``revenue:sum`` → ``revenue_sum``, ``*:count`` → ``_count``)."""
    suffix = agg_signature_suffix(agg_args, agg_kwargs)
    if measure_name == "*":
        return f"_{aggregation_name}{suffix}"
    return f"{measure_name}_{aggregation_name}{suffix}"


def strip_agg_suffix(raw: str) -> tuple[str, str | None]:
    """Return ``(prefix, agg_name)`` stripping a trailing ``:agg``/``:agg(...)`` (arglist discarded); locates the outermost colon (outside parens) so ``revenue:last(created_at)`` isn't fooled."""
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


def split_agg_suffix(raw: str) -> tuple[str, str | None]:
    """Return ``(prefix, suffix)`` splitting a trailing ``:agg`` but keeping the full suffix (args included) — unlike :func:`strip_agg_suffix`, so a re-rooted reference re-attaches it verbatim."""
    depth = 0
    for i, ch in enumerate(raw):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == ":" and depth == 0:
            return raw[:i], raw[i + 1:]
    return raw, None


