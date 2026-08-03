"""DEV-1706 Stage 2 — minimal, collision-safe alias allocator.

A single ``AliasAllocator`` is created per top-level ``generate_from_planned``
call and threaded to every ``ScopeFrame`` built during that call. It mints:

* ``_val_<n>`` materialisation aliases (Law 2 — projection-boundary columns),
* CTE names,

seeded from every name already in scope (bundle relations, ``__``-path join
aliases, public projection aliases, model names) so a minted name can never
collide with a user column, a path alias, or a reserved public alias. It also
hands out generation-local ``ScopeFrame`` ids.

The allocator is the *minimal* collision primitive (subsumes DEV-1692's
collision check). DEV-1713 Stage 9 grew this module into the single owner of
every alias / result-key decision:

* :func:`result_key` / :func:`result_key_from_alias` — the DOTTED user-facing
  FINAL-stage keys (``orders.customers.regions.name``);
* :func:`flat_name` — the ``__``-joined INNER-stage downstream schema names
  (``customers__regions__name``, the StageSchema bind contract);
* :func:`encode_alias` / :func:`decode_alias` — the BigQuery / T-SQL dotted
  alias mangling bijection (DEV-1571), relocated here from the dialect package;
* :func:`quote_mixed_case_identifiers` / :func:`maybe_quote_ident` — the
  DEV-1645 mixed-case identifier-quoting policy, relocated here from the
  generator;
* :func:`assert_unique_cte_names` — the DEV-1692 per-``WITH``-scope CTE
  name-collision belt.
"""

from __future__ import annotations

from typing import Optional, Tuple

import sqlglot
from pydantic import BaseModel, ConfigDict, PrivateAttr
from sqlglot import exp


class AliasAllocator(BaseModel):
    """Per-generation collision-safe name allocator (mutable)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # External names the allocator must avoid (user columns, join aliases,
    # public projection aliases, model names).
    # NOSONAR lines below: Pydantic v2 PrivateAttr idiom — the annotation is the
    # attribute's runtime type after model init; the ``PrivateAttr(...)`` sentinel
    # is replaced by Pydantic. S5890 can't model this and is a false positive.
    _reserved: set[str] = PrivateAttr(default_factory=set)  # NOSONAR(S5890)
    # Names already handed out by this allocator.
    _used: set[str] = PrivateAttr(default_factory=set)  # NOSONAR(S5890)
    # Monotonic ``_val_<n>`` cursor (never reset per scope, so sibling scopes
    # in one generation cannot mint the same ``_val_0``).
    _val_seq: int = PrivateAttr(default=0)  # NOSONAR(S5890)
    # Monotonic scope-id cursor.
    _scope_seq: int = PrivateAttr(default=0)  # NOSONAR(S5890)

    def reserve(self, *names: str) -> None:
        """Mark ``names`` as taken so they are never allocated."""
        self._reserved.update(names)

    def _taken(self, name: str) -> bool:
        return name in self._reserved or name in self._used

    def allocate(self, preferred: str) -> str:
        """Return ``preferred`` if free, else ``preferred_2``, ``preferred_3``, …"""
        candidate = preferred
        suffix = 2
        while self._taken(candidate):
            candidate = f"{preferred}_{suffix}"
            suffix += 1
        self._used.add(candidate)
        return candidate

    def allocate_val(self) -> str:
        """Return the next free ``_val_<n>`` materialisation alias."""
        while True:
            candidate = f"_val_{self._val_seq}"
            self._val_seq += 1
            if not self._taken(candidate):
                self._used.add(candidate)
                return candidate

    def allocate_cte(self, preferred: str) -> str:
        """Return a collision-safe CTE name (same walk as :meth:`allocate`)."""
        return self.allocate(preferred)

    def next_scope_id(self, root_relation: str) -> str:
        """Return a generation-local ``ScopeFrame`` id, ``<root>#<seq>``.

        Ephemeral — used only for in-generation materialisation dedup; it is
        never emitted into SQL, result keys, or persisted state (D-F / Codex L1).
        """
        scope_id = f"{root_relation}#{self._scope_seq}"
        self._scope_seq += 1
        return scope_id


# ---------------------------------------------------------------------------
# Result-key / flat-name builders (DEV-1713 Stage 9).
#
# A query renders as either a FINAL stage (its columns are the user-facing
# result keys — DOTTED, ``orders.customers.regions.name``) or an INNER stage
# of a multi-stage DAG (its columns are downstream bind names — ``__``-joined,
# ``customers__regions__name``). These two builders are the single owners of
# those two forms; the planner's is-final flag picks between them so the two
# can never mix (D3 / DEV-1495 bug 1).
# ---------------------------------------------------------------------------


def result_key(*, source_relation: str, path: Tuple[str, ...] = (), leaf: str) -> str:
    """Build the DOTTED final-stage result key from STRUCTURED parts.

    ``source_relation`` then each ``path`` hop then ``leaf``, dot-joined:
    ``result_key(source_relation="orders", path=("customers",), leaf="revenue")``
    → ``"orders.customers.revenue"``.

    ``leaf`` must not contain a dot — hop information belongs in ``path`` so
    ownership is unambiguous. For an already-canonical relative alias that
    legitimately embeds hop dots (a cross-model measure alias such as
    ``customers.revenue_sum``), use :func:`result_key_from_alias` instead.
    """
    if "." in leaf:
        raise ValueError(
            f"result_key leaf must not contain '.': {leaf!r}. Pass hops via "
            f"`path`, or use result_key_from_alias for a canonical dotted alias."
        )
    return ".".join((source_relation, *path, leaf))


def result_key_from_alias(*, source_relation: str, alias: str) -> str:
    """Build a final-stage result key from an already-canonical relative
    ``alias`` that may embed hop dots (e.g. a cross-model measure alias
    ``customers.revenue_sum`` → ``orders.customers.revenue_sum``)."""
    return f"{source_relation}.{alias}"


def flat_name(dotted: str, *, strip_relation: Optional[str] = None) -> str:
    """Flatten a dotted name to its ``__``-joined INNER-stage bind name.

    When ``strip_relation`` is given, the exact ``f"{strip_relation}."``
    prefix is removed first (a dot-boundary match, so ``strip_relation='orders'``
    strips ``orders.`` but never the char prefix of a sibling ``orders_archive``).
    Remaining dots become ``__``:
    ``flat_name("orders.customers.revenue", strip_relation="orders")`` →
    ``"customers__revenue"``.
    """
    remainder = dotted
    if strip_relation is not None:
        prefix = f"{strip_relation}."
        if remainder.startswith(prefix):
            remainder = remainder[len(prefix):]
    return remainder.replace(".", "__")


# ---------------------------------------------------------------------------
# BigQuery / T-SQL dotted-alias mangling bijection (DEV-1571).
#
# Relocated from ``slayer/sql/dialects/_alias_mangle.py`` (DEV-1713 D-a) so the
# naming module owns the result-key <-> wire-identifier bijection. Used by
# ``BigqueryDialect`` (backtick-anchored regex) and ``TsqlDialect`` (bracket-
# anchored regex): both need IDENTICAL encode/decode logic — BigQuery rejects
# dotted output-column names; T-SQL's ORDER BY parser does not resolve bracketed
# dotted identifiers as SELECT aliases. The fix is the same: mangle ``.`` to
# ``___`` on emit, decode on result-row keys.
#
# The bijection's only domain constraint is that ``decode_alias`` inverts
# ``encode_alias`` ONLY on the latter's image. A key like ``my___metric`` (no
# dot in the original) is OUTSIDE the image — decoding it would corrupt the
# value to ``my.metric``. This never bites because SLayer projection aliases are
# always model-qualified (``<model>.<column>``), so they always contain a dot
# and always pass through ``encode_alias``.
# ---------------------------------------------------------------------------

_ALIAS_SEP = "___"


def encode_alias(alias: str) -> str:
    """Forward encode: escape any pre-existing ``___`` to ``______``, then
    map ``.`` to ``___``. Inverse is :func:`decode_alias`."""
    return alias.replace(_ALIAS_SEP, _ALIAS_SEP * 2).replace(".", _ALIAS_SEP)


def decode_alias(key: str) -> str:
    """Reverse of :func:`encode_alias`. Walks ``key`` left-to-right, consuming
    the escape-doubled ``______`` BEFORE the plain ``___`` so the two encodings
    stay unambiguous. Inverse of ``encode_alias`` only on its image (see the
    module-level bijection note)."""
    out: list[str] = []
    i = 0
    n = len(key)
    esc = _ALIAS_SEP * 2
    while i < n:
        if key.startswith(esc, i):
            out.append(_ALIAS_SEP)
            i += len(esc)
        elif key.startswith(_ALIAS_SEP, i):
            out.append(".")
            i += len(_ALIAS_SEP)
        else:
            out.append(key[i])
            i += 1
    return "".join(out)


# ---------------------------------------------------------------------------
# Mixed-case identifier quoting (DEV-1645).
#
# Relocated from ``SQLGenerator`` (DEV-1713 D-b) so the naming module owns the
# identifier-quoting policy. Case-folding dialects (Postgres/Redshift fold to
# lower; Snowflake/Oracle to upper) reach the wrong physical object unless a
# mixed-case identifier is quoted. The generator keeps thin delegators.
# ---------------------------------------------------------------------------


def maybe_quote_ident(ident: Optional[exp.Expression]) -> None:
    """Set ``quoted=True`` in place on ``ident`` when it is an unquoted
    ``Identifier`` containing an uppercase letter. No-op otherwise (None,
    already-quoted, all-lowercase, non-Identifier)."""
    if (
        isinstance(ident, exp.Identifier)
        and not ident.quoted
        and any(c.isupper() for c in ident.this)
    ):
        ident.set("quoted", True)


def quote_mixed_case_identifiers(node: exp.Expression) -> exp.Expression:
    """Quote mixed-case DB identifiers so case-folding dialects reach the right
    physical object. Context-aware: quotes only the column-name leaf of a
    ``Column`` and the physical-table name parts of a ``Table`` — never table
    aliases or the qualifier side of a column reference (SLayer-internal
    aliases that fold consistently within a query). Idempotent; intended as a
    ``.transform(...)`` callback."""
    if isinstance(node, exp.Column):
        maybe_quote_ident(node.this)
    elif isinstance(node, exp.Table):
        maybe_quote_ident(node.this)
        maybe_quote_ident(node.args.get("db"))
        maybe_quote_ident(node.args.get("catalog"))
    return node


# ---------------------------------------------------------------------------
# CTE name-collision belt (DEV-1692).
# ---------------------------------------------------------------------------


def assert_unique_cte_names(sql: str, *, dialect: str = "postgres") -> None:
    """Assert every CTE name is unique WITHIN each ``WITH`` scope.

    CTE names must be unique inside a single ``WITH`` clause, but the same name
    may legally recur in a separate nested ``WITH`` scope (an inner subquery);
    each ``exp.With`` is validated independently. Raises ``ValueError`` on a
    same-scope duplicate — the loud failure the DEV-1692 de-collision guards
    against (a duplicate ``shifted_*`` CTE otherwise silently shadows).
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    for with_node in parsed.find_all(exp.With):
        names = [cte.alias_or_name for cte in with_node.expressions]
        seen: set[str] = set()
        for name in names:
            if name in seen:
                raise ValueError(
                    f"Duplicate CTE name {name!r} within one WITH scope: {names}"
                )
            seen.add(name)
