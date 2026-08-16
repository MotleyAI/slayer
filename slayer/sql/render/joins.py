"""The one grain join-back builder (P-I).

Every place a scope's rows are joined back to another scope on the query grain
builds its ``ON`` predicate here: the cross-model (``_cm_``) join-back, the
windowed (``_wm_``) join-back and its inner ``_src`` join, and the time-shift
``sjoin_`` pair.

Two properties, both of which had counter-examples before this module existed:

**Null-safe.** A grain member is frequently NULL — a nullable dimension, an
outer-joined column, a bucket with no rows. Plain ``=`` never matches NULL
against NULL, so the group silently loses its aggregate instead of receiving it.
The dialect strategy owns the spelling (``IS NOT DISTINCT FROM``, MySQL
``<=>``, SQLite ``IS``, or the expanded ``a = b OR (a IS NULL AND b IS NULL)``
where there is no native operator).

**Built as AST, never re-parsed.** The superseded helper rendered both sides to
pre-quoted strings and parsed them back. SLayer's public aliases are dotted
(``orders.customers.status``), and on a dialect that mangles dots at emission
the round-trip re-reads such an alias as a multi-part *reference*, producing a
qualifier for a table that does not exist::

    ON `_base___orders___customers`.`status` IS NOT DISTINCT FROM ...
       ^^^^^^^^^^^^^^^^^^^^^^^^^^ not a table in scope

Building the column node directly makes that unrepresentable: the alias is one
identifier and stays one identifier.

The core builder takes **expression** operands so a caller can compare anything
it has already resolved — a cast, a date-truncated bucket, a materialised
alias. :func:`grain_alias_column` covers the common case, where both sides are
columns projected under the same public alias in two different scopes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Tuple

from sqlglot import exp

if TYPE_CHECKING:  # pragma: no cover - typing only
    from slayer.sql.dialects.base import SqlDialect

__all__ = ["grain_alias_column", "build_grain_joinback_condition"]


def grain_alias_column(*, alias: str, table: str) -> exp.Column:
    """A reference to ``table``'s output column named ``alias``.

    ``alias`` is a projected column NAME, not a path: it is quoted as a single
    identifier even when it contains dots, which is exactly what the dotted
    public aliases (``orders.customers.status``) require. ``table`` is a CTE
    alias minted by the allocator and needs no quoting of its own.
    """
    return exp.Column(
        this=exp.to_identifier(alias, quoted=True),
        table=exp.to_identifier(table),
    )


def build_grain_joinback_condition(
    *,
    pairs: Sequence[Tuple[exp.Expression, exp.Expression]],
    dialect: "SqlDialect",
) -> Optional[exp.Expression]:
    """Null-safe ``ON`` predicate equating each ``(left, right)`` grain member.

    Returns ``None`` for an empty grain — a scalar aggregate has no grain to
    join on, and the caller emits a ``CROSS JOIN`` instead. Returning a truthy
    ``TRUE`` would look equivalent but is not: it would turn every scalar
    aggregate's join into a predicate-bearing one and hide the distinction the
    empty grain actually carries.

    Operands are copied, so a caller may pass expressions it also holds
    elsewhere without the AST being re-parented out from under it.
    """
    conditions = [
        dialect.build_null_safe_eq(left.copy(), right.copy())
        for left, right in pairs
    ]
    if not conditions:
        return None
    combined = conditions[0]
    for condition in conditions[1:]:
        combined = exp.And(this=combined, expression=condition)
    return combined
