"""DEV-1756: shared identifier-length fitting + the write-side substitution.

Backends cap identifier length, and Postgres — the tightest of the Tier-1 set
at 63 bytes — **silently truncates** past it (a NOTICE, never an error). SLayer's
universal alias convention ``<root_model>.<join.path>.<column>`` crosses that on
a 3-hop join, so two sibling aliases collapse onto one effective output name and
the query either fails with ``AmbiguousColumnError`` or, worse, quietly returns
a column under a name nobody looks up.

Two primitives live here:

:func:`fit_identifier`
    Shortens an over-limit identifier to ``<head>_<hash8>_<tail>``. It is a PURE
    function of ``name`` — the digest covers the *full original*, not the
    truncated head — which is what lets the read side rebuild the
    emitted->canonical map by simply re-running it, with no map threaded through
    generation. Identity when the name already fits, so the overwhelmingly
    common case emits byte-identical SQL.

:func:`substitute_quoted`
    Rewrites quoted identifier tokens in emitted SQL. Driven by an exact
    canonical->emitted map rather than a length regex, so — unlike a regex over
    arbitrary quoted spans — the only text it can reach inside a string literal
    is the exact quoted spelling of one of *this query's own* over-limit
    aliases. See its docstring for the residual case.

Sibling of :mod:`slayer.sql.dialects._alias_mangle` (the BigQuery/T-SQL dotted
alias codec) and composes with it: those dialects size against ``encode_alias``
via the ``expand`` hook, because their mangling *lengthens* the identifier after
fitting.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping


#: Hex characters of digest carried in the marker. 32 bits is ample given that
#: every namespace validates its allocation and raises on collision — width only
#: affects how often that (astronomically rare) error could fire.
HASH_LEN = 8

#: Below this there is no room for both the marker and any readable context.
#: Every dialect SLayer supports is far above it; the guard exists so a
#: mis-configured limit fails loudly rather than emitting a useless name.
MIN_LIMIT = 16

#: ``_`` + digest + ``_``
_MARKER_LEN = HASH_LEN + 2

#: Trimmed from the inner edges of head/tail so the marker never abuts a path
#: separator (``foo._a1b2c3d4_.bar``).
_TRIM = "._"

#: Two-phase substitution sentinel. NUL bytes cannot appear in SQL SLayer
#: generates, so a sentinel can never be confused with real content.
_SENTINEL = "\x00\x01{}\x01\x00"


def _digest(name: str) -> str:
    """Stable digest of the FULL original name.

    ``sha256`` rather than the builtin ``hash`` because the read side
    recomputes this in a different process, where ``hash`` would be salted by
    ``PYTHONHASHSEED``. Patched by tests to force collisions.
    """
    return hashlib.sha256(name.encode("utf-8")).hexdigest()[:HASH_LEN]


def _head_bytes(name: str, n: int) -> str:
    """Leading ``n`` bytes of ``name``, cut on a UTF-8 codepoint boundary."""
    if n <= 0:
        return ""
    return name.encode("utf-8")[:n].decode("utf-8", "ignore")


def _tail_bytes(name: str, n: int) -> str:
    """Trailing ``n`` bytes of ``name``, cut on a UTF-8 codepoint boundary."""
    if n <= 0:
        return ""
    return name.encode("utf-8")[-n:].decode("utf-8", "ignore")


def fit_identifier(
    name: str,
    *,
    limit: int | None,
    expand: Callable[[str], str] | None = None,
) -> str:
    """Shorten ``name`` to at most ``limit`` **bytes** as ``<head>_<hash>_<tail>``.

    Returns ``name`` unchanged when ``limit`` is ``None`` (unbounded dialect) or
    the name already fits — shortening only kicks in when it must, so SQL stays
    readable everywhere else and existing emission tests see no churn.

    Both ends are preserved because they carry the information: the head names
    the root model, the tail names the actual column. In the reported repro the
    two colliding aliases differ *only* in their final segment, so a head-only
    truncation would render them indistinguishable in ``dry_run`` output.

    ``expand`` sizes the budget against a post-fit transform. BigQuery and T-SQL
    mangle ``.`` to ``___`` *after* this runs, adding 2 bytes per dot; passing
    ``encode_alias`` makes the loop shrink until the mangled form fits. The
    returned value is NOT expanded — the dialect's own pass does that.

    Injective in practice, and any residual collision is caught by the caller's
    per-namespace allocation check, so the scheme is collision-*detected* rather
    than collision-free.
    """
    grow = expand or (lambda s: s)
    if limit is None or len(grow(name).encode("utf-8")) <= limit:
        return name
    if limit < MIN_LIMIT:
        raise ValueError(
            f"identifier limit must be at least MIN_LIMIT ({MIN_LIMIT}) bytes to "
            f"leave room for the {_MARKER_LEN}-byte hash marker plus context; got {limit}"
        )
    marker = f"_{_digest(name)}_"
    # Shrink the budget until the (possibly expanded) candidate fits. The final
    # iteration leaves head and tail empty, yielding the bare ``_<digest>_``,
    # which is <= MIN_LIMIT bytes and starts with an underscore — legal even
    # unquoted, where a bare hex digest could have started with a digit.
    for budget in range(limit, _MARKER_LEN - 1, -1):
        avail = budget - _MARKER_LEN
        tail_n = avail // 2
        head_n = avail - tail_n
        head = _head_bytes(name, head_n).rstrip(_TRIM)
        tail = _tail_bytes(name, tail_n).lstrip(_TRIM)
        candidate = f"{head}{marker}{tail}"
        if len(grow(candidate).encode("utf-8")) <= limit:
            return candidate
    raise ValueError(
        f"cannot fit {name!r} into {limit} bytes: the supplied `expand` grows "
        f"even the bare {_MARKER_LEN}-byte marker beyond the limit"
    )


def substitute_quoted(
    sql: str,
    mapping: Mapping[str, str],
    *,
    quote: Callable[[str], str],
) -> str:
    """Replace each quoted ``canonical`` identifier token with its ``emitted``
    form, everywhere it appears.

    Two-phase (canonical -> sentinel -> emitted) so no substitution can be
    re-read by a later one. With today's allocation the key set (over-limit) and
    the value set (within-limit) are provably disjoint, so a single sequential
    pass would also be correct; the two-phase form keeps that from becoming a
    silent trap if the allocation ever changes.

    Only *quoted* occurrences move. A bare occurrence of the same text is a
    different identifier — a table alias, say — and is left alone, which is what
    keeps the deferred join-path-alias surface (DEV-1743) out of scope here.

    This is a string pass, not an AST pass, so it is not literal-aware. Being
    keyed to an exact alias set rather than a length regex bounds the exposure
    to one contrived case: a string literal containing the exact dialect-quoted
    spelling of an over-limit alias *of the same query* (``note = '"Root.a.b.
    <62 more bytes>"'``) would have its contents rewritten. A regex over quoted
    spans — which the BigQuery/T-SQL dot-manglers already run over this same
    SQL — has strictly wider exposure, so this pass does not add a risk class.
    """
    if not mapping:
        return sql
    items = sorted(mapping.items())
    for index, (canonical, _) in enumerate(items):
        sql = sql.replace(quote(canonical), _SENTINEL.format(index))
    for index, (_, emitted) in enumerate(items):
        sql = sql.replace(_SENTINEL.format(index), quote(emitted))
    return sql
