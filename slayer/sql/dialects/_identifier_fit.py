"""DEV-1756: shared identifier-length fitting + the write-side substitution.

Postgres SILENTLY truncates identifiers past 63 bytes (a NOTICE, never an
error), so SLayer's ``<root>.<join.path>.<column>`` aliases can collapse two
siblings onto one output name on a deep join.

:func:`fit_identifier` shortens an over-limit name to ``<head>_<hash8>_<tail>``.
It is a PURE function of ``name`` (the digest covers the full original), so the
read side rebuilds the emitted->canonical map by re-running it — no map threaded
through generation. :func:`substitute_quoted` applies that map to emitted SQL.
BigQuery/T-SQL size the budget against their post-mangle form via ``expand``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping


HASH_LEN = 8  # digest hex chars; collisions are caught per-namespace, not by width
MIN_LIMIT = 16  # floor so a mis-configured limit fails loudly, not silently
_MARKER_LEN = HASH_LEN + 2  # ``_`` + digest + ``_``
_TRIM = "._"  # trimmed off head/tail so the marker never abuts a separator
#: NUL bytes never appear in generated SQL, so this sentinel can't clash.
_SENTINEL = "\x00\x01{}\x01\x00"


def _digest(name: str) -> str:
    """Stable digest of the full original name.

    ``sha256``, not the builtin ``hash`` (salted by ``PYTHONHASHSEED`` and the
    read side recomputes in another process). Patched by tests to force collisions.
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

    Identity when ``limit`` is ``None`` or the name already fits, so common-case
    SQL is byte-identical. Both ends are kept: head names the root model, tail
    the column, and the repro's colliding aliases differ only in their tail.

    ``expand`` sizes the budget against a post-fit transform (BigQuery/T-SQL
    ``encode_alias`` adds 2 bytes per dot); the return value itself is NOT
    expanded. Residual collisions are caught by the caller's allocation check.
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
    # Shrink the budget until the (expanded) candidate fits. The last iteration
    # leaves head/tail empty -> bare ``_<digest>_``, legal even unquoted.
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
    """Replace each quoted ``canonical`` identifier token with its ``emitted`` form.

    Two-phase (canonical -> sentinel -> emitted) so one substitution can't be
    re-read by a later one. Only QUOTED occurrences move; a bare occurrence is a
    different identifier (e.g. a table alias) and is left alone. String pass, not
    literal-aware, but keyed to an exact alias set — no wider exposure than the
    dot-mangling regexes already run over this SQL.
    """
    if not mapping:
        return sql
    items = sorted(mapping.items())
    for index, (canonical, _) in enumerate(items):
        sql = sql.replace(quote(canonical), _SENTINEL.format(index))
    for index, (_, emitted) in enumerate(items):
        sql = sql.replace(_SENTINEL.format(index), quote(emitted))
    return sql
