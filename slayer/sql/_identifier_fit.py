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
import re
from collections.abc import Callable, Iterator, Mapping


HASH_LEN = 8  # digest hex chars; collisions are caught per-namespace, not by width
MIN_LIMIT = 16  # floor so a mis-configured limit fails loudly, not silently
_MARKER_LEN = HASH_LEN + 2  # ``_`` + digest + ``_``
_TRIM = "._"  # trimmed off head/tail so the marker never abuts a separator
#: Same-length filler for masked literal/comment content; never appears in SQL.
_MASK = "\x1f"
_WORD = re.compile(r"\w+")


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


def _mask_sql(sql: str) -> str:
    """Same-length copy of ``sql`` with string-literal and comment CONTENT blanked.

    Delimiters (quotes, ``--``, ``/* */``, ``$$``) are kept in place; only the
    bytes between them become :data:`_MASK`. Length and every non-masked offset
    are preserved, so a match position in the mask is a valid position in ``sql``.
    Handles ``''`` doubling inside single-quoted literals.
    """
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        two = sql[i : i + 2]
        if c == "'":
            out.append(c)
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        out.append(_MASK * 2)
                        i += 2
                        continue
                    out.append("'")
                    i += 1
                    break
                out.append(_MASK)
                i += 1
            continue
        if two in ("--", "/*", "$$"):
            close = {"--": "\n", "/*": "*/", "$$": "$$"}[two]
            keep_close = two != "--"  # newline stays outside the comment
            out.append(two)
            i += 2
            while i < n and sql[i : i + len(close)] != close:
                out.append(_MASK)
                i += 1
            if keep_close and i < n:
                out.append(close)
                i += len(close)
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _iter_quoted(
    masked: str, quote_open: str, quote_close: str,
) -> Iterator[tuple[int, int, bool]]:
    """Yield ``(start, end, has_escaped_quote)`` interiors of each quoted span in
    ``masked`` (``quote_open``/``quote_close`` differ for ``[...]``). ``end`` is
    exclusive; a doubled close-quote is an embedded escape."""
    i, n = 0, len(masked)
    while i < n:
        if masked[i] != quote_open:
            i += 1
            continue
        j = i + 1
        start = j
        escaped = False
        while j < n:
            if masked[j] == quote_close:
                if j + 1 < n and masked[j + 1] == quote_close:
                    escaped = True
                    j += 2
                    continue
                break
            j += 1
        yield (start, j, escaped)
        i = j + 1


def quoted_identifiers(
    sql: str, *, quote_open: str, quote_close: str,
) -> set[str]:
    """Every distinct quoted identifier in ``sql`` (literals/comments masked out,
    escaped-quote spans skipped) — the set a fitted form must not collide with."""
    masked = _mask_sql(sql)
    return {
        sql[s:e]
        for s, e, escaped in _iter_quoted(masked, quote_open, quote_close)
        if not escaped
    }


def find_overlimit_quoted(
    sql: str, *, limit: int | None, quote_open: str, quote_close: str,
) -> list[str]:
    """Distinct quoted identifiers in ``sql`` whose content exceeds ``limit`` bytes.

    Literals/comments are masked first, so identifier-looking text inside them is
    invisible. A span carrying an escaped quote is user text (SLayer-minted names
    never contain a quote char) and is skipped. ``None`` limit yields nothing.
    """
    if limit is None:
        return []
    return sorted(
        name
        for name in quoted_identifiers(
            sql, quote_open=quote_open, quote_close=quote_close,
        )
        if len(name.encode("utf-8")) > limit
    )


_QUOTE_STYLES = (('"', '"'), ("`", "`"), ("[", "]"))


def overlimit_tokens(text: str, *, limit: int | None) -> list[str]:
    """Distinct over-limit identifier-shaped tokens in ``text`` — bare ``\\w+``
    runs and whole quoted spans (any of the three quote styles) alike.

    Masks literals/comments first. Used to enumerate a bundle's user-authored
    surfaces (the exemption inventory) and, on final SQL, to backstop survivors.
    ``None`` limit yields nothing (unbounded dialect)."""
    if limit is None:
        return []
    masked = _mask_sql(text)
    out = {
        m.group(0)
        for m in _WORD.finditer(masked)
        # A bare run starting with a digit is a numeric literal, not an unquoted
        # identifier — don't let a long number trip the backstop.
        if not m.group(0)[0].isdigit() and len(m.group(0).encode("utf-8")) > limit
    }
    for quote_open, quote_close in _QUOTE_STYLES:
        out.update(find_overlimit_quoted(
            text, limit=limit, quote_open=quote_open, quote_close=quote_close,
        ))
    return sorted(out)


def substitute_quoted(
    sql: str,
    mapping: Mapping[str, str],
    *,
    quote: Callable[[str], str],
) -> str:
    """Replace each quoted ``canonical`` identifier token with its ``emitted`` form.

    Walks the COMPLETE quoted spans of a literal/comment-masked copy (never a raw
    substring search), so a mapping only ever rewrites a whole quoted identifier —
    never text inside a literal/comment, never the suffix of an escaped-quote span
    (``"a""b"``), and never one substitution cascading into another (A→B and B→C
    land at their own tokens). A bare occurrence is a different identifier (e.g. a
    table alias) and is left alone.
    """
    if not mapping:
        return sql
    probe = quote("x")
    quote_open, quote_close = probe[0], probe[-1]
    masked = _mask_sql(sql)
    out: list[str] = []
    pos = 0
    for start, end, escaped in _iter_quoted(masked, quote_open, quote_close):
        if escaped:
            continue
        emitted = mapping.get(sql[start:end])
        if emitted is None:
            continue
        out.append(sql[pos : start - 1])  # up to the opening quote
        out.append(quote(emitted))
        pos = end + 1  # past the closing quote
    out.append(sql[pos:])
    return "".join(out)
