"""Shared identifier-length fitting + the write-side substitution.

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
from collections.abc import Callable, Iterator, Mapping, Sequence

from pydantic import BaseModel, ConfigDict


HASH_LEN = 8  # digest hex chars; collisions are caught per-namespace, not by width
MIN_LIMIT = 16  # floor so a mis-configured limit fails loudly, not silently
_MARKER_LEN = HASH_LEN + 2  # ``_`` + digest + ``_``
_TRIM = "._"  # trimmed off head/tail so the marker never abuts a separator
#: Same-length filler for masked literal/comment content; never appears in SQL.
_MASK = "\x1f"
_WORD = re.compile(r"\w+")


class SqlLexis(BaseModel):
    """Dialect lexical rules the identifier masker needs. Kept sqlglot-free here;
    the dialect layer derives each flag from sqlglot's tokenizer and passes this in.
    Defaults are the strict standard-SQL subset (Postgres-family for comments)."""

    model_config = ConfigDict(frozen=True)

    backslash_escapes: bool = False  # ordinary ``'...'`` honour ``\`` escapes (MySQL, …)
    nested_comments: bool = False    # ``/* */`` nests (Postgres, T-SQL, DuckDB, …)
    dollar_quotes: bool = False      # ``$$``/``$tag$`` literals (Postgres, DuckDB, …)


_DEFAULT_LEXIS = SqlLexis()


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
        head = _head_bytes(name, n=head_n).rstrip(_TRIM)
        tail = _tail_bytes(name, n=tail_n).lstrip(_TRIM)
        candidate = f"{head}{marker}{tail}"
        if len(grow(candidate).encode("utf-8")) <= limit:
            return candidate
    raise ValueError(
        f"cannot fit {name!r} into {limit} bytes: the supplied `expand` grows "
        f"even the bare {_MARKER_LEN}-byte marker beyond the limit"
    )


#: Opening delimiter of a Postgres dollar-quote: ``$$`` or a tagged ``$name$``.
_DOLLAR_OPEN = re.compile(r"\$(?:[^\d\W]\w*)?\$")


def _is_ident_char(c: str) -> bool:
    """Identifier-continuation char (Postgres allows ``$`` mid-identifier)."""
    return c.isalnum() or c in "_$"


def _scan_string(sql: str, i: int, *, escapes: bool = False) -> tuple[str, int] | None:
    """Mask a single-quoted literal at ``i`` (interior blanked, quotes kept), or
    ``None`` if ``sql[i]`` is not a quote. ``''`` doubling always embeds; ``escapes``
    additionally honours backslash escapes (Postgres ``E'...'``; MySQL et al.'s
    ordinary strings — see :meth:`SqlDialect.backslash_escapes_strings`)."""
    if sql[i] != "'":
        return None
    out = ["'"]
    j, n = i + 1, len(sql)
    while j < n:
        ch = sql[j]
        if escapes and ch == "\\" and j + 1 < n:
            out.append(_MASK * 2)
            j += 2
            continue
        if ch == "'":
            if j + 1 < n and sql[j + 1] == "'":
                out.append(_MASK * 2)
                j += 2
                continue
            out.append("'")
            return "".join(out), j + 1
        out.append(_MASK)
        j += 1
    return "".join(out), j  # unterminated


def _scan_escape_string(sql: str, i: int) -> tuple[str, int] | None:
    """Mask a Postgres ``E'...'``/``e'...'`` escape-string literal (backslash
    escapes honoured), or ``None`` if no such literal opens at ``i``."""
    if sql[i] not in "Ee" or sql[i + 1 : i + 2] != "'":
        return None
    if i > 0 and _is_ident_char(sql[i - 1]):
        return None
    inner = _scan_string(sql, i + 1, escapes=True)
    if inner is None:  # unreachable: sql[i + 1] == "'"
        return None
    chunk, end = inner
    return sql[i] + chunk, end


def _scan_line_comment(sql: str, i: int) -> tuple[str, int] | None:
    """Mask a ``-- ...`` line comment (newline stays outside), or ``None``."""
    if sql[i : i + 2] != "--":
        return None
    j, n = i + 2, len(sql)
    while j < n and sql[j] != "\n":
        j += 1
    return "--" + _MASK * (j - i - 2), j


def _scan_block_comment(sql: str, i: int, *, nested: bool) -> tuple[str, int] | None:
    """Mask a ``/* ... */`` block comment, or ``None`` if none opens at ``i``.
    ``nested`` tracks ``/* */`` depth (Postgres/T-SQL/DuckDB nest); otherwise the
    first ``*/`` closes (MySQL/BigQuery/standard SQL)."""
    if sql[i : i + 2] != "/*":
        return None
    out = ["/*"]
    j, n, depth = i + 2, len(sql), 1
    while j < n and depth > 0:
        pair = sql[j : j + 2]
        if nested and pair == "/*":
            depth += 1
            out.append(_MASK * 2)
            j += 2
        elif pair == "*/":
            depth -= 1
            out.append("*/" if depth == 0 else _MASK * 2)
            j += 2
        else:
            out.append(_MASK)
            j += 1
    return "".join(out), j


def _scan_dollar_quote(sql: str, i: int) -> tuple[str, int] | None:
    """Mask a Postgres dollar-quoted literal (``$$...$$`` or tagged ``$t$...$t$``),
    or ``None``. A ``$`` right after an identifier char is not an opener."""
    if i > 0 and _is_ident_char(sql[i - 1]):
        return None
    m = _DOLLAR_OPEN.match(sql, i)
    if m is None:
        return None
    delim = m.group(0)
    body = m.end()
    close = sql.find(delim, body)
    if close == -1:  # unterminated
        return delim + _MASK * (len(sql) - body), len(sql)
    return delim + _MASK * (close - body) + delim, close + len(delim)


def _scan_span(sql: str, i: int, *, lexis: SqlLexis) -> tuple[str, int] | None:
    """The masked literal/comment span opening at ``i``, or ``None`` for plain SQL.
    Which forms are recognised is gated by ``lexis`` (dialect lexical rules)."""
    esc = _scan_escape_string(sql, i)
    if esc is not None:
        return esc
    if sql[i] == "'":
        return _scan_string(sql, i, escapes=lexis.backslash_escapes)
    lc = _scan_line_comment(sql, i)
    if lc is not None:
        return lc
    bc = _scan_block_comment(sql, i, nested=lexis.nested_comments)
    if bc is not None:
        return bc
    if lexis.dollar_quotes:
        return _scan_dollar_quote(sql, i)
    return None


def _mask_sql(sql: str, *, lexis: SqlLexis = _DEFAULT_LEXIS) -> str:
    """Same-length copy of ``sql`` with string-literal and comment CONTENT blanked.

    Delimiters (quotes, ``E'``, ``--``, ``/* */``, ``$$``/``$tag$``) are kept in
    place; every other code point becomes one :data:`_MASK` code point. Character
    length and every non-masked code-point offset are preserved (consumers index
    by code point, not bytes), so a match position in the mask is a valid position
    in ``sql``. ``lexis`` gates the dialect-specific forms (ordinary-string
    backslash escapes, comment nesting, dollar-quoting).
    """
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        span = _scan_span(sql, i, lexis=lexis)
        if span is None:
            out.append(sql[i])
            i += 1
        else:
            chunk, i = span
            out.append(chunk)
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
    sql: str, *, quote_open: str, quote_close: str, lexis: SqlLexis = _DEFAULT_LEXIS,
) -> set[str]:
    """Every distinct quoted identifier in ``sql`` (literals/comments masked out,
    escaped-quote spans skipped) — the set a fitted form must not collide with."""
    masked = _mask_sql(sql, lexis=lexis)
    return {
        sql[s:e]
        for s, e, escaped in _iter_quoted(masked, quote_open=quote_open, quote_close=quote_close)
        if not escaped
    }


def find_overlimit_quoted(
    sql: str, *, limit: int | None, quote_open: str, quote_close: str,
    lexis: SqlLexis = _DEFAULT_LEXIS,
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
            sql, quote_open=quote_open, quote_close=quote_close, lexis=lexis,
        )
        if len(name.encode("utf-8")) > limit
    )


_QUOTE_STYLES = (('"', '"'), ("`", "`"), ("[", "]"))


def overlimit_tokens(
    text: str, *, limit: int | None,
    quote_styles: Sequence[tuple[str, str]] = _QUOTE_STYLES,
    lexis: SqlLexis = _DEFAULT_LEXIS,
) -> list[str]:
    """Distinct over-limit identifier-shaped tokens in ``text`` — bare ``\\w+`` runs
    and whole quoted spans of each ``(open, close)`` in ``quote_styles``.

    Masks literals/comments first. Used to enumerate a bundle's user-authored
    surfaces (the exemption inventory) and, on final SQL, to backstop survivors.
    Callers pass only the dialect's own identifier quote style so, e.g., a Postgres
    ``ARRAY[…]`` literal isn't misread as one oversized ``[…]`` identifier. ``None``
    limit yields nothing (unbounded dialect)."""
    if limit is None:
        return []
    masked = _mask_sql(text, lexis=lexis)
    out = {
        m.group(0)
        for m in _WORD.finditer(masked)
        # A bare run starting with a digit is a numeric literal, not an unquoted
        # identifier — don't let a long number trip the backstop.
        if not m.group(0)[0].isdigit() and len(m.group(0).encode("utf-8")) > limit
    }
    for quote_open, quote_close in quote_styles:
        out.update(find_overlimit_quoted(
            text, limit=limit, quote_open=quote_open, quote_close=quote_close, lexis=lexis,
        ))
    return sorted(out)


def substitute_quoted(
    sql: str,
    mapping: Mapping[str, str],
    *,
    quote: Callable[[str], str],
    lexis: SqlLexis = _DEFAULT_LEXIS,
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
    masked = _mask_sql(sql, lexis=lexis)
    out: list[str] = []
    pos = 0
    for start, end, escaped in _iter_quoted(masked, quote_open=quote_open, quote_close=quote_close):
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
