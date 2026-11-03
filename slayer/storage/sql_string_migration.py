"""SlayerModel v15 / SlayerQuery v6 / Memory v4: stored Mode-B text adopts the SQL string and identifier rules.

A double-quoted or backslash-escaped string literal becomes the equivalent single-quoted one, a ``|``
concatenation becomes ``||``, touching minus signs are spaced (``a--b`` → ``a- -b``) and a ``#`` comment becomes ``--``.
Stored-only: a fresh payload is never rewritten, nor is text the old grammar could not have held.
"""

import ast
import io
import logging
import re
import tokenize
from collections.abc import Callable

from slayer.storage.migrations import document_label, register_migration
from slayer.storage.rank_direction_migration import rewrite_model_texts_at, rewrite_query_texts_at, stamp_memory_query

logger = logging.getLogger(__name__)

_SKIP = frozenset({tokenize.NL, tokenize.NEWLINE, tokenize.COMMENT, tokenize.INDENT, tokenize.DEDENT,
                   tokenize.ENDMARKER})
# The old parser's pre-pass: ``x:agg`` / ``*:count`` refs and ``*`` sources become plain names.
_COLON_REF = re.compile(r"(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*(?:\.\*)?|\*)\s*:\s*[A-Za-z_]\w*")


def _star_sources_named(text: str) -> str:
    """``text`` with each ``*`` standing as an argument or path tail (``count(*)``, ``t.*``) as ``_``."""
    return "".join(
        "_" if ch == "*" and text[:i].rstrip()[-1:] in ("(", ".", ",") and text[i + 1:].lstrip()[:1] in (")", ",")
        else ch
        for i, ch in enumerate(text)
    )


def sql_quote_strings(text: str) -> str:
    """``text`` with Python string literals re-spelled as SQL ``'…'`` literals (adjacent ones joined), a lone
    ``|`` as ``||`` and ``#`` comments as ``--``; everything else byte-for-byte. Untokenisable text is returned unchanged."""
    try:
        every = list(tokenize.generate_tokens(io.StringIO(text).readline))
    except (tokenize.TokenError, SyntaxError):
        return text
    tokens = [t for t in every if t.type not in _SKIP]
    if any(t.type == tokenize.ERRORTOKEN for t in tokens):
        return text
    line_starts = [0]
    for line in text.splitlines(keepends=True):
        line_starts.append(line_starts[-1] + len(line))
    edits = _edits(tokens, line_starts=line_starts)
    if edits is None:
        return text
    for comment in (t for t in every if t.type == tokenize.COMMENT):
        start = line_starts[comment.start[0] - 1] + comment.start[1]
        edits.append((start, start + 1, " --" if text[start - 1:start] == "-" else "--"))
    for start, end, replacement in sorted(edits, reverse=True):
        text = text[:start] + replacement + text[end:]
    return text


def is_old_grammar(text: str) -> bool:
    """Whether the pre-SQL grammar could have held ``text``."""
    try:
        ast.parse(_star_sources_named(_COLON_REF.sub("_", text)).strip(), mode="eval")
    except SyntaxError:
        return False
    return True


def _edits(tokens: list[tokenize.TokenInfo], *, line_starts: list[int]) -> list[tuple[int, int, str]] | None:
    """The ``(start, end, replacement)`` rewrites; ``None`` when a bytes literal makes the text non-Mode-B."""
    def offset(pos: tuple[int, int]) -> int:
        return line_starts[pos[0] - 1] + pos[1]

    edits: list[tuple[int, int, str]] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.type == tokenize.STRING:
            j, value = _string_run(tokens, i)
            if value is None:
                return None
            edits.append((offset(tok.start), offset(tokens[j - 1].end), "'" + value.replace("'", "''") + "'"))
            i = j
            continue
        if tok.type == tokenize.OP and tok.string == "|" and not _pipe_pair(tokens, i):
            edits.append((offset(tok.start), offset(tok.end), "||"))
        if _minus_pair(tokens, i):
            edits.append((offset(tok.end), offset(tok.end), " "))
        i += 1
    return edits


def _minus_pair(tokens: list[tokenize.TokenInfo], i: int) -> bool:
    """Whether the ``-`` at ``i`` touches a following ``-`` (SQL would read the pair as a comment)."""
    return (i + 1 < len(tokens) and tokens[i].type == tokens[i + 1].type == tokenize.OP
            and tokens[i].string == tokens[i + 1].string == "-" and tokens[i].end == tokens[i + 1].start)


def _string_run(tokens: list[tokenize.TokenInfo], i: int) -> tuple[int, str | None]:
    """End index of the adjacent string literals from ``i`` and their joined value (``None`` if one is bytes)."""
    j, value = i, ""
    while j < len(tokens) and tokens[j].type == tokenize.STRING:
        decoded = ast.literal_eval(tokens[j].string)
        if not isinstance(decoded, str):
            return j, None
        value += decoded
        j += 1
    return j, value


def _pipe_pair(tokens: list[tokenize.TokenInfo], i: int) -> bool:
    """Whether the ``|`` at ``i`` is half of an already-SQL ``||``."""
    def touches(a: tokenize.TokenInfo, b: tokenize.TokenInfo) -> bool:
        return a.end == b.start and a.string == b.string == "|"

    return (i > 0 and touches(tokens[i - 1], tokens[i])) or (i + 1 < len(tokens) and touches(tokens[i], tokens[i + 1]))


def _gated(document: str | None) -> Callable[[str, str], str]:
    def rewrite(text: str, field: str) -> str:
        if is_old_grammar(text):
            return sql_quote_strings(text)
        # The text itself is not logged: it may hold sensitive literals.
        logger.warning(
            "stored document %r, field %s: not in the grammar its version claims; left unchanged", document, field,
        )
        return text
    return rewrite


def rewrite_model_strings(data: dict) -> dict:
    return rewrite_model_texts_at(data, _gated(document_label(data)))


def rewrite_query_strings(data: dict) -> dict:
    return rewrite_query_texts_at(data, _gated(document_label(data)))


register_migration(entity="SlayerModel", source_version=15, stored_only=True)(rewrite_model_strings)
register_migration(entity="SlayerQuery", source_version=6, stored_only=True)(rewrite_query_strings)
register_migration(entity="Memory", source_version=4, stored_only=True)(stamp_memory_query)
