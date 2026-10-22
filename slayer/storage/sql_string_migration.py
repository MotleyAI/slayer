"""SlayerModel v15 / SlayerQuery v6 / Memory v4: stored Mode-B text adopts the SQL string and identifier rules.

A double-quoted or backslash-escaped string literal becomes the equivalent single-quoted one and a ``|``
concatenation becomes ``||``. Stored-only: a fresh payload is never rewritten.
"""

import ast
import io
import tokenize

from slayer.storage.migrations import register_migration
from slayer.storage.rank_direction_migration import rewrite_model_texts, rewrite_query_texts, stamp_memory_query

_SKIP = frozenset({tokenize.NL, tokenize.NEWLINE, tokenize.COMMENT, tokenize.INDENT, tokenize.DEDENT,
                   tokenize.ENDMARKER})


def sql_quote_strings(text: str) -> str:
    """``text`` with Python string literals re-spelled as SQL ``'…'`` literals (adjacent ones joined) and a lone
    ``|`` as ``||``; everything else byte-for-byte. Untokenisable text is returned unchanged."""
    try:
        tokens = [t for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type not in _SKIP]
    except (tokenize.TokenError, SyntaxError):
        return text
    if any(t.type == tokenize.ERRORTOKEN for t in tokens):
        return text
    line_starts = [0]
    for line in text.splitlines(keepends=True):
        line_starts.append(line_starts[-1] + len(line))

    def offset(pos: tuple[int, int]) -> int:
        return line_starts[pos[0] - 1] + pos[1]

    edits: list[tuple[int, int, str]] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.type == tokenize.STRING:
            j = i
            value = ""
            while j < len(tokens) and tokens[j].type == tokenize.STRING:
                decoded = ast.literal_eval(tokens[j].string)
                if not isinstance(decoded, str):
                    return text
                value += decoded
                j += 1
            edits.append((offset(tok.start), offset(tokens[j - 1].end), "'" + value.replace("'", "''") + "'"))
            i = j
            continue
        if tok.type == tokenize.OP and tok.string == "|" and not _pipe_pair(tokens, i):
            edits.append((offset(tok.start), offset(tok.end), "||"))
        i += 1
    for start, end, replacement in sorted(edits, reverse=True):
        text = text[:start] + replacement + text[end:]
    return text


def _pipe_pair(tokens: list[tokenize.TokenInfo], i: int) -> bool:
    """Whether the ``|`` at ``i`` is half of an already-SQL ``||``."""
    def touches(a: tokenize.TokenInfo, b: tokenize.TokenInfo) -> bool:
        return a.end == b.start and a.string == b.string == "|"

    return (i > 0 and touches(tokens[i - 1], tokens[i])) or (i + 1 < len(tokens) and touches(tokens[i], tokens[i + 1]))


def rewrite_model_strings(data: dict) -> dict:
    return rewrite_model_texts(data, sql_quote_strings)


def rewrite_query_strings(data: dict) -> dict:
    return rewrite_query_texts(data, sql_quote_strings)


register_migration(entity="SlayerModel", source_version=15, stored_only=True)(rewrite_model_strings)
register_migration(entity="SlayerQuery", source_version=6, stored_only=True)(rewrite_query_strings)
register_migration(entity="Memory", source_version=4, stored_only=True)(stamp_memory_query)
