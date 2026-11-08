"""BigQuery / T-SQL dotted-alias mangling bijection (DEV-1571).

Owns the result-key <-> wire-identifier bijection used by ``BigqueryDialect``
(backtick-anchored regex) and ``TsqlDialect`` (bracket-anchored regex): both
need IDENTICAL encode/decode logic — BigQuery rejects dotted output-column
names; T-SQL's ORDER BY parser does not resolve bracketed dotted identifiers as
SELECT aliases. The fix is the same: mangle ``.`` to ``___`` on emit, decode on
result-row keys.

A pure-string leaf module (no dialect / naming dependencies) so ``dialects.base``
and ``slayer.sql.naming`` can both import it without a cycle. ``slayer.sql.naming``
re-exports these names for its existing consumers (DEV-1817).

The bijection's only domain constraint is that ``decode_alias`` inverts
``encode_alias`` ONLY on the latter's image. A key like ``my___metric`` (no dot
in the original) is OUTSIDE the image — decoding it would corrupt the value to
``my.metric``. This never bites because SLayer projection aliases are always
model-qualified (``<model>.<column>``), so they always contain a dot and always
pass through ``encode_alias``.
"""

from __future__ import annotations

_ALIAS_SEP = "___"


def encode_alias(alias: str) -> str:
    """Forward encode: escape any pre-existing ``___`` to ``______``, then
    map ``.`` to ``___``. Inverse is :func:`decode_alias`."""
    return alias.replace(_ALIAS_SEP, _ALIAS_SEP * 2).replace(".", _ALIAS_SEP)


def decode_alias(key: str) -> str:
    """Reverse of :func:`encode_alias`. Walks ``key`` left-to-right, consuming
    the escape-doubled ``______`` BEFORE the plain ``___`` so the two encodings
    stay unambiguous. Inverse of ``encode_alias`` only on its image (see the
    module docstring bijection note)."""
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
