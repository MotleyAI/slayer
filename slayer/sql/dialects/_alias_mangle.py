"""DEV-1571: shared dotted-alias encoder/decoder.

Used by ``BigqueryDialect`` (backtick-anchored regex) and ``TsqlDialect``
(bracket-anchored regex). The two dialects need IDENTICAL bijective
encode/decode logic: BigQuery rejects dotted output-column names; T-SQL's
``ORDER BY`` parser does not resolve bracketed dotted identifiers as SELECT
aliases. The fix is the same — mangle ``.`` to ``___`` on emit, decode on
result-row keys.

The bijection's only domain constraint is that ``decode_alias`` is the
inverse of ``encode_alias`` ONLY on the latter's image. A key like
``my___metric`` (no dot in the original) is OUTSIDE the image — calling
``decode_alias`` on it would corrupt the value to ``my.metric``. This
constraint never bites in practice because SLayer's projection aliases
are always model-qualified (``<model>.<column>``), so they always contain
at least one dot and always pass through ``encode_alias``.
"""

from __future__ import annotations


_ALIAS_SEP = "___"


def encode_alias(alias: str) -> str:
    """Forward encode: escape any pre-existing ``___`` to ``______``, then
    map ``.`` to ``___``.

    Inverse is the left-to-right walker in :func:`decode_alias` which
    consumes the longer ``______`` token BEFORE the shorter ``___``.
    """
    return alias.replace(_ALIAS_SEP, _ALIAS_SEP * 2).replace(".", _ALIAS_SEP)


def decode_alias(key: str) -> str:
    """Reverse decode of :func:`encode_alias`.

    Walks ``key`` left-to-right, consuming the escape-doubled ``______``
    BEFORE the plain ``___`` so the two encodings stay unambiguous.

    Domain constraint: inverse of ``encode_alias`` only on its image.
    See module docstring.
    """
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
