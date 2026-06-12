"""BigQuery dialect — Tier 1.

BigQuery is the one dialect today with output-shape logic on top of the
scalar config every other Tier-2 dialect has. It rejects column names
containing ``.`` (output schema names must match ``[A-Za-z_][A-Za-z0-9_]*``),
while SLayer's universal alias convention is dotted
(``orders._count``, ``orders.products.category``). This dialect mangles
``.`` -> ``___`` inside backticked aliases on the write side and decodes
``___`` -> ``.`` on the read side so the mangling is invisible to consumers.

The ``___`` separator is chosen specifically because ``__`` is already
used by ``_query_as_model`` to flatten cross-model leaves (e.g.
``stores__name``); using a distinct sentinel keeps the two encodings
unambiguous.

Per DEV-1542's "every dialect quirk lives behind a hook on
``SqlDialect``" rule, this file is BigQuery's home. The plain
``rewrite_emitted_sql`` / ``decode_result_keys`` hooks on the base class
have identity defaults; only ``BigqueryDialect`` overrides them today.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from slayer.sql.dialects.base import SqlDialect


# ---------------------------------------------------------------------------
# Alias mangling internals
# ---------------------------------------------------------------------------


_ALIAS_SEP = "___"


# Backtick-quoted dotted alias. The pattern is constrained to identifier
# characters ``\w`` separated by dots so it can't accidentally span
# unrelated SQL between two unrelated backticks. ``re.ASCII`` keeps ``\w``
# ASCII-only so stray Unicode word-chars in surrounding SQL don't widen
# the match accidentally.
#
# Caveats (documented constraint):
#   - Table fully-qualified paths whose project name contains a hyphen
#     (e.g. ``\`bigquery-public-data\`.thelook_ecommerce.orders``) are
#     safe: the hyphen breaks ``\w``, so the regex doesn't match the
#     backticked-project segment, and the inner ``thelook_ecommerce.orders``
#     isn't inside any backticks.
#   - A fully backticked dotted path of word-only segments
#     (``\`my_dataset.my_table\``) WOULD false-positive mangle. Users
#     writing ``Column.sql`` for BigQuery must backtick segments
#     individually (``\`my_dataset\`.\`my_table\``) to avoid this. See
#     ``tests/dialects/test_bigquery.py::test_rewrite_emitted_sql_false_positive_on_single_backticked_dotted_path``
#     for the characterization pin.
_DOTTED_ALIAS_RE = re.compile(r"`(\w+(?:\.\w+)+)`", re.ASCII)


def _encode_alias(alias: str) -> str:
    """Bijective forward encode: escape any pre-existing ``___`` in the
    alias to ``______`` so user-named measures like ``my___metric`` round-
    trip intact, then map ``.`` -> ``___``.

    Inverse is the left-to-right walker in ``_decode_alias`` which
    consumes the longer ``______`` token BEFORE the shorter ``___``.
    """
    return alias.replace(_ALIAS_SEP, _ALIAS_SEP * 2).replace(".", _ALIAS_SEP)


def _decode_alias(key: str) -> str:
    """Bijective reverse decode for keys produced by ``_encode_alias``.

    The escape-doubled form ``______`` is consumed BEFORE the plain
    ``___`` so the two encodings stay unambiguous.

    Domain constraint: this is the inverse of ``_encode_alias`` ONLY on
    the latter's image (strings produced by encoding a dotted alias).
    Calling it on an arbitrary string containing ``___`` but no
    dot-derived ``___`` is undefined and may corrupt the key. This
    constraint never bites in practice because SLayer's projection
    aliases are always model-qualified (``<model>.<column>``), so they
    always contain at least one dot and always pass through encoding.
    See ``test_decode_corrupts_no_dot_key_with_triple_underscore`` for
    the characterization pin.
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


# ---------------------------------------------------------------------------
# BigqueryDialect — Tier 1 (has logic, not just scalar config)
# ---------------------------------------------------------------------------


class BigqueryDialect(SqlDialect):
    """BigQuery output-alias mangling + scalar config.

    Promoted out of ``_tier2.py`` because it has logic
    (``rewrite_emitted_sql`` / ``decode_result_keys`` overrides), not
    just scalar config. ``_tier2.py``'s "data-shaped, no SQL-shape logic"
    contract stays accurate for the remaining tier-2 dialects.
    """

    sqlglot_name: str = "bigquery"
    ds_type_aliases: frozenset[str] = frozenset({"bigquery"})
    # BigQuery has no SQL-level EXPLAIN.
    explain_prefix: Optional[str] = None
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def rewrite_emitted_sql(self, sql: str) -> str:
        """Replace ``.`` with ``___`` inside backtick-quoted identifiers.

        Applied as a post-pass on the BigQuery dialect's final SQL so
        emitted column aliases (``SELECT ... AS \\`orders._count\\``) and
        references to those aliases
        (``ORDER BY \\`orders._count\\``) comply with BigQuery's column-name
        grammar.
        """
        return _DOTTED_ALIAS_RE.sub(
            lambda m: f"`{_encode_alias(m.group(1))}`", sql
        )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Reverse the BigQuery alias mangling on result-row keys so
        consumers see SLayer's universal dotted alias shape regardless of
        whether the query ran against BigQuery or another dialect."""
        return [{_decode_alias(k): v for k, v in row.items()} for row in rows]
