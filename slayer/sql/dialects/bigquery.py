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
have identity defaults; only ``BigqueryDialect`` (and ``TsqlDialect``,
DEV-1571) override them today. The shared encode/decode bijection lives
in :mod:`slayer.sql.dialects._alias_mangle` and is reused by both
dialects — only the regex anchor (backticks here, brackets in T-SQL)
differs.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional

from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects._alias_mangle import decode_alias, encode_alias
from slayer.sql.dialects.base import SqlDialect


# ---------------------------------------------------------------------------
# Alias mangling — backtick-anchored regex (BigQuery's identifier quote)
# ---------------------------------------------------------------------------


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

    def build_date_trunc(
        self,
        col_expr: exp.Expression,
        granularity: TimeGranularity,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """BigQuery override for WEEK_SUNDAY (DEV-1572).

        BigQuery's native ``DATE_TRUNC(x, WEEK)`` is already Sunday-based, so
        the base class's generic +1d/-1d shift (which reuses a Monday-based
        WEEK) would double-shift. Emit the native Sunday form
        ``DATE_TRUNC(col, WEEK(SUNDAY))`` instead.

        Built as an ``exp.Anonymous`` because sqlglot (30.4.x) drops the
        ``(SUNDAY)`` weekday modifier when re-emitting an ``exp.DateTrunc`` —
        the anonymous call renders verbatim on the single final emission.
        Non-column/non-cast operands are wrapped in ``CAST(... AS TIMESTAMP)``
        to mirror the base class's operand handling. Every other granularity
        delegates to the base implementation.
        """
        if granularity != TimeGranularity.WEEK_SUNDAY:
            return super().build_date_trunc(
                col_expr=col_expr, granularity=granularity, parse=parse,
            )
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        week_sunday = exp.Anonymous(this="WEEK", expressions=[exp.var("SUNDAY")])
        return exp.Anonymous(
            this="DATE_TRUNC", expressions=[col_expr, week_sunday],
        )

    def rewrite_emitted_sql(self, sql: str) -> str:
        """Replace ``.`` with ``___`` inside backtick-quoted identifiers.

        Applied as a post-pass on the BigQuery dialect's final SQL so
        emitted column aliases (``SELECT ... AS \\`orders._count\\``) and
        references to those aliases
        (``ORDER BY \\`orders._count\\``) comply with BigQuery's column-name
        grammar.
        """
        return _DOTTED_ALIAS_RE.sub(
            lambda m: f"`{encode_alias(m.group(1))}`", sql
        )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Reverse the BigQuery alias mangling on result-row keys so
        consumers see SLayer's universal dotted alias shape regardless of
        whether the query ran against BigQuery or another dialect."""
        return [{decode_alias(k): v for k, v in row.items()} for row in rows]
