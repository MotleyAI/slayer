"""DEV-1542: PostgresDialect.

Postgres is the Postgres-shaped default made explicit. Native DATE_TRUNC,
PERCENTILE_CONT, CORR, COVAR_SAMP, COVAR_POP, native log10/log2.
"""

from __future__ import annotations

from typing import Optional

from slayer.sql.dialects.base import SqlDialect


class PostgresDialect(SqlDialect):
    sqlglot_name: str = "postgres"
    ds_type_aliases: frozenset[str] = frozenset({"postgres", "postgresql"})
    explain_prefix: Optional[str] = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
