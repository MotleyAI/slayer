"""ClickhouseDialect.

ClickHouse uses native ``median(x)`` directly and the parametric
``quantile(p)(x)`` form for percentile. CORR / COVAR_SAMP / COVAR_POP
are native. log10 and log2 are native.
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.dialects.base import SqlDialect

_TIMEOUT_SETTING = "max_execution_time"
_ABSENT = object()


class ClickhouseDialect(SqlDialect):
    sqlglot_name: str = "clickhouse"
    ds_type_aliases: frozenset[str] = frozenset({"clickhouse"})
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True

    def set_connection_timeout(self, dbapi_connection: Any, timeout_seconds: int) -> object:
        """Per-request HTTP setting: the driver keeps no session, so a ``SET`` never reaches the query."""
        settings = dbapi_connection.transport.ch_settings
        prior = settings.get(_TIMEOUT_SETTING, _ABSENT)
        settings[_TIMEOUT_SETTING] = timeout_seconds
        return prior

    def restore_connection_timeout(self, dbapi_connection: Any, prior: object) -> None:
        settings = dbapi_connection.transport.ch_settings
        if prior is _ABSENT:
            settings.pop(_TIMEOUT_SETTING, None)
        else:
            settings[_TIMEOUT_SETTING] = prior

    def timeout_permission_sql(self) -> str | None:
        return "SELECT getSetting('readonly')"

    def timeout_permitted(self, value: Any) -> bool:
        """``readonly = 1`` forbids every setting change; ``0`` and ``2`` allow it."""
        return int(value) != 1

    def build_median(self, inner: Expression) -> Expression:
        """ClickHouse: ``quantile(0.5)(x)``."""
        return self.build_percentile(p=exp.Literal.number("0.5"), col_expr=inner)

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        """ClickHouse: parametric ``quantile(p)(x)`` syntax."""
        return exp.Quantile(this=col_expr.copy(), quantile=p.copy())
