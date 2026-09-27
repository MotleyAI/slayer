"""ClickhouseDialect.

ClickHouse uses native ``median(x)`` directly and the parametric
``quantile(p)(x)`` form for percentile. CORR / COVAR_SAMP / COVAR_POP
are native. log10 and log2 are native.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.dialects.base import ServerProfile, SqlDialect

_CH_CORRELATED_SETTING = "allow_experimental_correlated_subqueries"
_CORRELATED_MIN_VERSION = (25, 4)
_CORRELATED_DEFAULT_ON_VERSION = (25, 8)
_TIMEOUT_SETTING = "max_execution_time"
_ABSENT = object()
_BOOL_SETTING_VALUES = {"1": True, "true": True, "0": False, "false": False}


def _parse_clickhouse_version(raw: Any) -> tuple[int, int] | None:
    """Parse a ClickHouse ``version()`` string to ``(major, minor)``; ``None`` if unparseable."""
    if not isinstance(raw, str):
        return None
    match = re.match(r"\s*v?(\d+)\.(\d+)", raw)
    if not match:
        return None
    return (int(match.group(1)), int(match.group(2)))


def _parse_readonly(raw: Any) -> int | None:
    if isinstance(raw, int) and not isinstance(raw, bool):
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw)
    return None


def _parse_bool_setting(raw: Any) -> bool | None:
    """``getSetting`` of a Bool setting: the driver may yield a bool, an int, or its text."""
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return {0: False, 1: True}.get(raw)
    if isinstance(raw, str):
        return _BOOL_SETTING_VALUES.get(raw.strip().lower())
    return None


def _attach_ch_correlated_setting(ast: Expression) -> None:
    """Force ``allow_experimental_correlated_subqueries = 1``, preserving other settings.

    Any prior entry for this setting is dropped so a caller-supplied ``= 0`` can't leave
    the correlated subquery emitted with the setting disabled.
    """
    holder = _settings_holder(ast)
    kept = [
        s
        for s in (holder.args.get("settings") or [])
        if getattr(s.this, "name", None) != _CH_CORRELATED_SETTING
    ]
    holder.set("settings", [*kept, exp.var(_CH_CORRELATED_SETTING).eq(1)])


def _settings_holder(ast: Expression) -> Expression:
    """The node carrying (or that should carry) this statement's ClickHouse ``SETTINGS``.

    sqlglot parks a trailing ``SETTINGS`` on the last branch of an unparenthesised
    ``UNION`` rather than the root, so honour that placement to avoid emitting two
    clauses. Only the set-operation's own right spine is followed — a nested ``FROM``
    subquery's ``SETTINGS`` is local to that rowset.
    """
    if ast.args.get("settings"):
        return ast
    if isinstance(ast, exp.SetOperation):
        node: Expression = ast
        while isinstance(node, exp.SetOperation):
            node = node.expression  # right branch owns the trailing SETTINGS
        if node.args.get("settings"):
            return node
    return ast


class ClickhouseDialect(SqlDialect):
    sqlglot_name: str = "clickhouse"
    ds_type_aliases: frozenset[str] = frozenset({"clickhouse"})
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True
    correlated_subqueries_gated: bool = True
    global_in_subqueries: bool = True

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

    def server_profile_sql(self) -> str | None:
        return "SELECT version(), getSetting('readonly')"

    def correlated_setting_sql(self, profile: ServerProfile) -> str | None:
        """``getSetting`` of a setting the server lacks errors, so ask only where it exists."""
        if profile.version is None or profile.version < _CORRELATED_MIN_VERSION:
            return None
        return f"SELECT getSetting('{_CH_CORRELATED_SETTING}')"

    def parse_server_profile(
        self, *, base_row: Sequence[Any] | None, correlated_row: Sequence[Any] | None = None,
    ) -> ServerProfile:
        base = tuple(base_row or ())
        return ServerProfile(
            version=_parse_clickhouse_version(base[0]) if len(base) > 0 else None,
            readonly=_parse_readonly(base[1]) if len(base) > 1 else None,
            correlated_subqueries=_parse_bool_setting(correlated_row[0]) if correlated_row else None,
        )

    def timeout_permitted(self, profile: ServerProfile) -> bool:
        """``readonly = 1`` forbids every setting change; ``0`` and ``2`` allow it."""
        return profile.readonly != 1

    def correlated_subquery_refusal(self, profile: ServerProfile) -> str | None:
        version = profile.version
        if version is None or version < _CORRELATED_MIN_VERSION:
            detected = (
                "the server version could not be determined." if version is None
                else f"detected {version[0]}.{version[1]}. Upgrade the server."
            )
            return f"ClickHouse supports correlated subqueries only from server 25.4, but {detected}"
        if profile.correlated_subqueries or profile.readonly not in (None, 1):
            return None
        setting_state = (
            "could not be read" if profile.correlated_subqueries is None else "is 0"
        )
        level = (
            "the user's read-only level could not be determined" if profile.readonly is None
            else "the user is readonly = 1, which may not change it"
        )
        remedies = [
            f"enable {_CH_CORRELATED_SETTING} in the user's settings profile",
            "use readonly = 2",
        ]
        if version < _CORRELATED_DEFAULT_ON_VERSION:
            remedies.append("upgrade the server to 25.8 or later, where it is on by default")
        return (
            f"The user's {_CH_CORRELATED_SETTING} setting {setting_state}, and {level}. "
            f"Remedies: {'; or '.join(remedies)}."
        )

    def attach_correlated_setting(self, statement: Expression) -> None:
        _attach_ch_correlated_setting(statement)

    def in_subquery_key(self, key: Expression) -> Expression:
        """``toNullable(key)``: pre-25 ``nullIn`` errors probing a non-Nullable set with a Nullable key."""
        return exp.Anonymous(this="toNullable", expressions=[key.copy()])

    def build_median(self, inner: Expression) -> Expression:
        """ClickHouse: ``quantile(0.5)(x)``."""
        return self.build_percentile(p=exp.Literal.number("0.5"), col_expr=inner)

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        """ClickHouse: parametric ``quantile(p)(x)`` syntax."""
        return exp.Quantile(this=col_expr.copy(), quantile=p.copy())
