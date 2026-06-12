"""Postgres-facade connection probes (DEV-1486).

Datasource-aware canned answers for the connect-time pings Postgres clients
and BI drivers issue: ``version()``, ``current_database()``,
``current_schema()``, ``SHOW <setting>``, and the ``current_setting('jit')`` /
``set_config('jit', …)`` JIT probes asyncpg runs on server_version ≥ 11.

These differ from the Flight facade's generic probes (datasource-specific
``current_database()``, PostgreSQL-shaped ``version()``), so the Postgres
facade injects ``match_pg_probe`` as the translator's ``probe_matcher`` and
falls back to the shared ``match_probe`` for the truly generic ones
(``SELECT 1`` / ``SELECT NULL WHERE 1=0``).
"""

from __future__ import annotations

from typing import Optional

import sqlglot.expressions as exp

from slayer.core.enums import DataType
from slayer.facade.rows import FacadeColumn, RowBatch
from slayer.pg_facade.identity import PG_SERVER_VERSION

# Canned values for common SHOW settings. Unknown settings return "".
_SHOW_DEFAULTS = {
    "search_path": '"$user", public',
    "transaction_isolation": "read committed",
    "standard_conforming_strings": "on",
    "server_version": PG_SERVER_VERSION,
    "client_encoding": "UTF8",
    "datestyle": "ISO, MDY",
    "timezone": "UTC",
    "session_authorization": "slayer",
}

# Multi-word SHOW spellings → the canonical setting they report. pgjdbc's
# Connection.getTransactionIsolation() issues `SHOW TRANSACTION ISOLATION
# LEVEL` on every pooled connection (c3p0 caches it at pool-init).
_SHOW_ALIASES = {
    "transaction isolation level": "transaction_isolation",
    "time zone": "timezone",
    "session authorization": "session_authorization",
}


def _single(name: str, value: Optional[str], dtype: DataType = DataType.TEXT) -> RowBatch:
    return RowBatch(
        columns=[FacadeColumn(name=name, type=dtype)],
        rows=[{name: value}],
    )


def _single_projection(parsed: exp.Expression) -> Optional[exp.Expression]:
    if not isinstance(parsed, exp.Select):
        return None
    exprs = parsed.args.get("expressions") or []
    if len(exprs) != 1:
        return None
    body = exprs[0]
    if isinstance(body, exp.Alias):
        body = body.this
    return body


def _show_setting_name(parsed: exp.Expression) -> Optional[str]:
    if not isinstance(parsed, exp.Command):
        return None
    if str(parsed.this).upper() != "SHOW":
        return None
    expr = parsed.expression
    if expr is None:
        return None
    name = str(expr.this) if hasattr(expr, "this") else str(expr)
    return name.strip().strip("'\"")


def _anonymous_name(node: exp.Expression) -> Optional[str]:
    if isinstance(node, exp.Anonymous):
        return str(node.this).lower()
    return None


def match_pg_probe(
    parsed: exp.Expression, *, datasource: str, version_str: str,
) -> Optional[RowBatch]:
    """Return a datasource-aware canned ``RowBatch`` for a Postgres probe,
    else ``None`` (caller falls back to the shared probe matcher)."""
    # SHOW <setting> — `server_version` reports the bare "14.0" (matching
    # ParameterStatus / pg_settings), NOT the full version() string.
    setting = _show_setting_name(parsed)
    if setting is not None:
        key = setting.lower()
        key = _SHOW_ALIASES.get(key, key)
        value = _SHOW_DEFAULTS.get(key, "")
        return _single(key, value)

    body = _single_projection(parsed)
    if body is None:
        return None

    if isinstance(body, exp.CurrentVersion) or _anonymous_name(body) == "version":
        return _single("version", version_str)
    if isinstance(body, exp.CurrentDatabase) or _anonymous_name(body) == "current_database":
        return _single("current_database", datasource)
    # pgjdbc's PgConnection.getCatalog() issues the niladic `SELECT
    # current_catalog`; Metabase's c3p0 pool calls it on every new connection.
    if isinstance(body, exp.CurrentCatalog):
        return _single("current_catalog", datasource)
    if isinstance(body, exp.CurrentSchema) or _anonymous_name(body) == "current_schema":
        return _single("current_schema", "public")
    # The facade does not track per-connection login identity; a constant
    # satisfies driver probes (the username is ignored at auth anyway).
    if isinstance(body, exp.SessionUser):
        return _single("session_user", "slayer")
    if isinstance(body, exp.CurrentUser):
        return _single("current_user", "slayer")

    name = _anonymous_name(body)
    if name == "current_setting":
        return _single("current_setting", _setting_arg_value(body))
    if name == "set_config":
        return _single("set_config", _set_config_value(body))
    return None


def _first_literal(node: exp.Anonymous, index: int) -> Optional[str]:
    args = node.args.get("expressions") or []
    if index < len(args) and isinstance(args[index], exp.Literal):  # NOSONAR(S6466) — guarded by index < len(args)
        return str(args[index].this)
    return None


def _setting_arg_value(node: exp.Anonymous) -> str:
    """``current_setting('jit')`` → ``'off'``; otherwise empty string."""
    setting = (_first_literal(node, 0) or "").lower()
    if setting == "jit":
        return "off"
    return ""


def _set_config_value(node: exp.Anonymous) -> str:
    """``set_config('jit', 'off', false)`` → the new value being set."""
    return _first_literal(node, 1) or ""
