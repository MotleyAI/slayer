"""Postgres-facade connection probes (DEV-1486, DEV-1569).

Datasource-aware canned answers for the connect-time pings Postgres clients
and BI drivers issue: ``version()``, ``current_database()``,
``current_schema()``, ``SHOW <setting>``, and ``current_setting('<name>')`` /
``set_config('<name>', '<value>', …)`` GUC-style probes.

These differ from the Flight facade's generic probes (datasource-specific
``current_database()``, PostgreSQL-shaped ``version()``), so the Postgres
facade injects ``match_pg_probe`` as the translator's ``probe_matcher`` and
falls back to the shared ``match_probe`` for the truly generic ones
(``SELECT 1`` / ``SELECT NULL WHERE 1=0``).

DEV-1569: ``SHOW`` / ``current_setting`` consult a per-connection
``session_settings`` dict (passed in by the connection) so that
``SET application_name = 'foo'`` followed by ``SHOW application_name``
round-trips correctly. ``set_config(...)`` is purely read-only inside
this module — it returns the requested value via the matched ``RowBatch``
but never mutates the dict in place. The connection applies the mutation
on Execute (but not Describe) via ``match_pg_probe_with_mutation``, which
returns a ``ProbeMatcherOutcome`` carrying both the row batch and an
optional ``SetSettingOp`` hint.
"""

from __future__ import annotations

from typing import Dict, Optional

import sqlglot.expressions as exp

from slayer.core.enums import DataType
from slayer.facade.rows import FacadeColumn, RowBatch
from slayer.facade.translator import ProbeMatcherOutcome, SetSettingOp
from slayer.pg_facade.identity import PG_SERVER_VERSION

# Default per-connection session settings, seeded into every fresh
# ``PgConnection._session_settings``. Lowercase keys (Postgres GUC names
# are case-insensitive). Values align with what the startup
# ``ParameterStatus`` burst (identity.py) advertises for the same setting,
# so a client SHOW immediately after connect agrees with the burst.
SESSION_SETTING_SEED: Dict[str, str] = {
    "search_path": '"$user", public',
    "transaction_isolation": "read committed",
    "standard_conforming_strings": "on",
    "server_version": PG_SERVER_VERSION,
    "client_encoding": "UTF8",
    "server_encoding": "UTF8",
    "intervalstyle": "postgres",
    "datestyle": "ISO, MDY",
    "timezone": "UTC",
    "session_authorization": "slayer",
    "application_name": "",
    "jit": "off",
}

# Multi-word SHOW spellings → the canonical setting they report. pgjdbc's
# Connection.getTransactionIsolation() issues `SHOW TRANSACTION ISOLATION
# LEVEL` on every pooled connection (c3p0 caches it at pool-init).
SHOW_ALIASES = {
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
    session_settings: Optional[Dict[str, str]] = None,
) -> Optional[RowBatch]:
    """Return a datasource-aware canned ``RowBatch`` for a Postgres probe,
    else ``None`` (caller falls back to the shared probe matcher).

    DEV-1569: ``SHOW`` / ``current_setting`` consult ``session_settings``
    when provided (else fall back to the shared ``SESSION_SETTING_SEED``).
    ``set_config`` returns the requested value via the row batch but does
    NOT in-place mutate ``session_settings`` — keeping this function pure
    so the connection can call it during the Describe phase without
    side-effects. The connection applies the mutation on Execute via
    ``match_pg_probe_with_mutation``.
    """
    settings = session_settings if session_settings is not None else SESSION_SETTING_SEED
    # SHOW <setting> — `server_version` reports the bare "14.0" (matching
    # ParameterStatus / pg_settings), NOT the full version() string.
    setting = _show_setting_name(parsed)
    if setting is not None:
        key = setting.lower()
        key = SHOW_ALIASES.get(key, key)
        value = settings.get(key, "")
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
        return _single("current_setting", _setting_value(body, settings))
    if name == "set_config":
        return _single("set_config", _set_config_value(body))
    return None


def match_pg_probe_with_mutation(
    parsed: exp.Expression, *, datasource: str, version_str: str,
    session_settings: Optional[Dict[str, str]] = None,
) -> Optional[ProbeMatcherOutcome]:
    """Mutation-aware variant of :func:`match_pg_probe`. Returns a
    :class:`ProbeMatcherOutcome` carrying both the row batch and (for
    ``set_config(name, value, ...)`` matches) a :class:`SetSettingOp`
    hint the connection applies to its per-connection session-settings
    map on Execute.

    Important: this function does NOT in-place mutate ``session_settings``;
    the connection applies the mutation only after seeing the
    :class:`ProbeResult` in the Execute path (Describe-phase calls into
    the translator must remain pure — see DEV-1569 / Codex round 1
    F1+F2 in connection.py).
    """
    batch = match_pg_probe(
        parsed, datasource=datasource, version_str=version_str,
        session_settings=session_settings,
    )
    if batch is None:
        return None
    mutation = _extract_set_config_mutation(parsed)
    return ProbeMatcherOutcome(batch=batch, settings_mutation=mutation)


def _extract_set_config_mutation(parsed: exp.Expression) -> Optional[SetSettingOp]:
    """Inspect a parsed AST root for ``SELECT set_config('<name>', '<value>',
    <is_local>)`` and return a ``SetSettingOp`` carrying the (lowercased
    name, raw value) pair; return ``None`` otherwise.

    DEV-1569 / Codex F3: the value may arrive wrapped in an ``exp.Cast``
    (asyncpg / pgjdbc emit ``set_config('app', $1::text, false)`` and the
    bound substitution leaves ``'value'::text``); ``_first_literal``
    peers through one level of CAST so the mutation still surfaces.

    DEV-1569 / CodeRabbit thread: ``is_local=true`` is out of scope for
    DEV-1569 (we don't model transaction-bound restoration). When the
    third argument is explicitly ``true``, return ``None`` so the
    connection still emits the row but doesn't persist the value.
    """
    body = _single_projection(parsed)
    if body is None:
        return None
    if _anonymous_name(body) != "set_config":
        return None
    name_lit = _first_literal(body, 0)
    value_lit = _first_literal(body, 1)
    if name_lit is None or value_lit is None:
        return None
    if not _set_config_is_session_scope(body):
        return None
    return SetSettingOp(name=name_lit.lower(), value=value_lit)


def _set_config_is_session_scope(node: exp.Anonymous) -> bool:
    """Check ``set_config`` 's third argument (``is_local``).

    ``False`` (session scope) is permitted; explicit ``true`` (local scope)
    is blocked since DEV-1569 doesn't model transaction-bound restoration.
    A missing third argument or an unknown shape is treated as session
    scope (per real-Postgres default).

    DEV-1569 / Codex round 2 F1: extended-protocol substitution may
    surface the boolean wrapped in an ``exp.Cast`` (``FALSE::boolean`` /
    ``CAST(FALSE AS BOOLEAN)``); peer through one Cast level.
    """
    args = node.args.get("expressions") or []
    if len(args) < 3:
        return True
    is_local = args[2]
    if isinstance(is_local, exp.Cast):
        is_local = is_local.this
    if isinstance(is_local, exp.Boolean):
        return is_local.this is False
    if isinstance(is_local, exp.Literal):
        # Postgres accepts these as truthy boolean inputs (drivers rarely
        # use them for is_local, but match for consistency with PG parse
        # rules). Codex round 5 F1.
        return str(is_local.this).lower() not in ("true", "t", "on", "yes", "1")
    # Non-literal / non-boolean third arg: be conservative — skip mutation.
    return False


def _first_literal(node: exp.Anonymous, index: int) -> Optional[str]:
    """Return the string value of the ``index``-th argument of ``node`` if it
    is a string literal (or a CAST around a string literal). Returns
    ``None`` otherwise.

    DEV-1569 / Codex F3: drivers emit cast forms like ``'foo'::text`` and
    ``cast('foo' AS TEXT)`` for set_config arguments; sqlglot parses both
    as ``exp.Cast(this=Literal('foo'), to=DataType(TEXT))``. Peer through
    one level of CAST so the underlying literal is reachable.
    """
    args = node.args.get("expressions") or []
    if index >= len(args):
        return None
    arg = args[index]  # NOSONAR(S6466) — guarded by the index check on the line above
    if isinstance(arg, exp.Cast):
        arg = arg.this
    if isinstance(arg, exp.Literal):
        return str(arg.this)
    return None


def _setting_value(node: exp.Anonymous, settings: Dict[str, str]) -> str:
    """``current_setting('<name>')`` → ``settings[name]`` (lowercased lookup);
    unknown settings return the empty string."""
    setting = (_first_literal(node, 0) or "").lower()
    return settings.get(setting, "")


def _set_config_value(node: exp.Anonymous) -> str:
    """``set_config('jit', 'off', false)`` → the new value being set."""
    return _first_literal(node, 1) or ""
