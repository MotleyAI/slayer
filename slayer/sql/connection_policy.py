"""Connection policy: the engine arguments and pool listeners that make a warm statement cost one request."""

from __future__ import annotations

from time import monotonic
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
import sqlalchemy.event as sa_event
from sqlalchemy.dialects import plugins
from sqlalchemy.engine import CreateEnginePlugin
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import ArgumentError, DisconnectionError
from sqlalchemy.pool import StaticPool

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig
    from slayer.sql.dialects.base import SqlDialect

# A pooled connection idle longer than this is pinged before reuse.
IDLE_PING_SECONDS = 60.0

# Pool-record ``info`` keys; SQLAlchemy clears ``info`` whenever the record reconnects.
CHECKED_IN_AT = "slayer_checked_in_at"
TIMEOUT_IN_FORCE = "slayer_statement_timeout"

# The ``create_engine`` plugin installing ``_install`` on every engine built with ``engine_kwargs``.
_PLUGIN = "slayer_connection_policy"
_DIALECT_KWARG = "slayer_dialect"

_POOLER_TYPES = frozenset({"postgres", "postgresql"})
_POOLER_BACKEND = "postgresql"


def _pooler_refusal(named: str) -> ValueError:
    return ValueError(
        f"transaction_pooler applies only to postgres datasources; this one is {named}."
    )


def check_transaction_pooler(datasource: DatasourceConfig) -> None:
    """Refuse ``transaction_pooler`` on a datasource whose type is not Postgres."""
    if datasource.transaction_pooler and datasource.type not in _POOLER_TYPES:
        raise _pooler_refusal("of no type" if datasource.type is None else f"of type {datasource.type!r}")


def check_transaction_pooler_backend(datasource: DatasourceConfig, *, url: str | URL) -> None:
    """Refuse ``transaction_pooler`` when ``url``'s driver is not a PostgreSQL one."""
    if not datasource.transaction_pooler:
        return
    check_transaction_pooler(datasource)
    backend = make_url(url).get_backend_name()
    if backend != _POOLER_BACKEND:
        raise _pooler_refusal(f"reached through a {backend!r} driver")


def _detects_autocommit(url: str | URL, *, is_async: bool) -> bool:
    """Whether ``url``'s SQLAlchemy dialect reads a connection's autocommit state."""
    try:
        sa_dialect = make_url(url).get_dialect(_is_async=is_async)
    except ArgumentError:  # NoSuchModuleError included
        return False
    return sa_dialect.detect_autocommit_setting is not DefaultDialect.detect_autocommit_setting


def engine_kwargs(
    *, url: str | URL, dialect: SqlDialect, datasource: DatasourceConfig, is_async: bool = False, **own: Any,
) -> dict[str, Any]:
    """``own`` engine arguments plus the dialect's transaction mode for ``datasource`` and this policy's listeners."""
    kwargs = {**own, "plugins": [*own.get("plugins", []), _PLUGIN], _DIALECT_KWARG: dialect}
    if datasource.transaction_pooler:
        return kwargs
    if dialect.transaction_mode == "driver_autocommit" and _detects_autocommit(url, is_async=is_async):
        kwargs.update(isolation_level="AUTOCOMMIT", skip_autocommit_rollback=True)
    elif dialect.transaction_mode == "dialect_autocommit":
        kwargs["isolation_level"] = "AUTOCOMMIT"
    return kwargs


def _skip_in_autocommit(dialect: SqlDialect, end: Any) -> Any:
    def _end(dbapi_connection: Any) -> None:
        if not dialect.autocommit_in_force(dbapi_connection):
            end(dbapi_connection)
    return _end


def _install(engine: sa.Engine, *, dialect: SqlDialect) -> None:
    """Install the after-idle ping, and the dialect's own autocommit transaction ends."""
    if dialect.transaction_mode == "dialect_autocommit":
        sa_dialect: Any = engine.dialect
        sa_dialect.do_rollback = _skip_in_autocommit(dialect, sa_dialect.do_rollback)
        sa_dialect.do_commit = _skip_in_autocommit(dialect, sa_dialect.do_commit)
    if isinstance(engine.pool, StaticPool):
        return

    @sa_event.listens_for(engine, "checkin")
    def _slayer_stamp_checkin(dbapi_connection: Any, record: Any) -> None:
        if dbapi_connection is not None:
            record.info[CHECKED_IN_AT] = monotonic()

    @sa_event.listens_for(engine, "checkout")
    def _slayer_ping_after_idle(dbapi_connection: Any, record: Any, _proxy: Any) -> None:
        checked_in = record.info.get(CHECKED_IN_AT)
        if checked_in is None or monotonic() - checked_in <= IDLE_PING_SECONDS:
            return
        try:
            alive = engine.dialect.do_ping(dbapi_connection)
        except Exception as exc:
            raise DisconnectionError("idle connection failed its liveness check") from exc
        if not alive:
            raise DisconnectionError("idle connection failed its liveness check")


class _PolicyPlugin(CreateEnginePlugin):
    """Applies ``_install`` to the engine ``create_engine`` builds from ``engine_kwargs``."""

    def __init__(self, url: URL, kwargs: dict[str, Any]) -> None:
        super().__init__(url, kwargs)
        self._dialect: SqlDialect = kwargs.pop(_DIALECT_KWARG)

    def update_url(self, url: URL) -> URL:
        return url

    def engine_created(self, engine: sa.Engine) -> None:
        _install(engine, dialect=self._dialect)


plugins.register(_PLUGIN, __name__, "_PolicyPlugin")
