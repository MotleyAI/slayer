"""asyncio TCP server for the Postgres facade (DEV-1486).

``serve`` binds an ``asyncio`` server and drives one :class:`PgConnection` per
accepted connection. ``run_pg_serve`` is the CLI entry point; it mirrors the
Flight facade's ``run_flight_serve`` (storage/engine construction, ``--demo``,
loopback-no-token fallback, TLS pair validation).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import ssl
import sys

from slayer.pg_facade.auth import Authenticator, validate_bind_address, validate_tls_pair
from slayer.pg_facade.connection import EngineFactory, PgConnection, StorageProvider

logger = logging.getLogger(__name__)

DEFAULT_PG_PORT = 5145


async def serve(
    *,
    host: str,
    port: int,
    engine=None,
    storage=None,
    token: str | None = None,
    authenticator: Authenticator | None = None,
    storage_provider: StorageProvider | None = None,
    engine_factory: EngineFactory | None = None,
    tls_ctx: ssl.SSLContext | None = None,
    catalog_extra_relations=None,
    catalog_ttl_seconds: float | None = None,
) -> None:
    """Bind and serve forever. Validates the bind/token combination first.

    Supply either a static ``engine`` + ``storage``, or a ``storage_provider``
    that resolves a per-connection (e.g. tenant-scoped) storage from the
    authenticated principal.

    ``catalog_extra_relations``: optional iterable of
    ``slayer.facade.catalog_sql.CatalogRelation`` that extends or overrides
    the default ``pg_catalog`` / ``information_schema`` tables. Embedders
    (e.g. Storyline) use this to project real per-tenant data into
    ``pg_roles`` / ``pg_database`` / add new tables. Override is by table
    name; new tables are appended.

    ``catalog_ttl_seconds``: catalog freshness for each connection. ``None``
    (default) keeps the historical behavior — the catalog is built once at
    connect and stays static for the connection's lifetime. A float enables
    TTL-gated, on-demand refresh: an idle connection re-checks the storage
    fingerprint at most once per window and rebuilds only when it changed, so
    long-lived BI sessions pick up model/schema edits without reconnecting.
    """
    # A custom authenticator that prompts for a password counts as auth, so the
    # non-loopback-requires-a-secret rule is satisfied even without a token.
    # When an authenticator is supplied, ``PgConnection`` ignores ``token``
    # entirely — so the bind guard must check the auth mechanism that will
    # ACTUALLY be enforced, not a stale token that would never be consulted.
    authenticated = authenticator is not None and authenticator.requires_password
    effective_token = None if authenticator is not None else token
    validate_bind_address(host=host, token=effective_token, authenticated=authenticated)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = PgConnection(
            reader, writer, engine=engine, storage=storage,
            token=token, authenticator=authenticator,
            storage_provider=storage_provider, engine_factory=engine_factory,
            tls_ctx=tls_ctx,
            catalog_extra_relations=catalog_extra_relations,
            catalog_ttl_seconds=catalog_ttl_seconds,
        )
        try:
            await conn.run()
        except Exception:  # noqa: BLE001 — never let one connection kill the server
            logger.exception("pg facade connection failed")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(handle, host=host, port=port)
    async with server:
        await server.serve_forever()


def _build_tls_context(cert: str | None, key: str | None) -> ssl.SSLContext | None:
    validate_tls_pair(cert=cert, key=key)
    if cert is None or key is None:
        return None
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=cert, keyfile=key)
    return ctx


def _resolve_token(token_arg: str | None) -> str | None:
    """``--token`` wins over the ``$SLAYER_PG_TOKEN`` env var."""
    return token_arg or os.environ.get("SLAYER_PG_TOKEN")


def _resolve_host(*, host_arg: str | None, demo: bool, token: str | None) -> str:
    """Explicit --host wins; --demo without a token defaults to loopback so the
    no-token fallback applies; otherwise bind all interfaces."""
    if host_arg is not None:
        return host_arg
    if demo and not token:
        return "127.0.0.1"
    return "0.0.0.0"  # noqa: S104 — intentional default; non-loopback requires a token


def run_pg_serve(args, *, resolve_storage, prepare_demo) -> None:
    """Construct storage/engine, then block on ``serve``.

    ``resolve_storage`` / ``prepare_demo`` are injected by ``slayer/cli.py`` to
    avoid importing the CLI's argparse helpers (circular dependency).
    """
    from slayer.engine.query_engine import SlayerQueryEngine

    storage = resolve_storage(args)
    if getattr(args, "demo", False):
        prepare_demo(args, storage)

    engine = SlayerQueryEngine(storage=storage)
    token: str | None = _resolve_token(args.token)
    host = _resolve_host(host_arg=args.host, demo=args.demo, token=token)

    try:
        tls_ctx = _build_tls_context(args.tls_cert, args.tls_key)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc

    scheme = "postgresql+tls" if tls_ctx is not None else "postgresql"
    print(f"SLayer Postgres facade serving at {scheme}://{host}:{args.port}", flush=True)
    try:
        asyncio.run(
            serve(
                host=host,
                port=args.port,
                engine=engine,
                storage=storage,
                token=token,
                tls_ctx=tls_ctx,
            )
        )
    except ValueError as exc:
        # validate_bind_address rejection (non-loopback without a token).
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
    except KeyboardInterrupt:
        pass
