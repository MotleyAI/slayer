"""`slayer flight-serve` CLI subcommand (DEV-1390 §7.1).

Mounted from ``slayer/cli.py``'s argparse dispatch. Handles bind-address
defaults, the ``--demo`` interplay with loopback fallback, and TLS pair
validation before constructing the FlightSqlServer.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

logger = logging.getLogger(__name__)


def add_flight_serve_subparser(subparsers) -> None:
    """Register ``slayer flight-serve`` on the existing argparse subparsers."""
    p = subparsers.add_parser(
        "flight-serve",
        help="Start the Arrow Flight SQL server (dbt-SL JDBC compatible)",
        epilog="""\
examples:
  # Local dev — bind loopback, no auth needed
  slayer flight-serve --demo

  # Production-ish — bind all interfaces with a bearer token
  slayer flight-serve --host 0.0.0.0 --token "$(pass slayer-token)"

  # TLS-enabled
  slayer flight-serve --host 0.0.0.0 --token TOK \\
      --tls-cert /etc/ssl/slayer.crt --tls-key /etc/ssl/slayer.key
""",
    )
    p.add_argument(
        "--host",
        default=None,
        help=(
            "Bind address. Defaults to 0.0.0.0; if --demo is given AND "
            "--token is not, defaults to 127.0.0.1 for the no-token "
            "loopback fallback."
        ),
    )
    p.add_argument("--port", type=int, default=5144, help="Port (default: 5144)")
    p.add_argument(
        "--token",
        default=None,
        help=(
            "Bearer token for authentication. Falls back to "
            "$SLAYER_FLIGHT_TOKEN. Required when binding a non-loopback "
            "address."
        ),
    )
    p.add_argument("--tls-cert", default=None, help="Path to TLS certificate (PEM).")
    p.add_argument("--tls-key", default=None, help="Path to TLS private key (PEM).")
    p.add_argument(
        "--demo",
        action="store_true",
        help=(
            "Generate and ingest the bundled Jaffle Shop demo dataset "
            "before starting (idempotent)."
        ),
    )


def run_flight_serve(args, *, resolve_storage, prepare_demo) -> None:
    """Construct the storage, engine, handlers, server; block on serve().

    ``resolve_storage`` and ``prepare_demo`` are passed in by ``slayer/cli.py``
    so this module doesn't import the CLI's argparse-side helpers
    (which would close a circular dep).
    """
    try:
        import pyarrow.flight  # noqa: F401 — import-side check
    except ImportError as exc:
        print(
            "slayer flight-serve requires pyarrow with Flight support. "
            "Install via: pip install motley-slayer[flight]",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc

    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.flight.handlers import FlightHandlers
    from slayer.flight.server import build_server

    storage = resolve_storage(args)
    if getattr(args, "demo", False):
        prepare_demo(args, storage)

    engine = SlayerQueryEngine(storage=storage)
    handlers = FlightHandlers(engine=engine, storage=storage)

    token: Optional[str] = args.token or os.environ.get("SLAYER_FLIGHT_TOKEN")

    host = _resolve_host(host_arg=args.host, demo=args.demo, token=token)

    server = build_server(
        host=host,
        port=args.port,
        handlers=handlers,
        token=token,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
    )
    scheme = "grpc+tls" if args.tls_cert else "grpc"
    print(
        f"SLayer Flight SQL serving at {scheme}://{host}:{args.port}",
        flush=True,
    )
    server.serve()


def _resolve_host(*, host_arg: Optional[str], demo: bool, token: Optional[str]) -> str:
    """Apply the §7.1 demo-loopback default.

    If --host is not explicitly given AND --demo is set AND no token is
    configured, default to 127.0.0.1 so the no-token-on-loopback
    fallback applies cleanly. Otherwise default to 0.0.0.0.
    """
    if host_arg is not None:
        return host_arg
    if demo and not token:
        return "127.0.0.1"
    return "0.0.0.0"
