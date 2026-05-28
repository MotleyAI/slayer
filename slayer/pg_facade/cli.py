"""`slayer pg-serve` CLI subcommand (DEV-1486).

Mounted from ``slayer/cli.py``'s argparse dispatch. Mirrors ``flight-serve``:
bind-address defaults, the ``--demo`` loopback fallback, TLS pair validation.
The Postgres facade is pure-stdlib, so there is no optional-dependency guard.
"""

from __future__ import annotations

import logging

from slayer.pg_facade.server import DEFAULT_PG_PORT, run_pg_serve

logger = logging.getLogger(__name__)

__all__ = ["add_pg_serve_subparser", "run_pg_serve"]


def add_pg_serve_subparser(subparsers) -> None:
    """Register ``slayer pg-serve`` on the existing argparse subparsers."""
    p = subparsers.add_parser(
        "pg-serve",
        help="Start the Postgres wire-protocol server (BI-tool compatible)",
        epilog="""\
examples:
  # Local dev — bind loopback, no auth needed
  slayer pg-serve --demo

  # Production-ish — bind all interfaces with a password token
  slayer pg-serve --host 0.0.0.0 --token "$(pass slayer-token)"

  # TLS-enabled
  slayer pg-serve --host 0.0.0.0 --token TOK \\
      --tls-cert /etc/ssl/slayer.crt --tls-key /etc/ssl/slayer.key

  # Connect (psql): the database name selects the SLayer datasource
  psql "host=127.0.0.1 port=5145 dbname=<datasource>"
""",
    )
    p.add_argument(
        "--host",
        default=None,
        help=(
            "Bind address. Defaults to 0.0.0.0; if --demo is given AND --token "
            "is not, defaults to 127.0.0.1 for the no-token loopback fallback."
        ),
    )
    p.add_argument(
        "--port", type=int, default=DEFAULT_PG_PORT,
        help=f"Port (default: {DEFAULT_PG_PORT})",
    )
    p.add_argument(
        "--token",
        default=None,
        help=(
            "Password token for authentication. Falls back to $SLAYER_PG_TOKEN. "
            "Required when binding a non-loopback address."
        ),
    )
    p.add_argument("--tls-cert", default=None, help="Path to TLS certificate (PEM).")
    p.add_argument("--tls-key", default=None, help="Path to TLS private key (PEM).")
    p.add_argument(
        "--demo",
        action="store_true",
        help=(
            "Generate and ingest the bundled Jaffle Shop demo dataset before "
            "starting (idempotent)."
        ),
    )
