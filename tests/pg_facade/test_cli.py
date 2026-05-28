"""Tests for the `slayer pg-serve` CLI helpers (DEV-1486)."""

from __future__ import annotations

import pytest

from slayer.pg_facade.server import DEFAULT_PG_PORT, _resolve_host, _resolve_token


@pytest.mark.parametrize(
    "host_arg,demo,token,expected",
    [
        (None, False, None, "0.0.0.0"),
        (None, True, None, "127.0.0.1"),
        (None, True, "tok", "0.0.0.0"),
        (None, False, "tok", "0.0.0.0"),
        ("1.2.3.4", False, None, "1.2.3.4"),
        ("1.2.3.4", True, None, "1.2.3.4"),
    ],
)
def test_resolve_host(host_arg, demo, token, expected) -> None:
    assert _resolve_host(host_arg=host_arg, demo=demo, token=token) == expected


def test_resolve_token_flag_wins(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_PG_TOKEN", "from_env")
    assert _resolve_token("from_flag") == "from_flag"


def test_resolve_token_falls_back_to_env(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_PG_TOKEN", "from_env")
    assert _resolve_token(None) == "from_env"


def test_resolve_token_none_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("SLAYER_PG_TOKEN", raising=False)
    assert _resolve_token(None) is None


def test_default_port_is_5145() -> None:
    assert DEFAULT_PG_PORT == 5145


def test_cli_module_imports_without_extra() -> None:
    # The pg facade is pure-stdlib — importing its CLI must not require any
    # optional dependency (e.g. pyarrow).
    import importlib

    mod = importlib.import_module("slayer.pg_facade.cli")
    assert hasattr(mod, "add_pg_serve_subparser")
    assert hasattr(mod, "run_pg_serve")


def test_pg_serve_subparser_registers() -> None:
    import argparse

    from slayer.pg_facade.cli import add_pg_serve_subparser

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    add_pg_serve_subparser(subparsers)
    args = parser.parse_args(["pg-serve", "--port", "9999", "--demo"])
    assert args.command == "pg-serve"
    assert args.port == 9999
    assert args.demo is True
