"""DEV-1428: CLI memory commands accept string ids.

* ``slayer memory save --id ID`` accepts an optional user id.
* ``slayer memory forget <id>`` switches from ``type=int`` to ``type=str``.

We test argparse plumbing AND end-to-end behavior via direct invocation
of ``slayer.cli.main`` with a synthetic ``argv`` — this catches both the
new ``--id`` flag and the ``forget`` type flip.
"""

from __future__ import annotations

import os
import sys
import tempfile
from contextlib import contextmanager
from collections.abc import Iterator

import pytest

from slayer.cli import main as cli_main


@contextmanager
def _argv(*argv: str) -> Iterator[None]:
    original = sys.argv
    sys.argv = ["slayer", *argv]
    try:
        yield
    finally:
        sys.argv = original


def _exit_code(*argv: str) -> int:
    """Invoke ``slayer.cli.main`` with the given argv; capture SystemExit."""
    with _argv(*argv):
        try:
            cli_main()
        except SystemExit as exc:  # NOSONAR(S5754) — capturing CLI exit code; re-raising would defeat the test
            return int(exc.code or 0)
    return 0


def _seed(storage_dir: str) -> None:
    import asyncio

    from slayer.core.enums import DataType
    from slayer.core.models import Column, DatasourceConfig, SlayerModel
    from slayer.storage.yaml_storage import YAMLStorage

    storage = YAMLStorage(base_dir=storage_dir)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(
            storage.save_datasource(
                DatasourceConfig(name="mydb", type="postgres", host="x"),
            )
        )
        loop.run_until_complete(
            storage.save_model(
                SlayerModel(
                    name="orders",
                    sql_table="orders",
                    data_source="mydb",
                    columns=[
                        Column(
                            name="id", sql="id",
                            type=DataType.DOUBLE, primary_key=True,
                        ),
                        Column(
                            name="amount", sql="amount", type=DataType.DOUBLE,
                        ),
                    ],
                )
            )
        )
        loop.run_until_complete(
            storage.set_datasource_priority(["mydb"])
        )
    finally:
        loop.close()


@pytest.fixture
def storage_dir() -> Iterator[str]:
    with tempfile.TemporaryDirectory() as tmpdir:
        store = os.path.join(tmpdir, "store")
        os.makedirs(store, exist_ok=True)
        _seed(store)
        yield store


class TestCliArgparseStringIds:
    def test_save_with_id_flag(
        self, storage_dir: str, capsys: pytest.CaptureFixture,
    ) -> None:
        # argparse must accept --id; the CLI must forward it through to
        # the service. ``--storage`` lives on the ``memory`` parser, so
        # it goes between ``memory`` and the ``save`` subcommand.
        rc = _exit_code(
            "memory",
            "--storage", storage_dir,
            "save",
            "--learning", "x",
            "--entities", "mydb.orders.amount",
            "--id", "my-rule",
        )
        captured = capsys.readouterr()
        assert rc == 0, captured.out + captured.err
        assert "my-rule" in captured.out

    def test_forget_accepts_string_id(
        self, storage_dir: str, capsys: pytest.CaptureFixture,
    ) -> None:
        rc = _exit_code(
            "memory",
            "--storage", storage_dir,
            "save",
            "--learning", "x",
            "--entities", "mydb.orders.amount",
            "--id", "kb.del",
        )
        assert rc == 0
        capsys.readouterr()
        rc = _exit_code(
            "memory",
            "--storage", storage_dir,
            "forget", "kb.del",
        )
        captured = capsys.readouterr()
        assert rc == 0, captured.out + captured.err
        assert "kb.del" in captured.out
