"""CLI tests for ``slayer memory <subcommand>`` (DEV-1357 v2).

Two subcommands land:

    slayer memory save    --learning "<text>" [--entities a,b,c | --query <json|@file>]
    slayer memory forget  <id>

Memory retrieval is part of ``slayer search`` (covered in
``test_search_surfaces.py``). Tests invoke the dispatcher
(`_run_memory`) directly with a populated ``argparse.Namespace`` — same
pattern as ``test_cli.py`` already uses for ``_run_query``. End-to-end
argparse plumbing is covered by a single-shot subprocess test at the
bottom.
"""

import json
import os
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from typing import Optional

import pytest

from slayer.cli import _run_memory
from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def seeded_storage_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=tmpdir)
        await s.save_datasource(
            DatasourceConfig(name="mydb", type="postgres", host="x")
        )
        await s.save_model(
            SlayerModel(
                name="orders",
                data_source="mydb",
                sql_table="orders",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                    ),
                    Column(
                        name="amount",
                        sql="amount",
                        type=DataType.DOUBLE,
                    ),
                    Column(
                        name="status",
                        sql="status",
                        type=DataType.TEXT,
                    ),
                ],
                measures=[ModelMeasure(formula="amount:sum", name="rev")],
            )
        )
        await s.set_datasource_priority(["mydb"])
        yield tmpdir


def _args(
    *,
    storage_path: str,
    memory_command: str,
    learning: Optional[str] = None,
    entities: Optional[str] = None,
    query: Optional[str] = None,
    id: Optional[int] = None,  # noqa: A002 — argparse arg
) -> SimpleNamespace:
    return SimpleNamespace(
        storage=storage_path,
        models_dir=None,
        memory_command=memory_command,
        learning=learning,
        entities=entities,
        query=query,
        id=id,
    )


class TestMemorySaveSubcommand:
    def test_save_with_entity_list(self, seeded_storage_path, capsys):
        _run_memory(
            _args(
                storage_path=seeded_storage_path,
                memory_command="save",
                learning="orders.amount in cents",
                entities="mydb.orders.amount",
            )
        )
        out = capsys.readouterr().out
        # CLI prints the assigned id; first save → 1.
        assert "1" in out

    def test_save_with_query_inline(self, seeded_storage_path, capsys):
        query_json = json.dumps(
            {
                "source_model": "orders",
                "measures": [{"formula": "amount:sum"}],
            }
        )
        _run_memory(
            _args(
                storage_path=seeded_storage_path,
                memory_command="save",
                learning="rev",
                query=query_json,
            )
        )
        out = capsys.readouterr().out
        assert "1" in out

    def test_save_with_query_at_file(
        self,
        seeded_storage_path,
        tmp_path,
        capsys,
    ):
        f = tmp_path / "q.json"
        f.write_text(
            json.dumps(
                {
                    "source_model": "orders",
                    "measures": [{"formula": "amount:sum"}],
                }
            )
        )
        _run_memory(
            _args(
                storage_path=seeded_storage_path,
                memory_command="save",
                learning="rev",
                query=f"@{f}",
            )
        )
        out = capsys.readouterr().out
        assert "1" in out

    def test_save_requires_entities_or_query(
        self, seeded_storage_path, capsys
    ):
        with pytest.raises(SystemExit):
            _run_memory(
                _args(
                    storage_path=seeded_storage_path,
                    memory_command="save",
                    learning="x",
                )
            )


class TestMemoryForgetSubcommand:
    def test_forget_existing(self, seeded_storage_path, capsys):
        # Seed a row first via the same CLI dispatcher.
        _run_memory(
            _args(
                storage_path=seeded_storage_path,
                memory_command="save",
                learning="x",
                entities="mydb.orders.amount",
            )
        )
        capsys.readouterr()
        _run_memory(
            _args(
                storage_path=seeded_storage_path,
                memory_command="forget",
                id=1,
            )
        )
        out = capsys.readouterr().out
        assert "1" in out

    def test_forget_missing_exits_1(self, seeded_storage_path, capsys):
        with pytest.raises(SystemExit):
            _run_memory(
                _args(
                    storage_path=seeded_storage_path,
                    memory_command="forget",
                    id=999,
                )
            )


class TestMemoryArgparsePlumbing:
    """Single subprocess smoke test that the ``memory`` subcommand and
    its two subsubcommands are wired into the top-level argparser
    (`slayer memory --help` must exit 0 and list save+forget but no
    longer mention the removed ``recall`` subcommand)."""

    def test_top_level_help_lists_memory(self):
        env = dict(os.environ)
        result = subprocess.run(
            [sys.executable, "-m", "slayer.cli", "memory", "--help"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        out = result.stdout.lower()
        assert "save" in out
        assert "forget" in out
        assert "recall" not in out

    def test_memory_recall_subcommand_rejected(self):
        env = dict(os.environ)
        result = subprocess.run(
            [sys.executable, "-m", "slayer.cli", "memory", "recall"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        assert result.returncode != 0
