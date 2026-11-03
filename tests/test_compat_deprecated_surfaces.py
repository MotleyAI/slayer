"""Workarounds scheduled for removal in 2.0 warn once per use; their replacements stay silent.

Spec: compatibility/removal-tripwire (Deprecated surfaces warn once per use).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.errors import MemoryNotFoundError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.mcp.server import create_mcp_server
from slayer.storage.base import default_storage_path
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process
from tests._compat_helpers import assert_one_deprecation, future_warnings, next_line, recorded

ROLES = ("flag_storage", "flag_models_dir", "env_storage", "env_models_dir")
_ENV = {"env_storage": "SLAYER_STORAGE", "env_models_dir": "SLAYER_MODELS_DIR"}
_FLAG = {"flag_storage": "--storage", "flag_models_dir": "--models-dir"}

#: (inputs given, the one deciding the storage, the replacement its warning names or None)
PRECEDENCE = [
    (("flag_storage",), "flag_storage", None),
    (("flag_models_dir",), "flag_models_dir", "--storage"),
    (("env_storage",), "env_storage", None),
    (("env_models_dir",), "env_models_dir", "SLAYER_STORAGE"),
    (("flag_storage", "flag_models_dir"), "flag_storage", None),
    (("flag_storage", "env_storage"), "flag_storage", None),
    (("flag_storage", "env_models_dir"), "flag_storage", None),
    (("flag_models_dir", "env_storage"), "flag_models_dir", "--storage"),
    (("flag_models_dir", "env_models_dir"), "flag_models_dir", "--storage"),
    (("env_storage", "env_models_dir"), "env_storage", None),
]


def _store(base: Path, *, datasource: str) -> Path:
    run_sync(YAMLStorage(base_dir=str(base)).save_datasource(
        DatasourceConfig(name=datasource, type="sqlite", database=":memory:"),
    ))
    return base


@pytest.mark.parametrize(("inputs", "winner", "remedy"), PRECEDENCE, ids=["+".join(c[0]) for c in PRECEDENCE])
def test_cli_storage_precedence(inputs: tuple[str, ...], winner: str, remedy: str | None, tmp_path: Path) -> None:
    dirs = {role: _store(tmp_path / role, datasource=role) for role in ROLES}
    flags = [arg for role in inputs if role in _FLAG for arg in (_FLAG[role], str(dirs[role]))]
    env = {k: v for k, v in os.environ.items() if k not in _ENV.values()}
    env.update({_ENV[role]: str(dirs[role]) for role in inputs if role in _ENV})
    env.update({"HOME": str(tmp_path / "home"), "XDG_DATA_HOME": str(tmp_path / "xdg")})
    with recorded() as caught:
        result = run_cli_in_process(["datasources", *flags, "list"], env=env)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split()[0] == winner
    deprecations = [line for line in result.stderr.splitlines() if "SLayer 2.0" in line]
    if remedy is None:
        assert deprecations == []
        assert future_warnings(caught) == []
    else:
        assert len(deprecations) == 1, result.stderr
        assert remedy in deprecations[0]


def test_default_storage_path_warns_only_when_slayer_models_dir_decides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SLAYER_STORAGE", raising=False)
    monkeypatch.setenv("SLAYER_MODELS_DIR", str(tmp_path / "legacy"))
    with recorded() as caught:
        line = next_line()
        path = default_storage_path()
    assert path == str(tmp_path / "legacy")
    assert_one_deprecation(caught, call="SLAYER_MODELS_DIR", remedies=("SLAYER_STORAGE",), filename=__file__, line=line)

    monkeypatch.setenv("SLAYER_STORAGE", str(tmp_path / "current"))
    with recorded() as caught:
        path = default_storage_path()
    assert path == str(tmp_path / "current")
    assert future_warnings(caught) == []


async def test_mcp_inspect_model_warns_and_inspect_does_not(tmp_path: Path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="mydb", type="sqlite", database=":memory:"))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    ))
    server = create_mcp_server(storage=storage, _seed_help=False)
    with recorded() as caught:
        await server.call_tool(name="inspect_model", arguments={"model_name": "orders", "sections": ["columns"]})
    future = future_warnings(caught)
    assert len(future) == 1, [str(w.message) for w in future]
    message = str(future[0].message)
    assert "inspect_model" in message
    assert "SLayer 2.0" in message
    assert "inspect" in message.replace("inspect_model", "")

    with recorded() as caught:
        await server.call_tool(name="inspect", arguments={"entity_type": "model", "reference": "mydb.orders"})
    assert future_warnings(caught) == []


def test_memory_not_found_identifier_warns_and_memory_id_does_not() -> None:
    with recorded() as caught:
        error = MemoryNotFoundError("m-1")
        assert error.memory_id == "m-1"
    assert future_warnings(caught) == []

    with recorded() as caught:
        line = next_line()
        identifier = error.identifier
    assert identifier == "m-1"
    assert_one_deprecation(caught, call=".identifier", remedies=("memory_id",), filename=__file__, line=line)


def test_memory_not_found_identifier_stays_assignable_as_in_1_5_0() -> None:
    error = MemoryNotFoundError("m-1")
    with recorded() as caught:
        error.identifier = "other"
    assert future_warnings(caught) == []
    assert error.memory_id == "m-1"
    with recorded():
        assert error.identifier == "other"
