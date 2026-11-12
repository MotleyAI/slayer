"""The CLI storage location: ``--storage`` wins over ``SLAYER_STORAGE``, and neither warns."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process
from tests._compat_helpers import future_warnings, recorded

ROLES = ("flag_storage", "env_storage")
_ENV = {"env_storage": "SLAYER_STORAGE"}
_FLAG = {"flag_storage": "--storage"}

#: (inputs given, the one deciding the storage)
PRECEDENCE = [
    (("flag_storage",), "flag_storage"),
    (("env_storage",), "env_storage"),
    (("flag_storage", "env_storage"), "flag_storage"),
]


def _store(base: Path, *, datasource: str) -> Path:
    run_sync(YAMLStorage(base_dir=str(base)).save_datasource(
        DatasourceConfig(name=datasource, type="sqlite", database=":memory:"),
    ))
    return base


@pytest.mark.parametrize(("inputs", "winner"), PRECEDENCE, ids=["+".join(c[0]) for c in PRECEDENCE])
def test_cli_storage_precedence(inputs: tuple[str, ...], winner: str, tmp_path: Path) -> None:
    dirs = {role: _store(tmp_path / role, datasource=role) for role in ROLES}
    flags = [arg for role in inputs if role in _FLAG for arg in (_FLAG[role], str(dirs[role]))]
    env = {k: v for k, v in os.environ.items() if k not in _ENV.values()}
    env.update({_ENV[role]: str(dirs[role]) for role in inputs if role in _ENV})
    env.update({"HOME": str(tmp_path / "home"), "XDG_DATA_HOME": str(tmp_path / "xdg")})
    with recorded() as caught:
        result = run_cli_in_process(["datasources", *flags, "list"], env=env)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split()[0] == winner
    assert [line for line in result.stderr.splitlines() if "Deprecated" in line] == []
    assert future_warnings(caught) == []
