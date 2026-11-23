"""The ``mcp`` dependency is pinned to the tested 2.x line, and building the server refuses any other install."""

from __future__ import annotations

import pathlib
import sys
import tomllib
from importlib.metadata import PackageNotFoundError, version

import pytest
from mcp.server.mcpserver import MCPServer
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from slayer.mcp.server import create_mcp_server

PYPROJECT = pathlib.Path(__file__).parent.parent / "pyproject.toml"

REQUIRED_RANGE = ">=2.3,<3"
REMEDY = f"mcp{REQUIRED_RANGE}"
OBSOLETE_REMEDY = "Reinstall SLayer"


def _mcp_constraint() -> str:
    """The raw ``mcp`` constraint from pyproject, in either Poetry form."""
    with open(PYPROJECT, "rb") as fh:
        deps = tomllib.load(fh)["tool"]["poetry"]["dependencies"]
    spec = deps["mcp"]
    return spec["version"] if isinstance(spec, dict) else spec


def _mcp_specifier() -> SpecifierSet:
    """The declared constraint as a PEP 440 specifier set (raises on a non-PEP-440 form like a Poetry caret)."""
    return SpecifierSet(_mcp_constraint())


def _build() -> object:
    return create_mcp_server(storage=None, _seed_help=False)  # pyright: ignore[reportArgumentType]  # NOSONAR(S5655) — metadata-only build needs no storage


class TestMcpDependencyPin:
    @pytest.mark.parametrize("admitted", ["2.3.0", "2.3.1", "2.9.0"])
    def test_constraint_admits_the_tested_2_x_line(self, admitted: str) -> None:
        assert Version(admitted) in _mcp_specifier(), _mcp_constraint()

    @pytest.mark.parametrize("excluded", ["1.27.0", "2.0.0", "2.2.0", "2.2.9", "3.0.0"])
    def test_constraint_excludes_untested_lines(self, excluded: str) -> None:
        assert Version(excluded) not in _mcp_specifier(), _mcp_constraint()

    def test_constraint_is_pep440(self) -> None:
        _mcp_specifier()

    def test_installed_mcp_is_in_range(self) -> None:
        assert Version(version("mcp")) in _mcp_specifier()


class TestMcpImportGuard:
    """``_pkg_version`` stands in for the installed distribution; ``sys.modules[name]=None`` for an absent module."""

    @staticmethod
    def _pretend_installed(monkeypatch: pytest.MonkeyPatch, installed: str | None) -> None:
        def _absent(_name: str) -> str:
            raise PackageNotFoundError("mcp")

        monkeypatch.setattr("slayer.mcp.server._pkg_version", _absent if installed is None else lambda _name: installed)

    @staticmethod
    def _block_mcpserver(monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "mcp.server.mcpserver", None)

    def _message(self, monkeypatch: pytest.MonkeyPatch, *, installed: str | None, block: bool) -> str:
        self._pretend_installed(monkeypatch, installed)
        if block:
            self._block_mcpserver(monkeypatch)
        with pytest.raises(ImportError) as excinfo:
            _build()
        return str(excinfo.value)

    def test_builds_on_mcpserver_when_in_range(self) -> None:
        assert isinstance(_build(), MCPServer)

    def test_missing_package_names_the_install_remedy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        message = self._message(monkeypatch, installed=None, block=True)
        assert "not found" in message.lower()
        assert REMEDY in message

    def test_1_x_names_installed_version_and_required_range(self, monkeypatch: pytest.MonkeyPatch) -> None:
        message = self._message(monkeypatch, installed="1.27.0", block=True)
        assert "1.27.0" in message
        assert REQUIRED_RANGE in message

    @pytest.mark.parametrize("installed", ["2.2.0", "3.0.0"])
    def test_out_of_range_names_installed_version_and_required_range(
        self, monkeypatch: pytest.MonkeyPatch, installed: str,
    ) -> None:
        """The module imports, but the version is outside the tested range."""
        message = self._message(monkeypatch, installed=installed, block=False)
        assert installed in message
        assert REQUIRED_RANGE in message

    @pytest.mark.parametrize("installed", ["not-a-version", "2.3.invalid"])
    def test_unparseable_version_is_refused(self, monkeypatch: pytest.MonkeyPatch, installed: str) -> None:
        message = self._message(monkeypatch, installed=installed, block=False)
        assert installed in message
        assert REMEDY in message

    def test_pre_release_of_the_floor_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        message = self._message(monkeypatch, installed="2.3.0rc1", block=False)
        assert "2.3.0rc1" in message
        assert REQUIRED_RANGE in message

    @pytest.mark.parametrize("installed", ["2.3.0", "2.3.0.post1"])
    def test_full_releases_in_range_build(self, monkeypatch: pytest.MonkeyPatch, installed: str) -> None:
        self._pretend_installed(monkeypatch, installed)
        assert isinstance(_build(), MCPServer)

    @pytest.mark.parametrize(("installed", "block"), [
        (None, True), ("1.27.0", True), ("2.2.0", False), ("3.0.0", False), ("not-a-version", True),
    ])
    def test_every_branch_offers_the_same_remedy(
        self, monkeypatch: pytest.MonkeyPatch, installed: str | None, block: bool,
    ) -> None:
        message = self._message(monkeypatch, installed=installed, block=block)
        assert REMEDY in message
        assert "motley-slayer" in message
        assert OBSOLETE_REMEDY not in message
        assert "<2" not in message
