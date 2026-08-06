"""DEV-1757: the ``mcp`` dependency must stay on the 1.x FastMCP API.

`pyproject.toml` declared `mcp = ">=1.0"` with no upper bound. mcp 2.0.0
renamed ``mcp.server.fastmcp`` to ``mcp.server.mcpserver``, so every
lockfile-free install (`pip install motley-slayer`, `uv tool install`)
resolved a major that `slayer/mcp/server.py` cannot import. `poetry.lock`
pins a 1.x, so CI never saw it — only users did.

Three guards live here:

1. the declared constraint excludes 2.x (the bug reproduction);
2. a wrong / absent ``mcp`` produces an *actionable* error rather than the
   old "Reinstall SLayer" message, which reproduced the failure;
3. ``serverInfo.version`` reports SLayer's version, not the mcp SDK's.
"""

from __future__ import annotations

import pathlib
import sys
import tomllib
from importlib.metadata import PackageNotFoundError

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

import slayer
from slayer.mcp.server import _import_fastmcp, _set_server_version, create_mcp_server

PYPROJECT = pathlib.Path(__file__).parent.parent / "pyproject.toml"

# The 1.x the issue verified end-to-end (initialize handshake, tools/list,
# models_summary, a joined query) over stdio.
VERIFIED_1X = "1.29.0"
FIRST_BREAKING_MAJOR = "2.0.0"

# Every branch of the import error must offer the same remedy.
REMEDY = "mcp>=1.0,<2"
# The old text, which sent users round the loop that reproduced the failure.
OBSOLETE_REMEDY = "Reinstall SLayer"


def _mcp_constraint() -> str:
    """The raw ``mcp`` constraint from pyproject, in either Poetry form."""
    with open(PYPROJECT, "rb") as fh:
        deps = tomllib.load(fh)["tool"]["poetry"]["dependencies"]
    spec = deps["mcp"]
    # Poetry accepts a bare string or a table (`{version = "...", ...}`);
    # the guard must survive a future conversion to the table form.
    return spec["version"] if isinstance(spec, dict) else spec


def _mcp_specifier() -> SpecifierSet:
    """The declared constraint as a PEP 440 specifier set.

    Raises ``InvalidSpecifier`` if the constraint is not PEP 440 — a Poetry
    caret (``^1.0``) is semantically fine but unparseable here. Express the
    constraint in PEP 440 form (e.g. ``>=1.0,<2``) so this guard can evaluate
    it rather than silently skipping.
    """
    return SpecifierSet(_mcp_constraint())


class TestMcpDependencyPin:
    """Pin the mcp compatibility boundary at the packaging-metadata level."""

    def test_mcp_constraint_excludes_2_x(self) -> None:
        """The bug: an unbounded pin let resolvers pick mcp 2.0.0."""
        assert Version(FIRST_BREAKING_MAJOR) not in _mcp_specifier(), (
            f"`mcp = {_mcp_constraint()!r}` admits {FIRST_BREAKING_MAJOR}, which "
            f"removed `mcp.server.fastmcp` and breaks `slayer mcp` on every "
            f"fresh install (DEV-1757)."
        )

    def test_mcp_constraint_admits_1_x(self) -> None:
        """The cap must not be 'fixed' by over-tightening onto one release."""
        assert Version(VERIFIED_1X) in _mcp_specifier(), (
            f"`mcp = {_mcp_constraint()!r}` excludes {VERIFIED_1X}, the 1.x "
            f"verified against a live datasource in DEV-1757."
        )

    def test_mcp_constraint_is_pep440(self) -> None:
        _mcp_specifier()

    def test_installed_mcp_is_1_x(self) -> None:
        """The resolved environment, not just the declaration, is on 1.x."""
        from importlib.metadata import version

        installed = Version(version("mcp"))
        assert installed.major == 1
        assert installed in _mcp_specifier()


class TestFastMcpImportError:
    """The import failure must name the real remedy, not 'Reinstall SLayer'.

    ``sys.modules[name] = None`` makes ``import name`` raise ImportError
    ("halted; None in sys.modules"), so no mcp 2.x install is needed to
    exercise the failure paths. monkeypatch restores the entry afterwards.
    """

    @staticmethod
    def _block_fastmcp(monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "mcp.server.fastmcp", None)

    @staticmethod
    def _pretend_installed(monkeypatch: pytest.MonkeyPatch, version: str) -> None:
        monkeypatch.setattr("slayer.mcp.server._pkg_version", lambda _name: version)

    def test_returns_fastmcp_when_available(self) -> None:
        from mcp.server.fastmcp import FastMCP

        assert _import_fastmcp() is FastMCP

    def test_wrong_major_message_names_version_and_remedy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._block_fastmcp(monkeypatch)
        self._pretend_installed(monkeypatch, FIRST_BREAKING_MAJOR)

        with pytest.raises(ImportError) as excinfo:
            _import_fastmcp()

        message = str(excinfo.value)
        assert FIRST_BREAKING_MAJOR in message
        assert "mcp 2.x" in message  # the branch actually taken
        assert "mcp.server.fastmcp" in message
        assert REMEDY in message
        assert OBSOLETE_REMEDY not in message

    def test_1_x_metadata_is_not_blamed_on_the_2_x_rename(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Metadata reporting 1.x must not be diagnosed as the 2.x rename.

        Any ImportError raised from *inside* a genuine 1.x mcp — a broken
        transitive dependency, say — lands here too, and telling that user
        the module was renamed away would be a fresh misdiagnosis.
        """
        self._block_fastmcp(monkeypatch)
        self._pretend_installed(monkeypatch, "1.27.0")

        with pytest.raises(ImportError) as excinfo:
            _import_fastmcp()

        message = str(excinfo.value)
        assert "1.27.0" in message
        assert "could not be imported" in message  # the branch actually taken
        assert "mcpserver" not in message, (
            "mcp 1.27.0 predates the 2.x rename — the message must not claim "
            "the module was renamed away."
        )
        assert "2.x removed" not in message
        assert REMEDY in message

    def test_unparseable_version_falls_back_to_the_generic_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._block_fastmcp(monkeypatch)
        self._pretend_installed(monkeypatch, "not-a-version")

        with pytest.raises(ImportError) as excinfo:
            _import_fastmcp()

        message = str(excinfo.value)
        assert "not-a-version" in message
        assert "could not be imported" in message  # generic, not the 2.x branch
        assert "mcpserver" not in message
        assert "2.x removed" not in message
        assert REMEDY in message

    def test_missing_package_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._block_fastmcp(monkeypatch)

        def _absent(_name: str) -> str:
            raise PackageNotFoundError("mcp")

        monkeypatch.setattr("slayer.mcp.server._pkg_version", _absent)

        with pytest.raises(ImportError) as excinfo:
            _import_fastmcp()

        message = str(excinfo.value)
        assert "not found" in message.lower()
        assert REMEDY in message
        assert OBSOLETE_REMEDY not in message

    def test_every_branch_offers_the_same_remedy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No branch may drop part of the remedy the others give."""

        def _message_for(installed: str | None) -> str:
            with pytest.MonkeyPatch.context() as patch:
                patch.setitem(sys.modules, "mcp.server.fastmcp", None)
                if installed is None:
                    def _absent(_name: str) -> str:
                        raise PackageNotFoundError("mcp")

                    patch.setattr("slayer.mcp.server._pkg_version", _absent)
                else:
                    patch.setattr(
                        "slayer.mcp.server._pkg_version", lambda _name: installed
                    )
                with pytest.raises(ImportError) as excinfo:
                    _import_fastmcp()
                return str(excinfo.value)

        for installed in (None, "1.27.0", "not-a-version", FIRST_BREAKING_MAJOR):
            message = _message_for(installed)
            assert REMEDY in message, installed
            assert "motley-slayer" in message, installed
            assert OBSOLETE_REMEDY not in message, installed

    def test_original_exception_is_chained(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._block_fastmcp(monkeypatch)
        self._pretend_installed(monkeypatch, FIRST_BREAKING_MAJOR)

        with pytest.raises(ImportError) as excinfo:
            _import_fastmcp()

        assert isinstance(excinfo.value.__cause__, ImportError)

    def test_create_mcp_server_surfaces_the_actionable_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The helper is actually wired into the factory, not merely defined."""
        self._block_fastmcp(monkeypatch)
        self._pretend_installed(monkeypatch, FIRST_BREAKING_MAJOR)

        with pytest.raises(ImportError) as excinfo:
            create_mcp_server(storage=None, _seed_help=False)  # NOSONAR(S5655) — metadata-only build needs no storage

        assert REMEDY in str(excinfo.value)


class _ReadOnlyVersion:
    """Stands in for a future SDK exposing ``version`` as a read-only property."""

    @property
    def version(self) -> str:
        return "read-only"


class _ReadOnlyVersionServer:
    def __init__(self) -> None:
        self._mcp_server = _ReadOnlyVersion()


class _NoLowlevelServer:
    """A FastMCP-shaped object that exposes no ``_mcp_server`` at all."""


class TestServerInfoVersion:
    """serverInfo.version must be SLayer's version, not the mcp SDK's.

    FastMCP 1.x exposes no ``version`` kwarg and never forwards one to the
    lowlevel ``Server``, which then falls back to ``pkg_version("mcp")``.
    """

    @staticmethod
    def _server():
        # storage=None / _seed_help=False is the metadata-only build path
        # established by DEV-1669 — no storage or DB access needed.
        return create_mcp_server(storage=None, _seed_help=False)  # NOSONAR(S5655) — metadata-only build needs no storage

    def test_lowlevel_server_version_is_slayer_version(self) -> None:
        assert self._server()._mcp_server.version == slayer.__version__

    def test_initialization_options_report_slayer_version(self) -> None:
        options = self._server()._mcp_server.create_initialization_options()
        assert options.server_version == slayer.__version__

    def test_server_name_unchanged(self) -> None:
        options = self._server()._mcp_server.create_initialization_options()
        assert options.server_name == "SLayer"

    def test_read_only_version_degrades_instead_of_crashing(self) -> None:
        """A future SDK making ``version`` read-only must not break the build."""
        server = _ReadOnlyVersionServer()

        _set_server_version(server)

        assert server._mcp_server.version == "read-only"

    def test_absent_lowlevel_server_degrades_instead_of_crashing(self) -> None:
        _set_server_version(_NoLowlevelServer())
