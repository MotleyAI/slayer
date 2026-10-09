"""The ``env`` part of a report, read from the interpreter and package metadata (never by importing)."""

import functools
import os
import platform
import re
import sys
from importlib.metadata import PackageNotFoundError, distribution, version
from pathlib import Path

from slayer.telemetry.payload import EXTRAS, VERSION_PATTERN, ArchToken, Env, ExtraToken, OsToken
from slayer.telemetry.settings import DIST_NAME

_UNKNOWN_VERSION = "0.0.0+unknown"
_OS: dict[str, OsToken] = {"linux": "linux", "darwin": "darwin", "win32": "windows"}
_ARCH: dict[str, ArchToken] = {"x86_64": "x86_64", "amd64": "x86_64", "arm64": "arm64", "aarch64": "arm64"}
_REQUIREMENT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_EXTRA_MARKER = re.compile(r"""extra\s*==\s*["']([^"']+)["']""")


def _slayer_version() -> str:
    try:
        found = version(DIST_NAME)
    except PackageNotFoundError:
        return _UNKNOWN_VERSION
    return found if re.fullmatch(VERSION_PATTERN, found) and len(found) <= 64 else _UNKNOWN_VERSION


def _installed(name: str) -> bool:
    try:
        distribution(name)
    except PackageNotFoundError:
        return False
    return True


def _extras() -> list[ExtraToken]:
    """Extras whose every requirement is installed."""
    try:
        requires = distribution(DIST_NAME).requires or []
    except PackageNotFoundError:
        return []
    needs: dict[str, list[str]] = {}
    for requirement in requires:
        name = _REQUIREMENT_NAME.match(requirement)
        for extra in _EXTRA_MARKER.findall(requirement):
            if name is not None:
                needs.setdefault(extra, []).append(name.group(0))
    return [extra for extra in EXTRAS if extra in needs and all(_installed(n) for n in needs[extra])]


def _in_container() -> bool:
    if Path("/.dockerenv").exists() or Path("/run/.containerenv").exists():
        return True
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        return True
    try:
        cgroup = Path("/proc/1/cgroup").read_text()
    except OSError:
        return False
    return any(marker in cgroup for marker in ("docker", "kubepods", "containerd", "libpod"))


@functools.cache
def current() -> Env:
    return Env(
        slayer_version=_slayer_version(),
        python=f"{sys.version_info.major}.{sys.version_info.minor}",
        os=_OS.get(sys.platform, "other"),
        arch=_ARCH.get(platform.machine().lower(), "other"),
        in_container=_in_container(),
        extras=_extras(),
    )
