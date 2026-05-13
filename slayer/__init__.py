"""SLayer — a lightweight semantic layer for AI agents."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("motley-slayer")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"
