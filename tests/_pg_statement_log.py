"""What a ``pytest-postgresql`` server logged for one database: its statements and new connections."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

# TCP only (the sandbox forbids Unix sockets); every statement and connection logged, prefixed by database and pid.
LOGGED_POSTGRES_OPTIONS = (
    "-c unix_socket_directories= -c log_statement=all -c log_connections=on "
    "-c log_disconnections=on -c log_line_prefix='%d|%p|'"
)

_LINE = re.compile(
    r"^(?P<db>[^|\n]*)\|(?P<pid>\d+)\|LOG:  "
    r"(?:(?P<stmt>statement|execute [^:]*): (?P<sql>.*)|(?P<conn>connection authorized): .*)$",
)

_WAIT_SECONDS = 10.0


class LoggedEntry(BaseModel):
    """One logged statement (its first line) or one new connection."""

    pid: int
    kind: Literal["statement", "connection"]
    sql: str = ""


class ServerLog:
    """The server log of ``database`` from the moment this object is made."""

    def __init__(self, *, logfile: str | Path, database: str) -> None:
        self._path = Path(logfile)
        self._database = database
        self._offset = self._path.stat().st_size

    def mark(self) -> None:
        """Forget everything logged so far."""
        self._offset = self._path.stat().st_size

    def entries(self, *, until: str | None = None) -> list[LoggedEntry]:
        """Entries logged since the last mark; with ``until``, wait until a statement containing it is logged."""
        deadline = time.monotonic() + _WAIT_SECONDS
        while True:
            found = self._read()
            if until is None or any(until in e.sql for e in found) or time.monotonic() > deadline:
                return found
            time.sleep(0.05)

    def statements(self, *, until: str | None = None) -> list[LoggedEntry]:
        return [e for e in self.entries(until=until) if e.kind == "statement"]

    def connections(self, *, until: str | None = None) -> list[LoggedEntry]:
        return [e for e in self.entries(until=until) if e.kind == "connection"]

    def _read(self) -> list[LoggedEntry]:
        with self._path.open(encoding="utf-8", errors="replace") as log:
            log.seek(self._offset)
            text = log.read()
        out: list[LoggedEntry] = []
        for line in text.splitlines():
            match = _LINE.match(line)
            if match is None or match["db"] != self._database:
                continue
            if match["conn"]:
                out.append(LoggedEntry(pid=int(match["pid"]), kind="connection"))
            else:
                out.append(LoggedEntry(pid=int(match["pid"]), kind="statement", sql=match["sql"].strip()))
        return out


def between(entries: list[LoggedEntry], *, after: str, through: str) -> list[LoggedEntry]:
    """The statements strictly after the one containing ``after``, up to and including the one containing ``through``."""
    start = next(i for i, e in enumerate(entries) if after in e.sql)
    end = next(i for i, e in enumerate(entries) if i > start and through in e.sql)
    return entries[start + 1:end + 1]
