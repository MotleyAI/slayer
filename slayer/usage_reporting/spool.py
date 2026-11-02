"""Local spool of unsent batches, and the lease that lets one process send at a time."""

import datetime
import json
import logging
import os
import stat
import time
import uuid
from contextlib import AbstractContextManager
from pathlib import Path

from slayer.usage_reporting.payload import Batch, read_spooled
from slayer.usage_reporting.settings import exclusive_lock, write_private

logger = logging.getLogger(__name__)

_PENDING = "spool-"
_CLAIMED = "claim-"
_SUFFIX = ".json"
MAX_FILES = 100
MAX_BYTES = 1_000_000
LEASE_TTL = datetime.timedelta(minutes=5)


def _regular(path: Path) -> int | None:
    """Size of ``path`` if it is a regular file (never following a link), else ``None``."""
    try:
        st = path.lstat()
    except OSError:
        return None
    return st.st_size if stat.S_ISREG(st.st_mode) else None


def _entries(directory: Path, prefix: str) -> list[Path]:
    try:
        names = os.listdir(directory)
    except OSError:
        return []
    return [directory / n for n in names if n.startswith(prefix) and n.endswith(_SUFFIX)]


def _age_key(path: Path) -> str:
    return path.name.removeprefix(_PENDING).removeprefix(_CLAIMED)


def write(directory: Path, batch: Batch) -> None:
    """Add ``batch`` as a new spool file, then enforce the cap."""
    name = f"{_PENDING}{time.time_ns():020d}-{uuid.uuid4().hex[:12]}{_SUFFIX}"
    write_private(directory / name, batch.model_dump_json().encode())
    _enforce_cap(directory)


def _enforce_cap(directory: Path) -> None:
    files = [(p, size) for p in pending_files(directory) if (size := _regular(p)) is not None]
    files.sort(key=lambda item: _age_key(item[0]))
    total = sum(size for _, size in files)
    while files and (len(files) > MAX_FILES or total > MAX_BYTES):
        oldest, size = files.pop(0)
        oldest.unlink(missing_ok=True)
        total -= size


def pending_files(directory: Path) -> list[Path]:
    """Every regular spool file, claimed or not."""
    paths = _entries(directory, _PENDING) + _entries(directory, _CLAIMED)
    return [p for p in paths if _regular(p) is not None]


def load(paths: list[Path]) -> list[Batch]:
    batches = []
    for path in paths:
        try:
            batches.append(read_spooled(path.read_bytes()))
        except Exception:
            logger.debug("usage reporting: skipping unreadable spool file", exc_info=True)
    return batches


def claim(directory: Path) -> list[Path]:
    """Mark every pending file as claimed; return all claimed files (including a dead sender's)."""
    for path in _entries(directory, _PENDING):
        if _regular(path) is not None:
            try:
                path.rename(directory / (_CLAIMED + path.name.removeprefix(_PENDING)))
            except OSError:
                continue
    return [p for p in _entries(directory, _CLAIMED) if _regular(p) is not None]


def purge(directory: Path) -> None:
    """Delete every spool file."""
    for path in pending_files(directory):
        path.unlink(missing_ok=True)


def _guard(lease: Path) -> AbstractContextManager[bool]:
    return exclusive_lock(lease.with_name(lease.name + ".lock"), wait=False)


def acquire_lease(path: Path, *, now: datetime.datetime) -> str | None:
    """A token if this process may send now, else ``None`` (another sender's lease is live)."""
    with _guard(path) as held:
        if not held or not _lease_expired(path, now=now):
            return None
        token = uuid.uuid4().hex
        write_private(path, json.dumps({"token": token, "expires": (now + LEASE_TTL).isoformat()}).encode())
        return token


def _lease_token(path: Path) -> str | None:
    try:
        return json.loads(path.read_bytes()).get("token")
    except Exception:
        return None


def _lease_expired(path: Path, *, now: datetime.datetime) -> bool:
    try:
        return datetime.datetime.fromisoformat(json.loads(path.read_bytes())["expires"]) <= now
    except Exception:
        return True


def release_lease(path: Path, token: str) -> None:
    with _guard(path) as held:
        if held and _lease_token(path) == token:
            path.unlink(missing_ok=True)
