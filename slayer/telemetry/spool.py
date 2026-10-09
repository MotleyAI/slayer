"""Local spool of unsent batches, and the lease that lets one process send at a time."""

import datetime
import json
import logging
import os
import stat
import time
import uuid
from pathlib import Path

from slayer.telemetry.payload import Batch
from slayer.telemetry.settings import write_private

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
            batches.append(Batch.model_validate_json(path.read_bytes()))
        except Exception:
            logger.debug("telemetry: skipping unreadable spool file", exc_info=True)
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


def acquire_lease(path: Path, *, now: datetime.datetime) -> str | None:
    """A token if this process may send now, else ``None`` (another sender's lease is live)."""
    token = uuid.uuid4().hex
    data = json.dumps({"token": token, "expires": (now + LEASE_TTL).isoformat()}).encode()
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        if not _lease_expired(path, now=now):
            return None
        write_private(path, data)
        return token if _lease_token(path) == token else None
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
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
    if _lease_token(path) == token:
        path.unlink(missing_ok=True)
