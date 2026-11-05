"""Local spool of unsent batches, and the lease that lets one process send at a time."""

import datetime
import json
import logging
import os
import re
import stat
import time
import uuid
from contextlib import AbstractContextManager
from pathlib import Path

from slayer.usage_reporting.payload import SCHEMA_VERSION, Batch
from slayer.usage_reporting.settings import exclusive_lock, write_private

logger = logging.getLogger(__name__)

_PENDING = f"v{SCHEMA_VERSION}-spool-"
_CLAIMED = f"v{SCHEMA_VERSION}-claim-"
_SUFFIX = ".json"
# Schema 1 wrote unversioned ``spool-`` / ``claim-`` names.
_NAME = re.compile(r"^(?:v(?P<version>[1-9]\d*)-)?(?P<state>spool|claim)-(?P<age>.+)\.json$", re.ASCII)
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


def _entries(directory: Path, *, states: tuple[str, ...], sendable_only: bool) -> list[Path]:
    """Regular spool files in ``states``; ``sendable_only`` skips newer schema versions."""
    try:
        names = os.listdir(directory)
    except OSError:
        return []
    found = []
    for name in names:
        match = _NAME.match(name)
        if match is None or match["state"] not in states:
            continue
        if sendable_only and int(match["version"] or 1) > SCHEMA_VERSION:
            continue
        if _regular(directory / name) is not None:
            found.append(directory / name)
    return found


def _age_key(path: Path) -> str:
    match = _NAME.match(path.name)
    return match["age"] if match else path.name


def write(directory: Path, batch: Batch) -> None:
    """Add ``batch`` as a new spool file, then enforce the cap."""
    name = f"{_PENDING}{time.time_ns():020d}-{uuid.uuid4().hex[:12]}{_SUFFIX}"
    write_private(directory / name, batch.model_dump_json().encode())
    _enforce_cap(directory)


def _enforce_cap(directory: Path) -> None:
    files = [(p, size) for p in _all_files(directory) if (size := _regular(p)) is not None]
    files.sort(key=lambda item: _age_key(item[0]))
    total = sum(size for _, size in files)
    while files and (len(files) > MAX_FILES or total > MAX_BYTES):
        oldest, size = files.pop(0)
        oldest.unlink(missing_ok=True)
        total -= size


def _all_files(directory: Path) -> list[Path]:
    return _entries(directory, states=("spool", "claim"), sendable_only=False)


def pending_files(directory: Path) -> list[Path]:
    """Every spool file this version can send, claimed or not."""
    return _entries(directory, states=("spool", "claim"), sendable_only=True)


def load(paths: list[Path]) -> list[Batch]:
    batches = []
    for path in paths:
        try:
            batches.append(Batch.model_validate_json(path.read_bytes()))
        except Exception:
            logger.debug("usage reporting: skipping unreadable spool file", exc_info=True)
    return batches


def claim(directory: Path) -> list[Path]:
    """Claim every pending file of this or an older schema; return all such claimed files (a dead sender's too)."""
    for path in _entries(directory, states=("spool",), sendable_only=True):
        try:
            path.rename(directory / (_CLAIMED + _age_key(path) + _SUFFIX))
        except OSError:
            continue
    return _entries(directory, states=("claim",), sendable_only=True)


def purge(directory: Path) -> None:
    """Delete the spool files of every schema version."""
    for path in _all_files(directory):
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
