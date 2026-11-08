"""Crash-safe file replacement shared by YAML storage and the v4 migration.

Its own module so both ``yaml_storage`` and ``v4_migration`` can use it without
an import cycle (``yaml_storage`` imports ``v4_migration``).
"""

import contextlib
import os
import stat
import tempfile
from typing import Any

import yaml

# mkstemp appends 8 random chars; reserve room so a long basename can't push the
# temp name past the directory's NAME_MAX (regression vs the old pid suffix).
_MKSTEMP_SUFFIX_LEN = 8
_DEFAULT_NAME_MAX = 255


def _temp_prefix(dir_path: str, basename: str) -> str:
    """Dot-hidden ``.<basename>.tmp.`` prefix, byte-truncated to fit NAME_MAX
    (which is a byte limit, so multibyte names truncate on char boundaries)."""
    try:
        name_max = os.pathconf(dir_path, "PC_NAME_MAX")
    except (OSError, ValueError, AttributeError):  # pragma: no cover — platform
        name_max = _DEFAULT_NAME_MAX
    marker = ".tmp."
    budget = name_max - _MKSTEMP_SUFFIX_LEN - len(marker) - 1  # leading dot
    if budget < 0:  # pragma: no cover — pathological NAME_MAX
        budget = 0
    encoded = basename.encode("utf-8")
    if len(encoded) > budget:
        basename = encoded[:budget].decode("utf-8", errors="ignore")
    return f".{basename}{marker}"


def _fsync_dir(dir_path: str) -> None:
    """Best-effort directory fsync so the rename is durable. Never raises: it
    runs after ``os.replace``, so a sync failure must not turn an
    already-installed write into a reported failure."""
    try:
        dir_fd = os.open(dir_path, os.O_RDONLY)
    except OSError:  # pragma: no cover — e.g. Windows can't open a dir fd
        return
    try:
        os.fsync(dir_fd)
    except OSError:  # pragma: no cover — filesystem without dir fsync
        pass
    finally:
        with contextlib.suppress(OSError):  # pragma: no cover — close rarely fails
            os.close(dir_fd)


def _atomic_write_text(path: str, text: str) -> None:
    """Durably replace ``path``: serialize to a same-dir temp file, fsync it,
    then ``os.replace`` (atomic on POSIX). An existing file's permission mode
    is preserved; new files are created ``0o600``. Any raised error leaves the
    old file in place and removes the temp file — the post-replace directory
    sync is best-effort and never raises, so an exception here always means the
    replace did not happen.

    Durability covers the file and its containing directory; a directory the
    caller freshly created (e.g. a new datasource dir) is not parent-fsynced,
    so a power loss can still drop that new dir and its first file — only ever
    a re-creatable brand-new record, never an existing one."""
    try:
        mode = stat.S_IMODE(os.stat(path).st_mode)
    except FileNotFoundError:
        mode = 0o600
    dir_path = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(
        dir=dir_path, prefix=_temp_prefix(dir_path, os.path.basename(path)),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:  # NOSONAR(S7493) — sync I/O in async by design
            f.write(text)
            f.flush()
            os.chmod(tmp, mode)  # before fsync so the mode is durable too
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp)
        raise
    _fsync_dir(dir_path)


def _atomic_write_yaml(path: str, data: Any) -> None:
    """Serialize completely before atomically replacing the YAML file."""
    _atomic_write_text(path=path, text=yaml.dump(data, sort_keys=False))
