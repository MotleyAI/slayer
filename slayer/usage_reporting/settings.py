"""Where usage-reporting state lives and whether usage reporting is on."""

import calendar
import datetime
import json
import os
import sys
import uuid
from collections.abc import Callable, Generator
from contextlib import contextmanager
from enum import StrEnum
from importlib.metadata import Distribution, distribution
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

DIST_NAME = "motley-slayer"
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"", "0", "false", "no", "off"})
_ID_LIFETIME_MONTHS = 11


class DecidingRule(StrEnum):
    DO_NOT_TRACK = "DO_NOT_TRACK"
    ENV = "SLAYER_USAGE_REPORTING"
    CI = "CI"
    EDITABLE = "editable install"
    UNWRITABLE = "unwritable config directory"
    PERSISTED = "slayer usage-reporting enable/disable"
    DEFAULT = "default"


class UsageReportingState(BaseModel):
    enabled: bool
    rule: DecidingRule


class UsageReportingConfig(BaseModel):
    setting: Literal["enabled", "disabled"] | None = None
    install_id: uuid.UUID | None = None
    install_id_created: datetime.date | None = None
    notice_shown_schema: int | None = None
    last_sent: datetime.datetime | None = None


def config_dir() -> Path:
    if sys.platform == "win32":
        appdata = os.environ.get("APPDATA")
        return (Path(appdata) if appdata else Path.home() / "AppData" / "Roaming") / "slayer"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "slayer"
    xdg = os.environ.get("XDG_CONFIG_HOME")
    return (Path(xdg) if xdg else Path.home() / ".config") / "slayer"


def config_path() -> Path:
    return config_dir() / "usage-reporting.json"


def spool_dir() -> Path:
    return config_dir() / "usage-reporting-spool"


def lease_path() -> Path:
    return config_dir() / "usage-reporting-send.lease"


def is_editable_install(dist: Distribution | None = None) -> bool:
    """PEP 610: SLayer was installed with ``pip install -e`` (or ``poetry install``)."""
    try:
        text = (dist or distribution(DIST_NAME)).read_text("direct_url.json")
        return bool(text) and json.loads(text).get("dir_info", {}).get("editable") is True
    except Exception:
        return False


def _writable(directory: Path) -> bool:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        return directory.is_dir() and os.access(directory, os.W_OK)
    except OSError:
        return False


def _set(value: str | None) -> bool:
    return value is not None and value.strip().lower() not in _FALSY


def resolve_state(config: UsageReportingConfig | None = None) -> UsageReportingState:
    """The effective state and the first rule (highest precedence first) that decided it."""
    if (os.environ.get("DO_NOT_TRACK") or "").strip().lower() in _TRUTHY:
        return UsageReportingState(enabled=False, rule=DecidingRule.DO_NOT_TRACK)
    explicit = (os.environ.get("SLAYER_USAGE_REPORTING") or "").strip().lower()
    if explicit in ("on", "off"):
        return UsageReportingState(enabled=explicit == "on", rule=DecidingRule.ENV)
    if _set(os.environ.get("CI")):
        return UsageReportingState(enabled=False, rule=DecidingRule.CI)
    if is_editable_install():
        return UsageReportingState(enabled=False, rule=DecidingRule.EDITABLE)
    if not _writable(config_dir()):
        return UsageReportingState(enabled=False, rule=DecidingRule.UNWRITABLE)
    setting = (config or read_config()).setting
    if setting is not None:
        return UsageReportingState(enabled=setting == "enabled", rule=DecidingRule.PERSISTED)
    return UsageReportingState(enabled=True, rule=DecidingRule.DEFAULT)


def read_config(path: Path | None = None) -> UsageReportingConfig:
    """The stored config; missing or corrupt reads as empty."""
    try:
        config = UsageReportingConfig.model_validate_json((path or config_path()).read_bytes())
    except Exception:
        return UsageReportingConfig()
    if config.last_sent is not None and config.last_sent.tzinfo is None:
        config.last_sent = config.last_sent.replace(tzinfo=datetime.timezone.utc)
    return config


def update_config(
    change: Callable[[UsageReportingConfig], UsageReportingConfig], path: Path | None = None,
) -> tuple[UsageReportingConfig, UsageReportingConfig]:
    """Apply ``change`` to the stored config under a cross-process lock; returns (before, after)."""
    target = path or config_path()
    with exclusive_lock(target.with_name(target.name + ".lock"), wait=True):
        before = read_config(target)
        after = change(before)
        if after != before:
            write_private(target, after.model_dump_json().encode())
    return before, after


@contextmanager
def exclusive_lock(path: Path, *, wait: bool) -> Generator[bool]:
    """Hold an OS lock on ``path`` (freed on close or process death); without ``wait``, yields ``False`` if held."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            if sys.platform == "win32":
                msvcrt.locking(fd, msvcrt.LK_LOCK if wait else msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(fd, fcntl.LOCK_EX if wait else fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            if wait:
                raise
            yield False
        else:
            yield True
    finally:
        os.close(fd)


def write_private(path: Path, data: bytes) -> None:
    """Atomically replace ``path`` with ``data``, readable by the owner only."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".tmp-{uuid.uuid4().hex}")
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _add_months(day: datetime.date, months: int) -> datetime.date:
    year, month = divmod(day.month - 1 + months, 12)
    year, month = day.year + year, month + 1
    return day.replace(year=year, month=month, day=min(day.day, calendar.monthrange(year, month)[1]))


def with_install_id(config: UsageReportingConfig, *, today: datetime.date) -> UsageReportingConfig:
    """``config`` with a current install ID: created on first use, replaced once 11 months old."""
    created = config.install_id_created
    if config.install_id is not None and created is None:
        return config.model_copy(update={"install_id_created": today})
    if config.install_id is not None and created is not None and _add_months(created, _ID_LIFETIME_MONTHS) > today:
        return config
    return config.model_copy(update={"install_id": uuid.uuid4(), "install_id_created": today})
