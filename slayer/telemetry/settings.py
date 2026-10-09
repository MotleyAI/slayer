"""Where telemetry state lives and whether telemetry is on."""

import calendar
import datetime
import json
import os
import sys
import uuid
from enum import StrEnum
from importlib.metadata import Distribution, distribution
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

DIST_NAME = "motley-slayer"
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"", "0", "false", "no", "off"})
_ID_LIFETIME_MONTHS = 13


class DecidingRule(StrEnum):
    DO_NOT_TRACK = "DO_NOT_TRACK"
    ENV = "SLAYER_TELEMETRY"
    CI = "CI"
    EDITABLE = "editable install"
    UNWRITABLE = "unwritable config directory"
    PERSISTED = "slayer telemetry enable/disable"
    DEFAULT = "default"


class TelemetryState(BaseModel):
    enabled: bool
    rule: DecidingRule


class TelemetryConfig(BaseModel):
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
    return config_dir() / "telemetry.json"


def spool_dir() -> Path:
    return config_dir() / "telemetry-spool"


def lease_path() -> Path:
    return config_dir() / "telemetry-send.lease"


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


def resolve_state() -> TelemetryState:
    """The effective state and the first rule (highest precedence first) that decided it."""
    if (os.environ.get("DO_NOT_TRACK") or "").strip().lower() in _TRUTHY:
        return TelemetryState(enabled=False, rule=DecidingRule.DO_NOT_TRACK)
    explicit = (os.environ.get("SLAYER_TELEMETRY") or "").strip().lower()
    if explicit in ("on", "off"):
        return TelemetryState(enabled=explicit == "on", rule=DecidingRule.ENV)
    if _set(os.environ.get("CI")):
        return TelemetryState(enabled=False, rule=DecidingRule.CI)
    if is_editable_install():
        return TelemetryState(enabled=False, rule=DecidingRule.EDITABLE)
    if not _writable(config_dir()):
        return TelemetryState(enabled=False, rule=DecidingRule.UNWRITABLE)
    setting = read_config().setting
    if setting is not None:
        return TelemetryState(enabled=setting == "enabled", rule=DecidingRule.PERSISTED)
    return TelemetryState(enabled=True, rule=DecidingRule.DEFAULT)


def read_config(path: Path | None = None) -> TelemetryConfig:
    """The stored config; missing or corrupt reads as empty."""
    try:
        config = TelemetryConfig.model_validate_json((path or config_path()).read_bytes())
    except Exception:
        return TelemetryConfig()
    if config.last_sent is not None and config.last_sent.tzinfo is None:
        config.last_sent = config.last_sent.replace(tzinfo=datetime.timezone.utc)
    return config


def write_config(config: TelemetryConfig, path: Path | None = None) -> None:
    write_private(path or config_path(), config.model_dump_json().encode())


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


def with_install_id(config: TelemetryConfig, *, today: datetime.date) -> TelemetryConfig:
    """``config`` with a current install ID: created on first use, replaced once 13 months old."""
    created = config.install_id_created
    if config.install_id is not None and created is None:
        return config.model_copy(update={"install_id_created": today})
    if config.install_id is not None and created is not None and _add_months(created, _ID_LIFETIME_MONTHS) > today:
        return config
    return config.model_copy(update={"install_id": uuid.uuid4(), "install_id_created": today})
