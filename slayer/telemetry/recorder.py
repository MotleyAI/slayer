"""The per-process recorder: a no-op until ``start()``, and silent on every failure."""

import atexit
import datetime
import functools
import logging
import os
import sys
import threading
from collections import Counter
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any, ParamSpec, TypeVar

from slayer.telemetry import clock, features, sender, settings, spool
from slayer.telemetry.payload import (
    AGGREGATIONS,
    CLIENTS,
    DIALECTS,
    FLAGS,
    SCHEMA_VERSION,
    TRANSFORMS,
    USAGE_KEYS,
    Batch,
)

logger = logging.getLogger(__name__)

EXIT_WAIT = 1.0
NOTICE = (
    "SLayer collects anonymous, aggregated usage statistics (command, tool and query-feature counts, "
    "versions, OS) to guide its development - never queries, names, data or credentials. "
    "Details: https://docs.motley.ai/slayer/reference/telemetry/ - "
    "opt out with SLAYER_TELEMETRY=off, DO_NOT_TRACK=1 or `slayer telemetry disable`."
)

_ERROR_TOKENS = {
    "builtins.SystemExit": "exit",
    "builtins.KeyboardInterrupt": "interrupted",
    "asyncio.exceptions.CancelledError": "cancelled",
    "builtins.TimeoutError": "timeout",
    "builtins.ConnectionError": "connection",
    "builtins.FileNotFoundError": "file_not_found",
    "builtins.PermissionError": "permission",
    "builtins.OSError": "os_error",
    "builtins.ImportError": "import_error",
    "builtins.NotImplementedError": "not_implemented",
    "builtins.KeyError": "key_error",
    "builtins.TypeError": "type_error",
    "builtins.ValueError": "value_error",
    "json.decoder.JSONDecodeError": "json_error",
    "pydantic_core._pydantic_core.ValidationError": "validation_error",
    "sqlalchemy.exc.OperationalError": "db_operational",
    "sqlalchemy.exc.ProgrammingError": "db_programming",
    "sqlalchemy.exc.SQLAlchemyError": "db_error",
    "slayer.core.errors.SlayerError": "slayer_error",
    "slayer.core.errors.QueryTypeError": "query_type",
    "slayer.core.errors.AmbiguousModelError": "ambiguous_model",
    "slayer.core.errors.EntityResolutionError": "entity_resolution",
    "slayer.core.errors.MemoryNotFoundError": "memory_not_found",
    "slayer.core.errors.SchemaDriftError": "schema_drift",
    "slayer.core.errors.UnknownReferenceError": "unknown_reference",
    "slayer.core.errors.PopulationInferenceError": "population_inference",
    "slayer.core.errors.MissingDriverError": "missing_driver",
    "slayer.facade.translator.TranslationError": "translation",
    "mcp.server.fastmcp.exceptions.ToolError": "tool_error",
}
_STORAGE_TOKENS = {
    "slayer.storage.yaml_storage.YAMLStorage": "yaml",
    "slayer.storage.sqlite_storage.SQLiteStorage": "sqlite",
}
_CLIENT_NAMES = {
    "claude-code": "claude-code",
    "claude-ai": "claude-desktop",
    "claude-desktop": "claude-desktop",
    "cursor": "cursor",
    "cursor-vscode": "cursor",
    "visual studio code": "vscode",
    "visual-studio-code": "vscode",
    "vscode": "vscode",
    "windsurf": "windsurf",
    "windsurf-client": "windsurf",
    "codex": "codex",
    "codex-mcp-client": "codex",
    "zed": "zed",
    "cline": "cline",
    "goose": "goose",
    "continue": "continue",
    "continue-client": "continue",
}
_DATASOURCE_BUCKETS = ((0, "0"), (1, "1"), (5, "2-5"))
_MODEL_BUCKETS = ((0, "0"), (10, "1-10"), (50, "11-50"), (200, "51-200"))

P = ParamSpec("P")
R = TypeVar("R")


def _silent(fn: Callable[P, R]) -> Callable[P, R | None]:
    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | None:
        try:
            return fn(*args, **kwargs)
        except Exception:
            logger.debug("telemetry: %s failed", fn.__name__, exc_info=True)
            return None

    return wrapper


def _class_token(cls: type, table: dict[str, str]) -> str | None:
    for base in cls.__mro__:
        token = table.get(f"{base.__module__}.{base.__qualname__}")
        if token is not None:
            return token
    return None


class HttpError(Exception):
    """An HTTP error response, counted by its status class."""

    def __init__(self, status: int) -> None:
        super().__init__(status)
        self.status = status


def error_token(error: BaseException) -> str:
    if isinstance(error, HttpError):
        return "http_5xx" if error.status >= 500 else "http_4xx"
    return _class_token(type(error), _ERROR_TOKENS) or "other"


def _bucket(n: int, buckets: tuple[tuple[int, str], ...], top: str) -> str:
    return next((label for bound, label in buckets if n <= bound), top)


class _Recorder:
    """This process's counters; ``paths`` is ``None`` while inactive."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.paths: sender.Paths | None = None
        self.last_sent: datetime.datetime | None = None
        self.sending: threading.Thread | None = None
        self.clear()

    def clear(self) -> None:
        self.ok: Counter[str] = Counter()
        self.errors: dict[str, Counter[str]] = {}
        self.flags: Counter[str] = Counter()
        self.transforms: Counter[str] = Counter()
        self.aggregations: Counter[str] = Counter()
        self.queries: Counter[str] = Counter()
        self.datasources: dict[str, str] = {}
        self.storage: str | None = None
        self.model_count: str | None = None
        self.demo = False
        self.clients: Counter[tuple[str, int | None]] = Counter()
        self.first: datetime.date | None = None
        self.last: datetime.date | None = None

    def touch(self) -> None:
        today = clock.now().date()
        self.first = min(self.first or today, today)
        self.last = max(self.last or today, today)

    def take(self) -> Batch | None:
        """The live counters as a batch (``None`` if empty), clearing them."""
        if self.first is None or self.last is None:
            return None
        batch = Batch.model_validate({
            "period": {"first": self.first, "last": self.last},
            "usage": {
                key: {"ok": self.ok[key], "errors": dict(self.errors.get(key, {}))}
                for key in self.ok.keys() | self.errors.keys()
            },
            "features": {
                "flags": dict(self.flags),
                "transforms": dict(self.transforms),
                "aggregations": dict(self.aggregations),
            },
            "dialects": {
                "queries": dict(self.queries),
                "datasources": {d: {bucket: 1} for d, bucket in self.datasources.items()},
            },
            "context": {
                "storage": {self.storage: 1} if self.storage else {},
                "model_count": {self.model_count: 1} if self.model_count else {},
                "demo": int(self.demo),
            },
            "mcp_clients": [
                {"client": c, "major": m, "sessions": n} for (c, m), n in self.clients.items()
            ],
        })
        self.clear()
        return batch


_recorder = _Recorder()


def _after_fork_in_child() -> None:
    _recorder.lock = threading.RLock()
    _recorder.sending = None
    _recorder.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


def is_active() -> bool:
    return _recorder.paths is not None


def _send_in_background(paths: sender.Paths, now: datetime.datetime) -> None:
    thread = threading.Thread(
        target=sender.send, args=(paths,), kwargs={"now": now, "url": sender.endpoint()},
        name="slayer-telemetry-send", daemon=True,
    )
    _recorder.sending = thread
    thread.start()


@functools.cache
def _register_exit_hook() -> None:
    atexit.register(_at_exit)


def _at_exit() -> None:
    flush()
    shutdown()


@_silent
def start() -> None:
    """Activate recording in this process (called by the ``slayer`` CLI only)."""
    if _recorder.paths is not None or not settings.resolve_state().enabled:
        return
    now = clock.now()
    paths = sender.Paths(config=settings.config_path(), spool=settings.spool_dir(), lease=settings.lease_path())
    config = settings.with_install_id(settings.read_config(paths.config), today=now.date())
    if config.last_sent is None:
        config = config.model_copy(update={"last_sent": now})
    show_notice = config.notice_shown_schema != SCHEMA_VERSION
    config = config.model_copy(update={"notice_shown_schema": SCHEMA_VERSION})
    settings.write_config(config, paths.config)
    if show_notice:
        print(NOTICE, file=sys.stderr)
    with _recorder.lock:
        _recorder.paths = paths
        _recorder.last_sent = config.last_sent
        _recorder.clear()
    _register_exit_hook()
    if sender.is_due(config.last_sent, now=now):
        with _recorder.lock:
            _recorder.last_sent = now
        _send_in_background(paths, now)


def _maybe_send() -> None:
    """Send from a running process once 24 h have passed: spool the live counters, then send."""
    now = clock.now()
    with _recorder.lock:
        paths = _recorder.paths
        in_flight = _recorder.sending is not None and _recorder.sending.is_alive()
        if paths is None or in_flight or not sender.is_due(_recorder.last_sent, now=now):
            return
        _recorder.last_sent = now
        batch = _recorder.take()
        if batch is not None:
            spool.write(paths.spool, batch)
    _send_in_background(paths, now)


def _usage_key(surface: object, token: object) -> str | None:
    if not isinstance(surface, str):
        return None
    key = f"{surface}:{token}" if isinstance(token, str) else ""
    if key in USAGE_KEYS:
        return key
    other = f"{surface}:other"
    return other if other in USAGE_KEYS else None


def _succeeded(error: BaseException) -> bool:
    return isinstance(error, SystemExit) and error.code in (None, 0)


@_silent
def record(*, surface: str, token: str | None, error: BaseException | None = None) -> None:
    """Count one action on ``surface``, failed if ``error`` is given."""
    if _recorder.paths is None:
        return
    key = _usage_key(surface, token)
    if key is None:
        return
    with _recorder.lock:
        if error is None or _succeeded(error):
            _recorder.ok[key] += 1
        else:
            _recorder.errors.setdefault(key, Counter())[error_token(error)] += 1
        _recorder.touch()
    _maybe_send()


@contextmanager
def counting(*, surface: str, token: str | None) -> Generator[None]:
    """Count the enclosed action as ok, or as failed with the exception it raises."""
    try:
        yield
    except BaseException as exc:
        record(surface=surface, token=token, error=exc)
        raise
    record(surface=surface, token=token)


@_silent
def observe_query(query: Any) -> None:
    """Count the features of a query about to execute."""
    if _recorder.paths is None:
        return
    found = features.query_features(query)
    with _recorder.lock:
        _recorder.flags.update(f for f in found.flags if f in FLAGS)
        _recorder.transforms.update(n if n in TRANSFORMS else features.CUSTOM for n in found.transforms)
        _recorder.aggregations.update(n if n in AGGREGATIONS else features.CUSTOM for n in found.aggregations)
        _recorder.touch()


def _dialect(datasource: Any) -> str:
    name = features.dialect(datasource)
    return name if name in DIALECTS else features.OTHER


@_silent
def observe_executed(response: Any) -> None:
    """Count a query that ran, by the datasource on its engine response."""
    datasource = getattr(response, "datasource", None)
    if _recorder.paths is None or datasource is None:
        return
    dialect, demo = _dialect(datasource), features.is_demo(datasource)
    with _recorder.lock:
        _recorder.queries[dialect] += 1
        _recorder.demo = _recorder.demo or demo
        _recorder.touch()


@_silent
def observe_storage(storage: object) -> None:
    if _recorder.paths is None:
        return
    with _recorder.lock:
        _recorder.storage = _class_token(type(storage), _STORAGE_TOKENS) or "other"
        _recorder.touch()


@_silent
def observe_model_count(count: int) -> None:
    if _recorder.paths is None:
        return
    with _recorder.lock:
        _recorder.model_count = _bucket(count, _MODEL_BUCKETS, "200+")
        _recorder.touch()


@_silent
def observe_datasources(datasources: list[Any]) -> None:
    """Bucket the loaded datasources per dialect."""
    if _recorder.paths is None:
        return
    per_dialect = Counter(_dialect(ds) for ds in datasources)
    with _recorder.lock:
        _recorder.datasources = {d: _bucket(n, _DATASOURCE_BUCKETS, "6+") for d, n in per_dialect.items()}
        _recorder.touch()


def _major(version: object) -> int | None:
    head = version.split(".", 1)[0].strip() if isinstance(version, str) else ""
    return int(head) if head.isdigit() and head.isascii() and int(head) <= 999 else None


@_silent
def observe_mcp_client(*, name: object, version: object) -> None:
    """Count one MCP session of a client, by fixed client token and major version."""
    if _recorder.paths is None:
        return
    client = _CLIENT_NAMES.get(name.strip().lower(), "other") if isinstance(name, str) else "other"
    if client not in CLIENTS:
        client = "other"
    with _recorder.lock:
        _recorder.clients[(client, _major(version) if client != "other" else None)] += 1
        _recorder.touch()


@_silent
def flush() -> None:
    """Write this process's counters to its own spool file (no network)."""
    with _recorder.lock:
        paths = _recorder.paths
        batch = _recorder.take() if paths is not None else None
    if paths is not None and batch is not None:
        spool.write(paths.spool, batch)


@_silent
def shutdown() -> None:
    """Wait (bounded) for an in-flight send."""
    thread = _recorder.sending
    if thread is not None and thread.is_alive():
        thread.join(EXIT_WAIT)


def reset() -> None:
    """Deactivate and forget all live counters."""
    with _recorder.lock:
        _recorder.paths = None
        _recorder.last_sent = None
        _recorder.sending = None
        _recorder.clear()
