"""Sample-value profiling of ``Column.sampled`` / ``sampled_values`` / ``distinct_count``.

:func:`ensure_samples_fresh` is the one owner every read path and forced refresh goes through.
Categorical columns: top 50 values by frequency in one scan (overflow keeps the top 50, total
unknown). Numeric/temporal columns: one batched min/max query. Failures are classified by a
row-count probe plus a consecutive-failure breaker and cached per engine; under a session policy
samples live only in the engine, never in storage.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from typing import Any
from weakref import WeakKeyDictionary

from pydantic import BaseModel, Field

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel, is_identifier
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

_MAX_CATEGORICAL_VALUES = 50
_TEXT_SAMPLE_CAP = 20
_CATEGORICAL_TYPES = (DataType.TEXT, DataType.BOOLEAN)
_NUMERIC_TEMPORAL_TYPES = (DataType.INT, DataType.DOUBLE, DataType.DATE, DataType.TIMESTAMP)
_ALL_NULL = "all NULL"
_TTL_SECONDS = 3600.0
_BREAKER_THRESHOLD = 3
_SAMPLE_FIELDS = frozenset({"sampled", "sampled_values", "distinct_count"})

# Injectable for tests.
_clock: Callable[[], float] = time.monotonic

_Key = tuple[str, ...]


class _Sample(BaseModel):
    sampled: str | None = None
    sampled_values: list[str] | None = None
    distinct_count: int | None = None


_NO_SAMPLE = _Sample()


class ProfileOutcome(BaseModel):
    """The input columns (refreshed where profiled, in input order) plus error strings."""

    columns: list[Column]
    errors: list[str] = Field(default_factory=list)


class _Entry(BaseModel):
    expires_at: float
    error: str | None = None
    sample: _Sample | None = None


class _EngineProfileState(BaseModel):
    failures: dict[_Key, _Entry] = Field(default_factory=dict)
    samples: dict[_Key, _Entry] = Field(default_factory=dict)

    def live(self, *, table: dict[_Key, _Entry], key: _Key, now: float) -> _Entry | None:
        entry = table.get(key)
        if entry is None:
            return None
        if entry.expires_at <= now:
            del table[key]
            return None
        return entry


_ENGINE_STATE: WeakKeyDictionary[SlayerQueryEngine, _EngineProfileState] = WeakKeyDictionary()


def _state_for(engine: SlayerQueryEngine) -> _EngineProfileState:
    state = _ENGINE_STATE.get(engine)
    if state is None:
        state = _EngineProfileState()
        _ENGINE_STATE[engine] = state
    return state


def _digest(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _column_fingerprint(column: Column) -> str:
    return _digest(column.model_dump(mode="json", exclude=set(_SAMPLE_FIELDS)))


def _model_fingerprint(model: SlayerModel) -> str:
    columns = sorted(
        (c.model_dump(mode="json", exclude=set(_SAMPLE_FIELDS)) for c in model.columns),
        key=lambda d: str(d.get("name")),
    )
    return _digest({"model": model.model_dump(mode="json", exclude={"columns"}), "columns": columns})


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


def _is_sample_cached(column: Column, *, model: SlayerModel) -> bool:
    """True when the persisted sample needs no re-profile; never-profiled columns count as cached."""
    if not _is_profilable(column, model=model):
        return True
    if column.type in _CATEGORICAL_TYPES:
        return column.sampled_values is not None
    return column.sampled is not None


def _is_profilable(column: Column, *, model: SlayerModel) -> bool:
    return (
        not column.hidden
        and not is_identifier(column=column, columns=model.columns)
        and (column.type in _CATEGORICAL_TYPES or column.type in _NUMERIC_TEMPORAL_TYPES)
    )


def _is_table_backed(model: SlayerModel) -> bool:
    """Only ``sql_table`` models take part in forced refresh."""
    return bool(model.sql_table) and not model.sql and not model.source_queries


async def _profile_categorical(
    *, model: SlayerModel, column: Column, engine: SlayerQueryEngine,
) -> _Sample:
    """Top values by frequency (value asc tie-break) in one scan; raises on query failure."""
    q = SlayerQuery.model_validate({
        "source_model": model.name,
        "dimensions": [{"name": column.name}],
        "measures": [{"formula": "count(*)"}],
        "order": [
            {"column": "_count", "direction": "desc"},
            {"column": column.name, "direction": "asc"},
        ],
        # +2 so one NULL row cannot push a non-overflow result over the cap.
        "limit": _MAX_CATEGORICAL_VALUES + 2,
    })
    r = await engine.execute(query=q, data_source=model.data_source or None)
    value_key = f"{model.name}.{column.name}"
    count_key = f"{model.name}._count"
    pairs = [(row.get(value_key), row.get(count_key)) for row in r.data if row.get(value_key) is not None]
    pairs.sort(key=lambda p: (-(p[1] or 0), str(p[0])))
    values = [str(v) for v, _ in pairs]
    if len(values) <= _MAX_CATEGORICAL_VALUES:
        return _Sample(
            sampled=", ".join(values[:_TEXT_SAMPLE_CAP]), sampled_values=values, distinct_count=len(values),
        )
    top = values[:_MAX_CATEGORICAL_VALUES]
    return _Sample(
        sampled=f"{', '.join(top[:_TEXT_SAMPLE_CAP])} ... ({_MAX_CATEGORICAL_VALUES}+ distinct)",
        sampled_values=top,
        distinct_count=None,
    )


async def _profile_numeric_temporal(
    *, model: SlayerModel, columns: list[Column], engine: SlayerQueryEngine,
) -> list[_Sample]:
    """One batched min/max query; raises on query failure."""
    # Untyped on purpose: a typed CAST coerces SQLite date strings to NUMERIC.
    ext_columns = [{"name": f"_slayer_range_{c.name}", "sql": c.sql or c.name} for c in columns]
    measures = [
        {"formula": f"{agg}(_slayer_range_{c.name})"} for c in columns for agg in ("min", "max")
    ]
    q = SlayerQuery.model_validate({
        "source_model": {"source_name": model.name, "columns": ext_columns},
        "measures": measures,
    })
    r = await engine.execute(query=q, data_source=model.data_source or None)
    row = r.data[0] if r.data else {}
    out: list[_Sample] = []
    for c in columns:
        mn = row.get(f"{model.name}._slayer_range_{c.name}_min")
        mx = row.get(f"{model.name}._slayer_range_{c.name}_max")
        out.append(_Sample(sampled=_ALL_NULL if mn is None and mx is None else f"{mn} .. {mx}"))
    return out


async def _probe(*, model: SlayerModel, engine: SlayerQueryEngine) -> Exception | None:
    """Row-count the model; the exception when it fails, else ``None``."""
    q = SlayerQuery.model_validate({"source_model": model.name, "measures": [{"formula": "count(*)"}]})
    try:
        await engine.execute(query=q, data_source=model.data_source or None)
    except Exception as exc:  # NOSONAR(S112) — the probe's failure IS the classification
        return exc
    return None


class _Target(BaseModel):
    index: int
    column: Column
    key: _Key


class _Run:
    """One owner call: classification state, results, errors."""

    def __init__(
        self,
        *,
        model: SlayerModel,
        engine: SlayerQueryEngine,
        storage: StorageBackend,
        state: _EngineProfileState,
        model_key: _Key,
        result: list[Column],
        pending: int,
    ) -> None:
        self.model = model
        self.engine = engine
        self.storage = storage
        self.state = state
        self.model_key = model_key
        self.result = result
        self.scoped = engine.policy is not None
        self.errors: list[str] = []
        self.healthy: bool | None = None
        self.model_failed = False
        self.streak: list[tuple[_Target, Exception]] = []
        self.pending = pending

    def _expiry(self) -> float:
        return _clock() + _TTL_SECONDS

    async def run(self, targets: list[_Target]) -> None:
        for t in (t for t in targets if t.column.type in _CATEGORICAL_TYPES):
            if self.model_failed:
                return
            try:
                sample = await _profile_categorical(model=self.model, column=t.column, engine=self.engine)
            except Exception as exc:  # NOSONAR(S112) — classified by _on_failure
                await self._on_failure(target=t, exc=exc)
                continue
            await self._on_success(target=t, sample=sample)
        numeric = [t for t in targets if t.column.type in _NUMERIC_TEMPORAL_TYPES]
        if numeric and not self.model_failed:
            await self._run_numeric(numeric)
        if not self.model_failed:
            self._flush_streak()

    async def _run_numeric(self, targets: list[_Target]) -> None:
        try:
            samples = await _profile_numeric_temporal(
                model=self.model, columns=[t.column for t in targets], engine=self.engine,
            )
        except Exception as exc:  # NOSONAR(S112) — classified, then isolated per column
            await self._on_failure(target=None, exc=exc)
            for t in targets:
                if self.model_failed:
                    return
                try:
                    [sample] = await _profile_numeric_temporal(
                        model=self.model, columns=[t.column], engine=self.engine,
                    )
                except Exception as col_exc:  # NOSONAR(S112) — classified by _on_failure
                    await self._on_failure(target=t, exc=col_exc)
                    continue
                await self._on_success(target=t, sample=sample)
            return
        for t, sample in zip(targets, samples, strict=True):
            await self._on_success(target=t, sample=sample)

    async def _on_failure(self, *, target: _Target | None, exc: Exception) -> None:
        if self.healthy is None:
            probe_exc = await _probe(model=self.model, engine=self.engine)
            self.healthy = probe_exc is None
            if probe_exc is not None:
                self._fail_model(exc=probe_exc)
                return
        if target is None:
            return
        self.streak.append((target, exc))
        if len(self.streak) >= _BREAKER_THRESHOLD:
            self._fail_model(exc=exc)

    def _fail_model(self, *, exc: Exception) -> None:
        msg = _first_line(exc)
        self.model_failed = True
        self.streak.clear()
        self.state.failures[self.model_key] = _Entry(expires_at=self._expiry(), error=msg)
        logger.warning(
            "sample profiling: model %s.%s unavailable (%d columns skipped): %s",
            self.model.data_source, self.model.name, self.pending, msg,
        )
        self.errors.append(f"{self.model.name}: profiling unavailable: {msg}")

    def _flush_streak(self) -> None:
        for target, exc in self.streak:
            msg = _first_line(exc)
            self.state.failures[target.key] = _Entry(expires_at=self._expiry(), error=msg)
            logger.warning(
                "sample profiling: column %s.%s.%s failed: %s",
                self.model.data_source, self.model.name, target.column.name, msg,
            )
            self.errors.append(f"{self.model.name}.{target.column.name}: {msg}")
        self.streak.clear()

    async def _on_success(self, *, target: _Target, sample: _Sample) -> None:
        self._flush_streak()
        self.pending -= 1
        self.state.failures.pop(target.key, None)
        self.state.failures.pop(self.model_key, None)
        self.result[target.index] = target.column.model_copy(update=sample.model_dump())
        if self.scoped:
            self.state.samples[target.key] = _Entry(expires_at=self._expiry(), sample=sample)
            return
        try:
            await self.storage.update_column_sampled(
                data_source=self.model.data_source,
                model_name=self.model.name,
                column_name=target.column.name,
                **sample.model_dump(),
            )
        except Exception as exc:  # NOSONAR(S112) — reported; the fresh value is still returned
            msg = _first_line(exc)
            logger.warning(
                "sample profiling: failed to persist %s.%s.%s: %s",
                self.model.data_source, self.model.name, target.column.name, msg,
            )
            self.errors.append(f"{self.model.name}.{target.column.name} (persist): {msg}")


def _served_from_cache(
    *,
    target: _Target,
    model: SlayerModel,
    state: _EngineProfileState,
    result: list[Column],
    now: float,
    scoped: bool,
) -> bool:
    """True when ``target`` needs no query; scoped sample hits are written into ``result``."""
    c = target.column
    if scoped:
        hit = state.live(table=state.samples, key=target.key, now=now)
        if hit is not None and hit.sample is not None:
            result[target.index] = c.model_copy(update=hit.sample.model_dump())
            return True
    elif _is_sample_cached(c, model=model):
        return True
    if state.live(table=state.failures, key=target.key, now=now) is not None:
        logger.debug("sample profiling: %s.%s.%s skipped (cached failure)", model.data_source, model.name, c.name)
        return True
    return False


async def ensure_samples_fresh(
    *,
    model: SlayerModel,
    columns: list[Column],
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    force: bool = False,
) -> ProfileOutcome:
    """Profile ``columns`` of ``model`` that need it; ``force`` ignores the sample and failure caches."""
    scoped = engine.policy is not None
    state = _state_for(engine)
    now = _clock()
    model_key: _Key = (model.data_source or "", model.name, _model_fingerprint(model))
    # Under a policy, stored samples are never surfaced.
    result = [c.model_copy(update=_NO_SAMPLE.model_dump()) for c in columns] if scoped else list(columns)
    targets = [
        _Target(index=i, column=c, key=(*model_key, c.name, _column_fingerprint(c)))
        for i, c in enumerate(columns) if _is_profilable(c, model=model)
    ]
    if not force:
        targets = [
            t for t in targets
            if not _served_from_cache(target=t, model=model, state=state, result=result, now=now, scoped=scoped)
        ]
        if targets and state.live(table=state.failures, key=model_key, now=now) is not None:
            logger.debug("sample profiling: %s.%s skipped (cached model failure)", model.data_source, model.name)
            return ProfileOutcome(columns=result)
    if not targets:
        return ProfileOutcome(columns=result)
    run = _Run(
        model=model, engine=engine, storage=storage, state=state,
        model_key=model_key, result=result, pending=len(targets),
    )
    await run.run(targets)
    return ProfileOutcome(columns=run.result, errors=run.errors)


async def refresh_table_backed_model_sampled(
    *,
    model: SlayerModel,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    only_columns: set[str] | None = None,
) -> list[str]:
    """Force-refresh samples of a table-backed model (others skipped); returns error strings."""
    if not _is_table_backed(model):
        return []
    columns = [c for c in model.columns if only_columns is None or c.name in only_columns]
    outcome = await ensure_samples_fresh(
        model=model, columns=columns, engine=engine, storage=storage, force=True,
    )
    return outcome.errors


async def refresh_all_table_backed_sampled(
    *,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    data_source: str,
) -> list[str]:
    """Force-refresh every table-backed model in ``data_source``; returns error strings."""
    errors: list[str] = []
    for ds, name in await storage._list_all_model_identities():
        if ds != data_source:
            continue
        model = await storage.get_model(name, data_source=ds)
        if model is None:
            continue
        errors.extend(await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage))
    return errors
