"""Sample-value profiling of ``DatasetField.sampled`` / ``sampled_values`` / ``distinct_count``.

:func:`ensure_samples_fresh` is the one owner every read path and forced refresh goes through.
Categorical fields: top 50 values by frequency in one scan (overflow keeps the top 50, total
unknown). Numeric/temporal fields: one batched min/max query. Failures are classified by a
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

from slayer.core.enums import DataType, NUMERIC_DATATYPES, TEMPORAL_DATATYPES
from slayer.core.models import DatasetField, SlayerDataset, is_identifier
from slayer.core.names import qualified_reference
from slayer.core.query import SlayerQuery
from slayer.engine.binding import range_read
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures

logger = logging.getLogger(__name__)

_MAX_CATEGORICAL_VALUES = 50
_TEXT_SAMPLE_CAP = 20
_CATEGORICAL_TYPES = (DataType.STRING, DataType.BOOLEAN)
_NUMERIC_TEMPORAL_TYPES = NUMERIC_DATATYPES | TEMPORAL_DATATYPES
_ALL_NULL = "all NULL"
_TTL_SECONDS = 3600.0
_BREAKER_THRESHOLD = 3
_SAMPLE_FIELDS = frozenset({"sampled", "sampled_values", "distinct_count"})
_COUNT = "_count"

# Injectable for tests.
_clock: Callable[[], float] = time.monotonic

_Key = tuple[str, ...]


class _Sample(BaseModel):
    sampled: str | None = None
    sampled_values: list[str] | None = None
    distinct_count: int | None = None


_NO_SAMPLE = _Sample()


class ProfileOutcome(BaseModel):
    """The input fields (refreshed where profiled, in input order) plus error strings."""

    fields: list[DatasetField]
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


def _field_fingerprint(field: DatasetField) -> str:
    return _digest(field.model_dump(mode="json", exclude=set(_SAMPLE_FIELDS)))


def _dataset_fingerprint(dataset: SlayerDataset) -> str:
    fields = sorted(
        (c.model_dump(mode="json", exclude=set(_SAMPLE_FIELDS)) for c in dataset.fields),
        key=lambda d: str(d.get("name")),
    )
    return _digest({"dataset": dataset.model_dump(mode="json", exclude={"fields"}), "fields": fields})


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


def _profile_type(field: DatasetField) -> DataType | None:
    """The type a field is profiled as: a list's element type (its elements are sampled), else its own."""
    return field.element_type if field.datatype is DataType.ARRAY else field.datatype


def _values_ref(field: DatasetField, *, dataset: SlayerDataset) -> str:
    """The reference whose values profile ``field``: a list's ``<field>.value``."""
    parts = (dataset.name, field.name, "value") if field.datatype is DataType.ARRAY else (dataset.name, field.name)
    return qualified_reference(*parts)


def _is_sample_cached(field: DatasetField, *, dataset: SlayerDataset) -> bool:
    """True when the persisted sample needs no re-profile; never-profiled fields count as cached."""
    if not _is_profilable(field, dataset=dataset):
        return True
    if _profile_type(field) in _CATEGORICAL_TYPES:
        return field.sampled_values is not None
    return field.sampled is not None


def _is_profilable(field: DatasetField, *, dataset: SlayerDataset) -> bool:
    kind = _profile_type(field)
    return (
        not field.hidden
        and not is_identifier(field=field, fields=dataset.fields)
        and (kind in _CATEGORICAL_TYPES or kind in _NUMERIC_TEMPORAL_TYPES)
    )


def _is_table_backed(dataset: SlayerDataset) -> bool:
    """Only ``sql_table`` datasets take part in forced refresh."""
    return bool(dataset.sql_table) and not dataset.sql and not dataset.source_queries


async def _profile_categorical(
    *, dataset: SlayerDataset, field: DatasetField, engine: SlayerQueryEngine,
) -> _Sample:
    """Top values (a list's elements) by frequency (value asc tie-break) in one scan; raises on query failure."""
    ref = _values_ref(field, dataset=dataset)
    owner = (dataset.name, field.name) if field.datatype is DataType.ARRAY else (dataset.name,)
    counted = f"count({qualified_reference(*owner)}.*)"
    q = SlayerQuery.model_validate({
        "population": dataset.name,
        "dimensions": [ref],
        "metrics": [{"metric": counted, "alias": _COUNT}],
        "order": [
            {"field": _COUNT, "direction": "desc"},
            {"field": ref, "direction": "asc"},
        ],
        # +2 so one NULL row cannot push a non-overflow result over the cap.
        "limit": _MAX_CATEGORICAL_VALUES + 2,
    })
    r = await engine.execute(query=q, data_source=dataset.data_source or None)
    value_key = ".".join((dataset.name, field.name, "value") if field.datatype is DataType.ARRAY
                         else (dataset.name, field.name))
    count_key = _COUNT
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
    *, dataset: SlayerDataset, fields: list[DatasetField], engine: SlayerQueryEngine,
) -> list[_Sample]:
    """One batched min/max query (a list's over its elements); raises on query failure."""
    metrics = [
        {"metric": f"{agg}({_values_ref(c, dataset=dataset)})", "alias": f"_slayer_range_{c.name}_{agg}"}
        for c in fields for agg in ("min", "max")
    ]
    q = SlayerQuery.model_validate({"population": dataset.name, "metrics": metrics})
    with range_read():
        r = await engine.execute(query=q, data_source=dataset.data_source or None)
    row = r.data[0] if r.data else {}
    out: list[_Sample] = []
    for c in fields:
        mn = row.get(f"_slayer_range_{c.name}_min")
        mx = row.get(f"_slayer_range_{c.name}_max")
        out.append(_Sample(sampled=_ALL_NULL if mn is None and mx is None else f"{mn} .. {mx}"))
    return out


async def _probe(*, dataset: SlayerDataset, engine: SlayerQueryEngine) -> Exception | None:
    """Row-count the dataset; the exception when it fails, else ``None``."""
    q = SlayerQuery.model_validate({"population": dataset.name,
                                    "metrics": [f"count({qualified_reference(dataset.name)}.*)"]})
    try:
        await engine.execute(query=q, data_source=dataset.data_source or None)
    except Exception as exc:  # NOSONAR(S112) — the probe's failure IS the classification
        return exc
    return None


class _Target(BaseModel):
    index: int
    field: DatasetField
    key: _Key


class _Run:
    """One owner call: classification state, results, errors."""

    def __init__(
        self,
        *,
        dataset: SlayerDataset,
        engine: SlayerQueryEngine,
        storage: StorageBackend,
        state: _EngineProfileState,
        dataset_key: _Key,
        result: list[DatasetField],
        pending: int,
    ) -> None:
        self.dataset = dataset
        self.engine = engine
        self.storage = storage
        self.state = state
        self.model_key = dataset_key
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
        for t in (t for t in targets if _profile_type(t.field) in _CATEGORICAL_TYPES):
            if self.model_failed:
                return
            try:
                sample = await _profile_categorical(dataset=self.dataset, field=t.field, engine=self.engine)
            except Exception as exc:  # NOSONAR(S112) — classified by _on_failure
                await self._on_failure(target=t, exc=exc)
                continue
            await self._on_success(target=t, sample=sample)
        numeric = [t for t in targets if _profile_type(t.field) in _NUMERIC_TEMPORAL_TYPES]
        if numeric and not self.model_failed:
            await self._run_numeric(numeric)
        if not self.model_failed:
            self._flush_streak()

    async def _run_numeric(self, targets: list[_Target]) -> None:
        try:
            samples = await _profile_numeric_temporal(
                dataset=self.dataset, fields=[t.field for t in targets], engine=self.engine,
            )
        except Exception as exc:  # NOSONAR(S112) — classified, then isolated per field
            await self._on_failure(target=None, exc=exc)
            for t in targets:
                if self.model_failed:
                    return
                try:
                    [sample] = await _profile_numeric_temporal(
                        dataset=self.dataset, fields=[t.field], engine=self.engine,
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
            probe_exc = await _probe(dataset=self.dataset, engine=self.engine)
            self.healthy = probe_exc is None
            if probe_exc is not None:
                self._fail_dataset(exc=probe_exc)
                return
        if target is None:
            return
        self.streak.append((target, exc))
        if len(self.streak) >= _BREAKER_THRESHOLD:
            self._fail_dataset(exc=exc)

    def _fail_dataset(self, *, exc: Exception) -> None:
        msg = _first_line(exc)
        self.model_failed = True
        self.streak.clear()
        self.state.failures[self.model_key] = _Entry(expires_at=self._expiry(), error=msg)
        logger.warning(
            "sample profiling: dataset %s.%s unavailable (%d fields skipped): %s",
            self.dataset.data_source, self.dataset.name, self.pending, msg,
        )
        self.errors.append(f"{self.dataset.name}: profiling unavailable: {msg}")

    def _flush_streak(self) -> None:
        for target, exc in self.streak:
            msg = _first_line(exc)
            self.state.failures[target.key] = _Entry(expires_at=self._expiry(), error=msg)
            logger.warning(
                "sample profiling: field %s.%s.%s failed: %s",
                self.dataset.data_source, self.dataset.name, target.field.name, msg,
            )
            self.errors.append(f"{self.dataset.name}.{target.field.name}: {msg}")
        self.streak.clear()

    async def _on_success(self, *, target: _Target, sample: _Sample) -> None:
        self._flush_streak()
        self.pending -= 1
        self.state.failures.pop(target.key, None)
        self.state.failures.pop(self.model_key, None)
        self.result[target.index] = target.field.model_copy(update=sample.model_dump())
        if self.scoped:
            self.state.samples[target.key] = _Entry(expires_at=self._expiry(), sample=sample)
            return
        try:
            await self.storage.update_field_sampled(
                data_source=self.dataset.data_source,
                dataset_name=self.dataset.name,
                field_name=target.field.name,
                **sample.model_dump(),
            )
        except Exception as exc:  # NOSONAR(S112) — reported; the fresh value is still returned
            msg = _first_line(exc)
            logger.warning(
                "sample profiling: failed to persist %s.%s.%s: %s",
                self.dataset.data_source, self.dataset.name, target.field.name, msg,
            )
            self.errors.append(f"{self.dataset.name}.{target.field.name} (persist): {msg}")


def _served_from_cache(
    *,
    target: _Target,
    dataset: SlayerDataset,
    state: _EngineProfileState,
    result: list[DatasetField],
    now: float,
    scoped: bool,
) -> bool:
    """True when ``target`` needs no query; scoped sample hits are written into ``result``."""
    c = target.field
    if scoped:
        hit = state.live(table=state.samples, key=target.key, now=now)
        if hit is not None and hit.sample is not None:
            result[target.index] = c.model_copy(update=hit.sample.model_dump())
            return True
    elif _is_sample_cached(c, dataset=dataset):
        return True
    if state.live(table=state.failures, key=target.key, now=now) is not None:
        logger.debug("sample profiling: %s.%s.%s skipped (cached failure)", dataset.data_source, dataset.name, c.name)
        return True
    return False


async def ensure_samples_fresh(
    *,
    dataset: SlayerDataset,
    fields: list[DatasetField],
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    force: bool = False,
) -> ProfileOutcome:
    """Profile ``fields`` of ``dataset`` that need it; ``force`` ignores the sample and failure caches."""
    scoped = engine.policy is not None
    state = _state_for(engine)
    now = _clock()
    dataset_key: _Key = (dataset.data_source or "", dataset.name, _dataset_fingerprint(dataset))
    # Under a policy, stored samples are never surfaced.
    result = [c.model_copy(update=_NO_SAMPLE.model_dump()) for c in fields] if scoped else list(fields)
    targets = [
        _Target(index=i, field=c, key=(*dataset_key, c.name, _field_fingerprint(c)))
        for i, c in enumerate(fields) if _is_profilable(c, dataset=dataset)
    ]
    if not force:
        targets = [
            t for t in targets
            if not _served_from_cache(target=t, dataset=dataset, state=state, result=result, now=now, scoped=scoped)
        ]
        if targets and state.live(table=state.failures, key=dataset_key, now=now) is not None:
            logger.debug("sample profiling: %s.%s skipped (cached dataset failure)", dataset.data_source, dataset.name)
            return ProfileOutcome(fields=result)
    if not targets:
        return ProfileOutcome(fields=result)
    run = _Run(
        dataset=dataset, engine=engine, storage=storage, state=state,
        dataset_key=dataset_key, result=result, pending=len(targets),
    )
    await run.run(targets)
    return ProfileOutcome(fields=run.result, errors=run.errors)


async def refresh_table_backed_dataset_sampled(
    *,
    dataset: SlayerDataset,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    only_fields: set[str] | None = None,
) -> list[str]:
    """Force-refresh samples of a table-backed dataset (others skipped); returns error strings."""
    if not _is_table_backed(dataset):
        return []
    fields = [c for c in dataset.fields if only_fields is None or c.name in only_fields]
    outcome = await ensure_samples_fresh(
        dataset=dataset, fields=fields, engine=engine, storage=storage, force=True,
    )
    return outcome.errors


async def refresh_all_table_backed_sampled(
    *,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    data_source: str,
) -> list[str]:
    """Force-refresh every table-backed dataset in ``data_source``; returns error strings."""
    errors: list[str] = []
    for dataset in DocumentLoadFailures().skip(await storage.load_datasets(data_source=data_source)):
        errors.extend(await refresh_table_backed_dataset_sampled(dataset=dataset, engine=engine, storage=storage))
    return errors
