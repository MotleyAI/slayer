"""Stored datasource granularities colliding with a library name are renamed (spec: models/stored-upgrade)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import stat
import warnings
from collections.abc import Callable, Generator, Sequence
from typing import Any

import pytest

from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerResponse
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from tests._granularity_rename_fixtures import (
    BACKENDS,
    DS,
    EXPECTED_BUCKETS,
    OTHER_DS,
    SUM,
    bucket_stage,
    bucket_totals,
    datasource_doc,
    events_doc,
    granularity_definition,
    live_db,
    memory_doc,
    query_backed_doc,
    raw_documents,
    reopen,
    stage_doc,
    write_raw_store,
)
from tests._stored_upgrade_fixtures import run

#: Calendar-month buckets of ``sum(amount)``.
CALENDAR_BUCKETS = {"2025-01-01": 1.0, "2025-02-01": 6.0, "2025-03-01": 8.0}
CONTROL = "mid_month"


class _Store:
    def __init__(self, *, storage: StorageBackend, backend: str, base: str) -> None:
        self.storage = storage
        self.backend = backend
        self.base = base

    def raw(self) -> dict[str, str]:
        return raw_documents(backend=self.backend, base=self.base)


async def _store(
    *, tmp_path: Any, backend: str, granularities: Sequence[dict], models: Sequence[dict] = (),
    memories: Sequence[dict] = (), other: bool = False, label: str = "store",
) -> _Store:
    """``granularities`` are full definitions; ``other`` adds a plain ``OTHER_DS`` over the same database."""
    base = str(tmp_path / f"{label}-{backend}")
    db_path = live_db(base)
    ds = {**datasource_doc(db_path=db_path), "granularities": list(granularities)}
    datasources = [ds, datasource_doc(db_path=db_path, name=OTHER_DS)] if other else [ds]
    storage = await write_raw_store(backend=backend, base=base, datasources=datasources, models=models,
                                    memories=memories)
    return _Store(storage=storage, backend=backend, base=base)


def _mentions(text: str, name: str) -> bool:
    """Whether ``text`` uses ``name`` as a word (relative-token plural included)."""
    return re.search(rf"\b{re.escape(name)}s?\b", text, re.IGNORECASE) is not None


async def _granularity_names(storage: StorageBackend, name: str = DS) -> list[str]:
    ds = await storage.get_datasource(name)
    assert ds is not None
    return [g.name for g in ds.granularities]


async def _stage(storage: StorageBackend, name: str) -> SlayerQuery:
    model = await storage.get_model(name, data_source=DS)
    assert model is not None
    [stage] = model.source_queries or []
    return stage


async def _memory_query(storage: StorageBackend, memory_id: str) -> SlayerQuery:
    mem = await storage.get_memory(memory_id)
    assert mem.query is not None
    return mem.query


def _td_granularity(query: SlayerQuery) -> str:
    [td] = query.time_dimensions or []
    return str(td.granularity)


def _totals(resp: SlayerResponse) -> dict[str, float]:
    return {str(r["events.created_at"])[:10]: float(r["events.s"]) for r in resp.data}


def _rows(resp: SlayerResponse) -> list[tuple]:
    return [tuple(r.values()) for r in resp.data]


@contextlib.contextmanager
def _captured(caplog: pytest.LogCaptureFixture) -> Generator[list[str]]:
    """Text of every warning raised or logged inside the block."""
    texts: list[str] = []
    with caplog.at_level(logging.WARNING), warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        yield texts
    texts.extend(r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)
    texts.extend(str(w.message) for w in recorded)


def _names_rename(texts: list[str], *, old: str, new: str) -> bool:
    return any(DS in t and new in t and _mentions(t.replace(new, ""), old) for t in texts)


def test_datasource_version_bumped() -> None:
    assert CURRENT_VERSIONS["DatasourceConfig"] == 3


class TestLeft:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("first", ["datasource", "model", "memory"])
    async def test_renamed_with_every_reference(
        self, tmp_path: Any, caplog: pytest.LogCaptureFixture, backend: str, first: str,
    ) -> None:
        store = await _store(
            tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("left")],
            models=[events_doc(), query_backed_doc(name="bucketed", stage=bucket_stage("left"))],
            memories=[memory_doc(memory_id="m", query=bucket_stage("left"))],
        )
        loaders: dict[str, Callable[[], Any]] = {
            "datasource": lambda: store.storage.get_datasource(DS),
            "model": lambda: store.storage.get_model("bucketed", data_source=DS),
            "memory": lambda: store.storage.get_memory("m"),
        }
        with _captured(caplog) as texts:
            await loaders[first]()
            names = await _granularity_names(store.storage)
            stage = await _stage(store.storage, "bucketed")
            mem_query = await _memory_query(store.storage, "m")
        assert names == ["left_granularity"]
        assert _td_granularity(stage) == "left_granularity"
        assert _td_granularity(mem_query) == "left_granularity"
        assert _names_rename(texts, old="left", new="left_granularity"), texts
        assert await bucket_totals(store.storage) == EXPECTED_BUCKETS
        assert _totals(await run(store.storage, mem_query)) == EXPECTED_BUCKETS

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_other_datasource_untouched(self, tmp_path: Any, backend: str) -> None:
        initials = {"name": "initials", "formula": "count_distinct(left(status, 1))"}
        other_query = stage_doc(source_model="other_events", measures=[SUM],
                                dimensions=[{"expression": "left(status, 1)", "name": "initial"}])
        store = await _store(
            tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("left")], other=True,
            models=[events_doc(), events_doc(name="other_events", data_source=OTHER_DS, measures=[initials])],
            memories=[memory_doc(memory_id="m_other", query=other_query)],
        )
        assert await _granularity_names(store.storage) == ["left_granularity"]
        other = await store.storage.get_model("other_events", data_source=OTHER_DS)
        assert other is not None
        assert [m.formula for m in other.measures] == ["count_distinct(left(status, 1))"]
        mem_query = await _memory_query(store.storage, "m_other")
        assert [getattr(d, "expression", None) for d in mem_query.dimensions or []] == ["left(status, 1)"]
        resp = await run(store.storage, {"source_model": "other_events", "measures": ["initials"]})
        assert [int(v) for v in resp.data[0].values()] == [2]
        by_initial = {r["other_events.initial"]: float(r["other_events.s"]) for r in (await run(store.storage, mem_query)).data}
        assert by_initial == {"o": 5.0, "n": 10.0}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_collect(tmp_path: Any, backend: str) -> None:
    store = await _store(
        tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("collect")],
        models=[events_doc(), query_backed_doc(name="bucketed", stage=bucket_stage("collect"))],
    )
    assert await _granularity_names(store.storage) == ["collect_granularity"]
    assert _td_granularity(await _stage(store.storage, "bucketed")) == "collect_granularity"
    assert await bucket_totals(store.storage) == EXPECTED_BUCKETS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_current_version_store_colliding_with_a_later_addition(tmp_path: Any, backend: str) -> None:
    def current(doc: dict, entity: str) -> dict:
        return {**doc, "version": CURRENT_VERSIONS[entity]}

    stage = current(stage_doc(dimensions=["left(created_at)"], measures=[SUM]), "SlayerQuery")
    storage = await write_raw_store(
        backend=backend, base=str(tmp_path),
        datasources=[current(datasource_doc(db_path=live_db(str(tmp_path)), granularities=["left"]),
                             "DatasourceConfig")],
        models=[current(events_doc(), "SlayerModel"),
                current(query_backed_doc(name="bucketed", stage=stage), "SlayerModel")],
    )
    assert await _granularity_names(storage) == ["left_granularity"]
    loaded = await _stage(storage, "bucketed")
    assert loaded.dimensions is not None
    assert [getattr(d, "expression", d) for d in loaded.dimensions] == ["left_granularity(created_at)"]
    resp = await run(storage, {"source_model": "bucketed", "dimensions": ["created_at"], "measures": ["sum(s)"]})
    assert {str(r["bucketed.created_at"])[:10]: float(r["bucketed.s_sum"]) for r in resp.data} == EXPECTED_BUCKETS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_case_insensitive_collision(tmp_path: Any, backend: str) -> None:
    store = await _store(
        tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("Left")],
        models=[events_doc(), query_backed_doc(name="bucketed", stage=bucket_stage("Left"))],
    )
    assert [n.lower() for n in await _granularity_names(store.storage)] == ["left_granularity"]
    assert _td_granularity(await _stage(store.storage, "bucketed")).lower() == "left_granularity"
    assert await bucket_totals(store.storage) == EXPECTED_BUCKETS


def _calendar(name: str) -> dict:
    return {"name": name, "base": "month"}


class TestTakenName:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize(("taken", "expected"), [
        (["left_granularity"], "left_granularity_2"),
        (["left_granularity", "left_granularity_2"], "left_granularity_3"),
        (["LEFT_GRANULARITY"], "left_granularity_2"),
    ], ids=["suffix-2", "suffix-3", "taken-case-insensitively"])
    async def test_smallest_free_suffix(self, tmp_path: Any, backend: str, taken: list[str], expected: str) -> None:
        store = await _store(
            tmp_path=tmp_path, backend=backend,
            granularities=[granularity_definition("left"), *(_calendar(t) for t in taken)],
            models=[events_doc(), query_backed_doc(name="by_left", stage=bucket_stage("left")),
                    query_backed_doc(name="by_taken", stage=bucket_stage(taken[0]))],
        )
        assert sorted(await _granularity_names(store.storage)) == sorted([expected, *taken])
        assert _td_granularity(await _stage(store.storage, "by_left")) == expected
        assert _td_granularity(await _stage(store.storage, "by_taken")) == taken[0]
        assert await bucket_totals(store.storage, model="by_left") == EXPECTED_BUCKETS
        assert await bucket_totals(store.storage, model="by_taken") == CALENDAR_BUCKETS


# Every position of a stored query that accepts a granularity name.
QUERY_POSITIONS: dict[str, Callable[[str], dict]] = {
    "time-dimension": lambda g: {"time_dimensions": [{"dimension": "created_at", "granularity": g}], "measures": [SUM]},
    "time-dimension-call": lambda g: {"time_dimensions": [f"{g}(created_at)"], "measures": [SUM]},
    "dimension-call": lambda g: {"dimensions": [f"{g}(created_at)"], "measures": [SUM]},
    "computed-dimension": lambda g: {"dimensions": [{"expression": f"{g}(created_at)", "name": "b"}], "measures": [SUM]},
    "colon-dimension": lambda g: {"dimensions": [f"created_at:{g}"], "measures": [SUM]},
    "measure": lambda g: {"measures": [{"formula": f"count_distinct({g}(created_at))", "name": "n"}]},
    "measure-filter-clause": lambda g: {
        "measures": [{"formula": f"sum(amount) FILTER (WHERE {g}(created_at) = '2025-02-15')", "name": "f"}]},
    "filter": lambda g: {"measures": [SUM], "filters": [f"{g}(created_at) = '2025-01-15'"]},
    "order": lambda g: {"time_dimensions": [{"dimension": "created_at", "granularity": g}], "measures": [SUM],
                        "order": [{"column": f"{g}(created_at)", "direction": "desc"}]},
    "relative-token-filter": lambda g: {"measures": [SUM], "filters": [f"created_at >= 'last 1000 {g}s'"]},
    "relative-token-filter-singular": lambda g: {"measures": [SUM], "filters": [f"created_at < 'this {g}'"]},
    "relative-token-date-range": lambda g: {
        "time_dimensions": [{"dimension": "created_at", "granularity": "month", "date_range": f"last 1000 {g}s"}],
        "measures": [SUM]},
    "time-shift-unit": lambda g: {
        "time_dimensions": [{"dimension": "created_at", "granularity": g}],
        "measures": [SUM, {"formula": f"time_shift(sum(amount), -1, '{g}')", "name": "p"}]},
    "time-shift-keyword": lambda g: {
        "time_dimensions": [{"dimension": "created_at", "granularity": g}],
        "measures": [SUM, {"formula": f"time_shift(sum(amount), -1, granularity='{g}')", "name": "p"}]},
    "inline-extension-measure": lambda g: {
        "source_model": {"source_name": "events", "measures": [{"name": "n", "formula": f"count_distinct({g}(created_at))"}]},
        "measures": ["n"]},
}

# Every position of a stored model that accepts a granularity name: (model document, query over it).
MODEL_POSITIONS: dict[str, tuple[Callable[[str], dict], Callable[[str], dict]]] = {
    "column-granularity": (
        lambda g: events_doc(name="dep", column_granularity=g),
        lambda g: {"source_model": "dep", "measures": [SUM],
                   "time_dimensions": [{"dimension": "created_at", "granularity": g}]},
    ),
    "model-measure": (
        lambda g: events_doc(name="dep", measures=[{"name": "n", "formula": f"count_distinct({g}(created_at))"}]),
        lambda _: {"source_model": "dep", "measures": ["n"]},
    ),
}


async def _query_dependant(*, tmp_path: Any, backend: str, kind: str, position: str, g: str, label: str) -> tuple[
        _Store, str, SlayerQuery]:
    """A store whose ``kind`` dependant holds ``position`` over granularity ``g``; the loaded dump and query."""
    query = stage_doc(**QUERY_POSITIONS[position](g))
    models, memories = [events_doc()], []
    if kind == "query-backed-model":
        models.append(query_backed_doc(name="dep", stage=query))
    else:
        memories.append(memory_doc(memory_id="dep", query=query))
    store = await _store(tmp_path=tmp_path, backend=backend, granularities=[granularity_definition(g)],
                         models=models, memories=memories, label=label)
    if kind == "query-backed-model":
        model = await store.storage.get_model("dep", data_source=DS)
        assert model is not None
        [loaded] = model.source_queries or []
        return store, json.dumps(model.model_dump(mode="json")), loaded
    loaded = await _memory_query(store.storage, "dep")
    return store, json.dumps(loaded.model_dump(mode="json")), loaded


class TestEveryPosition:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("kind", ["query-backed-model", "memory"])
    @pytest.mark.parametrize("position", list(QUERY_POSITIONS))
    async def test_query_position(self, tmp_path: Any, backend: str, kind: str, position: str) -> None:
        control, _, control_query = await _query_dependant(
            tmp_path=tmp_path, backend=backend, kind=kind, position=position, g=CONTROL, label="control")
        expected = _rows(await run(control.storage, control_query))
        store, dumped, loaded = await _query_dependant(
            tmp_path=tmp_path, backend=backend, kind=kind, position=position, g="left", label="store")
        assert "left_granularity" in dumped, dumped
        assert not _mentions(dumped, "left"), dumped
        assert _rows(await run(store.storage, loaded)) == expected

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("position", list(MODEL_POSITIONS))
    async def test_model_position(self, tmp_path: Any, backend: str, position: str) -> None:
        doc_fn, query_fn = MODEL_POSITIONS[position]
        control = await _store(tmp_path=tmp_path, backend=backend, granularities=[granularity_definition(CONTROL)],
                               models=[doc_fn(CONTROL)], label="control")
        expected = _rows(await run(control.storage, query_fn(CONTROL)))
        store = await _store(tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("left")],
                             models=[doc_fn("left")], label="store")
        model = await store.storage.get_model("dep", data_source=DS)
        assert model is not None
        dumped = json.dumps(model.model_dump(mode="json"))
        assert "left_granularity" in dumped, dumped
        assert not _mentions(dumped, "left"), dumped
        assert _rows(await run(store.storage, query_fn("left_granularity"))) == expected


@pytest.mark.parametrize("backend", BACKENDS)
async def test_memory_over_an_inline_model_is_renamed(tmp_path: Any, backend: str) -> None:
    inline = {k: v for k, v in events_doc(name="inline_events").items() if k != "version"}
    query = stage_doc(source_model=inline, time_dimensions=[{"dimension": "created_at", "granularity": "left"}],
                      measures=[SUM])
    store = await _store(tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("left")],
                         models=[events_doc()], memories=[memory_doc(memory_id="m", query=query)])
    await store.storage.get_datasource(DS)
    assert "left_granularity" in store.raw()["memories/m"]
    mem_query = await _memory_query(store.storage, "m")
    assert _td_granularity(mem_query) == "left_granularity"
    rows = (await run(store.storage, mem_query)).data
    assert {str(r["inline_events.created_at"])[:10]: float(r["inline_events.s"]) for r in rows} == EXPECTED_BUCKETS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_concurrent_first_loads_all_see_the_rename(tmp_path: Any, backend: str) -> None:
    store = await _store(
        tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("left")],
        models=[events_doc(), query_backed_doc(name="bucketed", stage=bucket_stage("left"))],
        memories=[memory_doc(memory_id="m", query=bucket_stage("left"))],
    )
    loads = await asyncio.gather(
        *(store.storage.get_model("bucketed", data_source=DS) for _ in range(4)),
        *(store.storage.get_memory("m") for _ in range(4)),
    )
    for loaded in loads:
        assert loaded is not None
        query = loaded.query if isinstance(loaded, Memory) else (loaded.source_queries or [None])[0]
        assert query is not None
        assert _td_granularity(query) == "left_granularity"


def _current(doc: dict, entity: str) -> dict:
    return {**doc, "version": CURRENT_VERSIONS[entity]}


def _assert_consistent(raw: dict[str, str]) -> None:
    for key, text in raw.items():
        assert not _mentions(text, "left"), (key, text)
        assert "left_granularity_granularity" not in text, (key, text)
    assert "left_granularity" in raw[f"datasources/{DS}"]


class TestInterruptedLoad:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_half_migrated_store_completes(self, tmp_path: Any, backend: str) -> None:
        done_stage = _current(bucket_stage("left_granularity"), "SlayerQuery")
        store = await _store(
            tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("left")],
            models=[events_doc(),
                    _current(query_backed_doc(name="done", stage=done_stage), "SlayerModel"),
                    query_backed_doc(name="pending", stage=bucket_stage("left"))],
            memories=[_current(memory_doc(memory_id="m_done", query=done_stage), "Memory"),
                      memory_doc(memory_id="m_pending", query=bucket_stage("left"))],
        )
        assert await _granularity_names(store.storage) == ["left_granularity"]
        for name in ("done", "pending"):
            assert _td_granularity(await _stage(store.storage, name)) == "left_granularity"
            assert await bucket_totals(store.storage, model=name) == EXPECTED_BUCKETS
        for memory_id in ("m_done", "m_pending"):
            query = await _memory_query(store.storage, memory_id)
            assert _td_granularity(query) == "left_granularity"
            assert _totals(await run(store.storage, query)) == EXPECTED_BUCKETS
        _assert_consistent(store.raw())

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_failed_datasource_write_converges_on_the_next_load(self, tmp_path: Any, backend: str) -> None:
        store = await _store(
            tmp_path=tmp_path, backend=backend, granularities=[granularity_definition("left")],
            models=[events_doc(), query_backed_doc(name="bucketed", stage=bucket_stage("left"))],
            memories=[memory_doc(memory_id="m", query=bucket_stage("left"))],
        )
        with _datasource_writes_fail(backend=backend, base=store.base), contextlib.suppress(Exception):
            await store.storage.get_datasource(DS)
        raw = store.raw()
        assert _mentions(raw[f"datasources/{DS}"], "left"), "the datasource write must have failed"
        for key in (f"models/{DS}/bucketed", "memories/m"):
            assert "left_granularity" in raw[key], (key, raw[key])
            assert not _mentions(raw[key], "left"), (key, raw[key])

        storage = reopen(backend=backend, base=store.base)
        assert await _granularity_names(storage) == ["left_granularity"]
        assert await bucket_totals(storage) == EXPECTED_BUCKETS
        assert _totals(await run(storage, await _memory_query(storage, "m"))) == EXPECTED_BUCKETS
        _assert_consistent(store.raw())


@contextlib.contextmanager
def _datasource_writes_fail(*, backend: str, base: str) -> Generator[None]:
    """Every datasource write inside the block fails; other documents stay writable."""
    if backend == "yaml":
        if os.geteuid() == 0:
            pytest.skip("root ignores directory permissions")
        ds_dir = os.path.join(base, "datasources")
        mode = os.stat(ds_dir).st_mode
        os.chmod(ds_dir, stat.S_IRUSR | stat.S_IXUSR)
        try:
            yield
        finally:
            os.chmod(ds_dir, mode)
        return
    db_path = os.path.join(base, "store.db")
    with transaction(db_path) as conn:
        for event in ("INSERT", "UPDATE", "DELETE"):
            conn.execute(f"CREATE TRIGGER fail_ds_{event.lower()} BEFORE {event} ON datasources "
                         "BEGIN SELECT RAISE(ABORT, 'injected datasource write failure'); END")
    try:
        yield
    finally:
        with transaction(db_path) as conn:
            for event in ("insert", "update", "delete"):
                conn.execute(f"DROP TRIGGER fail_ds_{event}")


async def test_a_new_reserved_granularity_is_refused(tmp_path: Any) -> None:
    store = await _store(tmp_path=tmp_path, backend="yaml", granularities=[])
    ds = DatasourceConfig.model_validate({"name": "fresh", "type": "sqlite", "database": ":memory:",
                                          "granularities": [granularity_definition("left")]})
    with pytest.raises(SlayerError) as exc:
        await store.storage.save_datasource(ds)
    assert "left" in str(exc.value)
    assert await store.storage.get_datasource("fresh") is None
