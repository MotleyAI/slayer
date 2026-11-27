"""A renamed granularity records its former name; the rename applies only to documents written below its gate.

Spec: models/stored-upgrade › Stored granularities colliding with a library name are renamed; › slayer storage
upgrade persists the current format.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.query import SlayerQuery
from slayer.core.vocabulary import stored_query
from slayer.storage import granularity_rename
from slayer.storage.base import StorageBackend
from slayer.storage.document_keys import CURRENT_KEYS, dataset_in
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.upgrade import upgrade_store
from slayer.storage.yaml_storage import YAMLStorage
from tests._granularity_rename_fixtures import (
    BACKENDS,
    DS,
    EXPECTED_BUCKETS,
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
from tests._storage_upgrade_cli import store_path, upgrade
from tests._stored_upgrade_fixtures import run

RECORDED_DATASOURCE_VERSION = 4
CALENDAR_BUCKETS = {"2025-01-01": 1.0, "2025-02-01": 6.0, "2025-03-01": 8.0}
QUALIFIED_SUM = {"formula": "sum(events.amount)", "name": "s"}
RENAME_LOG = "reserved library name"


def _current(doc: dict, entity: str) -> dict:
    respelled = {"SlayerModel": lambda d: dataset_in(d, CURRENT_KEYS), "SlayerQuery": stored_query}
    return {**respelled.get(entity, dict)(doc), "version": CURRENT_VERSIONS[entity]}


def _recorded(name: str, former: str) -> dict:
    return {**granularity_definition(name), "renamed_from": former}


def _calendar(name: str) -> dict:
    return {"name": name, "base": "month"}


def _datasource(db_path: str, granularities: list[dict]) -> dict:
    """Datasource ``DS`` as the upgrade stores it once it records renames."""
    return {"version": RECORDED_DATASOURCE_VERSION, "name": DS, "type": "sqlite", "database": db_path,
            "granularities": granularities}


async def _store(
    tmp_path: Path, *, backend: str, granularities: list[dict] | None = None, legacy: list[str] | None = None,
    datasets: Sequence[dict] = (), memories: Sequence[dict] = (), label: str = "store",
) -> tuple[StorageBackend, str]:
    """A store whose datasource records ``granularities`` (v4), else defines ``legacy`` as v1.4.1 wrote them."""
    base = str(tmp_path / f"{label}-{backend}")
    db_path = live_db(base)
    ds = datasource_doc(db_path=db_path, granularities=legacy or []) if granularities is None else _datasource(
        db_path, granularities)
    storage = await write_raw_store(backend=backend, base=base, datasources=[ds], datasets=[events_doc(), *datasets],
                                    memories=list(memories))
    return storage, base


def _stored(*, backend: str, base: str, key: str) -> dict:
    return yaml.safe_load(raw_documents(backend=backend, base=base)[key])  # JSON is YAML


def _stored_datasource(*, backend: str, base: str) -> dict:
    return _stored(backend=backend, base=base, key=f"datasources/{DS}")


async def _granularities(storage: StorageBackend) -> list[tuple[str, str | None]]:
    ds = await storage.get_datasource(DS)
    assert ds is not None
    return [(g.name, g.renamed_from) for g in ds.granularities]


async def _stage(storage: StorageBackend, name: str) -> SlayerQuery:
    model = await storage.get_dataset(name, data_source=DS)
    assert model is not None
    [stage] = model.source_queries or []
    return stage


def _td_granularity(query: SlayerQuery) -> str:
    [td] = query.time_dimensions or []
    return str(td.granularity)


def _rename_logs(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if RENAME_LOG in r.getMessage()]


class TestRecord:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_renamed_granularity_records_its_former_name(self, tmp_path: Path, backend: str) -> None:
        storage, base = await _store(tmp_path, backend=backend, legacy=["left"])
        before = raw_documents(backend=backend, base=base)
        assert await _granularities(storage) == [("left_granularity", "left")]
        assert raw_documents(backend=backend, base=base) == before

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_taken_new_name_records_the_former_name(self, tmp_path: Path, backend: str) -> None:
        base = str(tmp_path / backend)
        ds = {**datasource_doc(db_path=live_db(base)),
              "granularities": [granularity_definition("left"), _calendar("left_granularity")]}
        storage = await write_raw_store(backend=backend, base=base, datasources=[ds])
        assert sorted(await _granularities(storage), key=str) == sorted(
            [("left_granularity_2", "left"), ("left_granularity", None)], key=str)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_null_record_is_no_record(self, tmp_path: Path, backend: str) -> None:
        base = str(tmp_path / backend)
        ds = {**datasource_doc(db_path=live_db(base)),
              "granularities": [{**granularity_definition("left"), "renamed_from": None}]}
        storage = await write_raw_store(backend=backend, base=base, datasources=[ds])
        assert await _granularities(storage) == [("left_granularity", "left")]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_stored_record_is_kept(self, tmp_path: Path, backend: str) -> None:
        storage, _ = await _store(tmp_path, backend=backend, granularities=[_recorded("left_granularity", "left")])
        assert await _granularities(storage) == [("left_granularity", "left")]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_record_decides_its_former_name(self, tmp_path: Path, backend: str) -> None:
        storage, _ = await _store(
            tmp_path, backend=backend,
            granularities=[_recorded("left_granularity_2", "left"), _calendar("left_granularity")],
            datasets=[query_backed_doc(name="by_left", stage=bucket_stage("left")),
                      query_backed_doc(name="by_taken", stage=bucket_stage("left_granularity"))],
        )
        assert _td_granularity(await _stage(storage, "by_left")) == "left_granularity_2"
        assert _td_granularity(await _stage(storage, "by_taken")) == "left_granularity"
        assert await bucket_totals(storage, dataset="by_left") == EXPECTED_BUCKETS
        assert await bucket_totals(storage, dataset="by_taken") == CALENDAR_BUCKETS

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_record_matches_its_former_name_case_insensitively(self, tmp_path: Path, backend: str) -> None:
        storage, _ = await _store(
            tmp_path, backend=backend, granularities=[_recorded("left_granularity", "LEFT")],
            datasets=[query_backed_doc(name="by_left", stage=bucket_stage("left"))],
        )
        assert _td_granularity(await _stage(storage, "by_left")) == "left_granularity"

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_the_first_record_wins(self, tmp_path: Path, backend: str) -> None:
        storage, _ = await _store(
            tmp_path, backend=backend, granularities=[_recorded("left_a", "left"), _recorded("left_b", "left")],
            datasets=[query_backed_doc(name="by_left", stage=bucket_stage("left"))],
        )
        assert _td_granularity(await _stage(storage, "by_left")) == "left_a"

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_record_overrides_a_pending_rename(self, tmp_path: Path, backend: str) -> None:
        storage, _ = await _store(
            tmp_path, backend=backend, granularities=[granularity_definition("left"), _recorded("left_x", "left")],
            datasets=[query_backed_doc(name="by_left", stage=bucket_stage("left"))],
        )
        assert _td_granularity(await _stage(storage, "by_left")) == "left_x"


class TestGate:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_an_old_document_arriving_after_the_upgrade_is_renamed(
        self, tmp_path: Path, backend: str, caplog: pytest.LogCaptureFixture,
    ) -> None:
        storage, base = await _store(tmp_path, backend=backend, legacy=["left"])
        assert (await upgrade_store(storage)).complete
        stored = _stored_datasource(backend=backend, base=base)
        assert stored["version"] == RECORDED_DATASOURCE_VERSION
        assert [(g["name"], g.get("renamed_from")) for g in stored["granularities"]] == [("left_granularity", "left")]
        await write_raw_store(backend=backend, base=base, datasources=[],
                              datasets=[query_backed_doc(name="late", stage=bucket_stage("left"))])
        reopened = reopen(backend=backend, base=base)
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            assert _td_granularity(await _stage(reopened, "late")) == "left_granularity"
            assert await bucket_totals(reopened, dataset="late") == EXPECTED_BUCKETS
        assert _rename_logs(caplog) == []

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_an_old_memory_under_a_recorded_rename_is_renamed(self, tmp_path: Path, backend: str) -> None:
        storage, _ = await _store(tmp_path, backend=backend, granularities=[_recorded("left_granularity", "left")],
                                  memories=[memory_doc(memory_id="m", query=bucket_stage("left"))])
        memory = await storage.get_memory("m")
        assert memory.query is not None
        assert _td_granularity(memory.query) == "left_granularity"

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("rename", ["pending", "recorded"])
    async def test_a_current_dataset_is_read_as_written(self, tmp_path: Path, backend: str, rename: str) -> None:
        initials = {"name": "initials", "formula": "count_distinct(left(status, 1))"}
        storage, _ = await _store(
            tmp_path, backend=backend, legacy=["left"] if rename == "pending" else None,
            granularities=[_recorded("left_granularity", "left")] if rename == "recorded" else None,
            datasets=[_current(events_doc(name="initialled", metrics=[initials]), "SlayerModel")],
        )
        dataset = await storage.get_dataset("initialled", data_source=DS)
        assert dataset is not None
        assert [m.expression for m in dataset.metrics] == ["count_distinct(left(status, 1))"]
        resp = await run(storage, {"population": "initialled", "metrics": ["initialled.initials"]})
        assert [int(v) for v in resp.data[0].values()] == [2]

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("rename", ["pending", "recorded"])
    async def test_a_current_memory_is_read_as_written(self, tmp_path: Path, backend: str, rename: str) -> None:
        query = _current(stage_doc(dimensions=[{"expression": "left(events.status, 1)", "name": "initial"}],
                                   measures=[QUALIFIED_SUM]), "SlayerQuery")
        current = {"version": CURRENT_VERSIONS["Memory"], "id": "m", "learning": "by initial", "entities": [],
                   "query": query}
        storage, _ = await _store(
            tmp_path, backend=backend, legacy=["left"] if rename == "pending" else None,
            granularities=[_recorded("left_granularity", "left")] if rename == "recorded" else None,
            memories=[current],
        )
        memory = await storage.get_memory("m")
        assert memory.query is not None
        assert [d.field for d in memory.query.dimensions or []] == ["left(events.status, 1)"]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_nested_stage_is_judged_by_its_own_version(self, tmp_path: Path, backend: str) -> None:
        stage = {**stored_query(stage_doc(time_dimensions=["date_trunc('left', events.created_at)"],
                                          measures=[QUALIFIED_SUM])),
                 "version": granularity_rename.RENAME_GATES["left"]["SlayerQuery"] - 1}
        backed = _current(query_backed_doc(name="bucketed", stage=stage), "SlayerModel")
        storage, _ = await _store(tmp_path, backend=backend, granularities=[_recorded("left_granularity", "left")],
                                  datasets=[backed])
        assert _td_granularity(await _stage(storage, "bucketed")) == "left_granularity"
        assert await bucket_totals(storage) == EXPECTED_BUCKETS

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_memorys_query_is_judged_by_its_own_version(self, tmp_path: Path, backend: str) -> None:
        query = {**stored_query(stage_doc(time_dimensions=["date_trunc('left', events.created_at)"],
                                          measures=[QUALIFIED_SUM])),
                 "version": granularity_rename.RENAME_GATES["left"]["SlayerQuery"] - 1}
        current = {"version": CURRENT_VERSIONS["Memory"], "id": "m", "learning": "by bucket", "entities": [],
                   "query": query}
        storage, _ = await _store(tmp_path, backend=backend, granularities=[_recorded("left_granularity", "left")],
                                  memories=[current])
        memory = await storage.get_memory("m")
        assert memory.query is not None
        assert _td_granularity(memory.query) == "left_granularity"

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_document_without_a_version_counts_as_old(self, tmp_path: Path, backend: str) -> None:
        stage = {k: v for k, v in bucket_stage("left").items() if k != "version"}
        backed = {k: v for k, v in query_backed_doc(name="bucketed", stage=stage).items() if k != "version"}
        storage, _ = await _store(tmp_path, backend=backend, granularities=[_recorded("left_granularity", "left")],
                                  datasets=[backed])
        assert _td_granularity(await _stage(storage, "bucketed")) == "left_granularity"


# --- slayer storage upgrade: no write order, nothing held back --------------------------------------------------------


def _legacy_store(*, backend: str, base: str) -> StorageBackend:
    db_path = live_db(base)
    return asyncio.run(write_raw_store(
        backend=backend, base=base, datasources=[datasource_doc(db_path=db_path, granularities=["left"])],
        datasets=[events_doc(), query_backed_doc(name="bucketed", stage=bucket_stage("left")),
                  query_backed_doc(name="bucketed_too", stage=bucket_stage("left"))],
        memories=[memory_doc(memory_id="m1", query=bucket_stage("left"))],
    ))


def _copy_datasource(*, backend: str, source: str, target: str) -> None:
    """Copy datasource ``DS`` from the store at ``source`` into the one at ``target``, reading ``target``'s live db."""
    if backend == "yaml":
        text = Path(source, "datasources", f"{DS}.yaml").read_text()
        Path(target, "datasources", f"{DS}.yaml").write_text(text.replace(source, target))
        return
    with transaction(os.path.join(source, "store.db")) as conn:
        [(data,)] = conn.execute("SELECT data FROM datasources WHERE name = ?", (DS,)).fetchall()
    with transaction(os.path.join(target, "store.db")) as conn:
        conn.execute("UPDATE datasources SET data = ? WHERE name = ?", (data.replace(source, target), DS))


def _comparable(*, backend: str, base: str) -> dict[str, Any]:
    """Each stored document parsed, its store path a placeholder, a memory without its read-time ``created_at``."""
    out: dict[str, Any] = {}
    for key, text in raw_documents(backend=backend, base=base).items():
        text = text.replace(base, "<store>")
        if key.startswith("memories/") and text.startswith("---"):
            front = yaml.safe_load(text.split("---")[1])
            out[key] = ({k: v for k, v in front.items() if k != "created_at"}, text.split("---")[-1])
        elif key.startswith("memories/"):
            out[key] = {k: v for k, v in json.loads(text).items() if k != "created_at"}
        else:
            out[key] = yaml.safe_load(text)
    return out


def _mentions_left(text: str) -> bool:
    return "'left'" in text or '"left"' in text or ": left\n" in text


@pytest.mark.parametrize("backend", BACKENDS)
def test_an_interruption_after_the_datasource_converges(backend: str, tmp_path: Path) -> None:
    done, interrupted = str(tmp_path / "done"), str(tmp_path / "interrupted")
    _legacy_store(backend=backend, base=done)
    _legacy_store(backend=backend, base=interrupted)
    assert upgrade(store_path(reopen(backend=backend, base=done))).code == 0
    _copy_datasource(backend=backend, source=done, target=interrupted)

    between = reopen(backend=backend, base=interrupted)
    for name in ("bucketed", "bucketed_too"):
        assert _td_granularity(asyncio.run(_stage(between, name))) == "left_granularity", name
    memory = asyncio.run(between.get_memory("m1"))
    assert memory.query is not None
    assert _td_granularity(memory.query) == "left_granularity"
    assert asyncio.run(bucket_totals(between)) == EXPECTED_BUCKETS

    assert upgrade(store_path(reopen(backend=backend, base=interrupted))).code == 0
    assert _comparable(backend=backend, base=interrupted) == _comparable(backend=backend, base=done)


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_conflicted_dependent_does_not_hold_the_datasource(
    backend: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = str(tmp_path / "store")
    _legacy_store(backend=backend, base=base)
    backend_class = SQLiteStorage if backend == "sqlite" else YAMLStorage
    write = backend_class._write_raw_if_unchanged

    async def conflict_on_bucketed(self: Any, **kwargs: Any) -> bool:
        return False if kwargs["name"] == "bucketed" else await write(self, **kwargs)

    monkeypatch.setattr(backend_class, "_write_raw_if_unchanged", conflict_on_bucketed)
    result = upgrade(store_path(reopen(backend=backend, base=base)))
    assert result.code == 1, result
    assert f"datasource {DS}: upgraded" in result.out.splitlines(), result.out
    assert not any("waits for" in line for line in result.out.splitlines()), result.out
    stored = _stored_datasource(backend=backend, base=base)
    assert [(g["name"], g.get("renamed_from")) for g in stored["granularities"]] == [("left_granularity", "left")]
    reopened = reopen(backend=backend, base=base)
    assert _td_granularity(asyncio.run(_stage(reopened, "bucketed"))) == "left_granularity"
    assert asyncio.run(bucket_totals(reopened)) == EXPECTED_BUCKETS
    monkeypatch.undo()
    assert upgrade(store_path(reopen(backend=backend, base=base))).code == 0
    finished = raw_documents(backend=backend, base=base)
    assert not any(_mentions_left(text) for key, text in finished.items() if not key.startswith("datasources/"))


@pytest.mark.parametrize("backend", BACKENDS)
def test_an_unreadable_memory_does_not_hold_a_scoped_rename(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    _legacy_store(backend=backend, base=base)
    if backend == "yaml":
        Path(base, "memories", "broken_memory.md").write_text("---\nid: [unclosed\n---\nbody\n")
    else:
        with transaction(os.path.join(base, "store.db")) as conn:
            conn.execute("INSERT INTO memories (id, data) VALUES (?, ?)", ("broken_memory", "{not json"))
    result = upgrade(store_path(reopen(backend=backend, base=base)), "--data-source", DS)
    assert result.code == 0, result
    stored = _stored_datasource(backend=backend, base=base)
    assert [(g["name"], g.get("renamed_from")) for g in stored["granularities"]] == [("left_granularity", "left")]


@pytest.mark.parametrize("backend", BACKENDS)
def test_the_upgrade_leaves_a_current_dependent_as_written(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    query = _current(stage_doc(dimensions=[{"expression": "left(events.status, 1)", "name": "initial"}],
                               measures=[QUALIFIED_SUM]), "SlayerQuery")
    backed = _current(query_backed_doc(name="initials", stage=query), "SlayerModel")
    asyncio.run(write_raw_store(
        backend=backend, base=base, datasources=[datasource_doc(db_path=live_db(base), granularities=["left"])],
        datasets=[events_doc(), backed],
    ))
    before = raw_documents(backend=backend, base=base)[f"models/{DS}/initials"]
    assert upgrade(store_path(reopen(backend=backend, base=base))).code == 0
    assert raw_documents(backend=backend, base=base)[f"models/{DS}/initials"] == before
