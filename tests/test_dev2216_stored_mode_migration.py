"""Stored ``to_many_handling: "broadcast"`` loads as the omitted default; fresh input keeps it
(spec: queries/attribution-modes › Stored broadcast defaults migrate)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._stored_upgrade_fixtures import BACKENDS, DS, memory_v2, query_backed_v10, raw_doc, raw_store, rev_query

#: The last SlayerQuery version whose saves wrote the broadcast default.
PREVIOUS = 9
#: The last SlayerModel version before this change.
PREVIOUS_MODEL = 18


def _stored(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"version": PREVIOUS, "source_model": "orders", **fields})


def _stage(*, mode: str | None, versioned: bool, name: str | None = None) -> dict:
    stage = rev_query(**({"to_many_handling": mode} if mode else {}))
    if versioned:
        stage["version"] = PREVIOUS
    if name:
        stage["name"] = name
    return stage


class TestQueryDocument:
    def test_version_bumped(self) -> None:
        assert CURRENT_VERSIONS["SlayerQuery"] > PREVIOUS

    def test_stored_broadcast_loads_as_the_default(self) -> None:
        query = _stored(to_many_handling="broadcast")
        assert query.to_many_handling is None
        assert "to_many_handling" not in query.model_dump(mode="json", exclude_none=True)

    @pytest.mark.parametrize("mode", ["associate", "error"])
    def test_other_stored_modes_are_kept(self, mode: str) -> None:
        assert _stored(to_many_handling=mode).to_many_handling == mode

    def test_stored_strict_false_loads_as_the_default(self) -> None:
        query = SlayerQuery.model_validate({"version": 3, "source_model": "orders", "strict": False})
        assert query.to_many_handling is None

    def test_stored_strict_true_still_means_error(self) -> None:
        query = SlayerQuery.model_validate({"version": 3, "source_model": "orders", "strict": True})
        assert query.to_many_handling == "error"

    def test_fresh_broadcast_is_kept(self) -> None:
        query = SlayerQuery.model_validate({"source_model": "orders", "to_many_handling": "broadcast"})
        assert query.to_many_handling == "broadcast"

    def test_current_version_broadcast_is_kept(self) -> None:
        query = SlayerQuery.model_validate({
            "version": CURRENT_VERSIONS["SlayerQuery"], "source_model": "orders", "to_many_handling": "broadcast"})
        assert query.to_many_handling == "broadcast"

    def test_memory_query(self) -> None:
        query = {"version": PREVIOUS, "source_model": "orders", "to_many_handling": "broadcast"}
        mem = Memory.model_validate(memory_v2(memory_id="m", version=7, query=query))
        assert mem.query is not None
        assert mem.query.to_many_handling is None


class TestNestedStages:
    @pytest.mark.parametrize("versioned", [True, False], ids=["versioned", "unversioned"])
    def test_every_stage_migrates(self, versioned: bool) -> None:
        doc = query_backed_v10(name="rev_q", version=PREVIOUS_MODEL, stages=[
            _stage(mode="broadcast", versioned=versioned, name="first"),
            {**_stage(mode="broadcast", versioned=versioned), "source_model": "first"},
        ])
        model = SlayerModel.model_validate(doc)
        assert [s.to_many_handling for s in model.source_queries or []] == [None, None]

    def test_other_modes_survive_in_stages(self) -> None:
        doc = query_backed_v10(name="rev_q", version=PREVIOUS_MODEL, stages=[
            _stage(mode="error", versioned=False, name="first"),
            {**_stage(mode="associate", versioned=True), "source_model": "first"},
        ])
        model = SlayerModel.model_validate(doc)
        assert [s.to_many_handling for s in model.source_queries or []] == ["error", "associate"]


class TestStorage:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_stored_stages_load_as_the_default(self, tmp_path: Path, backend: str) -> None:
        doc = query_backed_v10(name="rev_q", version=PREVIOUS_MODEL, stages=[
            _stage(mode="broadcast", versioned=True, name="first"),
            {**_stage(mode="broadcast", versioned=False), "source_model": "first"},
        ])
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=None, models=[doc])
        model = await storage.get_model("rev_q", data_source=DS)
        assert model is not None
        assert [s.to_many_handling for s in model.source_queries or []] == [None, None]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_stored_memory_query_loads_as_the_default(self, tmp_path: Path, backend: str) -> None:
        query = {"version": PREVIOUS, "source_model": "orders", "to_many_handling": "broadcast"}
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=None,
                                  memories=[memory_v2(memory_id="m_b", version=7, query=query)])
        mem = await storage.get_memory("m_b")
        assert mem.query is not None
        assert mem.query.to_many_handling is None

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("mode", [None, "broadcast"])
    async def test_a_fresh_save_round_trips(self, tmp_path: Path, backend: str, mode: str | None) -> None:
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=None)
        stage = SlayerQuery.model_validate(rev_query(**({"to_many_handling": mode} if mode else {})))
        await storage.save_model(
            SlayerModel(name="rev_q", data_source=DS, source_queries=[stage]), _validate=False)
        model = await storage.get_model("rev_q", data_source=DS)
        assert model is not None
        assert [s.to_many_handling for s in model.source_queries or []] == [mode]
        (raw_stage,) = (await raw_doc(storage, "rev_q"))["source_queries"]
        assert raw_stage.get("to_many_handling") == mode
