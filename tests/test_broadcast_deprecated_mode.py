"""``to_many_handling: "broadcast"`` is honoured, reported as deprecated on every run and save, and points to the
concrete ``partition_by=`` that returns the same values.

Spec: queries/attribution-modes › Broadcast is a deprecated mode; compatibility/removal-tripwire › Deprecated input
forms convert with a structured warning (the kept-form scenario).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import tempfile
import warnings
from collections.abc import AsyncGenerator, AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from pydantic import ValidationError

from slayer.api.server import QueryRequest, create_app
from slayer.core.models import SlayerDataset
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.core.warnings import DICE_SLICE_HINT, BroadcastGrainWarningPayload
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.mcp.server import create_mcp_server
from slayer.storage.engine_view import EngineStorage
from slayer.storage.migrations import CURRENT_VERSIONS, migrating_document
from slayer.storage.yaml_storage import YAMLStorage
from tests._dataset_access_fixtures import mcp_text
from tests._deprecated_input_helpers import (
    CM,
    CROSS,
    SAVED,
    deprecated_in,
    http_client,
    mcp_payload,
    mode,
    orders_engine,
    recorded,
    rows,
)
from tests._dev1840_fixtures import make_exec_engine
from tests._stored_upgrade_fixtures import rev_query

B = "broadcast"
SAVED_B = "by_status_b"
CROSS_B = mode(CROSS, B)
ATTRIBUTABLE = {"population": "orders", "dimensions": ["customers.tier"], "metrics": [CM]}
NO_DIMENSIONS = {"population": "orders", "metrics": [CM]}
STAGE_A = {"name": "a", "population": "orders", "dimensions": ["orders.status"], "metrics": [{"metric": "count(orders.*)", "alias": "n"}]}
STAGE_B = {"name": "b", "population": "a", "metrics": [{"metric": "sum(a.n)", "alias": "total"}]}
ROOT_ON_A = {"population": "a", "metrics": [{"metric": "sum(a.n)", "alias": "grand"}]}
ROOT_ON_B = {"population": "b", "metrics": [{"metric": "sum(b.total)", "alias": "grand"}]}
TWO_BROADCAST_STAGES = [mode(STAGE_A, B), mode(STAGE_B, B), ROOT_ON_B]
UNUSED_STAGE = {"name": "x", "population": "orders", "metrics": [{"metric": "count(orders.*)", "alias": "unused_n"}]}


def futures(caught: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
    """The recorded ``FutureWarning``s naming ``broadcast``."""
    return [w for w in caught if issubclass(w.category, FutureWarning) and B in str(w.message)]


def _get(w: Any, key: str) -> Any:
    return w.get(key) if isinstance(w, dict) else getattr(w, key)


def assert_broadcast_deprecation(w: Any) -> None:
    assert "to_many_handling" in _get(w, "old")
    assert B in _get(w, "old")
    assert "partition_by=" in _get(w, "new")
    assert _get(w, "removal") == "3.0"


def broadcasts(response: SlayerResponse) -> list[BroadcastGrainWarningPayload]:
    return [w for w in response.warnings if isinstance(w, BroadcastGrainWarningPayload)]


def kinds(response: SlayerResponse) -> set[str]:
    return {w.kind for w in response.warnings}


def mcp_reply(text: str) -> dict[str, Any]:
    reply = mcp_payload(text)
    reply.setdefault("warnings", [])
    return reply


def _next_line() -> int:
    frame = inspect.currentframe()
    assert frame is not None
    assert frame.f_back is not None
    return frame.f_back.f_lineno + 1


@pytest.fixture
async def engine() -> AsyncGenerator[SlayerQueryEngine]:
    """The deprecated-input engine plus ``by_status_b``: ``by_status`` saved under ``"broadcast"``."""
    async with orders_engine() as e:
        stage = mode({"population": "orders", "dimensions": ["orders.status"], "metrics": [{"metric": "count(orders.*)", "alias": "n"}]}, B)
        with recorded():
            await e.save_dataset(SlayerDataset.model_validate({"name": SAVED_B, "source_queries": [stage]}))
        yield e


async def _broadcast_message(engine: SlayerQueryEngine) -> str:
    with recorded():
        [w] = deprecated_in((await engine.execute(CROSS_B)).warnings)
    return w.human_message()


class TestFreshRun:
    """Scenario: Fresh broadcast reports one deprecation and keeps its values."""

    async def test_python(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            response = await engine.execute(SlayerQuery.model_validate(CROSS_B))
        assert len(futures(caught)) == 1
        [w] = deprecated_in(response.warnings)
        assert_broadcast_deprecation(w)
        assert {r["orders.status"]: r["cm"] for r in response.data} == {"new": 515.0, "ok": 515.0}
        [bw] = broadcasts(response)
        assert (bw.metric, bw.location, [d.dimension for d in bw.dimensions]) == ("cm", "stages[0]", ["status"])

    async def test_rest(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            reference = await engine.execute(CROSS_B)
        client = TestClient(create_app(storage=engine.storage))
        with recorded() as caught:
            resp = client.post("/query", json=CROSS_B)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert len(futures(caught)) == 1
        [w] = deprecated_in(body["warnings"])
        assert_broadcast_deprecation(w)
        assert rows(body["data"]) == rows(reference.data)
        with recorded():
            dry = client.post("/query", json={**CROSS_B, "dry_run": True}).json()
        assert dry["sql"] == reference.sql
        assert [b["metric"] for b in body["warnings"] if b["kind"] == B] == ["cm"]

    async def test_mcp(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            reference = await engine.execute(CROSS_B)
        with recorded() as caught:
            reply = mcp_reply(await mcp_text(engine.storage, "query", query=CROSS_B, format="json"))
        assert len(futures(caught)) == 1
        [w] = deprecated_in(reply["warnings"])
        assert_broadcast_deprecation(w)
        assert rows(reply["data"]) == rows(reference.data)
        assert [b["metric"] for b in reply["warnings"] if b["kind"] == B] == ["cm"]

    @pytest.mark.parametrize("runner", ["sync", "async"])
    async def test_http_client(self, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch, runner: str) -> None:
        client = http_client(engine, monkeypatch)
        with recorded():
            response = client.query_sync(CROSS_B) if runner == "sync" else await client.query(CROSS_B)
        [w] = deprecated_in(response.warnings)
        assert_broadcast_deprecation(w)


class TestPythonWarning:
    """Fresh input warns once per query object, at the first line outside SLayer."""

    def test_direct_construction(self) -> None:
        with recorded() as caught:
            line = _next_line()
            SlayerQuery(population="orders", to_many_handling=B)
        [w] = futures(caught)
        assert (w.filename, w.lineno) == (__file__, line)

    def test_model_validate(self) -> None:
        with recorded() as caught:
            line = _next_line()
            SlayerQuery.model_validate(CROSS_B)
        [w] = futures(caught)
        assert (w.filename, w.lineno) == (__file__, line)

    def test_nested_inside_a_dataset(self) -> None:
        with recorded() as caught:
            line = _next_line()
            SlayerDataset.model_validate({"name": "m", "source_queries": [CROSS_B]})
        [w] = futures(caught)
        assert (w.filename, w.lineno) == (__file__, line)

    def test_refinement(self) -> None:
        with recorded() as caught:
            line = _next_line()
            QueryRefinement.model_validate({"to_many_handling": B})
        [w] = futures(caught)
        assert (w.filename, w.lineno) == (__file__, line)

    def test_rest_request_body(self) -> None:
        with recorded() as caught:
            QueryRequest.model_validate(CROSS_B)
        assert len(futures(caught)) == 1

    def test_versioned_dict_is_not_fresh(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({**CROSS_B, "version": CURRENT_VERSIONS["SlayerQuery"]})
        assert query.to_many_handling == B
        assert futures(caught) == []

    def test_stored_document_is_not_fresh(self) -> None:
        with recorded() as caught, migrating_document("doc", stored=True):
            query = SlayerQuery.model_validate(CROSS_B)
        assert query.to_many_handling == B
        assert futures(caught) == []

    def test_a_dump_round_trip_does_not_warn_again(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate(CROSS_B)
            SlayerQuery.model_validate(query.model_dump(mode="json"))
            query.model_copy(update={"limit": 1})
        assert len(futures(caught)) == 1


class TestEveryRun:
    """Scenario: Every run reports the deprecation once."""

    async def test_built_once_run_three_times(self, engine: SlayerQueryEngine) -> None:
        with recorded() as built:
            query = SlayerQuery.model_validate(CROSS_B)
        assert len(futures(built)) == 1
        for _ in range(3):
            with recorded() as caught:
                response = await engine.execute(query)
            assert len(deprecated_in(response.warnings)) == 1
            assert futures(caught) == []

    async def test_a_versioned_dict_reports_on_its_run(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            response = await engine.execute({**CROSS_B, "version": CURRENT_VERSIONS["SlayerQuery"]})
        assert len(deprecated_in(response.warnings)) == 1
        assert futures(caught) == []

    async def test_runtime_variables_keep_the_report(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            query = SlayerQuery.model_validate({**CROSS_B, "filter": ["orders.status = '{s}'"]})
        response = await engine.execute(query, variables={"s": "ok"})
        assert len(deprecated_in(response.warnings)) == 1


async def _staged_run(engine: SlayerQueryEngine, surface: str, stages: list, monkeypatch: pytest.MonkeyPatch) -> list:
    if surface == "python":
        return (await engine.execute(stages)).warnings
    if surface == "rest":
        resp = TestClient(create_app(storage=engine.storage)).post("/query", json={"queries": stages})
        assert resp.status_code == 200, resp.text
        return resp.json()["warnings"]
    if surface == "mcp":
        return mcp_reply(await mcp_text(engine.storage, "query", query=stages, format="json"))["warnings"]
    return http_client(engine, monkeypatch).query_sync(stages).warnings


class TestEachStage:
    """Scenario: Each broadcast stage reports its own deprecation."""

    @pytest.mark.parametrize("surface", ["python", "rest", "mcp", "http_client"])
    async def test_two_named_stages(self, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch, surface: str) -> None:
        with recorded():
            reported = deprecated_in(await _staged_run(engine, surface, TWO_BROADCAST_STAGES, monkeypatch))
        assert len(reported) == 2
        for w in reported:
            assert_broadcast_deprecation(w)

    async def test_one_future_warning_per_stage(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            await engine.execute(TWO_BROADCAST_STAGES)
        assert len(futures(caught)) == 2

    async def test_a_pruned_stage_still_reports(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            await engine.save_dataset(SlayerDataset.model_validate(
                {"name": "qb", "source_queries": [mode(UNUSED_STAGE, B), mode(STAGE_A, B), ROOT_ON_A]},
            ))
        query = {"population": "qb", "metrics": [{"metric": "sum(qb.grand)", "alias": "g"}]}
        with recorded() as caught:
            response = await engine.execute(query)
        assert "unused_n" not in (response.sql or "")
        assert len(deprecated_in(response.warnings)) == 2
        assert futures(caught) == []

    async def test_a_root_without_the_mode_adds_none(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            response = await engine.execute([STAGE_A, ROOT_ON_A])
        assert deprecated_in(response.warnings) == []


class TestNeverBroadcasting:
    """Scenario: A broadcast stage that never broadcasts still reports."""

    @pytest.mark.parametrize("body", [ATTRIBUTABLE, NO_DIMENSIONS], ids=["attributable", "no_dimensions"])
    @pytest.mark.parametrize("path", ["executed", "dry_run", "explain"])
    async def test_reports_without_a_broadcast_warning(self, engine: SlayerQueryEngine, body: dict, path: str) -> None:
        with recorded():
            response = await engine.execute(mode(body, B), dry_run=path == "dry_run", explain=path == "explain")
        assert len(deprecated_in(response.warnings)) == 1
        assert broadcasts(response) == []


def _store_broadcast_stage(engine: SlayerQueryEngine) -> None:
    """Write ``"broadcast"`` into ``SAVED``'s stored current-version stage, as a hand edit would."""
    assert isinstance(engine.storage, EngineStorage)
    storage = engine.storage.inner
    assert isinstance(storage, YAMLStorage)
    path = next(Path(storage.base_dir).glob(f"models/*/{SAVED}.yaml"))
    doc = yaml.safe_load(path.read_text())
    doc["source_queries"][0]["to_many_handling"] = B
    path.write_text(yaml.safe_dump(doc))


class TestStoredStage:
    """Scenario: A stored broadcast stage reports on every run."""

    async def test_loading_records_no_future_warning(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            dataset = await engine.storage.get_dataset(SAVED_B)
        assert dataset is not None
        assert dataset.source_queries is not None
        assert dataset.source_queries[0].to_many_handling == B
        assert futures(caught) == []

    async def test_queried_twice_as_a_source(self, engine: SlayerQueryEngine) -> None:
        reference = await engine.execute({"population": SAVED, "metrics": [{"metric": f"sum({SAVED}.n)", "alias": "t"}]})
        for _ in range(2):
            with recorded() as caught:
                response = await engine.execute({"population": SAVED_B, "metrics": [{"metric": f"sum({SAVED_B}.n)", "alias": "t"}]})
            assert len(deprecated_in(response.warnings)) == 1
            assert rows(response.data) == rows(reference.data)
            assert futures(caught) == []

    async def test_run_by_name(self, engine: SlayerQueryEngine) -> None:
        reference = await engine.execute(SAVED)
        with recorded() as caught:
            response = await engine.execute(SAVED_B)
        assert len(deprecated_in(response.warnings)) == 1
        assert rows(response.data) == rows(reference.data)
        assert futures(caught) == []

    async def test_hand_edited_stored_stage(self, engine: SlayerQueryEngine) -> None:
        reference = await engine.execute(SAVED)
        _store_broadcast_stage(engine)
        for _ in range(2):
            with recorded() as caught:
                response = await engine.execute(SAVED)
            assert len(deprecated_in(response.warnings)) == 1
            assert rows(response.data) == rows(reference.data)
            assert futures(caught) == []

    def test_rest_run_by_name(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            resp = TestClient(create_app(storage=engine.storage)).post("/query", json={"name": SAVED_B})
        assert resp.status_code == 200, resp.text
        assert len(deprecated_in(resp.json()["warnings"])) == 1

    async def test_stored_v11_default_reports_nothing(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({"version": 11, **rev_query(to_many_handling=B)})
            response = await engine.execute(query)
        assert query.to_many_handling is None
        assert [w for w in deprecated_in(response.warnings) if B in _get(w, "old")] == []
        assert futures(caught) == []


REFINEMENTS = {
    "sets_broadcast": (SAVED, {"to_many_handling": B}, 1),
    "inherits_broadcast": (SAVED_B, {"filter": ["orders.status = 'ok'"]}, 1),
    "overrides_to_associate": (SAVED_B, {"to_many_handling": "associate"}, 0),
}


class TestRefinements:
    """Scenario: Refinements report the effective mode."""

    @pytest.mark.parametrize("case", list(REFINEMENTS))
    async def test_in_process(self, engine: SlayerQueryEngine, case: str) -> None:
        name, refine, expected = REFINEMENTS[case]
        with recorded():
            response = await engine.execute(name, refine=refine)
        assert len(deprecated_in(response.warnings)) == expected

    @pytest.mark.parametrize("case", list(REFINEMENTS))
    def test_rest(self, engine: SlayerQueryEngine, case: str) -> None:
        name, refine, expected = REFINEMENTS[case]
        with recorded():
            resp = TestClient(create_app(storage=engine.storage)).post("/query", json={"name": name, "refine": refine})
        assert resp.status_code == 200, resp.text
        assert len(deprecated_in(resp.json()["warnings"])) == expected

    @pytest.mark.parametrize("case", list(REFINEMENTS))
    async def test_mcp(self, engine: SlayerQueryEngine, case: str) -> None:
        name, refine, expected = REFINEMENTS[case]
        with recorded():
            reply = mcp_reply(await mcp_text(engine.storage, "query", query=name, refine=refine, format="json"))
        assert len(deprecated_in(reply["warnings"])) == expected

    @pytest.mark.parametrize("case", list(REFINEMENTS))
    async def test_http_client(self, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch, case: str) -> None:
        name, refine, expected = REFINEMENTS[case]
        with recorded():
            response = http_client(engine, monkeypatch).query_sync(name, refine=refine)
        assert len(deprecated_in(response.warnings)) == expected

    async def test_a_fresh_refinement_warns_once(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            await engine.execute(SAVED, refine={"to_many_handling": B})
        assert len(futures(caught)) == 1

    async def test_reused_refinement_reports_on_every_run(self, engine: SlayerQueryEngine) -> None:
        with recorded():
            refinement = QueryRefinement.model_validate({"to_many_handling": B})
        for _ in range(3):
            with recorded() as caught:
                response = await engine.execute(SAVED, refine=refinement)
            assert len(deprecated_in(response.warnings)) == 1
            assert futures(caught) == []


async def _stored_mode(engine: SlayerQueryEngine, name: str) -> str | None:
    dataset = await engine.storage.get_dataset(name)
    assert dataset is not None
    assert dataset.source_queries is not None
    return dataset.source_queries[0].to_many_handling


class TestSaves:
    """Scenario: A save reports the deprecation and keeps broadcast."""

    @pytest.mark.parametrize(("stages", "expected"), [([CROSS_B], 1), (TWO_BROADCAST_STAGES, 2)], ids=["one", "two_stages"])
    async def test_engine_save(self, engine: SlayerQueryEngine, stages: list, expected: int) -> None:
        with recorded():
            dataset = SlayerDataset.model_validate({"name": "saved_b", "source_queries": stages})
        notes: list = []
        await engine.save_dataset(dataset, warnings_out=notes)
        assert len(deprecated_in(notes)) == expected
        assert await _stored_mode(engine, "saved_b") == B

    @pytest.mark.parametrize(("stages", "expected"), [([CROSS_B], 1), (TWO_BROADCAST_STAGES, 2)], ids=["one", "two_stages"])
    async def test_rest_post(self, engine: SlayerQueryEngine, stages: list, expected: int) -> None:
        with recorded():
            resp = TestClient(create_app(storage=engine.storage)).post("/datasets", json={"name": "saved_b", "source_queries": stages})
        assert resp.status_code == 200, resp.text
        reported = deprecated_in(resp.json()["warnings"])
        assert len(reported) == expected
        assert_broadcast_deprecation(reported[0])
        assert await _stored_mode(engine, "saved_b") == B

    async def test_rest_put(self, engine: SlayerQueryEngine) -> None:
        client = TestClient(create_app(storage=engine.storage))
        assert client.post("/datasets", json={"name": "saved_b", "source_queries": [CROSS]}).status_code == 200
        with recorded():
            resp = client.put("/datasets/saved_b", json={"name": "saved_b", "source_queries": [CROSS_B]})
        assert resp.status_code == 200, resp.text
        assert len(deprecated_in(resp.json()["warnings"])) == 1
        assert await _stored_mode(engine, "saved_b") == B

    async def test_mcp_create(self, engine: SlayerQueryEngine) -> None:
        message = await _broadcast_message(engine)
        with recorded():
            text = await mcp_text(engine.storage, "create_dataset", name="saved_b", query=CROSS_B)
        assert text.count(message) == 1
        assert await _stored_mode(engine, "saved_b") == B

    async def test_mcp_edit(self, engine: SlayerQueryEngine) -> None:
        message = await _broadcast_message(engine)
        await mcp_text(engine.storage, "create_dataset", name="saved_b", query=CROSS)
        with recorded():
            reply = json.loads(await mcp_text(engine.storage, "edit_dataset", dataset_name="saved_b", source_queries=[CROSS_B]))
        assert sum(w.count(message) for w in reply["warnings"]) == 1
        assert await _stored_mode(engine, "saved_b") == B

    async def test_mcp_memory(self, engine: SlayerQueryEngine) -> None:
        message = await _broadcast_message(engine)
        with recorded():
            reply = json.loads(await mcp_text(engine.storage, "save_memory", learning="Spend by status", linked_entities=CROSS_B))
        assert sum(w.count(message) for w in reply["warnings"]) == 1
        memory = await engine.storage.get_memory(reply["memory_id"])
        assert memory.query is not None
        assert memory.query.to_many_handling == B

    async def test_rest_memory(self, engine: SlayerQueryEngine) -> None:
        message = await _broadcast_message(engine)
        with recorded():
            resp = TestClient(create_app(storage=engine.storage)).post(
                "/memories", json={"learning": "Spend by status", "linked_entities": CROSS_B},
            )
        assert resp.status_code == 200, resp.text
        reply = resp.json()
        assert sum(w.count(message) for w in reply["warnings"]) == 1
        memory = await engine.storage.get_memory(reply["memory_id"])
        assert memory.query is not None
        assert memory.query.to_many_handling == B


class TestKeptForm:
    """removal-tripwire › A kept form is honoured, persisted as given and reported from storage."""

    async def test_fresh_run_save_reload_run(self, engine: SlayerQueryEngine) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate(CROSS_B)
            first = await engine.execute(query)
            await engine.save_dataset(SlayerDataset(name="kept", source_queries=[query]))
            reloaded = await engine.storage.get_dataset("kept")
            assert reloaded is not None
            assert reloaded.source_queries is not None
            [stored] = reloaded.source_queries
            second = await engine.execute(stored)
        assert stored.to_many_handling == B
        assert rows(first.data) == rows(second.data)
        assert len(deprecated_in(first.warnings)) == len(deprecated_in(second.warnings)) == 1
        assert len(futures(caught)) == 1


#: case → (population, dimensions, time dimensions, broadcast metric, the call its hint names, partition_by,
#: the rewritten metric)
CONCRETE: dict[str, tuple[str, list[str], list[dict], str, str, list[str], str]] = {
    "grand_total": (
        "orders", ["orders.status"], [], "sum(customers.spend)", "sum(customers.spend)", [],
        "sum(customers.spend, partition_by=[])",
    ),
    "mixed_grain": (
        "orders", ["orders.status", "customers.tier"], [], "sum(customers.spend)", "sum(customers.spend)",
        ["customers.tier"], "sum(customers.spend, partition_by=[customers.tier])",
    ),
    "windowed_by_time_field": (
        "customers", ["orders.status"], [{"field": "customers.signup_at", "granularity": "month"}],
        "sum(customers.spend, window='1y')", "sum(customers.spend, window='1y')", ["customers.signup_at"],
        "sum(customers.spend, window='1y', partition_by=[customers.signup_at])",
    ),
    "composite_leaf": (
        "orders", ["orders.status"], [], "sum(customers.spend) / sum(orders.amount)", "sum(customers.spend)", [],
        "sum(customers.spend, partition_by=[]) / sum(orders.amount)",
    ),
}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_exec_engine(request):
        yield e


def _values(data: list[dict[str, Any]]) -> list[tuple]:
    return sorted(tuple(sorted((k, repr(v)) for k, v in r.items())) for r in data)


def _query(population: str, dims: list[str], tds: list[dict], metric: str) -> dict[str, Any]:
    body: dict[str, Any] = {"population": population, "dimensions": dims, "metrics": [{"metric": metric, "alias": "v"}]}
    if tds:
        body["time_dimensions"] = tds
    return body


def assert_names_the_replacement(w: BroadcastGrainWarningPayload, *, call: str, partition: list[str]) -> None:
    assert w.partition_by == partition
    assert call in w.hint
    assert f"partition_by=[{', '.join(partition)}]" in w.hint
    assert DICE_SLICE_HINT in w.hint
    assert "spend_sum" not in w.hint


class TestConcreteReplacement:
    """Scenario: The broadcast warning names the concrete partition."""

    @pytest.mark.parametrize("case", list(CONCRETE))
    async def test_partition_and_round_trip(self, exec_engine: SlayerQueryEngine, case: str) -> None:
        population, dims, tds, metric, call, partition, rewritten = CONCRETE[case]
        with recorded():
            broadcast = await exec_engine.execute(mode(_query(population, dims, tds, metric), B))
        [w] = broadcasts(broadcast)
        assert_names_the_replacement(w, call=call, partition=partition)
        replaced = await exec_engine.execute(_query(population, dims, tds, rewritten))
        assert _values(replaced.data) == _values(broadcast.data)
        assert kinds(replaced).isdisjoint({B, "associated", "deprecated_input"})


POSITIONS: dict[str, dict[str, Any]] = {
    "aliased": {"metrics": [{"metric": "sum(customers.spend)", "alias": "total_spend"}]},
    "repeated_in_a_composite": {"metrics": [{"metric": "sum(customers.spend) + sum(customers.spend)", "alias": "twice"}]},
    "filter_only": {"metrics": [{"metric": "count(orders.*)", "alias": "n"}], "filter": ["sum(customers.spend) > 100"]},
    "order_only": {"metrics": [{"metric": "count(orders.*)", "alias": "n"}], "order": [{"field": "sum(customers.spend)", "direction": "desc"}]},
}


class TestCallNaming:
    """Scenario: Aliased, repeated, filter and order positions name the call."""

    @pytest.mark.parametrize("case", list(POSITIONS))
    async def test_hint_names_the_call(self, engine: SlayerQueryEngine, case: str) -> None:
        body = {"population": "orders", "dimensions": ["orders.status"], "to_many_handling": B, **POSITIONS[case]}
        with recorded():
            response = await engine.execute(body)
        [w] = broadcasts(response)
        assert_names_the_replacement(w, call="sum(customers.spend)", partition=[])


def _mode_enums(schema: dict[str, Any], model: str | None = None) -> set[str]:
    """Every ``enum`` value under the ``to_many_handling`` property of ``schema`` (or of its ``$defs[model]``)."""
    node = schema["$defs"][model] if model else schema
    out: set[str] = set()

    def walk(n: Any) -> None:
        if isinstance(n, dict):
            out.update(n.get("enum", []))
            if "const" in n:
                out.add(n["const"])
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)

    walk(node["properties"]["to_many_handling"])
    return out


def _mcp_query_schema() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        server = create_mcp_server(storage=YAMLStorage(base_dir=tmp), _seed_help=False)
        tools = {t.name: t for t in asyncio.run(server.list_tools())}
    return tools["query"].input_schema


class TestNotOffered:
    """Scenario: The schema does not offer broadcast but validation accepts it."""

    @pytest.mark.parametrize("model", [SlayerQuery, QueryRefinement], ids=lambda m: m.__name__)
    def test_pydantic_schema_enumerates_two_modes(self, model: type) -> None:
        assert _mode_enums(model.model_json_schema()) == {"associate", "error"}

    @pytest.mark.parametrize("model", ["SlayerQuery", "QueryRefinement"])
    def test_mcp_schema_enumerates_two_modes(self, model: str) -> None:
        assert _mode_enums(_mcp_query_schema(), model) == {"associate", "error"}

    @pytest.mark.parametrize("model", [SlayerQuery, QueryRefinement], ids=lambda m: m.__name__)
    def test_broadcast_still_validates(self, model: type) -> None:
        with recorded():
            built = model.model_validate({"to_many_handling": B})
        assert built.to_many_handling == B

    @pytest.mark.parametrize("model", [SlayerQuery, QueryRefinement], ids=lambda m: m.__name__)
    def test_unrecognized_value_names_only_the_offered_modes(self, model: type) -> None:
        with pytest.raises(ValidationError) as ei:
            model.model_validate({"to_many_handling": "sometimes"})
        message = str(ei.value)
        assert "associate" in message
        assert "'error'" in message or '"error"' in message
        assert B not in message


class TestOtherModesSilent:
    """Scenario: Other modes report no deprecation."""

    @pytest.mark.parametrize("value", [None, "associate", "error"], ids=["omitted", "associate", "error"])
    async def test_no_deprecation(self, engine: SlayerQueryEngine, value: str | None) -> None:
        body = ATTRIBUTABLE if value is None else mode(ATTRIBUTABLE, value)
        with recorded() as caught:
            response = await engine.execute(SlayerQuery.model_validate(body))
        assert deprecated_in(response.warnings) == []
        assert [w for w in caught if issubclass(w.category, FutureWarning)] == []
