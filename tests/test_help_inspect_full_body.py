"""``inspect`` returns a ``help.*`` memory's full body whatever ``compact`` is, on every surface."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import tempfile
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.cli import _run_inspect
from slayer.inspect.service import InspectService
from slayer.mcp.server import create_mcp_server
from slayer.memories.help_seed import HelpTopic, load_help_topics, merge_help_topics, seed_help_memories
from slayer.storage.yaml_storage import YAMLStorage

from tests._cli_inprocess import run_cli_in_process

_HOST = HelpTopic(
    id="help.motley.x",
    learning="Host topic opening paragraph.\n\nHost topic closing paragraph that only the full body has.",
    description="Host-only help.",
)
_USER = dict(
    id="notes.cents",
    learning="Amounts are cents.\n\nUser memory closing paragraph that only the full body has.",
    description="Amounts are stored in cents.",
)
_TOPICS = {t.id: t for t in merge_help_topics(load_help_topics(), extra=[_HOST])}


def _tail(memory_id: str) -> str:
    """A chunk only the full body carries."""
    learning = _USER["learning"] if memory_id == _USER["id"] else _TOPICS[memory_id].learning
    return learning.strip()[-50:]


async def _seed(storage: YAMLStorage) -> None:
    await seed_help_memories(storage, topics=tuple(_TOPICS.values()))
    await storage.save_memory(entities=[], **_USER)


@pytest.fixture
def storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_seed(st))
        finally:
            loop.close()
        yield st


# --------------------------------------------------------------------------- #
# InspectService (shared by every surface)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("memory_id", ["help.intro", "help.queries", "help.motley.x"])
async def test_default_compact_returns_full_help_body(storage: YAMLStorage, memory_id: str) -> None:
    out = await InspectService(storage=storage).inspect(reference=f"memory:{memory_id}", entity_type="memory")
    assert _tail(memory_id) in out


async def test_explicit_compact_still_returns_full_help_body(storage: YAMLStorage) -> None:
    out = await InspectService(storage=storage).inspect(
        reference="memory:help.motley.x", entity_type="memory", compact=True,
    )
    assert _tail("help.motley.x") in out


async def test_json_carries_the_full_help_text(storage: YAMLStorage) -> None:
    out = await InspectService(storage=storage).inspect(
        reference="memory:help.intro", entity_type="memory", format="json",
    )
    assert _tail("help.intro") in json.loads(out)["text"]


async def test_batch_returns_every_full_help_body(storage: YAMLStorage) -> None:
    out = await InspectService(storage=storage).inspect(
        reference=["memory:help.intro", "memory:help.time"], entity_type="memory",
    )
    assert _tail("help.intro") in out
    assert _tail("help.time") in out


async def test_json_batch_carries_every_full_help_text(storage: YAMLStorage) -> None:
    out = await InspectService(storage=storage).inspect(
        reference=["memory:help.intro", "memory:help.time"], entity_type="memory", format="json",
    )
    texts = [item["text"] for item in json.loads(out)]
    assert _tail("help.intro") in texts[0]
    assert _tail("help.time") in texts[1]


async def test_descriptions_max_chars_still_truncates_help(storage: YAMLStorage) -> None:
    out = await InspectService(storage=storage).inspect(
        reference="memory:help.intro", entity_type="memory", descriptions_max_chars=40,
    )
    assert _tail("help.intro") not in out
    assert "truncated" in out


async def test_other_memories_stay_compact(storage: YAMLStorage) -> None:
    out = await InspectService(storage=storage).inspect(reference=f"memory:{_USER['id']}", entity_type="memory")
    assert _USER["description"] in out
    assert _tail(_USER["id"]) not in out


# --------------------------------------------------------------------------- #
# Surfaces: markdown / json × single / batch
# --------------------------------------------------------------------------- #
_SHAPES = [
    pytest.param("markdown", ["help.intro"], id="markdown-single"),
    pytest.param("json", ["help.intro"], id="json-single"),
    pytest.param("markdown", ["help.intro", "help.motley.x"], id="markdown-batch"),
    pytest.param("json", ["help.intro", "help.motley.x"], id="json-batch"),
]


def _reference(ids: list[str]) -> str | list[str]:
    refs = [f"memory:{i}" for i in ids]
    return refs[0] if len(refs) == 1 else refs


def _assert_full_bodies(out: str, *, fmt: str, ids: list[str]) -> None:
    if fmt == "json":
        payload = json.loads(out)
        out = "\n".join(item["text"] for item in (payload if isinstance(payload, list) else [payload]))
    for memory_id in ids:
        assert _tail(memory_id) in out, memory_id


@pytest.mark.parametrize(("fmt", "ids"), _SHAPES)
async def test_mcp_inspect_tool_returns_full_help_body(storage: YAMLStorage, fmt: str, ids: list[str]) -> None:
    server = create_mcp_server(storage=storage, _seed_help=False)
    blocks, _ = cast(Any, await server.call_tool(
        name="inspect", arguments={"reference": _reference(ids), "entity_type": "memory", "format": fmt},
    ))
    _assert_full_bodies(blocks[0].text, fmt=fmt, ids=ids)


@pytest.mark.parametrize(("fmt", "ids"), _SHAPES)
def test_rest_inspect_returns_full_help_body(storage: YAMLStorage, fmt: str, ids: list[str]) -> None:
    client = TestClient(create_app(storage=storage))
    r = client.post("/inspect", json={"reference": _reference(ids), "entity_type": "memory", "format": fmt})
    assert r.status_code == 200
    _assert_full_bodies(r.json()["result"], fmt=fmt, ids=ids)


@pytest.mark.parametrize(("fmt", "ids"), _SHAPES)
def test_cli_inspect_returns_full_help_body(storage: YAMLStorage, fmt: str, ids: list[str]) -> None:
    result = run_cli_in_process([
        "inspect", *(f"memory:{i}" for i in ids), "--type", "memory", "--format", fmt,
        "--storage", storage.base_dir,
    ])
    assert result.returncode == 0, result.stderr
    _assert_full_bodies(result.stdout, fmt=fmt, ids=ids)


def test_cli_run_inspect_explicit_compact_host_topic(storage: YAMLStorage) -> None:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _run_inspect(args=SimpleNamespace(
            reference="memory:help.motley.x", entity_type="memory", compact=True, format="markdown",
            num_rows=3, show_sql=False, sections=None, descriptions_max_chars=None,
        ), storage=storage)
    assert _tail("help.motley.x") in buf.getvalue()


@pytest.mark.parametrize(("fmt", "ids"), _SHAPES)
async def test_client_inspect_returns_full_help_body(storage: YAMLStorage, fmt: str, ids: list[str]) -> None:
    out = await SlayerClient(storage=storage).inspect(reference=_reference(ids), entity_type="memory", format=fmt)
    _assert_full_bodies(out, fmt=fmt, ids=ids)
