"""DEV-1549 round 4: ``edit_datasource`` must refresh the datasource
embedding so a description edit propagates to the dense-search channel
without waiting for the next ``slayer ingest``.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Generator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def yaml_storage() -> Generator[YAMLStorage, None, None]:
    tmpdir = tempfile.mkdtemp()
    try:
        yield YAMLStorage(base_dir=tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def _call(mcp_server, *, name: str, arguments: dict) -> str:
    content_blocks, _ = await mcp_server.call_tool(name=name, arguments=arguments)
    return content_blocks[0].text


@pytest.mark.asyncio
async def test_edit_datasource_calls_refresh_datasource_with_new_description(
    yaml_storage: YAMLStorage, monkeypatch,
) -> None:
    """Pin the refresh-on-edit contract: ``SearchService.refresh_datasource``
    must be called with the post-edit description so the persisted
    embedding row reflects the new text."""
    await yaml_storage.save_datasource(DatasourceConfig(
        name="warehouse",
        type="sqlite",
        database=":memory:",
        description="old description",
    ))
    await yaml_storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="warehouse",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    ))

    refresh_calls: list = []

    from slayer.search import service as svc_mod

    original = svc_mod.SearchService.refresh_datasource

    async def recording_refresh(self, *, name, models, description=None):  # NOSONAR(S7503) — async required to match the monkeypatched coroutine (`await SearchService.refresh_datasource(...)`)
        refresh_calls.append({
            "name": name,
            "description": description,
            "model_count": len(models),
        })
        return await original(
            self, name=name, models=models, description=description,
        )

    monkeypatch.setattr(
        svc_mod.SearchService, "refresh_datasource", recording_refresh,
    )

    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="edit_datasource", arguments={
        "name": "warehouse",
        "description": "fresh new description with reconciliation_token_zeta",
    })
    assert "updated" in result.lower()

    matching = [c for c in refresh_calls if c["name"] == "warehouse"]
    assert matching, "edit_datasource must call refresh_datasource"
    last = matching[-1]
    assert last["description"] == (
        "fresh new description with reconciliation_token_zeta"
    )
    # The model list must reach the refresh path so the rendered text
    # (which mentions visible models) is regenerated correctly.
    assert last["model_count"] == 1


@pytest.mark.asyncio
async def test_edit_datasource_refresh_failure_is_best_effort(
    yaml_storage: YAMLStorage, monkeypatch,
) -> None:
    """Round-7 (CodeRabbit): when ``refresh_datasource`` raises after the
    save commits, the tool reports partial success rather than failing
    the whole update. The persisted change must survive."""
    await yaml_storage.save_datasource(DatasourceConfig(
        name="warehouse", type="sqlite", database=":memory:",
        description="old",
    ))

    from slayer.search import service as svc_mod

    async def raising_refresh(self, *, name, models, description=None):  # NOSONAR(S7503) — async required to match the monkeypatched coroutine
        raise RuntimeError("simulated embedding outage")

    monkeypatch.setattr(
        svc_mod.SearchService, "refresh_datasource", raising_refresh,
    )

    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="edit_datasource", arguments={
        "name": "warehouse",
        "description": "new description value",
    })
    assert "updated" in result.lower()
    assert "Warning" in result
    assert "simulated embedding outage" in result

    # The datasource change is durable even though refresh raised.
    reloaded = await yaml_storage.get_datasource("warehouse")
    assert reloaded is not None
    assert reloaded.description == "new description value"


@pytest.mark.asyncio
async def test_edit_datasource_no_refresh_when_description_unchanged(
    yaml_storage: YAMLStorage, monkeypatch,
) -> None:
    """The refresh is a no-op when description didn't actually change —
    saves a litellm round-trip on inert edits."""
    await yaml_storage.save_datasource(DatasourceConfig(
        name="warehouse", type="sqlite", database=":memory:",
        description="same description",
    ))

    refresh_calls: list = []
    from slayer.search import service as svc_mod

    async def recording_refresh(self, *, name, models, description=None):  # NOSONAR(S7503) — async required to match the monkeypatched coroutine
        refresh_calls.append(name)
        return []

    monkeypatch.setattr(
        svc_mod.SearchService, "refresh_datasource", recording_refresh,
    )

    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="edit_datasource", arguments={
        "name": "warehouse",
        "description": "same description",  # unchanged
    })
    assert "updated" in result.lower()
    assert refresh_calls == [], (
        "refresh should be skipped when description didn't change"
    )


@pytest.mark.asyncio
async def test_edit_datasource_unknown_no_refresh(
    yaml_storage: YAMLStorage, monkeypatch,
) -> None:
    """Editing a missing datasource returns a friendly error AND does
    not trigger any refresh call (otherwise we'd embed nothing under
    the unknown name)."""
    refresh_calls: list = []

    from slayer.search import service as svc_mod

    async def recording_refresh(self, *, name, models, description=None):  # NOSONAR(S7503) — async required to match the monkeypatched coroutine (`await SearchService.refresh_datasource(...)`)
        refresh_calls.append(name)
        return []

    monkeypatch.setattr(
        svc_mod.SearchService, "refresh_datasource", recording_refresh,
    )

    mcp = create_mcp_server(storage=yaml_storage)
    result = await _call(mcp, name="edit_datasource", arguments={
        "name": "does_not_exist",
        "description": "x",
    })
    assert "not found" in result.lower()
    assert refresh_calls == []
