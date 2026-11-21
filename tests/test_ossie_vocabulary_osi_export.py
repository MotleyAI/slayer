"""The ``SLAYER`` vendor payload of an exported Ossie document is written at version 3 in the Ossie vocabulary;
a native load reads a version-1 payload, checked against its fingerprint as written.

``tests/fixtures/ossie_vocabulary/export_payload_v1.osi.yaml`` was exported by the pre-rename release.

Spec: compatibility/ossie-vocabulary › The Ossie export payload carries the vocabulary.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import yaml

from slayer.storage.yaml_storage import YAMLStorage
from tests._deprecated_input_helpers import recorded
from tests._dataset_access_fixtures import mcp_text
from tests._osi_export_fixtures import customers, monthly_revenue, rich_orders
from tests._osi_export_helpers import (
    Env,
    entities,
    export,
    make_env,
    payload,
    reimport,
    save,
    slayer_entries,
    stored,
)

V1_DOCUMENT = Path(__file__).parent / "fixtures" / "ossie_vocabulary" / "export_payload_v1.osi.yaml"
STALE = re.compile(r"edited|stale|fingerprint", re.IGNORECASE)
PRE_2_0_PAYLOAD_KEYS = frozenset({
    "columns", "column", "measures", "measure", "joins", "join", "model", "type", "formula", "target_model",
    "source_model", "dimension",
})


def payload_keys(value: Any) -> set[str]:
    """Every key in ``value``, except inside a ``format`` (a number format keeps its ``type``)."""
    if isinstance(value, list):
        return {k for v in value for k in payload_keys(v)}
    if not isinstance(value, dict):
        return set()
    return {*value} | {k for key, v in value.items() if key != "format" for k in payload_keys(v)}


def slayer_payloads(document: dict[str, Any]) -> list[dict[str, Any]]:
    return [payload(e) for e in entities(document) if slayer_entries(e)]


def v1_document() -> dict[str, Any]:
    return yaml.safe_load(V1_DOCUMENT.read_text())


async def inspected(env: Env, dataset: str) -> str:
    return await mcp_text(YAMLStorage(base_dir=str(env.store)), "inspect", reference=f"{env.datasource}.{dataset}",
                          entity_type="dataset", compact=False)


async def stored_datasets(env: Env) -> dict[str, dict[str, Any]]:
    storage = YAMLStorage(base_dir=str(env.store))
    out: dict[str, dict[str, Any]] = {}
    for name in await storage.list_datasets(data_source=env.datasource):
        dataset = await storage.get_dataset(name, data_source=env.datasource)
        assert dataset is not None
        out[name] = dataset.model_dump(mode="json", exclude={"version"})
    return out


def test_the_v1_fixture_is_a_pre_rename_export() -> None:
    bodies = slayer_payloads(v1_document())
    assert bodies
    assert {b["version"] for b in bodies} == {1}
    assert PRE_2_0_PAYLOAD_KEYS & payload_keys(bodies)


async def test_an_export_writes_payload_version_3_in_the_new_keys(tmp_path: Path) -> None:
    env = await make_env(tmp_path)
    await save(env, customers(), rich_orders(), monthly_revenue())
    bodies = slayer_payloads((await export(env)).document)
    assert {b["kind"] for b in bodies} == {"dataset", "field", "metric", "relationship"}
    assert {b["version"] for b in bodies} == {3}
    assert not PRE_2_0_PAYLOAD_KEYS & payload_keys(bodies), sorted(PRE_2_0_PAYLOAD_KEYS & payload_keys(bodies))
    fields = [b["field"] for b in bodies if b["kind"] == "field"]
    fields += [f for b in bodies if b["kind"] == "dataset" for f in b.get("fields") or []]
    assert not [f for f in fields if "sql" in f]


async def test_a_v1_payload_matches_its_fingerprint_as_written(tmp_path: Path) -> None:
    env = await make_env(tmp_path)
    with recorded() as caught:
        imported, _ = await reimport(env, v1_document())
    assert [w for w in caught if issubclass(w.category, FutureWarning)] == []
    # The SLAYER-only measure applies, so the payload was read.
    assert (await stored(imported, "orders")).get_metric("running") is not None
    assert not STALE.search(await inspected(imported, "orders"))


async def test_an_edited_v1_payload_fails_its_fingerprint(tmp_path: Path) -> None:
    env = await make_env(tmp_path)
    document = copy.deepcopy(v1_document())
    [orders] = [b for b in slayer_payloads(document) if (b["kind"], b["entity"]) == ("dataset", "orders")]
    running = next(m for m in orders["measures"] if m["name"] == "running")
    running["formula"] = "cumsum(n)"
    imported, _ = await reimport(env, document)
    # Stale payload: a load warning naming the dataset; its SLayer-only measure applies as edited.
    running = (await stored(imported, "orders")).get_metric("running")
    assert running is not None and running.expression == "cumsum(n)"
    text = await inspected(imported, "orders")
    assert "orders" in text
    assert "slayer" in text.lower()
    assert STALE.search(text), text


async def test_a_v1_export_reexports_its_payloads_unchanged(tmp_path: Path) -> None:
    env = await make_env(tmp_path / "src")
    with recorded() as caught:
        from_v1, _ = await reimport(env, v1_document(), name="from_v1")
    assert [w for w in caught if issubclass(w.category, FutureWarning)] == []
    # A one-document datasource re-exports verbatim: its payloads stay at version 1.
    re_export = await export(from_v1)
    assert {b["version"] for b in slayer_payloads(re_export.document)} == {1}
    from_re_export, _ = await reimport(env, re_export, name="from_re_export")
    imported_v1 = await stored_datasets(from_v1)
    assert sorted(imported_v1) == ["customers", "monthly_revenue", "orders"]
    assert imported_v1 == await stored_datasets(from_re_export)
