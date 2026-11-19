"""MCP registers only the new tools and converts pre-2.0 arguments, kinds, sections and help references inside
them; REST and the CLI keep deprecated twins that report the deprecation; tool descriptions and help topics use the
new words.

Spec: compatibility/ossie-vocabulary › Surfaces follow the vocabulary.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.models import SlayerDataset
from slayer.mcp.server import create_mcp_server
from slayer.memories.help_seed import HELP_TOPICS, seed_help_memories
from slayer.storage.base import StorageBackend
from tests._cli_inprocess import CliResult, run_cli_in_process
from tests._deprecated_input_helpers import deprecated_in, mcp_payload, recorded
from tests._model_access_fixtures import mcp_text
from tests._ossie_vocabulary_fixtures import shop_customers, shop_orders, shop_store
from tests._stored_upgrade_fixtures import DS

NEW_TOOLS = {"datasets_summary", "create_dataset", "edit_dataset", "delete_dataset", "validate_datasets",
             "recommend_population", "ingest_datasource"}
OLD_TOOLS = {"models_summary", "create_model", "edit_model", "delete_model", "validate_models",
             "recommend_root_model", "ingest_datasource_models"}
ITEMS = ["customers.region", "sum(orders.amount)"]
DEPRECATED = "Deprecated: "

# Words the rename retires everywhere, and pre-2.0 identifiers; ``model``, ``column`` and ``join`` keep carve-outs.
RETIRED_WORDS = re.compile(r"\b(measures?|formulas?)\b", re.IGNORECASE)
RETIRED_IDENTIFIERS = re.compile(
    r"\b(source_model|target_model|model_name|models_summary|create_model|edit_model|delete_model|validate_models"
    r"|recommend_root_model|ingest_datasource_models|help\.models|help\.joins)\b"
)


async def seeded(base: Path) -> StorageBackend:
    storage = await shop_store(backend="yaml", base=str(base))
    await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
    await storage.save_dataset(SlayerDataset.model_validate(shop_orders()))
    return storage


def reported(text: str, *, old: str, new: str) -> bool:
    """``text`` carries a deprecation naming ``old``, ``new`` and SLayer 3.0."""
    return any(old in line and new in line and "3.0" in line for line in text.splitlines())


def without_warnings(body: Any) -> Any:
    return {k: v for k, v in body.items() if k != "warnings"} if isinstance(body, dict) else body


def headers_of(resp: Any) -> set[str]:
    return {h.lower() for h in resp.headers}


class TestMcpTools:
    async def test_only_the_new_tools_are_listed(self, tmp_path: Path) -> None:
        names = {t.name for t in await create_mcp_server(storage=await seeded(tmp_path), _seed_help=False).list_tools()}
        assert NEW_TOOLS <= names
        assert not OLD_TOOLS & names

    async def test_tool_descriptions_use_the_new_words(self, tmp_path: Path) -> None:
        tools = await create_mcp_server(storage=await seeded(tmp_path), _seed_help=False).list_tools()
        for tool in tools:
            text = f"{tool.description}\n{json.dumps(tool.inputSchema)}"
            assert not RETIRED_WORDS.findall(text), (tool.name, RETIRED_WORDS.findall(text))
            assert not RETIRED_IDENTIFIERS.findall(text), (tool.name, RETIRED_IDENTIFIERS.findall(text))

    @pytest.mark.parametrize("topic", HELP_TOPICS, ids=lambda t: t.id)
    def test_help_topics_use_the_new_words(self, topic: Any) -> None:
        assert not RETIRED_WORDS.findall(topic.learning), RETIRED_WORDS.findall(topic.learning)
        assert not RETIRED_IDENTIFIERS.findall(topic.learning), RETIRED_IDENTIFIERS.findall(topic.learning)
        assert not RETIRED_WORDS.findall(topic.description)


class TestMcpArguments:
    async def test_old_edit_arguments_convert(self, tmp_path: Path) -> None:
        storage = await seeded(tmp_path)
        with recorded():
            reply = json.loads(await mcp_text(
                storage, "edit_dataset", model_name="orders", data_source=DS,
                columns=[{"name": "big", "sql": "amount > 30", "datatype": "Boolean"}],
                measures=[{"name": "n", "expression": "count(*)"}],
                joins=[{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], "name": "buyer"}],
            ))
        warnings = "\n".join(reply.get("warnings", []))
        for old, new in (("model_name", "dataset_name"), ("columns", "fields"), ("measures", "metrics"),
                         ("joins", "relationships")):
            assert reported(warnings, old=old, new=new), (old, warnings)
        dataset = await storage.get_dataset("orders", data_source=DS)
        assert dataset is not None
        assert "big" in {f.name for f in dataset.fields}
        assert "n" in {m.name for m in dataset.metrics}
        assert "buyer" in {r.name for r in dataset.relationships}

    async def test_an_old_remove_key_converts(self, tmp_path: Path) -> None:
        storage = await seeded(tmp_path)
        with recorded():
            reply = json.loads(await mcp_text(
                storage, "edit_dataset", dataset_name="orders", data_source=DS, remove={"measures": ["revenue"]},
            ))
        assert reported("\n".join(reply.get("warnings", [])), old="measures", new="metrics")
        dataset = await storage.get_dataset("orders", data_source=DS)
        assert dataset is not None
        assert "revenue" not in {m.name for m in dataset.metrics}

    @pytest.mark.parametrize(("old", "new"), [("columns", "fields"), ("measures", "metrics")])
    async def test_an_old_create_argument_converts(self, tmp_path: Path, old: str, new: str) -> None:
        storage = await seeded(tmp_path)
        orders = shop_orders()
        args = {"name": "orders2", "sql_table": "orders", "data_source": DS, "fields": orders["fields"],
                "metrics": orders["metrics"]}
        args[old] = args.pop(new)
        with recorded():
            text = await mcp_text(storage, "create_dataset", **args)
        assert reported(text, old=old, new=new), text
        dataset = await storage.get_dataset("orders2", data_source=DS)
        assert dataset is not None
        assert getattr(dataset, new)

    async def test_old_query_keys_convert_in_the_query_tool(self, tmp_path: Path) -> None:
        storage = await seeded(tmp_path)
        new = mcp_payload(await mcp_text(
            storage, "query", query={"population": "orders", "metrics": ["sum(orders.amount)"]}, format="json"))
        with recorded():
            old = mcp_payload(await mcp_text(
                storage, "query", query={"source_model": "orders", "measures": ["sum(amount)"]}, format="json"))
        assert old["data"] == new["data"]
        assert sorted((w["old"], w["new"]) for w in deprecated_in(old["warnings"])) == [
            ("measures", "metrics"), ("source_model", "population")]


class TestMcpInspect:
    @pytest.mark.parametrize(("old", "new", "reference"), [
        ("model", "dataset", f"{DS}.orders"),
        ("column", "field", f"{DS}.orders.amount"),
        ("measure", "metric", f"{DS}.orders.revenue"),
    ])
    async def test_an_old_kind_converts(self, tmp_path: Path, old: str, new: str, reference: str) -> None:
        storage = await seeded(tmp_path)
        expected = await mcp_text(storage, "inspect", entity_type=new, reference=reference, compact=False)
        with recorded():
            converted = await mcp_text(storage, "inspect", entity_type=old, reference=reference, compact=False)
        assert expected in converted
        assert reported(converted.replace(expected, ""), old=old, new=new), converted

    @pytest.mark.parametrize("fmt", ["markdown", "json"])
    async def test_an_old_kind_converts_for_a_collection(self, tmp_path: Path, fmt: str) -> None:
        storage = await seeded(tmp_path)
        expected = await mcp_text(storage, "inspect", entity_type="dataset", format=fmt)
        with recorded():
            converted = await mcp_text(storage, "inspect", entity_type="model", format=fmt)
        if fmt == "json":
            payload, fresh = json.loads(converted), json.loads(expected)
            assert without_warnings(payload) == without_warnings(fresh)
            added = [w for w in payload["warnings"] if w not in fresh.get("warnings", [])]
            assert reported("\n".join(added), old="model", new="dataset"), payload["warnings"]
        else:
            assert expected in converted
            assert reported(converted.replace(expected, ""), old="model", new="dataset"), converted

    @pytest.mark.parametrize(("old", "new"), [("columns", "fields"), ("measures", "metrics"),
                                              ("joins", "relationships")])
    async def test_an_old_section_converts(self, tmp_path: Path, old: str, new: str) -> None:
        storage = await seeded(tmp_path)
        expected = await mcp_text(storage, "inspect", entity_type="dataset", reference=f"{DS}.orders",
                                  compact=False, sections=[new])
        with recorded():
            converted = await mcp_text(storage, "inspect", entity_type="dataset", reference=f"{DS}.orders",
                                       compact=False, sections=[old])
        assert expected in converted
        assert reported(converted.replace(expected, ""), old=old, new=new), converted

    @pytest.mark.parametrize(("old", "new"), [("memory:help.joins", "memory:help.relationships"),
                                              ("memory:help.models", "memory:help.datasets")])
    async def test_an_old_help_reference_resolves(self, tmp_path: Path, old: str, new: str) -> None:
        storage = await seeded(tmp_path)
        await seed_help_memories(storage)
        expected = await mcp_text(storage, "inspect", entity_type="memory", reference=new, compact=False)
        with recorded():
            converted = await mcp_text(storage, "inspect", entity_type="memory", reference=old, compact=False)
        assert expected in converted
        assert reported(converted.replace(expected, ""), old=old, new=new), converted


class TestRest:
    @pytest.mark.parametrize(("method", "old", "new", "body"), [
        ("get", "/models", "/datasets", None),
        ("get", "/models/orders", "/datasets/orders", None),
        ("post", "/validate-models", "/validate-datasets", {"data_source": DS}),
        ("post", "/recommend-root-model", "/recommend-population", {"items": ITEMS, "data_source": DS}),
    ])
    async def test_an_old_route_answers_as_its_replacement(
        self, tmp_path: Path, method: str, old: str, new: str, body: dict[str, Any] | None,
    ) -> None:
        client = TestClient(create_app(storage=await seeded(tmp_path)))
        call = getattr(client, method)
        kwargs: dict[str, Any] = {"json": body} if body is not None else {"params": {"data_source": DS}}
        fresh = call(new, **kwargs)
        with recorded():
            legacy = call(old, **kwargs)
        assert fresh.status_code == legacy.status_code == 200, (fresh.text, legacy.text)
        assert without_warnings(legacy.json()) == without_warnings(fresh.json())
        if isinstance(legacy.json(), dict) and "warnings" in legacy.json():
            assert deprecated_in(legacy.json()["warnings"])
        assert "deprecation" in headers_of(legacy)
        assert "deprecation" not in headers_of(fresh)

    async def test_an_old_create_route_saves_and_reports(self, tmp_path: Path) -> None:
        storage = await seeded(tmp_path)
        client = TestClient(create_app(storage=storage))
        with recorded():
            resp = client.post("/models", json={**shop_orders(), "name": "orders2"})
        assert resp.status_code == 200, resp.text
        assert "deprecation" in headers_of(resp)
        assert any("/models" in w["old"] for w in deprecated_in(resp.json()["warnings"]))
        assert await storage.get_dataset("orders2", data_source=DS) is not None

    async def test_an_old_update_route_saves_and_reports(self, tmp_path: Path) -> None:
        storage = await seeded(tmp_path)
        client = TestClient(create_app(storage=storage))
        body = shop_orders(metrics=[{"name": "n", "expression": "count(*)"}])
        with recorded():
            resp = client.put("/models/orders", json=body)
        assert resp.status_code == 200, resp.text
        assert "deprecation" in headers_of(resp)
        assert any("/models" in w["old"] for w in deprecated_in(resp.json()["warnings"]))
        dataset = await storage.get_dataset("orders", data_source=DS)
        assert dataset is not None
        assert [m.name for m in dataset.metrics] == ["n"]

    async def test_an_old_delete_route_deletes(self, tmp_path: Path) -> None:
        storage = await seeded(tmp_path)
        client = TestClient(create_app(storage=storage))
        with recorded():
            resp = client.delete("/models/orders", params={"data_source": DS})
        assert resp.status_code == 200, resp.text
        assert "deprecation" in headers_of(resp)
        assert await storage.get_dataset("orders", data_source=DS) is None


def run(argv: list[str], *, base: str) -> CliResult:
    return run_cli_in_process(argv, env={**os.environ, "SLAYER_STORAGE": base})


def deprecated_lines(result: CliResult) -> list[str]:
    return [line for line in result.stderr.splitlines() if line.startswith(DEPRECATED)]


class TestCli:
    @pytest.mark.parametrize(("old", "new"), [
        (["models", "list"], ["datasets", "list"]),
        (["models", "show", "orders"], ["datasets", "show", "orders"]),
        (["validate-models", "--datasource", DS, "--format", "json"],
         ["validate-datasets", "--datasource", DS, "--format", "json"]),
        (["validate-datasets", "--datasource", DS, "--model", "orders", "--format", "json"],
         ["validate-datasets", "--datasource", DS, "--dataset", "orders", "--format", "json"]),
        (["recommend-root-model", *ITEMS, "--data-source", DS, "--format", "json"],
         ["recommend-population", *ITEMS, "--data-source", DS, "--format", "json"]),
        (["datasources", "refresh-samples", "--data-source", DS, "--model", "orders", "--format", "json"],
         ["datasources", "refresh-samples", "--data-source", DS, "--dataset", "orders", "--format", "json"]),
        (["inspect", f"{DS}.orders", "--type", "model"], ["inspect", f"{DS}.orders", "--type", "dataset"]),
    ], ids=["models_list", "models_show", "validate_models", "validate_model_flag", "recommend_root_model",
            "refresh_samples_model_flag", "inspect_type_model"])
    async def test_an_old_command_or_flag_runs_as_its_replacement(
        self, tmp_path: Path, old: list[str], new: list[str],
    ) -> None:
        await seeded(tmp_path)
        fresh = run(new, base=str(tmp_path))
        with recorded():
            legacy = run(old, base=str(tmp_path))
        assert fresh.returncode == legacy.returncode == 0, (fresh.stderr, legacy.stderr)
        assert legacy.stdout == fresh.stdout
        assert len(deprecated_lines(legacy)) == 1, legacy.stderr
        assert deprecated_lines(fresh) == []

    async def test_export_osi_takes_datasets(self, tmp_path: Path) -> None:
        await seeded(tmp_path)
        out_new, out_old = tmp_path / "new.osi.yaml", tmp_path / "old.osi.yaml"
        fresh = run(["export-osi", "--datasource", DS, "--datasets", "orders", "customers", "--out", str(out_new)],
                    base=str(tmp_path))
        with recorded():
            legacy = run(["export-osi", "--datasource", DS, "--models", "orders", "customers", "--out", str(out_old)],
                         base=str(tmp_path))
        assert fresh.returncode == legacy.returncode == 0, (fresh.stderr, legacy.stderr)
        assert out_new.read_text() == out_old.read_text()
        assert len(deprecated_lines(legacy)) == 1
        assert deprecated_lines(fresh) == []

    async def test_import_dbt_takes_include_hidden_datasets(self, tmp_path: Path) -> None:
        await seeded(tmp_path)
        missing = str(tmp_path / "no_such_dbt_project")
        fresh = run(["import-dbt", missing, "--datasource", DS, "--include-hidden-datasets"], base=str(tmp_path))
        with recorded():
            legacy = run(["import-dbt", missing, "--datasource", DS, "--include-hidden-models"], base=str(tmp_path))
        assert "unrecognized arguments" not in fresh.stderr
        assert legacy.returncode == fresh.returncode
        rest = [line for line in legacy.stderr.splitlines() if not line.startswith(DEPRECATED)]
        assert rest == fresh.stderr.splitlines()
        assert len(deprecated_lines(legacy)) == 1
