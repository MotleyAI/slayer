"""Every MCP tool's title, behaviour annotations and output schema, and the behaviour its hints promise."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import yaml
from mcp.client import Client
from mcp.types import CallToolResult, TextContent, Tool, ToolAnnotations

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.memories.service import MemoryService
import slayer.mcp.server as server_module
from slayer.mcp.server import _import_mcp_sdk, create_mcp_server
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2243_fixtures import dataset, document_text, field
from tests._dev2474_fixtures import SHOP_ONTOLOGY, concept, ontology_text, order_mapping

#: name -> (title, read-only, destructive, idempotent, open-world), the spec table.
TOOLS: dict[str, tuple[str, bool, bool, bool, bool]] = {
    "query": ("Query data", True, False, True, True),
    "inspect": ("Inspect entity", True, False, True, True),
    "search": ("Search memories and entities", True, False, True, True),
    "describe_datasource": ("Describe datasource", True, False, True, True),
    "validate_datasets": ("Validate datasets", True, False, True, True),
    "datasets_summary": ("Summarize datasets", True, False, True, False),
    "list_datasources": ("List datasources", True, False, True, False),
    "get_datasource_priority": ("Get datasource priority", True, False, True, False),
    "recommend_population": ("Recommend population", True, False, True, False),
    "create_dataset": ("Create dataset", False, True, True, True),
    "edit_dataset": ("Edit dataset", False, True, True, True),
    "create_datasource": ("Create datasource", False, True, True, True),
    "ingest_datasource": ("Ingest datasource", False, True, True, True),
    "edit_datasource": ("Edit datasource", False, True, True, False),
    "set_datasource_priority": ("Set datasource priority", False, True, True, False),
    "delete_dataset": ("Delete dataset", False, True, True, False),
    "delete_datasource": ("Delete datasource", False, True, True, False),
    "create_semantic_model": ("Create semantic model", False, True, True, False),
    "edit_semantic_model": ("Edit semantic model", False, True, True, False),
    "delete_semantic_model": ("Delete semantic model", False, True, True, False),
    "create_ontology": ("Create ontology", False, True, True, False),
    "edit_ontology": ("Edit ontology", False, True, True, False),
    "delete_ontology": ("Delete ontology", False, True, True, False),
    "create_ontology_mapping": ("Create ontology mapping", False, True, True, False),
    "edit_ontology_mapping": ("Edit ontology mapping", False, True, True, False),
    "delete_ontology_mapping": ("Delete ontology mapping", False, True, True, False),
    "forget_memory": ("Forget memory", False, True, True, False),
    "save_memory": ("Save memory", False, True, False, False),
}

DS = "shop"
MEMORY_ID = "kb.seed"
CLIENT_REFUSAL = "zqxj database client requested"
_SAMPLE_KEYS = frozenset({"sampled", "sampled_values", "distinct_count", "fingerprint"})
_HINTS = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)


# --------------------------------------------------------------------------- #
# Seeded storage
# --------------------------------------------------------------------------- #
def _seed_database(path: Path) -> None:
    with transaction(str(path)) as conn:
        conn.executescript(
            "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id TEXT, amount REAL);"
            "CREATE TABLE customers (customer_id TEXT PRIMARY KEY, name TEXT);"
            "CREATE TABLE products (product_id INTEGER PRIMARY KEY, name TEXT);"
            "INSERT INTO customers VALUES ('c1', 'Ann'), ('c2', 'Bob');"
            "INSERT INTO orders VALUES (1, 'c1', 10.0), (2, 'c2', 20.0), (3, 'c1', 5.0);"
            "INSERT INTO products VALUES (1, 'pen');"
        )


async def _seed(base: Path) -> YAMLStorage:
    """A datasource over a live SQLite file holding table-backed, query-backed and document datasets, an ontology
    with a mapping, a spare ontology, a memory and a priority list."""
    db = base / "shop.db"
    _seed_database(db)
    storage = YAMLStorage(base_dir=str(base / "store"))
    await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=str(db)))
    await storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[
            DatasetField(name="order_id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["customer_id"])],
    ))
    await storage.save_dataset(SlayerDataset(
        name="customers", sql_table="customers", data_source=DS,
        fields=[
            DatasetField(name="customer_id", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
        ],
    ))
    engine = SlayerQueryEngine(storage=storage)
    try:
        await engine.save_dataset(SlayerDataset(
            name="customer_totals", data_source=DS,
            source_queries=[SlayerQuery.model_validate({
                "population": "orders", "metrics": ["sum(orders.amount)"], "dimensions": ["orders.customer_id"],
            })],
        ))
    finally:
        await engine.aclose()
        engine.close()
    await storage.save_semantic_model_text(data_source=DS, name="catalog", text=_catalog_model())
    await storage.save_ontology_text(name="shop", text=SHOP_ONTOLOGY)
    await storage.save_ontology_text(name="spare", text=ontology_text(concept("Thing"), name="spare"))
    await storage.save_ontology_mapping_text(data_source=DS, name="shop_mapping",
                                             text=order_mapping(semantic_model=DS))
    await MemoryService(storage=storage).save_memory(
        learning="orders.amount is in USD", linked_entities=["orders.amount"], id=MEMORY_ID,
    )
    await storage.set_datasource_priority([DS])
    return storage


def _catalog_model(name: str = "catalog", *, dataset_name: str = "products") -> str:
    return document_text(name=name, datasets=[
        dataset(dataset_name, source="products", fields=[field("product_id", datatype="Integer"), field("name")]),
    ])


def _refusing_factory(calls: list[DatasourceConfig]) -> Any:
    def factory(datasource: DatasourceConfig) -> Any:
        calls.append(datasource)
        raise RuntimeError(CLIENT_REFUSAL)
    return factory


@contextlib.asynccontextmanager
async def _client(storage: YAMLStorage, **server_kwargs: Any) -> AsyncGenerator[Client, None]:
    server = create_mcp_server(storage=storage, _seed_help=False, **server_kwargs)
    try:
        async with Client(server, mode="legacy") as connected:
            yield connected
    finally:
        await getattr(server, "_slayer_engine").aclose()


async def _listed_tools(tmp_path: Path) -> dict[str, Tool]:
    async with _client(YAMLStorage(base_dir=str(tmp_path))) as client:
        return {t.name: t for t in (await client.list_tools()).tools}


def _text(result: CallToolResult) -> str:
    (block,) = result.content
    assert isinstance(block, TextContent)
    return block.text


# --------------------------------------------------------------------------- #
# Titles, hints and output schemas
# --------------------------------------------------------------------------- #
class TestToolTable:
    async def test_tool_names_match_the_table(self, tmp_path: Path) -> None:
        assert set(await _listed_tools(tmp_path)) == set(TOOLS)

    async def test_titles_and_hints_match_the_table(self, tmp_path: Path) -> None:
        tools = await _listed_tools(tmp_path)
        for name, (title, read_only, destructive, idempotent, open_world) in TOOLS.items():
            tool = tools[name]
            assert tool.title == title, name
            assert tool.annotations is not None, name
            hints = (tool.annotations.read_only_hint, tool.annotations.destructive_hint,
                     tool.annotations.idempotent_hint, tool.annotations.open_world_hint)
            assert hints == (read_only, destructive, idempotent, open_world), name

    async def test_output_schema_is_the_wrapped_result_string(self, tmp_path: Path) -> None:
        for name, tool in (await _listed_tools(tmp_path)).items():
            schema = tool.output_schema
            assert schema is not None, name
            assert schema["type"] == "object", name
            assert set(schema["properties"]) == {"result"}, name
            assert schema["properties"]["result"]["type"] == "string", name
            assert schema["required"] == ["result"], name


class TestRegistrationGuard:
    @staticmethod
    def _registrar() -> tuple[Any, Any]:
        server_cls, tool_error, mcp_error, _, _ = _import_mcp_sdk()
        server = server_cls("probe")
        # The registration wrapper create_mcp_server decorates every tool with.
        registrar = getattr(server_module, "_tool_registrar")
        return server, registrar(server, tool_error=tool_error, mcp_error=mcp_error)

    @staticmethod
    async def _probe() -> str:
        """Probe tool."""
        return "ok"

    def test_missing_title_fails(self) -> None:
        _, tool = self._registrar()
        with pytest.raises(TypeError, match="title"):
            tool(hints=_HINTS)

    def test_missing_hints_fails(self) -> None:
        _, tool = self._registrar()
        with pytest.raises(TypeError, match="hints"):
            tool(title="Probe")

    @pytest.mark.parametrize("omitted", ["read_only_hint", "destructive_hint", "idempotent_hint", "open_world_hint"])
    def test_partial_hints_fail(self, omitted: str) -> None:
        _, tool = self._registrar()
        partial = _HINTS.model_copy(update={omitted: None})
        with pytest.raises((TypeError, ValueError), match=omitted):
            tool(title="Probe", hints=partial)

    async def test_title_and_hints_reach_the_listing(self) -> None:
        server, tool = self._registrar()
        tool(title="Probe", hints=_HINTS)(self._probe)
        (listed,) = await server.list_tools()
        assert (listed.title, listed.annotations) == ("Probe", _HINTS)


# --------------------------------------------------------------------------- #
# Closed-world tools open no database connection
# --------------------------------------------------------------------------- #
#: tool -> (arguments, a fragment of its success text).
_CLOSED_WORLD_CALLS: dict[str, tuple[dict[str, Any], str]] = {
    "datasets_summary": ({"datasource_name": DS, "compact": False}, "## `customer_totals`"),
    "list_datasources": ({}, "- shop (sqlite)"),
    "get_datasource_priority": ({}, "['shop']"),
    "recommend_population": ({"items": ["sum(orders.amount)", "customers.name"], "data_source": DS},
                             "Recommended root dataset"),
    "edit_datasource": ({"name": DS, "description": "The shop."}, "updated"),
    "set_datasource_priority": ({"priority": []}, "cleared"),
    "delete_dataset": ({"name": "customer_totals", "data_source": DS}, "deleted"),
    "delete_datasource": ({"name": DS}, "deleted"),
    "create_semantic_model": ({"name": "extra", "data_source": DS,
                               "content": _catalog_model("extra", dataset_name="goods")}, "created"),
    "edit_semantic_model": ({"name": "catalog", "data_source": DS, "description": "Products."}, "edited"),
    "delete_semantic_model": ({"name": "catalog", "data_source": DS}, "deleted"),
    "create_ontology": ({"content": ontology_text(concept("Other"), name="other")}, "stored"),
    "edit_ontology": ({"name": "shop", "description": "Shop concepts."}, "edited"),
    "delete_ontology": ({"name": "spare"}, "deleted"),
    "create_ontology_mapping": ({"data_source": DS, "content": order_mapping(name="second_mapping",
                                                                            semantic_model=DS)}, "stored"),
    "edit_ontology_mapping": ({"name": "shop_mapping", "data_source": DS, "description": "Orders."}, "edited"),
    "delete_ontology_mapping": ({"name": "shop_mapping", "data_source": DS}, "deleted"),
    "forget_memory": ({"id": MEMORY_ID}, "deleted_id"),
    "save_memory": ({"learning": "customers.name is a display name", "linked_entities": ["customers.name"]},
                    "memory_id"),
}


def test_closed_world_calls_cover_every_closed_world_tool() -> None:
    assert set(_CLOSED_WORLD_CALLS) == {name for name, row in TOOLS.items() if not row[4]}


async def _call_refusing_clients(tmp_path: Path, *, tool: str,
                                 arguments: dict[str, Any]) -> tuple[CallToolResult, int]:
    """``tool``'s result over the seeded storage when every database client request fails, and the request count."""
    storage = await _seed(tmp_path)
    calls: list[DatasourceConfig] = []
    async with _client(storage, client_factory=_refusing_factory(calls)) as client:
        result = await client.call_tool(tool, arguments)
    return result, len(calls)


@pytest.mark.parametrize("tool", sorted(_CLOSED_WORLD_CALLS))
async def test_closed_world_tool_requests_no_database_client(tmp_path: Path, tool: str) -> None:
    arguments, success = _CLOSED_WORLD_CALLS[tool]
    result, requests = await _call_refusing_clients(tmp_path, tool=tool, arguments=arguments)
    assert not result.is_error, _text(result)
    assert success in _text(result)
    assert CLIENT_REFUSAL not in _text(result)
    assert requests == 0


async def test_open_world_tool_reaches_the_refusing_factory(tmp_path: Path) -> None:
    """Control: the factory is the seam a database read goes through."""
    _, requests = await _call_refusing_clients(tmp_path, tool="describe_datasource", arguments={"name": DS})
    assert requests > 0


# --------------------------------------------------------------------------- #
# Read-only tools leave stored definitions unchanged
# --------------------------------------------------------------------------- #
#: case -> (arguments, a fragment of its success text); a case is its tool's name plus an optional " (form)".
_READ_ONLY_CALLS: dict[str, tuple[dict[str, Any], str]] = {
    "query": ({"query": {"population": "orders", "metrics": ["sum(orders.amount)"], "dimensions": ["customers.name"]}},
              "| Ann | 15.0 |"),
    "query (saved query-backed dataset)": ({"query": "customer_totals"}, "| c1 | 15.0 |"),
    "inspect (point)": ({"entity_type": "dataset", "reference": "orders", "compact": False}, "# Dataset: `orders`"),
    "inspect (collection)": ({"entity_type": "dataset"}, "# Datasource: `shop`"),
    "inspect (query-backed)": ({"entity_type": "dataset", "reference": "customer_totals", "compact": False},
                               "## Backing Query"),
    "search": ({"question": "order amount"}, '"id": "shop.orders.amount"'),
    "describe_datasource": ({"name": DS}, "Connection: OK"),
    "validate_datasets": ({"data_source": DS}, "[]"),
    "datasets_summary": ({"datasource_name": DS, "compact": False}, "## `customer_totals`"),
    "list_datasources": ({}, "- shop (sqlite)"),
    "get_datasource_priority": ({}, "['shop']"),
    "recommend_population": ({"items": ["sum(orders.amount)", "customers.name"], "data_source": DS},
                             "Recommended root dataset"),
}


def test_read_only_calls_cover_every_read_only_tool() -> None:
    assert {case.split(" (")[0] for case in _READ_ONLY_CALLS} == {name for name, row in TOOLS.items() if row[1]}


def _without_samples(value: Any) -> Any:
    """``value`` with cached samples and payload fingerprints (which hash them) dropped at any depth."""
    if isinstance(value, dict):
        return {k: _without_samples(v) for k, v in value.items() if k not in _SAMPLE_KEYS}
    if isinstance(value, list):
        return [_without_samples(v) for v in value]
    return value


async def _stored_definitions(storage: YAMLStorage) -> dict[str, Any]:
    """Every stored definition, cached samples excluded."""
    datasources: dict[str, Any] = {}
    datasets: dict[str, Any] = {}
    for data_source in await storage.list_datasources():
        config = await storage.get_datasource(data_source)
        assert config is not None
        datasources[data_source] = config.model_dump(mode="json")
        for name in await storage.list_datasets(data_source=data_source):
            stored = await storage.get_dataset(name, data_source=data_source)
            assert stored is not None
            datasets[f"{data_source}.{name}"] = stored.model_dump(mode="json")
    return _without_samples({
        "datasources": datasources,
        "datasets": datasets,
        "semantic_models": {f"{ds}.{n}": yaml.safe_load(await storage.get_semantic_model_text(n, data_source=ds) or "")
                            for ds, n in await storage.list_semantic_models()},
        "ontologies": {n: await storage.get_ontology_text(n) for n in await storage.list_ontologies()},
        "mappings": {f"{ds}.{n}": await storage.get_ontology_mapping_text(n, ds)
                     for ds, n in await storage.list_ontology_mappings()},
        "memories": {m.id: m.model_dump(mode="json") for m in await storage.list_memories()},
        "priority": await storage.get_datasource_priority(),
    })


async def _definitions_around(tmp_path: Path, *, tool: str, arguments: dict[str, Any]) -> tuple[Any, Any, str]:
    """The stored definitions before and after ``tool`` runs over the seeded storage, and its text."""
    storage = await _seed(tmp_path)
    before = await _stored_definitions(storage)
    async with _client(storage) as client:
        result = await client.call_tool(tool, arguments)
    assert not result.is_error, _text(result)
    return before, await _stored_definitions(storage), _text(result)


@pytest.mark.parametrize("case", sorted(_READ_ONLY_CALLS))
async def test_read_only_tool_leaves_stored_definitions_unchanged(tmp_path: Path, case: str) -> None:
    arguments, success = _READ_ONLY_CALLS[case]
    before, after, text = await _definitions_around(tmp_path, tool=case.split(" (")[0], arguments=arguments)
    assert success in text
    assert after == before


async def test_writing_tool_changes_the_snapshot(tmp_path: Path) -> None:
    """Control: the snapshot sees a stored-definition change."""
    before, after, _ = await _definitions_around(tmp_path, tool="edit_datasource",
                                                 arguments={"name": DS, "description": "The shop."})
    assert after != before
