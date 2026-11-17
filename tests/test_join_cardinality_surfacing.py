"""Cardinality/unique surfacing in inspect, search graph, edit_model — not in embeddings."""

from __future__ import annotations

import json
import tempfile

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.inspect.dataset_render import render_dataset_inspection
from slayer.mcp.server import create_mcp_server
from slayer.search import graph as search_graph
from slayer.search.render import render_field_text, render_dataset_text
from slayer.storage.yaml_storage import YAMLStorage


def _orders_dataset(*, cardinality: RelationshipCardinality | None, email_unique: bool) -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="ds",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="email", sql="email", datatype=DataType.TEXT, unique=email_unique),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(
                to="customers",
                join_pairs=[["customer_id", "id"]],
                cardinality=cardinality,
            )
        ],
    )


def _customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        sql_table="customers",
        data_source="ds",
        fields=[DatasetField(name="id", sql="id", datatype=DataType.INT, primary_key=True)],
    )


# ---------------------------------------------------------------------------
# render_model_inspection
# ---------------------------------------------------------------------------


class TestModelRender:
    async def test_json_joins_carry_cardinality(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            model = _orders_dataset(
                cardinality=RelationshipCardinality.MANY_TO_ONE, email_unique=True
            )
            out = await render_dataset_inspection(
                dataset=model, storage=storage, engine=None, format="json"
            )
            payload = json.loads(out)
            join = payload["relationships"][0]
            assert join["cardinality"] == "many_to_one"

    async def test_json_columns_carry_unique(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            model = _orders_dataset(
                cardinality=RelationshipCardinality.MANY_TO_ONE, email_unique=True
            )
            out = await render_dataset_inspection(
                dataset=model, storage=storage, engine=None, format="json"
            )
            payload = json.loads(out)
            email = next(c for c in payload["fields"] if c["name"] == "email")
            assert email["unique"] is True

    async def test_markdown_shows_cardinality(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            model = _orders_dataset(
                cardinality=RelationshipCardinality.MANY_TO_ONE, email_unique=True
            )
            md = await render_dataset_inspection(
                dataset=model, storage=storage, engine=None, format="markdown"
            )
            assert "many_to_one" in md

    async def test_markdown_shows_unique_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            model = _orders_dataset(
                cardinality=RelationshipCardinality.MANY_TO_ONE, email_unique=True
            )
            md = await render_dataset_inspection(
                dataset=model, storage=storage, engine=None, format="markdown"
            )
            # The columns table gains a `unique` column.
            assert "unique" in md.lower()


# ---------------------------------------------------------------------------
# search-graph JOINS edge property
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not search_graph.is_available(), reason="graph backend (ladybug) not installed"
)
class TestSearchGraphEdge:
    async def test_joins_edge_has_cardinality_property(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            await storage.save_dataset(_customers_dataset())
            await storage.save_dataset(
                _orders_dataset(cardinality=RelationshipCardinality.MANY_TO_ONE, email_unique=False)
            )
            search_graph.clear_cache()
            ids = await search_graph.get_filtered_ids(
                "MATCH (m:Model {id: 'ds.orders'})-[r:JOINS]->(t:Model) "
                "WHERE r.cardinality = 'many_to_one' RETURN t.id AS id",
                storage,
            )
            assert ids == {"ds.customers"}


# ---------------------------------------------------------------------------
# edit_model round-trip (fields flow via _upsert_entity.model_validate)
# ---------------------------------------------------------------------------


class TestEditModelRoundTrip:
    async def test_edit_model_accepts_cardinality_and_unique(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            await storage.save_dataset(
                _orders_dataset(cardinality=None, email_unique=False)
            )
            server = create_mcp_server(storage=storage)

            await server.call_tool(
                name="edit_dataset",
                arguments={
                    "dataset_name": "orders",
                    "data_source": "ds",
                    "fields": [{"name": "email", "unique": True}],
                    "relationships": [
                        {
                            "to": "customers",
                            "join_pairs": [["customer_id", "id"]],
                            "cardinality": "many_to_one",
                        }
                    ],
                },
            )

            reloaded = await storage.get_dataset("orders", data_source="ds")
            join = next(j for j in reloaded.relationships if j.to == "customers")
            assert join.cardinality is RelationshipCardinality.MANY_TO_ONE
            email = next(c for c in reloaded.fields if c.name == "email")
            assert email.unique is True


# ---------------------------------------------------------------------------
# No embedding churn — new fields excluded from the embedded corpus text
# ---------------------------------------------------------------------------


class TestNoEmbeddingChurn:
    def test_cardinality_not_in_model_embedding_text(self) -> None:
        plain = _orders_dataset(cardinality=None, email_unique=False)
        carded = _orders_dataset(
            cardinality=RelationshipCardinality.MANY_TO_ONE, email_unique=False
        )
        assert render_dataset_text(dataset=carded) == render_dataset_text(dataset=plain)

    def test_unique_not_in_column_embedding_text(self) -> None:
        model = _orders_dataset(cardinality=None, email_unique=False)
        plain_col = DatasetField(name="email", sql="email", datatype=DataType.TEXT, unique=False)
        uniq_col = DatasetField(name="email", sql="email", datatype=DataType.TEXT, unique=True)
        assert render_field_text(dataset=model, field=uniq_col) == render_field_text(
            dataset=model, field=plain_col
        )
