"""Surfacing cardinality/unique (DEV-1688).

* ``render_model_inspection`` shows cardinality (joins) + unique (columns) in
  both markdown and JSON.
* search-graph ``JOINS`` edge carries a ``cardinality`` property.
* ``edit_model`` accepts the new fields via ``_upsert_entity.model_validate``.
* The embedding corpus text is UNCHANGED (no re-embed churn): the new fields do
  NOT appear in ``render_model_text`` / ``render_column_text``.
"""

from __future__ import annotations

import json
import tempfile

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.inspect.model_render import render_model_inspection
from slayer.mcp.server import create_mcp_server
from slayer.search import graph as search_graph
from slayer.search.render import render_column_text, render_model_text
from slayer.storage.yaml_storage import YAMLStorage


def _orders_model(*, cardinality: JoinCardinality | None, email_unique: bool) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="email", sql="email", type=DataType.TEXT, unique=email_unique),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
                cardinality=cardinality,
            )
        ],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="ds",
        columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
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
            model = _orders_model(
                cardinality=JoinCardinality.MANY_TO_ONE, email_unique=True
            )
            out = await render_model_inspection(
                model=model, storage=storage, engine=None, format="json", compact=False
            )
            payload = json.loads(out)
            join = payload["joins"][0]
            assert join["cardinality"] == "many_to_one"

    async def test_json_columns_carry_unique(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            model = _orders_model(
                cardinality=JoinCardinality.MANY_TO_ONE, email_unique=True
            )
            out = await render_model_inspection(
                model=model, storage=storage, engine=None, format="json", compact=False
            )
            payload = json.loads(out)
            email = next(c for c in payload["columns"] if c["name"] == "email")
            assert email["unique"] is True

    async def test_markdown_shows_cardinality(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            model = _orders_model(
                cardinality=JoinCardinality.MANY_TO_ONE, email_unique=True
            )
            md = await render_model_inspection(
                model=model, storage=storage, engine=None, format="markdown", compact=False
            )
            assert "many_to_one" in md

    async def test_markdown_shows_unique_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:")
            )
            model = _orders_model(
                cardinality=JoinCardinality.MANY_TO_ONE, email_unique=True
            )
            md = await render_model_inspection(
                model=model, storage=storage, engine=None, format="markdown", compact=False
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
            await storage.save_model(_customers_model())
            await storage.save_model(
                _orders_model(cardinality=JoinCardinality.MANY_TO_ONE, email_unique=False)
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
            await storage.save_model(
                _orders_model(cardinality=None, email_unique=False)
            )
            server = create_mcp_server(storage=storage)

            await server.call_tool(
                name="edit_model",
                arguments={
                    "model_name": "orders",
                    "data_source": "ds",
                    "columns": [{"name": "email", "unique": True}],
                    "joins": [
                        {
                            "target_model": "customers",
                            "join_pairs": [["customer_id", "id"]],
                            "cardinality": "many_to_one",
                        }
                    ],
                },
            )

            reloaded = await storage.get_model("orders", data_source="ds")
            join = next(j for j in reloaded.joins if j.target_model == "customers")
            assert join.cardinality is JoinCardinality.MANY_TO_ONE
            email = next(c for c in reloaded.columns if c.name == "email")
            assert email.unique is True


# ---------------------------------------------------------------------------
# No embedding churn — new fields excluded from the embedded corpus text
# ---------------------------------------------------------------------------


class TestNoEmbeddingChurn:
    def test_cardinality_not_in_model_embedding_text(self) -> None:
        plain = _orders_model(cardinality=None, email_unique=False)
        carded = _orders_model(
            cardinality=JoinCardinality.MANY_TO_ONE, email_unique=False
        )
        assert render_model_text(model=carded) == render_model_text(model=plain)

    def test_unique_not_in_column_embedding_text(self) -> None:
        model = _orders_model(cardinality=None, email_unique=False)
        plain_col = Column(name="email", sql="email", type=DataType.TEXT, unique=False)
        uniq_col = Column(name="email", sql="email", type=DataType.TEXT, unique=True)
        assert render_column_text(model=model, column=uniq_col) == render_column_text(
            model=model, column=plain_col
        )
