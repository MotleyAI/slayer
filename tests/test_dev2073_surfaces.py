"""Edge names are visible on inspection, summary, facade and ingest-report surfaces."""

from __future__ import annotations

import json
import re

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.engine.ingestion import _updated_detail_lines
from slayer.engine.schema_drift import ModelAddition
from slayer.facade.catalog import FacadeCatalog, FacadeTable, build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.inspect.collection_render import render_models_summary
from slayer.inspect.model_render import model_skeleton_fields, render_model_inspection
from slayer.mcp.server import _addition_has_changes, _addition_update_details

from tests._dev2073_fixtures import (
    DS,
    addresses_model,
    join_to,
    model_store,
    named_pair,
    orders_with,
    unnamed_pair,
)

EDGE_NAMES = ["billing_address", "shipping_address"]


def _orders() -> SlayerModel:
    return orders_with(*named_pair())


async def _inspect(model: SlayerModel, **kwargs) -> str:
    async with model_store(addresses_model(), _orders()) as storage:
        return await render_model_inspection(model=model, storage=storage, engine=None, compact=False, **kwargs)


class TestInspectShowsEdgeNames:
    async def test_json_join_rows_carry_names(self) -> None:
        payload = json.loads(await _inspect(_orders(), format="json"))
        assert sorted(j["name"] for j in payload["joins"]) == EDGE_NAMES

    async def test_incoming_hops_carry_names(self) -> None:
        payload = json.loads(await _inspect(addresses_model(), format="json"))
        assert sorted(j["name"] for j in payload["joins"]) == EDGE_NAMES

    async def test_markdown_join_table_has_a_name_cell(self) -> None:
        md = await _inspect(_orders(), format="markdown")
        assert re.search(r"\| billing_address \|", md)

    async def test_names_only_view_lists_edge_references(self) -> None:
        payload = json.loads(await _inspect(_orders(), format="json", sections=["columns"]))
        assert sorted(payload["joins_names"]) == EDGE_NAMES

    def test_skeleton_lists_edge_references(self) -> None:
        assert model_skeleton_fields(model=_orders())["joins_to"] == EDGE_NAMES


def _summary_joins(*models: SlayerModel) -> dict[str, list[str]]:
    out = json.loads(render_models_summary(datasource_name=DS, models=list(models), fmt="json", compact=True))
    return {m["name"]: m["joins_to"] for m in out["models"]}


class TestModelsSummaryListsAddressableTokens:
    def test_named_parallel_edges_list_their_names(self) -> None:
        joins = _summary_joins(addresses_model(), _orders())
        assert joins["orders"] == EDGE_NAMES
        assert joins["addresses"] == EDGE_NAMES

    def test_unnamed_parallel_pair_is_marked_ambiguous(self) -> None:
        joins = _summary_joins(addresses_model(), orders_with(*unnamed_pair()))
        assert joins["orders"] == ["addresses (ambiguous: name the edges)"]


def _table(catalog: FacadeCatalog, name: str) -> FacadeTable:
    return next(t for s in catalog.schemas for t in s.tables if t.name == name)


def _facade_edges(table: FacadeTable) -> set[tuple]:
    return {(j.name, j.target_model, str(j.join_pairs)) for j in table.joins}


class TestFacadeCatalogExposesBothOrientations:
    def test_declaring_side_carries_names(self) -> None:
        catalog = build_catalog(models_by_datasource={DS: [_orders(), addresses_model()]})
        assert _facade_edges(_table(catalog, "orders")) == {
            ("billing_address", "addresses", "[['billing_address_id', 'id']]"),
            ("shipping_address", "addresses", "[['shipping_address_id', 'id']]"),
        }

    def test_target_side_carries_oriented_incoming_edges(self) -> None:
        catalog = build_catalog(models_by_datasource={DS: [_orders(), addresses_model()]})
        assert _facade_edges(_table(catalog, "addresses")) == {
            ("billing_address", "orders", "[['id', 'billing_address_id']]"),
            ("shipping_address", "orders", "[['id', 'shipping_address_id']]"),
        }


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source=DS, sql_table="customers",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="name", type=DataType.TEXT)],
    )


class TestTranslatorMatchesTheDeclaredEdge:
    def test_reverse_direction_join_traverses_the_declared_edge(self) -> None:
        orders = orders_with(join_to([["customer_id", "id"]], target="customers"))
        catalog = build_catalog(models_by_datasource={DS: [orders, _customers()]})
        sql = (
            'SELECT "Orders"."status" AS "s" FROM "public"."customers" '
            'LEFT JOIN (SELECT "public"."orders"."id" AS "id", "public"."orders"."customer_id" AS "customer_id", '
            '"public"."orders"."status" AS "status" FROM "public"."orders") AS "Orders" '
            'ON "public"."customers"."id" = "Orders"."customer_id"'
        )
        result = translate(sql=sql, catalog=catalog, dialect="postgres")
        assert isinstance(result, QueryResult)
        assert result.query.source_model == "customers"
        assert [d.full_name for d in result.query.dimensions or []] == ["orders.status"]

    def test_parallel_join_uses_the_edge_name(self) -> None:
        catalog = build_catalog(models_by_datasource={DS: [_orders(), addresses_model()]})
        sql = (
            'SELECT "Ship"."city" AS "c" FROM "public"."orders" '
            'LEFT JOIN (SELECT "public"."addresses"."id" AS "id", "public"."addresses"."city" AS "city" '
            'FROM "public"."addresses") AS "Ship" '
            'ON "public"."orders"."shipping_address_id" = "Ship"."id"'
        )
        result = translate(sql=sql, catalog=catalog, dialect="postgres")
        assert isinstance(result, QueryResult)
        assert result.query.source_model == "orders"
        assert [d.full_name for d in result.query.dimensions or []] == ["shipping_address.city"]


class TestIngestReportListsNamedEdges:
    def test_name_only_update_is_rendered(self) -> None:
        addition = ModelAddition(model_name="transactions", data_source=DS, named_joins=["coupon_usage"])
        assert _addition_has_changes(addition)
        assert any("coupon_usage" in d for d in _addition_update_details(addition))
        assert any("coupon_usage" in d for d in _updated_detail_lines(addition))
