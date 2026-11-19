"""Edge names are visible on inspection, summary, facade and ingest-report surfaces."""

from __future__ import annotations

import json
import re

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.live_schema.ingestion import _updated_detail_lines
from slayer.live_schema.schema_drift import DatasetAddition
from slayer.facade.catalog import FacadeCatalog, FacadeTable, build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.inspect.collection_render import render_datasets_summary
from slayer.inspect.dataset_render import dataset_skeleton_fields, render_dataset_inspection
from slayer.mcp.server import _addition_has_changes, _addition_update_details

from tests._dev2073_fixtures import (
    DS,
    addresses_dataset,
    join_to,
    dataset_store,
    named_pair,
    orders_with,
    unnamed_pair,
)

EDGE_NAMES = ["billing_address", "shipping_address"]


def _orders() -> SlayerDataset:
    return orders_with(*named_pair())


async def _inspect(dataset: SlayerDataset, **kwargs) -> str:
    async with dataset_store(addresses_dataset(), _orders()) as storage:
        return await render_dataset_inspection(dataset=dataset, storage=storage, engine=None, **kwargs)


class TestInspectShowsEdgeNames:
    async def test_json_join_rows_carry_names(self) -> None:
        payload = json.loads(await _inspect(_orders(), format="json"))
        assert sorted(j["name"] for j in payload["relationships"]) == EDGE_NAMES

    async def test_incoming_hops_carry_names(self) -> None:
        payload = json.loads(await _inspect(addresses_dataset(), format="json"))
        assert sorted(j["name"] for j in payload["relationships"]) == EDGE_NAMES

    async def test_markdown_join_table_has_a_name_cell(self) -> None:
        md = await _inspect(_orders(), format="markdown")
        assert re.search(r"\| billing_address \|", md)

    async def test_names_only_view_lists_edge_references(self) -> None:
        payload = json.loads(await _inspect(_orders(), format="json", sections=["fields"]))
        assert sorted(payload["relationships_names"]) == EDGE_NAMES

    def test_skeleton_lists_edge_references(self) -> None:
        assert dataset_skeleton_fields(dataset=_orders())["relationships_to"] == EDGE_NAMES


def _summary_joins(*models: SlayerDataset) -> dict[str, list[str]]:
    out = json.loads(render_datasets_summary(datasource_name=DS, datasets=list(models), fmt="json", compact=True))
    return {m["name"]: m["relationships_to"] for m in out["datasets"]}


class TestModelsSummaryListsAddressableTokens:
    def test_named_parallel_edges_list_their_names(self) -> None:
        joins = _summary_joins(addresses_dataset(), _orders())
        assert joins["orders"] == EDGE_NAMES
        assert joins["addresses"] == EDGE_NAMES

    def test_unnamed_parallel_pair_is_marked_ambiguous(self) -> None:
        joins = _summary_joins(addresses_dataset(), orders_with(*unnamed_pair()))
        assert joins["orders"] == ["addresses (ambiguous: name the edges)"]


def _table(catalog: FacadeCatalog, name: str) -> FacadeTable:
    return next(t for s in catalog.schemas for t in s.tables if t.name == name)


def _facade_edges(table: FacadeTable) -> set[tuple]:
    return {(j.name, j.to, str([list(p) for p in j.column_pairs])) for j in table.relationships}


class TestFacadeCatalogExposesBothOrientations:
    def test_declaring_side_carries_names(self) -> None:
        catalog = build_catalog(datasets_by_datasource={DS: [_orders(), addresses_dataset()]})
        assert _facade_edges(_table(catalog, "orders")) == {
            ("billing_address", "addresses", "[['billing_address_id', 'id']]"),
            ("shipping_address", "addresses", "[['shipping_address_id', 'id']]"),
        }

    def test_target_side_carries_oriented_incoming_edges(self) -> None:
        catalog = build_catalog(datasets_by_datasource={DS: [_orders(), addresses_dataset()]})
        assert _facade_edges(_table(catalog, "addresses")) == {
            ("billing_address", "orders", "[['id', 'billing_address_id']]"),
            ("shipping_address", "orders", "[['id', 'shipping_address_id']]"),
        }


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source=DS, sql_table="customers",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                 DatasetField(name="name", datatype=DataType.STRING)],
    )


def _dimension_names(result: QueryResult) -> list[str]:
    return [d.ref.full_name if d.ref is not None else repr(d) for d in result.query.dimensions or []]


class TestTranslatorMatchesTheDeclaredEdge:
    def test_reverse_direction_join_traverses_the_declared_edge(self) -> None:
        orders = orders_with(join_to([["customer_id", "id"]], target="customers"))
        catalog = build_catalog(datasets_by_datasource={DS: [orders, _customers()]})
        sql = (
            'SELECT "Orders"."status" AS "s" FROM "public"."customers" '
            'LEFT JOIN (SELECT "public"."orders"."id" AS "id", "public"."orders"."customer_id" AS "customer_id", '
            '"public"."orders"."status" AS "status" FROM "public"."orders") AS "Orders" '
            'ON "public"."customers"."id" = "Orders"."customer_id"'
        )
        result = translate(sql=sql, catalog=catalog, dialect="postgres")
        assert isinstance(result, QueryResult)
        assert result.query.population == "customers"
        assert _dimension_names(result) == ["orders.status"]

    def test_parallel_join_uses_the_edge_name(self) -> None:
        catalog = build_catalog(datasets_by_datasource={DS: [_orders(), addresses_dataset()]})
        sql = (
            'SELECT "Ship"."city" AS "c" FROM "public"."orders" '
            'LEFT JOIN (SELECT "public"."addresses"."id" AS "id", "public"."addresses"."city" AS "city" '
            'FROM "public"."addresses") AS "Ship" '
            'ON "public"."orders"."shipping_address_id" = "Ship"."id"'
        )
        result = translate(sql=sql, catalog=catalog, dialect="postgres")
        assert isinstance(result, QueryResult)
        assert result.query.population == "orders"
        assert _dimension_names(result) == ["shipping_address.city"]


class TestIngestReportListsNamedEdges:
    def test_name_only_update_is_rendered(self) -> None:
        addition = DatasetAddition(dataset_name="transactions", data_source=DS, named_relationships=["coupon_usage"])
        assert _addition_has_changes(addition)
        assert any("coupon_usage" in d for d in _addition_update_details(addition))
        assert any("coupon_usage" in d for d in _updated_detail_lines(addition))
