"""DEV-1853 — both orientations are visible on inspection and audit surfaces.

Inspecting a model lists reverse-reachable neighbors with the cardinality
oriented for traversal from the inspected side; the safety audit reports each
edge once with per-orientation provability.
"""

from __future__ import annotations

import json
import tempfile

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.join_safety import audit_join_safety
from slayer.inspect.model_render import render_model_inspection
from slayer.inspect.service import InspectService
from slayer.storage.yaml_storage import YAMLStorage


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="ds", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="customers",
                         join_pairs=[["customer_id", "id"]],
                         cardinality=JoinCardinality.MANY_TO_ONE)],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="ds", sql_table="customers",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="name", type=DataType.TEXT)],
    )


async def _storage(tmpdir: str) -> YAMLStorage:
    storage = YAMLStorage(base_dir=tmpdir)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    await storage.save_model(_orders())
    await storage.save_model(_customers())
    return storage


class TestInspectShowsIncomingEdges:
    async def test_markdown_lists_the_oriented_reverse_hop(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            md = await render_model_inspection(
                model=_customers(), storage=storage, engine=None,
                format="markdown", compact=False)
            assert "orders" in md
            assert "one_to_many" in md

    async def test_json_payload_carries_the_reverse_hop(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            out = await render_model_inspection(
                model=_customers(), storage=storage, engine=None,
                format="json", compact=False)
            text = json.dumps(json.loads(out))
            assert '"orders"' in text
            assert '"one_to_many"' in text


class TestCollectionSurfaceListsReverseNeighbors:
    async def test_joins_to_includes_the_incoming_edge(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            out = await InspectService(storage=storage).inspect(
                reference=None, entity_type="model", format="json")
            data = json.loads(out)
            ds = next(x for x in data["datasources"] if x["data_source"] == "ds")
            customers = next(m for m in ds["models"] if m["name"] == "customers")
            assert "orders" in customers["joins_to"]


def _edge_finding(findings):
    matches = [f for f in findings
               if f.model == "orders" and f.target_model == "customers"]
    assert len(matches) == 1
    return matches[0]


class TestAuditReportsBothOrientations:
    def test_declared_many_to_one_edge_explains_both_directions(self) -> None:
        finding = _edge_finding(audit_join_safety(
            models=[_orders(), _customers()]))
        assert finding.forward_provably_to_one is True
        assert finding.reverse_provably_to_one is False

    def test_parallel_edges_each_report_their_own_orientation(self) -> None:
        # billing FK is unique (reverse structurally provable); shipping FK is
        # not — each edge's finding must reflect ITS orientation, never the
        # sibling's.
        orders = SlayerModel(
            name="orders", data_source="ds", sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="billing_customer_id", type=DataType.INT,
                       unique=True),
                Column(name="shipping_customer_id", type=DataType.INT),
            ],
            joins=[
                ModelJoin(target_model="customers",
                          join_pairs=[["billing_customer_id", "id"]],
                          name="billing"),
                ModelJoin(target_model="customers",
                          join_pairs=[["shipping_customer_id", "id"]],
                          name="shipping"),
            ],
        )
        findings = audit_join_safety(models=[orders, _customers()])
        assert len(findings) == 2
        billing, shipping = findings
        assert billing.reverse_provably_to_one is True
        assert shipping.reverse_provably_to_one is False

    def test_unproven_edge_reports_both_orientations_unproven(self) -> None:
        orders = _orders()
        orders.joins[0].cardinality = None
        customers = _customers()
        customers.columns[0].primary_key = False
        finding = _edge_finding(audit_join_safety(models=[orders, customers]))
        assert finding.forward_provably_to_one is False
        assert finding.reverse_provably_to_one is False
