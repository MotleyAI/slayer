"""DEV-1853 — both orientations are visible on inspection and audit surfaces.

Inspecting a model lists reverse-reachable neighbors with the cardinality
oriented for traversal from the inspected side; the safety audit reports each
edge once with per-orientation provability.
"""

from __future__ import annotations

import json
import tempfile

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.engine.cardinality.relationship_safety import RelationshipSafetyFinding, audit_relationship_safety
from slayer.inspect.dataset_render import render_dataset_inspection
from slayer.inspect.service import InspectService
from slayer.storage.yaml_storage import YAMLStorage


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="ds", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INT),
        ],
        relationships=[DatasetRelationship(to="customers",
                         join_pairs=[["customer_id", "id"]],
                         cardinality=RelationshipCardinality.MANY_TO_ONE)],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="ds", sql_table="customers",
        fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                 DatasetField(name="name", datatype=DataType.TEXT)],
    )


async def _storage(tmpdir: str) -> YAMLStorage:
    storage = YAMLStorage(base_dir=tmpdir)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    await storage.save_dataset(_orders())
    await storage.save_dataset(_customers())
    return storage


class TestInspectShowsIncomingEdges:
    async def test_markdown_lists_the_oriented_reverse_hop(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            md = await render_dataset_inspection(
                dataset=_customers(), storage=storage, engine=None,
                format="markdown")
            assert "orders" in md
            assert "one_to_many" in md

    async def test_json_payload_carries_the_reverse_hop(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            out = await render_dataset_inspection(
                dataset=_customers(), storage=storage, engine=None,
                format="json")
            text = json.dumps(json.loads(out))
            assert '"orders"' in text
            assert '"one_to_many"' in text


class TestCollectionSurfaceListsReverseNeighbors:
    async def test_joins_to_includes_the_incoming_edge(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            out = await InspectService(storage=storage).inspect(
                reference=None, entity_type="dataset", format="json")
            data = json.loads(out)
            ds = next(x for x in data["datasources"] if x["data_source"] == "ds")
            customers = next(m for m in ds["datasets"] if m["name"] == "customers")
            assert "orders" in customers["relationships_to"]


def _edge_finding(findings: list[RelationshipSafetyFinding]) -> RelationshipSafetyFinding:
    matches = [f for f in findings
               if f.dataset == "orders" and f.to == "customers"]
    assert len(matches) == 1
    return matches[0]


class TestAuditReportsBothOrientations:
    def test_declared_many_to_one_edge_explains_both_directions(self) -> None:
        finding = _edge_finding(audit_relationship_safety(
            datasets=[_orders(), _customers()]))
        assert finding.forward_provably_to_one is True
        assert finding.reverse_provably_to_one is False

    def test_parallel_edges_each_report_their_own_orientation(self) -> None:
        # billing FK is unique (reverse structurally provable); shipping FK is
        # not — each edge's finding must reflect ITS orientation, never the
        # sibling's.
        orders = SlayerDataset(
            name="orders", data_source="ds", sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="billing_customer_id", datatype=DataType.INT,
                       unique=True),
                DatasetField(name="shipping_customer_id", datatype=DataType.INT),
            ],
            relationships=[
                DatasetRelationship(to="customers",
                          join_pairs=[["billing_customer_id", "id"]],
                          name="billing"),
                DatasetRelationship(to="customers",
                          join_pairs=[["shipping_customer_id", "id"]],
                          name="shipping"),
            ],
        )
        findings = audit_relationship_safety(datasets=[orders, _customers()])
        assert len(findings) == 2
        billing, shipping = findings
        assert billing.reverse_provably_to_one is True
        assert shipping.reverse_provably_to_one is False

    def test_unproven_edge_reports_both_orientations_unproven(self) -> None:
        orders = _orders()
        orders.relationships[0].cardinality = None
        customers = _customers()
        customers.fields[0].primary_key = False
        finding = _edge_finding(audit_relationship_safety(datasets=[orders, customers]))
        assert finding.forward_provably_to_one is False
        assert finding.reverse_provably_to_one is False
