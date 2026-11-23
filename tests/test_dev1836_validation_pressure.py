"""Validation surfaces unproven and contradicted relationships.

Spec: models/join-cardinality — "Validation surfaces unproven and contradicted joins".
``audit_relationship_safety`` flags every relationship that is neither declared m:1/1:1 nor structurally
proven (association / input-refusal consequence + remedies), and — given a detection
report — declarations the data hard-contradicts. CLI: ``slayer validate-datasets`` prints the flags.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from slayer.async_utils import run_sync
from slayer.cli import _run_validate_datasets
from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.engine.cardinality.inference import (
    CardinalityVerdict,
    RelationshipCardinalityFinding,
    RelationshipCardinalityReport,
    SideStats,
)
from slayer.engine.cardinality.relationship_safety import RelationshipSafetyFinding, audit_relationship_safety
from slayer.storage.yaml_storage import YAMLStorage
from slayer.storage.sqlite_conn import transaction

from tests._dev1836_fixtures import dev1836_datasets


def _findings_by_edge(findings: list[RelationshipSafetyFinding]) -> dict:
    return {(f.dataset, f.to): f for f in findings}


class TestUnprovenJoins:
    def test_unproven_join_is_flagged_with_remedies(self) -> None:
        findings = audit_relationship_safety(datasets=dev1836_datasets())
        by_edge = _findings_by_edge(findings)

        flagged = by_edge[("customers", "segments")]
        message = flagged.message.lower()
        assert "partition_by=[]" in message
        assert "associat" in message
        # The remedies: declare cardinality, declare a covering unique key,
        # or run cardinality detection.
        assert "cardinality" in message
        assert "unique" in message
        assert "detect" in message

    def test_proven_joins_are_not_flagged(self) -> None:
        # One finding per edge — proven edges appear as info
        # rows recording both orientations, never with broadcast wording.
        findings = audit_relationship_safety(datasets=dev1836_datasets())
        by_edge = _findings_by_edge(findings)
        for edge in (("orders", "customers"), ("customers", "regions")):
            finding = by_edge[edge]  # PK-proven
            assert finding.forward_provably_to_one is True
            assert finding.severity != "warning"
            assert "broadcast" not in finding.message.lower()

    def test_unproven_forward_orientation_is_flagged_with_its_consequence(self) -> None:
        # Neither declared m:1/1:1 nor structurally proven — dimensions across it
        # associate (partition_by=[] repeats the total), and validation says so (the fixture's
        # only such declared orientation is customers → segments).
        findings = audit_relationship_safety(datasets=dev1836_datasets())
        by_edge = _findings_by_edge(findings)
        finding = by_edge[("customers", "segments")]
        assert finding.forward_provably_to_one is False
        assert "partition_by=[]" in finding.message.lower()
        assert "associat" in finding.message.lower()


class TestCrossDatasourceIsolation:
    def test_same_named_model_in_another_datasource_does_not_shadow(self) -> None:
        """Relationships resolve within the parent's datasource: a weaker same-named
        dataset elsewhere must not flip a proven relationship to unproven."""
        customers_a = SlayerDataset(
            name="customers", sql_table="customers", data_source="ds_a",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        orders_a = SlayerDataset(
            name="orders", sql_table="orders", data_source="ds_a",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="customers",
                             from_columns=["customer_id"], to_columns=["id"])],
        )
        # Same name, different datasource, and 'id' is NOT unique here.
        customers_b = SlayerDataset(
            name="customers", sql_table="customers", data_source="ds_b",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER)],
        )
        findings = audit_relationship_safety(
            datasets=[orders_a, customers_a, customers_b],
        )
        # The edge appears as a per-edge row; proven means no
        # broadcast flag — a weaker same-named dataset elsewhere must not flip it.
        finding = _findings_by_edge(findings)[("orders", "customers")]
        assert finding.forward_provably_to_one is True
        assert "broadcast" not in finding.message.lower()


class TestContradictedDeclarations:
    def _detection(self) -> RelationshipCardinalityReport:
        return RelationshipCardinalityReport(findings=[RelationshipCardinalityFinding(
            data_source="test", dataset="orders", to="customers",
            column_pairs=[("customer_id", "id")],
            stored=RelationshipCardinality.MANY_TO_ONE,
            detected=RelationshipCardinality.MANY_TO_MANY,
            source_side=SideStats(row_count=7, distinct_count=4,
                                  observed_unique=False),
            target_side=SideStats(row_count=5, distinct_count=4,
                                  observed_unique=False),
            verdict=CardinalityVerdict.CONTRADICTS_HARD,
        )])

    def test_contradicted_declaration_is_flagged(self) -> None:
        """F5 — a detection report that hard-contradicts a declaration."""
        models = dev1836_datasets()
        # Declare the orders → customers relationship m:1 so the contradiction bites.
        orders = models[0]
        orders.relationships[0].cardinality = RelationshipCardinality.MANY_TO_ONE
        findings = audit_relationship_safety(datasets=models, detection=self._detection())
        # The per-edge row and the contradiction are separate
        # findings — filter on the contradiction flag.
        contradiction = [
            f for f in findings
            if (f.dataset, f.to) == ("orders", "customers")
            and f.contradiction
        ]
        assert contradiction, [
            (f.dataset, f.to) for f in findings
        ]
        assert "contradict" in contradiction[0].message.lower()
        assert contradiction[0].severity == "error"

    def test_no_detection_report_no_contradiction_flags(self) -> None:
        models = dev1836_datasets()
        models[0].relationships[0].cardinality = RelationshipCardinality.MANY_TO_ONE
        findings = audit_relationship_safety(datasets=models)
        assert not any(
            "contradict" in f.message.lower() for f in findings
        )


class TestCliValidateModels:
    """The validate-datasets CLI surfaces the unproven-relationship flags."""

    @pytest.fixture
    def store(self, tmp_path: Path) -> str:
        db = str(tmp_path / "ds.db")
        with transaction(db) as conn:
            conn.executescript(
                """
                CREATE TABLE hosts (id INTEGER PRIMARY KEY, tag TEXT);
                CREATE TABLE tags (tag TEXT, label TEXT);
                INSERT INTO hosts VALUES (1, 'a'), (2, 'b');
                INSERT INTO tags VALUES ('a', 'A'), ('b', 'B');
                """
            )
        store = str(tmp_path / "store")
        storage = YAMLStorage(base_dir=store)
        run_sync(storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=db)
        ))
        run_sync(storage.save_dataset(SlayerDataset(
            name="tags", data_source="ds", sql_table="tags",
            fields=[
                # No PK/unique claim on `tag` — the hosts → tags hop is unproven.
                DatasetField(name="tag", datatype=DataType.STRING),
                DatasetField(name="label", datatype=DataType.STRING),
            ],
        ), _validate=False))
        run_sync(storage.save_dataset(SlayerDataset(
            name="hosts", data_source="ds", sql_table="hosts",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="tag", datatype=DataType.STRING),
            ],
            relationships=[DatasetRelationship(to="tags", from_columns=["tag"], to_columns=["tag"])],
        ), _validate=False))
        return store

    def test_text_output_flags_unproven_join(self, store: str, capsys) -> None:
        _run_validate_datasets(SimpleNamespace(
            datasource="ds", model=None, cardinality=False,
            persist_cardinality=False, format="text", force_clean=False,
            yes=False, storage=store,
        ))
        out = capsys.readouterr().out
        assert "tags" in out
        assert "partition_by=[]" in out.lower()
