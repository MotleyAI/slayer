"""DEV-1836 task 1.4 — validation surfaces unproven and contradicted joins
(design D1/D5 validation pressure, F5).

Spec: openspec …/specs/models/join-cardinality — "Validation surfaces unproven
and contradicted joins". Engine-level: ``audit_join_safety`` flags every join
that is neither declared m:1/1:1 nor structurally proven (broadcast consequence
+ remedies), and — given a detection report — declarations the data
hard-contradicts. CLI: ``slayer validate-models`` prints the flags.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from slayer.async_utils import run_sync
from slayer.cli import _run_validate_models
from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.cardinality import (
    CardinalityVerdict,
    JoinCardinalityFinding,
    JoinCardinalityReport,
    SideStats,
)
from slayer.engine.join_safety import audit_join_safety
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1836_fixtures import dev1836_models


def _findings_by_edge(findings) -> dict:
    return {(f.model, f.target_model): f for f in findings}


class TestUnprovenJoins:
    def test_unproven_join_is_flagged_with_remedies(self) -> None:
        findings = audit_join_safety(models=dev1836_models())
        by_edge = _findings_by_edge(findings)

        flagged = by_edge[("customers", "segments")]
        message = flagged.message.lower()
        assert "broadcast" in message
        # The remedies: declare cardinality, declare a covering unique key,
        # or run cardinality detection.
        assert "cardinality" in message
        assert "unique" in message
        assert "detect" in message

    def test_proven_joins_are_not_flagged(self) -> None:
        findings = audit_join_safety(models=dev1836_models())
        by_edge = _findings_by_edge(findings)
        assert ("orders", "customers") not in by_edge  # PK-proven
        assert ("customers", "regions") not in by_edge  # PK-proven

    def test_declared_one_to_many_is_flagged_as_broadcast(self) -> None:
        # Neither declared m:1/1:1 nor structurally proven — metrics crossing
        # it broadcast, and validation says so.
        findings = audit_join_safety(models=dev1836_models())
        by_edge = _findings_by_edge(findings)
        assert "broadcast" in by_edge[("customers", "orders")].message.lower()


class TestContradictedDeclarations:
    def _detection(self) -> JoinCardinalityReport:
        return JoinCardinalityReport(findings=[JoinCardinalityFinding(
            data_source="test", model="orders", target_model="customers",
            join_pairs=[["customer_id", "id"]],
            stored=JoinCardinality.MANY_TO_ONE,
            detected=JoinCardinality.MANY_TO_MANY,
            source_side=SideStats(row_count=7, distinct_count=4,
                                  observed_unique=False),
            target_side=SideStats(row_count=5, distinct_count=4,
                                  observed_unique=False),
            verdict=CardinalityVerdict.CONTRADICTS_HARD,
        )])

    def test_contradicted_declaration_is_flagged(self) -> None:
        """F5 — a detection report that hard-contradicts a declaration."""
        models = dev1836_models()
        # Declare the orders → customers join m:1 so the contradiction bites.
        orders = models[0]
        orders.joins[0].cardinality = JoinCardinality.MANY_TO_ONE
        findings = audit_join_safety(models=models, detection=self._detection())
        contradiction = [
            f for f in findings
            if (f.model, f.target_model) == ("orders", "customers")
        ]
        assert contradiction, [
            (f.model, f.target_model) for f in findings
        ]
        assert "contradict" in contradiction[0].message.lower()

    def test_no_detection_report_no_contradiction_flags(self) -> None:
        models = dev1836_models()
        models[0].joins[0].cardinality = JoinCardinality.MANY_TO_ONE
        findings = audit_join_safety(models=models)
        assert not any(
            "contradict" in f.message.lower() for f in findings
        )


class TestCliValidateModels:
    """The validate-models CLI surfaces the unproven-join flags."""

    @pytest.fixture
    def store(self, tmp_path: Path) -> str:
        db = str(tmp_path / "ds.db")
        conn = sqlite3.connect(db)
        conn.executescript(
            """
            CREATE TABLE hosts (id INTEGER PRIMARY KEY, tag TEXT);
            CREATE TABLE tags (tag TEXT, label TEXT);
            INSERT INTO hosts VALUES (1, 'a'), (2, 'b');
            INSERT INTO tags VALUES ('a', 'A'), ('b', 'B');
            """
        )
        conn.commit()
        conn.close()
        store = str(tmp_path / "store")
        storage = YAMLStorage(base_dir=store)
        run_sync(storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=db)
        ))
        run_sync(storage.save_model(SlayerModel(
            name="tags", data_source="ds", sql_table="tags",
            columns=[
                # No PK/unique claim on `tag` — the hosts → tags hop is unproven.
                Column(name="tag", type=DataType.TEXT),
                Column(name="label", type=DataType.TEXT),
            ],
        ), _validate=False))
        run_sync(storage.save_model(SlayerModel(
            name="hosts", data_source="ds", sql_table="hosts",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="tag", type=DataType.TEXT),
            ],
            joins=[ModelJoin(target_model="tags", join_pairs=[["tag", "tag"]])],
        ), _validate=False))
        return store

    def test_text_output_flags_unproven_join(self, store: str, capsys) -> None:
        _run_validate_models(SimpleNamespace(
            datasource="ds", model=None, cardinality=False,
            persist_cardinality=False, format="text", force_clean=False,
            yes=False, storage=store, models_dir=None,
        ))
        out = capsys.readouterr().out
        assert "tags" in out
        assert "broadcast" in out.lower()
