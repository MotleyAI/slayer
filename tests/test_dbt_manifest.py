"""Tests for the optional dbt-manifest integration."""

import json
from unittest.mock import MagicMock, patch

from slayer.dbt import manifest as dbt_manifest
from slayer.dbt.manifest import (
    find_orphan_model_nodes,
    load_or_generate_manifest,
    regular_models_from_manifest,
)


def _fake_manifest() -> dict:
    """A minimal manifest with one semantic_model wrapping one of two regular models."""
    return {
        "nodes": {
            "model.proj.orders": {
                "resource_type": "model",
                "name": "orders",
                "database": "analytics",
                "schema": "public",
                "alias": "orders",
                "description": "Orders fact table",
                "tags": ["facts"],
                "columns": {
                    "id": {"name": "id", "description": "Order ID", "data_type": "integer"},
                    "amount": {"name": "amount", "description": "Order amount", "data_type": "numeric"},
                },
            },
            "model.proj.raw_events": {
                "resource_type": "model",
                "name": "raw_events",
                "database": "analytics",
                "schema": "staging",
                "alias": "raw_events_v2",
                "description": "Raw event log",
                "tags": [],
                "columns": {
                    "event_id": {"name": "event_id", "description": None, "data_type": "varchar"},
                },
            },
            "test.proj.some_test": {
                "resource_type": "test",
                "name": "some_test",
            },
        },
        "semantic_models": {
            "semantic_model.proj.orders": {
                "name": "orders",
                "depends_on": {"nodes": ["model.proj.orders"]},
            },
        },
    }


class TestFindOrphanModelNodes:
    def test_returns_unreferenced_model_only(self) -> None:
        orphans = find_orphan_model_nodes(_fake_manifest())
        names = [n["name"] for n in orphans]
        assert names == ["raw_events"]

    def test_ignores_non_model_resource_types(self) -> None:
        orphans = find_orphan_model_nodes(_fake_manifest())
        assert all(n["resource_type"] == "model" for n in orphans)

    def test_empty_manifest_is_safe(self) -> None:
        assert find_orphan_model_nodes({}) == []

    def test_semantic_model_without_depends_on_doesnt_crash(self) -> None:
        manifest = {
            "nodes": {
                "model.p.a": {"resource_type": "model", "name": "a", "columns": {}},
            },
            "semantic_models": {
                "semantic_model.p.sm": {"name": "sm"},  # no depends_on
            },
        }
        orphans = find_orphan_model_nodes(manifest)
        assert [n["name"] for n in orphans] == ["a"]


class TestRegularModelsFromManifest:
    def test_builds_dbt_regular_model(self) -> None:
        models = regular_models_from_manifest(_fake_manifest())
        assert len(models) == 1
        rm = models[0]
        assert rm.name == "raw_events"
        assert rm.database == "analytics"
        assert rm.schema_name == "staging"
        assert rm.alias == "raw_events_v2"
        assert rm.description == "Raw event log"
        assert rm.tags == []
        assert len(rm.columns) == 1
        assert rm.columns[0].name == "event_id"
        assert rm.columns[0].description is None
        assert rm.columns[0].data_type == "varchar"


class TestLoadOrGenerateManifest:
    def test_loads_existing_manifest_file(self, tmp_path) -> None:
        target = tmp_path / "target"
        target.mkdir()
        payload = {"nodes": {}, "semantic_models": {}}
        (target / "manifest.json").write_text(json.dumps(payload))

        loaded = load_or_generate_manifest(str(tmp_path))
        assert loaded == payload

    def test_returns_none_when_dbt_unavailable_and_no_file(self, tmp_path) -> None:
        with patch.object(dbt_manifest, "DBT_AVAILABLE", False):
            assert load_or_generate_manifest(str(tmp_path)) is None

    def test_invokes_dbt_parse_when_manifest_missing(self, tmp_path) -> None:
        # Simulate a successful dbt parse that writes manifest.json.
        target = tmp_path / "target"
        payload = {"nodes": {}, "semantic_models": {}}

        def fake_runner():
            runner = MagicMock()

            def invoke(args):
                assert "parse" in args
                target.mkdir(exist_ok=True)
                (target / "manifest.json").write_text(json.dumps(payload))
                result = MagicMock()
                result.success = True
                return result

            runner.invoke.side_effect = invoke
            return runner

        with patch.object(dbt_manifest, "DBT_AVAILABLE", True), \
             patch.object(dbt_manifest, "dbtRunner", fake_runner):
            loaded = load_or_generate_manifest(str(tmp_path))
        assert loaded == payload

    def test_returns_none_when_dbt_parse_fails(self, tmp_path) -> None:
        def fake_runner():
            runner = MagicMock()
            result = MagicMock()
            result.success = False
            result.exception = RuntimeError("boom")
            runner.invoke.return_value = result
            return runner

        with patch.object(dbt_manifest, "DBT_AVAILABLE", True), \
             patch.object(dbt_manifest, "dbtRunner", fake_runner):
            assert load_or_generate_manifest(str(tmp_path)) is None
