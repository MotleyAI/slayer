"""Tests for dbt YAML parser."""

import json
import textwrap

import pytest

from slayer.dbt.models import DbtMeasure
from slayer.dbt.parser import parse_dbt_project, _extract_ref_name


class TestExtractRefName:
    def test_single_quotes(self) -> None:
        assert _extract_ref_name("ref('claim')") == "claim"

    def test_double_quotes(self) -> None:
        assert _extract_ref_name('ref("claim")') == "claim"

    def test_spaces(self) -> None:
        assert _extract_ref_name("ref( 'claim' )") == "claim"

    def test_plain_string(self) -> None:
        assert _extract_ref_name("plain_name") == "plain_name"


@pytest.fixture
def dbt_project_dir(tmp_path):
    """Create a minimal dbt project with semantic models and metrics."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    # Semantic model file
    (models_dir / "orders.yaml").write_text(textwrap.dedent("""\
        semantic_models:
          - name: orders
            model: ref('orders')
            description: "Order data"
            defaults:
              agg_time_dimension: order_date
            entities:
              - name: order_id
                type: primary
                expr: id
              - name: customer_id
                type: foreign
            dimensions:
              - name: status
                type: categorical
              - name: order_date
                type: time
                type_params:
                  time_granularity: day
            measures:
              - name: total_amount
                agg: sum
                expr: amount
                description: "Total order amount"
              - name: order_count
                agg: count
                expr: id
    """))

    # Metric file
    (models_dir / "metrics.yaml").write_text(textwrap.dedent("""\
        metrics:
          - name: completed_amount
            type: simple
            label: Completed Amount
            type_params:
              measure: total_amount
            filter: |
              {{Dimension('order_id__status')}} = 'completed'
          - name: avg_order_value
            type: derived
            type_params:
              expr: total_amount / order_count
              metrics:
                - name: total_amount
                - name: order_count
    """))

    return tmp_path


class TestParseDbtProject:
    def test_parse_semantic_models(self, dbt_project_dir) -> None:
        project = parse_dbt_project(str(dbt_project_dir))
        assert len(project.semantic_models) == 1

        sm = project.semantic_models[0]
        assert sm.name == "orders"
        assert sm.model == "orders"  # ref() extracted
        assert sm.defaults.agg_time_dimension == "order_date"

    def test_parse_entities(self, dbt_project_dir) -> None:
        project = parse_dbt_project(str(dbt_project_dir))
        sm = project.semantic_models[0]
        assert len(sm.entities) == 2
        assert sm.entities[0].name == "order_id"
        assert sm.entities[0].type == "primary"
        assert sm.entities[0].expr == "id"
        assert sm.entities[1].name == "customer_id"
        assert sm.entities[1].type == "foreign"

    def test_parse_dimensions(self, dbt_project_dir) -> None:
        project = parse_dbt_project(str(dbt_project_dir))
        sm = project.semantic_models[0]
        assert len(sm.dimensions) == 2
        assert sm.dimensions[0].name == "status"
        assert sm.dimensions[0].type == "categorical"
        assert sm.dimensions[1].name == "order_date"
        assert sm.dimensions[1].type == "time"
        assert sm.dimensions[1].type_params.time_granularity == "day"

    def test_parse_measures(self, dbt_project_dir) -> None:
        project = parse_dbt_project(str(dbt_project_dir))
        sm = project.semantic_models[0]
        assert len(sm.measures) == 2
        assert sm.measures[0].name == "total_amount"
        assert sm.measures[0].agg == "sum"
        assert sm.measures[0].expr == "amount"

    def test_parse_metrics(self, dbt_project_dir) -> None:
        project = parse_dbt_project(str(dbt_project_dir))
        assert len(project.metrics) == 2

        m = project.metrics[0]
        assert m.name == "completed_amount"
        assert m.type == "simple"
        assert m.label == "Completed Amount"
        assert m.type_params.measure == "total_amount"
        assert "Dimension" in (m.filter or "")

    def test_parse_derived_metric(self, dbt_project_dir) -> None:
        project = parse_dbt_project(str(dbt_project_dir))
        m = project.metrics[1]
        assert m.name == "avg_order_value"
        assert m.type == "derived"
        assert m.type_params.expr == "total_amount / order_count"
        assert len(m.type_params.metrics) == 2

    def test_empty_dir(self, tmp_path) -> None:
        project = parse_dbt_project(str(tmp_path))
        assert len(project.semantic_models) == 0
        assert len(project.metrics) == 0

    def test_skips_hidden_dirs(self, tmp_path) -> None:
        hidden = tmp_path / ".hidden"
        hidden.mkdir()
        (hidden / "test.yaml").write_text("semantic_models:\n  - name: secret\n")
        project = parse_dbt_project(str(tmp_path))
        assert len(project.semantic_models) == 0

    def test_numeric_measure_expr(self, tmp_path) -> None:
        """dbt allows `expr: 1` (int) for count-via-sum measures like number_of_policies."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "policy.yaml").write_text(textwrap.dedent("""\
            semantic_models:
              - name: policy
                model: ref('policy')
                entities:
                  - name: policy_id
                    type: primary
                dimensions:
                  - name: status
                    type: categorical
                measures:
                  - name: number_of_policies
                    agg: sum
                    expr: 1
        """))
        project = parse_dbt_project(str(tmp_path))
        assert len(project.semantic_models) == 1
        m = project.semantic_models[0].measures[0]
        assert m.name == "number_of_policies"
        assert m.expr == "1"
        assert isinstance(m.expr, str)


class TestParseDbtProjectRegularModels:
    def test_no_manifest_yields_empty_regular_models(self, dbt_project_dir) -> None:
        project = parse_dbt_project(str(dbt_project_dir))
        assert project.regular_models == []

    def test_populates_regular_models_from_manifest(self, dbt_project_dir) -> None:
        target = dbt_project_dir / "target"
        target.mkdir()
        manifest_payload = {
            "nodes": {
                "model.proj.orders": {
                    "resource_type": "model",
                    "name": "orders",
                    "schema": "public",
                    "alias": "orders",
                    "columns": {},
                },
                "model.proj.raw_events": {
                    "resource_type": "model",
                    "name": "raw_events",
                    "schema": "staging",
                    "alias": "raw_events",
                    "description": "Unwrapped raw events table",
                    "columns": {
                        "event_id": {"name": "event_id", "description": "PK"},
                    },
                },
            },
            "semantic_models": {
                "semantic_model.proj.orders": {
                    "name": "orders",
                    "depends_on": {"nodes": ["model.proj.orders"]},
                },
            },
        }
        (target / "manifest.json").write_text(json.dumps(manifest_payload))

        project = parse_dbt_project(str(dbt_project_dir))
        assert len(project.regular_models) == 1
        rm = project.regular_models[0]
        assert rm.name == "raw_events"
        assert rm.schema_name == "staging"
        assert rm.description == "Unwrapped raw events table"
        assert len(rm.columns) == 1
        assert rm.columns[0].name == "event_id"


class TestDbtMeasureExprCoercion:
    def test_int_expr_coerced_to_str(self) -> None:
        m = DbtMeasure(name="count_all", agg="sum", expr=1)
        assert m.expr == "1"

    def test_float_expr_coerced_to_str(self) -> None:
        m = DbtMeasure(name="weight", agg="sum", expr=1.5)
        assert m.expr == "1.5"

    def test_none_expr_stays_none(self) -> None:
        m = DbtMeasure(name="count_all", agg="count")
        assert m.expr is None

    def test_string_expr_unchanged(self) -> None:
        m = DbtMeasure(name="total", agg="sum", expr="amount")
        assert m.expr == "amount"
