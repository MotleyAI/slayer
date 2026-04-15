"""Tests for dbt YAML parser."""

import textwrap

import pytest

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
