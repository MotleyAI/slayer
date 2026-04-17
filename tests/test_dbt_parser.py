"""Tests for dbt YAML parser."""

import json
import textwrap
from unittest.mock import patch

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

    def test_package_qualified_two_arg(self) -> None:
        """Regression for CodeRabbit #5 — two-arg package-qualified ref().
        Model name is the SECOND positional string arg, not the first."""
        assert _extract_ref_name("ref('my_package', 'orders')") == "orders"
        assert _extract_ref_name('ref("pkg", "orders")') == "orders"
        assert _extract_ref_name("ref( 'pkg' , 'orders' )") == "orders"

    def test_versioned_ref(self) -> None:
        """Versioned single-arg ref() with v=N kwarg."""
        assert _extract_ref_name("ref('orders', v=1)") == "orders"
        assert _extract_ref_name("ref('orders', version=2)") == "orders"

    def test_versioned_package_qualified(self) -> None:
        """Combined: package-qualified AND versioned."""
        assert _extract_ref_name("ref('pkg', 'orders', v=1)") == "orders"


class TestRegularModelsOptIn:
    """Regression for CodeRabbit B6-2 — manifest loading must be opt-in.

    Plain `parse_dbt_project(path)` should NOT call load_or_generate_manifest,
    which can invoke `dbt parse` (slow) and fail noisily without dbt-core.
    The manifest is only needed when --include-hidden-models is set, so
    parse_dbt_project gates it on include_regular_models=True.
    """

    def test_default_does_not_load_manifest(self, tmp_path) -> None:
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "orders.yaml").write_text(textwrap.dedent("""\
            semantic_models:
              - name: orders
                model: ref('orders')
                entities:
                  - name: order_id
                    type: primary
                    expr: id
                dimensions: []
                measures: []
        """))

        with patch("slayer.dbt.parser.load_or_generate_manifest") as mock_load:
            project = parse_dbt_project(str(tmp_path))

        mock_load.assert_not_called()
        assert len(project.semantic_models) == 1
        assert project.regular_models == []

    def test_opt_in_loads_manifest(self, tmp_path) -> None:
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "orders.yaml").write_text(textwrap.dedent("""\
            semantic_models:
              - name: orders
                model: ref('orders')
                entities:
                  - name: order_id
                    type: primary
                    expr: id
                dimensions: []
                measures: []
        """))

        with patch(
            "slayer.dbt.parser.load_or_generate_manifest", return_value=None
        ) as mock_load:
            parse_dbt_project(str(tmp_path), include_regular_models=True)

        mock_load.assert_called_once_with(str(tmp_path))


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


class TestParseDbtProjectSqlFiles:
    """.sql file scanning is always on — populates DbtRegularModel.raw_code
    so the converter can inline regular-model SQL into SlayerModel.sql.
    """

    def test_populates_raw_code_from_sql_files(self, tmp_path) -> None:
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "orders.sql").write_text(
            "select id, amount from {{ ref('raw_orders') }}"
        )
        project = parse_dbt_project(str(tmp_path))
        assert len(project.regular_models) == 1
        rm = project.regular_models[0]
        assert rm.name == "orders"
        assert rm.raw_code is not None
        assert "{{ ref('raw_orders') }}" in rm.raw_code

    def test_scans_sql_files_even_without_include_regular_models(self, tmp_path) -> None:
        # Default include_regular_models=False must not block SQL scanning.
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "orders.sql").write_text("select 1")
        project = parse_dbt_project(str(tmp_path))
        names = [rm.name for rm in project.regular_models]
        assert "orders" in names

    def test_skips_target_directory(self, tmp_path) -> None:
        # Compiled dbt output lives under target/; must not be ingested.
        target = tmp_path / "target" / "compiled" / "proj"
        target.mkdir(parents=True)
        (target / "orders.sql").write_text("-- do not ingest this")
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "orders.sql").write_text("select real_content")
        project = parse_dbt_project(str(tmp_path))
        # We get the models/ version, not the target/ version
        orders = next(rm for rm in project.regular_models if rm.name == "orders")
        assert orders.raw_code is not None
        assert "real_content" in orders.raw_code
        assert "do not ingest" not in orders.raw_code


class TestParseDbtProjectRegularModels:
    def test_no_manifest_yields_empty_regular_models(self, dbt_project_dir) -> None:
        # Pass include_regular_models=True to actually exercise the manifest
        # code path; without it, the manifest isn't loaded at all (B6-2).
        # The dbt_project_dir fixture writes YAMLs only — no .sql files — so
        # regular_models stays empty.
        project = parse_dbt_project(str(dbt_project_dir), include_regular_models=True)
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

        project = parse_dbt_project(str(dbt_project_dir), include_regular_models=True)
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
