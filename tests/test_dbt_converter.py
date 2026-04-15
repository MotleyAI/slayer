"""Tests for the dbt-to-SLayer converter."""

import textwrap


from slayer.core.enums import DataType
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import (
    DbtDefaults,
    DbtDimension,
    DbtEntity,
    DbtMeasure,
    DbtMetric,
    DbtMetricInput,
    DbtMetricTypeParams,
    DbtProject,
    DbtSemanticModel,
)
from slayer.dbt.parser import parse_dbt_project


def _make_simple_project():
    """Create a minimal dbt project for testing."""
    return DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                description="Order data",
                defaults=DbtDefaults(agg_time_dimension="order_date"),
                entities=[
                    DbtEntity(name="order_id", type="primary", expr="id"),
                    DbtEntity(name="customer_id", type="foreign"),
                ],
                dimensions=[
                    DbtDimension(name="status", type="categorical"),
                    DbtDimension(name="order_date", type="time"),
                ],
                measures=[
                    DbtMeasure(name="total_amount", agg="sum", expr="amount"),
                    DbtMeasure(name="order_count", agg="count", expr="id"),
                ],
            ),
            DbtSemanticModel(
                name="customers",
                model="customers",
                entities=[
                    DbtEntity(name="customer_id", type="primary", expr="id"),
                ],
                dimensions=[
                    DbtDimension(name="name", type="categorical"),
                    DbtDimension(name="region", type="categorical"),
                ],
                measures=[],
            ),
        ],
        metrics=[],
    )


class TestBasicConversion:
    def test_model_count(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        assert len(result.models) == 2

    def test_model_fields(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = result.models[0]
        assert orders.name == "orders"
        assert orders.sql_table == "orders"
        assert orders.data_source == "test_db"
        assert orders.description == "Order data"
        assert orders.default_time_dimension == "order_date"

    def test_dimensions(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = result.models[0]
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names
        assert "order_date" in dim_names

    def test_dimension_types(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = result.models[0]
        status = next(d for d in orders.dimensions if d.name == "status")
        order_date = next(d for d in orders.dimensions if d.name == "order_date")
        assert status.type == DataType.STRING
        assert order_date.type == DataType.TIMESTAMP

    def test_measures(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = result.models[0]
        assert len(orders.measures) == 2
        amount = next(m for m in orders.measures if m.name == "total_amount")
        assert amount.sql == "amount"
        assert amount.allowed_aggregations == ["sum"]

    def test_primary_key_dimension(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = result.models[0]
        pk_dims = [d for d in orders.dimensions if d.primary_key]
        assert len(pk_dims) >= 1
        assert any(d.name == "id" for d in pk_dims)

    def test_joins_from_entities(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = result.models[0]
        assert len(orders.joins) == 1
        assert orders.joins[0].target_model == "customers"
        assert orders.joins[0].join_pairs == [["customer_id", "id"]]


class TestMeasureConsolidation:
    def test_same_expr_consolidated(self) -> None:
        """Measures with same expr but different aggs become one SLayer measure."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                dimensions=[],
                measures=[
                    DbtMeasure(name="revenue_sum", agg="sum", expr="amount"),
                    DbtMeasure(name="revenue_avg", agg="average", expr="amount"),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]
        # Should be consolidated into one measure
        assert len(orders.measures) == 1
        m = orders.measures[0]
        assert m.sql == "amount" or m.name == "amount"
        assert "sum" in m.allowed_aggregations
        assert "avg" in m.allowed_aggregations
        assert "revenue_sum" in m.description
        assert "revenue_avg" in m.description

    def test_different_expr_not_consolidated(self) -> None:
        """Measures with different exprs stay separate."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                dimensions=[],
                measures=[
                    DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    DbtMeasure(name="quantity", agg="sum", expr="qty"),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]
        assert len(orders.measures) == 2

    def test_no_strict_aggregations(self) -> None:
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                dimensions=[],
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
        ])
        result = DbtToSlayerConverter(
            project=project, data_source="test", strict_aggregations=False,
        ).convert()
        m = result.models[0].measures[0]
        assert m.allowed_aggregations is None


class TestSimpleMetricConversion:
    def test_filtered_metric_becomes_measure(self) -> None:
        """Simple metric with filter → filtered measure on the base model."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    dimensions=[DbtDimension(name="status", type="categorical")],
                    measures=[DbtMeasure(name="total_amount", agg="sum", expr="amount")],
                ),
            ],
            metrics=[
                DbtMetric(
                    name="completed_amount",
                    type="simple",
                    label="Completed Amount",
                    type_params=DbtMetricTypeParams(measure="total_amount"),
                    filter="{{Dimension('order_id__status')}} = 'completed'",
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]
        # Should have the original measure plus the filtered one
        filtered = [m for m in orders.measures if m.name == "completed_amount"]
        assert len(filtered) == 1
        assert filtered[0].filter is not None
        assert "completed" in filtered[0].filter
        assert filtered[0].label == "Completed Amount"

    def test_unfiltered_simple_metric_no_extra_measure(self) -> None:
        """Simple metric without filter doesn't add anything."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    dimensions=[],
                    measures=[DbtMeasure(name="total_amount", agg="sum", expr="amount")],
                ),
            ],
            metrics=[
                DbtMetric(
                    name="total_amount",
                    type="simple",
                    type_params=DbtMetricTypeParams(measure="total_amount"),
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]
        assert len(orders.measures) == 1  # Just the original, no duplicate


class TestDerivedMetricConversion:
    def test_derived_metric_generates_query(self) -> None:
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    dimensions=[],
                    measures=[
                        DbtMeasure(name="total_amount", agg="sum", expr="amount"),
                        DbtMeasure(name="order_count", agg="count", expr="id"),
                    ],
                ),
            ],
            metrics=[
                DbtMetric(
                    name="total_amount_metric",
                    type="simple",
                    type_params=DbtMetricTypeParams(measure="total_amount"),
                ),
                DbtMetric(
                    name="order_count_metric",
                    type="simple",
                    type_params=DbtMetricTypeParams(measure="order_count"),
                ),
                DbtMetric(
                    name="avg_order_value",
                    type="derived",
                    description="Average order value",
                    type_params=DbtMetricTypeParams(
                        expr="total_amount_metric / order_count_metric",
                        metrics=[
                            DbtMetricInput(name="total_amount_metric"),
                            DbtMetricInput(name="order_count_metric"),
                        ],
                    ),
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        assert len(result.queries) == 1
        q = result.queries[0]
        assert q["name"] == "avg_order_value"
        assert "source_model" in q
        assert len(q["fields"]) == 1


class TestConversionWarnings:
    def test_conversion_metric_warning(self) -> None:
        project = DbtProject(
            semantic_models=[],
            metrics=[
                DbtMetric(name="visit_to_buy", type="conversion"),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        assert len(result.warnings) == 1
        assert "not supported" in result.warnings[0].message.lower()

    def test_unknown_metric_type_warning(self) -> None:
        project = DbtProject(
            semantic_models=[],
            metrics=[
                DbtMetric(name="weird", type="unknown_type"),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        assert len(result.warnings) == 1


class TestParserRoundTrip:
    """Test parsing YAML → converting → verifying output."""

    def test_roundtrip(self, tmp_path) -> None:
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        (models_dir / "orders.yaml").write_text(textwrap.dedent("""\
            semantic_models:
              - name: orders
                model: ref('orders')
                defaults:
                  agg_time_dimension: order_date
                entities:
                  - name: order_id
                    type: primary
                    expr: id
                dimensions:
                  - name: status
                    type: categorical
                    label: Order Status
                  - name: order_date
                    type: time
                    type_params:
                      time_granularity: day
                measures:
                  - name: revenue
                    agg: sum
                    expr: amount
                    label: Revenue
        """))

        project = parse_dbt_project(str(tmp_path))
        result = DbtToSlayerConverter(project=project, data_source="mydb").convert()

        assert len(result.models) == 1
        m = result.models[0]
        assert m.name == "orders"
        assert m.sql_table == "orders"
        assert m.data_source == "mydb"
        assert m.default_time_dimension == "order_date"

        # Labels preserved
        status_dim = next(d for d in m.dimensions if d.name == "status")
        assert status_dim.label == "Order Status"

        rev_measure = next(me for me in m.measures if me.name == "revenue")
        assert rev_measure.label == "Revenue"
        assert rev_measure.allowed_aggregations == ["sum"]
