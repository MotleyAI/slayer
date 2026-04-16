"""Tests for the dbt-to-SLayer converter."""

import textwrap
from unittest.mock import MagicMock, patch

import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError

from slayer.core.enums import DataType
from slayer.core.models import Dimension, Measure, SlayerModel
from slayer.dbt import converter as converter_module
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import (
    DbtColumnMeta,
    DbtDefaults,
    DbtDimension,
    DbtEntity,
    DbtMeasure,
    DbtMetric,
    DbtMetricInput,
    DbtMetricTypeParams,
    DbtProject,
    DbtRegularModel,
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
        orders = next(m for m in result.models if m.name == "orders")
        assert orders.name == "orders"
        assert orders.sql_table == "orders"
        assert orders.data_source == "test_db"
        assert orders.description == "Order data"
        assert orders.default_time_dimension == "order_date"

    def test_dimensions(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names
        assert "order_date" in dim_names

    def test_dimension_types(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")
        status = next(d for d in orders.dimensions if d.name == "status")
        order_date = next(d for d in orders.dimensions if d.name == "order_date")
        assert status.type == DataType.STRING
        assert order_date.type == DataType.TIMESTAMP

    def test_measures(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")
        assert len(orders.measures) == 2
        amount = next(m for m in orders.measures if m.name == "total_amount")
        assert amount.sql == "amount"
        assert amount.allowed_aggregations == ["sum"]

    def test_primary_key_dimension(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")
        pk_dims = [d for d in orders.dimensions if d.primary_key]
        assert len(pk_dims) >= 1
        assert any(d.name == "id" for d in pk_dims)

    def test_joins_from_entities(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")
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
        assert m.sql == "amount"
        assert m.name == "revenue_sum"
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
    def test_derived_metric_ref_replacement_is_token_aware(self) -> None:
        """Regression for CodeRabbit #3 — when a metric named 'total' is referenced
        inside a derived expression that also mentions 'subtotal' or 'total_orders',
        only the standalone 'total' token must be replaced. Plain str.replace
        previously mutated the substring inside the other identifiers."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    dimensions=[],
                    measures=[
                        DbtMeasure(name="total", agg="sum", expr="amount"),
                        DbtMeasure(name="subtotal", agg="sum", expr="subtotal"),
                        DbtMeasure(name="total_orders", agg="count", expr="id"),
                    ],
                ),
            ],
            metrics=[
                DbtMetric(name="total", type="simple",
                          type_params=DbtMetricTypeParams(measure="total")),
                DbtMetric(name="subtotal", type="simple",
                          type_params=DbtMetricTypeParams(measure="subtotal")),
                DbtMetric(name="total_orders", type="simple",
                          type_params=DbtMetricTypeParams(measure="total_orders")),
                DbtMetric(
                    name="weird_ratio",
                    type="derived",
                    type_params=DbtMetricTypeParams(
                        expr="(subtotal + total) / total_orders",
                        metrics=[
                            DbtMetricInput(name="total"),
                            DbtMetricInput(name="subtotal"),
                            DbtMetricInput(name="total_orders"),
                        ],
                    ),
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        q = next(qq for qq in result.queries if qq["name"] == "weird_ratio")
        formula = q["fields"][0]["formula"]
        # `total:sum`, `subtotal:sum`, and `total_orders:count` should all appear,
        # each as a complete token. The bug would have produced something like
        # `subtotal:sum + total:sum) / total:sum_orders` because plain replace
        # rewrites the "total" substring inside "subtotal" and "total_orders".
        assert "total:sum" in formula
        assert "subtotal:sum" in formula
        assert "total_orders:count" in formula
        # Bug check: the "total" inside "subtotal" was NOT mangled into "total:sum"
        assert "subtotal:sum" in formula
        assert "subtotal:sum:sum" not in formula
        # Bug check: the "total" inside "total_orders" was NOT mangled
        assert "total:sum_orders" not in formula

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


class TestQueriesDirForStorage:
    """Regression tests for CodeRabbit #1 — _queries_dir_for_storage helper.

    `slayer import-dbt --storage slayer.db` must write queries.yaml beside the
    SQLite file, not inside `slayer.db/queries.yaml` (which would fail because
    the .db file is not a directory).
    """

    def test_directory_storage_path_returned_as_is(self) -> None:
        from slayer.cli import _queries_dir_for_storage

        assert _queries_dir_for_storage("./slayer_data") == "./slayer_data"
        assert _queries_dir_for_storage("/tmp/models") == "/tmp/models"

    def test_sqlite_db_uses_parent_directory(self) -> None:
        from slayer.cli import _queries_dir_for_storage

        assert _queries_dir_for_storage("/tmp/slayer.db") == "/tmp"
        assert _queries_dir_for_storage("./data/slayer.sqlite") == "./data"
        assert _queries_dir_for_storage("project.sqlite3") == "."

    def test_bare_sqlite_filename_in_cwd(self) -> None:
        """A bare 'slayer.db' (no directory) should write to the current dir."""
        from slayer.cli import _queries_dir_for_storage

        assert _queries_dir_for_storage("slayer.db") == "."


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


def _sample_slayer_model(name: str = "raw_events") -> SlayerModel:
    """A realistic result of introspecting a regular dbt model."""
    return SlayerModel(
        name=name,
        sql_table="staging.raw_events",
        data_source="test_db",
        dimensions=[
            Dimension(name="event_id", sql="event_id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="event_type", sql="event_type", type=DataType.STRING),
        ],
        measures=[
            Measure(name="event_type", sql="event_type"),
        ],
    )


def _project_with_orphan(
    *,
    with_semantic: bool = True,
    orphan_name: str = "raw_events",
    extra_column_descriptions: bool = True,
) -> DbtProject:
    semantic_models = []
    if with_semantic:
        semantic_models.append(
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                dimensions=[DbtDimension(name="status", type="categorical")],
                measures=[DbtMeasure(name="total", agg="sum", expr="amount")],
            )
        )
    columns = []
    if extra_column_descriptions:
        columns = [
            DbtColumnMeta(name="event_id", description="Unique event identifier"),
            DbtColumnMeta(name="event_type", description="Category of event"),
        ]
    return DbtProject(
        semantic_models=semantic_models,
        metrics=[],
        regular_models=[
            DbtRegularModel(
                name=orphan_name,
                schema_name="staging",
                alias=orphan_name,
                description="Raw event log",
                columns=columns,
            ),
        ],
    )


class TestRegularModelConversion:
    """Hidden-model import from regular dbt models."""

    def test_default_off_skips_regular_models(self) -> None:
        project = _project_with_orphan()
        # No sa_engine, no flag — hidden-model pass must be a no-op.
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        assert all(not m.hidden for m in result.models)
        assert [m.name for m in result.models] == ["orders"]

    def test_opt_in_without_engine_warns_and_skips(self) -> None:
        project = _project_with_orphan()
        result = DbtToSlayerConverter(
            project=project, data_source="test_db", include_hidden_models=True,
        ).convert()
        assert [m.name for m in result.models] == ["orders"]
        assert any("no SQLAlchemy engine" in w.message for w in result.warnings)

    def test_opt_in_with_engine_produces_hidden_model(self) -> None:
        project = _project_with_orphan()
        engine = MagicMock(spec=sa.Engine)
        fake_model = _sample_slayer_model(name="raw_events")

        with patch.object(sa, "inspect", return_value=MagicMock()), \
             patch.object(converter_module, "introspect_table_to_model", return_value=fake_model):
            result = DbtToSlayerConverter(
                project=project,
                data_source="test_db",
                include_hidden_models=True,
                sa_engine=engine,
            ).convert()

        hidden = [m for m in result.models if m.hidden]
        assert len(hidden) == 1
        raw = hidden[0]
        assert raw.name == "raw_events"
        # Model description overlaid from dbt manifest
        assert raw.description == "Raw event log"
        # Column descriptions overlaid onto dimensions
        event_id_dim = next(d for d in raw.dimensions if d.name == "event_id")
        assert event_id_dim.description == "Unique event identifier"

    def test_introspection_failure_is_skipped_with_warning(self) -> None:
        project = _project_with_orphan()
        engine = MagicMock(spec=sa.Engine)

        def raise_err(**_kwargs):
            raise SQLAlchemyError("table not found")

        with patch.object(sa, "inspect", return_value=MagicMock()), \
             patch.object(converter_module, "introspect_table_to_model", side_effect=raise_err):
            result = DbtToSlayerConverter(
                project=project,
                data_source="test_db",
                include_hidden_models=True,
                sa_engine=engine,
            ).convert()

        # Semantic model still came through
        assert [m.name for m in result.models] == ["orders"]
        # And a warning was recorded
        assert any(w.model_name == "raw_events" for w in result.warnings)

    def test_name_collision_prefers_semantic_model(self) -> None:
        # Regular model named the same as the semantic model — must be skipped
        # so the semantic (visible) model is not shadowed.
        project = _project_with_orphan(orphan_name="orders")
        engine = MagicMock(spec=sa.Engine)
        fake_model = _sample_slayer_model(name="orders")

        with patch.object(sa, "inspect", return_value=MagicMock()), \
             patch.object(converter_module, "introspect_table_to_model", return_value=fake_model):
            result = DbtToSlayerConverter(
                project=project,
                data_source="test_db",
                include_hidden_models=True,
                sa_engine=engine,
            ).convert()

        # Only the semantic (visible) model survives under the name 'orders'
        assert len(result.models) == 1
        assert result.models[0].name == "orders"
        assert not result.models[0].hidden
