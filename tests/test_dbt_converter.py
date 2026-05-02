"""Tests for the dbt-to-SLayer converter."""

import textwrap
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError

from slayer.core.enums import DataType
from slayer.core.format import NumberFormatType
from slayer.core.models import Column, SlayerModel
from slayer.dbt import converter as converter_module
from slayer.dbt.converter import DbtConversionError, DbtToSlayerConverter
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
        dim_names = [d.name for d in orders.columns]
        assert "status" in dim_names
        assert "order_date" in dim_names

    def test_dimension_types(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")
        status = next(d for d in orders.columns if d.name == "status")
        order_date = next(d for d in orders.columns if d.name == "order_date")
        assert status.type == DataType.STRING
        assert order_date.type == DataType.TIMESTAMP

    def test_measures(self) -> None:
        """v2 dbt converter splits dbt measures: a Column per unique expr,
        a ModelMeasure per dbt measure carrying the agg as colon syntax.
        """
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")

        col_names = {c.name for c in orders.columns}
        # Bare-identifier exprs become Column names directly (Q-1 / Q-I).
        assert "amount" in col_names
        # ``order_count`` had ``expr=id`` but ``id`` is the entity PK already on the
        # model, so the measure-derived Column gets a ``_col`` suffix (Q-2 / Q-I).
        assert "id_col" in col_names

        amount = next(c for c in orders.columns if c.name == "amount")
        assert amount.allowed_aggregations is None
        assert amount.format is not None
        assert amount.format.type == NumberFormatType.FLOAT
        assert amount.hidden is False  # Q-A

        # ModelMeasures carry the agg + the dbt name.
        measures_by_name = {m.name: m for m in orders.measures}
        assert "total_amount" in measures_by_name
        assert measures_by_name["total_amount"].formula == "amount:sum"
        assert "order_count" in measures_by_name
        assert measures_by_name["order_count"].formula == "id_col:count"

    def test_primary_key_dimension(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")
        pk_dims = [d for d in orders.columns if d.primary_key]
        assert len(pk_dims) >= 1
        assert any(d.name == "id" for d in pk_dims)

    def test_joins_from_entities(self) -> None:
        project = _make_simple_project()
        result = DbtToSlayerConverter(project=project, data_source="test_db").convert()
        orders = next(m for m in result.models if m.name == "orders")
        assert len(orders.joins) == 1
        assert orders.joins[0].target_model == "customers"
        assert orders.joins[0].join_pairs == [["customer_id", "id"]]


    def test_peer_joins_from_shared_primary_entity(self) -> None:
        """Two models with the same primary entity get bidirectional joins."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="claim",
                model="claim",
                entities=[DbtEntity(name="claim_identifier", type="primary")],
                dimensions=[DbtDimension(name="status", type="categorical"),
                ],
                measures=[
DbtMeasure(name="count", agg="count", expr="1")
                ,
                ],
            ),
            DbtSemanticModel(
                name="claim_coverage",
                model="claim_coverage",
                entities=[
                    DbtEntity(name="claim_identifier", type="primary"),
                    DbtEntity(name="policy_coverage_detail", type="foreign", expr="policy_coverage_detail_identifier"),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        claim = next(m for m in result.models if m.name == "claim")
        claim_cov = next(m for m in result.models if m.name == "claim_coverage")

        # claim should join to claim_coverage
        assert any(j.target_model == "claim_coverage" for j in claim.joins)
        # claim_coverage should join to claim
        assert any(j.target_model == "claim" for j in claim_cov.joins)
        # Join key should be claim_identifier on both sides
        claim_to_cov = next(j for j in claim.joins if j.target_model == "claim_coverage")
        assert claim_to_cov.join_pairs == [["claim_identifier", "claim_identifier"]]

    def test_peer_join_with_aliased_entity(self) -> None:
        """Peer join works when entity expr differs from name."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="agreement_party_role",
                model="agreement_party_role",
                entities=[DbtEntity(name="policy", type="primary", expr="agreement_identifier")],
                dimensions=[DbtDimension(name="party_role_code", type="categorical"),
                ],
            ),
            DbtSemanticModel(
                name="policy",
                model="policy",
                entities=[DbtEntity(name="policy", type="primary", expr="Policy_Identifier")],
                dimensions=[DbtDimension(name="policy_number", type="categorical"),
                ],
                measures=[
DbtMeasure(name="number_of_policies", agg="sum", expr="1")
                ,
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        apr = next(m for m in result.models if m.name == "agreement_party_role")
        policy = next(m for m in result.models if m.name == "policy")

        apr_to_policy = next(j for j in apr.joins if j.target_model == "policy")
        assert apr_to_policy.join_pairs == [["agreement_identifier", "Policy_Identifier"]]
        assert any(j.target_model == "agreement_party_role" for j in policy.joins)

    def test_peer_join_not_duplicated_with_foreign(self) -> None:
        """Foreign entity join is not duplicated by the peer pass."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[
                    DbtEntity(name="order_id", type="primary"),
                    DbtEntity(name="customer_id", type="foreign"),
                ],
            ),
            DbtSemanticModel(
                name="customers",
                model="customers",
                entities=[DbtEntity(name="customer_id", type="primary", expr="id")],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = next(m for m in result.models if m.name == "orders")
        customer_joins = [j for j in orders.joins if j.target_model == "customers"]
        assert len(customer_joins) == 1

    def test_three_model_peer_group(self) -> None:
        """Three models sharing the same primary entity all get peer joins."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="a", model="a",
                entities=[DbtEntity(name="shared_id", type="primary")],
            ),
            DbtSemanticModel(
                name="b", model="b",
                entities=[DbtEntity(name="shared_id", type="primary")],
            ),
            DbtSemanticModel(
                name="c", model="c",
                entities=[DbtEntity(name="shared_id", type="primary")],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        a = next(m for m in result.models if m.name == "a")
        b = next(m for m in result.models if m.name == "b")
        c = next(m for m in result.models if m.name == "c")
        assert {j.target_model for j in a.joins} == {"b", "c"}
        assert {j.target_model for j in b.joins} == {"a", "c"}
        assert {j.target_model for j in c.joins} == {"a", "b"}


class TestMeasureConsolidation:
    def test_same_expr_consolidated(self) -> None:
        """Measures with same expr collapse into one SLayer column; one ModelMeasure per dbt measure."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                measures=[
                    DbtMeasure(name="revenue_sum", agg="sum", expr="amount"),
                    DbtMeasure(name="revenue_avg", agg="average", expr="amount"),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]

        # One Column for the shared expr; consolidation description (Q-C) dropped.
        amount_cols = [c for c in orders.columns if c.name == "amount"]
        assert len(amount_cols) == 1
        assert amount_cols[0].description in (None, "")
        assert amount_cols[0].allowed_aggregations is None

        # One ModelMeasure per dbt measure, formula carries the agg.
        by_name = {m.name: m for m in orders.measures}
        assert by_name["revenue_sum"].formula == "amount:sum"
        assert by_name["revenue_avg"].formula == "amount:avg"

    def test_different_expr_not_consolidated(self) -> None:
        """Measures with different exprs stay separate columns."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                measures=[
                    DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    DbtMeasure(name="quantity", agg="sum", expr="qty"),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]
        col_names = {c.name for c in orders.columns}
        # Bare-identifier exprs become the Column names.
        assert "amount" in col_names
        assert "qty" in col_names

        measures_by_name = {m.name: m for m in orders.measures}
        assert measures_by_name["revenue"].formula == "amount:sum"
        assert measures_by_name["quantity"].formula == "qty:sum"

    def test_primary_entity_does_not_duplicate_pk_column(self) -> None:
        """When primary_entity resolves to the same column the entity loop already
        appended, the shorthand block must not append it a second time."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                primary_entity="order_id",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        id_cols = [c for c in result.models[0].columns if c.name == "id"]
        assert len(id_cols) == 1
        assert id_cols[0].primary_key is True


class TestSimpleMetricConversion:
    def test_filtered_metric_becomes_measure(self) -> None:
        """Simple metric with filter → filtered measure on the base model."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    dimensions=[DbtDimension(name="status", type="categorical"),
                    ],
                    measures=[
DbtMeasure(name="total_amount", agg="sum", expr="amount")
                    ,
                    ],
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
        # Filtered simple metric: a Column carries the WHERE filter (no
        # allowed_aggregations whitelist), and a ModelMeasure points at it
        # using the dbt measure's aggregation.
        filtered_cols = [c for c in orders.columns if c.filter is not None and "completed" in c.filter]
        assert len(filtered_cols) == 1
        assert filtered_cols[0].allowed_aggregations is None

        completed = next(m for m in orders.measures if m.name == "completed_amount")
        assert completed.label == "Completed Amount"
        # Formula references the filter-bearing column with the dbt agg.
        assert completed.formula == f"{filtered_cols[0].name}:sum"

    def test_filtered_metric_collision_with_existing_column_skipped(self) -> None:
        """Filtered metric whose name collides with an existing column on the model
        must NOT silently produce a duplicate column. Skip + emit a warning instead."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    dimensions=[
                        # A dimension named "status" — becomes a Column on the slayer model.
                        DbtDimension(name="status", type="categorical"),
                    ],
                    measures=[
                        DbtMeasure(name="total_amount", agg="sum", expr="amount"),
                    ],
                ),
            ],
            metrics=[
                DbtMetric(
                    # Collides with the existing "status" dimension column.
                    name="status",
                    type="simple",
                    type_params=DbtMetricTypeParams(measure="total_amount"),
                    filter="{{Dimension('order_id__status')}} = 'completed'",
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = next(m for m in result.models if m.name == "orders")
        # Exactly one column named "status" — no duplicate appended.
        assert [c.name for c in orders.columns].count("status") == 1
        # No ModelMeasure with the colliding name was added either.
        assert not any(m.name == "status" for m in orders.measures)
        warning_msgs = [w.message for w in result.warnings]
        assert any("status" in m and "collide" in m.lower() for m in warning_msgs), (
            f"Expected collision warning, got: {warning_msgs}"
        )

    def test_unfiltered_simple_metric_no_extra_measure(self) -> None:
        """Simple metric without filter doesn't add anything — the underlying
        ModelMeasure is already addressable on its own model."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    measures=[
DbtMeasure(name="total_amount", agg="sum", expr="amount")
                    ,
                    ],
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
        # No extra Column added — only the original "amount" expr column.
        amount_cols = [c for c in orders.columns if c.name == "amount"]
        assert len(amount_cols) == 1
        # Exactly one ModelMeasure named total_amount.
        assert [m.name for m in orders.measures].count("total_amount") == 1


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
        # Derived metric is a ModelMeasure on the orders model; bare-name refs
        # (Q-5) — the formula references each input metric by its name, no
        # colon-aggregation suffix.
        orders = next(m for m in result.models if m.name == "orders")
        weird = next(m for m in orders.measures if m.name == "weird_ratio")
        formula = weird.formula
        # All three input names appear as complete tokens.
        assert "total" in formula
        assert "subtotal" in formula
        assert "total_orders" in formula
        # Token-aware substitution didn't mangle "subtotal" or "total_orders".
        assert "subtotal:sum" not in formula
        assert "total:sum_orders" not in formula

    def test_derived_metric_becomes_model_measure(self) -> None:
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
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
        # Derived metric folded into a ModelMeasure on the source model — no
        # SlayerQuery dicts are produced.
        orders = next(m for m in result.models if m.name == "orders")
        m = next(mm for mm in orders.measures if mm.name == "avg_order_value")
        assert m.formula == "total_amount_metric / order_count_metric"
        assert m.description == "Average order value"


class TestConversionWarnings:
    def test_conversion_metric_routed_to_unconverted(self) -> None:
        project = DbtProject(
            semantic_models=[],
            metrics=[
                DbtMetric(name="visit_to_buy", type="conversion"),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        assert len(result.unconverted_metrics) == 1
        assert "not supported" in result.unconverted_metrics[0].message.lower()

    def test_unknown_metric_type_routed_to_unconverted(self) -> None:
        project = DbtProject(
            semantic_models=[],
            metrics=[
                DbtMetric(name="weird", type="unknown_type"),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        assert len(result.unconverted_metrics) == 1


class TestImportDbtCli:
    """End-to-end regression tests for slayer import-dbt (`_run_import_dbt`)."""

    def test_models_are_persisted_to_storage(self, tmp_path) -> None:
        """Regression for CodeRabbit B6-1 — _run_import_dbt must wrap the async
        storage.save_model with run_sync. Without that wrapper, save_model returns
        a coroutine that's silently discarded and the model is never written.

        End-to-end: build a minimal dbt project on disk, run the CLI handler,
        and assert the model is actually retrievable from storage afterwards."""
        import argparse
        import textwrap as _tw

        from slayer.async_utils import run_sync
        from slayer.cli import _run_import_dbt
        from slayer.storage.yaml_storage import YAMLStorage

        # Minimal dbt project with one semantic model + one measure
        project_dir = tmp_path / "dbt_project"
        models_dir = project_dir / "models"
        models_dir.mkdir(parents=True)
        (models_dir / "orders.yaml").write_text(_tw.dedent("""\
            semantic_models:
              - name: orders
                model: ref('orders')
                entities:
                  - name: order_id
                    type: primary
                    expr: id
                dimensions:
                  - name: status
                    type: categorical
                measures:
                  - name: total
                    agg: sum
                    expr: amount
        """))

        storage_dir = tmp_path / "slayer_data"
        storage_dir.mkdir()

        args = argparse.Namespace(
            dbt_project_path=str(project_dir),
            datasource="test_db",
            storage=str(storage_dir),
            models_dir=None,
            include_hidden_models=False,
        )

        _run_import_dbt(args)

        # The persisted model should be retrievable. If save_model's coroutine
        # was discarded (the bug), get_model returns None and this assertion fails.
        storage = YAMLStorage(base_dir=str(storage_dir))
        persisted = run_sync(storage.get_model("orders"))
        assert persisted is not None, (
            "orders model was not persisted — storage.save_model coroutine "
            "was likely discarded without run_sync"
        )
        assert persisted.name == "orders"
        # The dbt measure 'total' becomes a ModelMeasure (not a Column) under v2.
        assert any(m.name == "total" for m in persisted.measures)


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

        # Dimension labels preserved on Columns.
        status_dim = next(d for d in m.columns if d.name == "status")
        assert status_dim.label == "Order Status"

        # Measure labels live on the ModelMeasure (Q-D); the dbt measure
        # 'revenue' has expr=amount so it lowers into a Column 'amount' and
        # a ModelMeasure 'revenue' carrying the agg + label.
        rev_measure = next(me for me in m.measures if me.name == "revenue")
        assert rev_measure.label == "Revenue"
        assert rev_measure.formula == "amount:sum"


def _sample_slayer_model(name: str = "raw_events") -> SlayerModel:
    """A realistic result of introspecting a regular dbt model."""
    return SlayerModel(
        name=name,
        sql_table="staging.raw_events",
        data_source="test_db",
        columns=[
            Column(name="event_id", sql="event_id", type=DataType.NUMBER, primary_key=True),
            Column(name="event_type", sql="event_type", type=DataType.STRING),
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
                dimensions=[DbtDimension(name="status", type="categorical"),
                ],
                measures=[
DbtMeasure(name="total", agg="sum", expr="amount")
                ,
                ],
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
        event_id_dim = next(d for d in raw.columns if d.name == "event_id")
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


class TestForeignEntityJoinsAllPrimaries:
    """Foreign entities must produce joins to ALL matching primary models, not just the first."""

    def test_foreign_entity_joins_both_policy_and_agreement_party_role(self) -> None:
        """policy_amount foreign entity 'policy' matches both policy and agreement_party_role."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="policy_amount",
                model="policy_amount",
                entities=[
                    DbtEntity(name="policy_amount", type="primary", expr="Policy_Amount_Identifier"),
                    DbtEntity(name="policy", type="foreign", expr="Policy_Identifier"),
                ],
                measures=[
DbtMeasure(name="total", agg="sum", expr="amount")
                ,
                ],
            ),
            DbtSemanticModel(
                name="policy",
                model="policy",
                entities=[DbtEntity(name="policy", type="primary", expr="Policy_Identifier")],
                dimensions=[DbtDimension(name="policy_number", type="categorical"),
                ],
            ),
            DbtSemanticModel(
                name="agreement_party_role",
                model="agreement_party_role",
                entities=[DbtEntity(name="policy", type="primary", expr="agreement_identifier")],
                dimensions=[DbtDimension(name="party_role_code", type="categorical"),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        pa = next(m for m in result.models if m.name == "policy_amount")
        targets = {j.target_model for j in pa.joins}
        assert "policy" in targets, f"Missing direct join to policy. Joins: {targets}"
        assert "agreement_party_role" in targets, f"Missing join to agreement_party_role. Joins: {targets}"

    def test_foreign_entity_single_primary_unchanged(self) -> None:
        """Foreign entity with one matching primary still works."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[
                    DbtEntity(name="order_id", type="primary"),
                    DbtEntity(name="customer", type="foreign", expr="customer_id"),
                ],
            ),
            DbtSemanticModel(
                name="customers",
                model="customers",
                entities=[DbtEntity(name="customer", type="primary", expr="id")],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = next(m for m in result.models if m.name == "orders")
        assert len([j for j in orders.joins if j.target_model == "customers"]) == 1


class TestMetricFilterDimensionQualification:
    """Metric filters referencing cross-model dimensions must be qualified at ingestion time."""

    def test_filter_dim_on_peer_model_gets_qualified(self) -> None:
        """Dimension('claim_amount__has_loss_payment') where dim is on loss_payment, not claim_amount."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="claim_amount",
                    model="claim_amount",
                    entities=[
                        DbtEntity(name="claim_amount", type="primary", expr="claim_amount_identifier"),
                    ],
                    dimensions=[DbtDimension(name="amount_type_code", type="categorical"),
                    ],
                    measures=[
DbtMeasure(name="total_claim_amount", agg="sum", expr="claim_amount")
                    ,
                    ],
                ),
                DbtSemanticModel(
                    name="loss_payment",
                    model="loss_payment",
                    entities=[
                        DbtEntity(name="claim_amount", type="primary", expr="Claim_Amount_Identifier"),
                    ],
                    dimensions=[DbtDimension(name="has_loss_payment", type="categorical", expr="1"),
                    ],
                ),
            ],
            metrics=[
                DbtMetric(
                    name="loss_payment_amount",
                    type="simple",
                    label="Loss Payment Amount",
                    type_params=DbtMetricTypeParams(measure="total_claim_amount"),
                    filter="{{Dimension('claim_amount__has_loss_payment')}} = 1",
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        ca = next(m for m in result.models if m.name == "claim_amount")
        # The filtered metric becomes a (Column with .filter) + (ModelMeasure
        # pointing at it). Find the Column carrying the filter.
        filtered_cols = [c for c in ca.columns if c.filter is not None]
        assert len(filtered_cols) == 1, "Filtered Column not created"
        actual_filter = filtered_cols[0].filter or ""
        assert "loss_payment.has_loss_payment" in actual_filter, (
            f"Filter not qualified: {actual_filter!r}"
        )
        # And the ModelMeasure references that column.
        loss_metric = next(m for m in ca.measures if m.name == "loss_payment_amount")
        assert loss_metric.label == "Loss Payment Amount"
        assert loss_metric.formula == f"{filtered_cols[0].name}:sum"

    def test_filter_dim_on_source_model_stays_bare(self) -> None:
        """Dimension('orders__status') where status exists on orders → bare 'status'."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="orders", type="primary", expr="id")],
                    dimensions=[DbtDimension(name="status", type="categorical"),
                    ],
                    measures=[
DbtMeasure(name="revenue", agg="sum", expr="amount")
                    ,
                    ],
                ),
            ],
            metrics=[
                DbtMetric(
                    name="active_revenue",
                    type="simple",
                    label="Active Revenue",
                    type_params=DbtMetricTypeParams(measure="revenue"),
                    filter="{{Dimension('orders__status')}} = 'active'",
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = next(m for m in result.models if m.name == "orders")
        filtered_cols = [c for c in orders.columns if c.filter is not None]
        assert len(filtered_cols) == 1
        assert filtered_cols[0].filter == "status = 'active'", (
            f"Got: {filtered_cols[0].filter!r}"
        )
        active_rev = next(m for m in orders.measures if m.name == "active_revenue")
        assert active_rev.label == "Active Revenue"
        assert not result.models[0].hidden


class TestJoinTypeFromDbt:
    """dbt entity-based joins should use JoinType.INNER."""

    def test_foreign_entity_join_is_inner(self) -> None:
        """Foreign entity join gets join_type=inner."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[
                    DbtEntity(name="order_id", type="primary"),
                    DbtEntity(name="customer", type="foreign", expr="customer_id"),
                ],
            ),
            DbtSemanticModel(
                name="customers",
                model="customers",
                entities=[DbtEntity(name="customer", type="primary", expr="id")],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = next(m for m in result.models if m.name == "orders")
        cust_join = next(j for j in orders.joins if j.target_model == "customers")
        assert str(cust_join.join_type) == "inner"

    def test_peer_join_is_inner(self) -> None:
        """Peer join (shared primary entity) gets join_type=inner."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="claim",
                model="claim",
                entities=[DbtEntity(name="claim_identifier", type="primary")],
            ),
            DbtSemanticModel(
                name="claim_coverage",
                model="claim_coverage",
                entities=[DbtEntity(name="claim_identifier", type="primary")],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        claim = next(m for m in result.models if m.name == "claim")
        cov_join = next(j for j in claim.joins if j.target_model == "claim_coverage")
        assert str(cov_join.join_type) == "inner"

    def test_inner_join_mirrored(self) -> None:
        """Inner join from A→B is auto-mirrored as B→A."""
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="policy_amount",
                model="policy_amount",
                entities=[
                    DbtEntity(name="policy_amount", type="primary", expr="id"),
                    DbtEntity(name="policy", type="foreign", expr="policy_id"),
                ],
            ),
            DbtSemanticModel(
                name="policy",
                model="policy",
                entities=[DbtEntity(name="policy", type="primary", expr="id")],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        policy = next(m for m in result.models if m.name == "policy")
        # policy should have a reverse inner join back to policy_amount
        reverse = next((j for j in policy.joins if j.target_model == "policy_amount"), None)
        assert reverse is not None, f"Missing reverse join. Policy joins: {[j.target_model for j in policy.joins]}"
        assert str(reverse.join_type) == "inner"
        assert reverse.join_pairs == [["id", "policy_id"]]


class TestColumnNamingS4:
    """S4 specifics: Column-name resolution rules in _convert_measures."""

    def test_non_identifier_expr_uses_first_name_col_suffix(self) -> None:
        """Q-I: a SQL fragment expr (not a bare identifier) gets the first
        dbt measure's name + ``_col`` as the SLayer Column name.
        """
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                measures=[
                    DbtMeasure(name="line_total", agg="sum", expr="amount * quantity"),
                    DbtMeasure(name="line_max", agg="max", expr="amount * quantity"),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]

        # One Column for the SQL fragment, named after the first dbt measure.
        col_names = {c.name for c in orders.columns}
        assert "line_total_col" in col_names
        line_col = next(c for c in orders.columns if c.name == "line_total_col")
        assert line_col.sql == "amount * quantity"
        assert line_col.allowed_aggregations is None
        assert line_col.format is not None
        assert line_col.format.type == NumberFormatType.FLOAT

        # ModelMeasures point at that column with their respective aggs.
        by_name = {m.name: m for m in orders.measures}
        assert by_name["line_total"].formula == "line_total_col:sum"
        assert by_name["line_max"].formula == "line_total_col:max"

    def test_column_name_collision_with_measure_name_suffixed_with_col(self) -> None:
        """Q-2: when the natural Column name (= the bare expr) would collide
        with a ModelMeasure name, suffix the Column with ``_col``.
        """
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                measures=[
                    # expr == name → Column would naturally be named "revenue",
                    # but the ModelMeasure for this same dbt measure is also
                    # named "revenue". Suffix the Column to break the tie.
                    DbtMeasure(name="revenue", agg="sum", expr="revenue"),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]

        col_names = {c.name for c in orders.columns}
        assert "revenue_col" in col_names
        # Bare "revenue" must NOT also be a Column — it's the ModelMeasure name.
        assert "revenue" not in col_names

        rev_meas = next(m for m in orders.measures if m.name == "revenue")
        assert rev_meas.formula == "revenue_col:sum"

    def test_label_and_description_on_model_measure_only(self) -> None:
        """Q-D: label/description live on the ModelMeasure verbatim — no
        ``Default aggregation: …`` tail, and the Column itself has neither.
        """
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                measures=[
                    DbtMeasure(
                        name="revenue",
                        agg="sum",
                        expr="amount",
                        label="Revenue",
                        description="Total revenue",
                    ),
                ],
            ),
        ])
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = result.models[0]

        amount_col = next(c for c in orders.columns if c.name == "amount")
        assert amount_col.label is None
        assert amount_col.description is None

        rev = next(m for m in orders.measures if m.name == "revenue")
        assert rev.label == "Revenue"
        assert rev.description == "Total revenue"


class TestDbtMeasureToModelMeasure:
    """Step 7 (Q-1, Q-5, Q-6): metrics fold into ModelMeasures, not SlayerQuery dicts."""

    def test_ratio_metric_becomes_model_measure(self) -> None:
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    measures=[
                        DbtMeasure(name="total_amount", agg="sum", expr="amount"),
                        DbtMeasure(name="order_count", agg="count", expr="id"),
                    ],
                ),
            ],
            metrics=[
                DbtMetric(
                    name="aov",
                    type="ratio",
                    type_params=DbtMetricTypeParams(
                        numerator=DbtMetricInput(name="total_amount"),
                        denominator=DbtMetricInput(name="order_count"),
                    ),
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = next(m for m in result.models if m.name == "orders")

        aov = next(m for m in orders.measures if m.name == "aov")
        # Q-5: bare ModelMeasure names — formula refs total_amount / order_count
        assert aov.formula == "total_amount / order_count"

    def test_cumulative_metric_becomes_model_measure(self) -> None:
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    measures=[
                        DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    ],
                ),
            ],
            metrics=[
                DbtMetric(
                    name="cumulative_revenue",
                    type="cumulative",
                    type_params=DbtMetricTypeParams(measure="revenue"),
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        orders = next(m for m in result.models if m.name == "orders")
        cum = next(m for m in orders.measures if m.name == "cumulative_revenue")
        assert cum.formula == "cumsum(revenue)"


class TestFindMetricSourceModelRatio:
    """Q-E regression: _find_metric_source_model must walk numerator/denominator."""

    def test_find_metric_source_model_resolves_ratio_numerator_denominator(self) -> None:
        """A ratio metric whose source is reachable only through its
        numerator (or denominator) must still resolve."""
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    measures=[
                        DbtMeasure(name="total_amount", agg="sum", expr="amount"),
                        DbtMeasure(name="order_count", agg="count", expr="id"),
                    ],
                ),
            ],
            metrics=[
                # Ratio metric — only reachable via numerator/denominator names,
                # NOT via type_params.measure or type_params.metrics.
                DbtMetric(
                    name="aov",
                    type="ratio",
                    type_params=DbtMetricTypeParams(
                        numerator=DbtMetricInput(name="total_amount"),
                        denominator=DbtMetricInput(name="order_count"),
                    ),
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        # Source resolution worked — the ModelMeasure ended up on orders.
        orders = next(m for m in result.models if m.name == "orders")
        assert any(m.name == "aov" for m in orders.measures), (
            "Ratio metric source model not resolved via numerator/denominator"
        )


class TestDbtConversionErrorOnDimMeasureCollision:
    """Q-G: a semantic model whose dim and measure share a name must hard-fail."""

    def test_dim_measure_name_collision_raises_dbt_conversion_error(self) -> None:
        project = DbtProject(semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                dimensions=[DbtDimension(name="amount", type="categorical")],
                measures=[DbtMeasure(name="amount", agg="sum", expr="amount")],
            ),
        ])
        with pytest.raises(DbtConversionError) as exc_info:
            DbtToSlayerConverter(project=project, data_source="test").convert()
        msg = str(exc_info.value)
        assert "orders" in msg
        assert "amount" in msg


class TestUnconvertedTransformShadowing:
    """Q-F: a dbt measure or metric named after a SLayer transform is routed
    to ``unconverted_metrics`` rather than crashing."""

    def test_unconverted_metrics_for_transform_shadowing_name(self) -> None:
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="orders",
                    model="orders",
                    entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                    measures=[
                        # Valid measure to anchor the model (so we can test
                        # the metric-side transform shadowing path).
                        DbtMeasure(name="total", agg="sum", expr="amount"),
                        # ``cumsum`` is a SLayer transform — ModelMeasure
                        # construction must reject it and route to unconverted.
                        DbtMeasure(name="cumsum", agg="sum", expr="amount"),
                    ],
                ),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()

        # The good measure converted normally.
        orders = next(m for m in result.models if m.name == "orders")
        assert any(m.name == "total" for m in orders.measures)
        # The shadowing measure was routed to unconverted_metrics.
        assert any(
            u.metric_name == "cumsum"
            for u in result.unconverted_metrics
        )
