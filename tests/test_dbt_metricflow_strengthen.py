"""DEV-1595: strengthen MetricFlow ingestion.

Covers the importer-side behaviors of the plan:
  Part 1  correctness fixes (percentile p=, ratio nullif guard)
  Part 3  represent-exactly (sum_boolean CASE, offset_window->time_shift,
          metric/per-input filter push-down, string-or-list filters)
  Part 4  clean-fail routing (percentile flags, offset_to_grain,
          non_additive_dimension, conversion, windowed/grain_to_date
          cumulative, period_agg!=default, measure-less simple metrics)
  Part 5b info preservation via meta (config.meta, clean-fail raw stash)
  Part 6  structured report (category / severity / suggestion + render_report)

Tests use the public ``DbtToSlayerConverter(...).convert()`` API. Inputs that
exercise not-yet-parsed DSI fields are built with ``model_validate`` dicts so a
missing field surfaces as wrong behavior (clean-fail not produced), not an
ImportError at collection time.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.dbt.converter import ConversionResult, DbtToSlayerConverter
from slayer.dbt.models import (
    DbtDimension,
    DbtEntity,
    DbtMeasure,
    DbtMeasureAggParams,
    DbtMetric,
    DbtMetricInput,
    DbtMetricTypeParams,
    DbtProject,
    DbtSemanticModel,
)


# ───────────────────────── helpers ─────────────────────────


def _convert(project: DbtProject) -> ConversionResult:
    return DbtToSlayerConverter(project=project, data_source="test").convert()


def _model(result: ConversionResult, name: str = "orders"):
    return next(m for m in result.models if m.name == name)


def _measure(result: ConversionResult, name: str, model: str = "orders"):
    return next(m for m in _model(result, model).measures if m.name == name)


def _column_for(result: ConversionResult, formula: str, model: str = "orders"):
    """The Column referenced by the leading ``<col>:<agg>`` in a formula."""
    col_name = formula.split(":", 1)[0].strip()
    return next(c for c in _model(result, model).columns if c.name == col_name)


def _all_report_entries(result: ConversionResult):
    return list(result.unconverted_metrics) + list(result.warnings)


# ───────────────────────── Part 1.1 — percentile p= ─────────────────────────


def test_percentile_emits_p_value() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[
                    DbtMeasure(
                        name="p95_latency",
                        agg="percentile",
                        expr="latency",
                        agg_params=DbtMeasureAggParams(percentile=0.95),
                    ),
                ],
            ),
        ],
    )
    result = _convert(project)
    m = _measure(result, "p95_latency")
    assert m.formula == "latency:percentile(p=0.95)"


def test_percentile_without_value_clean_fails() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[DbtMeasure(name="p_nope", agg="percentile", expr="latency")],
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "p_nope" for m in _model(result).measures)
    entries = _all_report_entries(result)
    assert any("p_nope" in (e.metric_name or e.message) for e in entries)


@pytest.mark.parametrize("flag", ["use_discrete_percentile", "use_approximate_percentile"])
def test_percentile_discrete_or_approx_flag_clean_fails(flag: str) -> None:
    params = DbtMeasureAggParams(percentile=0.9, **{flag: True})
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[DbtMeasure(name="p_flag", agg="percentile", expr="latency", agg_params=params)],
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "p_flag" for m in _model(result).measures)
    assert any("p_flag" in (e.metric_name or e.message) for e in _all_report_entries(result))


# ───────────────────────── Part 1.2 — ratio nullif guard ─────────────────────────


def test_ratio_metric_guards_denominator_with_nullif() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
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
    result = _convert(project)
    aov = _measure(result, "aov")
    assert aov.formula == "total_amount / nullif(order_count, 0)"


# ───────────────────────── Part 3.2 — sum_boolean ─────────────────────────


def test_sum_boolean_wraps_case_when() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[DbtMeasure(name="paid_orders", agg="sum_boolean", expr="is_paid")],
            ),
        ],
    )
    result = _convert(project)
    m = _measure(result, "paid_orders")
    assert m.formula.endswith(":sum")
    col = _column_for(result, m.formula)
    assert col.sql is not None
    norm = col.sql.upper().replace(" ", "")
    assert "CASEWHEN" in norm and "THEN1ELSE0END" in norm
    assert "IS_PAID" in col.sql.upper()
    assert col.type == DataType.INT


# ───────────────────────── Part 3.3 — offset_window -> time_shift ─────────────────────────


def test_offset_window_maps_to_time_shift() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
        ],
        metrics=[
            DbtMetric(name="revenue_metric", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue")),
            DbtMetric(
                name="revenue_growth",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="revenue_metric - revenue_prev",
                    metrics=[
                        DbtMetricInput(name="revenue_metric"),
                        DbtMetricInput(name="revenue_metric", alias="revenue_prev",
                                       offset_window="1 month"),
                    ],
                ),
            ),
        ],
    )
    result = _convert(project)
    growth = _measure(result, "revenue_growth")
    norm = growth.formula.replace(" ", "")
    assert "time_shift(" in growth.formula
    assert "-1" in norm and "month" in norm.lower()


def test_offset_to_grain_clean_fails() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
        ],
        metrics=[
            DbtMetric(name="revenue_metric", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue")),
            DbtMetric(
                name="rev_vs_month_start",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="revenue_metric - revenue_anchor",
                    metrics=[
                        DbtMetricInput(name="revenue_metric"),
                        DbtMetricInput(name="revenue_metric", alias="revenue_anchor",
                                       offset_to_grain="month"),
                    ],
                ),
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "rev_vs_month_start" for m in _model(result).measures)
    assert any("rev_vs_month_start" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


# ───────────────────────── Part 3.4/3.5 — filter push-down ─────────────────────────


def test_metric_level_filter_pushes_into_leaf_column() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
        ],
        metrics=[
            DbtMetric(
                name="us_revenue",
                type="simple",
                type_params=DbtMetricTypeParams(measure="revenue"),
                filter="{{ Dimension('orders__region') }} = 'US'",
            ),
        ],
    )
    result = _convert(project)
    m = _measure(result, "us_revenue")
    col = _column_for(result, m.formula)
    assert col.filter is not None
    assert "region" in col.filter and "US" in col.filter


def test_ratio_per_input_filters_push_down_independently() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[
                    DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    DbtMeasure(name="cost", agg="sum", expr="cost_amount"),
                ],
            ),
        ],
        metrics=[
            DbtMetric(
                name="us_margin_ratio",
                type="ratio",
                type_params=DbtMetricTypeParams(
                    numerator=DbtMetricInput(name="revenue",
                                             filter="{{ Dimension('orders__region') }} = 'US'"),
                    denominator=DbtMetricInput(name="cost",
                                               filter="{{ Dimension('orders__dept') }} = 'sales'"),
                ),
            ),
        ],
    )
    result = _convert(project)
    cols = _model(result).columns
    filters = [c.filter for c in cols if c.filter]
    assert any(f and "region" in f and "US" in f for f in filters)
    assert any(f and "dept" in f and "sales" in f for f in filters)
    # nullif guard preserved around the (now filtered) denominator
    assert "nullif(" in _measure(result, "us_margin_ratio").formula


def test_filter_accepts_list_intersection() -> None:
    """DSI filter is a WhereFilterIntersection: string OR list-of-strings."""
    metric = DbtMetric.model_validate({
        "name": "scoped_revenue",
        "type": "simple",
        "type_params": {"measure": "revenue"},
        "filter": [
            "{{ Dimension('orders__region') }} = 'US'",
            "{{ Dimension('orders__status') }} = 'paid'",
        ],
    })
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
        ],
        metrics=[metric],
    )
    result = _convert(project)
    m = _measure(result, "scoped_revenue")
    col = _column_for(result, m.formula)
    assert col.filter is not None
    assert "region" in col.filter and "status" in col.filter
    assert " AND " in col.filter.upper() or "AND" in col.filter.upper()


# ───────────────────────── Part 4 — clean-fail routing ─────────────────────────


def test_non_additive_dimension_clean_fails() -> None:
    measure = DbtMeasure.model_validate({
        "name": "account_balance",
        "agg": "sum",
        "expr": "balance",
        "non_additive_dimension": {"name": "ds", "window_choice": "max",
                                   "window_groupings": ["account_id"]},
    })
    project = DbtProject(
        semantic_models=[DbtSemanticModel(name="orders", model="orders", measures=[measure])],
    )
    result = _convert(project)
    entries = _all_report_entries(result)
    assert any("account_balance" in (e.metric_name or e.message) for e in entries)
    assert any("non_additive" in (getattr(e, "category", "") or "").lower()
               or "semi-additive" in e.message.lower()
               or "non_additive" in e.message.lower() for e in entries)


def test_conversion_metric_clean_fails_with_category() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="visits", agg="sum", expr="v")]),
        ],
        metrics=[DbtMetric.model_validate({
            "name": "visit_to_purchase",
            "type": "conversion",
            "type_params": {"conversion_type_params": {
                "base_measure": {"name": "visits"},
                "conversion_measure": {"name": "purchases"},
                "entity": "user",
                "calculation": "conversion_rate",
            }},
        })],
    )
    result = _convert(project)
    assert all(m.name != "visit_to_purchase" for mdl in result.models for m in mdl.measures)
    assert any("visit_to_purchase" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


def test_windowed_cumulative_clean_fails() -> None:
    metric = DbtMetric.model_validate({
        "name": "rolling_7d_revenue",
        "type": "cumulative",
        "type_params": {"measure": "revenue", "window": "7 days"},
    })
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[metric],
    )
    result = _convert(project)
    assert all(m.name != "rolling_7d_revenue" for m in _model(result).measures)
    assert any("rolling_7d_revenue" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


def test_grain_to_date_cumulative_clean_fails() -> None:
    metric = DbtMetric.model_validate({
        "name": "mtd_revenue",
        "type": "cumulative",
        "type_params": {"measure": "revenue", "grain_to_date": "month"},
    })
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[metric],
    )
    result = _convert(project)
    assert all(m.name != "mtd_revenue" for m in _model(result).measures)
    assert any("mtd_revenue" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


def test_unbounded_cumulative_still_cumsum() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[DbtMetric(name="running_revenue", type="cumulative",
                           type_params=DbtMetricTypeParams(measure="revenue"))],
    )
    result = _convert(project)
    assert _measure(result, "running_revenue").formula == "cumsum(revenue)"


def test_cumulative_non_default_period_agg_clean_fails() -> None:
    metric = DbtMetric.model_validate({
        "name": "running_last",
        "type": "cumulative",
        "type_params": {"cumulative_type_params": {"measure": "revenue", "period_agg": "last"}},
    })
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[metric],
    )
    result = _convert(project)
    assert all(m.name != "running_last" for m in _model(result).measures)
    # Must fail specifically because period_agg != FIRST — not merely because
    # the (nested) measure ref was not resolved.
    assert any(
        "running_last" in (e.metric_name or "")
        and "period_agg" in (e.message + (getattr(e, "category", "") or "")).lower()
        for e in _all_report_entries(result)
    )


# ───────────────────────── Part 5b — info preservation ─────────────────────────


def test_config_meta_preserved_on_model() -> None:
    sm = DbtSemanticModel.model_validate({
        "name": "orders",
        "model": "orders",
        "config": {"meta": {"team": "growth", "tier": 1}},
        "measures": [{"name": "revenue", "agg": "sum", "expr": "amount"}],
    })
    result = _convert(DbtProject(semantic_models=[sm]))
    model = _model(result)
    assert model.meta is not None
    assert model.meta.get("team") == "growth"


def test_clean_fail_stashes_raw_construct_in_meta() -> None:
    measure = DbtMeasure.model_validate({
        "name": "account_balance",
        "agg": "sum",
        "expr": "balance",
        "non_additive_dimension": {"name": "ds", "window_choice": "max"},
    })
    result = _convert(DbtProject(
        semantic_models=[DbtSemanticModel(name="orders", model="orders", measures=[measure])],
    ))
    model = _model(result)
    blob = model.meta or {}
    # The dropped semantics are retained in the model's meta, not thrown away.
    assert any(
        "non_additive" in str(k).lower() or "non_additive" in str(v).lower()
        for k, v in blob.items()
    )


# ───────────────────────── Part 6 — structured report ─────────────────────────


def test_conversion_warning_has_category_and_severity() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="visits", agg="sum", expr="v")]),
        ],
        metrics=[DbtMetric(name="conv", type="conversion",
                           type_params=DbtMetricTypeParams())],
    )
    result = _convert(project)
    entry = next(e for e in _all_report_entries(result)
                 if (e.metric_name or "") == "conv")
    assert getattr(entry, "category", None)
    assert getattr(entry, "severity", None) in {"unconverted", "dropped", "info"}
    assert hasattr(entry, "suggestion")  # present (may be None for some categories)


def test_render_report_groups_by_category() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="visits", agg="sum", expr="v")]),
        ],
        metrics=[
            DbtMetric(name="conv", type="conversion", type_params=DbtMetricTypeParams()),
            DbtMetric(name="weird", type="frobnicate", type_params=DbtMetricTypeParams()),
        ],
    )
    result = _convert(project)
    report = result.render_report()
    assert isinstance(report, str)
    assert "conv" in report and "weird" in report
    # Grouped by category: each failing entry's category appears as a heading.
    cats = {getattr(e, "category", None) for e in _all_report_entries(result)}
    cats.discard(None)
    assert cats, "entries must carry categories"
    for cat in cats:
        assert cat in report


# ───────────────── Part 3.3 — grain normalization / custom grain ─────────────────


def test_offset_window_plural_granularity_normalized() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[
            DbtMetric(name="revenue_metric", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue")),
            DbtMetric(
                name="rev_2w",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="revenue_metric - revenue_prev",
                    metrics=[
                        DbtMetricInput(name="revenue_metric"),
                        DbtMetricInput(name="revenue_metric", alias="revenue_prev",
                                       offset_window="2 weeks"),
                    ],
                ),
            ),
        ],
    )
    result = _convert(project)
    f = _measure(result, "rev_2w").formula.replace(" ", "").lower()
    assert "time_shift(" in _measure(result, "rev_2w").formula
    assert "-2" in f and "week" in f and "weeks" not in f


def test_offset_window_custom_granularity_clean_fails() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[
            DbtMetric(name="revenue_metric", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue")),
            DbtMetric(
                name="rev_fortnight",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="revenue_metric - revenue_prev",
                    metrics=[
                        DbtMetricInput(name="revenue_metric"),
                        DbtMetricInput(name="revenue_metric", alias="revenue_prev",
                                       offset_window="1 fortnight"),
                    ],
                ),
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "rev_fortnight" for m in _model(result).measures)
    assert any("rev_fortnight" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


# ───────────────── Part 3.4 — filtered-column dedup ─────────────────


def test_same_measure_different_filters_make_distinct_columns() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[
            DbtMetric(name="us_rev", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue"),
                      filter="{{ Dimension('orders__region') }} = 'US'"),
            DbtMetric(name="eu_rev", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue"),
                      filter="{{ Dimension('orders__region') }} = 'EU'"),
        ],
    )
    result = _convert(project)
    us_col = _column_for(result, _measure(result, "us_rev").formula)
    eu_col = _column_for(result, _measure(result, "eu_rev").formula)
    assert us_col.name != eu_col.name
    assert "US" in (us_col.filter or "") and "EU" in (eu_col.filter or "")


def test_same_measure_same_filter_reuses_one_column() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[
            DbtMetric(name="us_rev_a", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue"),
                      filter="{{ Dimension('orders__region') }} = 'US'"),
            DbtMetric(name="us_rev_b", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue"),
                      filter="{{ Dimension('orders__region') }} = 'US'"),
        ],
    )
    result = _convert(project)
    a = _column_for(result, _measure(result, "us_rev_a").formula)
    b = _column_for(result, _measure(result, "us_rev_b").formula)
    assert a.name == b.name


# ───────────────── Part 3.4 — cross-model filter reachability ─────────────────


def _orders_customers_project(metric: DbtMetric) -> DbtProject:
    return DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                entities=[
                    DbtEntity(name="order_id", type="primary", expr="id"),
                    DbtEntity(name="customer", type="foreign", expr="customer_id"),
                ],
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
            DbtSemanticModel(
                name="customers", model="customers",
                entities=[DbtEntity(name="customer", type="primary", expr="id")],
                dimensions=[DbtDimension(name="region", type="categorical")],
            ),
        ],
        metrics=[metric],
    )


def test_cross_model_filter_pushes_down_when_join_reachable() -> None:
    metric = DbtMetric(
        name="us_revenue",
        type="simple",
        type_params=DbtMetricTypeParams(measure="revenue"),
        filter="{{ Dimension('customer__region') }} = 'US'",
    )
    result = _convert(_orders_customers_project(metric))
    m = _measure(result, "us_revenue", model="orders")
    col = _column_for(result, m.formula, model="orders")
    assert col.filter is not None
    assert "region" in col.filter and "US" in col.filter


def test_cross_model_filter_unreachable_clean_fails() -> None:
    # 'warehouse__zone' references an entity that orders has no join to.
    metric = DbtMetric(
        name="zone_revenue",
        type="simple",
        type_params=DbtMetricTypeParams(measure="revenue"),
        filter="{{ Dimension('warehouse__zone') }} = 'A'",
    )
    result = _convert(_orders_customers_project(metric))
    assert all(m.name != "zone_revenue" for m in _model(result, "orders").measures)
    assert any("zone_revenue" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


def test_cross_model_filter_multi_hop_clean_fails() -> None:
    """A filter on a model two hops away (orders → customers → regions) is
    clean-failed: the dbt filter converter only emits a one-hop
    ``regions.zone`` path, which would not resolve from orders (it needs the
    full ``customers__regions.zone`` join path). Full multi-hop support is
    tracked separately (DEV-1445)."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                entities=[
                    DbtEntity(name="order_id", type="primary", expr="id"),
                    DbtEntity(name="customer", type="foreign", expr="customer_id"),
                ],
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
            DbtSemanticModel(
                name="customers", model="customers",
                entities=[
                    DbtEntity(name="customer", type="primary", expr="id"),
                    DbtEntity(name="region", type="foreign", expr="region_id"),
                ],
            ),
            DbtSemanticModel(
                name="regions", model="regions",
                entities=[DbtEntity(name="region", type="primary", expr="id")],
                dimensions=[DbtDimension(name="zone", type="categorical")],
            ),
        ],
        metrics=[
            DbtMetric(
                name="zone_a_revenue",
                type="simple",
                type_params=DbtMetricTypeParams(measure="revenue"),
                filter="{{ Dimension('region__zone') }} = 'A'",
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "zone_a_revenue" for m in _model(result, "orders").measures)
    assert any("zone_a_revenue" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


def test_cross_model_filter_foreign_entity_without_owner_clean_fails() -> None:
    """A filter on a foreign entity that no model owns as primary clean-fails:
    convert_dbt_filter would otherwise fall back to a bare, invalid column."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                entities=[
                    DbtEntity(name="order_id", type="primary", expr="id"),
                    # 'vendor' is foreign but no semantic model declares it primary.
                    DbtEntity(name="vendor", type="foreign", expr="vendor_id"),
                ],
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
        ],
        metrics=[
            DbtMetric(
                name="vendor_x_revenue",
                type="simple",
                type_params=DbtMetricTypeParams(measure="revenue"),
                filter="{{ Dimension('vendor__tier') }} = 'X'",
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "vendor_x_revenue" for m in _model(result).measures)
    assert any("vendor_x_revenue" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


def test_input_filter_intersects_referenced_metric_filter() -> None:
    """When a derived input adds a filter on top of an already-filtered simple
    metric, BOTH filters must apply to the leaf — the referenced metric's filter
    must not be silently dropped (which would widen results)."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
        ],
        metrics=[
            DbtMetric(
                name="us_revenue", type="simple",
                type_params=DbtMetricTypeParams(measure="revenue"),
                filter="{{ Dimension('orders__region') }} = 'US'",
            ),
            DbtMetric(
                name="us_web_revenue",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="us_revenue",
                    metrics=[DbtMetricInput(
                        name="us_revenue",
                        filter="{{ Dimension('orders__channel') }} = 'web'",
                    )],
                ),
            ),
        ],
    )
    result = _convert(project)
    m = _measure(result, "us_web_revenue")
    col = _column_for(result, m.formula)
    assert col.filter is not None
    assert "region" in col.filter and "US" in col.filter
    assert "channel" in col.filter and "web" in col.filter


def test_cross_model_filter_ambiguous_multi_owner_entity_clean_fails() -> None:
    """A foreign entity owned as primary by more than one model can't be
    unambiguously qualified to a single join, so it clean-fails instead of
    being lowered to a possibly-wrong model."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                entities=[
                    DbtEntity(name="order_id", type="primary", expr="id"),
                    DbtEntity(name="party", type="foreign", expr="party_id"),
                ],
                measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")],
            ),
            # Two models both declare 'party' as their primary entity.
            DbtSemanticModel(
                name="agreement_party", model="agreement_party",
                entities=[DbtEntity(name="party", type="primary", expr="id")],
                dimensions=[DbtDimension(name="tier", type="categorical")],
            ),
            DbtSemanticModel(
                name="billing_party", model="billing_party",
                entities=[DbtEntity(name="party", type="primary", expr="id")],
                dimensions=[DbtDimension(name="tier", type="categorical")],
            ),
        ],
        metrics=[
            DbtMetric(
                name="tier_x_revenue",
                type="simple",
                type_params=DbtMetricTypeParams(measure="revenue"),
                filter="{{ Dimension('party__tier') }} = 'X'",
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "tier_x_revenue" for m in _model(result).measures)
    assert any("tier_x_revenue" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


def test_derived_input_referencing_unsupported_simple_clean_fails() -> None:
    """A derived metric whose input is an unsupported (non-materialized) simple
    metric must clean-fail, not emit a formula referencing a missing measure."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[
            # Unsupported: time-spine gap fill → not materialized.
            DbtMetric.model_validate({
                "name": "gap_filled_rev",
                "type": "simple",
                "type_params": {"measure": {"name": "revenue", "fill_nulls_with": 0}},
            }),
            DbtMetric(
                name="rev_minus_gap",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="gap_filled_rev - 1",
                    metrics=[DbtMetricInput(name="gap_filled_rev")],
                ),
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "rev_minus_gap" for m in _model(result).measures)
    assert any("rev_minus_gap" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


# ───────────────── Part 4 — measure-less / timespine clean-fails ─────────────────


def test_measure_less_simple_metric_clean_fails() -> None:
    metric = DbtMetric.model_validate({
        "name": "agg_only",
        "type": "simple",
        "type_params": {"metric_aggregation_params": {
            "semantic_model": "orders", "agg": "sum"}},
    })
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[metric],
    )
    result = _convert(project)
    assert all(m.name != "agg_only" for m in _model(result).measures)
    assert any("agg_only" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


@pytest.mark.parametrize("field,value", [("join_to_timespine", True), ("fill_nulls_with", 0)])
def test_timespine_gap_fill_clean_fails(field: str, value) -> None:
    metric = DbtMetric.model_validate({
        "name": "gap_filled_rev",
        "type": "simple",
        "type_params": {"measure": {"name": "revenue", field: value}},
    })
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[metric],
    )
    result = _convert(project)
    assert all(m.name != "gap_filled_rev" for m in _model(result).measures)
    assert any("gap_filled_rev" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


# ───────────────── Part 2 — count_distinct_approx importer mapping ─────────────────


def test_importer_maps_count_distinct_approx_measure() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="uniq_cust", agg="count_distinct_approx",
                                                  expr="customer_id")]),
        ],
    )
    result = _convert(project)
    assert _measure(result, "uniq_cust").formula == "customer_id:count_distinct_approx"


# ───────────────── Part 3.3 — offset on multi-aggregate input clean-fails ─────────────────


def test_offset_window_on_ratio_input_clean_fails() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                measures=[
                    DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    DbtMeasure(name="orders_count", agg="count", expr="id"),
                ],
            ),
        ],
        metrics=[
            DbtMetric(name="aov", type="ratio",
                      type_params=DbtMetricTypeParams(
                          numerator=DbtMetricInput(name="revenue"),
                          denominator=DbtMetricInput(name="orders_count"))),
            DbtMetric(
                name="aov_growth",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="aov - aov_prev",
                    metrics=[
                        DbtMetricInput(name="aov"),
                        # offset on a ratio (multi-aggregate) input is not exactly expressible
                        DbtMetricInput(name="aov", alias="aov_prev", offset_window="1 month"),
                    ],
                ),
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "aov_growth" for m in _model(result).measures)
    assert any("aov_growth" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


# ───────────────── Part 3.4/3.5 — combined metric + per-input filter ─────────────────


def test_metric_filter_and_input_filter_intersect() -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                measures=[
                    DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    DbtMeasure(name="cost", agg="sum", expr="cost_amount"),
                ],
            ),
        ],
        metrics=[
            DbtMetric(
                name="scoped_margin",
                type="ratio",
                type_params=DbtMetricTypeParams(
                    numerator=DbtMetricInput(name="revenue",
                                             filter="{{ Dimension('orders__channel') }} = 'web'"),
                    denominator=DbtMetricInput(name="cost"),
                ),
                filter="{{ Dimension('orders__region') }} = 'US'",
            ),
        ],
    )
    result = _convert(project)
    cols = [c for c in _model(result).columns if c.filter]
    # numerator leaf: BOTH the metric-level (region) and input-level (channel) filters
    num = [c for c in cols if "channel" in (c.filter or "")]
    assert num and "region" in (num[0].filter or "")
    # denominator leaf: only the metric-level filter
    den = [c for c in cols if "region" in (c.filter or "") and "channel" not in (c.filter or "")]
    assert den


# ───────────────── Part 5b — config.meta breadth + label ─────────────────


def test_config_meta_preserved_on_dimension_and_measure() -> None:
    sm = DbtSemanticModel.model_validate({
        "name": "orders",
        "model": "orders",
        "dimensions": [{"name": "region", "type": "categorical",
                        "config": {"meta": {"pii": False}}}],
        "measures": [{"name": "revenue", "agg": "sum", "expr": "amount",
                      "config": {"meta": {"owner": "finance"}}}],
    })
    result = _convert(DbtProject(semantic_models=[sm]))
    model = _model(result)
    region = next(c for c in model.columns if c.name == "region")
    assert (region.meta or {}).get("pii") is False
    revenue = next(m for m in model.measures if m.name == "revenue")
    assert (revenue.meta or {}).get("owner") == "finance"


def test_semantic_model_label_preserved_in_meta() -> None:
    sm = DbtSemanticModel(name="orders", model="orders", label="Customer Orders",
                          measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")])
    result = _convert(DbtProject(semantic_models=[sm]))
    assert (_model(result).meta or {}).get("label") == "Customer Orders"


# ───────────────── Part 4 — percentile dialect caveat (MySQL / T-SQL) ─────────────────


@pytest.mark.parametrize("dialect", ["mysql", "tsql"])
def test_percentile_on_unsupported_dialect_emits_caveat(dialect: str) -> None:
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                measures=[DbtMeasure(name="p95", agg="percentile", expr="latency",
                                     agg_params=DbtMeasureAggParams(percentile=0.95))],
            ),
        ],
    )
    result = DbtToSlayerConverter(
        project=project, data_source="test", target_dialect=dialect
    ).convert()
    # The measure still imports (formula valid), but a report caveat warns it
    # won't execute on this dialect.
    assert _measure(result, "p95").formula == "latency:percentile(p=0.95)"
    assert any("percentile" in e.message.lower() or "percentile" in (getattr(e, "category", "") or "").lower()
               for e in _all_report_entries(result))


# ───────── DEV-1595 review follow-ups: filtered special aggs / derived input filters ─────────


def test_filtered_percentile_metric_preserves_p() -> None:
    """A filtered simple metric over a percentile measure must keep its p=."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                measures=[DbtMeasure(name="latency_p95", agg="percentile", expr="latency",
                                     agg_params=DbtMeasureAggParams(percentile=0.95))],
            ),
        ],
        metrics=[
            DbtMetric(name="us_latency_p95", type="simple",
                      type_params=DbtMetricTypeParams(measure="latency_p95"),
                      filter="{{ Dimension('orders__region') }} = 'US'"),
        ],
    )
    result = _convert(project)
    m = _measure(result, "us_latency_p95")
    assert m.formula.endswith(":percentile(p=0.95)")
    col = _column_for(result, m.formula)
    assert col.filter is not None and "region" in col.filter


def test_filtered_sum_boolean_metric_builds_case_int_column() -> None:
    """A filtered simple metric over a sum_boolean measure must keep the
    CASE-WHEN INT form (not collapse to SUM of a raw boolean)."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                measures=[DbtMeasure(name="paid_orders", agg="sum_boolean", expr="is_paid")],
            ),
        ],
        metrics=[
            DbtMetric(name="us_paid_orders", type="simple",
                      type_params=DbtMetricTypeParams(measure="paid_orders"),
                      filter="{{ Dimension('orders__region') }} = 'US'"),
        ],
    )
    result = _convert(project)
    m = _measure(result, "us_paid_orders")
    assert m.formula.endswith(":sum")
    col = _column_for(result, m.formula)
    assert col.type == DataType.INT
    norm = (col.sql or "").upper().replace(" ", "")
    assert "CASEWHEN" in norm and "THEN1ELSE0END" in norm
    assert col.filter is not None and "region" in col.filter


def test_derived_input_filter_pushes_down() -> None:
    """A per-input filter on a derived metric's single-aggregate input pushes
    into that input's leaf column (DEV-1595 Part 3.5)."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                measures=[
                    DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    DbtMeasure(name="cost", agg="sum", expr="cost_amount"),
                ],
            ),
        ],
        metrics=[
            DbtMetric(name="rev_metric", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue")),
            DbtMetric(name="cost_metric", type="simple",
                      type_params=DbtMetricTypeParams(measure="cost")),
            DbtMetric(
                name="us_rev_minus_cost",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="rev_metric - cost_metric",
                    metrics=[
                        DbtMetricInput(name="rev_metric",
                                       filter="{{ Dimension('orders__region') }} = 'US'"),
                        DbtMetricInput(name="cost_metric"),
                    ],
                ),
            ),
        ],
    )
    result = _convert(project)
    cols = [c for c in _model(result).columns if c.filter]
    assert any(c.filter and "region" in c.filter and "US" in c.filter for c in cols)


def test_derived_input_filter_on_multi_aggregate_clean_fails() -> None:
    """A per-input filter on a ratio (multi-aggregate) derived input clean-fails."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                measures=[
                    DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    DbtMeasure(name="orders_count", agg="count", expr="id"),
                ],
            ),
        ],
        metrics=[
            DbtMetric(name="aov", type="ratio",
                      type_params=DbtMetricTypeParams(
                          numerator=DbtMetricInput(name="revenue"),
                          denominator=DbtMetricInput(name="orders_count"))),
            DbtMetric(
                name="us_aov_plus_one",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="aov + 1",
                    metrics=[DbtMetricInput(name="aov",
                                            filter="{{ Dimension('orders__region') }} = 'US'")],
                ),
            ),
        ],
    )
    result = _convert(project)
    assert all(m.name != "us_aov_plus_one" for m in _model(result).measures)
    assert any("us_aov_plus_one" in (e.metric_name or e.message)
               for e in _all_report_entries(result))


def test_offset_window_object_form_parses() -> None:
    """offset_window given as the DSI object {count, granularity} is accepted
    and lowered to time_shift (not rejected at parse time)."""
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(name="orders", model="orders",
                             measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")]),
        ],
        metrics=[
            DbtMetric(name="revenue_metric", type="simple",
                      type_params=DbtMetricTypeParams(measure="revenue")),
            DbtMetric(
                name="rev_obj_offset",
                type="derived",
                type_params=DbtMetricTypeParams(
                    expr="revenue_metric - revenue_prev",
                    metrics=[
                        DbtMetricInput(name="revenue_metric"),
                        DbtMetricInput.model_validate({
                            "name": "revenue_metric", "alias": "revenue_prev",
                            "offset_window": {"count": 1, "granularity": "month"},
                        }),
                    ],
                ),
            ),
        ],
    )
    result = _convert(project)
    f = _measure(result, "rev_obj_offset").formula.replace(" ", "").lower()
    assert "time_shift(" in f and "-1" in f and "month" in f
