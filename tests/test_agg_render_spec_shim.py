"""DEV-1452 Stage A — ``_agg_render_spec_from_enriched`` shim.

The legacy ``SQLGenerator.generate(enriched=...)`` path stays alive through
Stages B and C (until the deletion in Stage D), but Stage A refactors every
dialect helper to consume ``AggRenderSpec`` instead of ``EnrichedMeasure``.
A trivial field-mapping shim — ``_agg_render_spec_from_enriched(em)`` —
adapts the legacy path's measures into specs so the refactored helpers emit
byte-identical SQL with no fork in the dialect-emission codebase.

These tests fail against current code because ``_agg_render_spec_from_enriched``
(and ``AggRenderSpec``) do not exist yet. They will pass once the shim is in
place. Byte-identical SQL parity is verified end-to-end by the existing
``tests/test_sql_generator.py`` fixtures, which exercise the legacy path
through the shim; here we pin only the shim's per-field translation.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, AggregationParam
from slayer.engine.enriched import EnrichedMeasure

# These imports drive the failing-test contract — neither exists yet.
from slayer.sql.generator import (  # type: ignore[attr-defined]
    AggRenderSpec,
    _agg_render_spec_from_enriched,
)


class TestShimFieldMapping:
    """Each field on ``EnrichedMeasure`` that the dialect helpers read must
    surface verbatim on the resulting ``AggRenderSpec``.
    """

    def test_basic_sum(self):
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="sum",
            alias="orders.amount_sum",
            model_name="orders",
            type=DataType.DOUBLE,
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert isinstance(spec, AggRenderSpec)
        assert spec.sql == "amount"
        assert spec.name == "amount"
        assert spec.model_name == "orders"
        assert spec.aggregation == "sum"
        assert spec.alias == "orders.amount_sum"
        assert spec.type is DataType.DOUBLE
        assert spec.column_type is DataType.DOUBLE
        assert spec.filter_sql is None
        assert spec.time_column is None
        assert spec.aggregation_def is None
        assert spec.agg_kwargs == {}

    def test_star_count(self):
        em = EnrichedMeasure(
            name="",
            sql=None,
            aggregation="count",
            alias="orders._count",
            model_name="orders",
            type=DataType.INT,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.sql is None
        assert spec.name == ""
        assert spec.aggregation == "count"
        assert spec.type is DataType.INT

    def test_filter_sql_propagated(self):
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="sum",
            alias="orders.paid_amount_sum",
            model_name="orders",
            filter_sql="orders.status = 'paid'",
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.filter_sql == "orders.status = 'paid'"

    def test_time_column_propagated(self):
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="first",
            alias="orders.amount_first",
            model_name="orders",
            time_column="orders.created_at",
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.time_column == "orders.created_at"

    def test_filtered_first_combines_time_and_filter(self):
        # Codex finding #3: filtered first/last is its own legacy code
        # path (filtered ranked-column branch in ``_build_agg``). The shim
        # must propagate BOTH ``time_column`` AND ``filter_sql`` so the
        # refactored helper fires the filtered branch.
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="first",
            alias="orders.paid_amount_first",
            model_name="orders",
            time_column="orders.created_at",
            filter_sql="orders.status = 'paid'",
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.aggregation == "first"
        assert spec.time_column == "orders.created_at"
        assert spec.filter_sql == "orders.status = 'paid'"

    def test_filtered_last_combines_time_and_filter(self):
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="last",
            alias="orders.paid_amount_last",
            model_name="orders",
            time_column="orders.created_at",
            filter_sql="orders.status = 'paid'",
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.aggregation == "last"
        assert spec.time_column == "orders.created_at"
        assert spec.filter_sql == "orders.status = 'paid'"

    def test_type_and_column_type_independent(self):
        # Codex finding #4: ``EM.type`` (outer agg result) and
        # ``EM.column_type`` (inner pre-agg CAST) are distinct fields. The
        # shim must propagate them independently — a test that uses the
        # same DataType for both would not detect a cross-wired assignment.
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="count",
            alias="orders.amount_count",
            model_name="orders",
            type=DataType.INT,          # count returns integer
            column_type=DataType.DOUBLE,  # the source column is double
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.type is DataType.INT
        assert spec.column_type is DataType.DOUBLE

    def test_percentile_agg_kwargs(self):
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="percentile",
            alias="orders.amount_percentile",
            model_name="orders",
            agg_kwargs={"p": "0.5"},
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.agg_kwargs == {"p": "0.5"}

    def test_custom_aggregation_def(self):
        agg_def = Aggregation(
            name="rolling_avg",
            formula="AVG({value}) OVER (ORDER BY {time} ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)",
            params=[
                AggregationParam(name="time", sql="created_at"),
                AggregationParam(name="window", sql="6"),
            ],
        )
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="rolling_avg",
            alias="orders.amount_rolling_avg",
            model_name="orders",
            aggregation_def=agg_def,
            agg_kwargs={"window": "6"},
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.aggregation_def is agg_def
        assert spec.agg_kwargs == {"window": "6"}

    def test_stat_agg_with_other_kwarg(self):
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="corr",
            alias="orders.amount_corr",
            model_name="orders",
            agg_kwargs={"other": "quantity"},
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.aggregation == "corr"
        assert spec.agg_kwargs == {"other": "quantity"}

    def test_non_aggregation_passthrough(self):
        # ``em.aggregation == ""`` is the bare-column non-aggregation
        # branch in legacy ``_build_agg`` (generator.py:2235). The shim
        # must round-trip the empty string verbatim so the refactored
        # helper hits the same branch.
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="",
            alias="orders.amount",
            model_name="orders",
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.aggregation == ""
        assert spec.sql == "amount"

    def test_sql_none_bare_column(self):
        # When ``sql is None`` and ``aggregation != "count"``, legacy
        # ``_build_agg`` emits ``exp.Column(name, table=model_name)``.
        # The shim must preserve ``sql=None`` so the same branch fires.
        em = EnrichedMeasure(
            name="amount",
            sql=None,
            aggregation="sum",
            alias="orders.amount_sum",
            model_name="orders",
            column_type=DataType.DOUBLE,
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.sql is None
        assert spec.aggregation == "sum"
        assert spec.name == "amount"

    def test_count_with_filter_sql(self):
        # COUNT(*) with a row-level filter — legacy renders as
        # ``COUNT(CASE WHEN filter THEN 1 END)``. Both ``sql=None`` and
        # ``filter_sql`` must propagate.
        em = EnrichedMeasure(
            name="",
            sql=None,
            aggregation="count",
            alias="orders._count_paid",
            model_name="orders",
            filter_sql="orders.status = 'paid'",
        )
        spec = _agg_render_spec_from_enriched(em)
        assert spec.sql is None
        assert spec.aggregation == "count"
        assert spec.filter_sql == "orders.status = 'paid'"


class TestShimDoesNotReadDropped:
    """The shim must not consume fields the AggRenderSpec deliberately
    drops (``agg_args`` / ``source_measure_name`` / ``distinct`` / the
    DEV-1444 ``user_declared``). Passing them through must not break the
    shim, and they must not appear on the resulting spec.
    """

    def test_unused_fields_ignored(self):
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="sum",
            alias="orders.amount_sum",
            model_name="orders",
            user_declared=True,
            source_measure_name="amount_sum_renamed",
            window="7 days",
            window_time_alias="orders.created_at",
            label="Total amount",
            filter_columns=["orders.status"],
        )
        spec = _agg_render_spec_from_enriched(em)
        # The 11 carried fields are correct; nothing about user_declared /
        # source_measure_name / window / label / filter_columns leaks into
        # the spec (AggRenderSpec does not expose those attributes).
        assert not hasattr(spec, "user_declared")
        assert not hasattr(spec, "source_measure_name")
        assert not hasattr(spec, "window")
        assert not hasattr(spec, "label")
        assert not hasattr(spec, "filter_columns")
        # The 11 fields that DO carry are intact.
        assert spec.aggregation == "sum"
        assert spec.alias == "orders.amount_sum"


class TestShimReturnsFrozen:
    """The shim returns the frozen AggRenderSpec — callers cannot mutate."""

    def test_returned_spec_is_frozen(self):
        em = EnrichedMeasure(
            name="amount",
            sql="amount",
            aggregation="sum",
            alias="orders.amount_sum",
            model_name="orders",
        )
        spec = _agg_render_spec_from_enriched(em)
        with pytest.raises((TypeError, ValueError)):
            spec.aggregation = "avg"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# SQL parity (Codex test-review finding #1 / plan's failing-test list)
# ---------------------------------------------------------------------------


# Expected SQL captured from the LEGACY ``_build_agg(EnrichedMeasure)`` on
# the pre-refactor codebase (postgres dialect). After Stage A the refactored
# ``_build_agg(AggRenderSpec)`` invoked with ``_agg_render_spec_from_enriched(em)``
# MUST emit byte-identical SQL — that's the entire point of the shim.
#
# When sqlglot is upgraded and changes any of these renderings, both the
# refactored helper and the expected strings move together; the parity
# contract is what we pin here, not a particular sqlglot version.
_LEGACY_PARITY: dict = {
    "basic_sum": (
        EnrichedMeasure(
            name="amount", sql="amount", aggregation="sum",
            alias="orders.amount_sum", model_name="orders",
            type=DataType.DOUBLE, column_type=DataType.DOUBLE,
        ),
        "SUM(orders.amount)",
    ),
    "count_star": (
        EnrichedMeasure(
            name="", sql=None, aggregation="count",
            alias="orders._count", model_name="orders", type=DataType.INT,
        ),
        "COUNT(*)",
    ),
    "filtered_sum": (
        EnrichedMeasure(
            name="amount", sql="amount", aggregation="sum",
            alias="orders.paid_amount_sum", model_name="orders",
            filter_sql="orders.status = 'paid'",
            type=DataType.DOUBLE, column_type=DataType.DOUBLE,
        ),
        "SUM(CASE WHEN orders.status = 'paid' THEN orders.amount END)",
    ),
    "filtered_count_star": (
        EnrichedMeasure(
            name="", sql=None, aggregation="count",
            alias="orders._count_paid", model_name="orders",
            filter_sql="orders.status = 'paid'", type=DataType.INT,
        ),
        "COUNT(CASE WHEN orders.status = 'paid' THEN 1 END)",
    ),
    "percentile": (
        EnrichedMeasure(
            name="amount", sql="amount", aggregation="percentile",
            alias="orders.amount_percentile", model_name="orders",
            agg_kwargs={"p": "0.5"},
            type=DataType.DOUBLE, column_type=DataType.DOUBLE,
        ),
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders.amount)",
    ),
    "stat_corr_with_other": (
        EnrichedMeasure(
            name="amount", sql="amount", aggregation="corr",
            alias="orders.amount_corr", model_name="orders",
            agg_kwargs={"other": "quantity"},
            type=DataType.DOUBLE, column_type=DataType.DOUBLE,
        ),
        "CORR(orders.amount, orders.quantity)",
    ),
    "non_aggregation_bare_column": (
        EnrichedMeasure(
            name="amount", sql="amount", aggregation="",
            alias="orders.amount", model_name="orders",
            column_type=DataType.DOUBLE,
        ),
        "orders.amount",
    ),
    "sql_none_bare_column_sum": (
        EnrichedMeasure(
            name="amount", sql=None, aggregation="sum",
            alias="orders.amount_sum", model_name="orders",
            column_type=DataType.DOUBLE,
        ),
        "SUM(orders.amount)",
    ),
    "median": (
        EnrichedMeasure(
            name="amount", sql="amount", aggregation="median",
            alias="orders.amount_median", model_name="orders",
            type=DataType.DOUBLE, column_type=DataType.DOUBLE,
        ),
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders.amount)",
    ),
}


@pytest.mark.parametrize("case_id", list(_LEGACY_PARITY.keys()))
def test_shim_sql_parity_byte_identical(case_id: str):
    """Every legacy-emitted SQL string must survive the
    ``EM → shim → AggRenderSpec → refactored _build_agg → .sql('postgres')``
    pipeline unchanged.
    """
    # Imported here to keep collection failing-mode tied to the AggRenderSpec
    # import in the file header rather than masking it.
    from slayer.sql.generator import SQLGenerator

    em, expected_sql = _LEGACY_PARITY[case_id]
    spec = _agg_render_spec_from_enriched(em)
    gen = SQLGenerator(dialect="postgres")
    # Post-refactor: ``_build_agg`` accepts ``spec`` (AggRenderSpec) as its
    # first positional. Stage A's behavior is that the shim path produces
    # SQL byte-identical to legacy.
    expr, _is_agg = gen._build_agg(spec)
    assert expr.sql(dialect="postgres") == expected_sql
