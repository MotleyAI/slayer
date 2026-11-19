"""End-to-end tests for bare-name ``ModelMeasure`` resolution.

These tests exercise the full typed-pipeline (parse → bind → plan → render).
They prove that a query referencing a saved measure by bare name produces
the same SQL as the equivalent query with the saved formula inlined.
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.binding import _name_suggestion

from tests._engine_helpers import _engine_generate


def _orders_dataset(metrics=None) -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.DOUBLE, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.TEXT),
            DatasetField(name="revenue", sql="amount", datatype=DataType.DOUBLE),
            DatasetField(name="tax", sql="tax_amount", datatype=DataType.DOUBLE),
        ],
        metrics=metrics or [],
    )


async def _generate(query: SlayerQuery, dataset: SlayerDataset) -> str:
    return await _engine_generate(query=query, dataset=dataset)


class TestNamedMeasureSQL:
    async def test_root_position_matches_inline(self) -> None:
        """Query with ``{formula: "aov"}`` produces the same SQL as
        ``{formula: "revenue:sum / *:count"}``.
        """
        formula = "sum(revenue) / count(*)"
        with_saved = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression=formula)]
        )
        inline = _orders_dataset()

        saved_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "aov", "name": "result"}],
        })
        inline_query = SlayerQuery(
            population="orders",
            metrics=[{"expression": formula, "name": "result"}],
        )

        saved_sql = await _generate(saved_query, with_saved)
        inline_sql = await _generate(inline_query, inline)
        assert saved_sql == inline_sql

    async def test_in_transform(self) -> None:
        """``cumsum(aov)`` matches ``cumsum(sum(revenue) / count(*))``."""
        formula = "sum(revenue) / count(*)"
        with_saved = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression=formula)]
        )
        inline = _orders_dataset()

        # cumsum needs a time dimension — add a dummy one for both
        with_saved.fields.append(
            DatasetField(name="created_at", sql="created_at", datatype=DataType.TIMESTAMP)
        )
        inline.fields.append(
            DatasetField(name="created_at", sql="created_at", datatype=DataType.TIMESTAMP)
        )

        saved_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "cumsum(aov)", "name": "result"}],
            "time_dimensions": [{"field": "created_at", "granularity": "month"}],
        })
        inline_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": f"cumsum({formula})", "name": "result"}],
            "time_dimensions": [{"field": "created_at", "granularity": "month"}],
        })

        saved_sql = await _generate(saved_query, with_saved)
        inline_sql = await _generate(inline_query, inline)
        assert saved_sql == inline_sql

    async def test_in_arithmetic(self) -> None:
        """``aov * 1.1`` matches inlined."""
        formula = "sum(revenue)"
        with_saved = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression=formula)]
        )
        inline = _orders_dataset()

        saved_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "aov * 1.1", "name": "result"}],
        })
        inline_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": f"{formula} * 1.1", "name": "result"}],
        })

        saved_sql = await _generate(saved_query, with_saved)
        inline_sql = await _generate(inline_query, inline)
        assert saved_sql == inline_sql

    async def test_chained_named_measures(self) -> None:
        """``b → a → revenue:sum`` resolves transitively."""
        chained = _orders_dataset(
            metrics=[
                DatasetMetric(name="a", expression="sum(revenue)"),
                DatasetMetric(name="b", expression="a"),
            ]
        )
        inline = _orders_dataset()

        chained_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "b", "name": "result"}],
        })
        inline_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "sum(revenue)", "name": "result"}],
        })

        chained_sql = await _generate(chained_query, chained)
        inline_sql = await _generate(inline_query, inline)
        assert chained_sql == inline_sql

    async def test_cycle_raises_at_query_time(self) -> None:
        """A cyclic chain in a model's saved measures raises with the chain
        in the error message when a query references it.
        """
        model = _orders_dataset(
            metrics=[
                DatasetMetric(name="a", expression="b"),
                DatasetMetric(name="b", expression="a"),
            ]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "a", "name": "result"}],
        })

        with pytest.raises(
            ValueError, match=r"Cyclic reference in named-metric expansion"
        ):
            await _generate(query, model)

    async def test_unknown_bare_name_still_errors(self) -> None:
        """A bare name that is neither a saved measure nor a column still
        produces the existing helpful error.
        """
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression="sum(revenue)")]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "nonexistent", "name": "result"}],
        })

        with pytest.raises(
            ValueError, match=r"Cannot resolve reference 'nonexistent'"
        ):
            await _generate(query, model)

    async def test_near_miss_bare_name_suggests_the_saved_measure(self) -> None:
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov_net", expression="sum(revenue)")]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "aov_nett", "name": "result"}],
        })

        with pytest.raises(ValueError, match="Did you mean 'aov_net'"):
            await _generate(query, model)

    async def test_aggregating_a_saved_measure_says_to_drop_the_suffix(self) -> None:
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression="sum(revenue)")]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "sum(aov)", "name": "result"}],
        })

        with pytest.raises(
            ValueError, match="is a saved metric.*takes no aggregation"
        ):
            await _generate(query, model)

    async def test_bare_column_in_expression_asks_for_an_aggregation(self) -> None:
        model = _orders_dataset()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "round(revenue, 2)", "name": "result"}],
        })

        with pytest.raises(
            ValueError, match="needs an aggregation inside an expression"
        ):
            await _generate(query, model)

    def test_name_suggestion_skips_unnamed_measures(self) -> None:
        # Tested directly: the full pipeline rejects unnamed measures at model
        # re-validation, so they only reach the helper via a raw mutation.
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression="sum(revenue)")]
        )
        model.metrics.append(DatasetMetric(expression="avg(revenue)"))
        assert (
            _name_suggestion(name="revenu", dataset=model)
            == "Did you mean 'revenue'?"
        )

    async def test_unknown_column_suggests_a_close_column(self) -> None:
        model = _orders_dataset()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "sum(revenu)", "name": "result"}],
        })

        with pytest.raises(ValueError, match="Did you mean 'revenue'"):
            await _generate(query, model)

    async def test_duplicate_saved_measure_name_rejected_at_query_time(self) -> None:
        """Defense-in-depth: even if a model with duplicate saved-measure names
        slips past the construction-time validator (e.g., direct mutation),
        the storage-to-engine reload path re-validates via Pydantic and
        raises before the query can run. Replaces the legacy
        enrichment-time guard.
        """
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression="sum(revenue)")]
        )
        # Bypass the model validator by appending after construction.
        model.metrics.append(DatasetMetric(name="aov", expression="avg(revenue)"))

        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "aov", "name": "result"}],
        })

        with pytest.raises(ValueError, match=r"duplicate metric names"):
            await _generate(query, model)


class TestBareNamedMeasureAliasing:
    """Regression: bare named-measure ref must surface the measure NAME as
    the SELECT alias (not the formula-derived canonical), so ORDER BY by
    that name and downstream result-key lookups stay consistent.
    """

    async def test_select_alias_uses_measure_name(self) -> None:
        model = _orders_dataset(
            metrics=[DatasetMetric(name="rev_total", expression="sum(revenue)")]
        )
        query = SlayerQuery(
            population="orders",
            metrics=["rev_total"],
        )
        sql = await _generate(query, model)
        assert '"orders.rev_total"' in sql
        # The formula-derived canonical alias must NOT leak into SELECT.
        assert '"orders.revenue_sum"' not in sql

    async def test_order_by_resolves_against_measure_name(self) -> None:
        """The original bug: SELECT aliased by formula, ORDER BY by name."""
        model = _orders_dataset(
            metrics=[
                DatasetMetric(name="companies_count", expression="count_distinct(revenue)"),
            ]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["status"],
            "metrics": ["companies_count"],
            "order": [{"field": "companies_count", "direction": "desc"}],
        })
        sql = await _generate(query, model)
        # The ORDER BY reference must point at the alias we actually emitted.
        assert '"orders.companies_count"' in sql
        assert "ORDER BY" in sql
        # And the canonical formula-derived alias must NOT appear at all.
        assert "revenue_count_distinct" not in sql
