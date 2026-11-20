"""End-to-end tests for bare-name ``DatasetMetric`` resolution.

These tests exercise the full typed-pipeline (parse → bind → plan → render).
They prove that a query referencing a saved metric by bare name produces
the same SQL as the equivalent query with the saved expression inlined.
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery

from tests._engine_helpers import _engine_generate


def _orders_dataset(metrics=None) -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT),
            DatasetField(name="tax", sql="tax_amount", datatype=DataType.FLOAT),
        ],
        metrics=metrics or [],
    )


async def _generate(query: SlayerQuery, dataset: SlayerDataset) -> str:
    return await _engine_generate(query=query, dataset=dataset)


class TestNamedMeasureSQL:
    async def test_root_position_matches_inline(self) -> None:
        """Query with ``{metric: "aov"}`` produces the same SQL as
        ``{metric: "sum(revenue) / count(*)"}``.
        """
        formula = "sum(orders.revenue) / count(orders.*)"
        with_saved = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression=formula)]
        )
        inline = _orders_dataset()

        saved_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "orders.aov", "alias": "result"}],
        })
        inline_query = SlayerQuery(
            population="orders",
            metrics=[{"metric": formula, "alias": "result"}],
        )

        saved_sql = await _generate(saved_query, with_saved)
        inline_sql = await _generate(inline_query, inline)
        assert saved_sql == inline_sql

    async def test_in_transform(self) -> None:
        """``cumsum(aov)`` matches ``cumsum(sum(revenue) / count(*))``."""
        formula = "sum(orders.revenue) / count(orders.*)"
        with_saved = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression=formula)]
        )
        inline = _orders_dataset()

        # cumsum needs a time dimension — add a dummy one for both
        with_saved.fields.append(
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME)
        )
        inline.fields.append(
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME)
        )

        saved_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "cumsum(orders.aov)", "alias": "result"}],
            "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
        })
        inline_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": f"cumsum({formula})", "alias": "result"}],
            "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
        })

        saved_sql = await _generate(saved_query, with_saved)
        inline_sql = await _generate(inline_query, inline)
        assert saved_sql == inline_sql

    async def test_in_arithmetic(self) -> None:
        """``aov * 1.1`` matches inlined."""
        formula = "sum(orders.revenue)"
        with_saved = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression=formula)]
        )
        inline = _orders_dataset()

        saved_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "orders.aov * 1.1", "alias": "result"}],
        })
        inline_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": f"{formula} * 1.1", "alias": "result"}],
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
            "metrics": [{"metric": "orders.b", "alias": "result"}],
        })
        inline_query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.revenue)", "alias": "result"}],
        })

        chained_sql = await _generate(chained_query, chained)
        inline_sql = await _generate(inline_query, inline)
        assert chained_sql == inline_sql

    async def test_cycle_raises_at_query_time(self) -> None:
        """A cyclic chain in a dataset's saved metrics raises with the chain
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
            "metrics": [{"metric": "orders.a", "alias": "result"}],
        })

        with pytest.raises(
            ValueError, match=r"Cyclic reference in named-metric expansion"
        ):
            await _generate(query, model)

    async def test_unknown_bare_name_still_errors(self) -> None:
        """A bare name that is neither a saved metric nor a field still
        produces the existing helpful error.
        """
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression="sum(revenue)")]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "orders.nonexistent", "alias": "result"}],
        })

        with pytest.raises(
            ValueError, match=r"Cannot resolve reference 'orders\.nonexistent'"
        ):
            await _generate(query, model)

    async def test_near_miss_bare_name_suggests_the_saved_measure(self) -> None:
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov_net", expression="sum(revenue)")]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "orders.aov_nett", "alias": "result"}],
        })

        with pytest.raises(ValueError, match="Did you mean 'aov_net'"):
            await _generate(query, model)

    async def test_aggregating_a_saved_measure_says_to_drop_the_suffix(self) -> None:
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression="sum(revenue)")]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.aov)", "alias": "result"}],
        })

        with pytest.raises(
            ValueError, match="is a saved metric.*takes no aggregation"
        ):
            await _generate(query, model)

    async def test_bare_column_in_expression_asks_for_an_aggregation(self) -> None:
        model = _orders_dataset()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "round(orders.revenue, 2)", "alias": "result"}],
        })

        with pytest.raises(
            ValueError, match="needs an aggregation inside an expression"
        ):
            await _generate(query, model)

    async def test_unknown_column_suggests_a_close_column(self) -> None:
        model = _orders_dataset()
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.revenu)", "alias": "result"}],
        })

        with pytest.raises(ValueError, match="Did you mean 'revenue'"):
            await _generate(query, model)

    async def test_duplicate_saved_measure_name_rejected_at_query_time(self) -> None:
        """Defense-in-depth: even if a dataset with duplicate saved-metric names
        slips past the construction-time validator (e.g., direct mutation),
        the storage-to-engine reload path re-validates via Pydantic and
        raises before the query can run. Replaces the legacy
        enrichment-time guard.
        """
        model = _orders_dataset(
            metrics=[DatasetMetric(name="aov", expression="sum(revenue)")]
        )
        # Bypass the dataset validator by appending after construction.
        model.metrics.append(DatasetMetric(name="aov", expression="avg(revenue)"))

        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "orders.aov", "alias": "result"}],
        })

        with pytest.raises(ValueError, match=r"duplicate metric names"):
            await _generate(query, model)


class TestBareNamedMeasureAliasing:
    """Regression: bare named-metric ref must surface the metric NAME as
    the SELECT alias (not the expression-derived canonical), so ORDER BY by
    that name and downstream result-key lookups stay consistent.
    """

    async def test_select_alias_uses_measure_name(self) -> None:
        model = _orders_dataset(
            metrics=[DatasetMetric(name="rev_total", expression="sum(revenue)")]
        )
        query = SlayerQuery(
            population="orders",
            metrics=["orders.rev_total"],
        )
        sql = await _generate(query, model)
        assert '"orders.rev_total"' in sql
        # The expression-derived canonical alias must NOT leak into SELECT.
        assert '"orders.revenue_sum"' not in sql

    async def test_order_by_resolves_against_measure_name(self) -> None:
        """The original bug: SELECT aliased by expression, ORDER BY by name."""
        model = _orders_dataset(
            metrics=[
                DatasetMetric(name="companies_count", expression="count_distinct(revenue)"),
            ]
        )
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": ["orders.companies_count"],
            "order": [{"field": "orders.companies_count", "direction": "desc"}],
        })
        sql = await _generate(query, model)
        # The ORDER BY reference must point at the alias we actually emitted.
        assert '"orders.companies_count"' in sql
        assert "ORDER BY" in sql
        # And the canonical expression-derived alias must NOT appear at all.
        assert "revenue_count_distinct" not in sql
