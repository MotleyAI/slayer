"""End-to-end tests for bare-name ``ModelMeasure`` resolution.

These tests exercise the full typed-pipeline (parse → bind → plan → render).
They prove that a query referencing a saved measure by bare name produces
the same SQL as the equivalent query with the saved formula inlined.
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery

from tests._engine_helpers import _engine_generate


def _orders_model(measures=None) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="tax", sql="tax_amount", type=DataType.DOUBLE),
        ],
        measures=measures or [],
    )


async def _generate(query: SlayerQuery, model: SlayerModel) -> str:
    return await _engine_generate(query, model)


class TestNamedMeasureSQL:
    async def test_root_position_matches_inline(self) -> None:
        """Query with ``{formula: "aov"}`` produces the same SQL as
        ``{formula: "revenue:sum / *:count"}``.
        """
        formula = "revenue:sum / *:count"
        with_saved = _orders_model(
            measures=[ModelMeasure(name="aov", formula=formula)]
        )
        inline = _orders_model()

        saved_query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "aov", "name": "result"}],
        )
        inline_query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": formula, "name": "result"}],
        )

        saved_sql = await _generate(saved_query, with_saved)
        inline_sql = await _generate(inline_query, inline)
        assert saved_sql == inline_sql

    async def test_in_transform(self) -> None:
        """``cumsum(aov)`` matches ``cumsum(revenue:sum / *:count)``."""
        formula = "revenue:sum / *:count"
        with_saved = _orders_model(
            measures=[ModelMeasure(name="aov", formula=formula)]
        )
        inline = _orders_model()

        # cumsum needs a time dimension — add a dummy one for both
        with_saved.columns.append(
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP)
        )
        inline.columns.append(
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP)
        )

        saved_query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "cumsum(aov)", "name": "result"}],
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        )
        inline_query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": f"cumsum({formula})", "name": "result"}],
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        )

        saved_sql = await _generate(saved_query, with_saved)
        inline_sql = await _generate(inline_query, inline)
        assert saved_sql == inline_sql

    async def test_in_arithmetic(self) -> None:
        """``aov * 1.1`` matches inlined."""
        formula = "revenue:sum"
        with_saved = _orders_model(
            measures=[ModelMeasure(name="aov", formula=formula)]
        )
        inline = _orders_model()

        saved_query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "aov * 1.1", "name": "result"}],
        )
        inline_query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": f"{formula} * 1.1", "name": "result"}],
        )

        saved_sql = await _generate(saved_query, with_saved)
        inline_sql = await _generate(inline_query, inline)
        assert saved_sql == inline_sql

    async def test_chained_named_measures(self) -> None:
        """``b → a → revenue:sum`` resolves transitively."""
        chained = _orders_model(
            measures=[
                ModelMeasure(name="a", formula="revenue:sum"),
                ModelMeasure(name="b", formula="a"),
            ]
        )
        inline = _orders_model()

        chained_query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "b", "name": "result"}],
        )
        inline_query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "revenue:sum", "name": "result"}],
        )

        chained_sql = await _generate(chained_query, chained)
        inline_sql = await _generate(inline_query, inline)
        assert chained_sql == inline_sql

    async def test_cycle_raises_at_query_time(self) -> None:
        """A cyclic chain in a model's saved measures raises with the chain
        in the error message when a query references it.
        """
        model = _orders_model(
            measures=[
                ModelMeasure(name="a", formula="b"),
                ModelMeasure(name="b", formula="a"),
            ]
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "a", "name": "result"}],
        )

        with pytest.raises(
            ValueError, match=r"Cyclic reference in named-measure expansion"
        ):
            await _generate(query, model)

    async def test_unknown_bare_name_still_errors(self) -> None:
        """A bare name that is neither a saved measure nor a column still
        produces the existing helpful error.
        """
        model = _orders_model(
            measures=[ModelMeasure(name="aov", formula="revenue:sum")]
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "nonexistent", "name": "result"}],
        )

        with pytest.raises(
            ValueError, match=r"Cannot resolve reference 'nonexistent'"
        ):
            await _generate(query, model)

    async def test_duplicate_saved_measure_name_rejected_at_query_time(self) -> None:
        """Defense-in-depth: even if a model with duplicate saved-measure names
        slips past the construction-time validator (e.g., direct mutation),
        the storage-to-engine reload path re-validates via Pydantic and
        raises before the query can run. Replaces the legacy
        enrichment-time guard.
        """
        model = _orders_model(
            measures=[ModelMeasure(name="aov", formula="revenue:sum")]
        )
        # Bypass the model validator by appending after construction.
        model.measures.append(ModelMeasure(name="aov", formula="revenue:avg"))

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "aov", "name": "result"}],
        )

        with pytest.raises(ValueError, match=r"duplicate measure names"):
            await _generate(query, model)
