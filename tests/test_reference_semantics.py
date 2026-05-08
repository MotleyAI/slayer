"""Tests for the two-mode reference semantics introduced by DEV-1369.

Mode A — SQL expression: Column.sql, Column.filter, SlayerModel.filters.
    Free SQL via sqlglot. Bare names = underlying-table columns; ``__``-paths
    = joined-model aliases (with the leaf following a single dot, e.g.
    ``customers__regions.name``). Rejects aggregation colon syntax, transform
    calls, ModelMeasure refs, and (for filters) raw OVER (...).

Mode B — DSL expression: ModelMeasure.formula, SlayerQuery.{measures,
    filters, dimensions, time_dimensions, order, main_time_dimension}.
    Pure DSL via Python AST. Accepts only Column / ModelMeasure references,
    aggregation colon syntax, transforms, single-dot dotted paths, and
    arithmetic / boolean / comparison ops. Rejects raw SQL function calls,
    ``__`` in user input, and bare names that don't resolve to a Column or
    ModelMeasure on the model (strict resolution at enrichment time).

The internal ``__`` carve-out on ``Column.name`` (used by ``_query_as_model``
to flatten joined-model columns into virtual-model columns) is preserved.
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.enrichment import enrich_query


async def _noop_async(**_kwargs):  # NOSONAR
    return None


def _make_planets_model(*, with_window_column: bool = False) -> SlayerModel:
    columns = [
        Column(name="id", sql="id", type=DataType.INT, primary_key=True),
        Column(name="name", sql="name", type=DataType.TEXT),
        Column(name="mass", sql="mass", type=DataType.DOUBLE),
    ]
    if with_window_column:
        columns.append(
            Column(
                name="rn",
                sql="row_number() over (order by mass desc)",
                type=DataType.INT,
            )
        )
    return SlayerModel(
        name="planets",
        sql_table="planets",
        data_source="test",
        columns=columns,
    )


# ---------------------------------------------------------------------------
# Mode A — SQL expression mode (Column.sql, Column.filter, SlayerModel.filters)
# ---------------------------------------------------------------------------


class TestSqlModeAcceptance:
    """Free SQL function calls and SQL-spelled predicates are accepted."""

    def test_column_filter_accepts_json_extract(self) -> None:
        col = Column(
            name="active_amount",
            sql="amount",
            filter="json_extract(metadata, '$.active') = 1",
            type=DataType.DOUBLE,
        )
        assert col.filter is not None
        assert "json_extract" in col.filter.lower()

    def test_column_filter_accepts_coalesce_is_null(self) -> None:
        col = Column(
            name="amt",
            sql="amount",
            filter="coalesce(a, b) IS NULL",
            type=DataType.DOUBLE,
        )
        assert col.filter is not None
        assert "coalesce" in col.filter.lower()
        assert "IS NULL" in col.filter.upper()

    def test_column_filter_accepts_case_when(self) -> None:
        col = Column(
            name="amt",
            sql="amount",
            filter="CASE WHEN status = 'active' THEN 1 ELSE 0 END = 1",
            type=DataType.DOUBLE,
        )
        assert col.filter is not None
        assert "CASE" in col.filter.upper()

    def test_model_filter_accepts_json_extract(self) -> None:
        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["json_extract(metadata, '$.active') = 1"],
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        )
        assert "json_extract" in model.filters[0].lower()

    def test_model_filter_accepts_lower_function(self) -> None:
        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["lower(status) = 'active'"],
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        )
        assert model.filters == ["lower(status) = 'active'"]

    def test_model_filter_accepts_double_underscore_join_path(self) -> None:
        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["customers__regions.name = 'US'"],
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        )
        assert "customers__regions.name" in model.filters[0]

    def test_model_filter_accepts_in_operator(self) -> None:
        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["status IN ('a', 'b', 'c')"],
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        )
        assert "IN" in model.filters[0].upper()

    def test_model_filter_accepts_is_null(self) -> None:
        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["deleted_at IS NULL"],
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        )
        assert "IS NULL" in model.filters[0].upper()


class TestSqlModeRejection:
    """DSL constructs (aggregation colon, transforms, OVER) are rejected on
    the model side. SQL is free, but it cannot reach into the DSL."""

    def test_column_filter_rejects_aggregation_colon(self) -> None:
        with pytest.raises(ValueError, match="(?i)aggregat|colon|measure|DSL"):
            Column(
                name="x",
                sql="amount",
                filter="revenue:sum > 100",
                type=DataType.DOUBLE,
            )

    def test_column_filter_rejects_transform_call(self) -> None:
        with pytest.raises(ValueError, match="(?i)transform|cumsum|DSL"):
            Column(
                name="x",
                sql="amount",
                filter="cumsum(amount) > 0",
                type=DataType.DOUBLE,
            )

    def test_model_filter_rejects_aggregation_colon(self) -> None:
        with pytest.raises(ValueError, match="(?i)aggregat|colon|measure|DSL"):
            SlayerModel(
                name="m",
                sql_table="t",
                data_source="test",
                filters=["revenue:sum > 100"],
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True)
                ],
            )

    def test_model_filter_rejects_transform_call(self) -> None:
        with pytest.raises(ValueError, match="(?i)transform|cumsum|DSL"):
            SlayerModel(
                name="m",
                sql_table="t",
                data_source="test",
                filters=["cumsum(revenue:sum) > 0"],
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True)
                ],
            )

    def test_model_filter_rejects_raw_over(self) -> None:
        """Raw OVER (...) in a model filter is rejected (existing behavior preserved)."""
        with pytest.raises(ValueError, match="(?i)window function|OVER"):
            SlayerModel(
                name="m",
                sql_table="t",
                data_source="test",
                filters=["row_number() over (order by mass desc) <= 3"],
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True)
                ],
            )

    def test_column_filter_rejects_raw_over(self) -> None:
        with pytest.raises(ValueError, match="(?i)window function|OVER"):
            Column(
                name="x",
                sql="amount",
                filter="row_number() over (order by mass) <= 3",
                type=DataType.DOUBLE,
            )


# ---------------------------------------------------------------------------
# Mode B — DSL expression mode (queries + ModelMeasure)
# ---------------------------------------------------------------------------


class TestDslModeAcceptance:
    """DSL constructs work as before. Ensures the new validators don't over-reject."""

    def test_query_filter_accepts_aggregation_colon(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["revenue:sum > 100"],
        )
        assert q.filters == ["revenue:sum > 100"]

    def test_query_filter_accepts_transform_predicate(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["change(revenue:sum) > 0"],
        )
        assert q.filters == ["change(revenue:sum) > 0"]

    def test_query_filter_accepts_nested_transform(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["last(change(revenue:sum)) < 0"],
        )
        assert q.filters == ["last(change(revenue:sum)) < 0"]

    def test_query_filter_accepts_dotted_join_path(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["customers.region == 'EU'"],
        )
        assert q.filters == ["customers.region == 'EU'"]

    def test_query_filter_accepts_variable_placeholder(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["status = '{status_val}'"],
            variables={"status_val": "active"},
        )
        assert q.filters == ["status = '{status_val}'"]

    def test_model_measure_accepts_cross_model_dotted_ref(self) -> None:
        """Cross-model dotted refs in ModelMeasure formulas remain supported (per spec)."""
        m = ModelMeasure(name="cust_rev", formula="customers.revenue:sum")
        assert m.formula == "customers.revenue:sum"


class TestDslModeRejection:
    """Raw SQL functions, __ in user input, and OVER (...) are rejected at
    SlayerQuery / ModelMeasure construction time."""

    async def test_query_filter_rejects_raw_sql_function_at_enrichment(self) -> None:
        """If a user needs json_extract/coalesce/etc. they must define a Column
        on the model and reference it from the DSL. The rejection fires at
        enrichment time, where the DSL parser has full custom-aggregation
        context and can produce an accurate ``Unknown filter function`` error.
        """
        model = SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="data", sql="data", type=DataType.TEXT),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=["id"],
            filters=["json_extract(data, '$.x') > 5"],
        )
        with pytest.raises(Exception, match="(?i)function|json_extract|raw SQL|unknown|transform"):
            await enrich_query(
                query=query,
                model=model,
                resolve_dimension_via_joins=_noop_async,
                resolve_cross_model_measure=_noop_async,
                resolve_join_target=_noop_async,
            )

    async def test_query_filter_rejects_unknown_double_underscore_at_enrichment(self) -> None:
        """``customers__region`` is a typo (no virtual column with that name on
        the source model). Strict resolution at enrichment catches it. Note:
        ``__`` is NOT rejected at construction because virtual-model columns
        produced by ``_query_as_model`` legitimately contain ``__`` in their
        names (e.g. ``kpis__total_amount_sum``)."""
        model = SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=["id"],
            filters=["customers__region = 'EU'"],
        )
        with pytest.raises(Exception, match="(?i)customers__region|unknown|not a Column"):
            await enrich_query(
                query=query,
                model=model,
                resolve_dimension_via_joins=_noop_async,
                resolve_cross_model_measure=_noop_async,
                resolve_join_target=_noop_async,
            )

    def test_query_filter_rejects_raw_over(self) -> None:
        with pytest.raises(ValueError, match="(?i)window function|OVER"):
            SlayerQuery(
                source_model="orders",
                filters=["row_number() over (order by mass) <= 3"],
            )

    async def test_model_measure_rejects_raw_sql_function_at_enrichment(self) -> None:
        """ModelMeasure is DSL — ``json_extract`` has no place there. The
        rejection fires at enrichment time (where the DSL parser runs with
        full custom-aggregation context), not at ModelMeasure construction.
        """
        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
            measures=[ModelMeasure(name="bad", formula="json_extract(data, '$.x')")],
        )
        query = SlayerQuery(
            source_model="m",
            measures=["bad"],
            dimensions=["id"],
        )
        with pytest.raises(Exception, match="(?i)function|json_extract|raw SQL|unknown|transform"):
            await enrich_query(
                query=query,
                model=model,
                resolve_dimension_via_joins=_noop_async,
                resolve_cross_model_measure=_noop_async,
                resolve_join_target=_noop_async,
            )


# ---------------------------------------------------------------------------
# Strict resolution at enrichment time
# ---------------------------------------------------------------------------


class TestStrictResolution:
    """A bare name in a query field that doesn't resolve to a defined Column,
    a ModelMeasure, or a {variable} raises ReferenceError at enrichment.
    """

    async def test_unknown_filter_name_raises_at_enrichment(self) -> None:
        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        )
        query = SlayerQuery(
            source_model="m",
            dimensions=["id"],
            filters=["unknown_col > 0"],
        )
        with pytest.raises(Exception, match="(?i)unknown_col|not.*column.*measure|undefined|unknown"):
            await enrich_query(
                query=query,
                model=model,
                resolve_dimension_via_joins=_noop_async,
                resolve_cross_model_measure=_noop_async,
                resolve_join_target=_noop_async,
            )

    async def test_known_filter_name_passes(self) -> None:
        """Control: a filter naming a defined Column enriches without error."""
        model = SlayerModel(
            name="m",
            sql_table="t",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        )
        query = SlayerQuery(
            source_model="m",
            dimensions=["id"],
            filters=["status = 'active'"],
        )
        # Should not raise.
        await enrich_query(
            query=query,
            model=model,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=_noop_async,
        )


# ---------------------------------------------------------------------------
# Predicate-promotion drop (DEV-1336 reversal)
# ---------------------------------------------------------------------------


class TestPredicatePromotionRemoved:
    """A query filter referencing a Column whose `sql` contains a window
    function used to auto-promote to a post-aggregation outer WHERE. That
    escape hatch is dropped — users use rank-family transforms instead.
    """

    async def test_filter_on_windowed_column_raises_with_actionable_message(self) -> None:
        model = _make_planets_model(with_window_column=True)
        query = SlayerQuery(
            source_model="planets",
            dimensions=["name"],
            filters=["rn <= 3"],
        )
        with pytest.raises(Exception) as excinfo:
            await enrich_query(
                query=query,
                model=model,
                resolve_dimension_via_joins=_noop_async,
                resolve_cross_model_measure=_noop_async,
                resolve_join_target=_noop_async,
            )
        msg = str(excinfo.value).lower()
        assert "window function" in msg or "rank" in msg, (
            f"Expected message to mention 'window function' and/or 'rank' "
            f"transform suggestion. Got: {excinfo.value}"
        )

    async def test_select_only_on_windowed_column_still_works(self) -> None:
        """A windowed Column.sql is still legal as a *projection* — only as a
        filter target does it now error."""
        model = _make_planets_model(with_window_column=True)
        query = SlayerQuery(
            source_model="planets",
            dimensions=["name", "rn"],
        )
        # Should enrich without error; only filter use is restricted.
        await enrich_query(
            query=query,
            model=model,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=_noop_async,
        )


# ---------------------------------------------------------------------------
# Internal __ carve-out for virtual-model columns
# ---------------------------------------------------------------------------


class TestVirtualModelColumnCarveOut:
    """`_query_as_model` flattens joined-model columns into virtual-model
    column names like `stores__name`. The Column-level validator must keep
    accepting `__` so that path keeps working. The user-input dunder check
    fires at SlayerQuery / ModelMeasure construction, not at Column
    construction (which is also used internally).
    """

    def test_column_with_double_underscore_name_accepted(self) -> None:
        col = Column(name="stores__name", type=DataType.TEXT)
        assert col.name == "stores__name"

    def test_column_with_three_segment_double_underscore_name_accepted(self) -> None:
        col = Column(name="customers__regions__name", type=DataType.TEXT)
        assert col.name == "customers__regions__name"
