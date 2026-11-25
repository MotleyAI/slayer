"""Tests for the two-mode reference semantics.

Mode A — SQL expression: DatasetField.sql, DatasetField.filter, SlayerDataset.filters.
    Free SQL via sqlglot. Bare names = underlying-table columns; ``__``-paths
    = joined-dataset aliases (with the leaf following a single dot, e.g.
    ``customers__regions.name``). Rejects aggregation colon syntax, transform
    calls, DatasetMetric refs, and (for filters) raw OVER (...).

Mode B — DSL expression: DatasetMetric.expression, SlayerQuery.{metrics,
    filters, dimensions, time_dimensions, order, main_time_dimension}.
    Pure DSL via Python AST. Accepts only DatasetField / DatasetMetric references,
    aggregation colon syntax, transforms, single-dot dotted paths, and
    arithmetic / boolean / comparison ops. Rejects raw SQL function calls,
    ``__`` in user input, and bare names that don't resolve to a DatasetField or
    DatasetMetric on the dataset (strict resolution at enrichment time).

The internal ``__`` carve-out on ``DatasetField.name`` (used by the query-backed
virtual-dataset expansion to flatten joined-dataset fields into virtual-dataset
fields) is preserved.
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery

from tests._engine_helpers import _dataset_where_text, _engine_generate


def _make_planets_dataset(*, with_window_field: bool = False) -> SlayerDataset:
    columns = [
        DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="name", expression="name", datatype=DataType.STRING),
        DatasetField(name="mass", expression="mass", datatype=DataType.FLOAT),
    ]
    if with_window_field:
        columns.append(
            DatasetField(
                name="rn",
                expression=ExpressionObject(dialects=[DialectEntry(dialect="POSTGRES", expression="row_number() over (order by mass desc)")]),
                datatype=DataType.INTEGER,
            )
        )
    return SlayerDataset(
        name="planets",
        sql_table="planets",
        data_source="test",
        fields=columns,
    )


# ---------------------------------------------------------------------------
# Mode A — SQL expression mode (DatasetField.sql, DatasetField.filter, SlayerDataset.filters)
# ---------------------------------------------------------------------------


class TestSqlModeAcceptance:
    """Free SQL function calls and SQL-spelled predicates are accepted."""

    def test_column_filter_accepts_json_extract(self) -> None:
        col = DatasetField(
            name="active_amount",
            expression="amount",
            filter="json_extract(metadata, '$.active') = 1",
            datatype=DataType.FLOAT,
        )
        assert col.filter is not None
        assert "json_extract" in col.filter.lower()

    def test_column_filter_accepts_coalesce_is_null(self) -> None:
        col = DatasetField(
            name="amt",
            expression="amount",
            filter="coalesce(a, b) IS NULL",
            datatype=DataType.FLOAT,
        )
        assert col.filter is not None
        assert "coalesce" in col.filter.lower()
        assert "IS NULL" in col.filter.upper()

    def test_column_filter_accepts_case_when(self) -> None:
        col = DatasetField(
            name="amt",
            expression="amount",
            filter="CASE WHEN status = 'active' THEN 1 ELSE 0 END = 1",
            datatype=DataType.FLOAT,
        )
        assert col.filter is not None
        assert "CASE" in col.filter.upper()

    def test_model_filter_accepts_json_extract(self) -> None:
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["json_extract(metadata, '$.active') = 1"],
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        assert "json_extract" in model.filters[0].lower()

    def test_model_filter_accepts_lower_function(self) -> None:
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["lower(status) = 'active'"],
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        assert model.filters == ["lower(status) = 'active'"]

    def test_model_filter_accepts_double_underscore_join_path(self) -> None:
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["customers__regions.name = 'US'"],
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        assert "customers__regions.name" in model.filters[0]

    def test_model_filter_accepts_in_operator(self) -> None:
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["status IN ('a', 'b', 'c')"],
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        assert "IN" in model.filters[0].upper()

    def test_model_filter_accepts_is_null(self) -> None:
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["deleted_at IS NULL"],
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        assert "IS NULL" in model.filters[0].upper()


class TestSqlModeRuntime:
    """Mode A SQL filters (``DatasetField.filter`` /
    ``SlayerDataset.filters``) must not just construct cleanly — they
    have to flow through enrichment without the DSL parser rejecting
    arbitrary SQL function calls. Once, the construction
    tests passed but the engine failed at first query against any
    such dataset.
    """

    async def test_model_filter_with_json_extract_enriches(self) -> None:
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["json_extract(metadata, '$.active') = 1"],
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        query = SlayerQuery(
            population="m",
            metrics=[QueryMetric(metric="sum(m.amount)")],
        )
        # This once raised "Unknown filter function 'json_extract'" at enrichment
        # time; it renders — the Mode A dataset filter survives into the WHERE of
        # the dataset's filtered scan.
        sql = await _engine_generate(query=query, dataset=model)
        assert "json_extract" in _dataset_where_text(sql).lower(), (
            f"dataset filter dropped from WHERE clause:\n{sql}"
        )

    async def test_column_filter_with_lower_enriches(self) -> None:
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(
                    name="active_amount",
                    expression="amount",
                    filter="lower(status) = 'active'",
                    datatype=DataType.FLOAT,
                ),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
            ],
        )
        query = SlayerQuery(
            population="m",
            metrics=[QueryMetric(metric="sum(m.active_amount)")],
        )
        sql = await _engine_generate(query=query, dataset=model)
        # The DatasetField.filter goes onto the metric as a CASE WHEN wrapper
        # inside the aggregate — the lowercase ``lower(...)`` predicate must
        # appear within the SUM expression.
        assert "lower" in sql.lower(), f"field filter dropped:\n{sql}"
        assert "CASE" in sql.upper(), f"field filter not wrapped in CASE:\n{sql}"

    async def test_model_filter_with_double_underscore_join_path_enriches(self) -> None:
        # ``customers__regions.name`` is a ``__``-delimited relationship path —
        # parse_sql_predicate must preserve it as one column ref so the
        # downstream relationship detection in _scan_filter_column_ref recognises the
        # path and pulls the two-hop ``m → customers → regions`` relationship chain.
        # With a real relationship target the ``__``-alias resolves to a bound
        # ``LEFT JOIN r AS customers__regions``, so the emitted SQL is
        # scope-closed AND the ``__``-path survives into it.
        regions = SlayerDataset(
            name="regions", sql_table="r", data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="name", expression="name", datatype=DataType.STRING),
            ],
        )
        customers = SlayerDataset(
            name="customers", sql_table="c", data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="region_id", expression="region_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
        )
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            filters=["customers.regions.name = 'EU'"],
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
        query = SlayerQuery(
            population="m",
            metrics=[QueryMetric(metric="count(m.*)")],
        )
        sql = await _engine_generate(
            query=query, dataset=model, extra_datasets=[customers, regions],
        )
        assert "customers__regions.name" in sql, (
            f"__-delimited relationship path dropped from emitted SQL:\n{sql}"
        )
        assert "AS customers__regions" in sql, (
            f"__-path did not resolve to a bound join alias:\n{sql}"
        )


class TestSqlModeRejection:
    """DSL constructs (aggregation colon, transforms, OVER) are rejected on
    the dataset side. SQL is free, but it cannot reach into the DSL."""

    def test_column_filter_rejects_aggregation_colon(self) -> None:
        with pytest.raises(ValueError, match="(?i)aggregat|colon|measure|DSL"):
            DatasetField(
                name="x",
                expression="amount",
                filter="revenue:sum > 100",
                datatype=DataType.FLOAT,
            )

    def test_column_filter_rejects_transform_call(self) -> None:
        with pytest.raises(ValueError, match="(?i)transform|cumsum|DSL"):
            DatasetField(
                name="x",
                expression="amount",
                filter="cumsum(amount) > 0",
                datatype=DataType.FLOAT,
            )

    def test_model_filter_rejects_aggregation_colon(self) -> None:
        columns = [DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)]
        with pytest.raises(ValueError, match="(?i)aggregat|colon|measure|DSL"):
            SlayerDataset(
                name="m",
                sql_table="t",
                data_source="test",
                filters=["revenue:sum > 100"],
                fields=columns,
            )

    def test_model_filter_rejects_transform_call(self) -> None:
        columns = [DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)]
        with pytest.raises(ValueError, match="(?i)transform|cumsum|DSL"):
            SlayerDataset(
                name="m",
                sql_table="t",
                data_source="test",
                filters=["cumsum(sum(revenue)) > 0"],
                fields=columns,
            )

    def test_model_filter_rejects_raw_over(self) -> None:
        """Raw OVER (...) in a dataset filter is rejected (existing behavior preserved)."""
        columns = [DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)]
        with pytest.raises(ValueError, match="(?i)window function|OVER"):
            SlayerDataset(
                name="m",
                sql_table="t",
                data_source="test",
                filters=["row_number() over (order by mass desc) <= 3"],
                fields=columns,
            )

    def test_column_filter_rejects_raw_over(self) -> None:
        with pytest.raises(ValueError, match="(?i)window function|OVER"):
            DatasetField(
                name="x",
                expression="amount",
                filter="row_number() over (order by mass) <= 3",
                datatype=DataType.FLOAT,
            )


# ---------------------------------------------------------------------------
# Mode B — DSL expression mode (queries + DatasetMetric)
# ---------------------------------------------------------------------------


class TestDslModeAcceptance:
    """DSL constructs work as before. Ensures the new validators don't over-reject."""

    def test_query_filter_accepts_aggregation(self) -> None:
        q = SlayerQuery(
            population="orders",
            filter=["sum(orders.revenue) > 100"],
        )
        assert q.filter == ["sum(orders.revenue) > 100"]

    def test_query_filter_accepts_transform_predicate(self) -> None:
        q = SlayerQuery(
            population="orders",
            filter=["change(sum(orders.revenue)) > 0"],
        )
        assert q.filter == ["change(sum(orders.revenue)) > 0"]

    def test_query_filter_accepts_nested_transform(self) -> None:
        q = SlayerQuery(
            population="orders",
            filter=["last(change(sum(orders.revenue))) < 0"],
        )
        assert q.filter == ["last(change(sum(orders.revenue))) < 0"]

    def test_query_filter_accepts_dotted_join_path(self) -> None:
        q = SlayerQuery(
            population="orders",
            filter=["customers.region == 'EU'"],
        )
        assert q.filter == ["customers.region == 'EU'"]

    def test_query_filter_accepts_variable_placeholder(self) -> None:
        q = SlayerQuery(
            population="orders",
            filter=["orders.status = '{status_val}'"],
            variables={"status_val": "active"},
        )
        assert q.filter == ["orders.status = '{status_val}'"]

    def test_model_measure_accepts_cross_model_dotted_ref(self) -> None:
        """Cross-dataset dotted refs in DatasetMetric expressions remain supported (per spec)."""
        m = DatasetMetric(name="cust_rev", expression="sum(customers.revenue)")
        assert m.expression == "sum(customers.revenue)"


class TestDslModeRejection:
    """DSL-mode rejection rules. Raw ``OVER (...)`` is rejected at
    SlayerQuery / DatasetMetric construction (Python AST cannot parse it).
    Raw SQL function calls and unresolved bare names — including
    ``__``-prefixed tokens that don't resolve to a virtual-dataset field
    — are rejected at enrichment time, where the parser has full
    custom-aggregation and named-metric context.
    """

    async def test_query_filter_rejects_raw_sql_function_at_enrichment(self) -> None:
        """A function outside the Mode-B library (``initcap``) must be a DatasetField on the dataset,
        referenced from the DSL. The rejection fires at enrichment time, where the DSL parser has
        full custom-aggregation context and can produce an accurate ``Unknown filter function`` error.
        """
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="data", expression="data", datatype=DataType.STRING),
            ],
        )
        query = SlayerQuery(
            population="orders",
            dimensions=["orders.id"],
            filter=["initcap(orders.data) = 'X'"],
        )
        with pytest.raises(Exception, match="(?i)function|initcap|raw SQL|unknown|transform"):
            await _engine_generate(query=query, dataset=model)

    async def test_query_filter_rejects_unknown_double_underscore_at_enrichment(self) -> None:
        """``customers__region`` is a typo (no virtual field with that name on
        the source dataset). Strict resolution at bind time catches it. Note:
        ``__`` is NOT rejected at construction because virtual-dataset fields
        produced by query-backed expansion legitimately contain ``__`` in their
        names (e.g. ``kpis__total_amount_sum``)."""
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        query = SlayerQuery(
            population="orders",
            dimensions=["orders.id"],
            filter=["orders.customers__region = 'EU'"],
        )
        with pytest.raises(Exception, match="(?i)customers__region|unknown|not a Column"):
            await _engine_generate(query=query, dataset=model)

    def test_query_filter_rejects_raw_over(self) -> None:
        with pytest.raises(ValueError, match="(?i)window function|OVER"):
            SlayerQuery(
                population="orders",
                filter=["row_number() over (order by mass) <= 3"],
            )

    async def test_model_measure_rejects_raw_sql_function_at_enrichment(self) -> None:
        """DatasetMetric is DSL — ``json_extract`` has no place there. The
        rejection fires at enrichment time (where the DSL parser runs with
        full custom-aggregation context), not at DatasetMetric construction.
        """
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
            metrics=[DatasetMetric(name="bad", expression="json_extract(data, '$.x')")],
        )
        query = SlayerQuery(
            population="m",
            metrics=["m.bad"],
            dimensions=["m.id"],
        )
        with pytest.raises(Exception, match="(?i)function|json_extract|raw SQL|unknown|transform"):
            await _engine_generate(query=query, dataset=model)


# ---------------------------------------------------------------------------
# Strict resolution at enrichment time
# ---------------------------------------------------------------------------


class TestStrictResolution:
    """A bare name in a query field that doesn't resolve to a defined DatasetField,
    a DatasetMetric, or a {variable} raises ReferenceError at enrichment.
    """

    async def test_unknown_filter_name_raises_at_enrichment(self) -> None:
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        query = SlayerQuery(
            population="m",
            dimensions=[QueryDimension(field="m.id")],
            filter=["m.unknown_col > 0"],
        )
        with pytest.raises(Exception, match="(?i)unknown_col|not.*column.*measure|undefined|unknown"):
            await _engine_generate(query=query, dataset=model)

    async def test_unknown_canonical_agg_shaped_typo_raises_at_enrichment(self) -> None:
        """Regression for round-2 review: a name that *looks* like a canonical
        agg alias (``made_up_sum``, ``typo_count_distinct``, ``foo_avg``) but
        was never synthesised by the parser must NOT pass the strict-resolution
        check. The prior implementation used a permissive regex over
        BUILTIN_AGGREGATIONS that let these typos through."""
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            fields=[DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        for typo in ("m.made_up_sum", "m.typo_count_distinct", "m.foo_avg"):
            query = SlayerQuery(
                population="m",
                dimensions=[QueryDimension(field="m.id")],
                filter=[f"{typo} > 0"],
            )
            with pytest.raises(Exception, match=f"(?i){typo}|unknown|not a Column"):
                await _engine_generate(query=query, dataset=model)

    async def test_filter_referencing_unjoined_model_raises(self) -> None:
        """A filter like ``transportation_assets.total_vehicles >= 3``
        where ``transportation_assets`` is **not** in the source dataset's
        ``relationships`` used to silently render SQL with an unbound table reference
        in the WHERE clause and fail at execution with a cryptic database
        error. The strict-resolution check now raises at enrichment with
        a clear "no relationship" message so agents can recover on retry.
        """
        households = SlayerDataset(
            name="households",
            sql_table="households",
            data_source="test",
            fields=[
                DatasetField(name="housenum", expression="housenum", datatype=DataType.INTEGER, primary_key=True),
            ],
            # Note: NO relationship to ``transportation_assets``.
        )
        query = SlayerQuery(
            population="households",
            dimensions=[QueryDimension(field="households.housenum")],
            filter=["households.transportation_assets.total_vehicles >= 3"],
        )
        # An unreachable short form is route-aware-rejected (a ValueError) — still a loud raise, never silent unbound SQL.
        with pytest.raises(
            ValueError,
            match=(
                r"no relationship to 'transportation_assets'"
                r"|Cannot resolve reference 'transportation_assets"
                r"|not a valid relationship path from 'households'"
            ),
        ):
            await _engine_generate(query=query, dataset=households)

    # The two former lenient-drop tests moved to producer filter
    # inheritance (tests/test_dev1836_filter_inheritance, test_dev1747_reroot_filter_routing).

    async def test_known_filter_name_passes(self) -> None:
        """Control: a filter naming a defined DatasetField enriches without error."""
        model = SlayerDataset(
            name="m",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
            ],
        )
        query = SlayerQuery(
            population="m",
            dimensions=[QueryDimension(field="m.id")],
            filter=["m.status = 'active'"],
        )
        # Should not raise.
        await _engine_generate(query=query, dataset=model)


# ---------------------------------------------------------------------------
# Predicate-promotion drop (reversed)
# ---------------------------------------------------------------------------


class TestPredicatePromotionRemoved:
    """A windowed DatasetField is a column of its dataset: a query filter on it generates, as does a projection."""

    async def test_filter_on_windowed_column_generates(self) -> None:
        model = _make_planets_dataset(with_window_field=True)
        query = SlayerQuery(
            population="planets",
            dimensions=[QueryDimension(field="planets.name")],
            filter=["planets.rn <= 3"],
        )
        await _engine_generate(query=query, dataset=model)

    async def test_select_only_on_windowed_column_still_works(self) -> None:
        """A windowed DatasetField.sql projects."""
        model = _make_planets_dataset(with_window_field=True)
        query = SlayerQuery(
            population="planets",
            dimensions=["planets.name", "planets.rn"],
        )
        await _engine_generate(query=query, dataset=model)


# ---------------------------------------------------------------------------
# Internal __ carve-out for virtual-dataset fields
# ---------------------------------------------------------------------------


class TestVirtualModelColumnCarveOut:
    """Query-backed virtual-dataset expansion flattens joined-dataset fields
    into virtual-dataset field names like `stores__name`. The DatasetField-level
    validator must keep accepting `__` so that path keeps working. The
    user-input dunder check fires at SlayerQuery / DatasetMetric
    construction, not at DatasetField construction (which is also used internally).
    """

    def test_column_with_double_underscore_name_accepted(self) -> None:
        col = DatasetField(name="stores__name", datatype=DataType.STRING)
        assert col.name == "stores__name"

    def test_column_with_three_segment_double_underscore_name_accepted(self) -> None:
        col = DatasetField(name="customers__regions__name", datatype=DataType.STRING)
        assert col.name == "customers__regions__name"
