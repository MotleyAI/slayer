"""Tests for core domain models."""

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Aggregation, DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.core.query import ColumnRef, Field, OrderItem, SlayerQuery, TimeDimension


class TestColumnRef:
    def test_from_string_with_model(self) -> None:
        ref = ColumnRef.from_string("orders.status")
        assert ref.model == "orders"
        assert ref.name == "status"
        assert ref.full_name == "orders.status"

    def test_from_string_without_model(self) -> None:
        ref = ColumnRef.from_string("status")
        assert ref.model is None
        assert ref.name == "status"
        assert ref.full_name == "status"

    def test_dotted_name_parsed_into_model(self) -> None:
        """Dotted name like 'customers.name' is auto-parsed: model='customers', name='name'."""
        ref = ColumnRef(name="customers.name")
        assert ref.model == "customers"
        assert ref.name == "name"
        assert ref.full_name == "customers.name"

    def test_multihop_dotted_name_parsed(self) -> None:
        """Multi-hop 'customers.regions.name' splits on last dot."""
        ref = ColumnRef(name="customers.regions.name")
        assert ref.model == "customers.regions"
        assert ref.name == "name"
        assert ref.full_name == "customers.regions.name"

    def test_simple_name_no_model(self) -> None:
        """Simple name without dots leaves model as None."""
        ref = ColumnRef(name="status")
        assert ref.model is None
        assert ref.name == "status"

    def test_explicit_model_not_overwritten(self) -> None:
        """If model is explicitly provided, dotted parsing is skipped."""
        ref = ColumnRef(model="customers", name="name")
        assert ref.model == "customers"
        assert ref.name == "name"

    def test_from_string_multihop(self) -> None:
        """from_string splits on last dot for multi-hop paths."""
        ref = ColumnRef.from_string("customers.regions.name")
        assert ref.model == "customers.regions"
        assert ref.name == "name"
        assert ref.full_name == "customers.regions.name"

    def test_invalid_name_part_rejected(self) -> None:
        """Name parts must match identifier pattern."""
        with pytest.raises(ValueError):
            ColumnRef(name="123invalid")


class TestSlayerModel:
    def test_get_dimension(self) -> None:
        model = SlayerModel(
            name="test",
            sql_table="t",
            data_source="test",
            dimensions=[Dimension(name="x", type=DataType.STRING)],
        )
        assert model.get_dimension("x") is not None
        assert model.get_dimension("y") is None

    def test_get_measure(self) -> None:
        model = SlayerModel(
            name="test",
            sql_table="t",
            data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        )
        assert model.get_measure("revenue") is not None
        assert model.get_measure("missing") is None

    def test_filter_bare_column_allowed(self) -> None:
        """Bare column names in model filters are valid."""
        model = SlayerModel(
            name="test", sql_table="t", data_source="test",
            filters=["status == 'active'", "amount > 100"],
        )
        assert model.filters == ["status == 'active'", "amount > 100"]

    def test_filter_single_dot_allowed(self) -> None:
        """Single-dot (table.column) references in model filters are valid SQL."""
        model = SlayerModel(
            name="test", sql_table="t", data_source="test",
            filters=["customers.region == 'US'"],
        )
        assert model.filters == ["customers.region == 'US'"]

    def test_filter_double_underscore_allowed(self) -> None:
        """Double-underscore alias references in model filters are valid."""
        model = SlayerModel(
            name="test", sql_table="t", data_source="test",
            filters=["customers__regions.name == 'US'"],
        )
        assert model.filters == ["customers__regions.name == 'US'"]

    def test_filter_multidot_auto_converted(self) -> None:
        """Multi-dot references in model filters are auto-converted to __ syntax."""
        model = SlayerModel(
            name="test", sql_table="t", data_source="test",
            filters=["customers.regions.name == 'US'"],
        )
        assert model.filters == ["customers__regions.name == 'US'"]

    def test_filter_multidot_complex_auto_converted(self) -> None:
        """Multi-dot references are converted even in complex filter expressions."""
        model = SlayerModel(
            name="test", sql_table="t", data_source="test",
            filters=["orders.customers.region == warehouses.stores.region"],
        )
        assert model.filters == ["orders__customers.region == warehouses__stores.region"]

    def test_filter_string_literal_dots_not_converted(self) -> None:
        """Dots inside string literals are not converted."""
        model = SlayerModel(
            name="test", sql_table="t", data_source="test",
            filters=["name == 'foo.bar.baz'"],
        )
        assert model.filters == ["name == 'foo.bar.baz'"]

    def test_dimension_sql_multidot_auto_converted(self) -> None:
        """Multi-dot references in dimension sql are auto-converted."""
        dim = Dimension(name="region_name", sql="customers.regions.name")
        assert dim.sql == "customers__regions.name"

    def test_dimension_sql_single_dot_unchanged(self) -> None:
        """Single-dot references in dimension sql are left as-is."""
        dim = Dimension(name="cust_name", sql="customers.name")
        assert dim.sql == "customers.name"

    def test_measure_sql_multidot_auto_converted(self) -> None:
        """Multi-dot references in measure sql are auto-converted."""
        meas = Measure(name="region_count", sql="customers.regions.id")
        assert meas.sql == "customers__regions.id"

    def test_measure_sql_single_dot_unchanged(self) -> None:
        """Single-dot references in measure sql are left as-is."""
        meas = Measure(name="total", sql="orders.amount")
        assert meas.sql == "orders.amount"

    def test_filter_multidot_three_levels_auto_converted(self) -> None:
        """Three-level multi-dot references are converted correctly."""
        model = SlayerModel(
            name="test", sql_table="t", data_source="test",
            filters=["a.b.c.d == 1"],
        )
        assert model.filters == ["a__b__c.d == 1"]

    def test_model_name_rejects_double_underscore(self) -> None:
        with pytest.raises(ValueError, match="must not contain '__'"):
            SlayerModel(name="my__model", sql_table="t", data_source="test")

    def test_dimension_name_allows_double_underscore(self) -> None:
        """__ is allowed in dimension names — used for flattened join paths in virtual models."""
        dim = Dimension(name="stores__name")
        assert dim.name == "stores__name"

    def test_measure_name_allows_double_underscore(self) -> None:
        """__ is allowed in measure names — used for flattened join paths in virtual models."""
        meas = Measure(name="stores__tax_rate_sum", sql="tax_rate")
        assert meas.name == "stores__tax_rate_sum"

    def test_query_name_rejects_double_underscore(self) -> None:
        with pytest.raises(ValueError, match="must not contain '__'"):
            SlayerQuery(name="my__query", source_model="orders")

    def test_model_name_single_underscore_allowed(self) -> None:
        model = SlayerModel(name="my_model", sql_table="t", data_source="test")
        assert model.name == "my_model"

    def test_dimension_name_single_underscore_allowed(self) -> None:
        dim = Dimension(name="customer_name")
        assert dim.name == "customer_name"

    def test_dimension_name_rejects_dot(self) -> None:
        """Dots are path syntax, not allowed in dimension names."""
        with pytest.raises(ValueError, match="must not contain '.'"):
            Dimension(name="customers.name")

    def test_measure_name_rejects_dot(self) -> None:
        """Dots are path syntax, not allowed in measure names."""
        with pytest.raises(ValueError, match="must not contain '.'"):
            Measure(name="customers.name_sum", sql="name")

    def test_dimension_name_without_dot_allowed(self) -> None:
        dim = Dimension(name="region_name")
        assert dim.name == "region_name"

    def test_measure_name_without_dot_allowed(self) -> None:
        meas = Measure(name="order_total_sum", sql="total")
        assert meas.name == "order_total_sum"


class TestDatasourceConfig:
    def test_postgres_connection_string(self) -> None:
        ds = DatasourceConfig(
            name="test",
            type="postgres",
            host="localhost",
            port=5432,
            database="mydb",
            username="user",
            password="pass",
        )
        cs = ds.get_connection_string()
        assert cs == "postgresql://user:pass@localhost:5432/mydb"

    def test_explicit_connection_string(self) -> None:
        ds = DatasourceConfig(
            name="test",
            connection_string="postgresql://custom@host/db",
        )
        assert ds.get_connection_string() == "postgresql://custom@host/db"

    def test_user_alias(self) -> None:
        ds = DatasourceConfig.model_validate({
            "name": "test", "type": "postgres", "user": "pg", "password": "secret",
        })
        assert ds.username == "pg"
        assert "pg:secret@" in ds.get_connection_string()

    def test_sqlite_connection_string(self) -> None:
        ds = DatasourceConfig(name="test", type="sqlite", database="/tmp/test.db")
        assert ds.get_connection_string() == "sqlite:////tmp/test.db"


class TestDataType:
    def test_python_type(self) -> None:
        assert DataType.STRING.python_type is str
        assert DataType.COUNT.python_type is int
        assert DataType.SUM.python_type is float


class TestTimeGranularity:
    def test_period_start_week(self) -> None:
        import datetime
        # Wednesday 2024-03-13 -> Monday 2024-03-11
        start = TimeGranularity.WEEK.period_start(datetime.date(2024, 3, 13))
        assert start == datetime.date(2024, 3, 11)

    def test_period_end_month(self) -> None:
        import datetime
        end = TimeGranularity.MONTH.period_end(datetime.date(2024, 3, 15))
        assert end == datetime.date(2024, 3, 31)


class TestStringCoercion:
    """Plain strings are accepted in fields and dimensions lists."""

    def test_fields_plain_strings(self) -> None:
        query = SlayerQuery(source_model="orders", fields=["*:count", "revenue:sum"])
        assert len(query.fields) == 2
        assert query.fields[0] == Field(formula="*:count")
        assert query.fields[1] == Field(formula="revenue:sum")

    def test_dimensions_plain_strings(self) -> None:
        query = SlayerQuery(source_model="orders", dimensions=["status", "customers.name"])
        assert len(query.dimensions) == 2
        assert query.dimensions[0].name == "status"
        assert query.dimensions[0].model is None
        assert query.dimensions[1].name == "name"
        assert query.dimensions[1].model == "customers"

    def test_fields_mixed_strings_and_dicts(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=["*:count", {"formula": "revenue:sum / *:count", "name": "aov"}],
        )
        assert len(query.fields) == 2
        assert query.fields[0] == Field(formula="*:count")
        assert query.fields[1] == Field(formula="revenue:sum / *:count", name="aov")

    def test_dimensions_mixed_strings_and_dicts(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=["status", {"name": "customers.name", "label": "Customer"}],
        )
        assert len(query.dimensions) == 2
        assert query.dimensions[0].name == "status"
        assert query.dimensions[1].name == "name"
        assert query.dimensions[1].model == "customers"
        assert query.dimensions[1].label == "Customer"

    def test_dict_syntax_still_works(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
        )
        assert query.fields[0] == Field(formula="*:count")
        assert query.dimensions[0].name == "status"

    def test_none_fields_and_dimensions(self) -> None:
        query = SlayerQuery(source_model="orders")
        assert query.fields is None
        assert query.dimensions is None

    def test_order_column_string(self) -> None:
        item = OrderItem(column="revenue_sum", direction="desc")
        assert item.column.name == "revenue_sum"
        assert item.column.model is None

    def test_order_column_dotted_string(self) -> None:
        item = OrderItem(column="customers._count", direction="asc")
        assert item.column.name == "_count"
        assert item.column.model == "customers"

    def test_order_column_dict_still_works(self) -> None:
        item = OrderItem(column={"name": "revenue_sum"}, direction="desc")
        assert item.column.name == "revenue_sum"

    def test_time_dimension_string(self) -> None:
        td = TimeDimension(dimension="created_at", granularity="month")
        assert td.dimension.name == "created_at"
        assert td.dimension.model is None

    def test_time_dimension_dotted_string(self) -> None:
        td = TimeDimension(dimension="customers.ordered_at", granularity="month")
        assert td.dimension.name == "ordered_at"
        assert td.dimension.model == "customers"

    def test_time_dimension_dict_still_works(self) -> None:
        td = TimeDimension(dimension={"name": "created_at"}, granularity="month")
        assert td.dimension.name == "created_at"

    def test_query_with_simplified_order_and_time_dimensions(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=["*:count"],
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            order=[{"column": "_count", "direction": "desc"}],
        )
        assert query.time_dimensions[0].dimension.name == "created_at"
        assert query.order[0].column.name == "_count"
        assert query.order[0].direction == "desc"


class TestWholePeriodsOnly:
    def test_adds_lte_filter_when_none(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            whole_periods_only=True,
        )
        snapped = query.snap_to_whole_periods()
        assert len(snapped.filters) == 1
        assert "<=" in snapped.filters[0]

    def test_noop_when_false(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            whole_periods_only=False,
        )
        snapped = query.snap_to_whole_periods()
        assert snapped.filters is None

    def test_period_start_quarter(self) -> None:
        import datetime
        start = TimeGranularity.QUARTER.period_start(datetime.date(2024, 5, 15))
        assert start == datetime.date(2024, 4, 1)


class TestCoerceFieldsAndDimensions:
    """Tests for _coerce_fields and _coerce_dimensions input validation."""

    def test_fields_scalar_string_raises(self) -> None:
        with pytest.raises(Exception, match="must be a list"):
            SlayerQuery(source_model="orders", fields="count")

    def test_dimensions_scalar_string_raises(self) -> None:
        with pytest.raises(Exception, match="must be a list"):
            SlayerQuery(source_model="orders", dimensions="status")

    def test_fields_list_of_strings_coerced(self) -> None:
        q = SlayerQuery(source_model="orders", fields=["*:count", "revenue:sum"])
        assert q.fields[0].formula == "*:count"
        assert q.fields[1].formula == "revenue:sum"

    def test_fields_list_of_dicts_accepted(self) -> None:
        q = SlayerQuery(source_model="orders", fields=[{"formula": "*:count", "name": "cnt"}])
        assert q.fields[0].formula == "*:count"
        assert q.fields[0].name == "cnt"

    def test_dimensions_list_of_strings_coerced(self) -> None:
        q = SlayerQuery(source_model="orders", dimensions=["status", "region"])
        assert q.dimensions[0].name == "status"
        assert q.dimensions[1].name == "region"

    def test_fields_none_accepted(self) -> None:
        q = SlayerQuery(source_model="orders", fields=None)
        assert q.fields is None

    def test_dimensions_none_accepted(self) -> None:
        q = SlayerQuery(source_model="orders", dimensions=None)
        assert q.dimensions is None


class TestAggregationValidation:
    """Aggregation must require formula for non-built-in names."""

    def test_builtin_without_formula_succeeds(self) -> None:
        agg = Aggregation(name="sum")
        assert agg.name == "sum"
        assert agg.formula is None

    def test_builtin_with_formula_succeeds(self) -> None:
        agg = Aggregation(name="sum", formula="SUM({value})")
        assert agg.formula == "SUM({value})"

    def test_custom_with_formula_succeeds(self) -> None:
        agg = Aggregation(name="my_agg", formula="CUSTOM({value})")
        assert agg.name == "my_agg"
        assert agg.formula == "CUSTOM({value})"

    def test_custom_without_formula_raises(self) -> None:
        with pytest.raises(ValueError, match="not a built-in aggregation"):
            Aggregation(name="my_agg")


class TestDimensionLabel:
    def test_label_optional(self) -> None:
        d = Dimension(name="status", sql="status")
        assert d.label is None

    def test_label_set(self) -> None:
        d = Dimension(name="status", sql="status", label="Order Status")
        assert d.label == "Order Status"

    def test_label_in_model_dump(self) -> None:
        d = Dimension(name="status", label="Order Status")
        data = d.model_dump(exclude_none=True)
        assert data["label"] == "Order Status"

    def test_label_excluded_when_none(self) -> None:
        d = Dimension(name="status")
        data = d.model_dump(exclude_none=True)
        assert "label" not in data


class TestMeasureLabel:
    def test_label_optional(self) -> None:
        m = Measure(name="revenue", sql="amount")
        assert m.label is None

    def test_label_set(self) -> None:
        m = Measure(name="revenue", sql="amount", label="Total Revenue")
        assert m.label == "Total Revenue"


class TestMeasureFilter:
    def test_filter_optional(self) -> None:
        m = Measure(name="revenue", sql="amount")
        assert m.filter is None

    def test_filter_set(self) -> None:
        m = Measure(name="active_revenue", sql="amount", filter="status = 'active'")
        assert m.filter == "status = 'active'"

    def test_filter_multidot_autoconvert(self) -> None:
        m = Measure(name="x", sql="amount", filter="a.b.c = 1")
        assert "a__b.c" in m.filter

    def test_filter_in_model_dump(self) -> None:
        m = Measure(name="x", sql="amount", filter="status = 'active'")
        data = m.model_dump(exclude_none=True)
        assert data["filter"] == "status = 'active'"

    def test_filter_excluded_when_none(self) -> None:
        m = Measure(name="x", sql="amount")
        data = m.model_dump(exclude_none=True)
        assert "filter" not in data


class TestSubstituteVariables:
    def test_string_variable(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="status = '{status_val}'",
            variables={"status_val": "active"},
        )
        assert result == "status = 'active'"

    def test_number_variable(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="amount > {min_amount}",
            variables={"min_amount": 100},
        )
        assert result == "amount > 100"

    def test_float_variable(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="rate < {max_rate}",
            variables={"max_rate": 0.05},
        )
        assert result == "rate < 0.05"

    def test_multiple_variables(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="status = '{status}' AND amount > {min}",
            variables={"status": "completed", "min": 50},
        )
        assert result == "status = 'completed' AND amount > 50"

    def test_escaped_braces(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="name LIKE '{{prefix}}%' AND status = '{val}'",
            variables={"val": "ok"},
        )
        assert result == "name LIKE '{prefix}%' AND status = 'ok'"

    def test_undefined_variable_raises(self) -> None:
        from slayer.core.query import substitute_variables

        with pytest.raises(ValueError, match="Undefined variable 'missing'"):
            substitute_variables(
                filter_str="status = '{missing}'",
                variables={},
            )

    def test_invalid_variable_name_raises(self) -> None:
        from slayer.core.query import substitute_variables

        with pytest.raises(ValueError, match="Invalid variable name"):
            substitute_variables(
                filter_str="status = '{bad-name}'",
                variables={"bad-name": "x"},
            )

    def test_invalid_type_raises(self) -> None:
        from slayer.core.query import substitute_variables

        with pytest.raises(ValueError, match="must be a string or number"):
            substitute_variables(
                filter_str="status = '{val}'",
                variables={"val": [1, 2, 3]},
            )

    def test_no_variables_no_change(self) -> None:
        from slayer.core.query import substitute_variables

        result = substitute_variables(
            filter_str="status = 'active'",
            variables={},
        )
        assert result == "status = 'active'"

    def test_variable_in_slayer_query(self) -> None:
        """Variables field is accepted on SlayerQuery."""
        q = SlayerQuery(
            source_model="orders",
            fields=["*:count"],
            filters=["status = '{val}'"],
            variables={"val": "completed"},
        )
        assert q.variables == {"val": "completed"}


class TestStripSourceModelPrefix:
    """strip_source_model_prefix() removes redundant source model name from query references."""

    # --- Dimensions ---

    def test_simple_self_ref_dimension_stripped(self) -> None:
        """orders.status on source_model=orders -> status"""
        q = SlayerQuery(source_model="orders", dimensions=["orders.status"])
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model is None
        assert stripped.dimensions[0].name == "status"
        assert stripped.dimensions[0].full_name == "status"

    def test_cross_model_self_ref_dimension_stripped(self) -> None:
        """orders.customers.name on source_model=orders -> customers.name"""
        q = SlayerQuery(source_model="orders", dimensions=["orders.customers.name"])
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model == "customers"
        assert stripped.dimensions[0].name == "name"

    def test_multihop_self_ref_dimension_stripped(self) -> None:
        """orders.customers.regions.name -> customers.regions.name"""
        q = SlayerQuery(source_model="orders", dimensions=["orders.customers.regions.name"])
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model == "customers.regions"
        assert stripped.dimensions[0].name == "name"

    def test_non_prefixed_dimension_unchanged(self) -> None:
        """customers.name on source_model=orders stays as customers.name"""
        q = SlayerQuery(source_model="orders", dimensions=["customers.name"])
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model == "customers"
        assert stripped.dimensions[0].name == "name"

    def test_local_dimension_unchanged(self) -> None:
        """status on source_model=orders stays as status"""
        q = SlayerQuery(source_model="orders", dimensions=["status"])
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model is None
        assert stripped.dimensions[0].name == "status"

    def test_mixed_dimensions_partial_strip(self) -> None:
        """Only prefixed dimensions are stripped; others unchanged."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=["orders.status", "customers.name", "region"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].full_name == "status"
        assert stripped.dimensions[1].full_name == "customers.name"
        assert stripped.dimensions[2].full_name == "region"

    def test_dimension_label_preserved(self) -> None:
        """Stripping preserves the label on a ColumnRef."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=[{"name": "orders.status", "label": "Status"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model is None
        assert stripped.dimensions[0].name == "status"
        assert stripped.dimensions[0].label == "Status"

    # --- Time dimensions ---

    def test_time_dimension_stripped(self) -> None:
        """orders.created_at on source_model=orders -> created_at"""
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "orders.created_at", "granularity": "month"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.time_dimensions[0].dimension.model is None
        assert stripped.time_dimensions[0].dimension.name == "created_at"

    def test_time_dimension_preserves_other_fields(self) -> None:
        """Granularity, date_range, label are preserved after stripping."""
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[{
                "dimension": "orders.created_at",
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-12-31"],
                "label": "Month",
            }],
        )
        stripped = q.strip_source_model_prefix()
        td = stripped.time_dimensions[0]
        assert td.dimension.name == "created_at"
        assert td.dimension.model is None
        assert td.granularity == TimeGranularity.MONTH
        assert td.date_range == ["2024-01-01", "2024-12-31"]
        assert td.label == "Month"

    def test_time_dimension_cross_model_not_stripped(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "customers.created_at", "granularity": "day"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.time_dimensions[0].dimension.model == "customers"

    # --- Fields (formulas) ---

    def test_formula_self_ref_stripped(self) -> None:
        """orders.revenue:sum -> revenue:sum"""
        q = SlayerQuery(source_model="orders", fields=["orders.revenue:sum"])
        stripped = q.strip_source_model_prefix()
        assert stripped.fields[0].formula == "revenue:sum"

    def test_formula_star_count_stripped(self) -> None:
        """orders.*:count -> *:count"""
        q = SlayerQuery(source_model="orders", fields=["orders.*:count"])
        stripped = q.strip_source_model_prefix()
        assert stripped.fields[0].formula == "*:count"

    def test_formula_arithmetic_stripped(self) -> None:
        """orders.revenue:sum / orders.*:count -> revenue:sum / *:count"""
        q = SlayerQuery(source_model="orders", fields=["orders.revenue:sum / orders.*:count"])
        stripped = q.strip_source_model_prefix()
        assert stripped.fields[0].formula == "revenue:sum / *:count"

    def test_formula_transform_stripped(self) -> None:
        """cumsum(orders.revenue:sum) -> cumsum(revenue:sum)"""
        q = SlayerQuery(source_model="orders", fields=["cumsum(orders.revenue:sum)"])
        stripped = q.strip_source_model_prefix()
        assert stripped.fields[0].formula == "cumsum(revenue:sum)"

    def test_formula_cross_model_self_ref_stripped(self) -> None:
        """orders.customers.score:avg -> customers.score:avg"""
        q = SlayerQuery(source_model="orders", fields=["orders.customers.score:avg"])
        stripped = q.strip_source_model_prefix()
        assert stripped.fields[0].formula == "customers.score:avg"

    def test_formula_cross_model_not_stripped(self) -> None:
        """customers.score:avg on source_model=orders stays unchanged"""
        q = SlayerQuery(source_model="orders", fields=["customers.score:avg"])
        stripped = q.strip_source_model_prefix()
        assert stripped.fields[0].formula == "customers.score:avg"

    def test_formula_name_and_label_preserved(self) -> None:
        """Field name and label are preserved after formula stripping."""
        q = SlayerQuery(
            source_model="orders",
            fields=[{"formula": "orders.revenue:sum", "name": "rev", "label": "Revenue"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.fields[0].formula == "revenue:sum"
        assert stripped.fields[0].name == "rev"
        assert stripped.fields[0].label == "Revenue"

    # --- Filters ---

    def test_filter_self_ref_stripped(self) -> None:
        """orders.status = 'active' -> status = 'active'"""
        q = SlayerQuery(
            source_model="orders",
            fields=["*:count"],
            filters=["orders.status = 'active'"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters[0] == "status = 'active'"

    def test_filter_cross_model_self_ref_stripped(self) -> None:
        """orders.customers.name = 'foo' -> customers.name = 'foo'"""
        q = SlayerQuery(
            source_model="orders",
            fields=["*:count"],
            filters=["orders.customers.name = 'foo'"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters[0] == "customers.name = 'foo'"

    def test_filter_no_prefix_unchanged(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            fields=["*:count"],
            filters=["status = 'active'", "customers.name = 'foo'"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters == ["status = 'active'", "customers.name = 'foo'"]

    def test_filter_with_transform_stripped(self) -> None:
        """change(orders.revenue:sum) > 0 -> change(revenue:sum) > 0"""
        q = SlayerQuery(
            source_model="orders",
            fields=["*:count"],
            filters=["change(orders.revenue:sum) > 0"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters[0] == "change(revenue:sum) > 0"

    # --- Order ---

    def test_order_self_ref_stripped(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            fields=["*:count"],
            order=[{"column": "orders.revenue_sum", "direction": "desc"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.order[0].column.model is None
        assert stripped.order[0].column.name == "revenue_sum"
        assert stripped.order[0].direction == "desc"

    def test_order_no_prefix_unchanged(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            fields=["*:count"],
            order=[{"column": "revenue_sum", "direction": "asc"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.order[0].column.model is None
        assert stripped.order[0].column.name == "revenue_sum"

    # --- main_time_dimension ---

    def test_main_time_dimension_stripped(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            main_time_dimension="orders.created_at",
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.main_time_dimension == "created_at"

    def test_main_time_dimension_no_prefix_unchanged(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            main_time_dimension="created_at",
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.main_time_dimension == "created_at"

    # --- ModelExtension source ---

    def test_model_extension_dict_source(self) -> None:
        """ModelExtension dict with source_name is used for stripping."""
        q = SlayerQuery(
            source_model={"source_name": "orders"},
            dimensions=["orders.status"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model is None
        assert stripped.dimensions[0].name == "status"

    def test_model_extension_object_source(self) -> None:
        """ModelExtension object uses source_name for stripping."""
        from slayer.core.query import ModelExtension

        q = SlayerQuery(
            source_model=ModelExtension(source_name="orders"),
            dimensions=["orders.status"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model is None
        assert stripped.dimensions[0].name == "status"

    def test_inline_model_source(self) -> None:
        """Inline SlayerModel uses .name for stripping."""
        model = SlayerModel(name="orders", sql_table="orders", data_source="test")
        q = SlayerQuery(
            source_model=model,
            dimensions=["orders.status"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions[0].model is None
        assert stripped.dimensions[0].name == "status"

    # --- No-op cases ---

    def test_no_stripping_returns_same_object(self) -> None:
        """When nothing to strip, returns self (no copy)."""
        q = SlayerQuery(source_model="orders", dimensions=["status"])
        stripped = q.strip_source_model_prefix()
        assert stripped is q

    def test_none_fields_handled(self) -> None:
        """All-None optional fields don't crash."""
        q = SlayerQuery(source_model="orders")
        stripped = q.strip_source_model_prefix()
        assert stripped is q

    # --- Word boundary safety ---

    def test_similar_model_name_not_stripped_in_filter(self) -> None:
        """reorders.status should NOT be stripped when source_model=orders"""
        q = SlayerQuery(
            source_model="orders",
            filters=["reorders.status = 'active'"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters[0] == "reorders.status = 'active'"

    def test_similar_model_name_not_stripped_in_formula(self) -> None:
        """reorders.revenue:sum should NOT be stripped when source_model=orders"""
        q = SlayerQuery(
            source_model="orders",
            fields=["reorders.revenue:sum"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.fields[0].formula == "reorders.revenue:sum"
