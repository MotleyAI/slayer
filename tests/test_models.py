"""Tests for core domain models."""

import datetime

import pytest
from pydantic import ValidationError
from sqlalchemy.engine import make_url

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Aggregation, Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension


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


class TestOrderItem:
    """OrderItem column coercion and raw_formula capture."""

    def test_colon_syntax_normalized(self) -> None:
        item = OrderItem(column="revenue:sum", direction="desc")
        assert item.column.name == "revenue_sum"
        assert item.raw_formula == "revenue:sum"

    def test_funcstyle_builtin_agg_normalized(self) -> None:
        """Built-in function-style aggregations get rewritten to colon form."""
        item = OrderItem(column="sum(revenue)", direction="desc")
        assert item.column.name == "revenue_sum"
        assert item.raw_formula == "revenue:sum"

    def test_funcstyle_custom_agg_accepted(self) -> None:
        """Function-style custom aggregations must validate without rejecting on parens.

        Regression: _coerce_order_column previously wrapped the unrewritten string
        as ``{"name": "rolling_avg(revenue)"}``, which ColumnRef's name validator
        rejected. raw_formula is preserved so enrichment can resolve the agg via
        extra_agg_names.
        """
        item = OrderItem(column="rolling_avg(revenue)", direction="desc")
        assert item.raw_formula == "rolling_avg(revenue)"

    def test_funcstyle_custom_agg_with_args_accepted(self) -> None:
        """Function-style custom agg with args is captured as raw_formula."""
        item = OrderItem(column="rolling_avg(revenue, window=7)", direction="asc")
        assert item.raw_formula == "rolling_avg(revenue, window=7)"

    def test_plain_column_no_raw_formula(self) -> None:
        """A plain column reference has no raw_formula."""
        item = OrderItem(column="status", direction="asc")
        assert item.column.name == "status"
        assert item.raw_formula is None


class TestOrderShorthandHealing:
    """DEV-1575: heal LLM-shorthand ``order`` items and harden direction validation.

    Shorthand ``{col: "desc"}`` (single- or multi-key) heals to canonical
    ``{"column": col, "direction": "desc"}``; direction is normalized
    (case-insensitive, ``asc``/``desc``/``ascending``/``descending``, whitespace
    trimmed) and anything else is rejected; ``OrderItem`` forbids extra keys.
    """

    # --- single-key shorthand ---

    def test_shorthand_desc(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"avg_processing_time_days": "desc"}])
        assert len(q.order) == 1
        assert q.order[0].column.name == "avg_processing_time_days"
        assert q.order[0].direction == "desc"

    def test_shorthand_asc(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"status": "asc"}])
        assert q.order[0].column.name == "status"
        assert q.order[0].direction == "asc"

    def test_shorthand_equals_canonical(self) -> None:
        shorthand = SlayerQuery(source_model="orders", order=[{"status": "desc"}])
        canonical = SlayerQuery(
            source_model="orders", order=[{"column": "status", "direction": "desc"}]
        )
        assert shorthand.order == canonical.order

    # --- direction vocabulary: case-insensitive, synonyms, whitespace ---

    def test_shorthand_uppercase_normalized(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"status": "DESC"}])
        assert q.order[0].direction == "desc"

    def test_shorthand_descending_synonym(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"status": "DESCENDING"}])
        assert q.order[0].direction == "desc"

    def test_shorthand_ascending_synonym_mixed_case(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"status": "Ascending"}])
        assert q.order[0].direction == "asc"

    def test_shorthand_whitespace_trimmed(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"status": " Asc "}])
        assert q.order[0].direction == "asc"

    # --- formula key composes with existing normalization ---

    def test_shorthand_formula_key(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"revenue:sum": "desc"}])
        assert q.order[0].column.name == "revenue_sum"
        assert q.order[0].raw_formula == "revenue:sum"
        assert q.order[0].direction == "desc"

    def test_shorthand_formula_key_equals_canonical(self) -> None:
        shorthand = SlayerQuery(source_model="orders", order=[{"revenue:sum": "desc"}])
        canonical = SlayerQuery(
            source_model="orders", order=[{"column": "revenue:sum", "direction": "desc"}]
        )
        assert shorthand.order == canonical.order

    # --- multi-key expansion (one dict -> N OrderItems, insertion order) ---

    def test_multikey_expands_in_order(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"a": "desc", "b": "asc"}])
        assert [(o.column.name, o.direction) for o in q.order] == [("a", "desc"), ("b", "asc")]

    def test_multikey_synonyms_and_case(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"a": "ASCENDING", "b": "Desc"}])
        assert [(o.column.name, o.direction) for o in q.order] == [("a", "asc"), ("b", "desc")]

    def test_multikey_partial_non_direction_raises(self) -> None:
        # Not every value is a direction -> not shorthand -> canonical validation rejects.
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[{"a": "desc", "b": "downward"}])

    # --- bare single item passed without the enclosing list gets wrapped ---

    def test_bare_dict_shorthand_wrapped(self) -> None:
        q = SlayerQuery(source_model="orders", order={"status": "desc"})
        assert len(q.order) == 1
        assert q.order[0].column.name == "status"
        assert q.order[0].direction == "desc"

    def test_bare_canonical_dict_wrapped(self) -> None:
        q = SlayerQuery(source_model="orders", order={"column": "status", "direction": "asc"})
        assert len(q.order) == 1
        assert q.order[0].column.name == "status"
        assert q.order[0].direction == "asc"

    def test_bare_multikey_dict_wrapped(self) -> None:
        q = SlayerQuery(source_model="orders", order={"a": "desc", "b": "asc"})
        assert [(o.column.name, o.direction) for o in q.order] == [("a", "desc"), ("b", "asc")]

    def test_bare_orderitem_wrapped(self) -> None:
        item = OrderItem(column="status", direction="desc")
        q = SlayerQuery(source_model="orders", order=item)
        assert len(q.order) == 1
        assert q.order[0].column.name == "status"
        assert q.order[0].direction == "desc"

    # --- canonical preserved; latent uppercase-direction bug fixed ---

    def test_canonical_unchanged(self) -> None:
        q = SlayerQuery(source_model="orders", order=[{"column": "status", "direction": "desc"}])
        assert q.order[0].column.name == "status"
        assert q.order[0].direction == "desc"

    def test_canonical_uppercase_direction_normalized(self) -> None:
        # Pre-existing latent bug: SQL generator compares ``direction == "asc"`` strictly,
        # so a stored "ASCENDING"/"ASC" used to silently emit DESC. Now normalized.
        q = SlayerQuery(
            source_model="orders", order=[{"column": "status", "direction": "ASCENDING"}]
        )
        assert q.order[0].direction == "asc"

    def test_canonical_column_named_like_direction_word(self) -> None:
        # Single reserved 'column' key whose value happens to be a direction word
        # stays canonical (order by a column literally named "desc").
        q = SlayerQuery(source_model="orders", order=[{"column": "desc"}])
        assert q.order[0].column.name == "desc"
        assert q.order[0].direction == "asc"

    # --- rejections: malformed / ambiguous never silently mangled ---

    def test_invalid_direction_value_shorthand_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[{"col": "downward"}])

    def test_invalid_direction_value_canonical_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[{"column": "status", "direction": "sideways"}])

    def test_mixed_canonical_and_stray_direction_key_raises(self) -> None:
        # extra='forbid' rejects the stray 'b' rather than silently dropping it.
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[{"column": "status", "b": "asc"}])

    def test_mixed_canonical_and_stray_nondirection_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[{"column": "status", "b": "downward"}])

    def test_direction_only_key_raises(self) -> None:
        # {"direction": "desc"} is a malformed canonical item (no column), not shorthand.
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[{"direction": "desc"}])

    def test_empty_dict_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[{}])

    def test_nested_list_item_raises(self) -> None:
        # No list-of-lists support.
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[["a", "desc"]])

    def test_non_wrappable_scalar_string_raises(self) -> None:
        with pytest.raises(Exception, match="must be a list"):
            SlayerQuery(source_model="orders", order="status")

    def test_non_wrappable_int_raises(self) -> None:
        with pytest.raises(Exception, match="must be a list"):
            SlayerQuery(source_model="orders", order=42)

    # --- mixed list: shorthand + canonical + multi-key together ---

    def test_mixed_shorthand_canonical_multikey_list(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            order=[
                {"a": "desc"},
                {"column": "b", "direction": "asc"},
                {"c": "asc", "d": "desc"},
            ],
        )
        assert [(o.column.name, o.direction) for o in q.order] == [
            ("a", "desc"),
            ("b", "asc"),
            ("c", "asc"),
            ("d", "desc"),
        ]

    # --- None / tuple / non-string keys / passthrough ---

    def test_order_none_stays_none(self) -> None:
        q = SlayerQuery(source_model="orders", order=None)
        assert q.order is None

    def test_tuple_input_accepted(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            order=({"a": "desc"}, {"column": "b", "direction": "asc"}),
        )
        assert [(o.column.name, o.direction) for o in q.order] == [("a", "desc"), ("b", "asc")]

    def test_non_string_key_not_healed_raises(self) -> None:
        # A non-string key means the dict is not treated as shorthand; it falls
        # through to OrderItem validation, which rejects it (no column).
        with pytest.raises(ValidationError):
            SlayerQuery(source_model="orders", order=[{1: "desc"}])

    def test_multikey_formula_keys_preserve_raw_formula(self) -> None:
        q = SlayerQuery(
            source_model="orders", order=[{"revenue:sum": "desc", "amount:max": "asc"}]
        )
        assert len(q.order) == 2
        assert q.order[0].column.name == "revenue_sum"
        assert q.order[0].raw_formula == "revenue:sum"
        assert q.order[0].direction == "desc"
        assert q.order[1].column.name == "amount_max"
        assert q.order[1].raw_formula == "amount:max"
        assert q.order[1].direction == "asc"

    def test_list_orderitem_passthrough(self) -> None:
        item = OrderItem(column="revenue:sum", direction="desc")
        q = SlayerQuery(source_model="orders", order=[item])
        assert len(q.order) == 1
        assert q.order[0].column.name == "revenue_sum"
        assert q.order[0].direction == "desc"

    # --- OrderItem-level direction validation + extra=forbid ---

    def test_orderitem_direction_synonym_normalized(self) -> None:
        assert OrderItem(column="status", direction="DESCENDING").direction == "desc"

    def test_orderitem_direction_default_asc(self) -> None:
        assert OrderItem(column="status").direction == "asc"

    def test_orderitem_invalid_direction_raises(self) -> None:
        with pytest.raises(ValidationError):
            OrderItem(column="status", direction="sideways")

    def test_orderitem_extra_key_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            OrderItem(column="status", direction="asc", bogus="x")


class TestSlayerModel:
    def test_get_dimension(self) -> None:
        model = SlayerModel(
            name="test",
            sql_table="t",
            data_source="test",
            columns=[Column(name="x", type=DataType.TEXT)],
        )
        assert model.get_column("x") is not None
        assert model.get_column("y") is None

    def test_get_measure(self) -> None:
        """``get_measure`` returns a ModelMeasure formula by name."""
        model = SlayerModel(
            name="test",
            sql_table="t",
            data_source="test",
            measures=[ModelMeasure(name="aov", formula="revenue:sum / *:count")],
        )
        assert model.get_measure("aov") is not None
        assert model.get_measure("missing") is None

    def test_model_measure_name_cannot_shadow_transform(self) -> None:
        """A ``ModelMeasure`` named after a built-in transform (``cumsum`` etc.)
        would shadow the transform in formulas like ``cumsum(...)``. Reject at
        model-validation time.
        """
        with pytest.raises(ValueError, match="reserved"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
                measures=[ModelMeasure(name="cumsum", formula="revenue:sum")],
            )

    def test_model_measure_formula_with_raw_over_raises(self) -> None:
        """DEV-1336: a ``ModelMeasure`` formula containing raw `OVER (...)` SQL
        cannot be parsed by SLayer's formula grammar (Python AST) and would
        produce invalid SQL on every dialect if used as a filter. Reject at
        construction time with an actionable error pointing at SLayer's
        ``rank()`` / ``first()`` / ``last()`` / ``lag()`` / ``lead()`` transforms
        or a ``Column.sql``-with-window pattern.
        """
        with pytest.raises(ValueError) as excinfo:
            ModelMeasure(
                name="top_3",
                formula="row_number() over (order by mass desc) <= 3",
            )
        msg = str(excinfo.value)
        assert "window function" in msg.lower(), msg
        assert any(
            keyword in msg
            for keyword in ("rank(", "first(", "last(", "lag(", "lead(", "Column.sql", "multi-stage")
        ), msg

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
        dim = Column(name="region_name", sql="customers.regions.name", type=DataType.DOUBLE)
        assert dim.sql == "customers__regions.name"

    def test_dimension_sql_single_dot_unchanged(self) -> None:
        """Single-dot references in dimension sql are left as-is."""
        dim = Column(name="cust_name", sql="customers.name", type=DataType.DOUBLE)
        assert dim.sql == "customers.name"

    def test_measure_sql_multidot_auto_converted(self) -> None:
        """Multi-dot references in measure sql are auto-converted."""
        meas = Column(name="region_count", sql="customers.regions.id", type=DataType.DOUBLE)
        assert meas.sql == "customers__regions.id"

    def test_measure_sql_single_dot_unchanged(self) -> None:
        """Single-dot references in measure sql are left as-is."""
        meas = Column(name="total", sql="orders.amount", type=DataType.DOUBLE)
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
        dim = Column(name="stores__name")
        assert dim.name == "stores__name"

    def test_measure_name_allows_double_underscore(self) -> None:
        """__ is allowed in measure names — used for flattened join paths in virtual models."""
        meas = Column(name="stores__tax_rate_sum", sql="tax_rate", type=DataType.DOUBLE)
        assert meas.name == "stores__tax_rate_sum"

    def test_query_name_rejects_double_underscore(self) -> None:
        with pytest.raises(ValueError, match="must not contain '__'"):
            SlayerQuery(name="my__query", source_model="orders")

    def test_model_name_single_underscore_allowed(self) -> None:
        model = SlayerModel(name="my_model", sql_table="t", data_source="test")
        assert model.name == "my_model"

    def test_dimension_name_single_underscore_allowed(self) -> None:
        dim = Column(name="customer_name")
        assert dim.name == "customer_name"

    def test_dimension_name_rejects_dot(self) -> None:
        """Dots are path syntax, not allowed in dimension names."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            Column(name="customers.name")

    def test_measure_name_rejects_dot(self) -> None:
        """Dots are path syntax, not allowed in measure names."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            Column(name="customers.name_sum", sql="name", type=DataType.DOUBLE)

    def test_dimension_name_without_dot_allowed(self) -> None:
        dim = Column(name="region_name")
        assert dim.name == "region_name"

    def test_measure_name_without_dot_allowed(self) -> None:
        meas = Column(name="order_total_sum", sql="total", type=DataType.DOUBLE)
        assert meas.name == "order_total_sum"

    def test_model_data_source_rejects_dot(self) -> None:
        """DEV-1405: dots in data_source would let a sibling datasource
        ``prod.legacy`` collide with cascade-delete of ``prod``."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            SlayerModel(
                name="orders", sql_table="t", data_source="prod.legacy",
            )

    def test_datasource_name_rejects_dot(self) -> None:
        """DEV-1405: dots in DatasourceConfig.name break canonical-id
        namespace boundaries."""
        from slayer.core.models import DatasourceConfig
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            DatasourceConfig(name="prod.legacy", type="postgres")

    def test_datasource_name_allows_double_underscore(self) -> None:
        """``__`` is reserved as a SQL join-path alias separator on
        model/query names, but datasource names never appear in SQL
        alias positions, so they accept ``__`` freely."""
        from slayer.core.models import DatasourceConfig
        ds = DatasourceConfig(name="prod__staging", type="postgres")
        assert ds.name == "prod__staging"

    def test_datasource_name_rejects_path_separator(self) -> None:
        from slayer.core.models import DatasourceConfig
        with pytest.raises(ValueError, match="must not contain '/'"):
            DatasourceConfig(name="prod/legacy", type="postgres")

    def test_datasource_name_rejects_empty(self) -> None:
        from slayer.core.models import DatasourceConfig
        with pytest.raises(ValueError, match="non-empty string"):
            DatasourceConfig(name="", type="postgres")

    def test_datasource_name_rejects_colon(self) -> None:
        """Colon is reserved as the DSL aggregation separator
        (``revenue:sum``) and the ``memory:<int>`` canonical-id prefix.
        Allowing it in a datasource name would let ``memory:42`` collide
        with the memory canonical-id namespace."""
        from slayer.core.models import DatasourceConfig
        with pytest.raises(ValueError, match="must not contain ':'"):
            DatasourceConfig(name="memory:42", type="sqlite")

    def test_model_data_source_rejects_colon(self) -> None:
        """A model's ``data_source`` shares the same canonical-id
        namespace constraints as ``DatasourceConfig.name``."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerModel(name="orders", sql_table="t", data_source="memory:42")

    def test_model_name_rejects_colon(self) -> None:
        """Colon is reserved as the DSL aggregation separator
        (``revenue:sum``) — model names sharing the shape would collide
        with formula parsing."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerModel(name="rev:sum", sql_table="t", data_source="ds")

    def test_query_name_rejects_colon(self) -> None:
        """SlayerQuery names share the same naming space as SlayerModel
        names (a query can be persisted as a query-backed model), so the
        same rejection rules apply."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerQuery(name="rev:sum", source_model="orders")

    def test_query_name_rejects_dot(self) -> None:
        """Dotted SlayerQuery names would collide with the dotted-path
        reference syntax used in queries."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            SlayerQuery(name="prod.summary", source_model="orders")

    def test_column_name_rejects_colon(self) -> None:
        """Column names containing ``:`` would collide with the
        aggregation colon syntax (``revenue:sum``)."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            Column(name="rev:sum")


class TestWithinListDuplicateNames:
    """Duplicate names within ``columns`` or within ``measures`` are rejected.

    Codex's Major 4: today only cross-list overlap is validated, so two
    columns named ``revenue`` (or two measures named ``aov``) silently pass
    and ``get_column`` / ``get_measure`` return the first match.
    """

    def test_duplicate_column_names_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate.*column|column.*duplicate"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                columns=[
                    Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                    Column(name="revenue", sql="net_amount", type=DataType.DOUBLE),
                ],
            )

    def test_duplicate_measure_names_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate.*measure|measure.*duplicate"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
                measures=[
                    ModelMeasure(name="aov", formula="amount:sum / *:count"),
                    ModelMeasure(name="aov", formula="amount:avg"),
                ],
            )

    def test_unnamed_model_measure_rejected(self) -> None:
        """Every ``ModelMeasure`` stored on a ``SlayerModel`` must have a name.

        Unnamed entries are unreachable via ``get_measure()`` and bare-name
        expansion, so persisting them is meaningless. (Inline measures on
        ``SlayerQuery.measures`` may still be unnamed — only model-level
        measures are required to be named.)
        """
        with pytest.raises(ValueError, match="must have a name"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
                measures=[ModelMeasure(formula="amount:sum")],
            )

    def test_unique_names_accepted(self) -> None:
        model = SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            columns=[
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
            measures=[
                ModelMeasure(name="aov", formula="amount:sum / *:count"),
                ModelMeasure(name="rev", formula="amount:sum"),
            ],
        )
        assert len(model.columns) == 2
        assert len(model.measures) == 2


class TestSourceModeExclusivity:
    """Source-mode exclusivity: exactly one of sql_table, sql, or
    source_queries must be populated.
    """

    def test_accepts_sql_table_only(self) -> None:
        m = SlayerModel(name="orders", sql_table="orders_t", data_source="ds")
        assert m.sql_table == "orders_t"
        assert m.sql is None
        assert m.source_queries is None

    def test_accepts_sql_only(self) -> None:
        m = SlayerModel(name="orders", sql="SELECT 1 AS x", data_source="ds")
        assert m.sql == "SELECT 1 AS x"
        assert m.sql_table is None

    def test_accepts_source_queries_only(self) -> None:
        m = SlayerModel(
            name="saved",
            data_source="ds",
            source_queries=[SlayerQuery(source_model="orders")],
        )
        assert m.source_queries is not None and len(m.source_queries) == 1

    def test_rejects_no_source(self) -> None:
        with pytest.raises(ValueError, match="exactly one source.*none specified"):
            SlayerModel(name="orders", data_source="ds")

    def test_rejects_sql_table_plus_sql(self) -> None:
        with pytest.raises(ValueError, match="exactly one source"):
            SlayerModel(
                name="orders", sql_table="t", sql="SELECT 1", data_source="ds"
            )

    def test_rejects_sql_table_plus_source_queries(self) -> None:
        with pytest.raises(ValueError, match="exactly one source"):
            SlayerModel(
                name="orders",
                sql_table="t",
                source_queries=[SlayerQuery(source_model="orders")],
                data_source="ds",
            )

    def test_rejects_sql_plus_source_queries(self) -> None:
        with pytest.raises(ValueError, match="exactly one source"):
            SlayerModel(
                name="orders",
                sql="SELECT 1",
                source_queries=[SlayerQuery(source_model="orders")],
                data_source="ds",
            )

    def test_rejects_all_three(self) -> None:
        with pytest.raises(ValueError, match="exactly one source"):
            SlayerModel(
                name="orders",
                sql_table="t",
                sql="SELECT 1",
                source_queries=[SlayerQuery(source_model="orders")],
                data_source="ds",
            )

    def test_rejects_empty_source_queries_list(self) -> None:
        with pytest.raises(ValueError, match="cannot be an empty list"):
            SlayerModel(name="orders", source_queries=[], data_source="ds")


class TestSourceQueryStages:
    """Stage-name rules on ``source_queries``."""

    def test_single_unnamed_stage_accepted(self) -> None:
        """Single (final) stage may be unnamed."""
        m = SlayerModel(
            name="saved",
            data_source="ds",
            source_queries=[SlayerQuery(source_model="orders")],
        )
        assert m.source_queries is not None

    def test_unnamed_non_final_stage_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-final stage at index 0.*must have a 'name'"):
            SlayerModel(
                name="saved",
                data_source="ds",
                source_queries=[
                    SlayerQuery(source_model="orders"),
                    SlayerQuery(source_model="orders"),
                ],
            )

    def test_named_non_final_stage_with_unnamed_final_accepted(self) -> None:
        m = SlayerModel(
            name="saved",
            data_source="ds",
            source_queries=[
                SlayerQuery(name="stage1", source_model="orders"),
                SlayerQuery(source_model="stage1"),
            ],
        )
        assert m.source_queries is not None and len(m.source_queries) == 2

    def test_duplicate_stage_names_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate stage name"):
            SlayerModel(
                name="saved",
                data_source="ds",
                source_queries=[
                    SlayerQuery(name="dup", source_model="orders"),
                    SlayerQuery(name="dup", source_model="orders"),
                ],
            )

    def test_dicts_parsed_to_slayer_query(self) -> None:
        """source_queries entries given as dicts are parsed into SlayerQuery
        instances by the before-validator."""
        m = SlayerModel(
            name="saved",
            data_source="ds",
            source_queries=[
                {"source_model": "orders", "measures": [{"formula": "*:count"}]}
            ],
        )
        assert m.source_queries is not None
        assert isinstance(m.source_queries[0], SlayerQuery)
        assert m.source_queries[0].measures is not None
        assert m.source_queries[0].measures[0].formula == "*:count"

    def test_invalid_entry_type_rejected(self) -> None:
        with pytest.raises((ValueError, TypeError)):
            SlayerModel(
                name="saved",
                data_source="ds",
                source_queries=[123],  # type: ignore[list-item]
            )

    def test_invalid_entry_raises_pydantic_validation_error(self) -> None:
        """``source_queries`` is a public input surface (REST/MCP/CLI). Bad
        items must surface as a Pydantic ``ValidationError`` (wraps
        ``ValueError``), not a raw ``TypeError`` traceback that escapes the
        validator.
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SlayerModel(
                name="saved",
                data_source="ds",
                source_queries=[123],  # type: ignore[list-item]
            )

    def test_non_list_input_raises_pydantic_validation_error(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SlayerModel.model_validate({
                "name": "saved",
                "data_source": "ds",
                "source_queries": "not a list",
            })


class TestQueryVariablesAndCacheFields:
    """``query_variables`` and ``backing_query_sql`` defaults and shape."""

    def test_query_variables_default_is_empty_dict(self) -> None:
        m = SlayerModel(name="orders", sql_table="t", data_source="ds")
        assert m.query_variables == {}

    def test_query_variables_persists_user_value(self) -> None:
        m = SlayerModel(
            name="saved",
            data_source="ds",
            source_queries=[SlayerQuery(source_model="orders")],
            query_variables={"threshold": 1500, "region": "US"},
        )
        assert m.query_variables == {"threshold": 1500, "region": "US"}

    def test_backing_query_sql_default_is_none(self) -> None:
        m = SlayerModel(name="orders", sql_table="t", data_source="ds")
        assert m.backing_query_sql is None


class TestAllowedAggregationsBuildTimeValidation:
    """Build-time validation of ``Column.allowed_aggregations``.

    The intersection contract: a whitelist entry must satisfy
    (1) the PK rule (only ``count`` / ``count_distinct`` for PKs),
    (2) type-default eligibility (``DEFAULT_AGGREGATIONS_BY_TYPE[col.type]``).
    Custom aggregations defined on the model bypass type-default eligibility
    (their formula determines applicability), but PK restrictions still apply
    and the name must be a known custom aggregation.
    """

    def test_pk_column_with_disallowed_aggregation_in_whitelist_rejected(self) -> None:
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                columns=[
                    Column(
                        name="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                        allowed_aggregations=["sum"],
                    ),
                ],
            )

    def test_pk_column_with_count_only_whitelist_accepted(self) -> None:
        model = SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            columns=[
                Column(
                    name="id",
                    type=DataType.DOUBLE,
                    primary_key=True,
                    allowed_aggregations=["count", "count_distinct"],
                ),
            ],
        )
        assert model.columns[0].allowed_aggregations == ["count", "count_distinct"]

    def test_string_column_with_sum_in_whitelist_rejected(self) -> None:
        with pytest.raises(ValueError, match="not applicable|string"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                columns=[
                    Column(
                        name="status",
                        type=DataType.TEXT,
                        allowed_aggregations=["sum"],
                    ),
                ],
            )

    def test_string_column_with_min_max_in_whitelist_accepted(self) -> None:
        """String ``min``/``max`` are intentionally type-eligible."""
        model = SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            columns=[
                Column(
                    name="status",
                    type=DataType.TEXT,
                    allowed_aggregations=["min", "max", "count"],
                ),
            ],
        )
        assert "min" in (model.columns[0].allowed_aggregations or [])
        assert "max" in (model.columns[0].allowed_aggregations or [])

    def test_date_column_with_min_max_accepted(self) -> None:
        model = SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            columns=[
                Column(
                    name="ordered_on",
                    type=DataType.DATE,
                    allowed_aggregations=["min", "max"],
                ),
            ],
        )
        assert model.columns[0].allowed_aggregations == ["min", "max"]

    def test_timestamp_column_with_min_max_accepted(self) -> None:
        model = SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            columns=[
                Column(
                    name="ordered_at",
                    type=DataType.TIMESTAMP,
                    allowed_aggregations=["min", "max"],
                ),
            ],
        )
        assert model.columns[0].allowed_aggregations == ["min", "max"]

    def test_custom_aggregation_in_whitelist_bypasses_type_check(self) -> None:
        """A custom-aggregation entry is exempt from type-default eligibility.
        Its applicability depends on the custom formula, not the type-default map.
        """
        model = SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            aggregations=[
                Aggregation(
                    name="custom_concat",
                    formula="STRING_AGG({value}, ',')",
                ),
            ],
            columns=[
                Column(
                    name="status",
                    type=DataType.TEXT,
                    allowed_aggregations=["custom_concat"],
                ),
            ],
        )
        assert model.columns[0].allowed_aggregations == ["custom_concat"]

    def test_unknown_aggregation_in_whitelist_still_rejected(self) -> None:
        """Existing behavior — keep it."""
        with pytest.raises(ValueError, match="not a built-in aggregation"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                columns=[
                    Column(
                        name="revenue",
                        type=DataType.DOUBLE,
                        allowed_aggregations=["bogus_agg"],
                    ),
                ],
            )

    def test_builtin_override_still_type_gated(self) -> None:
        """Overriding a built-in name (e.g., ``sum``) with a custom formula must
        still respect type-default eligibility — built-ins keep their type
        semantics regardless of the override formula. Only truly novel custom
        names bypass the type-default gate.
        """
        with pytest.raises(ValueError, match="not applicable|string"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                aggregations=[
                    Aggregation(name="sum", formula="STRING_AGG({value}, ',')"),
                ],
                columns=[
                    Column(
                        name="status",
                        type=DataType.TEXT,
                        allowed_aggregations=["sum"],
                    ),
                ],
            )

    def test_pk_column_with_custom_agg_rejected(self) -> None:
        """Even custom aggregations cannot be whitelisted on a PK column —
        the PK rule (count/count_distinct only) is absolute.
        """
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            SlayerModel(
                name="orders",
                sql_table="t",
                data_source="test",
                aggregations=[
                    Aggregation(name="custom_sum", formula="SUM({value})"),
                ],
                columns=[
                    Column(
                        name="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                        allowed_aggregations=["custom_sum"],
                    ),
                ],
            )


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

    @pytest.mark.parametrize("datasource_type", ["postgres", "mysql", "clickhouse"])
    def test_connection_string_encodes_reserved_password_characters(
        self,
        datasource_type: str,
    ) -> None:
        ds = DatasourceConfig(
            name="test",
            type=datasource_type,
            host="db.example",
            port=5432,
            database="analytics",
            username="user",
            password="p@ss/w:rd",  # NOSONAR(S2068) — test-only fixture credential, not a real secret
        )

        url = make_url(ds.get_connection_string())

        assert (url.username, url.password, url.host, url.port, url.database) == (
            "user",
            "p@ss/w:rd",
            "db.example",
            5432,
            "analytics",
        )

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

    def test_sqlserver_connection_string_uses_pyodbc_driver(self) -> None:
        ds = DatasourceConfig(
            name="test",
            type="mssql",
            host="localhost",
            port=1433,
            database="mydb",
            username="sa",
            password="Secret!123",  # NOSONAR(S2068) — test-only fixture credential, not a real secret
        )
        cs = ds.get_connection_string()
        assert cs.startswith("mssql+pyodbc://")
        assert "localhost" in cs
        assert "mydb" in cs

    def test_sqlserver_connection_string_includes_odbc_driver_param(self) -> None:
        ds = DatasourceConfig(name="test", type="mssql", host="sqlhost", database="db")
        cs = ds.get_connection_string()
        assert "driver=ODBC+Driver+18+for+SQL+Server" in cs

    def test_sqlserver_connection_string_includes_trust_server_cert(self) -> None:
        ds = DatasourceConfig(name="test", type="mssql", host="sqlhost", database="db")
        cs = ds.get_connection_string()
        # Required for self-signed certs in Docker dev environments; must be lowercase=yes
        cs_lower = cs.lower()
        assert "trustservercertificate" in cs_lower
        assert "yes" in cs_lower

    def test_sqlserver_type_alias_sqlserver(self) -> None:
        """'sqlserver' alias gets pyodbc driver and TrustServerCertificate params."""
        ds = DatasourceConfig(name="test", type="sqlserver", host="h", database="db")
        cs = ds.get_connection_string()
        assert cs.startswith("mssql+pyodbc://")
        assert "trustservercertificate" in cs.lower()
        assert "odbc" in cs.lower()

    def test_sqlserver_type_alias_tsql(self) -> None:
        """'tsql' alias gets pyodbc driver and TrustServerCertificate params."""
        ds = DatasourceConfig(name="test", type="tsql", host="h", database="db")
        cs = ds.get_connection_string()
        assert cs.startswith("mssql+pyodbc://")
        assert "trustservercertificate" in cs.lower()
        assert "odbc" in cs.lower()

    def test_sqlserver_with_port(self) -> None:
        ds = DatasourceConfig(
            name="test", type="mssql", host="sqlhost", port=1433, database="mydb",
        )
        cs = ds.get_connection_string()
        assert "1433" in cs

    def test_sqlserver_special_chars_in_password_are_url_encoded(self) -> None:
        """Passwords with '@' must not break URL parsing (the Docker example uses 'YourStrong@Passw0rd')."""
        ds = DatasourceConfig(
            name="test",
            type="mssql",
            host="sqlserver",
            port=1433,
            database="slayer_demo",
            username="sa",
            password="YourStrong@Passw0rd",  # NOSONAR(S2068) — test-only fixture credential, not a real secret
        )
        cs = ds.get_connection_string()
        assert "@Passw0rd" not in cs, "raw '@' in password must be percent-encoded"
        assert "%40" in cs, "the '@' in password must appear as %40"
        assert "sqlserver" in cs
        assert "slayer_demo" in cs


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

    # DEV-1572: WEEK_SUNDAY — Sunday-anchored week. period_start rounds back to
    # the Sunday at or before the date; period_end is the following Saturday.
    # 2024-01-07 is a Sunday; 2024-01-08..13 are Mon..Sat of that week, all of
    # which belong to the Sunday-week [2024-01-07 .. 2024-01-13].
    @pytest.mark.parametrize(
        "day,expected_start",
        [
            (7, datetime.date(2024, 1, 7)),   # Sunday  -> itself
            (8, datetime.date(2024, 1, 7)),   # Monday
            (9, datetime.date(2024, 1, 7)),   # Tuesday
            (10, datetime.date(2024, 1, 7)),  # Wednesday
            (11, datetime.date(2024, 1, 7)),  # Thursday
            (12, datetime.date(2024, 1, 7)),  # Friday
            (13, datetime.date(2024, 1, 7)),  # Saturday
        ],
    )
    def test_period_start_week_sunday_each_weekday(
        self, day: int, expected_start: datetime.date
    ) -> None:
        start = TimeGranularity.WEEK_SUNDAY.period_start(datetime.date(2024, 1, day))
        assert start == expected_start

    @pytest.mark.parametrize(
        "day,expected_end",
        [
            (7, datetime.date(2024, 1, 13)),   # Sunday
            (8, datetime.date(2024, 1, 13)),   # Monday
            (9, datetime.date(2024, 1, 13)),   # Tuesday
            (10, datetime.date(2024, 1, 13)),  # Wednesday
            (11, datetime.date(2024, 1, 13)),  # Thursday
            (12, datetime.date(2024, 1, 13)),  # Friday
            (13, datetime.date(2024, 1, 13)),  # Saturday -> itself
        ],
    )
    def test_period_end_week_sunday_each_weekday(
        self, day: int, expected_end: datetime.date
    ) -> None:
        end = TimeGranularity.WEEK_SUNDAY.period_end(datetime.date(2024, 1, day))
        assert end == expected_end

    def test_period_start_week_sunday_crosses_year_boundary(self) -> None:
        # 2024-01-01 is a Monday; its Sunday-week starts on 2023-12-31.
        start = TimeGranularity.WEEK_SUNDAY.period_start(datetime.date(2024, 1, 1))
        assert start == datetime.date(2023, 12, 31)

    def test_period_end_week_sunday_crosses_year_boundary(self) -> None:
        # 2024-12-30 is a Monday; its Sunday-week ends on Saturday 2025-01-04.
        end = TimeGranularity.WEEK_SUNDAY.period_end(datetime.date(2024, 12, 30))
        assert end == datetime.date(2025, 1, 4)


class TestStringCoercion:
    """Plain strings are accepted in fields and dimensions lists."""

    def test_fields_plain_strings(self) -> None:
        query = SlayerQuery(source_model="orders", measures=["*:count", "revenue:sum"])
        assert len(query.measures) == 2
        assert query.measures[0] == ModelMeasure(formula="*:count")
        assert query.measures[1] == ModelMeasure(formula="revenue:sum")

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
            measures=["*:count", {"formula": "revenue:sum / *:count", "name": "aov"}],
        )
        assert len(query.measures) == 2
        assert query.measures[0] == ModelMeasure(formula="*:count")
        assert query.measures[1] == ModelMeasure(formula="revenue:sum / *:count", name="aov")

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
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
        )
        assert query.measures[0] == ModelMeasure(formula="*:count")
        assert query.dimensions[0].name == "status"

    def test_none_fields_and_dimensions(self) -> None:
        query = SlayerQuery(source_model="orders")
        assert query.measures is None
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
            measures=["*:count"],
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
            measures=[ModelMeasure(formula="*:count")],
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
            measures=[ModelMeasure(formula="*:count")],
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
        q = SlayerQuery(source_model="orders", measures=["*:count", "revenue:sum"])
        assert q.measures[0].formula == "*:count"
        assert q.measures[1].formula == "revenue:sum"

    def test_fields_list_of_dicts_accepted(self) -> None:
        q = SlayerQuery(source_model="orders", measures=[{"formula": "*:count", "name": "cnt"}])
        assert q.measures[0].formula == "*:count"
        assert q.measures[0].name == "cnt"

    def test_dimensions_list_of_strings_coerced(self) -> None:
        q = SlayerQuery(source_model="orders", dimensions=["status", "region"])
        assert q.dimensions[0].name == "status"
        assert q.dimensions[1].name == "region"

    def test_fields_none_accepted(self) -> None:
        q = SlayerQuery(source_model="orders", fields=None)
        assert q.measures is None

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

    def test_name_with_dot_rejected(self) -> None:
        """DEV-1567: ``Aggregation.name`` must enforce the same identifier
        rules as ``Column.name`` and ``ModelMeasure.name``. Otherwise the
        ``column_x_custom_aggs`` catalog expansion produces a same-model
        metric like ``amount_my.agg`` whose dotted name the cross-model
        flatten filter (catalog.local_metrics / local_dimensions) would
        misclassify."""
        with pytest.raises(ValueError, match="Invalid name"):
            Aggregation(name="my.agg", formula="CUSTOM({value})")

    def test_name_with_hyphen_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid name"):
            Aggregation(name="my-agg", formula="CUSTOM({value})")

    def test_name_starting_with_digit_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid name"):
            Aggregation(name="9agg", formula="CUSTOM({value})")


class TestDimensionLabel:
    def test_label_optional(self) -> None:
        d = Column(name="status", sql="status", type=DataType.DOUBLE)
        assert d.label is None

    def test_label_set(self) -> None:
        d = Column(name="status", sql="status", label="Order Status", type=DataType.DOUBLE)
        assert d.label == "Order Status"

    def test_label_in_model_dump(self) -> None:
        d = Column(name="status", label="Order Status")
        data = d.model_dump(exclude_none=True)
        assert data["label"] == "Order Status"

    def test_label_excluded_when_none(self) -> None:
        d = Column(name="status")
        data = d.model_dump(exclude_none=True)
        assert "label" not in data


class TestMeasureLabel:
    def test_label_optional(self) -> None:
        m = Column(name="revenue", sql="amount", type=DataType.DOUBLE)
        assert m.label is None

    def test_label_set(self) -> None:
        m = Column(name="revenue", sql="amount", label="Total Revenue", type=DataType.DOUBLE)
        assert m.label == "Total Revenue"


class TestMeasureFilter:
    def test_filter_optional(self) -> None:
        m = Column(name="revenue", sql="amount", type=DataType.DOUBLE)
        assert m.filter is None

    def test_filter_set(self) -> None:
        m = Column(name="active_revenue", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        assert m.filter == "status = 'active'"

    def test_filter_multidot_autoconvert(self) -> None:
        m = Column(name="x", sql="amount", filter="a.b.c = 1", type=DataType.DOUBLE)
        assert "a__b.c" in m.filter

    def test_filter_in_model_dump(self) -> None:
        m = Column(name="x", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        data = m.model_dump(exclude_none=True)
        assert data["filter"] == "status = 'active'"

    def test_filter_excluded_when_none(self) -> None:
        m = Column(name="x", sql="amount", type=DataType.DOUBLE)
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
            measures=["*:count"],
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
        q = SlayerQuery(source_model="orders", measures=["orders.revenue:sum"])
        stripped = q.strip_source_model_prefix()
        assert stripped.measures[0].formula == "revenue:sum"

    def test_formula_star_count_stripped(self) -> None:
        """orders.*:count -> *:count"""
        q = SlayerQuery(source_model="orders", measures=["orders.*:count"])
        stripped = q.strip_source_model_prefix()
        assert stripped.measures[0].formula == "*:count"

    def test_formula_arithmetic_stripped(self) -> None:
        """orders.revenue:sum / orders.*:count -> revenue:sum / *:count"""
        q = SlayerQuery(source_model="orders", measures=["orders.revenue:sum / orders.*:count"])
        stripped = q.strip_source_model_prefix()
        assert stripped.measures[0].formula == "revenue:sum / *:count"

    def test_formula_transform_stripped(self) -> None:
        """cumsum(orders.revenue:sum) -> cumsum(revenue:sum)"""
        q = SlayerQuery(source_model="orders", measures=["cumsum(orders.revenue:sum)"])
        stripped = q.strip_source_model_prefix()
        assert stripped.measures[0].formula == "cumsum(revenue:sum)"

    def test_formula_cross_model_self_ref_stripped(self) -> None:
        """orders.customers.score:avg -> customers.score:avg"""
        q = SlayerQuery(source_model="orders", measures=["orders.customers.score:avg"])
        stripped = q.strip_source_model_prefix()
        assert stripped.measures[0].formula == "customers.score:avg"

    def test_formula_cross_model_not_stripped(self) -> None:
        """customers.score:avg on source_model=orders stays unchanged"""
        q = SlayerQuery(source_model="orders", measures=["customers.score:avg"])
        stripped = q.strip_source_model_prefix()
        assert stripped.measures[0].formula == "customers.score:avg"

    def test_formula_name_and_label_preserved(self) -> None:
        """Field name and label are preserved after formula stripping."""
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "orders.revenue:sum", "name": "rev", "label": "Revenue"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.measures[0].formula == "revenue:sum"
        assert stripped.measures[0].name == "rev"
        assert stripped.measures[0].label == "Revenue"

    # --- Filters ---

    def test_filter_self_ref_stripped(self) -> None:
        """orders.status = 'active' -> status = 'active'"""
        q = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            filters=["orders.status = 'active'"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters[0] == "status = 'active'"

    def test_filter_cross_model_self_ref_stripped(self) -> None:
        """orders.customers.name = 'foo' -> customers.name = 'foo'"""
        q = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            filters=["orders.customers.name = 'foo'"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters[0] == "customers.name = 'foo'"

    def test_filter_no_prefix_unchanged(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            filters=["status = 'active'", "customers.name = 'foo'"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters == ["status = 'active'", "customers.name = 'foo'"]

    def test_filter_with_transform_stripped(self) -> None:
        """change(orders.revenue:sum) > 0 -> change(revenue:sum) > 0"""
        q = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            filters=["change(orders.revenue:sum) > 0"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.filters[0] == "change(revenue:sum) > 0"

    # --- Order ---

    def test_order_self_ref_stripped(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            order=[{"column": "orders.revenue_sum", "direction": "desc"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.order[0].column.model is None
        assert stripped.order[0].column.name == "revenue_sum"
        assert stripped.order[0].direction == "desc"

    def test_order_no_prefix_unchanged(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            order=[{"column": "revenue_sum", "direction": "asc"}],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.order[0].column.model is None
        assert stripped.order[0].column.name == "revenue_sum"

    def test_order_raw_formula_preserved_after_strip(self) -> None:
        """Source-prefixed colon syntax in order: raw_formula survives stripping.

        Regression: strip_source_model_prefix() used to drop OrderItem.raw_formula
        when reconstructing the OrderItem, which broke enrichment's hidden ORDER BY
        materialization (enrichment.py: ``if not item.raw_formula: continue``).
        """
        q = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            order=[{"column": "orders.revenue:sum", "direction": "desc"}],
        )
        # Before stripping, raw_formula captured the rewritten input
        assert q.order[0].raw_formula == "orders.revenue:sum"
        stripped = q.strip_source_model_prefix()
        assert stripped.order[0].column.model is None
        assert stripped.order[0].column.name == "revenue_sum"
        # After stripping the source-model prefix, raw_formula must also be stripped
        assert stripped.order[0].raw_formula == "revenue:sum"

    def test_order_raw_formula_preserved_after_strip_multihop(self) -> None:
        """Multi-hop source-prefixed colon syntax: raw_formula gets prefix stripped."""
        q = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            order=[{"column": "orders.customers.score:sum", "direction": "asc"}],
        )
        assert q.order[0].raw_formula == "orders.customers.score:sum"
        stripped = q.strip_source_model_prefix()
        assert stripped.order[0].column.model == "customers"
        assert stripped.order[0].column.name == "score_sum"
        assert stripped.order[0].raw_formula == "customers.score:sum"

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
            measures=["reorders.revenue:sum"],
        )
        stripped = q.strip_source_model_prefix()
        assert stripped.measures[0].formula == "reorders.revenue:sum"


class TestColumnTypeLenientValidator:
    """DEV-1361: a Pydantic ``before``-validator absorbs legacy lowercase type
    spellings on ``Column.type`` so old MCP/REST input keeps working after
    the sqlglot-aligned enum rename. No deprecation warning."""

    def test_legacy_string_maps_to_text(self) -> None:
        col = Column(name="x", type="string")
        assert col.type == DataType.TEXT

    def test_legacy_number_maps_to_double(self) -> None:
        col = Column(name="x", type="number")
        assert col.type == DataType.DOUBLE

    def test_legacy_integer_maps_to_int(self) -> None:
        col = Column(name="x", type="integer")
        assert col.type == DataType.INT

    def test_legacy_time_maps_to_timestamp(self) -> None:
        col = Column(name="x", type="time")
        assert col.type == DataType.TIMESTAMP

    def test_legacy_date_maps_to_date(self) -> None:
        col = Column(name="x", type="date")
        assert col.type == DataType.DATE

    def test_legacy_boolean_maps_to_boolean(self) -> None:
        col = Column(name="x", type="boolean")
        assert col.type == DataType.BOOLEAN

    def test_pseudo_type_count_drops_to_default(self) -> None:
        # Aggregation pseudo-types are gone; the lenient validator drops them
        # and the field falls through to its default.
        col = Column(name="x", type="count")
        assert col.type == DataType.TEXT  # current default

    @pytest.mark.parametrize("legacy", ["sum", "avg", "min", "max", "last", "count_distinct"])
    def test_other_pseudo_types_drop_to_default(self, legacy: str) -> None:
        col = Column(name="x", type=legacy)
        assert col.type == DataType.TEXT

    def test_uppercase_passes_through(self) -> None:
        # Already-canonical strings round-trip unchanged (validator no-op).
        col = Column(name="x", type="DOUBLE")
        assert col.type == DataType.DOUBLE

    def test_enum_value_passes_through(self) -> None:
        col = Column(name="x", type=DataType.INT)
        assert col.type == DataType.INT


class TestModelMeasureType:
    """DEV-1361: ModelMeasure gains an optional ``type`` declaring the formula's
    result type. None (default) → no cast. Set value → outer CAST on the
    aggregation expression. Surfaces transparently on
    ``SlayerQuery.measures[i]`` since query measures use the same shape."""

    def test_default_is_none(self) -> None:
        m = ModelMeasure(formula="*:count")
        assert m.type is None

    def test_explicit_double(self) -> None:
        m = ModelMeasure(formula="*:count / orders:count", type=DataType.DOUBLE)
        assert m.type == DataType.DOUBLE

    def test_explicit_int(self) -> None:
        m = ModelMeasure(formula="*:count", type=DataType.INT)
        assert m.type == DataType.INT

    def test_legacy_string_value_absorbed(self) -> None:
        # Lenient validator on ModelMeasure.type mirrors Column.type.
        m = ModelMeasure(formula="*:count", type="number")
        assert m.type == DataType.DOUBLE

    def test_pseudo_type_drops_to_none(self) -> None:
        m = ModelMeasure(formula="*:count", type="count")
        assert m.type is None  # default

    def test_query_inline_measure_accepts_type(self) -> None:
        # SlayerQuery.measures[i] is a ModelMeasure — same shape, same field.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count / orders:count", "name": "ratio", "type": "DOUBLE"}],
        )
        assert q.measures[0].type == DataType.DOUBLE


class TestColumnSampledValuesAndDistinctCount:
    """DEV-1480: ``Column`` gains a structured sibling ``sampled_values`` for
    the text ``sampled`` string, and a ``distinct_count`` integer for the
    column's total cardinality at profile time. Both default to ``None``."""

    def test_sampled_values_defaults_to_none(self) -> None:
        col = Column(name="status", type=DataType.TEXT)
        assert col.sampled_values is None

    def test_distinct_count_defaults_to_none(self) -> None:
        col = Column(name="status", type=DataType.TEXT)
        assert col.distinct_count is None

    def test_sampled_values_accepts_empty_list(self) -> None:
        # All-NULL profiled categorical column.
        col = Column(name="status", type=DataType.TEXT, sampled_values=[])
        assert col.sampled_values == []

    def test_sampled_values_accepts_populated_list(self) -> None:
        col = Column(
            name="status",
            type=DataType.TEXT,
            sampled_values=["paid", "refunded"],
        )
        assert col.sampled_values == ["paid", "refunded"]

    def test_sampled_values_round_trip_through_dict(self) -> None:
        col = Column(
            name="status",
            type=DataType.TEXT,
            sampled_values=["a", "b", "c"],
        )
        dumped = col.model_dump()
        rebuilt = Column.model_validate(dumped)
        assert rebuilt.sampled_values == ["a", "b", "c"]

    def test_distinct_count_round_trip_through_dict(self) -> None:
        col = Column(
            name="status",
            type=DataType.TEXT,
            distinct_count=42,
        )
        dumped = col.model_dump()
        rebuilt = Column.model_validate(dumped)
        assert rebuilt.distinct_count == 42

    def test_distinct_count_accepts_zero(self) -> None:
        col = Column(name="status", type=DataType.TEXT, distinct_count=0)
        assert col.distinct_count == 0

    def test_sampled_text_string_preserved_alongside_structured_fields(self) -> None:
        col = Column(
            name="status",
            type=DataType.TEXT,
            sampled="paid, refunded",
            sampled_values=["paid", "refunded"],
            distinct_count=2,
        )
        assert col.sampled == "paid, refunded"
        assert col.sampled_values == ["paid", "refunded"]
        assert col.distinct_count == 2


class TestSlayerModelVersionBump:
    """DEV-1480: SlayerModel.version bumps from 6 to 7. v6→v7 is a no-op
    forward migration; the bump is purely about the new optional fields on
    Column."""

    def test_slayer_model_version_is_7(self) -> None:
        from slayer.core.models import SlayerModel

        m = SlayerModel(name="orders", sql_table="orders", data_source="ds")
        assert m.version == 7
