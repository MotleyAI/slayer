"""Tests for core domain models."""

import datetime

import pytest
from pydantic import ValidationError
from sqlalchemy.engine import make_url

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import (
    _FUNCSTYLE_PENDING,
    FieldRef,
    DatasetExtension,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.sql.dialects import SqliteDialect
from slayer.sql.mode_a_variables import bind_mode_a
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._engine_helpers import _engine_generate

_SQLITE = SqliteDialect()


class TestColumnRef:
    def test_from_string_with_model(self) -> None:
        ref = FieldRef.from_string("orders.status")
        assert ref.dataset == "orders"
        assert ref.name == "status"
        assert ref.full_name == "orders.status"

    def test_from_string_without_model(self) -> None:
        ref = FieldRef.from_string("status")
        assert ref.dataset is None
        assert ref.name == "status"
        assert ref.full_name == "status"

    def test_dotted_name_parsed_into_model(self) -> None:
        """Dotted name like 'customers.name' is auto-parsed: model='customers', name='name'."""
        ref = FieldRef(name="customers.name")
        assert ref.dataset == "customers"
        assert ref.name == "name"
        assert ref.full_name == "customers.name"

    def test_multihop_dotted_name_parsed(self) -> None:
        """Multi-hop 'customers.regions.name' splits on last dot."""
        ref = FieldRef(name="customers.regions.name")
        assert ref.dataset == "customers.regions"
        assert ref.name == "name"
        assert ref.full_name == "customers.regions.name"

    def test_simple_name_no_model(self) -> None:
        """Simple name without dots leaves model as None."""
        ref = FieldRef(name="status")
        assert ref.dataset is None
        assert ref.name == "status"

    def test_explicit_model_not_overwritten(self) -> None:
        """If model is explicitly provided, dotted parsing is skipped."""
        ref = FieldRef(dataset="customers", name="name")
        assert ref.dataset == "customers"
        assert ref.name == "name"

    def test_from_string_multihop(self) -> None:
        """from_string splits on last dot for multi-hop paths."""
        ref = FieldRef.from_string("customers.regions.name")
        assert ref.dataset == "customers.regions"
        assert ref.name == "name"
        assert ref.full_name == "customers.regions.name"

    def test_invalid_name_part_rejected(self) -> None:
        """Name parts must match identifier pattern."""
        with pytest.raises(ValueError):
            FieldRef(name="123invalid")


class TestOrderItem:
    """OrderItem column coercion and raw_formula capture."""

    def test_funcstyle_builtin_agg_preserved(self) -> None:
        """The functional spelling is preserved verbatim — a
        placeholder defers resolution to binding (like custom aggs)."""
        item = OrderItem(field="sum(revenue)", direction="desc")
        assert item.field.name == _FUNCSTYLE_PENDING
        assert item.raw_expression == "sum(revenue)"

    def test_funcstyle_custom_agg_accepted(self) -> None:
        """Function-style custom aggregations must validate without rejecting on parens.

        Regression: _coerce_order_column previously wrapped the unrewritten string
        as ``{"name": "rolling_avg(revenue)"}``, which ColumnRef's name validator
        rejected. raw_formula is preserved so enrichment can resolve the agg via
        extra_agg_names.
        """
        item = OrderItem(field="rolling_avg(revenue)", direction="desc")
        assert item.raw_expression == "rolling_avg(revenue)"

    def test_funcstyle_custom_agg_with_args_accepted(self) -> None:
        """Function-style custom agg with args is captured as raw_formula."""
        item = OrderItem(field="rolling_avg(revenue, window=7)", direction="asc")
        assert item.raw_expression == "rolling_avg(revenue, window=7)"

    def test_plain_column_no_raw_formula(self) -> None:
        """A plain column reference has no raw_formula."""
        item = OrderItem(field="status", direction="asc")
        assert item.field.name == "status"
        assert item.raw_expression is None


class TestOrderShorthandHealing:
    """Heal LLM-shorthand ``order`` items and harden direction validation.

    Shorthand ``{col: "desc"}`` (single- or multi-key) heals to canonical
    ``{"column": col, "direction": "desc"}``; direction is normalized
    (case-insensitive, ``asc``/``desc``/``ascending``/``descending``, whitespace
    trimmed) and anything else is rejected; ``OrderItem`` forbids extra keys.
    """

    # --- single-key shorthand ---

    def test_shorthand_desc(self) -> None:
        q = SlayerQuery(population="orders", order=[{"avg_processing_time_days": "desc"}])
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field.name == "avg_processing_time_days"
        assert q.order[0].direction == "desc"

    def test_shorthand_asc(self) -> None:
        q = SlayerQuery(population="orders", order=[{"status": "asc"}])
        assert q.order is not None
        assert q.order[0].field.name == "status"
        assert q.order[0].direction == "asc"

    def test_shorthand_equals_canonical(self) -> None:
        shorthand = SlayerQuery(population="orders", order=[{"status": "desc"}])
        canonical = SlayerQuery.model_validate({
            "population": "orders", "order": [{"field": "status", "direction": "desc"}]
        })
        assert shorthand.order == canonical.order

    # --- direction vocabulary: case-insensitive, synonyms, whitespace ---

    def test_shorthand_uppercase_normalized(self) -> None:
        q = SlayerQuery(population="orders", order=[{"status": "DESC"}])
        assert q.order[0].direction == "desc"

    def test_shorthand_descending_synonym(self) -> None:
        q = SlayerQuery(population="orders", order=[{"status": "DESCENDING"}])
        assert q.order[0].direction == "desc"

    def test_shorthand_ascending_synonym_mixed_case(self) -> None:
        q = SlayerQuery(population="orders", order=[{"status": "Ascending"}])
        assert q.order[0].direction == "asc"

    def test_shorthand_whitespace_trimmed(self) -> None:
        q = SlayerQuery(population="orders", order=[{"status": " Asc "}])
        assert q.order[0].direction == "asc"

    # --- formula key composes with existing normalization ---

    def test_shorthand_formula_key(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "order": [{"sum(revenue)": "desc"}]})
        assert q.order is not None
        assert q.order[0].field.name == "_funcstyle_pending"
        assert q.order[0].raw_expression == "sum(revenue)"
        assert q.order[0].direction == "desc"

    def test_shorthand_formula_key_equals_canonical(self) -> None:
        shorthand = SlayerQuery.model_validate({"population": "orders", "order": [{"sum(revenue)": "desc"}]})
        canonical = SlayerQuery.model_validate({
            "population": "orders",
            "order": [{"field": "sum(revenue)", "direction": "desc"}],
        })
        assert shorthand.order == canonical.order

    # --- multi-key expansion (one dict -> N OrderItems, insertion order) ---

    def test_multikey_expands_in_order(self) -> None:
        q = SlayerQuery(population="orders", order=[{"a": "desc", "b": "asc"}])
        assert [(o.field.name, o.direction) for o in q.order] == [("a", "desc"), ("b", "asc")]

    def test_multikey_synonyms_and_case(self) -> None:
        q = SlayerQuery(population="orders", order=[{"a": "ASCENDING", "b": "Desc"}])
        assert [(o.field.name, o.direction) for o in q.order] == [("a", "asc"), ("b", "desc")]

    def test_multikey_partial_non_direction_raises(self) -> None:
        # Not every value is a direction -> not shorthand -> canonical validation rejects.
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[{"a": "desc", "b": "downward"}])

    # --- bare single item passed without the enclosing list gets wrapped ---

    def test_bare_dict_shorthand_wrapped(self) -> None:
        q = SlayerQuery(population="orders", order={"status": "desc"})
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field.name == "status"
        assert q.order[0].direction == "desc"

    def test_bare_canonical_dict_wrapped(self) -> None:
        q = SlayerQuery(population="orders", order={"column": "status", "direction": "asc"})
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field.name == "status"
        assert q.order[0].direction == "asc"

    def test_bare_multikey_dict_wrapped(self) -> None:
        q = SlayerQuery(population="orders", order={"a": "desc", "b": "asc"})
        assert [(o.field.name, o.direction) for o in q.order] == [("a", "desc"), ("b", "asc")]

    def test_bare_orderitem_wrapped(self) -> None:
        item = OrderItem(field="status", direction="desc")
        q = SlayerQuery(population="orders", order=item)
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field.name == "status"
        assert q.order[0].direction == "desc"

    # --- canonical preserved; latent uppercase-direction bug fixed ---

    def test_canonical_unchanged(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "order": [{"field": "status", "direction": "desc"}]})
        assert q.order is not None
        assert q.order[0].field.name == "status"
        assert q.order[0].direction == "desc"

    def test_canonical_uppercase_direction_normalized(self) -> None:
        # Pre-existing latent bug: SQL generator compares ``direction == "asc"`` strictly,
        # so a stored "ASCENDING"/"ASC" used to silently emit DESC. Now normalized.
        q = SlayerQuery.model_validate({
            "population": "orders", "order": [{"field": "status", "direction": "ASCENDING"}]
        })
        assert q.order[0].direction == "asc"

    def test_canonical_column_named_like_direction_word(self) -> None:
        # Single reserved 'column' key whose value happens to be a direction word
        # stays canonical (order by a column literally named "desc").
        q = SlayerQuery.model_validate({"population": "orders", "order": [{"field": "desc"}]})
        assert q.order is not None
        assert q.order[0].field.name == "desc"
        assert q.order[0].direction == "asc"

    # --- rejections: malformed / ambiguous never silently mangled ---

    def test_invalid_direction_value_shorthand_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[{"col": "downward"}])

    def test_invalid_direction_value_canonical_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery.model_validate({"population": "orders", "order": [{"field": "status", "direction": "sideways"}]})

    def test_mixed_canonical_and_stray_direction_key_raises(self) -> None:
        # extra='forbid' rejects the stray 'b' rather than silently dropping it.
        with pytest.raises(ValidationError):
            SlayerQuery.model_validate({"population": "orders", "order": [{"field": "status", "b": "asc"}]})

    def test_mixed_canonical_and_stray_nondirection_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery.model_validate({"population": "orders", "order": [{"field": "status", "b": "downward"}]})

    def test_direction_only_key_raises(self) -> None:
        # {"direction": "desc"} is a malformed canonical item (no column), not shorthand.
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[{"direction": "desc"}])

    def test_empty_dict_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[{}])

    def test_nested_list_item_raises(self) -> None:
        # No list-of-lists support.
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[["a", "desc"]])

    def test_non_wrappable_scalar_string_raises(self) -> None:
        with pytest.raises(Exception, match="must be a list"):
            SlayerQuery(population="orders", order="status")

    def test_non_wrappable_int_raises(self) -> None:
        with pytest.raises(Exception, match="must be a list"):
            SlayerQuery(population="orders", order=42)

    # --- mixed list: shorthand + canonical + multi-key together ---

    def test_mixed_shorthand_canonical_multikey_list(self) -> None:
        q = SlayerQuery(
            population="orders",
            order=[
                {"a": "desc"},
                {"field": "b", "direction": "asc"},
                {"c": "asc", "d": "desc"},
            ],
        )
        assert [(o.field.name, o.direction) for o in q.order] == [
            ("a", "desc"),
            ("b", "asc"),
            ("c", "asc"),
            ("d", "desc"),
        ]

    # --- None / tuple / non-string keys / passthrough ---

    def test_order_none_stays_none(self) -> None:
        q = SlayerQuery(population="orders", order=None)
        assert q.order is None

    def test_tuple_input_accepted(self) -> None:
        q = SlayerQuery.model_validate({
            "population": "orders",
            "order": ({"a": "desc"}, {"field": "b", "direction": "asc"}),
        })
        assert [(o.field.name, o.direction) for o in q.order] == [("a", "desc"), ("b", "asc")]

    def test_non_string_key_not_healed_raises(self) -> None:
        # A non-string key means the dict is not treated as shorthand; it falls
        # through to OrderItem validation, which rejects it (no column).
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[{1: "desc"}])

    def test_multikey_formula_keys_preserve_raw_formula(self) -> None:
        q = SlayerQuery.model_validate({
            "population": "orders",
            "order": [{"sum(revenue)": "desc", "max(amount)": "asc"}],
        })
        assert q.order is not None
        assert len(q.order) == 2
        assert q.order[0].field.name == "_funcstyle_pending"
        assert q.order[0].raw_expression == "sum(revenue)"
        assert q.order[0].direction == "desc"
        assert q.order[1].field.name == "_funcstyle_pending"
        assert q.order[1].raw_expression == "max(amount)"
        assert q.order[1].direction == "asc"

    def test_list_orderitem_passthrough(self) -> None:
        item = OrderItem.model_validate({"field": "sum(revenue)", "direction": "desc"})
        q = SlayerQuery(population="orders", order=[item])
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field.name == "_funcstyle_pending"
        assert q.order[0].direction == "desc"

    # --- OrderItem-level direction validation + extra=forbid ---

    def test_orderitem_direction_synonym_normalized(self) -> None:
        assert OrderItem(field="status", direction="DESCENDING").direction == "desc"

    def test_orderitem_direction_default_asc(self) -> None:
        assert OrderItem(field="status").direction == "asc"

    def test_orderitem_invalid_direction_raises(self) -> None:
        with pytest.raises(ValidationError):
            OrderItem(field="status", direction="sideways")

    def test_orderitem_extra_key_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            OrderItem(field="status", direction="asc", bogus="x")


class TestSlayerModel:
    def test_get_dimension(self) -> None:
        model = SlayerDataset(
            name="test",
            sql_table="t",
            data_source="test",
            fields=[DatasetField(name="x", datatype=DataType.TEXT)],
        )
        assert model.get_field("x") is not None
        assert model.get_field("y") is None

    def test_get_measure(self) -> None:
        """``get_measure`` returns a ModelMeasure formula by name."""
        model = SlayerDataset(
            name="test",
            sql_table="t",
            data_source="test",
            metrics=[DatasetMetric(name="aov", expression="sum(revenue) / count(*)")],
        )
        assert model.get_metric("aov") is not None
        assert model.get_metric("missing") is None

    def test_model_measure_may_be_named_after_transform(self) -> None:
        """A transform name acts as a transform only in call position."""
        assert DatasetMetric(name="cumsum", expression="sum(revenue)").name == "cumsum"

    def test_model_measure_formula_with_raw_over_raises(self) -> None:
        """A ``ModelMeasure`` formula containing raw `OVER (...)` SQL
        cannot be parsed by SLayer's formula grammar (Python AST) and would
        produce invalid SQL on every dialect if used as a filter. Reject at
        construction time with an actionable error pointing at SLayer's
        ``rank()`` / ``first()`` / ``last()`` / ``lag()`` / ``lead()`` transforms
        or a ``Column.sql``-with-window pattern.
        """
        with pytest.raises(ValueError) as excinfo:
            DatasetMetric(
                name="top_3",
                expression="row_number() over (order by mass desc) <= 3",
            )
        msg = str(excinfo.value)
        assert "window function" in msg.lower(), msg
        assert any(
            keyword in msg
            for keyword in ("rank(", "first(", "last(", "lag(", "lead(", "Column.sql", "multi-stage")
        ), msg

    def test_filter_bare_column_allowed(self) -> None:
        """Bare column names in model filters are valid."""
        model = SlayerDataset(
            name="test", sql_table="t", data_source="test",
            filters=["status == 'active'", "amount > 100"],
        )
        assert model.filters == ["status == 'active'", "amount > 100"]

    def test_filter_single_dot_allowed(self) -> None:
        """Single-dot (table.column) references in model filters are valid SQL."""
        model = SlayerDataset(
            name="test", sql_table="t", data_source="test",
            filters=["customers.region == 'US'"],
        )
        assert model.filters == ["customers.region == 'US'"]

    def test_filter_double_underscore_allowed(self) -> None:
        """Double-underscore alias references in model filters are valid."""
        model = SlayerDataset(
            name="test", sql_table="t", data_source="test",
            filters=["customers__regions.name == 'US'"],
        )
        assert model.filters == ["customers__regions.name == 'US'"]

    def test_filter_string_literal_dots_not_converted(self) -> None:
        """Dots inside string literals are not converted."""
        model = SlayerDataset(
            name="test", sql_table="t", data_source="test",
            filters=["name == 'foo.bar.baz'"],
        )
        assert model.filters == ["name == 'foo.bar.baz'"]

    def test_dimension_sql_single_dot_unchanged(self) -> None:
        """Single-dot references in dimension sql are left as-is."""
        dim = DatasetField(name="cust_name", sql="customers.name", datatype=DataType.DOUBLE)
        assert dim.sql == "customers.name"

    def test_measure_sql_single_dot_unchanged(self) -> None:
        """Single-dot references in measure sql are left as-is."""
        meas = DatasetField(name="total", sql="orders.amount", datatype=DataType.DOUBLE)
        assert meas.sql == "orders.amount"

    def test_model_name_allows_double_underscore(self) -> None:
        # `__` is legal in model names (the dotted-canonical flip
        # made join-path ambiguity structurally impossible).
        model = SlayerDataset(name="my__model", sql_table="t", data_source="test")
        assert model.name == "my__model"

    def test_model_name_rejects_reserved_slayer_prefix(self) -> None:
        with pytest.raises(ValueError, match="__slayer_"):
            SlayerDataset(name="__slayer_x", sql_table="t", data_source="test")

    def test_dimension_name_allows_double_underscore(self) -> None:
        """__ is allowed in dimension names — used for flattened join paths in virtual models."""
        dim = DatasetField(name="stores__name")
        assert dim.name == "stores__name"

    def test_measure_name_allows_double_underscore(self) -> None:
        """__ is allowed in measure names — used for flattened join paths in virtual models."""
        meas = DatasetField(name="stores__tax_rate_sum", sql="tax_rate", datatype=DataType.DOUBLE)
        assert meas.name == "stores__tax_rate_sum"

    def test_query_name_allows_double_underscore(self) -> None:
        # `__` is legal in query names.
        q = SlayerQuery(name="my__query", population="orders")
        assert q.name == "my__query"

    def test_query_name_rejects_reserved_slayer_prefix(self) -> None:
        with pytest.raises(ValueError, match="__slayer_"):
            SlayerQuery(name="__slayer_x", population="orders")

    def test_model_name_single_underscore_allowed(self) -> None:
        model = SlayerDataset(name="my_model", sql_table="t", data_source="test")
        assert model.name == "my_model"

    def test_dimension_name_single_underscore_allowed(self) -> None:
        dim = DatasetField(name="customer_name")
        assert dim.name == "customer_name"

    def test_dimension_name_rejects_dot(self) -> None:
        """Dots are path syntax, not allowed in dimension names."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            DatasetField(name="customers.name")

    def test_measure_name_rejects_dot(self) -> None:
        """Dots are path syntax, not allowed in measure names."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            DatasetField(name="customers.name_sum", sql="name", datatype=DataType.DOUBLE)

    def test_dimension_name_without_dot_allowed(self) -> None:
        dim = DatasetField(name="region_name")
        assert dim.name == "region_name"

    def test_measure_name_without_dot_allowed(self) -> None:
        meas = DatasetField(name="order_total_sum", sql="total", datatype=DataType.DOUBLE)
        assert meas.name == "order_total_sum"

    def test_model_data_source_rejects_dot(self) -> None:
        """Dots in data_source would let a sibling datasource
        ``prod.legacy`` collide with cascade-delete of ``prod``."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            SlayerDataset(
                name="orders", sql_table="t", data_source="prod.legacy",
            )

    def test_datasource_name_rejects_dot(self) -> None:
        """Dots in DatasourceConfig.name break canonical-id
        namespace boundaries."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            DatasourceConfig(name="prod.legacy", type="postgres")

    def test_datasource_name_allows_double_underscore(self) -> None:
        """``__`` is reserved as a SQL join-path alias separator on
        model/query names, but datasource names never appear in SQL
        alias positions, so they accept ``__`` freely."""
        ds = DatasourceConfig(name="prod__staging", type="postgres")
        assert ds.name == "prod__staging"

    def test_datasource_name_rejects_path_separator(self) -> None:
        with pytest.raises(ValueError, match="must not contain '/'"):
            DatasourceConfig(name="prod/legacy", type="postgres")

    def test_datasource_name_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="non-empty string"):
            DatasourceConfig(name="", type="postgres")

    def test_datasource_name_rejects_colon(self) -> None:
        """Colon is reserved as the DSL aggregation separator
        (``revenue:sum``) and the ``memory:<int>`` canonical-id prefix.
        Allowing it in a datasource name would let ``memory:42`` collide
        with the memory canonical-id namespace."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            DatasourceConfig(name="memory:42", type="sqlite")

    def test_model_data_source_rejects_colon(self) -> None:
        """A model's ``data_source`` shares the same canonical-id
        namespace constraints as ``DatasourceConfig.name``."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerDataset(name="orders", sql_table="t", data_source="memory:42")

    def test_model_name_rejects_colon(self) -> None:
        """Colon is reserved as the DSL aggregation separator
        (``revenue:sum``) — model names sharing the shape would collide
        with formula parsing."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerDataset(name="rev:sum", sql_table="t", data_source="ds")

    def test_query_name_rejects_colon(self) -> None:
        """SlayerQuery names share the same naming space as SlayerModel
        names (a query can be persisted as a query-backed model), so the
        same rejection rules apply."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerQuery(name="rev:sum", population="orders")

    def test_query_name_rejects_dot(self) -> None:
        """Dotted SlayerQuery names would collide with the dotted-path
        reference syntax used in queries."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            SlayerQuery(name="prod.summary", population="orders")

    def test_column_name_rejects_colon(self) -> None:
        """Column names containing ``:`` would collide with the
        aggregation colon syntax (``revenue:sum``)."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            DatasetField(name="rev:sum")


class TestWithinListDuplicateNames:
    """Duplicate names within ``columns`` or within ``measures`` are rejected.

    Codex's Major 4: today only cross-list overlap is validated, so two
    columns named ``revenue`` (or two measures named ``aov``) silently pass
    and ``get_column`` / ``get_measure`` return the first match.
    """

    def test_duplicate_column_names_rejected(self) -> None:
        columns = [
            DatasetField(name="revenue", sql="amount", datatype=DataType.DOUBLE),
            DatasetField(name="revenue", sql="net_amount", datatype=DataType.DOUBLE),
        ]
        with pytest.raises(ValueError, match="duplicate.*column|column.*duplicate"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                fields=columns,
            )

    def test_duplicate_measure_names_rejected(self) -> None:
        columns = [DatasetField(name="amount", sql="amount", datatype=DataType.DOUBLE)]
        measures = [
            DatasetMetric(name="aov", expression="sum(amount) / count(*)"),
            DatasetMetric(name="aov", expression="avg(amount)"),
        ]
        with pytest.raises(ValueError, match="duplicate.*measure|measure.*duplicate"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                fields=columns,
                metrics=measures,
            )

    def test_unnamed_model_measure_rejected(self) -> None:
        """Every ``ModelMeasure`` stored on a ``SlayerModel`` must have a name.

        Unnamed entries are unreachable via ``get_measure()`` and bare-name
        expansion, so persisting them is meaningless. (Inline measures on
        ``SlayerQuery.metrics`` may still be unnamed — only model-level
        measures are required to be named.)
        """
        columns = [DatasetField(name="amount", sql="amount", datatype=DataType.DOUBLE)]
        measures = [DatasetMetric(expression="sum(amount)")]
        with pytest.raises(ValueError, match="must have a name"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                fields=columns,
                metrics=measures,
            )

    def test_unique_names_accepted(self) -> None:
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(name="amount", sql="amount", datatype=DataType.DOUBLE),
                DatasetField(name="status", sql="status", datatype=DataType.TEXT),
            ],
            metrics=[
                DatasetMetric(name="aov", expression="sum(amount) / count(*)"),
                DatasetMetric(name="rev", expression="sum(amount)"),
            ],
        )
        assert len(model.fields) == 2
        assert len(model.metrics) == 2


class TestSourceModeExclusivity:
    """Source-mode exclusivity: exactly one of sql_table, sql, or
    source_queries must be populated.
    """

    def test_accepts_sql_table_only(self) -> None:
        m = SlayerDataset(name="orders", sql_table="orders_t", data_source="ds")
        assert m.sql_table == "orders_t"
        assert m.sql is None
        assert m.source_queries is None

    def test_accepts_sql_only(self) -> None:
        m = SlayerDataset(name="orders", sql="SELECT 1 AS x", data_source="ds")
        assert m.sql == "SELECT 1 AS x"
        assert m.sql_table is None

    def test_accepts_source_queries_only(self) -> None:
        m = SlayerDataset(
            name="saved",
            data_source="ds",
            source_queries=[SlayerQuery(population="orders")],
        )
        assert m.source_queries is not None
        assert len(m.source_queries) == 1

    def test_rejects_no_source(self) -> None:
        with pytest.raises(ValueError, match="exactly one source.*none specified"):
            SlayerDataset(name="orders", data_source="ds")

    def test_rejects_sql_table_plus_sql(self) -> None:
        with pytest.raises(ValueError, match="exactly one source"):
            SlayerDataset(
                name="orders", sql_table="t", sql="SELECT 1", data_source="ds"
            )

    def test_rejects_sql_table_plus_source_queries(self) -> None:
        source_queries = [SlayerQuery(population="orders")]
        with pytest.raises(ValueError, match="exactly one source"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                source_queries=source_queries,
                data_source="ds",
            )

    def test_rejects_sql_plus_source_queries(self) -> None:
        source_queries = [SlayerQuery(population="orders")]
        with pytest.raises(ValueError, match="exactly one source"):
            SlayerDataset(
                name="orders",
                sql="SELECT 1",
                source_queries=source_queries,
                data_source="ds",
            )

    def test_rejects_all_three(self) -> None:
        source_queries = [SlayerQuery(population="orders")]
        with pytest.raises(ValueError, match="exactly one source"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                sql="SELECT 1",
                source_queries=source_queries,
                data_source="ds",
            )

    def test_rejects_empty_source_queries_list(self) -> None:
        with pytest.raises(ValueError, match="cannot be an empty list"):
            SlayerDataset(name="orders", source_queries=[], data_source="ds")


class TestSelfJoinRejected:
    """A join targeting the model itself is unaddressable (joins resolve by
    target-model name) and must fail loudly at construction."""

    def test_self_join_raises_informative_error(self) -> None:
        columns = [
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="manager_id", datatype=DataType.INT),
        ]
        joins = [DatasetRelationship(to="staff",
                           join_pairs=[["manager_id", "id"]])]
        with pytest.raises(ValueError, match="[Ss]elf-join") as ei:
            SlayerDataset(
                name="staff", data_source="ds", sql_table="staff",
                fields=columns, relationships=joins,
            )
        message = str(ei.value)
        assert "staff" in message
        # The remedy is named, not just the ban.
        assert "separate model" in message

    def test_distinct_target_still_accepted(self) -> None:
        m = SlayerDataset(
            name="staff", data_source="ds", sql_table="staff",
            fields=[DatasetField(name="team_id", datatype=DataType.INT)],
            relationships=[DatasetRelationship(to="teams",
                             join_pairs=[["team_id", "id"]])],
        )
        assert m.relationships[0].to == "teams"


class TestSourceQueryStages:
    """Stage-name rules on ``source_queries``."""

    def test_single_unnamed_stage_accepted(self) -> None:
        """Single (final) stage may be unnamed."""
        m = SlayerDataset(
            name="saved",
            data_source="ds",
            source_queries=[SlayerQuery(population="orders")],
        )
        assert m.source_queries is not None

    def test_unnamed_non_final_stage_rejected(self) -> None:
        source_queries = [
            SlayerQuery(population="orders"),
            SlayerQuery(population="orders"),
        ]
        with pytest.raises(ValueError, match="non-final stage at index 0.*must have a 'name'"):
            SlayerDataset(
                name="saved",
                data_source="ds",
                source_queries=source_queries,
            )

    def test_named_non_final_stage_with_unnamed_final_accepted(self) -> None:
        m = SlayerDataset(
            name="saved",
            data_source="ds",
            source_queries=[
                SlayerQuery(name="stage1", population="orders"),
                SlayerQuery(population="stage1"),
            ],
        )
        assert m.source_queries is not None
        assert len(m.source_queries) == 2

    def test_duplicate_stage_names_rejected(self) -> None:
        source_queries = [
            SlayerQuery(name="dup", population="orders"),
            SlayerQuery(name="dup", population="orders"),
        ]
        with pytest.raises(ValueError, match="duplicate stage name"):
            SlayerDataset(
                name="saved",
                data_source="ds",
                source_queries=source_queries,
            )

    def test_dicts_parsed_to_slayer_query(self) -> None:
        """source_queries entries given as dicts are parsed into SlayerQuery
        instances by the before-validator."""
        m = SlayerDataset.model_validate({
            "name": "saved",
            "data_source": "ds",
            "source_queries": [
                {"population": "orders", "metrics": [{"expression": "count(*)"}]}
            ],
        })
        assert m.source_queries is not None
        assert isinstance(m.source_queries[0], SlayerQuery)
        assert m.source_queries[0].metrics is not None
        assert m.source_queries[0].metrics[0].expression == "count(*)"

    def test_invalid_entry_type_rejected(self) -> None:
        with pytest.raises((ValueError, TypeError)):
            SlayerDataset(
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

        with pytest.raises(ValidationError):
            SlayerDataset(
                name="saved",
                data_source="ds",
                source_queries=[123],  # type: ignore[list-item]
            )

    def test_non_list_input_raises_pydantic_validation_error(self) -> None:

        with pytest.raises(ValidationError):
            SlayerDataset.model_validate({
                "name": "saved",
                "data_source": "ds",
                "source_queries": "not a list",
            })


class TestQueryVariablesAndCacheFields:
    """``query_variables`` defaults and shape."""

    def test_query_variables_default_is_empty_dict(self) -> None:
        m = SlayerDataset(name="orders", sql_table="t", data_source="ds")
        assert m.query_variables == {}

    def test_query_variables_persists_user_value(self) -> None:
        m = SlayerDataset(
            name="saved",
            data_source="ds",
            source_queries=[SlayerQuery(population="orders")],
            query_variables={"threshold": 1500, "region": "US"},
        )
        assert m.query_variables == {"threshold": 1500, "region": "US"}


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
        columns = [
            DatasetField(
                name="id",
                datatype=DataType.DOUBLE,
                primary_key=True,
                allowed_aggregations=["sum"],
            ),
        ]
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                fields=columns,
            )

    def test_pk_column_with_count_only_whitelist_accepted(self) -> None:
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(
                    name="id",
                    datatype=DataType.DOUBLE,
                    primary_key=True,
                    allowed_aggregations=["count", "count_distinct"],
                ),
            ],
        )
        assert model.fields[0].allowed_aggregations == ["count", "count_distinct"]

    def test_string_column_with_sum_in_whitelist_rejected(self) -> None:
        columns = [
            DatasetField(
                name="status",
                datatype=DataType.TEXT,
                allowed_aggregations=["sum"],
            ),
        ]
        with pytest.raises(ValueError, match="not applicable|string"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                fields=columns,
            )

    def test_string_column_with_min_max_in_whitelist_accepted(self) -> None:
        """String ``min``/``max`` are intentionally type-eligible."""
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(
                    name="status",
                    datatype=DataType.TEXT,
                    allowed_aggregations=["min", "max", "count"],
                ),
            ],
        )
        assert "min" in (model.fields[0].allowed_aggregations or [])
        assert "max" in (model.fields[0].allowed_aggregations or [])

    def test_date_column_with_min_max_accepted(self) -> None:
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(
                    name="ordered_on",
                    datatype=DataType.DATE,
                    allowed_aggregations=["min", "max"],
                ),
            ],
        )
        assert model.fields[0].allowed_aggregations == ["min", "max"]

    def test_timestamp_column_with_min_max_accepted(self) -> None:
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(
                    name="ordered_at",
                    datatype=DataType.TIMESTAMP,
                    allowed_aggregations=["min", "max"],
                ),
            ],
        )
        assert model.fields[0].allowed_aggregations == ["min", "max"]

    def test_custom_aggregation_in_whitelist_bypasses_type_check(self) -> None:
        """A custom-aggregation entry is exempt from type-default eligibility.
        Its applicability depends on the custom formula, not the type-default map.
        """
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            aggregations=[
                Aggregation(
                    name="custom_concat",
                    expression="STRING_AGG({value}, ',')",
                ),
            ],
            fields=[
                DatasetField(
                    name="status",
                    datatype=DataType.TEXT,
                    allowed_aggregations=["custom_concat"],
                ),
            ],
        )
        assert model.fields[0].allowed_aggregations == ["custom_concat"]

    def test_unknown_aggregation_in_whitelist_still_rejected(self) -> None:
        """Existing behavior — keep it."""
        columns = [
            DatasetField(
                name="revenue",
                datatype=DataType.DOUBLE,
                allowed_aggregations=["bogus_agg"],
            ),
        ]
        with pytest.raises(ValueError, match="not a built-in aggregation"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                fields=columns,
            )

    def test_builtin_override_still_type_gated(self) -> None:
        """Overriding a built-in name (e.g., ``sum``) with a custom formula must
        still respect type-default eligibility — built-ins keep their type
        semantics regardless of the override formula. Only truly novel custom
        names bypass the type-default gate.
        """
        aggregations = [
            Aggregation(name="sum", expression="STRING_AGG({value}, ',')"),
        ]
        columns = [
            DatasetField(
                name="status",
                datatype=DataType.TEXT,
                allowed_aggregations=["sum"],
            ),
        ]
        with pytest.raises(ValueError, match="not applicable|string"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                aggregations=aggregations,
                fields=columns,
            )

    def test_pk_column_with_custom_agg_rejected(self) -> None:
        """Even custom aggregations cannot be whitelisted on a PK column —
        the PK rule (count/count_distinct only) is absolute.
        """
        aggregations = [
            Aggregation(name="custom_sum", expression="SUM({value})"),
        ]
        columns = [
            DatasetField(
                name="id",
                datatype=DataType.DOUBLE,
                primary_key=True,
                allowed_aggregations=["custom_sum"],
            ),
        ]
        with pytest.raises(ValueError, match="primary[- ]key|count"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                aggregations=aggregations,
                fields=columns,
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
        assert cs == "postgresql+psycopg://user:pass@localhost:5432/mydb"

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

    # The generic (non-tsql) connection-string branch must not
    # corrupt credentials whose password contains reserved URL characters.
    # The password below exercises every generic-branch delimiter: '@'
    # (userinfo/host), ':' (userinfo/port), '/' (path), '#' (fragment),
    # '?' (query), and a space.
    _RESERVED_PASSWORD = "p@ss/w:rd#q?x y"  # NOSONAR(S2068) — test-only fixture credential, not a real secret

    @pytest.mark.parametrize(
        ("ds_type", "expected_driver"),
        [
            ("postgres", "postgresql+psycopg"),
            ("postgresql", "postgresql+psycopg"),
            ("mysql", "mysql+pymysql"),
            ("mariadb", "mysql+pymysql"),
            ("clickhouse", "clickhouse+http"),
            # An unmapped type keeps its own name as the driver and must still
            # encode reserved-char credentials safely.
            ("customdb", "customdb"),
        ],
    )
    def test_reserved_char_password_round_trips(
        self, ds_type: str, expected_driver: str
    ) -> None:
        ds = DatasourceConfig(
            name="example",
            type=ds_type,
            host="db.example",
            port=5432,
            database="analytics",
            username="user",
            password=self._RESERVED_PASSWORD,
        )
        cs = ds.get_connection_string()

        # The reserved delimiters the issue names ('@', '/', ':') must be
        # percent-encoded, not left as raw URL delimiters.
        assert "%40" in cs  # '@'
        assert "%2F" in cs  # '/'
        assert "%3A" in cs  # ':'
        assert self._RESERVED_PASSWORD not in cs

        url = make_url(cs)
        assert url.drivername == expected_driver
        assert url.username == "user"
        assert url.password == self._RESERVED_PASSWORD
        assert url.host == "db.example"
        assert url.port == 5432
        assert url.database == "analytics"

    def test_reserved_char_username_round_trips(self) -> None:
        # Manually interpolated usernames are affected too, not just
        # passwords. URL.create must percent-encode reserved
        # characters in the username so it round-trips intact.
        username = "user@tenant/name:role"
        ds = DatasourceConfig(
            name="example",
            type="postgres",
            host="db.example",
            port=5432,
            database="analytics",
            username=username,
            password="plain_pass",  # NOSONAR(S2068) — test-only fixture credential, not a real secret
        )
        cs = ds.get_connection_string()

        assert username not in cs  # raw delimiters must not leak through
        url = make_url(cs)
        assert url.username == username
        assert url.password == "plain_pass"
        assert url.host == "db.example"
        assert url.port == 5432
        assert url.database == "analytics"

    @pytest.mark.parametrize(
        "ds_type",
        ["postgres", "postgresql", "mysql", "mariadb", "clickhouse", "customdb"],
    )
    def test_delimiter_safe_password_no_regression(self, ds_type: str) -> None:
        # A password with no reserved characters must round-trip exactly as
        # it did before the URL.create switch.
        ds = DatasourceConfig(
            name="example",
            type=ds_type,
            host="db.example",
            port=5432,
            database="analytics",
            username="user",
            password="plain_pass",  # NOSONAR(S2068) — test-only fixture credential, not a real secret
        )
        url = make_url(ds.get_connection_string())
        assert url.username == "user"
        assert url.password == "plain_pass"
        assert url.host == "db.example"
        assert url.port == 5432
        assert url.database == "analytics"

    def test_host_with_embedded_port_is_split(self) -> None:
        # Backward-compat: the pre-fix generic branch let a caller stuff
        # "host:port" into the host field with port unset. URL.create would
        # otherwise treat the colon as an IPv6-style host and bracket it, so
        # a single-colon numeric-port host is split back into host + port.
        ds = DatasourceConfig(
            name="example",
            type="postgres",
            host="db.example:5432",
            database="analytics",
            username="user",
            password="plain_pass",  # NOSONAR(S2068) — test-only fixture credential, not a real secret
        )
        url = make_url(ds.get_connection_string())
        assert url.host == "db.example"
        assert url.port == 5432
        assert url.database == "analytics"

    def test_ipv6_host_is_not_split(self) -> None:
        # A bare IPv6 host (multiple colons, no bracket) must NOT be treated
        # as host:port — URL.create brackets it correctly on its own.
        ds = DatasourceConfig(
            name="example",
            type="postgres",
            host="::1",
            port=5432,
            database="analytics",
        )
        url = make_url(ds.get_connection_string())
        assert url.host == "::1"
        assert url.port == 5432

    def test_bracketed_ipv6_host_is_unwrapped(self) -> None:
        # The pre-fix string branch tolerated a bracketed IPv6 host; URL.create
        # wants the raw host, so the brackets are stripped.
        ds = DatasourceConfig(
            name="example",
            type="postgres",
            host="[::1]",
            port=5432,
            database="analytics",
        )
        url = make_url(ds.get_connection_string())
        assert url.host == "::1"
        assert url.port == 5432

    def test_conflicting_port_in_host_and_port_field_raises(self) -> None:
        # A port embedded in the host AND a separate port field is
        # contradictory config — raise rather than guess which wins.
        ds = DatasourceConfig(
            name="example",
            type="postgres",
            host="db.example:5432",
            port=9999,
            database="analytics",
        )
        with pytest.raises(ValueError, match="only one place"):
            ds.get_connection_string()

    def test_conflicting_port_bracketed_ipv6_and_port_field_raises(self) -> None:
        # Same conflict for a bracketed IPv6 host that embeds a port.
        ds = DatasourceConfig(
            name="example",
            type="postgres",
            host="[::1]:5432",
            port=9999,
            database="analytics",
        )
        with pytest.raises(ValueError, match="only one place"):
            ds.get_connection_string()

    def test_bracketed_ipv6_host_with_embedded_port_is_split(self) -> None:
        # Bracketed IPv6 with an embedded port and no separate port field.
        ds = DatasourceConfig(
            name="example",
            type="postgres",
            host="[::1]:5432",
            database="analytics",
        )
        url = make_url(ds.get_connection_string())
        assert url.host == "::1"
        assert url.port == 5432

    def test_password_without_username_is_dropped(self) -> None:
        # Parity with the pre-fix generic branch, which only emitted the
        # password when a username was present. URL.create matches this:
        # a password without a username is not rendered.
        ds = DatasourceConfig(
            name="example",
            type="postgres",
            host="db.example",
            port=5432,
            database="analytics",
            password="secret",  # NOSONAR(S2068) — test-only fixture credential, not a real secret
        )
        url = make_url(ds.get_connection_string())
        assert url.username is None
        assert url.password is None
        assert url.host == "db.example"
        assert url.database == "analytics"


class TestTimeGranularity:
    def test_period_start_week(self) -> None:
        # Wednesday 2024-03-13 -> Monday 2024-03-11
        start = TimeGranularity.WEEK.period_start(datetime.date(2024, 3, 13))
        assert start == datetime.date(2024, 3, 11)

    def test_period_end_month(self) -> None:
        end = TimeGranularity.MONTH.period_end(datetime.date(2024, 3, 15))
        assert end == datetime.date(2024, 3, 31)

    # WEEK_SUNDAY — Sunday-anchored week. period_start rounds back to
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
        query = SlayerQuery.model_validate({"population": "orders", "metrics": ["count(*)", "sum(revenue)"]})
        assert len(query.metrics) == 2
        assert query.metrics[0] == DatasetMetric(expression="count(*)")
        assert query.metrics[1] == DatasetMetric(expression="sum(revenue)")

    def test_dimensions_plain_strings(self) -> None:
        query = SlayerQuery(population="orders", dimensions=["status", "customers.name"])
        assert query.dimensions is not None
        assert len(query.dimensions) == 2
        assert isinstance(query.dimensions[0], FieldRef)
        assert isinstance(query.dimensions[1], FieldRef)
        assert query.dimensions[0].name == "status"
        assert query.dimensions[0].dataset is None
        assert query.dimensions[1].name == "name"
        assert query.dimensions[1].dataset == "customers"

    def test_fields_mixed_strings_and_dicts(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": ["count(*)", {"expression": "sum(revenue) / count(*)", "name": "aov"}],
        })
        assert len(query.metrics) == 2
        assert query.metrics[0] == DatasetMetric(expression="count(*)")
        assert query.metrics[1] == DatasetMetric(expression="sum(revenue) / count(*)", name="aov")

    def test_dimensions_mixed_strings_and_dicts(self) -> None:
        query = SlayerQuery(
            population="orders",
            dimensions=["status", {"name": "customers.name", "label": "Customer"}],
        )
        assert query.dimensions is not None
        assert len(query.dimensions) == 2
        assert isinstance(query.dimensions[1], FieldRef)
        assert query.dimensions[0].name == "status"
        assert query.dimensions[1].name == "name"
        assert query.dimensions[1].dataset == "customers"
        assert query.dimensions[1].label == "Customer"

    def test_dict_syntax_still_works(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "count(*)"}],
            "dimensions": [{"name": "status"}],
        })
        assert query.metrics[0] == DatasetMetric(expression="count(*)")
        assert query.dimensions[0].name == "status"

    def test_none_fields_and_dimensions(self) -> None:
        query = SlayerQuery(population="orders")
        assert query.metrics is None
        assert query.dimensions is None

    def test_order_column_string(self) -> None:
        item = OrderItem(field="revenue_sum", direction="desc")
        assert item.field.name == "revenue_sum"
        assert item.field.dataset is None

    def test_order_column_dotted_string(self) -> None:
        item = OrderItem(field="customers._count", direction="asc")
        assert item.field.name == "_count"
        assert item.field.dataset == "customers"

    def test_order_column_dict_still_works(self) -> None:
        item = OrderItem(field={"name": "revenue_sum"}, direction="desc")
        assert item.field.name == "revenue_sum"

    def test_time_dimension_string(self) -> None:
        td = TimeDimension(field="created_at", granularity="month")
        assert td.field == "created_at"

    def test_time_dimension_dotted_string(self) -> None:
        td = TimeDimension(field="customers.ordered_at", granularity="month")
        assert td.field == "customers.ordered_at"

    def test_time_dimension_dict_still_works(self) -> None:
        td = TimeDimension.model_validate({"dimension": {"name": "created_at"}, "granularity": "month"})
        assert td.field == "created_at"

    def test_query_with_simplified_order_and_time_dimensions(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": ["count(*)"],
            "time_dimensions": [{"field": "created_at", "granularity": "month"}],
            "order": [{"field": "_count", "direction": "desc"}],
        })
        assert query.time_dimensions is not None
        assert query.time_dimensions[0].field == "created_at"
        assert query.order is not None
        assert query.order[0].field.name == "_count"
        assert query.order[0].direction == "desc"


class TestWholePeriodsOnly:
    def test_no_textual_snap_on_the_query(self) -> None:
        # Snapping is a typed planning pass, never a filter-text rewrite.
        assert not hasattr(SlayerQuery, "snap_to_whole_periods")

    def test_period_start_quarter(self) -> None:
        start = TimeGranularity.QUARTER.period_start(datetime.date(2024, 5, 15))
        assert start == datetime.date(2024, 4, 1)


class TestCoerceFieldsAndDimensions:
    """Tests for _coerce_fields and _coerce_dimensions input validation."""

    def test_fields_scalar_string_raises(self) -> None:
        with pytest.raises(Exception, match="must be a list"):
            SlayerQuery(population="orders", fields="count")

    def test_dimensions_scalar_string_raises(self) -> None:
        with pytest.raises(Exception, match="must be a list"):
            SlayerQuery(population="orders", dimensions="status")

    def test_fields_list_of_strings_coerced(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "metrics": ["count(*)", "sum(revenue)"]})
        assert q.metrics is not None
        assert q.metrics[0].expression == "count(*)"
        assert q.metrics[1].expression == "sum(revenue)"

    def test_fields_list_of_dicts_accepted(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"expression": "count(*)", "name": "cnt"}]})
        assert q.metrics is not None
        assert q.metrics[0].expression == "count(*)"
        assert q.metrics[0].name == "cnt"

    def test_dimensions_list_of_strings_coerced(self) -> None:
        q = SlayerQuery(population="orders", dimensions=["status", "region"])
        assert q.dimensions[0].name == "status"
        assert q.dimensions[1].name == "region"

    def test_fields_none_accepted(self) -> None:
        q = SlayerQuery(population="orders", fields=None)
        assert q.metrics is None

    def test_dimensions_none_accepted(self) -> None:
        q = SlayerQuery(population="orders", dimensions=None)
        assert q.dimensions is None


class TestAggregationValidation:
    """Aggregation must require formula for non-built-in names."""

    def test_builtin_without_formula_succeeds(self) -> None:
        agg = Aggregation(name="sum")
        assert agg.name == "sum"
        assert agg.expression is None

    def test_builtin_with_formula_succeeds(self) -> None:
        agg = Aggregation(name="sum", expression="SUM({value})")
        assert agg.expression == "SUM({value})"

    def test_custom_with_formula_succeeds(self) -> None:
        agg = Aggregation(name="my_agg", expression="CUSTOM({value})")
        assert agg.name == "my_agg"
        assert agg.expression == "CUSTOM({value})"

    def test_custom_without_formula_raises(self) -> None:
        with pytest.raises(ValueError, match="not a built-in aggregation"):
            Aggregation(name="my_agg")

    def test_name_with_dot_rejected(self) -> None:
        """``Aggregation.name`` must enforce the same identifier
        rules as ``Column.name`` and ``ModelMeasure.name``. Otherwise the
        ``column_x_custom_aggs`` catalog expansion produces a same-model
        metric like ``amount_my.agg`` whose dotted name the cross-model
        flatten filter (catalog.local_metrics / local_dimensions) would
        misclassify."""
        with pytest.raises(ValueError, match="Invalid name"):
            Aggregation(name="my.agg", expression="CUSTOM({value})")

    def test_name_with_hyphen_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid name"):
            Aggregation(name="my-agg", expression="CUSTOM({value})")

    def test_name_starting_with_digit_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid name"):
            Aggregation(name="9agg", expression="CUSTOM({value})")


class TestDimensionLabel:
    def test_label_optional(self) -> None:
        d = DatasetField(name="status", sql="status", datatype=DataType.DOUBLE)
        assert d.label is None

    def test_label_set(self) -> None:
        d = DatasetField(name="status", sql="status", label="Order Status", datatype=DataType.DOUBLE)
        assert d.label == "Order Status"

    def test_label_in_model_dump(self) -> None:
        d = DatasetField(name="status", label="Order Status")
        data = d.model_dump(exclude_none=True)
        assert data["label"] == "Order Status"

    def test_label_excluded_when_none(self) -> None:
        d = DatasetField(name="status")
        data = d.model_dump(exclude_none=True)
        assert "label" not in data


class TestMeasureLabel:
    def test_label_optional(self) -> None:
        m = DatasetField(name="revenue", sql="amount", datatype=DataType.DOUBLE)
        assert m.label is None

    def test_label_set(self) -> None:
        m = DatasetField(name="revenue", sql="amount", label="Total Revenue", datatype=DataType.DOUBLE)
        assert m.label == "Total Revenue"


class TestMeasureFilter:
    def test_filter_optional(self) -> None:
        m = DatasetField(name="revenue", sql="amount", datatype=DataType.DOUBLE)
        assert m.filter is None

    def test_filter_set(self) -> None:
        m = DatasetField(name="active_revenue", sql="amount", filter="status = 'active'", datatype=DataType.DOUBLE)
        assert m.filter == "status = 'active'"

    def test_filter_in_model_dump(self) -> None:
        m = DatasetField(name="x", sql="amount", filter="status = 'active'", datatype=DataType.DOUBLE)
        data = m.model_dump(exclude_none=True)
        assert data["filter"] == "status = 'active'"

    def test_filter_excluded_when_none(self) -> None:
        m = DatasetField(name="x", sql="amount", datatype=DataType.DOUBLE)
        data = m.model_dump(exclude_none=True)
        assert "filter" not in data


class TestSubstituteVariables:
    def test_string_variable(self) -> None:

        result = bind_mode_a("status = '{status_val}'", {"status_val": "active"}, dialect=_SQLITE, field="f")
        assert result == "status = 'active'"

    def test_number_variable(self) -> None:

        result = bind_mode_a("amount > {min_amount}", {"min_amount": 100}, dialect=_SQLITE, field="f")
        assert result == "amount > 100"

    def test_float_variable(self) -> None:

        result = bind_mode_a("rate < {max_rate}", {"max_rate": 0.05}, dialect=_SQLITE, field="f")
        assert result == "rate < 0.05"

    def test_multiple_variables(self) -> None:

        result = bind_mode_a("status = '{status}' AND amount > {min}", {"status": "completed", "min": 50}, dialect=_SQLITE, field="f")
        assert result == "status = 'completed' AND amount > 50"

    def test_escaped_braces(self) -> None:

        result = bind_mode_a("name LIKE '{{prefix}}%' AND status = '{val}'", {"val": "ok"}, dialect=_SQLITE, field="f")
        assert result == "name LIKE '{prefix}%' AND status = 'ok'"

    def test_undefined_variable_raises(self) -> None:

        with pytest.raises(ValueError, match="Undefined variable 'missing'"):
            bind_mode_a("status = '{missing}'", {}, dialect=_SQLITE, field="f")

    def test_invalid_variable_name_raises(self) -> None:

        with pytest.raises(ValueError, match="Invalid variable name"):
            bind_mode_a("status = '{bad-name}'", {"bad-name": "x"}, dialect=_SQLITE, field="f")

    def test_invalid_type_raises(self) -> None:

        # A dict is neither scalar nor list/tuple → rejected. (Lists ARE now a
        # valid IN-list body, covered in
        # tests/test_mode_a_variable_substitution.py.)
        with pytest.raises(ValueError, match="must be a string, number, or list/tuple"):
            bind_mode_a("status = '{val}'", {"val": {"a": 1}}, dialect=_SQLITE, field="f")

    def test_no_variables_no_change(self) -> None:

        result = bind_mode_a("status = 'active'", {}, dialect=_SQLITE, field="f")
        assert result == "status = 'active'"

    def test_variable_in_slayer_query(self) -> None:
        """Variables field is accepted on SlayerQuery."""
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": ["count(*)"],
            "filter": ["status = '{val}'"],
            "variables": {"val": "completed"},
        })
        assert q.variables == {"val": "completed"}


def _prefix_models() -> list[SlayerDataset]:
    return [
        SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="status", datatype=DataType.TEXT),
                DatasetField(name="created_at", datatype=DataType.TIMESTAMP),
                DatasetField(name="customer_id", datatype=DataType.INT),
                DatasetField(name="revenue", sql="amount", datatype=DataType.DOUBLE),
            ],
            relationships=[DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]])],
        ),
        SlayerDataset(
            name="customers", sql_table="customers", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="name", datatype=DataType.TEXT),
                DatasetField(name="score", datatype=DataType.DOUBLE),
                DatasetField(name="region_id", datatype=DataType.INT),
            ],
            relationships=[DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]])],
        ),
        SlayerDataset(
            name="regions", sql_table="regions", data_source="test",
            fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True), DatasetField(name="name", datatype=DataType.TEXT)],
        ),
    ]


async def _prefix_sql(query: dict) -> str:
    model, *extra = _prefix_models()
    return await _engine_generate(query=SlayerQuery.model_validate(query), model=model, extra_models=extra)


_MONTH = {"dimension": "created_at", "granularity": "month"}

# (root-qualified, bare) spellings of one query.
_ROOT_QUALIFIED = {
    "dimension": ({"dimensions": ["orders.status"]}, {"dimensions": ["status"]}),
    "joined-dimension": ({"dimensions": ["orders.customers.name"]}, {"dimensions": ["customers.name"]}),
    "multihop-dimension": ({"dimensions": ["orders.customers.regions.name"]}, {"dimensions": ["customers.regions.name"]}),
    "dimension-label": ({"dimensions": [{"name": "orders.status", "label": "Status"}]},
                        {"dimensions": [{"name": "status", "label": "Status"}]}),
    "time-dimension": (
        {"time_dimensions": [{"field": "orders.created_at", "granularity": "month",
                              "date_range": ["2024-01-01", "2024-12-31"], "label": "Month"}]},
        {"time_dimensions": [{"field": "created_at", "granularity": "month",
                              "date_range": ["2024-01-01", "2024-12-31"], "label": "Month"}]},
    ),
    "formula": ({"metrics": ["sum(orders.revenue)"]}, {"metrics": ["sum(revenue)"]}),
    "star-count": ({"metrics": ["count(orders.*)"]}, {"metrics": ["count(*)"]}),
    "arithmetic": ({"metrics": ["sum(orders.revenue) / count(orders.*)"]}, {"metrics": ["sum(revenue) / count(*)"]}),
    "transform": ({"time_dimensions": [_MONTH], "metrics": ["cumsum(sum(orders.revenue))"]},
                  {"time_dimensions": [_MONTH], "metrics": ["cumsum(sum(revenue))"]}),
    "joined-formula": ({"metrics": ["avg(orders.customers.score)"]}, {"metrics": ["avg(customers.score)"]}),
    "named-formula": ({"metrics": [{"expression": "sum(orders.revenue)", "name": "rev", "label": "Revenue"}]},
                      {"metrics": [{"expression": "sum(revenue)", "name": "rev", "label": "Revenue"}]}),
    "filter": ({"metrics": ["count(*)"], "filter": ["orders.status = 'active'"]},
               {"metrics": ["count(*)"], "filter": ["status = 'active'"]}),
    "joined-filter": ({"metrics": ["count(*)"], "filter": ["orders.customers.name = 'foo'"]},
                      {"metrics": ["count(*)"], "filter": ["customers.name = 'foo'"]}),
    "transform-filter": (
        {"time_dimensions": [_MONTH], "metrics": ["count(*)"], "filter": ["change(sum(orders.revenue)) > 0"]},
        {"time_dimensions": [_MONTH], "metrics": ["count(*)"], "filter": ["change(sum(revenue)) > 0"]},
    ),
    "order-column": ({"dimensions": ["status"], "metrics": ["count(*)"], "order": [{"field": "orders.status", "direction": "desc"}]},
                     {"dimensions": ["status"], "metrics": ["count(*)"], "order": [{"field": "status", "direction": "desc"}]}),
    "order-aggregate": ({"metrics": ["count(*)"], "order": [{"field": "sum(orders.revenue)", "direction": "desc"}]},
                    {"metrics": ["count(*)"], "order": [{"field": "sum(revenue)", "direction": "desc"}]}),
    "order-multihop-aggregate": ({"metrics": ["count(*)"], "order": [{"field": "sum(orders.customers.score)"}]},
                             {"metrics": ["count(*)"], "order": [{"field": "sum(customers.score)"}]}),
    "main-time-dimension": (
        {"time_dimensions": [_MONTH], "metrics": ["change(sum(revenue))"], "main_time_dimension": "orders.created_at"},
        {"time_dimensions": [_MONTH], "metrics": ["change(sum(revenue))"], "main_time_dimension": "created_at"},
    ),
}


class TestRootQualifier:
    """A leading qualifier naming the query root reads as the bare reference."""

    @pytest.mark.parametrize("case", sorted(_ROOT_QUALIFIED))
    async def test_root_qualified_equals_bare(self, case: str) -> None:
        qualified, bare = _ROOT_QUALIFIED[case]
        assert await _prefix_sql({"population": "orders", **qualified}) == await _prefix_sql(
            {"population": "orders", **bare})

    @pytest.mark.parametrize("source", [
        {"population": "orders", "extensions": [{"source_name": "orders"}]},
        {"population": "orders", "extensions": [DatasetExtension(source_name="orders")]},
    ], ids=["extension-dict", "extension-object"])
    async def test_every_root_spec_takes_its_name(self, source: dict) -> None:
        assert await _prefix_sql({**source, "dimensions": ["orders.status"]}) == await _prefix_sql(
            {**source, "dimensions": ["status"]})

    @pytest.mark.parametrize("query", [
        {"metrics": ["count(customers.orders.id)"]},
        {"metrics": ["count(*)"], "filter": ["customers.orders.status = 'paid'"]},
    ], ids=["formula", "filter"])
    async def test_an_inner_hop_named_like_the_root_is_still_refused(self, query: dict) -> None:
        with pytest.raises(Exception, match="customers"):
            await _prefix_sql({"population": "orders", **query})

    @pytest.mark.parametrize("query", [
        {"metrics": ["count(*)"], "filter": ["reorders.status = 'active'"]},
        {"metrics": ["sum(reorders.revenue)"]},
    ], ids=["filter", "formula"])
    async def test_a_longer_model_name_is_no_root_qualifier(self, query: dict) -> None:
        with pytest.raises(Exception, match="reorders"):
            await _prefix_sql({"population": "orders", **query})


class TestColumnTypeLenientValidator:
    """A Pydantic ``before``-validator absorbs legacy lowercase type
    spellings on ``Column.datatype`` so old MCP/REST input keeps working after
    the sqlglot-aligned enum rename. No deprecation warning."""

    def test_legacy_string_maps_to_text(self) -> None:
        col = DatasetField(name="x", datatype="string")
        assert col.datatype == DataType.TEXT

    def test_legacy_number_maps_to_double(self) -> None:
        col = DatasetField(name="x", datatype="number")
        assert col.datatype == DataType.DOUBLE

    def test_legacy_integer_maps_to_int(self) -> None:
        col = DatasetField(name="x", datatype="integer")
        assert col.datatype == DataType.INT

    def test_legacy_time_maps_to_timestamp(self) -> None:
        col = DatasetField(name="x", datatype="time")
        assert col.datatype == DataType.TIMESTAMP

    def test_legacy_date_maps_to_date(self) -> None:
        col = DatasetField(name="x", datatype="date")
        assert col.datatype == DataType.DATE

    def test_legacy_boolean_maps_to_boolean(self) -> None:
        col = DatasetField(name="x", datatype="boolean")
        assert col.datatype == DataType.BOOLEAN

    def test_pseudo_type_count_drops_to_default(self) -> None:
        # Aggregation pseudo-types are gone; the lenient validator drops them
        # and the field falls through to its default.
        col = DatasetField(name="x", datatype="count")
        assert col.datatype == DataType.TEXT  # current default

    @pytest.mark.parametrize("legacy", ["sum", "avg", "min", "max", "last", "count_distinct"])
    def test_other_pseudo_types_drop_to_default(self, legacy: str) -> None:
        col = DatasetField(name="x", datatype=legacy)
        assert col.datatype == DataType.TEXT

    def test_uppercase_passes_through(self) -> None:
        # Already-canonical strings round-trip unchanged (validator no-op).
        col = DatasetField(name="x", datatype="DOUBLE")
        assert col.datatype == DataType.DOUBLE

    def test_enum_value_passes_through(self) -> None:
        col = DatasetField(name="x", datatype=DataType.INT)
        assert col.datatype == DataType.INT

    def test_legacy_lowercase_unknown_maps_to_unknown(self) -> None:
        col = DatasetField(name="x", datatype="unknown")
        assert col.datatype == DataType.UNKNOWN
        assert col.datatype.is_opaque is True


class TestOpaqueColumn:
    """Opaque (``UNKNOWN``) columns are stored + displayed but never operated
    on — the raw DB type rides along on ``db_type``."""

    def test_db_type_defaults_to_none(self) -> None:
        assert DatasetField(name="x", datatype=DataType.TEXT).db_type is None

    def test_db_type_round_trips(self) -> None:
        col = DatasetField(name="loc", datatype=DataType.UNKNOWN, db_type="point")
        assert col.db_type == "point"
        assert DatasetField.model_validate(col.model_dump()).db_type == "point"

    def test_allowed_aggregations_rejected_on_opaque_column(self) -> None:
        # Built outside the raises block so only the SlayerModel construction
        # can produce the expected failure.
        opaque_col = DatasetField(
            name="loc",
            datatype=DataType.UNKNOWN,
            db_type="point",
            allowed_aggregations=["count"],
        )
        with pytest.raises(ValidationError) as exc_info:
            SlayerDataset(
                name="places",
                sql_table="places",
                data_source="ds",
                fields=[opaque_col],
            )
        msg = str(exc_info.value)
        assert "loc" in msg
        assert "point" in msg
        assert "not supported" in msg

    def test_opaque_column_without_allowed_aggregations_is_valid(self) -> None:
        model = SlayerDataset(
            name="places",
            sql_table="places",
            data_source="ds",
            fields=[DatasetField(name="loc", datatype=DataType.UNKNOWN, db_type="point")],
        )
        assert model.fields[0].datatype is DataType.UNKNOWN


class TestModelMeasureType:
    """ModelMeasure has an optional ``type`` declaring the formula's
    result type. None (default) → no cast. Set value → outer CAST on the
    aggregation expression. Surfaces transparently on
    ``SlayerQuery.metrics[i]`` since query measures use the same shape."""

    def test_default_is_none(self) -> None:
        m = DatasetMetric(expression="count(*)")
        assert m.datatype is None

    def test_explicit_double(self) -> None:
        m = DatasetMetric(expression="count(*) / count(orders)", datatype=DataType.DOUBLE)
        assert m.datatype == DataType.DOUBLE

    def test_explicit_int(self) -> None:
        m = DatasetMetric(expression="count(*)", datatype=DataType.INT)
        assert m.datatype == DataType.INT

    def test_legacy_string_value_absorbed(self) -> None:
        # Lenient validator on ModelMeasure.datatype mirrors Column.datatype.
        m = DatasetMetric.model_validate({"expression": "count(*)", "datatype": "number"})
        assert m.datatype == DataType.DOUBLE

    def test_pseudo_type_drops_to_none(self) -> None:
        m = DatasetMetric.model_validate({"expression": "count(*)", "datatype": "count"})
        assert m.datatype is None  # default

    def test_query_inline_measure_accepts_type(self) -> None:
        # SlayerQuery.metrics[i] is a ModelMeasure — same shape, same field.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "count(*) / count(orders)", "name": "ratio", "datatype": "DOUBLE"}],
        })
        assert q.metrics is not None
        assert q.metrics[0].datatype == DataType.DOUBLE


class TestColumnSampledValuesAndDistinctCount:
    """``Column`` has a structured sibling ``sampled_values`` for
    the text ``sampled`` string, and a ``distinct_count`` integer for the
    column's total cardinality at profile time. Both default to ``None``."""

    def test_sampled_values_defaults_to_none(self) -> None:
        col = DatasetField(name="status", datatype=DataType.TEXT)
        assert col.sampled_values is None

    def test_distinct_count_defaults_to_none(self) -> None:
        col = DatasetField(name="status", datatype=DataType.TEXT)
        assert col.distinct_count is None

    def test_sampled_values_accepts_empty_list(self) -> None:
        # All-NULL profiled categorical column.
        col = DatasetField(name="status", datatype=DataType.TEXT, sampled_values=[])
        assert col.sampled_values == []

    def test_sampled_values_accepts_populated_list(self) -> None:
        col = DatasetField(
            name="status",
            datatype=DataType.TEXT,
            sampled_values=["paid", "refunded"],
        )
        assert col.sampled_values == ["paid", "refunded"]

    def test_sampled_values_round_trip_through_dict(self) -> None:
        col = DatasetField(
            name="status",
            datatype=DataType.TEXT,
            sampled_values=["a", "b", "c"],
        )
        dumped = col.model_dump()
        rebuilt = DatasetField.model_validate(dumped)
        assert rebuilt.sampled_values == ["a", "b", "c"]

    def test_distinct_count_round_trip_through_dict(self) -> None:
        col = DatasetField(
            name="status",
            datatype=DataType.TEXT,
            distinct_count=42,
        )
        dumped = col.model_dump()
        rebuilt = DatasetField.model_validate(dumped)
        assert rebuilt.distinct_count == 42

    def test_distinct_count_accepts_zero(self) -> None:
        col = DatasetField(name="status", datatype=DataType.TEXT, distinct_count=0)
        assert col.distinct_count == 0

    def test_sampled_text_string_preserved_alongside_structured_fields(self) -> None:
        col = DatasetField(
            name="status",
            datatype=DataType.TEXT,
            sampled="paid, refunded",
            sampled_values=["paid", "refunded"],
            distinct_count=2,
        )
        assert col.sampled == "paid, refunded"
        assert col.sampled_values == ["paid", "refunded"]
        assert col.distinct_count == 2


class TestSlayerModelVersionBump:
    """SlayerModel.version tracks ``CURRENT_VERSIONS['SlayerModel']`` — v8 added
    the optional ``source_kind`` field; v9 lifted the ``__`` ban and added the
    legacy-alias load-path rewrite. Both forward steps are no-op
    converters."""

    def test_slayer_model_version_matches_current(self) -> None:

        m = SlayerDataset(name="orders", sql_table="orders", data_source="ds")
        assert m.version == CURRENT_VERSIONS["SlayerModel"]
