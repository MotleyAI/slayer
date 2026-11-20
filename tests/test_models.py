"""Tests for core domain models."""

import datetime

import pytest
import sqlglot
from sqlglot import exp
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
    FieldRef,
    OrderItem,
    QueryMetric,
    SlayerQuery,
    TimeDimension,
)
from slayer.sql.dialects import SqliteDialect
from slayer.sql.mode_a_variables import bind_mode_a
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._deprecated_input_helpers import deprecations, payload_of, recorded
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
        """Dotted name like 'customers.name' is auto-parsed: dataset='customers', name='name'."""
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
        """Simple name without dots leaves dataset as None."""
        ref = FieldRef(name="status")
        assert ref.dataset is None
        assert ref.name == "status"

    def test_explicit_model_not_overwritten(self) -> None:
        """If dataset is explicitly provided, dotted parsing is skipped."""
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
    """OrderItem field coercion and raw_expression capture."""

    def test_funcstyle_builtin_agg_preserved(self) -> None:
        """The functional spelling is preserved verbatim — a
        placeholder defers resolution to binding (like custom aggs)."""
        item = OrderItem(field="sum(revenue)", direction="desc")
        assert item.ref is None
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
        """Function-style custom agg with args is captured as raw_expression."""
        item = OrderItem(field="rolling_avg(revenue, window=7)", direction="asc")
        assert item.raw_expression == "rolling_avg(revenue, window=7)"

    def test_plain_column_no_raw_formula(self) -> None:
        """A plain field reference has no raw_expression."""
        item = OrderItem(field="status", direction="asc")
        assert item.field == "status"
        assert item.raw_expression is None


class TestOrderShorthandHealing:
    """Heal LLM-shorthand ``order`` items and harden direction validation.

    Shorthand ``{col: "desc"}`` (single- or multi-key) heals to canonical
    ``{"field": col, "direction": "desc"}``; direction is normalized
    (case-insensitive, ``asc``/``desc``/``ascending``/``descending``, whitespace
    trimmed) and anything else is rejected; ``OrderItem`` forbids extra keys.
    """

    # --- single-key shorthand ---

    def test_shorthand_desc(self) -> None:
        q = SlayerQuery(population="orders", order=[{"avg_processing_time_days": "desc"}])
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field == "avg_processing_time_days"
        assert q.order[0].direction == "desc"

    def test_shorthand_asc(self) -> None:
        q = SlayerQuery(population="orders", order=[{"status": "asc"}])
        assert q.order is not None
        assert q.order[0].field == "status"
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

    # --- expression key composes with existing normalization ---

    def test_shorthand_formula_key(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "order": [{"sum(revenue)": "desc"}]})
        assert q.order is not None
        assert q.order[0].ref is None
        assert q.order[0].raw_expression == "sum(revenue)"
        assert q.order[0].direction == "desc"

    def test_shorthand_formula_key_equals_canonical(self) -> None:
        shorthand = SlayerQuery.model_validate({"population": "orders", "order": [{"sum(orders.revenue)": "desc"}]})
        canonical = SlayerQuery.model_validate({
            "population": "orders",
            "order": [{"field": "sum(orders.revenue)", "direction": "desc"}],
        })
        assert shorthand.order == canonical.order

    # --- multi-key expansion (one dict -> N OrderItems, insertion order) ---

    def test_multikey_expands_in_order(self) -> None:
        q = SlayerQuery(population="orders", order=[{"a": "desc", "b": "asc"}])
        assert [(o.field, o.direction) for o in q.order or []] == [("a", "desc"), ("b", "asc")]

    def test_multikey_synonyms_and_case(self) -> None:
        q = SlayerQuery(population="orders", order=[{"a": "ASCENDING", "b": "Desc"}])
        assert [(o.field, o.direction) for o in q.order or []] == [("a", "asc"), ("b", "desc")]

    def test_multikey_partial_non_direction_raises(self) -> None:
        # Not every value is a direction -> not shorthand -> canonical validation rejects.
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[{"a": "desc", "b": "downward"}])

    # --- bare single item passed without the enclosing list gets wrapped ---

    def test_bare_dict_shorthand_wrapped(self) -> None:
        q = SlayerQuery(population="orders", order={"status": "desc"})
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field == "status"
        assert q.order[0].direction == "desc"

    def test_bare_canonical_dict_wrapped(self) -> None:
        q = SlayerQuery(population="orders", order={"field": "status", "direction": "asc"})  # pyright: ignore[reportArgumentType] — a bare dict on purpose: the coercion under test
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field == "status"
        assert q.order[0].direction == "asc"

    def test_bare_multikey_dict_wrapped(self) -> None:
        q = SlayerQuery(population="orders", order={"a": "desc", "b": "asc"})
        assert [(o.field, o.direction) for o in q.order or []] == [("a", "desc"), ("b", "asc")]

    def test_bare_orderitem_wrapped(self) -> None:
        item = OrderItem(field="status", direction="desc")
        q = SlayerQuery(population="orders", order=item)
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].field == "status"
        assert q.order[0].direction == "desc"

    # --- canonical preserved; latent uppercase-direction bug fixed ---

    def test_canonical_unchanged(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "order": [{"field": "status", "direction": "desc"}]})
        assert q.order is not None
        assert q.order[0].field == "status"
        assert q.order[0].direction == "desc"

    def test_canonical_uppercase_direction_normalized(self) -> None:
        # Pre-existing latent bug: SQL generator compares ``direction == "asc"`` strictly,
        # so a stored "ASCENDING"/"ASC" used to silently emit DESC. Now normalized.
        q = SlayerQuery.model_validate({
            "population": "orders", "order": [{"field": "orders.status", "direction": "ASCENDING"}]
        })
        assert q.order[0].direction == "asc"

    def test_the_pre_2_0_reserved_key_stays_canonical(self) -> None:
        with recorded():
            q = SlayerQuery.model_validate({"population": "orders", "order": [{"column": "desc"}]})
        assert q.order is not None
        assert q.order[0].field == "desc"
        assert q.order[0].direction == "asc"
        assert [(p.old, p.new) for p in q.deprecated_inputs] == [("column", "field")]

    def test_canonical_column_named_like_direction_word(self) -> None:
        # Single reserved 'field' key whose value happens to be a direction word
        # stays canonical (order by a field literally named "desc").
        q = SlayerQuery.model_validate({"population": "orders", "order": [{"field": "desc"}]})
        assert q.order is not None
        assert q.order[0].field == "desc"
        assert q.order[0].direction == "asc"

    # --- rejections: malformed / ambiguous never silently mangled ---

    def test_invalid_direction_value_shorthand_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[{"col": "downward"}])

    def test_invalid_direction_value_canonical_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery.model_validate({"population": "orders", "order": [{"field": "orders.status", "direction": "sideways"}]})

    def test_mixed_canonical_and_stray_direction_key_raises(self) -> None:
        # extra='forbid' rejects the stray 'b' rather than silently dropping it.
        with pytest.raises(ValidationError):
            SlayerQuery.model_validate({"population": "orders", "order": [{"field": "orders.status", "b": "asc"}]})

    def test_mixed_canonical_and_stray_nondirection_key_raises(self) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery.model_validate({"population": "orders", "order": [{"field": "orders.status", "b": "downward"}]})

    def test_direction_only_key_raises(self) -> None:
        # {"direction": "desc"} is a malformed canonical item (no field), not shorthand.
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
            SlayerQuery(population="orders", order="orders.status")

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
        assert [(o.field, o.direction) for o in q.order or []] == [
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
        assert [(o.field, o.direction) for o in q.order or []] == [("a", "desc"), ("b", "asc")]

    def test_non_string_key_not_healed_raises(self) -> None:
        # A non-string key means the dict is not treated as shorthand; it falls
        # through to OrderItem validation, which rejects it (no field).
        with pytest.raises(ValidationError):
            SlayerQuery(population="orders", order=[{1: "desc"}])

    def test_multikey_formula_keys_preserve_raw_formula(self) -> None:
        q = SlayerQuery.model_validate({
            "population": "orders",
            "order": [{"sum(revenue)": "desc", "max(amount)": "asc"}],
        })
        assert q.order is not None
        assert len(q.order) == 2
        assert q.order[0].ref is None
        assert q.order[0].raw_expression == "sum(revenue)"
        assert q.order[0].direction == "desc"
        assert q.order[1].ref is None
        assert q.order[1].raw_expression == "max(amount)"
        assert q.order[1].direction == "asc"

    def test_list_orderitem_passthrough(self) -> None:
        item = OrderItem.model_validate({"field": "sum(orders.revenue)", "direction": "desc"})
        q = SlayerQuery(population="orders", order=[item])
        assert q.order is not None
        assert len(q.order) == 1
        assert q.order[0].ref is None
        assert q.order[0].direction == "desc"

    # --- OrderItem-level direction validation + extra=forbid ---

    def test_orderitem_direction_synonym_normalized(self) -> None:
        assert OrderItem(field="orders.status", direction="DESCENDING").direction == "desc"

    def test_orderitem_direction_default_asc(self) -> None:
        assert OrderItem(field="orders.status").direction == "asc"

    def test_orderitem_invalid_direction_raises(self) -> None:
        with pytest.raises(ValidationError):
            OrderItem(field="orders.status", direction="sideways")

    def test_orderitem_extra_key_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            OrderItem(field="orders.status", direction="asc", bogus="x")


class TestSlayerModel:
    def test_get_dimension(self) -> None:
        model = SlayerDataset(
            name="test",
            sql_table="t",
            data_source="test",
            fields=[DatasetField(name="x", datatype=DataType.STRING)],
        )
        assert model.get_field("x") is not None
        assert model.get_field("y") is None

    def test_get_measure(self) -> None:
        """``get_metric`` returns a DatasetMetric by name."""
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
        """A ``DatasetMetric`` expression containing raw `OVER (...)` SQL
        cannot be parsed by SLayer's expression grammar (Python AST) and would
        produce invalid SQL on every dialect if used as a filter. Reject at
        construction time with an actionable error pointing at SLayer's
        ``rank()`` / ``first()`` / ``last()`` / ``lag()`` / ``lead()`` transforms
        or a ``DatasetField.sql``-with-window pattern.
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
        """Bare field names in dataset filters are valid."""
        model = SlayerDataset(
            name="test", sql_table="t", data_source="test",
            filters=["status == 'active'", "amount > 100"],
        )
        assert model.filters == ["status == 'active'", "amount > 100"]

    def test_filter_single_dot_allowed(self) -> None:
        """Single-dot (table.column) references in dataset filters are valid SQL."""
        model = SlayerDataset(
            name="test", sql_table="t", data_source="test",
            filters=["customers.region == 'US'"],
        )
        assert model.filters == ["customers.region == 'US'"]

    def test_filter_double_underscore_allowed(self) -> None:
        """Double-underscore alias references in dataset filters are valid."""
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
        dim = DatasetField(name="cust_name", expression="customers.name", datatype=DataType.FLOAT)
        assert dim.expression == "customers.name"

    def test_measure_sql_single_dot_unchanged(self) -> None:
        """Single-dot references in field sql are left as-is."""
        meas = DatasetField(name="total", expression="orders.amount", datatype=DataType.FLOAT)
        assert meas.expression == "orders.amount"

    def test_model_name_allows_double_underscore(self) -> None:
        # `__` is legal in dataset names (the dotted-canonical flip
        # made relationship-path ambiguity structurally impossible).
        model = SlayerDataset(name="my__dataset", sql_table="t", data_source="test")
        assert model.name == "my__dataset"

    def test_model_name_rejects_reserved_slayer_prefix(self) -> None:
        with pytest.raises(ValueError, match="__slayer_"):
            SlayerDataset(name="__slayer_x", sql_table="t", data_source="test")

    def test_dimension_name_allows_double_underscore(self) -> None:
        """__ is allowed in dimension names — used for flattened relationship paths in virtual datasets."""
        dim = DatasetField(name="stores__name")
        assert dim.name == "stores__name"

    def test_measure_name_allows_double_underscore(self) -> None:
        """__ is allowed in metric names — used for flattened relationship paths in virtual datasets."""
        meas = DatasetField(name="stores__tax_rate_sum", expression="tax_rate", datatype=DataType.FLOAT)
        assert meas.name == "stores__tax_rate_sum"

    def test_query_name_allows_double_underscore(self) -> None:
        # `__` is legal in query names.
        q = SlayerQuery(name="my__query", population="orders")
        assert q.name == "my__query"

    def test_query_name_rejects_reserved_slayer_prefix(self) -> None:
        with pytest.raises(ValueError, match="__slayer_"):
            SlayerQuery(name="__slayer_x", population="orders")

    def test_model_name_single_underscore_allowed(self) -> None:
        model = SlayerDataset(name="my_dataset", sql_table="t", data_source="test")
        assert model.name == "my_dataset"

    def test_dimension_name_single_underscore_allowed(self) -> None:
        dim = DatasetField(name="customer_name")
        assert dim.name == "customer_name"

    def test_dimension_name_rejects_dot(self) -> None:
        """Dots are path syntax, not allowed in dimension names."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            DatasetField(name="customers.name")

    def test_measure_name_rejects_dot(self) -> None:
        """Dots are path syntax, not allowed in metric names."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            DatasetField(name="customers.name_sum", expression="name", datatype=DataType.FLOAT)

    def test_dimension_name_without_dot_allowed(self) -> None:
        dim = DatasetField(name="region_name")
        assert dim.name == "region_name"

    def test_measure_name_without_dot_allowed(self) -> None:
        meas = DatasetField(name="order_total_sum", expression="total", datatype=DataType.FLOAT)
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
        """``__`` is reserved as a SQL relationship-path alias separator on
        dataset/query names, but datasource names never appear in SQL
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
        """A dataset's ``data_source`` shares the same canonical-id
        namespace constraints as ``DatasourceConfig.name``."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerDataset(name="orders", sql_table="t", data_source="memory:42")

    def test_model_name_rejects_colon(self) -> None:
        """Colon is reserved as the DSL aggregation separator
        (``revenue:sum``) — dataset names sharing the shape would collide
        with expression parsing."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerDataset(name="rev:sum", sql_table="t", data_source="ds")

    def test_query_name_rejects_colon(self) -> None:
        """SlayerQuery names share the same naming space as SlayerDataset
        names (a query can be persisted as a query-backed dataset), so the
        same rejection rules apply."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            SlayerQuery(name="rev:sum", population="orders")

    def test_query_name_rejects_dot(self) -> None:
        """Dotted SlayerQuery names would collide with the dotted-path
        reference syntax used in queries."""
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            SlayerQuery(name="prod.summary", population="orders")

    def test_column_name_rejects_colon(self) -> None:
        """Field names containing ``:`` would collide with the
        aggregation colon syntax (``revenue:sum``)."""
        with pytest.raises(ValueError, match="must not contain ':'"):
            DatasetField(name="rev:sum")


class TestWithinListDuplicateNames:
    """Duplicate names within ``fields`` or within ``metrics`` are rejected.

    Codex's Major 4: today only cross-list overlap is validated, so two
    fields named ``revenue`` (or two metrics named ``aov``) silently pass
    and ``get_field`` / ``get_metric`` return the first match.
    """

    def test_duplicate_column_names_rejected(self) -> None:
        columns = [
            DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT),
            DatasetField(name="revenue", expression="net_amount", datatype=DataType.FLOAT),
        ]
        with pytest.raises(ValueError, match="duplicate.*field|field.*duplicate"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                fields=columns,
            )

    def test_duplicate_measure_names_rejected(self) -> None:
        columns = [DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT)]
        measures = [
            DatasetMetric(name="aov", expression="sum(amount) / count(*)"),
            DatasetMetric(name="aov", expression="avg(amount)"),
        ]
        with pytest.raises(ValueError, match="duplicate.*metric|metric.*duplicate"):
            SlayerDataset(
                name="orders",
                sql_table="t",
                data_source="test",
                fields=columns,
                metrics=measures,
            )

    def test_unnamed_model_measure_rejected(self) -> None:
        """A dataset metric must have a name."""
        raw = {"name": "orders", "sql_table": "t", "data_source": "test",
               "fields": [{"name": "amount", "expression": "amount", "datatype": "Float"}],
               "metrics": [{"expression": "sum(amount)"}]}
        with pytest.raises(ValueError, match="must have a name"):
            SlayerDataset.model_validate(raw)

    def test_unique_names_accepted(self) -> None:
        model = SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
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
    """A relationship targeting the dataset itself is unaddressable (relationships resolve by
    target-dataset name) and must fail loudly at construction."""

    def test_self_join_raises_informative_error(self) -> None:
        columns = [
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="manager_id", datatype=DataType.INTEGER),
        ]
        joins = [DatasetRelationship(to="staff",
                           from_columns=["manager_id"], to_columns=["id"])]
        with pytest.raises(ValueError, match="[Ss]elf-relationship") as ei:
            SlayerDataset(
                name="staff", data_source="ds", sql_table="staff",
                fields=columns, relationships=joins,
            )
        message = str(ei.value)
        assert "staff" in message
        # The remedy is named, not just the ban.
        assert "separate dataset" in message

    def test_distinct_target_still_accepted(self) -> None:
        m = SlayerDataset(
            name="staff", data_source="ds", sql_table="staff",
            fields=[DatasetField(name="team_id", datatype=DataType.INTEGER)],
            relationships=[DatasetRelationship(to="teams",
                             from_columns=["team_id"], to_columns=["id"])],
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
                {"population": "orders", "metrics": [{"metric": "count(*)"}]}
            ],
        })
        assert m.source_queries is not None
        assert isinstance(m.source_queries[0], SlayerQuery)
        assert m.source_queries[0].metrics is not None
        assert m.source_queries[0].metrics[0].metric == "count(*)"

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
    """Build-time validation of ``DatasetField.allowed_aggregations``.

    The intersection contract: a whitelist entry must satisfy
    (1) the PK rule (only ``count`` / ``count_distinct`` for PKs),
    (2) type-default eligibility (``DEFAULT_AGGREGATIONS_BY_TYPE[col.type]``).
    Custom aggregations defined on the dataset bypass type-default eligibility
    (their expression determines applicability), but PK restrictions still apply
    and the name must be a known custom aggregation.
    """

    def test_pk_column_with_disallowed_aggregation_in_whitelist_rejected(self) -> None:
        columns = [
            DatasetField(
                name="id",
                datatype=DataType.FLOAT,
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
                    datatype=DataType.FLOAT,
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
                datatype=DataType.STRING,
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
                    datatype=DataType.STRING,
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
                    datatype=DataType.DATETIME,
                    allowed_aggregations=["min", "max"],
                ),
            ],
        )
        assert model.fields[0].allowed_aggregations == ["min", "max"]

    def test_custom_aggregation_in_whitelist_bypasses_type_check(self) -> None:
        """A custom-aggregation entry is exempt from type-default eligibility.
        Its applicability depends on the custom expression, not the type-default map.
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
                    datatype=DataType.STRING,
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
                datatype=DataType.FLOAT,
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
        """Overriding a built-in name (e.g., ``sum``) with a custom expression must
        still respect type-default eligibility — built-ins keep their type
        semantics regardless of the override expression. Only truly novel custom
        names bypass the type-default gate.
        """
        aggregations = [
            Aggregation(name="sum", expression="STRING_AGG({value}, ',')"),
        ]
        columns = [
            DatasetField(
                name="status",
                datatype=DataType.STRING,
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
        """Even custom aggregations cannot be whitelisted on a PK field —
        the PK rule (count/count_distinct only) is absolute.
        """
        aggregations = [
            Aggregation(name="custom_sum", expression="SUM({value})"),
        ]
        columns = [
            DatasetField(
                name="id",
                datatype=DataType.FLOAT,
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
        query = SlayerQuery.model_validate({"population": "orders", "metrics": ["count(orders.*)", "sum(orders.revenue)"]})
        assert len(query.metrics) == 2
        assert query.metrics[0] == QueryMetric(metric="count(orders.*)")
        assert query.metrics[1] == QueryMetric(metric="sum(orders.revenue)")

    def test_dimensions_plain_strings(self) -> None:
        query = SlayerQuery(population="orders", dimensions=["status", "customers.name"])
        assert query.dimensions is not None
        assert len(query.dimensions) == 2
        assert [d.field for d in query.dimensions or []] == ["status", "customers.name"]
        assert all(d.ref is not None for d in query.dimensions or [])

    def test_fields_mixed_strings_and_dicts(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": ["count(orders.*)", {"metric": "sum(orders.revenue) / count(orders.*)", "alias": "aov"}],
        })
        assert len(query.metrics) == 2
        assert query.metrics[0] == QueryMetric(metric="count(orders.*)")
        assert query.metrics[1] == QueryMetric(metric="sum(orders.revenue) / count(orders.*)", alias="aov")

    def test_dimensions_mixed_strings_and_dicts(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["status", {"field": "customers.name", "label": "Customer"}],
        })
        assert query.dimensions is not None
        assert len(query.dimensions) == 2
        assert query.dimensions[1].ref is not None
        assert [d.field for d in query.dimensions or []] == ["status", "customers.name"]
        assert query.dimensions[1].label == "Customer"

    def test_dict_syntax_still_works(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(*)"}],
            "dimensions": ["status"],
        })
        assert query.metrics[0] == QueryMetric(metric="count(*)")
        assert query.dimensions[0].field == "status"

    def test_none_fields_and_dimensions(self) -> None:
        query = SlayerQuery(population="orders")
        assert query.metrics is None
        assert query.dimensions is None

    def test_order_column_string(self) -> None:
        ref = OrderItem(field="revenue_sum", direction="desc").ref
        assert ref is not None
        assert (ref.dataset, ref.name) == (None, "revenue_sum")

    def test_order_column_dotted_string(self) -> None:
        ref = OrderItem(field="customers._count", direction="asc").ref
        assert ref is not None
        assert (ref.dataset, ref.name) == ("customers", "_count")

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
        assert query.order[0].field == "_count"
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
            SlayerQuery(population="orders", dimensions="orders.status")

    def test_fields_list_of_strings_coerced(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "metrics": ["count(*)", "sum(revenue)"]})
        assert q.metrics is not None
        assert q.metrics[0].metric == "count(*)"
        assert q.metrics[1].metric == "sum(revenue)"

    def test_fields_list_of_dicts_accepted(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(*)", "alias": "cnt"}]})
        assert q.metrics is not None
        assert q.metrics[0].metric == "count(*)"
        assert q.metrics[0].alias == "cnt"

    def test_dimensions_list_of_strings_coerced(self) -> None:
        q = SlayerQuery(population="orders", dimensions=["status", "region"])
        assert [d.field for d in q.dimensions or []] == ["status", "region"]

    def test_the_v1_fields_key_is_reported_as_written(self) -> None:
        with recorded() as caught:
            q = SlayerQuery.model_validate({"population": "orders", "fields": ["count(*)"]})
        assert q.metrics is not None
        assert [(p.old, p.new) for p in map(payload_of, deprecations(caught))] == [("fields", "metrics")]
        assert [(p.old, p.new) for p in q.deprecated_inputs] == [("fields", "metrics")]

    def test_fields_none_accepted(self) -> None:
        q = SlayerQuery(population="orders", fields=None)
        assert q.metrics is None

    def test_dimensions_none_accepted(self) -> None:
        q = SlayerQuery(population="orders", dimensions=None)
        assert q.dimensions is None


class TestAggregationValidation:
    """Aggregation must require expression for non-built-in names."""

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
        rules as ``DatasetField.name`` and ``DatasetMetric.name``. Otherwise the
        ``column_x_custom_aggs`` catalog expansion produces a same-dataset
        metric like ``amount_my.agg`` whose dotted name the cross-dataset
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
        d = DatasetField(name="status", expression="status", datatype=DataType.FLOAT)
        assert d.label is None

    def test_label_set(self) -> None:
        d = DatasetField(name="status", expression="status", label="Order Status", datatype=DataType.FLOAT)
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
        m = DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT)
        assert m.label is None

    def test_label_set(self) -> None:
        m = DatasetField(name="revenue", expression="amount", label="Total Revenue", datatype=DataType.FLOAT)
        assert m.label == "Total Revenue"


class TestMeasureFilter:
    def test_filter_optional(self) -> None:
        m = DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT)
        assert m.filter is None

    def test_filter_set(self) -> None:
        m = DatasetField(name="active_revenue", expression="amount", filter="status = 'active'", datatype=DataType.FLOAT)
        assert m.filter == "status = 'active'"

    def test_filter_in_model_dump(self) -> None:
        m = DatasetField(name="x", expression="amount", filter="status = 'active'", datatype=DataType.FLOAT)
        data = m.model_dump(exclude_none=True)
        assert data["filter"] == "status = 'active'"

    def test_filter_excluded_when_none(self) -> None:
        m = DatasetField(name="x", expression="amount", datatype=DataType.FLOAT)
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
            "metrics": ["count(orders.*)"],
            "filter": ["status = '{val}'"],
            "variables": {"val": "completed"},
        })
        assert q.variables == {"val": "completed"}


def _prefix_datasets() -> list[SlayerDataset]:
    return [
        SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="status", datatype=DataType.STRING),
                DatasetField(name="created_at", datatype=DataType.DATETIME),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="revenue", expression="amount", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        ),
        SlayerDataset(
            name="customers", sql_table="customers", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="name", datatype=DataType.STRING),
                DatasetField(name="score", datatype=DataType.FLOAT),
                DatasetField(name="region_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
        ),
        SlayerDataset(
            name="regions", sql_table="regions", data_source="test",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="name", datatype=DataType.STRING)],
        ),
    ]


async def _prefix_sql(query: dict) -> str:
    model, *extra = _prefix_datasets()
    return await _engine_generate(query=SlayerQuery.model_validate(query), dataset=model, extra_datasets=extra)


def _unaliased(sql: str) -> str:
    """``sql`` with each outer result alias replaced by its position."""
    tree = sqlglot.parse_one(sql)
    assert isinstance(tree, exp.Query)
    for i, name in enumerate(tree.named_selects):
        sql = sql.replace(f'"{name}"', f'"_c{i}"')
    return sql


_MONTH = {"field": "created_at", "granularity": "month"}

_ANCHORED_KEYS = {"joined-dimension", "joined-formula", "multihop-dimension"}
# (root-qualified, bare) spellings of one query.
_ROOT_QUALIFIED = {
    "dimension": ({"dimensions": ["status"]}, {"dimensions": ["orders.status"]}),
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
    "formula": ({"metrics": ["sum(revenue)"]}, {"metrics": ["sum(revenue)"]}),
    "star-count": ({"metrics": ["count(*)"]}, {"metrics": ["count(orders.*)"]}),
    "arithmetic": ({"metrics": ["sum(revenue) / count(*)"]}, {"metrics": ["sum(orders.revenue) / count(orders.*)"]}),
    "transform": ({"time_dimensions": [_MONTH], "metrics": ["cumsum(sum(orders.revenue))"]},
                  {"time_dimensions": [_MONTH], "metrics": ["cumsum(sum(revenue))"]}),
    "joined-formula": ({"metrics": ["avg(orders.customers.score)"]}, {"metrics": ["avg(customers.score)"]}),
    "named-formula": ({"metrics": [{"metric": "sum(orders.revenue)", "alias": "rev", "label": "Revenue"}]},
                      {"metrics": [{"metric": "sum(revenue)", "alias": "rev", "label": "Revenue"}]}),
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
        got = await _prefix_sql({"population": "orders", **qualified})
        expected = await _prefix_sql({"population": "orders", **bare})
        if case in _ANCHORED_KEYS:  # different anchors are different result keys
            got, expected = _unaliased(got), _unaliased(expected)
        assert got == expected

    @pytest.mark.parametrize("query", [
        {"metrics": ["count(customers.orders.id)"]},
        {"metrics": ["count(orders.*)"], "filter": ["customers.orders.status = 'paid'"]},
    ], ids=["formula", "filter"])
    async def test_an_inner_hop_named_like_the_root_is_still_refused(self, query: dict) -> None:
        with pytest.raises(Exception, match="customers"):
            await _prefix_sql({"population": "orders", **query})

    @pytest.mark.parametrize("query", [
        {"metrics": ["count(orders.*)"], "filter": ["orders.reorders.status = 'active'"]},
        {"metrics": ["sum(orders.reorders.revenue)"]},
    ], ids=["filter", "formula"])
    async def test_a_longer_model_name_is_no_root_qualifier(self, query: dict) -> None:
        with pytest.raises(Exception, match="reorders"):
            await _prefix_sql({"population": "orders", **query})


class TestColumnTypeLenientValidator:
    """A Pydantic ``before``-validator absorbs legacy lowercase type
    spellings on ``DatasetField.datatype`` so old MCP/REST input keeps working after
    the sqlglot-aligned enum rename. No deprecation warning."""

    def test_legacy_string_maps_to_text(self) -> None:
        col = DatasetField(name="x", datatype="string")
        assert col.datatype == DataType.STRING

    def test_legacy_number_maps_to_double(self) -> None:
        col = DatasetField(name="x", datatype="number")
        assert col.datatype == DataType.FLOAT

    def test_legacy_integer_maps_to_int(self) -> None:
        col = DatasetField(name="x", datatype="integer")
        assert col.datatype == DataType.INTEGER

    def test_legacy_time_maps_to_timestamp(self) -> None:
        col = DatasetField(name="x", datatype="time")
        assert col.datatype == DataType.DATETIME

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
        assert col.datatype == DataType.STRING  # current default

    @pytest.mark.parametrize("legacy", ["sum", "avg", "min", "max", "last", "count_distinct"])
    def test_other_pseudo_types_drop_to_default(self, legacy: str) -> None:
        col = DatasetField(name="x", datatype=legacy)
        assert col.datatype == DataType.STRING

    def test_uppercase_passes_through(self) -> None:
        # Already-canonical strings round-trip unchanged (validator no-op).
        col = DatasetField(name="x", datatype="Float")
        assert col.datatype == DataType.FLOAT

    def test_enum_value_passes_through(self) -> None:
        col = DatasetField(name="x", datatype=DataType.INTEGER)
        assert col.datatype == DataType.INTEGER

    def test_legacy_lowercase_unknown_maps_to_unknown(self) -> None:
        col = DatasetField(name="x", datatype="unknown")
        assert col.datatype == DataType.OPAQUE
        assert col.datatype.is_opaque is True


class TestOpaqueColumn:
    """Opaque (``UNKNOWN``) fields are stored + displayed but never operated
    on — the raw DB type rides along on ``db_type``."""

    def test_db_type_defaults_to_none(self) -> None:
        assert DatasetField(name="x", datatype=DataType.STRING).db_type is None

    def test_db_type_round_trips(self) -> None:
        col = DatasetField(name="loc", datatype=DataType.OPAQUE, db_type="point")
        assert col.db_type == "point"
        assert DatasetField.model_validate(col.model_dump()).db_type == "point"

    def test_allowed_aggregations_rejected_on_opaque_column(self) -> None:
        # Built outside the raises block so only the SlayerModel construction
        # can produce the expected failure.
        opaque_col = DatasetField(
            name="loc",
            datatype=DataType.OPAQUE,
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
            fields=[DatasetField(name="loc", datatype=DataType.OPAQUE, db_type="point")],
        )
        assert model.fields[0].datatype is DataType.OPAQUE


class TestModelMeasureType:
    """DatasetMetric has an optional ``datatype`` declaring the expression's
    result type. None (default) → no cast. Set value → outer CAST on the
    aggregation expression. Surfaces transparently on
    ``SlayerQuery.metrics[i]`` since query metrics use the same shape."""

    def test_default_is_none(self) -> None:
        m = DatasetMetric(name="m", expression="count(*)")
        assert m.datatype is None

    def test_explicit_double(self) -> None:
        m = DatasetMetric(name="m", expression="count(*) / count(orders)", datatype=DataType.FLOAT)
        assert m.datatype == DataType.FLOAT

    def test_explicit_int(self) -> None:
        m = DatasetMetric(name="m", expression="count(*)", datatype=DataType.INTEGER)
        assert m.datatype == DataType.INTEGER

    def test_legacy_string_value_absorbed(self) -> None:
        # Lenient validator on DatasetMetric.datatype mirrors DatasetField.datatype.
        m = DatasetMetric.model_validate({"name": "m", "expression": "count(*)", "datatype": "number"})
        assert m.datatype == DataType.FLOAT

    def test_pseudo_type_drops_to_none(self) -> None:
        m = DatasetMetric.model_validate({"name": "m", "expression": "count(*)", "datatype": "count"})
        assert m.datatype is None  # default

    def test_query_inline_measure_accepts_type(self) -> None:
        # SlayerQuery.metrics[i] is a ModelMeasure — same shape, same field.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(*) / count(orders)", "alias": "ratio", "datatype": "Float"}],
        })
        assert q.metrics is not None
        assert q.metrics[0].datatype == DataType.FLOAT


class TestColumnSampledValuesAndDistinctCount:
    """``DatasetField`` has a structured sibling ``sampled_values`` for
    the text ``sampled`` string, and a ``distinct_count`` integer for the
    field's total cardinality at profile time. Both default to ``None``."""

    def test_sampled_values_defaults_to_none(self) -> None:
        col = DatasetField(name="status", datatype=DataType.STRING)
        assert col.sampled_values is None

    def test_distinct_count_defaults_to_none(self) -> None:
        col = DatasetField(name="status", datatype=DataType.STRING)
        assert col.distinct_count is None

    def test_sampled_values_accepts_empty_list(self) -> None:
        # All-NULL profiled categorical field.
        col = DatasetField(name="status", datatype=DataType.STRING, sampled_values=[])
        assert col.sampled_values == []

    def test_sampled_values_accepts_populated_list(self) -> None:
        col = DatasetField(
            name="status",
            datatype=DataType.STRING,
            sampled_values=["paid", "refunded"],
        )
        assert col.sampled_values == ["paid", "refunded"]

    def test_sampled_values_round_trip_through_dict(self) -> None:
        col = DatasetField(
            name="status",
            datatype=DataType.STRING,
            sampled_values=["a", "b", "c"],
        )
        dumped = col.model_dump()
        rebuilt = DatasetField.model_validate(dumped)
        assert rebuilt.sampled_values == ["a", "b", "c"]

    def test_distinct_count_round_trip_through_dict(self) -> None:
        col = DatasetField(
            name="status",
            datatype=DataType.STRING,
            distinct_count=42,
        )
        dumped = col.model_dump()
        rebuilt = DatasetField.model_validate(dumped)
        assert rebuilt.distinct_count == 42

    def test_distinct_count_accepts_zero(self) -> None:
        col = DatasetField(name="status", datatype=DataType.STRING, distinct_count=0)
        assert col.distinct_count == 0

    def test_sampled_text_string_preserved_alongside_structured_fields(self) -> None:
        col = DatasetField(
            name="status",
            datatype=DataType.STRING,
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
