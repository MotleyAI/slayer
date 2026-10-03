"""``slayer.ir.variables``: layer merging, query-surface substitution, saved-measure substitution."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import (
    ColumnRef,
    ComputedDimension,
    ModelExtension,
    QueryRefinement,
    SlayerQuery,
    TimeDimension,
    refine_query,
)
from slayer.ir.variables import (
    apply_variables_to_query,
    extract_placeholder_names,
    merge_query_variables,
    model_needs_substitution_pass,
    model_placeholder_names,
    substitute_model_sql_surfaces,
    substitute_variables,
)
from slayer.core.query import substitute_variables as core_sv
from slayer.core.query import extract_placeholder_names as core_epn


class TestMergeQueryVariables:
    def test_precedence_runtime_wins(self) -> None:
        merged = merge_query_variables(
            runtime={"k": "runtime"},
            stage={"k": "stage"},
            outer={"k": "outer"},
            model_defaults={"k": "model"},
        )
        assert merged["k"] == "runtime"

    def test_precedence_stage_over_outer_over_model(self) -> None:
        merged = merge_query_variables(
            runtime=None,
            stage={"k": "stage"},
            outer={"k": "outer"},
            model_defaults={"k": "model"},
        )
        assert merged["k"] == "stage"

    def test_precedence_outer_over_model(self) -> None:
        merged = merge_query_variables(
            runtime=None,
            stage=None,
            outer={"k": "outer"},
            model_defaults={"k": "model"},
        )
        assert merged["k"] == "outer"

    def test_model_defaults_only(self) -> None:
        merged = merge_query_variables(
            runtime=None,
            stage=None,
            outer=None,
            model_defaults={"k": "model"},
        )
        assert merged == {"k": "model"}

    def test_all_none_returns_empty_dict(self) -> None:
        merged = merge_query_variables(
            runtime=None,
            stage=None,
            outer=None,
            model_defaults=None,
        )
        assert merged == {}

    def test_disjoint_keys_combined(self) -> None:
        merged = merge_query_variables(
            runtime={"r": 1},
            stage={"s": 2},
            outer={"o": 3},
            model_defaults={"m": 4},
        )
        assert merged == {"r": 1, "s": 2, "o": 3, "m": 4}

    def test_empty_dicts_treated_like_none(self) -> None:
        merged = merge_query_variables(
            runtime={},
            stage={},
            outer={},
            model_defaults={"k": "v"},
        )
        assert merged == {"k": "v"}

    def test_does_not_mutate_inputs(self) -> None:
        runtime = {"k": "runtime"}
        stage = {"k": "stage"}
        outer = {"o": 3}
        model_defaults = {"m": 4}
        snapshot_runtime = dict(runtime)
        snapshot_stage = dict(stage)
        snapshot_outer = dict(outer)
        snapshot_model = dict(model_defaults)
        merge_query_variables(
            runtime=runtime,
            stage=stage,
            outer=outer,
            model_defaults=model_defaults,
        )
        assert runtime == snapshot_runtime
        assert stage == snapshot_stage
        assert outer == snapshot_outer
        assert model_defaults == snapshot_model


class TestApplyVariablesToQuery:
    def test_simple_string_substitution(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["status = '{status}'"])
        out = apply_variables_to_query(query=q, variables={"status": "active"})
        assert out.filters == ["status = 'active'"]

    def test_integer_value_inserted_as_string(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["amount > {min_amt}"])
        out = apply_variables_to_query(query=q, variables={"min_amt": 100})
        assert out.filters == ["amount > 100"]

    def test_float_value(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["rate >= {r}"])
        out = apply_variables_to_query(query=q, variables={"r": 0.5})
        assert out.filters == ["rate >= 0.5"]

    def test_returns_new_query_without_mutating_input(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["status = '{s}'"])
        assert q.filters is not None
        original_filters = list(q.filters)
        out = apply_variables_to_query(query=q, variables={"s": "active"})
        assert q.filters == original_filters
        assert out is not q
        assert out.filters == ["status = 'active'"]

    def test_double_brace_escape_to_literal_braces(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["data = '{{literal}}'"])
        out = apply_variables_to_query(query=q, variables={})
        assert out.filters == ["data = '{literal}'"]

    def test_escaped_braces_around_real_placeholder(self) -> None:
        """``{{`` and ``}}`` escape independently of a real ``{var}``."""
        q = SlayerQuery(
            source_model="orders", filters=["label = '{{{x}}}'"]
        )
        out = apply_variables_to_query(query=q, variables={"x": "abc"})
        assert out.filters == ["label = '{abc}'"]

    def test_unmatched_open_brace_left_alone(self) -> None:
        """Legacy regex leaves stray ``{`` / ``}`` characters unchanged
        when they do not form a placeholder or escape. Pin that no-op
        behaviour so the new helper does not accidentally reject existing
        filters."""
        q = SlayerQuery(
            source_model="orders",
            filters=["note = '{literal' AND x = 1"],
        )
        out = apply_variables_to_query(query=q, variables={})
        assert out.filters == ["note = '{literal' AND x = 1"]

    def test_undefined_variable_raises_valueerror(self) -> None:
        q = SlayerQuery(
            source_model="orders", filters=["status = '{undefined_var}'"]
        )
        with pytest.raises(ValueError, match="Undefined variable 'undefined_var'"):
            apply_variables_to_query(query=q, variables={"other": "x"})

    def test_invalid_variable_name_raises_valueerror(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["status = '{bad-name}'"])
        with pytest.raises(ValueError, match="Invalid variable name"):
            apply_variables_to_query(query=q, variables={})

    def test_list_value_renders_a_mode_b_tuple(self) -> None:
        """Was ``test_list_value_raises``. DEV-1730 made list values legal (the
        ``IN``-pushdown primitive), so the rejection is superseded.

        Under Mode-B's ``python`` escaping the body carries a TRAILING COMMA —
        ``('x',)`` is a Python tuple literal whereas ``('x')`` is a
        parenthesised string, so the comma is what makes a single-element list
        parse as a collection. The documented spelling puts the parentheses in
        the filter (``region in ({regions})``); this pins the substituted body
        itself.
        """
        q = SlayerQuery(source_model="orders", filters=["a in ({b})"])
        out = apply_variables_to_query(query=q, variables={"b": [1, 2, 3]})
        assert out.filters == ["a in (1, 2, 3,)"], out.filters

        single = apply_variables_to_query(
            query=SlayerQuery(source_model="orders", filters=["a in ({b})"]),
            variables={"b": ["x"]},
        )
        assert single.filters == ["a in ('x',)"], single.filters

    def test_dict_value_raises(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["a = {b}"])
        with pytest.raises(ValueError, match="must be a string, number, or list"):
            apply_variables_to_query(query=q, variables={"b": {"k": "v"}})

    def test_none_value_raises(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["a = {b}"])
        with pytest.raises(ValueError, match="must be a string, number, or list"):
            apply_variables_to_query(query=q, variables={"b": None})

    def test_filters_none_returns_copy_unchanged(self) -> None:
        q = SlayerQuery(source_model="orders")
        out = apply_variables_to_query(query=q, variables={"x": 1})
        assert out is not q
        assert out == q
        assert out.filters is None

    def test_empty_filters_list_returns_copy_unchanged(self) -> None:
        q = SlayerQuery(source_model="orders", filters=[])
        out = apply_variables_to_query(query=q, variables={"x": 1})
        assert out is not q
        assert out == q
        assert out.filters == []

    def test_filters_with_no_placeholders_returns_copy_unchanged(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["status = 'active'"])
        out = apply_variables_to_query(query=q, variables={"unused": "x"})
        assert out is not q
        assert out == q

    def test_variables_defaults_to_none_no_substitution_required(self) -> None:
        """Caller may omit ``variables`` when filters carry no placeholders.

        Matches the engine path where ``variables=None`` is the natural
        signal for "no caller-supplied overrides".
        """
        q = SlayerQuery(source_model="orders", filters=["status = 'active'"])
        out = apply_variables_to_query(query=q)
        assert out is not q
        assert out == q

    def test_variables_none_with_placeholder_raises(self) -> None:
        """``variables=None`` is equivalent to an empty dict, so a referenced
        placeholder still raises."""
        q = SlayerQuery(source_model="orders", filters=["status = '{s}'"])
        with pytest.raises(ValueError, match="Undefined variable 's'"):
            apply_variables_to_query(query=q, variables=None)

    def test_empty_filters_list_is_not_shared_with_input(self) -> None:
        """``filters=[]`` produces a fresh empty list on the output so
        downstream mutation can't bleed back into the input query."""
        q = SlayerQuery(source_model="orders", filters=[])
        out = apply_variables_to_query(query=q, variables={"x": 1})
        assert out.filters is not None
        assert out.filters is not q.filters
        out.filters.append("status = 'active'")
        assert q.filters == []

    def test_multiple_filters_all_substituted(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["status = '{s}'", "amount > {m}", "literal_only = 1"],
        )
        out = apply_variables_to_query(
            query=q, variables={"s": "active", "m": 100}
        )
        assert out.filters == [
            "status = 'active'",
            "amount > 100",
            "literal_only = 1",
        ]

    def test_multiple_substitutions_in_one_filter(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["status = '{s}' AND amount > {m}"],
        )
        out = apply_variables_to_query(
            query=q, variables={"s": "active", "m": 100}
        )
        assert out.filters == ["status = 'active' AND amount > 100"]

    def test_non_filter_fields_untouched(self) -> None:
        """Measure metadata (``label``) is not a variable surface."""
        q = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(
                    formula="amount:sum", name="rev", label="display_{x}"
                )
            ],
            filters=["status = '{s}'"],
        )
        out = apply_variables_to_query(
            query=q, variables={"s": "active", "x": "ignored"}
        )
        assert out.filters == ["status = 'active'"]
        assert out.measures is not None
        assert out.measures[0].label == "display_{x}"

    def test_measure_formula_substituted(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum * {x}", name="rev")],
            filters=["status = '{s}'"],
        )
        out = apply_variables_to_query(
            query=q, variables={"s": "active", "x": 10}
        )
        assert out.measures is not None
        assert out.measures[0].formula == "amount:sum * 10"
        assert out.measures[0].name == "rev"

    def test_applying_twice_with_same_vars_is_a_no_op_on_second_pass(self) -> None:
        """A second call has no placeholders left to substitute, so the
        result equals the first call. Pins the simple idempotence case;
        the function is NOT idempotent across `{{`/`}}` escape unwrap
        — escapes are intentionally one-shot, matching legacy."""
        q = SlayerQuery(source_model="orders", filters=["status = '{s}'"])
        once = apply_variables_to_query(query=q, variables={"s": "active"})
        twice = apply_variables_to_query(
            query=once, variables={"s": "ignored"}
        )
        assert twice == once

    def test_other_fields_preserved_after_substitution(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
            dimensions=[ColumnRef(name="status")],
            filters=["status = '{s}'"],
            limit=10,
            offset=5,
        )
        out = apply_variables_to_query(query=q, variables={"s": "active"})
        assert out.source_model == "orders"
        assert out.measures is not None
        assert len(out.measures) == 1
        assert out.measures[0].name == "rev"
        assert out.dimensions is not None
        assert [d.name for d in out.dimensions] == ["status"]
        assert out.limit == 10
        assert out.offset == 5
        assert out.filters == ["status = 'active'"]


def _q(**kw) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def _template(entry) -> str | None:
    return entry._template


class TestFormulaSurfaces:
    def test_unnamed_measure_substituted_with_template(self) -> None:
        out = apply_variables_to_query(query=_q(measures=["amount:sum * {k} / 100"]), variables={"k": 10})
        assert out.measures is not None
        (m,) = out.measures
        assert m.formula == "amount:sum * 10 / 100"
        assert m.name is None
        assert _template(m) == "amount:sum * {k} / 100"

    def test_unchanged_measure_has_no_template(self) -> None:
        out = apply_variables_to_query(query=_q(measures=["amount:sum"], filters=["a > {k}"]), variables={"k": 1})
        assert out.measures is not None
        assert _template(out.measures[0]) is None

    @pytest.mark.parametrize("dimension", ["amount * {k}", {"expression": "amount * {k}"}])
    def test_computed_dimension_substituted_name_kept(self, dimension) -> None:
        out = apply_variables_to_query(query=_q(dimensions=[dimension]), variables={"k": 10})
        assert out.dimensions is not None
        (d,) = out.dimensions
        assert isinstance(d, ComputedDimension)
        assert d.expression == "amount * 10"
        assert d.name == "amount_k"
        assert _template(d) == "amount * {k}"

    def test_unchanged_computed_dimension_has_no_template(self) -> None:
        out = apply_variables_to_query(query=_q(dimensions=[{"expression": "amount * 2"}]), variables={"k": 1})
        assert out.dimensions is not None
        assert _template(out.dimensions[0]) is None

    def test_column_dimension_untouched(self) -> None:
        out = apply_variables_to_query(query=_q(dimensions=["region"]), variables={"k": 1})
        assert out.dimensions == [ColumnRef(name="region")]

    @pytest.mark.parametrize("column", ["amount:sum * {k}", "sum(amount) * {k}"])
    def test_order_expression_substituted(self, column: str) -> None:
        out = apply_variables_to_query(
            query=_q(order=[{"column": column, "direction": "desc"}]), variables={"k": -1},
        )
        assert out.order is not None
        assert out.order[0].raw_formula == column.replace("{k}", "-1")
        assert out.order[0].direction == "desc"

    def test_date_range_bounds_substituted(self) -> None:
        q = _q(time_dimensions=[{
            "dimension": "ordered_at", "granularity": "month", "date_range": ["{start}", "{end}"],
        }])
        out = apply_variables_to_query(query=q, variables={"start": "2024-01-01", "end": "2024-02-28"})
        assert out.time_dimensions is not None
        assert out.time_dimensions[0].date_range == ["2024-01-01", "2024-02-28"]

    def test_substituted_date_range_bound_is_shape_checked(self) -> None:
        q = _q(time_dimensions=[{"dimension": "ordered_at", "granularity": "month", "date_range": ["{start}", None]}])
        with pytest.raises(ValueError) as info:
            apply_variables_to_query(query=q, variables={"start": "2025/01/01"})
        msg = str(info.value)
        for form in ("YYYY-Qn", "YYYY-MM", "last N"):
            assert form in msg, msg

    def test_equal_template_time_dimensions_collapse_after_substitution(self) -> None:
        td = {"dimension": "ordered_at", "granularity": "month", "date_range": ["{start}", None]}
        out = apply_variables_to_query(query=_q(time_dimensions=[td, dict(td)]), variables={"start": "2024-01-01"})
        assert out.time_dimensions is not None
        assert [t.date_range for t in out.time_dimensions] == [["2024-01-01", None]]

    def test_string_value_escaped_python_regime(self) -> None:
        q = _q(measures=["sum(CASE WHEN status == '{name}' THEN 1 ELSE 0 END)"])
        out = apply_variables_to_query(query=q, variables={"name": "O'Brien"})
        assert out.measures is not None
        assert out.measures[0].formula == "sum(CASE WHEN status == 'O\\'Brien' THEN 1 ELSE 0 END)"

    def test_list_value_renders_tuple_in_dimension(self) -> None:
        q = _q(dimensions=[{"expression": "region in ({regions})", "name": "ns"}])
        out = apply_variables_to_query(query=q, variables={"regions": ["North", "South"]})
        assert out.dimensions is not None
        (d,) = out.dimensions
        assert isinstance(d, ComputedDimension)
        assert d.expression == "region in ('North', 'South',)"

    @pytest.mark.parametrize("kw", [
        {"measures": ["amount:sum * {? {k} ?}"]},
        {"dimensions": [{"expression": "amount * {? {k} ?}", "name": "d"}]},
        {"order": [{"column": "amount:sum * {? {k} ?}"}]},
    ])
    def test_optional_block_rejected(self, kw) -> None:
        query = _q(**kw)
        with pytest.raises(ValueError, match=r"Optional blocks") as info:
            apply_variables_to_query(query=query, variables={"k": 1})
        assert "filters" not in str(info.value)

    @pytest.mark.parametrize("kw", [
        {"measures": ["amount:sum * {k}"]},
        {"dimensions": ["amount * {k}"]},
        {"order": [{"column": "amount:sum * {k}"}]},
    ])
    def test_undefined_variable_raises(self, kw) -> None:
        query = _q(**kw)
        with pytest.raises(ValueError, match="Undefined variable 'k'"):
            apply_variables_to_query(query=query, variables={})

    def test_invalid_variable_name_in_measure_raises(self) -> None:
        query = _q(measures=["amount:sum * {bad-name}"])
        with pytest.raises(ValueError, match="Invalid variable name"):
            apply_variables_to_query(query=query, variables={})


class TestRebuild:
    def test_fields_set_and_version_preserved(self) -> None:
        q = _q(measures=["amount:sum * {k}"])
        out = apply_variables_to_query(query=q, variables={"k": 2})
        assert out.model_fields_set == q.model_fields_set
        assert out.version == q.version

    def test_extension_source_preserved(self) -> None:
        ext = ModelExtension(source_name="orders", measures=[ModelMeasure(name="x", formula="amount:sum")])
        q = SlayerQuery.model_validate({"source_model": ext, "measures": ["amount:sum * {k}"]})
        out = apply_variables_to_query(query=q, variables={"k": 2})
        assert isinstance(out.source_model, ModelExtension)
        assert out.source_model == ext

    def test_full_inline_source_preserved(self) -> None:
        model = SlayerModel(
            name="inline_orders", sql_table="orders", data_source="ds",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
        )
        q = SlayerQuery.model_validate({"source_model": model, "measures": ["amount:sum * {k}"]})
        out = apply_variables_to_query(query=q, variables={"k": 2})
        assert isinstance(out.source_model, SlayerModel)
        assert out.source_model == model

    def test_refined_saved_query_preserved(self) -> None:
        saved = _q(dimensions=["region"], measures=["amount:sum * {k}"], limit=5)
        refined = refine_query(saved=saved, refinement=QueryRefinement.model_validate({"filters": ["status == '{s}'"]}))
        out = apply_variables_to_query(query=refined, variables={"k": 3, "s": "ok"})
        assert out.model_fields_set == refined.model_fields_set
        assert out.filters == ["status == 'ok'"]
        assert out.measures is not None
        assert out.measures[0].formula == "amount:sum * 3"
        assert out.dimensions == [ColumnRef(name="region")]
        assert out.limit == 5

    def test_input_not_mutated(self) -> None:
        q = _q(measures=["amount:sum * {k}"], dimensions=["amount * {k}"])
        apply_variables_to_query(query=q, variables={"k": 2})
        assert q.measures is not None
        assert q.measures[0].formula == "amount:sum * {k}"
        assert q.dimensions is not None
        d = q.dimensions[0]
        assert isinstance(d, ComputedDimension)
        assert d.expression == "amount * {k}"


class TestPlaceholderNames:
    def _all_surfaces(self) -> SlayerQuery:
        return _q(
            filters=["amount > {f}"],
            measures=["amount:sum * {m}"],
            dimensions=["amount * {d}"],
            order=[{"column": "amount:sum * {o}"}],
            time_dimensions=[TimeDimension.model_validate(
                {"dimension": "ordered_at", "granularity": "month", "date_range": ["{s}", None]},
            )],
        )

    def test_extract_placeholder_names_covers_every_surface(self) -> None:
        assert extract_placeholder_names(self._all_surfaces()) == {"f", "m", "d", "o", "s"}


def _model_with_measure(formula: str, **kw) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        measures=[ModelMeasure(name="scaled", formula=formula)],
        **kw,
    )


class TestSavedMeasureSubstitution:
    def test_placeholder_names_include_saved_measures(self) -> None:
        assert model_placeholder_names(_model_with_measure("amount:sum * {k}")) == {"k"}

    def test_saved_measure_block_needs_pass(self) -> None:
        assert model_needs_substitution_pass(_model_with_measure("amount:sum * {? {k} ?}"))

    def test_saved_measure_formula_substituted(self) -> None:
        out = substitute_model_sql_surfaces(
            model=_model_with_measure("amount:sum * {k}"), variables={"k": 10}, backslash_escapes=False,
        )
        assert out.measures[0].formula == "amount:sum * 10"
        assert out.measures[0].name == "scaled"

    @pytest.mark.parametrize("backslash_escapes", [False, True])
    def test_saved_measure_uses_python_regime(self, backslash_escapes: bool) -> None:
        out = substitute_model_sql_surfaces(
            model=_model_with_measure("sum(CASE WHEN status == '{n}' THEN 1 ELSE 0 END)"),
            variables={"n": "O'Brien"}, backslash_escapes=backslash_escapes,
        )
        assert out.measures[0].formula == "sum(CASE WHEN status == 'O\\'Brien' THEN 1 ELSE 0 END)"

    def test_saved_measure_optional_block_rejected(self) -> None:
        model = _model_with_measure("amount:sum * {? {k} ?}")
        with pytest.raises(ValueError, match="Optional blocks"):
            substitute_model_sql_surfaces(model=model, variables={"k": 1}, backslash_escapes=False)

    def test_saved_measure_undefined_variable_raises(self) -> None:
        model = _model_with_measure("amount:sum * {k}")
        with pytest.raises(ValueError, match="Undefined variable 'k'"):
            substitute_model_sql_surfaces(model=model, variables={"j": 1}, backslash_escapes=False)


class TestReExportsMatchCoreQuery:
    """Re-exported helpers stay symbolically identical to the originals
    so callers can import either path."""

    def test_substitute_variables_is_core_query_re_export(self) -> None:

        assert substitute_variables is core_sv

    def test_extract_placeholder_names_is_core_query_re_export(self) -> None:

        assert extract_placeholder_names is core_epn
