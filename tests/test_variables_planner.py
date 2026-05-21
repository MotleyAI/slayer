"""Stage 7b.1 (DEV-1450) — variables substitution in the new pipeline.

Pins the contract for ``slayer.engine.variables``:

- ``merge_query_variables`` collapses the four variable layers into the
  effective dict that populates ``ResolvedSourceBundle.query_variables``.
  Precedence: runtime > stage > outer > model_defaults.
- ``apply_variables_to_query`` returns a copy of the input ``SlayerQuery``
  with ``{var}`` substituted in its ``filters`` list. Idempotent.
  ``dry_run_placeholders=True`` injects the legacy ``"0"`` fill for
  unresolved placeholders.

Scope deliberately matches the legacy enrichment scope — variable
substitution touches ``SlayerQuery.filters`` only. Formulas, ``Column.sql``,
``Column.filter``, and ``SlayerModel.filters`` are NOT variable-substituted
today and this module preserves that contract.
"""

from __future__ import annotations

import pytest

from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.variables import (
    apply_variables_to_query,
    extract_placeholder_names,
    merge_query_variables,
    substitute_variables,
)


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

    def test_list_value_raises(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["a = {b}"])
        with pytest.raises(ValueError, match="must be a string or number"):
            apply_variables_to_query(query=q, variables={"b": [1, 2, 3]})

    def test_dict_value_raises(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["a = {b}"])
        with pytest.raises(ValueError, match="must be a string or number"):
            apply_variables_to_query(query=q, variables={"b": {"k": "v"}})

    def test_none_value_raises(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["a = {b}"])
        with pytest.raises(ValueError, match="must be a string or number"):
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
        """Legacy scope: only ``query.filters`` is substituted.

        Variables must NOT substitute into measure metadata (``label``,
        etc.). This pins the legacy-scope contract: ``SlayerQuery.filters``
        is the only field the helper touches.
        """
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

    def test_measure_formula_text_not_substituted(self) -> None:
        """Formula text is Mode-B DSL, not a variable surface.

        ``{x}`` inside a formula stays put even though ``x`` is supplied.
        """
        q = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount_{x}:sum", name="rev")],
            filters=["status = '{s}'"],
        )
        out = apply_variables_to_query(
            query=q, variables={"s": "active", "x": "ignored"}
        )
        assert out.measures is not None
        assert out.measures[0].formula == "amount_{x}:sum"

    def test_dry_run_placeholders_fills_undefined_with_zero(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["amount > {threshold}"])
        out = apply_variables_to_query(
            query=q, variables={}, dry_run_placeholders=True
        )
        assert out.filters == ["amount > 0"]

    def test_dry_run_placeholders_preserves_supplied_values(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["a > {x}", "b > {y}"])
        out = apply_variables_to_query(
            query=q, variables={"x": 50}, dry_run_placeholders=True
        )
        assert out.filters == ["a > 50", "b > 0"]

    def test_dry_run_placeholders_idempotent_with_no_missing_vars(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["a > {x}"])
        out = apply_variables_to_query(
            query=q, variables={"x": 5}, dry_run_placeholders=True
        )
        assert out.filters == ["a > 5"]

    def test_dry_run_placeholders_off_by_default(self) -> None:
        q = SlayerQuery(source_model="orders", filters=["amount > {threshold}"])
        with pytest.raises(ValueError, match="Undefined variable 'threshold'"):
            apply_variables_to_query(query=q, variables={})

    def test_dry_run_placeholders_does_not_mask_invalid_names(self) -> None:
        """``dry_run_placeholders`` fills missing VALID placeholders only.

        Invalid names like ``{bad-name}`` still fail ``substitute_variables``'s
        validation — the dry-run shortcut is for missing values, not for
        bypassing name validation.
        """
        q = SlayerQuery(
            source_model="orders", filters=["status = '{bad-name}'"]
        )
        with pytest.raises(ValueError, match="Invalid variable name"):
            apply_variables_to_query(
                query=q, variables={}, dry_run_placeholders=True
            )

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


class TestReExportsMatchCoreQuery:
    """Re-exported helpers stay symbolically identical to the originals
    so callers can import either path."""

    def test_substitute_variables_is_core_query_re_export(self) -> None:
        from slayer.core.query import substitute_variables as core_sv

        assert substitute_variables is core_sv

    def test_extract_placeholder_names_is_core_query_re_export(self) -> None:
        from slayer.core.query import extract_placeholder_names as core_epn

        assert extract_placeholder_names is core_epn
