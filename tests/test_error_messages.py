"""Stage 5 (DEV-1450) — stable str() snapshots for the new error / warning classes.

The format is fixed so tests can snapshot::

    <ErrorName>: <one-line summary>
      at <location>
      scope: <short scope summary>
      suggestion: <did-you-mean>

Each line after the summary is optional. The contract is exercised here so
later stages can rely on the exact format when binding/planning code raises.
"""

from __future__ import annotations

import warnings

import pytest

from slayer.core.errors import (
    AggregationNotAllowedError,
    AmbiguousReferenceError,
    CanonicalAliasShadowsColumnError,
    DuplicateMeasureNameError,
    IllegalScopeReferenceError,
    IllegalWindowInFilterError,
    MeasureCycleError,
    MeasureNameCollidesWithColumnError,
    MeasureRecursionLimitError,
    SlayerError,
    UnknownFunctionError,
    UnknownReferenceError,
    UnreachableFilterDroppedWarning,
)
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning


# ---------------------------------------------------------------------------
# All new errors subclass SlayerError
# ---------------------------------------------------------------------------


class TestInheritance:
    @pytest.mark.parametrize("cls", [
        UnknownReferenceError, AmbiguousReferenceError,
        IllegalScopeReferenceError, IllegalWindowInFilterError,
        AggregationNotAllowedError, UnknownFunctionError,
        MeasureRecursionLimitError, MeasureCycleError,
        DuplicateMeasureNameError, MeasureNameCollidesWithColumnError,
        CanonicalAliasShadowsColumnError,
    ])
    def test_subclasses_slayer_error(self, cls):
        assert issubclass(cls, SlayerError)

    def test_warning_subclasses_user_warning(self):
        assert issubclass(UnreachableFilterDroppedWarning, UserWarning)
        assert issubclass(SlayerNormalizationWarning, UserWarning)


# ---------------------------------------------------------------------------
# UnknownReferenceError
# ---------------------------------------------------------------------------


class TestUnknownReferenceError:
    def test_minimal(self):
        e = UnknownReferenceError(
            name="revenoo",
            scope_kind="ModelScope",
            scope_summary="orders",
        )
        s = str(e)
        assert s == (
            "UnknownReferenceError: Cannot resolve reference 'revenoo'.\n"
            "  scope: ModelScope: orders"
        )

    def test_with_suggestion(self):
        e = UnknownReferenceError(
            name="revenoo",
            scope_kind="ModelScope",
            scope_summary="orders",
            suggestion="did you mean 'revenue'?",
        )
        s = str(e)
        assert s == (
            "UnknownReferenceError: Cannot resolve reference 'revenoo'.\n"
            "  scope: ModelScope: orders\n"
            "  suggestion: did you mean 'revenue'?"
        )

    def test_carries_fields(self):
        e = UnknownReferenceError(
            name="x", scope_kind="ModelScope", scope_summary="orders",
            suggestion="hint",
        )
        assert e.name == "x"
        assert e.scope_kind == "ModelScope"
        assert e.scope_summary == "orders"
        assert e.suggestion == "hint"


# ---------------------------------------------------------------------------
# AmbiguousReferenceError
# ---------------------------------------------------------------------------


class TestAmbiguousReferenceError:
    def test_lists_candidates_sorted(self):
        e = AmbiguousReferenceError(
            name="status",
            candidates=["customers.status", "orders.status"],
        )
        assert str(e) == (
            "AmbiguousReferenceError: Reference 'status' has multiple candidates.\n"
            "  candidates: ['customers.status', 'orders.status']"
        )

    def test_sorts_candidates(self):
        # Order independence — candidates render sorted.
        e = AmbiguousReferenceError(name="x", candidates=["b", "a"])
        assert "['a', 'b']" in str(e)


# ---------------------------------------------------------------------------
# IllegalScopeReferenceError
# ---------------------------------------------------------------------------


class TestIllegalScopeReferenceError:
    def test_dunder_in_modelscope(self):
        # C8: __ in Mode-B ModelScope is illegal (unless it exact-matches
        # a legacy persisted column literal name).
        e = IllegalScopeReferenceError(
            name="customers__regions",
            scope_kind="ModelScope",
            reason="`__` in Mode-B is reserved for SQL aliases; use dotted form (e.g. 'customers.regions').",
        )
        assert str(e) == (
            "IllegalScopeReferenceError: Reference 'customers__regions' is not legal "
            "in this scope.\n"
            "  scope: ModelScope\n"
            "  reason: `__` in Mode-B is reserved for SQL aliases; use dotted form "
            "(e.g. 'customers.regions')."
        )

    def test_dotted_in_stage_schema(self):
        # DEV-1449: downstream stages see a flat schema; dotted refs are
        # illegal.
        e = IllegalScopeReferenceError(
            name="customers.regions.name",
            scope_kind="StageSchema",
            reason="dots are not join syntax in stage scope; use the flat alias.",
        )
        s = str(e)
        assert "scope: StageSchema" in s
        assert "reason: dots are not join syntax in stage scope" in s


# ---------------------------------------------------------------------------
# IllegalWindowInFilterError
# ---------------------------------------------------------------------------


class TestIllegalWindowInFilterError:
    def test_raw_over_in_filter(self):
        e = IllegalWindowInFilterError(
            filter_expr="rank() OVER (ORDER BY x) <= 3",
            source="raw OVER(...) in DSL filter",
        )
        assert str(e) == (
            "IllegalWindowInFilterError: Window expressions are not allowed in filters.\n"
            "  expr: 'rank() OVER (ORDER BY x) <= 3'\n"
            "  source: raw OVER(...) in DSL filter\n"
            "  suggestion: use a rank-family transform "
            "(e.g. `rank(<measure>) <= N`)."
        )

    def test_filter_naming_windowed_column(self):
        # Filter references a Column.sql that contains a window function.
        e = IllegalWindowInFilterError(
            filter_expr="rn <= 3",
            source="references Column 'rn' whose sql contains a window function",
        )
        s = str(e)
        assert "Window expressions are not allowed in filters" in s
        assert "Column 'rn'" in s


# ---------------------------------------------------------------------------
# AggregationNotAllowedError
# ---------------------------------------------------------------------------


class TestAggregationNotAllowedError:
    def test_type_bucket_violation(self):
        e = AggregationNotAllowedError(
            column="status",
            agg="sum",
            reason="aggregation 'sum' is numeric-only; column 'status' is TEXT.",
        )
        assert str(e) == (
            "AggregationNotAllowedError: Aggregation 'sum' is not allowed on column "
            "'status'.\n"
            "  reason: aggregation 'sum' is numeric-only; column 'status' is TEXT."
        )

    def test_pk_restriction(self):
        e = AggregationNotAllowedError(
            column="orders.id",
            agg="sum",
            reason="primary-key columns are restricted to count / count_distinct.",
        )
        assert "primary-key" in str(e)


# ---------------------------------------------------------------------------
# UnknownFunctionError
# ---------------------------------------------------------------------------


class TestUnknownFunctionError:
    def test_minimal(self):
        e = UnknownFunctionError(name="regexp_match", location="measures[0].formula")
        assert str(e) == (
            "UnknownFunctionError: Function 'regexp_match' is not allowed in Mode B.\n"
            "  at measures[0].formula\n"
            "  suggestion: move the call to a derived Column.sql (Mode A)."
        )

    def test_with_explicit_suggestion(self):
        e = UnknownFunctionError(
            name="json_extract",
            location="filters[1]",
            suggestion="move json_extract into a derived Column.sql; filter on the column instead.",
        )
        assert "json_extract into a derived Column.sql" in str(e)


# ---------------------------------------------------------------------------
# MeasureRecursionLimitError / MeasureCycleError
# ---------------------------------------------------------------------------


class TestMeasureRecursionLimitError:
    def test_renders_chain(self):
        e = MeasureRecursionLimitError(chain=["a", "b", "c", "d"], limit=32)
        s = str(e)
        assert "MeasureRecursionLimitError" in s
        assert "limit=32" in s
        assert "a → b → c → d" in s


class TestMeasureCycleError:
    def test_renders_cycle(self):
        e = MeasureCycleError(chain=["a", "b", "a"])
        assert str(e) == (
            "MeasureCycleError: Cyclic reference in named-measure expansion.\n"
            "  chain: a → b → a"
        )


# ---------------------------------------------------------------------------
# Alias-collision validations (DEV-1443)
# ---------------------------------------------------------------------------


class TestDuplicateMeasureNameError:
    def test_renders_occurrences(self):
        e = DuplicateMeasureNameError(
            name="rev",
            occurrences=["measures[0]", "measures[3]"],
        )
        assert str(e) == (
            "DuplicateMeasureNameError: Measure name 'rev' is declared more than once.\n"
            "  occurrences: ['measures[0]', 'measures[3]']"
        )


class TestMeasureNameCollidesWithColumnError:
    def test_basic(self):
        e = MeasureNameCollidesWithColumnError(name="status", model="orders")
        assert str(e) == (
            "MeasureNameCollidesWithColumnError: Declared measure name 'status' "
            "matches a source column on model 'orders'."
        )


class TestCanonicalAliasShadowsColumnError:
    def test_basic(self):
        e = CanonicalAliasShadowsColumnError(
            formula="status:count",
            canonical="status_count",
            model="orders",
        )
        assert str(e) == (
            "CanonicalAliasShadowsColumnError: Canonical alias 'status_count' for "
            "formula 'status:count' shadows a source column on model 'orders'."
        )


# ---------------------------------------------------------------------------
# Warning classes
# ---------------------------------------------------------------------------


class TestUnreachableFilterDroppedWarning:
    def test_basic(self):
        w = UnreachableFilterDroppedWarning(
            filter_text="customers.score > 5",
            reason="filter refs slots unreachable from the cross-model CTE root",
        )
        assert "customers.score > 5" in str(w)
        assert "unreachable" in str(w)


# ---------------------------------------------------------------------------
# NormalizationWarning (Pydantic) + SlayerNormalizationWarning (carrier)
# ---------------------------------------------------------------------------


class TestNormalizationWarning:
    def test_carries_fields(self):
        nw = NormalizationWarning(
            rule_id="FUNC_STYLE_AGG",
            original="sum(revenue)",
            normalized="revenue:sum",
            location="measures[0].formula",
        )
        assert nw.rule_id == "FUNC_STYLE_AGG"
        assert nw.original == "sum(revenue)"
        assert nw.normalized == "revenue:sum"
        assert nw.location == "measures[0].formula"
        assert nw.rule_doc_url is None

    def test_with_doc_url(self):
        nw = NormalizationWarning(
            rule_id="DOT_PATH_IN_SQL",
            original="customers.regions.name",
            normalized="customers__regions.name",
            location="filters[0]",
            rule_doc_url="docs/agent_input_slack.md#dot-path-in-sql",
        )
        assert nw.rule_doc_url == "docs/agent_input_slack.md#dot-path-in-sql"


class TestSlayerNormalizationWarning:
    def test_carries_payload(self):
        nw = NormalizationWarning(
            rule_id="FUNC_STYLE_AGG",
            original="sum(revenue)",
            normalized="revenue:sum",
            location="measures[0].formula",
        )
        w = SlayerNormalizationWarning(nw)
        assert w.payload is nw

    def test_message_includes_rule_and_rewrite(self):
        nw = NormalizationWarning(
            rule_id="FUNC_STYLE_AGG",
            original="sum(revenue)",
            normalized="revenue:sum",
            location="measures[0].formula",
        )
        s = str(SlayerNormalizationWarning(nw))
        assert "FUNC_STYLE_AGG" in s
        assert "sum(revenue)" in s
        assert "revenue:sum" in s

    def test_emittable_via_warnings_module(self):
        # The carrier is usable as the second arg to warnings.warn(...).
        nw = NormalizationWarning(
            rule_id="FUNC_STYLE_AGG",
            original="sum(x)", normalized="x:sum", location="measures[0]",
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            warnings.warn(SlayerNormalizationWarning(nw))
        assert len(caught) == 1
        assert issubclass(caught[0].category, SlayerNormalizationWarning)
        assert isinstance(caught[0].message, SlayerNormalizationWarning)
        assert caught[0].message.payload.rule_id == "FUNC_STYLE_AGG"
