"""The ``QueryTypeError`` family: constructor, rendering, hierarchy, parser, surfaces."""

from __future__ import annotations

import pytest
from mcp.types import TextContent
from pydantic import ValidationError

from slayer.core.errors import (
    AssociationError,
    CanonicalAliasShadowsColumnError,
    ComputedDimensionError,
    DimensionTypeError,
    DistinctDimensionValuesError,
    DuplicateMeasureNameError,
    MeasureNameCollidesWithColumnError,
    ModelFilterError,
    NameCollisionError,
    ParameterGrainError,
    PartitionKeyError,
    PositionTypingError,
    QueryTypeError,
    ReaggregationError,
    SlayerError,
    TimeAxisError,
    TimeDimensionColumnError,
    TransformInputError,
    UnanalyzableDependencyError,
    UnknownReferenceError,
    UnsafeJoinInputError,
    WindowDurationError,
)
from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.core.window_duration import parse_window_duration
from slayer.mcp.server import create_mcp_server
from slayer.memories.service import MemoryService
from slayer.storage.yaml_storage import YAMLStorage

FAMILIES = (
    TimeAxisError, WindowDurationError, PartitionKeyError, UnsafeJoinInputError,
    UnanalyzableDependencyError, TransformInputError, ComputedDimensionError,
    AssociationError, ParameterGrainError, ReaggregationError, NameCollisionError,
    DimensionTypeError, ModelFilterError,
)
#: Re-parented classes that take the base's keyword constructor.
KEYWORD_REPARENTED = (PositionTypingError, DistinctDimensionValuesError, TimeDimensionColumnError)


class TestBaseConstructor:
    def test_base_is_a_slayer_value_error(self) -> None:
        assert issubclass(QueryTypeError, SlayerError)
        assert issubclass(QueryTypeError, ValueError)

    def test_base_cannot_be_constructed(self) -> None:
        with pytest.raises(TypeError):
            QueryTypeError(summary="s")

    @pytest.mark.parametrize("cls", FAMILIES + KEYWORD_REPARENTED)
    def test_constructor_is_keyword_only(self, cls) -> None:
        with pytest.raises(TypeError):
            cls("s")

    @pytest.mark.parametrize("cls", FAMILIES + KEYWORD_REPARENTED)
    def test_summary_only_renders_one_line(self, cls) -> None:
        exc = cls(summary="Bad thing.")
        assert str(exc) == f"{cls.__name__}: Bad thing."
        assert (exc.summary, exc.location, exc.scope, exc.suggestion) == ("Bad thing.", None, None, None)

    def test_full_layout(self) -> None:
        exc = TimeAxisError(
            summary="Bad thing.", location="measure 'm'", scope="model orders",
            suggestion="Fix it.", extras=[("chain", "a → b")],
        )
        assert str(exc) == (
            "TimeAxisError: Bad thing.\n"
            "  at measure 'm'\n"
            "  scope: model orders\n"
            "  chain: a → b\n"
            "  suggestion: Fix it."
        )
        assert (exc.summary, exc.location, exc.scope, exc.suggestion) == (
            "Bad thing.", "measure 'm'", "model orders", "Fix it.",
        )

    def test_location_without_suggestion(self) -> None:
        exc = PartitionKeyError(summary="S.", location="transform 'rank'")
        assert str(exc) == "PartitionKeyError: S.\n  at transform 'rank'"


class TestHierarchy:
    @pytest.mark.parametrize("cls", FAMILIES + KEYWORD_REPARENTED + (
        MeasureNameCollidesWithColumnError, CanonicalAliasShadowsColumnError,
        DuplicateMeasureNameError,
    ))
    def test_every_family_is_a_query_type_error(self, cls) -> None:
        assert issubclass(cls, QueryTypeError)
        assert issubclass(cls, SlayerError)
        assert issubclass(cls, ValueError)
        assert cls is not QueryTypeError

    def test_time_dimension_column_error_is_a_time_axis_error(self) -> None:
        assert issubclass(TimeDimensionColumnError, TimeAxisError)

    @pytest.mark.parametrize("cls", (
        MeasureNameCollidesWithColumnError, CanonicalAliasShadowsColumnError,
        DuplicateMeasureNameError,
    ))
    def test_structured_collisions_are_name_collisions(self, cls) -> None:
        assert issubclass(cls, NameCollisionError)

    def test_time_axis_error_is_not_not_implemented(self) -> None:
        assert not issubclass(TimeAxisError, NotImplementedError)


class TestStructuredCollisions:
    """Structured constructors and attributes survive the move onto the family schema."""

    @pytest.mark.parametrize(("exc", "rendered"), [
        pytest.param(
            MeasureNameCollidesWithColumnError(name="amount", model="orders"),
            "MeasureNameCollidesWithColumnError: The declared measure name matches a source column "
            "on model 'orders'.\n  at measure 'amount'",
            id="measure-collides",
        ),
        pytest.param(
            CanonicalAliasShadowsColumnError(formula="sum(amount)", canonical="amount_sum", model="orders"),
            "CanonicalAliasShadowsColumnError: The canonical alias 'amount_sum' shadows a source column "
            "on model 'orders'.\n  at measure 'sum(amount)'",
            id="canonical-shadows",
        ),
        pytest.param(
            DuplicateMeasureNameError(name="total", occurrences=["sum(a)", "sum(b)"]),
            "DuplicateMeasureNameError: The measure name is declared more than once.\n"
            "  at measure 'total'\n  occurrences: ['sum(a)', 'sum(b)']",
            id="duplicate",
        ),
    ])
    def test_stable_layout(self, exc, rendered: str) -> None:
        assert str(exc) == rendered
        assert str(exc).splitlines()[0] == f"{type(exc).__name__}: {exc.summary}"
        assert exc.suggestion is None

    def test_attributes_kept(self) -> None:
        assert MeasureNameCollidesWithColumnError(name="amount", model="orders").model == "orders"
        exc = CanonicalAliasShadowsColumnError(formula="sum(amount)", canonical="amount_sum", model="orders")
        assert (exc.formula, exc.canonical, exc.model) == ("sum(amount)", "amount_sum", "orders")
        dup = DuplicateMeasureNameError(name="total", occurrences=["sum(a)", "sum(b)"])
        assert (dup.name, dup.occurrences) == ("total", ["sum(a)", "sum(b)"])
        assert "sum(b)" in str(dup)


class TestWindowDurationParser:
    @pytest.mark.parametrize("value", [90, None, "", "d90", "0d", "90x", "9 0d"])
    def test_malformed_raises_window_duration_error(self, value) -> None:
        with pytest.raises(WindowDurationError) as ei:
            parse_window_duration(value)
        assert str(ei.value).startswith("WindowDurationError: ")

    def test_non_string_names_value_and_syntax(self) -> None:
        with pytest.raises(WindowDurationError) as ei:
            parse_window_duration(90)  # pyright: ignore[reportArgumentType]
        assert "90" in ei.value.summary
        assert ei.value.suggestion == "Use syntax like '1y2m3w5d6h7min8s'."

    def test_well_formed_still_parses(self) -> None:
        assert parse_window_duration("1y7min") == [(1, "y"), (7, "min")]


class TestRawRowsConstruction:
    def _underlying(self, **kwargs) -> Exception:
        with pytest.raises(ValidationError) as ei:
            SlayerQuery(source_model="orders", distinct_dimension_values=False, **kwargs)
        ctx = ei.value.errors()[0].get("ctx") or {}
        return ctx["error"]

    def test_measures_fail_with_the_stable_format(self) -> None:
        exc = self._underlying(
            dimensions=[ColumnRef(name="status")], measures=[ModelMeasure(formula="*:count")],
        )
        assert isinstance(exc, DistinctDimensionValuesError)
        assert isinstance(exc, QueryTypeError)
        assert str(exc).startswith("DistinctDimensionValuesError: ")
        assert str(exc).splitlines()[0] == f"DistinctDimensionValuesError: {exc.summary}"
        assert exc.suggestion

    def test_no_projection_fails_with_the_stable_format(self) -> None:
        exc = self._underlying()
        assert isinstance(exc, DistinctDimensionValuesError)
        assert isinstance(exc, QueryTypeError)
        assert str(exc).startswith("DistinctDimensionValuesError: ")


async def _forget_text(server) -> str:
    blocks, _ = await server.call_tool(name="forget_memory", arguments={"id": "m1"})
    (block,) = blocks
    assert isinstance(block, TextContent)
    return block.text


class TestMcpPrefix:
    """``_format_resolution_error`` names the class once for a stably formatted error."""

    @pytest.mark.parametrize("exc", (
        TimeAxisError(summary="S.", location="measure 'm'"),
        UnknownReferenceError(name="x", scope_kind="model", scope_summary="orders"),
    ), ids=lambda e: type(e).__name__)
    async def test_class_name_once(self, exc, tmp_path, monkeypatch) -> None:
        async def _raise(self, *, identifier):  # noqa: ARG001
            raise exc

        monkeypatch.setattr(MemoryService, "forget_memory", _raise)
        server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)))
        text = await _forget_text(server)
        assert text == f"Error: {exc}"
        assert text.count(type(exc).__name__) == 1

    async def test_plain_value_error_keeps_its_prefix(self, tmp_path, monkeypatch) -> None:
        async def _raise(self, *, identifier):  # noqa: ARG001
            raise ValueError("plain")

        monkeypatch.setattr(MemoryService, "forget_memory", _raise)
        server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)))
        assert await _forget_text(server) == "Error: ValueError: plain"
