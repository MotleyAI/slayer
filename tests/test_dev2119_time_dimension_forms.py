"""Time dimensions as Mode-B text: coercion, dumps, schema, functional forms, names (spec: queries/time-dimensions)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from slayer.core.errors import GranularityCallError
from slayer.core.query import SlayerQuery, TimeDimension
from tests._dev2119_fixtures import EFF

CAST_TXT = "CAST(created_txt AS DATE)"


def _q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "count(*)", "name": "n"}],
                                       **kw})


def _tds(query: SlayerQuery) -> list[TimeDimension]:
    assert query.time_dimensions is not None
    return query.time_dimensions


def _dumped_tds(query: SlayerQuery) -> list[dict]:
    return query.model_dump(exclude_none=True)["time_dimensions"]


def _td_schema() -> dict:
    defs = SlayerQuery.model_json_schema()["$defs"]
    return defs["TimeDimension"]


class TestTextCoercion:
    def test_column_alias_validates_and_dumps_as_dimension_text(self) -> None:
        query = _q(time_dimensions=[{"column": "created_at", "granularity": "month"}])
        assert _tds(query)[0].dimension == "created_at"
        assert _dumped_tds(query) == [{"dimension": "created_at", "granularity": "month"}]

    def test_dimension_keyword_takes_text(self) -> None:
        td = TimeDimension(dimension="created_at", granularity="month")
        assert td.dimension == "created_at"

    @pytest.mark.parametrize("key", ["dimension", "column"])
    def test_legacy_object_form_reads_as_dotted_text(self, key: str) -> None:
        legacy = _q(time_dimensions=[{key: {"name": "created_at", "model": "customers"}, "granularity": "month"}])
        text = _q(time_dimensions=[{"dimension": "customers.created_at", "granularity": "month"}])
        assert legacy == text
        assert _dumped_tds(legacy) == [{"dimension": "customers.created_at", "granularity": "month"}]

    def test_legacy_object_without_model_reads_as_its_name(self) -> None:
        query = _q(time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}])
        assert _tds(query)[0].dimension == "created_at"

    @pytest.mark.parametrize("text", [EFF, CAST_TXT, "coalesce(customers.signup_ts, created_at)"])
    def test_expression_text_kept_verbatim(self, text: str) -> None:
        query = _q(time_dimensions=[{"dimension": text, "granularity": "month"}])
        assert _tds(query)[0].dimension == text
        assert _dumped_tds(query) == [{"dimension": text, "granularity": "month"}]

    def test_dump_round_trips(self) -> None:
        query = _q(time_dimensions=[{"dimension": EFF, "granularity": "month", "name": "effective_at"}])
        assert SlayerQuery.model_validate(query.model_dump()) == query


class TestSchema:
    def test_dimension_is_a_string_property_without_column(self) -> None:
        props = _td_schema()["properties"]
        assert props["dimension"]["type"] == "string"
        assert "column" not in props

    def test_name_is_advertised(self) -> None:
        assert "name" in _td_schema()["properties"]


class TestFunctionalForms:
    def test_functional_dimension_over_an_expression(self) -> None:
        functional = _q(dimensions=[f"month({EFF})"])
        explicit = _q(time_dimensions=[{"dimension": EFF, "granularity": "month"}])
        assert functional == explicit
        assert functional.dimensions is None

    def test_functional_time_dimension_string_over_a_cast(self) -> None:
        functional = _q(time_dimensions=[f"month({CAST_TXT})"])
        explicit = _q(time_dimensions=[{"dimension": CAST_TXT, "granularity": "month"}])
        assert functional == explicit

    def test_functional_order_of_appearance_with_expressions(self) -> None:
        query = _q(time_dimensions=[{"dimension": "created_at", "granularity": "year"}],
                   dimensions=["region", f"month({EFF})", f"week({CAST_TXT})"])
        assert [(t.dimension, t.granularity) for t in _tds(query)] == [
            ("created_at", "year"), (EFF, "month"), (CAST_TXT, "week"),
        ]
        assert query.dimensions is not None and [d.name for d in query.dimensions] == ["region"]

    def test_non_temporal_argument_is_not_a_shape_error(self) -> None:
        query = _q(dimensions=["month(upper(status))"])
        assert [(t.dimension, t.granularity) for t in _tds(query)] == [("upper(status)", "month")]

    @pytest.mark.parametrize("entry", ["month()", "month(a, b)", "month(*)", "month(created_at, granularity=2)"])
    @pytest.mark.parametrize("field", ["dimensions", "time_dimensions"])
    def test_wrong_shapes_still_rejected(self, entry: str, field: str) -> None:
        with pytest.raises((GranularityCallError, ValidationError)) as ei:
            _q(**{field: [entry]})
        msg = str(ei.value)
        assert "gran(" in msg
        assert "month" in msg and "quarter" in msg


class TestNames:
    def test_name_field_dumps(self) -> None:
        query = _q(time_dimensions=[{"dimension": EFF, "granularity": "month", "name": "effective_at"}])
        assert _tds(query)[0].name == "effective_at"
        assert _dumped_tds(query) == [{"dimension": EFF, "granularity": "month", "name": "effective_at"}]

    def test_unnamed_has_no_name(self) -> None:
        assert _tds(_q(time_dimensions=[{"dimension": EFF, "granularity": "month"}]))[0].name is None

    def test_collision_with_a_computed_dimension(self) -> None:
        with pytest.raises(ValueError, match="'m'|\\bm\\b"):
            _q(dimensions=[{"expression": "upper(city)", "name": "m"}],
               time_dimensions=[{"dimension": EFF, "granularity": "month", "name": "m"}])

    def test_collision_between_time_dimensions(self) -> None:
        with pytest.raises(ValueError, match="'m'|\\bm\\b"):
            _q(time_dimensions=[{"dimension": EFF, "granularity": "month", "name": "m"},
                                {"dimension": "created_at", "granularity": "year", "name": "m"}])

    def test_collision_with_a_plain_dimension(self) -> None:
        with pytest.raises(ValueError, match="region"):
            _q(dimensions=["region"], time_dimensions=[{"dimension": EFF, "granularity": "month", "name": "region"}])


class TestDedupe:
    @pytest.mark.parametrize("second", [f"month({EFF})", {"dimension": EFF, "granularity": "month"}])
    def test_exact_duplicate_expression_dedupes(self, second: Any) -> None:
        query = _q(dimensions=[f"month({EFF})"], time_dimensions=[second])
        assert [(t.dimension, t.granularity) for t in _tds(query)] == [(EFF, "month")]

    def test_model_prefixed_column_dedupes(self) -> None:
        query = _q(time_dimensions=[{"dimension": "created_at", "granularity": "month"},
                                    {"dimension": "orders.created_at", "granularity": "month"}])
        assert len(_tds(query)) == 1

    @pytest.mark.parametrize("diff", [{"label": "b"}, {"date_range": ["2025-01-01", "2025-02-01"]}, {"name": "b"}])
    def test_same_text_differing_metadata_rejected(self, diff: dict) -> None:
        with pytest.raises((GranularityCallError, ValidationError), match="(?i)conflicting|collision"):
            _q(time_dimensions=[{"dimension": EFF, "granularity": "month", "label": "a"},
                                {"dimension": EFF, "granularity": "month", "label": "a", **diff}])
