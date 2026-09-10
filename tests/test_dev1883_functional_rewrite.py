"""DEV-1883 — ``gran(col)`` entries rewrite to ``TimeDimension`` at construction.

Spec: openspec/changes/dev-1883-support-functional-time-granularity-form-monthcol-in-query/
specs/queries/time-dimensions (functional form in dimensions / time_dimensions).
"""
import re

import pydantic
import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension

GRANULARITIES = [g.value for g in TimeGranularity]


class TestDimensionsRewrite:
    @pytest.mark.parametrize("gran", GRANULARITIES)
    def test_every_granularity_rewrites(self, gran: str) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=[f"{gran}(created_at)"],
            measures=[{"formula": "*:count"}],
        )
        assert not q.dimensions, q.dimensions
        assert q.time_dimensions == [
            TimeDimension(dimension="created_at", granularity=gran)
        ]

    def test_case_insensitive_callee(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=["MONTH(created_at)"],
            measures=[{"formula": "*:count"}],
        )
        assert q.time_dimensions is not None
        assert q.time_dimensions[0].granularity == TimeGranularity.MONTH

    def test_dotted_join_path(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=["MONTH(customers.created_at)"],
            measures=[{"formula": "*:count"}],
        )
        assert q.time_dimensions == [
            TimeDimension(dimension="customers.created_at", granularity="month")
        ]

    def test_plain_entries_survive_in_place(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=["status", "month(created_at)"],
            measures=[{"formula": "*:count"}],
        )
        assert q.dimensions == [ColumnRef(name="status")]
        assert q.time_dimensions == [
            TimeDimension(dimension="created_at", granularity="month")
        ]

    def test_rewritten_append_after_explicit_in_appearance_order(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "updated_at", "granularity": "day"}],
            dimensions=["month(created_at)", "week(shipped_at)"],
            measures=[{"formula": "*:count"}],
        )
        assert q.time_dimensions == [
            TimeDimension(dimension="updated_at", granularity="day"),
            TimeDimension(dimension="created_at", granularity="month"),
            TimeDimension(dimension="shipped_at", granularity="week"),
        ]

    def test_canonical_serialization_moves_entry(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=["month(created_at)"],
            measures=[{"formula": "*:count"}],
        )
        dump = q.model_dump()
        assert not dump["dimensions"]
        assert [
            (td["dimension"]["name"], td["granularity"])
            for td in dump["time_dimensions"]
        ] == [("created_at", "month")]

    def test_dump_identical_to_explicit_form(self) -> None:
        functional = SlayerQuery(
            source_model="orders",
            dimensions=["month(created_at)"],
            measures=[{"formula": "*:count"}],
        )
        explicit = SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "*:count"}],
        )
        assert functional.model_dump() == explicit.model_dump()

    def test_legacy_version_input_migrates_then_rewrites(self) -> None:
        q = SlayerQuery.model_validate({
            "version": 3,
            "strict": True,
            "source_model": "orders",
            "dimensions": ["month(created_at)"],
            "measures": [{"formula": "*:count"}],
        })
        assert q.to_many_handling == "error"
        assert q.time_dimensions == [
            TimeDimension(dimension="created_at", granularity="month")
        ]


class TestTimeDimensionsStringEntries:
    def test_functional_string_entry_coerces(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=["month(created_at)"],
            measures=[{"formula": "*:count"}],
        )
        assert q.time_dimensions == [
            TimeDimension(dimension="created_at", granularity="month")
        ]

    def test_string_and_dict_entries_mix(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                {"dimension": "updated_at", "granularity": "day"},
                "month(created_at)",
            ],
            measures=[{"formula": "*:count"}],
        )
        assert q.time_dimensions == [
            TimeDimension(dimension="updated_at", granularity="day"),
            TimeDimension(dimension="created_at", granularity="month"),
        ]

    def test_bare_column_string_rejected_with_remedy(self) -> None:
        with pytest.raises(pydantic.ValidationError) as ei:
            SlayerQuery(
                source_model="orders",
                time_dimensions=["created_at"],
                measures=[{"formula": "*:count"}],
            )
        msg = str(ei.value)
        assert "created_at" in msg
        for gran in GRANULARITIES:
            assert gran in msg, f"error must name granularity {gran!r}: {msg}"
        assert "(col" in msg or re.search(r"\b\w+\(created_at\)", msg), (
            f"error must show the gran(col) form: {msg}"
        )
