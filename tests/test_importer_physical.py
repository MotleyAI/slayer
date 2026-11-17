"""Which logical column reads an importer's physical reference: an exact-name column wins over a case-only namesake."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import DatasetField
from slayer.importers.physical import unreadable_fields

STATUS = DatasetField(name="status", sql="status", datatype=DataType.TEXT)
LEGAL = DatasetField(name="Status", sql='"LegalType"', datatype=DataType.TEXT)
DONE = DatasetField(name="done", sql="amount", datatype=DataType.DOUBLE, filter="status = 'completed'")


def test_an_exact_name_column_wins_over_a_case_only_namesake() -> None:
    assert unreadable_fields([STATUS, LEGAL, DONE], dialect="duckdb") == {}


def test_a_case_only_namesake_alone_still_masks() -> None:
    found = unreadable_fields([LEGAL, DONE], dialect="duckdb")
    assert "masked by the column 'Status'" in found["done"]
