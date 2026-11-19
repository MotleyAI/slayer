"""Which field reads an importer's physical reference: an exact-name field wins over a case-only namesake."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import DatasetField
from slayer.importers.physical import unreadable_fields

STATUS = DatasetField(name="status", sql="status", datatype=DataType.STRING)
LEGAL = DatasetField(name="Status", sql='"LegalType"', datatype=DataType.STRING)
DONE = DatasetField(name="done", sql="amount", datatype=DataType.FLOAT, filter="status = 'completed'")


def test_an_exact_name_column_wins_over_a_case_only_namesake() -> None:
    assert unreadable_fields([STATUS, LEGAL, DONE], dialect="duckdb") == {}


def test_a_case_only_namesake_alone_still_masks() -> None:
    found = unreadable_fields([LEGAL, DONE], dialect="duckdb")
    assert "masked by the field 'Status'" in found["done"]
