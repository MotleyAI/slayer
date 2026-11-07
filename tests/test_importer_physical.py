"""Which logical column reads an importer's physical reference: an exact-name column wins over a case-only namesake."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column
from slayer.importers.physical import unreadable_columns

STATUS = Column(name="status", sql="status", type=DataType.TEXT)
LEGAL = Column(name="Status", sql='"LegalType"', type=DataType.TEXT)
DONE = Column(name="done", sql="amount", type=DataType.DOUBLE, filter="status = 'completed'")


def test_an_exact_name_column_wins_over_a_case_only_namesake() -> None:
    assert unreadable_columns([STATUS, LEGAL, DONE], dialect="duckdb") == {}


def test_a_case_only_namesake_alone_still_masks() -> None:
    found = unreadable_columns([LEGAL, DONE], dialect="duckdb")
    assert "masked by the column 'Status'" in found["done"]
