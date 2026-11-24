"""Semantic-model format gaps of the Ossie compliance statement: each a strict xfail asserting Ossie's behaviour."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests._dev2220_compliance_gaps import GAPS, Gap


@pytest.mark.parametrize("gap", [
    pytest.param(g, id=g.id, marks=pytest.mark.xfail(strict=True, raises=g.raises, reason=g.issue)) for g in GAPS
])
async def test_gap_has_ossies_behaviour(tmp_path: Path, gap: Gap) -> None:
    await gap.probe(tmp_path)


def test_gap_ids_are_unique() -> None:
    ids = [g.id for g in GAPS]
    assert len(ids) == len(set(ids))
