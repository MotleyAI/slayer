"""Ossie's query shapes change no result: every Ossie form gives the 1.x spelling's rows (an alias renames a key).

Spec: compatibility/ossie-vocabulary › Query entries take Ossie's form › Results are unchanged by the new form.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests._deprecated_input_helpers import deprecated_in, deprecations, recorded
from tests._ossie_shapes_fixtures import QUERIES, REFINEMENTS, ShapeCase, shape_normal
from tests._ossie_vocabulary_fixtures import (
    BACKENDS,
    SAVED_QUERY,
    SPELLINGS,
    Spelling,
    fresh,
    refine_vocab_engine,
    vocab_engine,
)


def _forms(cases: dict[str, ShapeCase]) -> list[Any]:
    """``(case, form index)`` of every Ossie form compared with the case's reference spelling."""
    out: list[Any] = []
    for name, case in sorted(cases.items()):
        first = 0 if case.x1 is not None else 1
        out += [pytest.param(name, i, id=f"{name}-ossie{i}") for i in range(first, len(case.ossie))]
    return out


def _reference(case: ShapeCase) -> Any:
    """The 1.x spelling, else the first Ossie form."""
    return fresh(case.x1 if case.x1 is not None else case.ossie[0])


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("datasets", SPELLINGS)
@pytest.mark.parametrize(("name", "form"), _forms(QUERIES))
async def test_a_query_matches_its_reference_spelling(backend: str, datasets: Spelling, name: str, form: int) -> None:
    case = QUERIES[name]
    async with vocab_engine(backend, spelling=datasets) as engine:
        with recorded():
            reference = await engine.execute(_reference(case))
        with recorded() as caught:
            response = await engine.execute(fresh(case.ossie[form]))
    assert shape_normal(response, ordered=case.ordered)["rows"] == shape_normal(reference, ordered=case.ordered)["rows"]
    if datasets == "new":
        assert deprecations(caught) == []
        assert deprecated_in(response.warnings) == []


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("saved", SPELLINGS)
@pytest.mark.parametrize(("name", "form"), _forms(REFINEMENTS))
async def test_a_refinement_matches_its_reference_spelling(backend: str, saved: Spelling, name: str, form: int) -> None:
    case = REFINEMENTS[name]
    async with refine_vocab_engine(backend, spelling=saved) as engine:
        with recorded():
            reference = await engine.execute(SAVED_QUERY, refine=_reference(case))
        with recorded() as caught:
            response = await engine.execute(SAVED_QUERY, refine=fresh(case.ossie[form]))
    assert shape_normal(response, ordered=case.ordered)["rows"] == shape_normal(reference, ordered=case.ordered)["rows"]
    if saved == "new":
        assert deprecations(caught) == []
