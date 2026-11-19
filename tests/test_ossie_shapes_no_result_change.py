"""Ossie's query shapes change no result: the 1.x spelling and every Ossie form reproduce the pre-change capture.

Spec: compatibility/ossie-vocabulary › Query entries take Ossie's form › Results are unchanged by the new form.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests._deprecated_input_helpers import deprecated_in, deprecations, recorded
from tests._ossie_shapes_fixtures import QUERIES, REFINEMENTS, ShapeCase, shape_golden, shape_normal
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
    """``(case, form index)``; index -1 is the 1.x spelling."""
    out: list[Any] = []
    for name, case in sorted(cases.items()):
        if case.x1 is not None:
            out.append(pytest.param(name, -1, id=f"{name}-x1"))
        out += [pytest.param(name, i, id=f"{name}-ossie{i}") for i in range(len(case.ossie))]
    return out


def _query(case: ShapeCase, form: int) -> Any:
    return fresh(case.x1 if form < 0 else case.ossie[form])


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("datasets", SPELLINGS)
@pytest.mark.parametrize(("name", "form"), _forms(QUERIES))
async def test_a_query_matches_the_capture(backend: str, datasets: Spelling, name: str, form: int) -> None:
    case = QUERIES[name]
    async with vocab_engine(backend, spelling=datasets) as engine:
        with recorded() as caught:
            response = await engine.execute(_query(case, form))
    assert shape_normal(response, ordered=case.ordered) == shape_golden(name, backend)
    if form >= 0 and datasets == "new":
        assert deprecations(caught) == []
        assert deprecated_in(response.warnings) == []


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("saved", SPELLINGS)
@pytest.mark.parametrize(("name", "form"), _forms(REFINEMENTS))
async def test_a_refinement_matches_the_capture(backend: str, saved: Spelling, name: str, form: int) -> None:
    case = REFINEMENTS[name]
    async with refine_vocab_engine(backend, spelling=saved) as engine:
        with recorded() as caught:
            response = await engine.execute(SAVED_QUERY, refine=_query(case, form))
    assert shape_normal(response, ordered=case.ordered) == shape_golden(f"refinement_{name}", backend)
    if form >= 0 and saved == "new":
        assert deprecations(caught) == []
