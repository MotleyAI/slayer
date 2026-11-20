"""The rename changes no result: both spellings give the same result.

Spec: compatibility/ossie-vocabulary › The rename changes no result.
"""

from __future__ import annotations

import pytest

from tests._deprecated_input_helpers import deprecated_in, deprecations, recorded
from tests._ossie_vocabulary_fixtures import (
    BACKENDS,
    GOLDEN_QUERIES,
    REFINEMENTS,
    SAVED_QUERY,
    SPELLINGS,
    Spelling,
    fresh,
    normal,
    refine_vocab_engine,
    vocab_engine,
)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("name", sorted(GOLDEN_QUERIES))
@pytest.mark.parametrize("datasets", SPELLINGS)
async def test_both_spellings_give_one_result(backend: str, name: str, datasets: Spelling) -> None:
    old, new = GOLDEN_QUERIES[name]
    async with vocab_engine(backend, spelling=datasets) as engine:
        with recorded():
            old_response = await engine.execute(fresh(old))
        with recorded() as caught:
            new_response = await engine.execute(fresh(new))
    assert normal(new_response) == normal(old_response)
    assert deprecations(caught) == []
    assert deprecated_in(new_response.warnings) == []


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("name", sorted(REFINEMENTS))
@pytest.mark.parametrize("saved", SPELLINGS)
async def test_both_refinement_spellings_give_one_result(backend: str, name: str, saved: Spelling) -> None:
    async with refine_vocab_engine(backend, spelling=saved) as engine:
        old_refinement, new_refinement = REFINEMENTS[name]
        with recorded():
            old = await engine.execute(SAVED_QUERY, refine=fresh(old_refinement))
            new = await engine.execute(SAVED_QUERY, refine=fresh(new_refinement))
    assert normal(new) == normal(old)
