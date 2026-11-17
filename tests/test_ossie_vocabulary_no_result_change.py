"""The rename changes no result: both spellings reproduce the results captured on the pre-rename code.

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
    golden,
    normal,
    refine_vocab_engine,
    vocab_engine,
)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("name", sorted(GOLDEN_QUERIES))
class TestGoldenQueries:
    @pytest.mark.parametrize("datasets", SPELLINGS)
    async def test_old_spelling_matches_the_capture(self, backend: str, name: str, datasets: Spelling) -> None:
        async with vocab_engine(backend, spelling=datasets) as engine:
            with recorded():
                response = await engine.execute(fresh(GOLDEN_QUERIES[name][0]))
        assert normal(response) == golden(name, backend)

    @pytest.mark.parametrize("datasets", SPELLINGS)
    async def test_new_spelling_matches_the_capture(self, backend: str, name: str, datasets: Spelling) -> None:
        async with vocab_engine(backend, spelling=datasets) as engine:
            with recorded() as caught:
                response = await engine.execute(fresh(GOLDEN_QUERIES[name][1]))
        assert normal(response) == golden(name, backend)
        assert deprecations(caught) == []
        assert deprecated_in(response.warnings) == []


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("name", sorted(REFINEMENTS))
class TestRefinements:
    @pytest.mark.parametrize("saved", SPELLINGS)
    @pytest.mark.parametrize("refinement", [0, 1], ids=["old_refinement", "new_refinement"])
    async def test_a_refinement_matches_the_capture(
        self, backend: str, name: str, saved: Spelling, refinement: int,
    ) -> None:
        async with refine_vocab_engine(backend, spelling=saved) as engine:
            with recorded():
                response = await engine.execute(SAVED_QUERY, refine=fresh(REFINEMENTS[name][refinement]))
        assert normal(response) == golden(f"refinement_{name}", backend)
