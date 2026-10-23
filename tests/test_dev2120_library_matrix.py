"""Every Mode-B library function and operator has executed cases with one meaning (spec: queries/scalar-functions › One meaning on every Tier-1 dialect)."""

from __future__ import annotations

import pytest

from slayer.core.keys import SCALAR_SPECS
from tests._dev2120_fixtures import (
    EXEC_BACKENDS,
    NEW_FUNCTIONS,
    TIER1,
    Case,
    emit_lib,
    lib_engine,
    library_cases,
    required_names,
    run_case,
)

CASES = library_cases()
CASE_IDS = [c.case_id for c in CASES]


class TestRegistry:
    def test_case_ids_are_unique(self) -> None:
        assert len(set(CASE_IDS)) == len(CASE_IDS)

    def test_new_functions_are_registered(self) -> None:
        assert sorted(NEW_FUNCTIONS - set(SCALAR_SPECS)) == []

    @pytest.mark.parametrize("name", sorted(required_names()))
    def test_every_function_and_operator_has_cases(self, name: str) -> None:
        assert any(name in c.covers for c in CASES), f"no matrix case covers {name!r}"

    def test_every_registered_scalar_has_cases(self) -> None:
        covered = {n for c in CASES for n in c.covers}
        assert sorted(set(SCALAR_SPECS) - covered) == []


@pytest.fixture(scope="module", params=EXEC_BACKENDS)
async def engine(request):
    async with lib_engine(request.param) as e:
        yield e


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
async def test_executes(engine, case: Case) -> None:
    await run_case(engine, case)


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
async def test_emits(case: Case, dialect: str) -> None:
    await emit_lib(case.query(), dialect=dialect)
