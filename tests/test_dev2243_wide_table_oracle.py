"""Spec: models/semantic-model-documents › Upstream's wide-table cases are the query oracle.

Every case runs on every Tier-1 engine in ``tests/test_ossie_gate.py`` and
``tests/integration/test_integration_ossie_gate.py``; these are the requirement's named scenarios.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests._dev2217_fixtures import FIXTURES, STRICT, WIDE_TABLE, fixture_engine, ossie_query_error
from tests._ossie_gate import assert_logical
from tests._ossie_gate_suite import case_query, cases, read_expected


def test_every_case_names_a_known_fixture() -> None:
    assert cases()
    assert {read_expected(case).fixture for case in cases()} <= set(FIXTURES)


async def test_the_model_scoped_metric_components_case(tmp_path: Path) -> None:
    """The model-scoped metric components case."""
    case = WIDE_TABLE / "cases" / "a08_model_metric_components"
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await engine.execute(case_query(case))
    assert [list(r) for r in response.data] == [["revenue_per_customer"]]
    assert_logical(response.data[0]["revenue_per_customer"], 162.5, engine="duckdb")


async def test_error_cases_raise_their_codes_under_strict(tmp_path: Path) -> None:
    """Error cases raise their codes under strict."""
    query = case_query(WIDE_TABLE / "cases" / "e16_partial_dimension_reachability")
    async with fixture_engine(tmp_path, "retail") as engine:
        with pytest.raises(ossie_query_error()) as raised:
            await engine.execute(query, conformance=STRICT)
        assert raised.value.code == "E_PARTIAL_DIMENSION_REACHABILITY"
        assert (await engine.execute(query)).data


async def test_output_names_label_the_strict_result(tmp_path: Path) -> None:
    """Output names label the strict result."""
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await engine.execute(case_query(WIDE_TABLE / "cases" / "a09_aliases"), conformance=STRICT)
    assert response.columns == ["area", "revenue"]
