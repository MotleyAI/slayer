"""Spec: models/semantic-model-documents › Upstream's wide-table cases are the query oracle.

Every case runs on every Tier-1 engine in ``tests/test_ossie_gate.py`` and
``tests/integration/test_integration_ossie_gate.py``; these are the requirement's named scenarios.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from decimal import Decimal
from pathlib import Path

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import STRICT, WIDE_TABLE, ossie_query_error, sql_engine
from tests._ossie_gate import assert_logical
from tests._ossie_gate_suite import WideCase, cases, read_case


@contextlib.asynccontextmanager
async def case_engine(base: Path, case: WideCase) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over the data and model the case's script loads."""
    async with sql_engine(base, sql=case.data_path.read_text(encoding="utf-8"),
                          documents=[case.model_path.read_text(encoding="utf-8")]) as engine:
        yield engine


def test_every_case_loads_upstream_data_and_a_model() -> None:
    assert cases()
    for case in cases():
        assert case.data_path.is_file(), case.name
        assert case.model_path.is_file(), case.name


async def test_the_model_scoped_metric_components_case(tmp_path: Path) -> None:
    """The model-scoped metric components case."""
    case = read_case(WIDE_TABLE / "a08_model_metric_components.ost")
    assert (case.data, case.model) == ("retail", "retail_model")
    async with case_engine(tmp_path, case) as engine:
        response = await engine.execute(case.query)
    assert [list(r) for r in response.data] == [case.header] == [["revenue_per_customer"]]
    expected = case.rows[0][0]
    assert isinstance(expected, Decimal)
    assert_logical(response.data[0]["revenue_per_customer"], float(expected), engine="duckdb")


async def test_error_cases_raise_their_codes_under_strict(tmp_path: Path) -> None:
    """Error cases raise their codes under strict."""
    case = read_case(WIDE_TABLE / "e16_partial_dimension_reachability.ost")
    assert (case.data, case.model, case.error) == ("retail", "retail_model", "E_PARTIAL_DIMENSION_REACHABILITY")
    async with case_engine(tmp_path, case) as engine:
        with pytest.raises(ossie_query_error()) as raised:
            await engine.execute(case.query, conformance=STRICT)
        assert raised.value.code == case.error
        assert (await engine.execute(case.query)).data


async def test_output_names_label_the_strict_result(tmp_path: Path) -> None:
    """Output names label the strict result."""
    case = read_case(WIDE_TABLE / "a09_aliases.ost")
    async with case_engine(tmp_path, case) as engine:
        response = await engine.execute(case.query, conformance=STRICT)
    assert response.columns == case.header == ["area", "revenue"]
