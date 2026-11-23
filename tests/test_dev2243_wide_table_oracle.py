"""Spec: models/semantic-model-documents › Upstream's wide-table cases are the query oracle.

Each case of upstream ``compliance/wide_table/cases`` runs on its fixture model, stored as a native document over the
fixture's SQL loaded into in-process DuckDB: every case with its ``query.json`` verbatim under
``conformance="ossie-l3"``, every row case also under the default conformance. The corpus root is
``$SLAYER_OSSIE_CORPUS``, defaulting to the vendored snapshot.
"""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from tests._dev2217_fixtures import FIXTURES, STRICT, WIDE_TABLE, fixture_engine, ossie_query_error


Cell = Decimal | str | None


class Expected(BaseModel):
    fixture: str
    header: list[str] = []
    rows: list[list[Cell]] = []
    error: str | None = None


def _cell(text: str) -> Cell:
    """A value as written: empty is NULL, ``""`` the empty string, a number its exact value."""
    if text == "":
        return None
    if text == '""':
        return ""
    try:
        return Decimal(text)
    except InvalidOperation:
        return text


def _value(value: Any) -> Cell:
    """A result value compared by value (``350.00`` matches ``350``)."""
    if value is None or value == "":
        return value
    if isinstance(value, str):
        return _cell(value)
    if isinstance(value, bool):
        return str(value).upper()
    return Decimal(str(value))


def read_expected(case: Path) -> Expected:
    lines = (case / "expected.txt").read_text(encoding="utf-8").splitlines()
    fixture = next(line.split(":", 1)[1].strip() for line in lines if line.startswith("# fixture:"))
    error = next((line.removeprefix("> error:").strip() for line in lines if line.startswith("> error:")), None)
    if error is not None:
        return Expected(fixture=fixture, error=error)
    table = [[c.strip() for c in line.strip().strip("|").split("|")] for line in lines if line.startswith("|")]
    return Expected(fixture=fixture, header=table[0], rows=[[_cell(c) for c in row] for row in table[1:]])


def translate(query: dict[str, Any]) -> dict[str, Any]:
    """Ossie's JSON binding (wide_table §3.7) as a root-less SLayer query under the default conformance."""
    out = {key: value for key, value in query.items() if key != "filter"}
    if "filter" in query:
        out["filters"] = [query["filter"]]
    return out


def result_keys(query: dict[str, Any]) -> list[str]:
    """Each entry's native result key, in Ossie's output order: dimensions, then metrics."""
    keys = []
    for kind, entries in (("field", query.get("dimensions") or []), ("metric", query.get("metrics") or [])):
        keys += [entry if isinstance(entry, str) else entry.get("alias") or entry[kind] for entry in entries]
    return keys


CASES = sorted(p for p in (WIDE_TABLE / "cases").iterdir() if p.is_dir())
ROW_CASES = [c for c in CASES if c.name.startswith("a")]


def _case_param(case: Path) -> Any:
    return pytest.param(case, id=case.name)


def _query(case: Path) -> dict[str, Any]:
    return json.loads((case / "query.json").read_text(encoding="utf-8"))


def _rows(data: list[dict[str, Any]], keys: list[str]) -> Counter[tuple[Cell, ...]]:
    return Counter(tuple(_value(row[key]) for key in keys) for row in data)


def test_every_case_names_a_known_fixture() -> None:
    assert CASES
    assert {read_expected(case).fixture for case in CASES} <= set(FIXTURES)


@pytest.mark.parametrize("case", [_case_param(c) for c in CASES])
async def test_wide_table_case_under_strict(case: Path, tmp_path: Path) -> None:
    expected = read_expected(case)
    query = _query(case)
    async with fixture_engine(tmp_path, expected.fixture) as engine:
        if expected.error is not None:
            with pytest.raises(ossie_query_error()) as raised:
                await engine.execute(query, conformance=STRICT)
            assert raised.value.code == expected.error, raised.value
            return
        response = await engine.execute(query, conformance=STRICT)
    assert response.columns == expected.header
    assert _rows(response.data, expected.header) == Counter(tuple(row) for row in expected.rows)


@pytest.mark.parametrize("case", [_case_param(c) for c in ROW_CASES])
async def test_wide_table_row_case_under_the_default(case: Path, tmp_path: Path) -> None:
    expected = read_expected(case)
    query = _query(case)
    async with fixture_engine(tmp_path, expected.fixture) as engine:
        response = await engine.execute(translate(query))
    keys = result_keys(query)
    assert len(keys) == len(expected.header)
    assert _rows(response.data, keys) == Counter(tuple(row) for row in expected.rows)


async def test_the_model_scoped_metric_components_case(tmp_path: Path) -> None:
    """The model-scoped metric components case."""
    case = WIDE_TABLE / "cases" / "a08_model_metric_components"
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await engine.execute(_query(case))
    assert response.data == [{"revenue_per_customer": pytest.approx(162.5)}]


async def test_error_cases_raise_their_codes_under_strict(tmp_path: Path) -> None:
    """Error cases raise their codes under strict."""
    query = _query(WIDE_TABLE / "cases" / "e16_partial_dimension_reachability")
    async with fixture_engine(tmp_path, "retail") as engine:
        with pytest.raises(ossie_query_error()) as raised:
            await engine.execute(query, conformance=STRICT)
        assert raised.value.code == "E_PARTIAL_DIMENSION_REACHABILITY"
        assert (await engine.execute(query)).data


async def test_output_names_label_the_strict_result(tmp_path: Path) -> None:
    """Output names label the strict result."""
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await engine.execute(_query(WIDE_TABLE / "cases" / "a09_aliases"), conformance=STRICT)
    assert response.columns == ["area", "revenue"]
