"""Spec: models/semantic-model-documents › Upstream's wide-table cases are the query oracle.

Each case of upstream ``compliance/wide_table/cases`` runs on its fixture model, stored as a native document over the
fixture's SQL loaded into in-process DuckDB. The corpus root is ``$SLAYER_OSSIE_CORPUS``, defaulting to the vendored
snapshot.
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pydantic import BaseModel

from slayer.core.models import DatasourceConfig
from tests._dev2243_fixtures import CORPUS, make_storage, store_document
from tests._engine_helpers import owned_query_engine

ROOT = Path(os.environ.get("SLAYER_OSSIE_CORPUS") or CORPUS)
WIDE_TABLE = ROOT / "compliance" / "wide_table"
DS = "wide_table"
#: Fixture name → (SQL, model), per upstream's wide_table README.
FIXTURES = {
    "retail": ("retail/retail.sql", "retail/retail_model.ossie.yaml"),
    "stores": ("stores/stores.sql", "stores/stores_model.ossie.yaml"),
    "stores_ambiguous": ("stores/stores.sql", "stores/stores_ambiguous_model.ossie.yaml"),
}
#: Row cases whose metric binds through the stage's host, which has two routes to the metric's home.
OWN_HOME = frozenset({"a16_shared_dimensions_stores"})
OWN_HOME_XFAIL = "DEV-2388: a metric binds through its host, not from its own home"
ERROR_XFAIL = "DEV-2217: the strict binding does not raise Ossie's error codes yet"

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
    """Each entry's result key, in Ossie's output order: dimensions, then metrics."""
    keys = []
    for kind, entries in (("field", query.get("dimensions") or []), ("metric", query.get("metrics") or [])):
        keys += [entry if isinstance(entry, str) else entry.get("alias") or entry[kind] for entry in entries]
    return keys


def ossie_code(exc: BaseException) -> str | None:
    """The Ossie error code an exception carries."""
    for attribute in ("ossie_code", "code"):
        value = getattr(exc, attribute, None)
        if isinstance(value, str) and value.startswith("E_"):
            return value
    found = re.search(r"\bE_[A-Z_]+\b", str(exc))
    return found.group(0) if found else None


def _case_param(case: Path) -> Any:
    name = case.name
    marks = []
    if name.startswith("e"):
        marks.append(pytest.mark.xfail(strict=True, reason=ERROR_XFAIL))
    elif name in OWN_HOME:
        marks.append(pytest.mark.xfail(strict=True, reason=OWN_HOME_XFAIL))
    return pytest.param(case, id=name, marks=marks)


CASES = sorted(p for p in (WIDE_TABLE / "cases").iterdir() if p.is_dir())


def _load_fixture_data(sql: Path, database: Path) -> None:
    connection = duckdb.connect(str(database))
    try:
        connection.execute(sql.read_text(encoding="utf-8"))
    finally:
        connection.close()


async def run_case(case: Path, tmp: Path) -> tuple[Expected, dict[str, Any], Any]:
    """Load the case's fixture and submit its query; returns the expectation, the query and the response."""
    expected = read_expected(case)
    sql, model = FIXTURES[expected.fixture]
    database = tmp / f"oracle_{expected.fixture}.duckdb"
    _load_fixture_data(WIDE_TABLE / sql, database)
    storage = make_storage("yaml", tmp)
    query = json.loads((case / "query.json").read_text(encoding="utf-8"))
    async with owned_query_engine(storage=storage,
                                  datasources=[DatasourceConfig(name=DS, type="duckdb", database=str(database))]
                                  ) as engine:
        await store_document(storage, (WIDE_TABLE / model).read_text(encoding="utf-8"), data_source=DS)
        if expected.error is not None:
            with pytest.raises(Exception) as raised:  # noqa: PT011 — the case checks only the Ossie code
                await engine.execute(translate(query))
            return expected, query, raised.value
        return expected, query, await engine.execute(translate(query))


def test_every_case_names_a_known_fixture() -> None:
    assert CASES
    assert {read_expected(case).fixture for case in CASES} <= set(FIXTURES)
    assert OWN_HOME <= {case.name for case in CASES}


@pytest.mark.parametrize("case", [_case_param(c) for c in CASES])
async def test_wide_table_case(case: Path, tmp_path: Path) -> None:
    expected, query, outcome = await run_case(case, tmp_path)
    if expected.error is not None:
        assert ossie_code(outcome) == expected.error, outcome
        return
    keys = result_keys(query)
    assert len(keys) == len(expected.header)
    actual = Counter(tuple(_value(row[key]) for key in keys) for row in outcome.data)
    assert actual == Counter(tuple(row) for row in expected.rows)


async def test_the_model_scoped_metric_components_case(tmp_path: Path) -> None:
    """The model-scoped metric components case."""
    _, _, response = await run_case(WIDE_TABLE / "cases" / "a08_model_metric_components", tmp_path)
    assert response.data == [{"revenue_per_customer": pytest.approx(162.5)}]
