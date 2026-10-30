"""Shared fixtures for list columns: the ``contractors`` / ``assignments`` / ``twins`` tables, a Python oracle for
element access and ``collect``, and the live-backend check every list-capable suite runs."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Callable
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev2120_fixtures import (
    LIST_STORAGE,
    ListStorage,
    check_library,
    contractors_engine,
    contractors_model,
    elements,
    list_cases,
    stored_contractors,
)

#: ``(id, contractor_id)``: Ann has two, Bob, Cid and Eve one each.
ASSIGNMENTS: tuple[tuple[int, int], ...] = ((1, 1), (2, 1), (3, 2), (4, 3), (5, 5))
ASSIGNMENTS_DDL = "CREATE TABLE assignments (id INTEGER, contractor_id INTEGER)"
ASSIGNMENTS_INSERT = "INSERT INTO assignments VALUES " + ", ".join(f"({a}, {c})" for a, c in ASSIGNMENTS)
#: Two identical keyless rows.
TWINS_DDL = "CREATE TABLE twins (name VARCHAR, skills VARCHAR[])"
TWINS_INSERT = "INSERT INTO twins VALUES ('X', ['go']), ('X', ['go'])"
EXTRA = (ASSIGNMENTS_DDL, ASSIGNMENTS_INSERT, TWINS_DDL, TWINS_INSERT)


def assignments_model(*, data_source: str = "test", sql_table: str = "assignments") -> SlayerModel:
    return SlayerModel(
        name="assignments", sql_table=sql_table, data_source=data_source,
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="contractor_id", type=DataType.INT)],
        joins=[ModelJoin(target_model="contractors", join_pairs=[["contractor_id", "id"]])],
    )


def twins_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="twins", sql_table="twins", data_source=data_source,
        columns=[Column(name="name", type=DataType.TEXT),
                 Column(name="skills", type=DataType.ARRAY, element_type=DataType.TEXT)],
    )


def all_models(*, data_source: str = "test", keyed: bool = True) -> list[SlayerModel]:
    return [contractors_model(data_source=data_source, keyed=keyed), assignments_model(data_source=data_source),
            twins_model(data_source=data_source)]


@contextlib.asynccontextmanager
async def list_engine(*, keyed: bool = True, models: Optional[list[SlayerModel]] = None,
                      backend: str = "duckdb") -> AsyncGenerator[SlayerQueryEngine]:
    """DuckDB (default) holding ``contractors``, ``assignments`` and ``twins``."""
    async with contractors_engine(backend, models=models or all_models(keyed=keyed),
                                  extra=EXTRA if backend == "duckdb" else ()) as engine:
        yield engine


def query(**body: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "contractors", **body})


def measure(formula: str, name: str) -> dict[str, str]:
    return {"formula": formula, "name": name}


def num(value: Any) -> Optional[float]:
    return None if value is None else float(Decimal(str(value)))


def as_list(value: Any) -> Optional[list[Any]]:
    """A list result cell as a Python list (``None`` stays ``None``)."""
    return None if value is None else list(value)


def by(resp: SlayerResponse, key: str, value: str, *, model: str = "contractors") -> dict[Any, Any]:
    return {r[f"{model}.{key}"]: r[f"{model}.{value}"] for r in resp.data}


# --------------------------------------------------------------------------- #
# Oracle over ``contractors`` as a backend stores it
# --------------------------------------------------------------------------- #
def _element_rows(rows: list[dict[str, Any]]) -> list[tuple[dict[str, Any], Any]]:
    return [(r, e) for r in rows for e in elements(r["skills"] or [])]


def _collect(values: list[Any]) -> list[Any]:
    return sorted({v for v in values if v is not None})


class Probe(BaseModel):
    """One live list query: ``read`` turns the response into what the oracle predicts."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    name: str
    query: SlayerQuery
    read: Callable[[SlayerResponse], Any]
    expected: Any


def list_probes(storage: ListStorage = ListStorage()) -> list[Probe]:
    rows = stored_contractors(storage)
    elems = _element_rows(rows)
    with_go = [r for r in rows if "go" in elements(r["skills"] or [])]
    teams = sorted({r["team"] for r in rows})
    skills = sorted({e for _, e in elems})

    def per_team(fn: Callable[[list[dict[str, Any]]], Any]) -> dict[str, Any]:
        return {t: fn([r for r in rows if r["team"] == t]) for t in teams}

    def team_elements(team_rows: list[dict[str, Any]]) -> list[Any]:
        return [e for r in team_rows for e in elements(r["skills"] or [])]

    sql_ids = {r["id"] for r in rows if "sql" in elements(r["skills"] or [])}
    assigned = [r for r in rows if r["id"] in {c for a, c in ASSIGNMENTS if a in (1, 4)}]
    return [
        Probe(name="count-elements", query=query(measures=[measure("count(skills.*)", "n")]),
              read=lambda resp: num(resp.data[0]["contractors.n"]), expected=len(elems)),
        Probe(name="element-filter",
              query=query(measures=[measure("count(*)", "n"), measure("sum(rate)", "s")],
                          filters=["skills.value = 'go'"]),
              read=lambda resp: (num(resp.data[0]["contractors.n"]), num(resp.data[0]["contractors.s"])),
              expected=(len(with_go), sum(r["rate"] for r in with_go))),
        Probe(name="element-in", query=query(measures=[measure("count(*)", "n")],
                                             filters=["skills.value IN ('go', 'sql')"]),
              read=lambda resp: num(resp.data[0]["contractors.n"]),
              expected=len([r for r in rows if {"go", "sql"} & set(elements(r["skills"] or []))])),
        Probe(name="negated-element-filter",
              query=query(dimensions=["name"], filters=["NOT (skills.value = 'go')"]),
              read=lambda resp: {r["contractors.name"] for r in resp.data},
              expected={r["name"] for r in rows if any(e != "go" for e in elements(r["skills"] or []))}),
        Probe(name="elements-per-skill",
              query=query(dimensions=["skills.value"], measures=[measure("count(skills.*)", "n")]),
              read=lambda resp: {k: num(v) for k, v in by(resp, "skills.value", "n").items()},
              expected={s: sum(1 for _, e in elems if e == s) for s in skills}),
        Probe(name="associate-per-skill",
              query=query(dimensions=["skills.value"], measures=[measure("count(*)", "n")],
                          to_many_handling="associate"),
              read=lambda resp: {k: num(v) for k, v in by(resp, "skills.value", "n").items()},
              expected={s: len({r["id"] for r, e in elems if e == s}) for s in skills}),
        Probe(name="parent-and-element",
              query=query(dimensions=["team"], measures=[measure("count(*)", "n"), measure("sum(rate)", "s"),
                                                          measure("count_distinct(skills.value)", "d")]),
              read=lambda resp: {r["contractors.team"]: (num(r["contractors.n"]), num(r["contractors.s"]),
                                                         num(r["contractors.d"])) for r in resp.data},
              expected=per_team(lambda rs: (len(rs), sum(r["rate"] for r in rs), len(set(team_elements(rs)))))),
        Probe(name="collect-city", query=query(dimensions=["team"], measures=[measure("collect(city)", "c")]),
              read=lambda resp: {k: as_list(v) for k, v in by(resp, "team", "c").items()},
              expected=per_team(lambda rs: _collect([r["city"] for r in rs]))),
        Probe(name="collect-elements",
              query=query(dimensions=["team"], measures=[measure("collect(skills.value)", "c")]),
              read=lambda resp: {k: as_list(v) for k, v in by(resp, "team", "c").items()},
              expected=per_team(lambda rs: _collect(team_elements(rs)))),
        Probe(name="collect-filter",
              query=query(dimensions=["team"], measures=[measure("collect(skills.value)", "c")],
                          filters=["contains(collect(skills.value), 'sql')"]),
              read=lambda resp: {r["contractors.team"] for r in resp.data},
              expected={t for t in teams if "sql" in team_elements([r for r in rows if r["team"] == t])}),
        Probe(name="through-to-one",
              query=SlayerQuery.model_validate({"source_model": "assignments", "dimensions": ["id"],
                                                "filters": ["contractors.skills.value = 'sql'"]}),
              read=lambda resp: {int(r["assignments.id"]) for r in resp.data},
              expected={a for a, c in ASSIGNMENTS if c in sql_ids}),
        Probe(name="elements-filtered-across-another-to-many",
              query=query(dimensions=["team"], measures=[measure("count(skills.*)", "n"), measure("count(*)", "m"),
                                                          measure("collect(skills.value)", "c")],
                          filters=["assignments.id IN (1, 4)"]),
              read=lambda resp: {r["contractors.team"]: (num(r["contractors.n"]), num(r["contractors.m"]),
                                                         as_list(r["contractors.c"])) for r in resp.data},
              expected={t: (len(team_elements(rs)), len(rs), _collect(team_elements(rs)))
                        for t in teams if (rs := [r for r in assigned if r["team"] == t])}),
    ]


async def check_lists(
    engine: SlayerQueryEngine, *, dialect: str, refused: Optional[dict[str, str]] = None,
) -> None:
    """Every list-function case and list probe on a live engine; failures are collected and reported together.

    ``refused`` maps a probe name to a text its typed refusal must contain on this server."""
    storage = LIST_STORAGE.get(dialect, ListStorage())
    await check_library(engine, cases=list_cases(storage))
    failures: list[str] = []
    for probe in list_probes(storage):
        expected_refusal = (refused or {}).get(probe.name)
        if expected_refusal is not None:
            try:
                await engine.execute(probe.query)
                failures.append(f"{probe.name}: ran, expected a refusal naming {expected_refusal!r}")
            except SlayerError as exc:
                if expected_refusal not in str(exc):
                    failures.append(f"{probe.name}: refused without {expected_refusal!r}: {exc}")
            continue
        try:
            resp = await engine.execute(probe.query)
            got = probe.read(resp)
            if got != probe.expected:
                failures.append(f"{probe.name}: got {got!r}, expected {probe.expected!r}\nSQL:\n{resp.sql}")
        except Exception as exc:  # noqa: BLE001 — report every failing probe, not the first
            failures.append(f"{probe.name}: {type(exc).__name__}: {exc}")
    assert not failures, "\n\n".join(failures)


def assignments_statements(*, dialect: str, prefix: str = "") -> list[str]:
    """CREATE + INSERT of ``assignments`` on a server backend."""
    int_type = {"clickhouse": "Int32", "bigquery": "INT64", "snowflake": "NUMBER(38,0)"}.get(dialect, "INTEGER")
    suffix = " ENGINE = MergeTree ORDER BY tuple()" if dialect == "clickhouse" else ""
    name = f"{prefix}assignments"
    values = ", ".join(f"({a}, {c})" for a, c in ASSIGNMENTS)
    return [f"CREATE TABLE {name} (id {int_type}, contractor_id {int_type}){suffix}",
            f"INSERT INTO {name} VALUES {values}"]
