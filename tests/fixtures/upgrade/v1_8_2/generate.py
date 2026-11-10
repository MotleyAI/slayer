"""Regenerate the 1.8.2 list-column upgrade corpus; run under a venv with ``motley-slayer==1.8.2`` (not in CI).

    uv venv /tmp/v182 && uv pip install --python /tmp/v182 motley-slayer==1.8.2 duckdb==1.5.2
    /tmp/v182/bin/python tests/fixtures/upgrade/v1_8_2/generate.py
"""

import asyncio
import json
import shutil
from importlib.metadata import version
from pathlib import Path

import duckdb

import slayer

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource_idempotent  # pyright: ignore[reportAttributeAccessIssue] — the 1.8.2 module path
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

OUT = Path(__file__).resolve().parent
DB = OUT / "data.duckdb"
YAML_STORE = OUT / "yaml_store"
SQLITE_STORE = OUT / "sqlite_store.db"
EXPECTED = OUT / "expected.json"
SHOP = "shop"

_CONTRACTORS = [
    (1, "Ann", "A", ["go", "sql"], [3, 9]),
    (2, "Bob", "A", ["sql", None], [1]),
    (3, "Cid", "B", [], []),
    (4, "Dee", "B", None, None),
    (5, "Eve", "B", ["go"], [5, None]),
]
_ASSIGNMENTS = [(1, 1), (2, 1), (3, 2), (4, 3), (5, 5)]

_STORED_MEASURES = [
    ModelMeasure(name="ids", formula="collect(id)"),
    ModelMeasure(name="skill_set", formula="collect(skills.value)"),
    ModelMeasure(name="with_skills", formula="count(skills)"),
]

# An sql-mode list column declaring no element type; 1.8.2 stores it as UNKNOWN.
DEV_SKILLS = SlayerModel(
    name="dev_skills", data_source=SHOP, sql="SELECT id, name, skills FROM contractors",
    columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT),
             Column(name="skills", type=DataType.ARRAY)],
)

# 1.8.2 spells a list literal's strings with double quotes.
QUERY_BACKED = [
    {"name": "go_teams", "data_source": SHOP, "source_queries": [{
        "source_model": "contractors", "dimensions": ["team"],
        "measures": [{"formula": "collect(id)", "name": "ids"}, {"formula": "collect(skills.value)", "name": "skill_set"}],
        "filters": ["contains(skills, 'go')", 'contains_any(skills, ["go", "sql"])'],
    }]},
]

MEMORIES = [
    {"id": "m_assignments", "learning": "Assignment ids per contractor.", "entities": [f"{SHOP}.contractors"],
     "query": {"source_model": "contractors", "dimensions": ["name"],
               "measures": [{"formula": "collect(assignments.id)", "name": "assignment_ids"}]}},
]


def _over(model: str, *, dims: list, measures: list, **extra) -> dict:
    return {"source_model": model, "dimensions": dims, "measures": measures, **extra}


QUERIES = {
    "stored_ids": _over("contractors", dims=["team"], measures=["ids"]),
    "stored_skill_set": _over("contractors", dims=["team"], measures=["skill_set"]),
    "stored_with_skills": _over("contractors", dims=["team"], measures=["with_skills"]),
    "collect_pk": _over("contractors", dims=["team"], measures=[{"formula": "collect(id)", "name": "c"}]),
    "score_elements": _over("contractors", dims=["team"], measures=[
        {"formula": "collect(scores.value)", "name": "c"}, {"formula": "sum(scores.value)", "name": "s"}]),
    "element_filter": _over("contractors", dims=["name"], measures=[], filters=["skills.value = 'sql'"]),
    "contains_filter": _over("contractors", dims=["name"], measures=[],
                             filters=["contains_any(skills, ['go', 'sql']) and not contains(skills, 'go')"]),
    "go_teams": _over("go_teams", dims=["team"], measures=[{"formula": "count(*)", "name": "n"}]),
    "dev_skills_count": _over("dev_skills", dims=[], measures=[{"formula": "count(skills)", "name": "n"}]),
    "dev_skills_null": _over("dev_skills", dims=["name"], measures=[], filters=["skills IS NULL"]),
}


def _seed_db() -> None:
    DB.unlink(missing_ok=True)
    con = duckdb.connect(str(DB))
    try:
        con.execute("CREATE TABLE contractors (id INTEGER PRIMARY KEY, name VARCHAR, team VARCHAR, "
                    "skills VARCHAR[], scores INTEGER[])")
        con.executemany("INSERT INTO contractors VALUES (?,?,?,?,?)", _CONTRACTORS)
        con.execute("CREATE TABLE assignments (id INTEGER PRIMARY KEY, contractor_id INTEGER)")
        con.executemany("INSERT INTO assignments VALUES (?,?)", _ASSIGNMENTS)
    finally:
        con.close()


async def _populate(storage: StorageBackend) -> None:
    await storage.save_datasource(DatasourceConfig(name=SHOP, type="duckdb", database=str(DB)))
    shop = await storage.get_datasource(SHOP)
    assert shop is not None
    engine = SlayerQueryEngine(storage=storage)
    try:
        result = await ingest_datasource_idempotent(datasource=shop, storage=storage, client=engine.client_for(shop))
    finally:
        await engine.aclose()
        engine.close()
    assert not result.errors, result.errors
    contractors = await storage.get_model("contractors", data_source=SHOP)
    assert contractors is not None
    pk = contractors.get_column("id")
    assert pk is not None
    pk.allowed_aggregations = ["count", "collect"]
    contractors.measures.extend(_STORED_MEASURES)
    await storage.save_model(SlayerModel.model_validate(contractors.model_dump()))
    assignments = await storage.get_model("assignments", data_source=SHOP)
    assert assignments is not None
    if not any(j.target_model == "contractors" for j in assignments.joins):
        assignments.joins.append(ModelJoin(target_model="contractors", join_pairs=[["contractor_id", "id"]]))
        await storage.save_model(assignments)
    await storage.save_model(DEV_SKILLS)
    for raw in QUERY_BACKED:
        await storage.save_model(SlayerModel.model_validate(raw))
    for mem in MEMORIES:
        await storage.save_memory(
            id=mem["id"], learning=mem["learning"], entities=mem["entities"],
            query=SlayerQuery.model_validate(mem["query"]))


def _cell(value):
    if isinstance(value, (list, tuple)):
        return [_cell(v) for v in value]
    return round(value, 6) if isinstance(value, float) else (None if value is None else str(value))


async def _record(storage: StorageBackend) -> dict:
    engine = SlayerQueryEngine(storage=storage)
    queries = {**QUERIES, **{f"memory:{m['id']}": m["query"] for m in MEMORIES}}
    out = {}
    try:
        for key, query in queries.items():
            resp = await engine.execute(SlayerQuery.model_validate(query), data_source=SHOP)
            rows = [{k.rsplit(".", 1)[-1]: _cell(v) for k, v in row.items()} for row in resp.data]
            out[key] = {"query": query, "data_source": SHOP, "ordered": False,
                        "rows": sorted(rows, key=lambda r: json.dumps(r, sort_keys=True))}
    finally:
        await engine.aclose()
        engine.close()
    return out


async def main() -> None:
    assert version("motley-slayer") == "1.8.2", version("motley-slayer")
    assert "site-packages" in slayer.__file__, slayer.__file__
    _seed_db()
    shutil.rmtree(YAML_STORE, ignore_errors=True)
    SQLITE_STORE.unlink(missing_ok=True)
    stores = (YAMLStorage(base_dir=str(YAML_STORE)), SQLiteStorage(db_path=str(SQLITE_STORE)))
    results = []
    for storage in stores:
        await _populate(storage)
        results.append(await _record(storage))
    for key in results[0]:
        assert results[0][key] == results[1][key], (key, results[0][key], results[1][key])
    EXPECTED.write_text(json.dumps(results[0], indent=2, sort_keys=True) + "\n")
    for storage in stores:
        # Bare data file name; the test points it at a copy.
        await storage.save_datasource(DatasourceConfig(name=SHOP, type="duckdb", database=DB.name))


if __name__ == "__main__":
    asyncio.run(main())
