"""List functions over the usual ingested Postgres array types, and ``collect`` over an exact numeric (spec:
queries/list-functions › Physical array types differ on Postgres; aggregations/collect)."""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2137_fixtures import as_list, by, measure

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

#: ``skills`` VARCHAR[], ``levels`` BIGINT[], ``required`` TEXT[], ``ints`` INTEGER[], ``amount`` NUMERIC,
#: ``uid`` UUID (ingested as TEXT).
UUID_A, UUID_B = "00000000-0000-0000-0000-00000000000a", "00000000-0000-0000-0000-00000000000b"
TYPED = (
    "CREATE TABLE typed (id INTEGER PRIMARY KEY, team TEXT, skills VARCHAR[], levels BIGINT[], required TEXT[], "
    "ints INTEGER[], amount NUMERIC(10, 2), uid UUID)",
    "INSERT INTO typed VALUES "
    f"(1, 'a', ARRAY['go', 'sql'], ARRAY[1, 2, 3], ARRAY['rust', 'sql'], ARRAY[3], 1.50, '{UUID_B}'), "
    f"(2, 'a', ARRAY['sql'], ARRAY[2], ARRAY['go'], ARRAY[9], 2.25, '{UUID_A}'), "
    f"(3, 'a', ARRAY['rust'], ARRAY[]::BIGINT[], ARRAY[]::TEXT[], ARRAY[]::INTEGER[], 1.50, '{UUID_B}'), "
    "(4, 'b', NULL, NULL, ARRAY['go'], ARRAY[1], NULL, NULL)",
)


def typed_model() -> SlayerModel:
    def array(name: str, element: DataType) -> Column:
        return Column(name=name, type=DataType.ARRAY, element_type=element)

    return SlayerModel(name="typed", sql_table="typed", data_source="pg", columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name="team", type=DataType.TEXT),
        array("skills", DataType.TEXT), array("levels", DataType.INT), array("required", DataType.TEXT),
        array("ints", DataType.INT), Column(name="amount", type=DataType.DOUBLE),
        Column(name="uid", type=DataType.TEXT),
    ])


@pytest.fixture
async def pg(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in TYPED:
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name="pg", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    await storage.save_model(typed_model())
    try:
        yield SlayerQueryEngine(storage=storage)
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.mark.integration
@pytest.mark.parametrize(("expr", "kept"), [
    ("contains(skills, 'go')", {1}),
    ("contains(levels, 2)", {1, 2}),
    ("contains_any(skills, ['go'])", {1}),
    ("contains_any(skills, ['go', 'rust'])", {1, 3}),
    ("contains_all(skills, ['sql'])", {1, 2}),
    ("contains_any(levels, [2, 9])", {1, 2}),
    ("contains_all(levels, [1, 2])", {1}),
    ("contains_any(skills, required)", {1}),
    ("contains_all(skills, required)", {3}),
    ("contains_any(levels, ints)", {1}),
    ("contains_all(levels, ints)", {1, 3}),
])
async def test_lists_of_differing_physical_types(pg, expr: str, kept: set[int]) -> None:
    resp = await pg.execute(SlayerQuery.model_validate({"source_model": "typed", "dimensions": ["id"],
                                                        "filters": [expr]}))
    assert {int(r["typed.id"]) for r in resp.data} == kept, resp.sql


@pytest.mark.integration
async def test_collect_over_numeric(pg) -> None:
    resp = await pg.execute(SlayerQuery.model_validate({"source_model": "typed", "dimensions": ["team"],
                                                        "measures": [measure("collect(amount)", "c")]}))
    got = {k: [float(x) for x in as_list(v) or []] for k, v in by(resp, "team", "c", model="typed").items()}
    assert got == {"a": [1.5, 2.25], "b": []}, resp.sql


@pytest.mark.integration
async def test_collect_over_uuid(pg) -> None:
    resp = await pg.execute(SlayerQuery.model_validate({"source_model": "typed", "dimensions": ["team"],
                                                        "measures": [measure("collect(uid)", "c")]}))
    got = {k: [str(x) for x in as_list(v) or []] for k, v in by(resp, "team", "c", model="typed").items()}
    assert got == {"a": [UUID_A, UUID_B], "b": []}, resp.sql
