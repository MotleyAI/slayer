"""List functions over the usual ingested Postgres array types, and ``collect`` over an exact numeric (spec:
queries/list-functions › Physical array types differ on Postgres; aggregations/collect)."""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from tests._dev2137_fixtures import by, metric
from tests._pg_helpers import client_datasource, pg_query_engine

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


def typed_dataset() -> SlayerDataset:
    def array(name: str, element: DataType) -> DatasetField:
        return DatasetField(name=name, datatype=DataType.ARRAY, element_type=element)

    return SlayerDataset(name="typed", sql_table="typed", data_source="pg", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="team", datatype=DataType.STRING),
        array("skills", DataType.STRING), array("levels", DataType.INTEGER), array("required", DataType.STRING),
        array("ints", DataType.INTEGER), DatasetField(name="amount", datatype=DataType.FLOAT),
        DatasetField(name="uid", datatype=DataType.STRING),
    ])


@pytest.fixture
async def pg(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in TYPED:
        cur.execute(stmt)
    postgresql.commit()
    async with pg_query_engine(client_datasource(postgresql.info), base_dir=str(tmp_path),
                               datasets=[typed_dataset()]) as engine:
        yield engine


@pytest.mark.parametrize(("expr", "kept"), [
    ("contains(typed.skills, 'go')", {1}),
    ("contains(typed.levels, 2)", {1, 2}),
    ("contains_any(typed.skills, ['go'])", {1}),
    ("contains_any(typed.skills, ['go', 'rust'])", {1, 3}),
    ("contains_all(typed.skills, ['sql'])", {1, 2}),
    ("contains_any(typed.levels, [2, 9])", {1, 2}),
    ("contains_all(typed.levels, [1, 2])", {1}),
    ("contains_any(typed.skills, typed.required)", {1}),
    ("contains_all(typed.skills, typed.required)", {3}),
    ("contains_any(typed.levels, typed.ints)", {1}),
    ("contains_all(typed.levels, typed.ints)", {1, 3}),
])
async def test_lists_of_differing_physical_types(pg, expr: str, kept: set[int]) -> None:
    resp = await pg.execute(SlayerQuery.model_validate({"population": "typed", "dimensions": ["typed.id"],
                                                        "filter": [expr]}))
    assert {int(r["typed.id"]) for r in resp.data} == kept, resp.sql


async def test_collect_over_numeric(pg) -> None:
    resp = await pg.execute(SlayerQuery.model_validate({"population": "typed", "dimensions": ["typed.team"],
                                                        "metrics": [metric("collect(typed.amount)", "c")]}))
    got = {k: None if v is None else [float(x) for x in v] for k, v in by(resp, "team", "c", dataset="typed").items()}
    assert got == {"a": [1.5, 2.25], "b": []}, resp.sql


async def test_collect_over_uuid(pg) -> None:
    resp = await pg.execute(SlayerQuery.model_validate({"population": "typed", "dimensions": ["typed.team"],
                                                        "metrics": [metric("collect(typed.uid)", "c")]}))
    got = {k: None if v is None else [str(x) for x in v] for k, v in by(resp, "team", "c", dataset="typed").items()}
    assert got == {"a": [UUID_A, UUID_B], "b": []}, resp.sql


@pytest.mark.parametrize("fn", ["contains_any", "contains_all"])
async def test_lists_of_two_attached_collects(pg, fn: str) -> None:
    # uuid[] against text[]: each attached list keeps its element type, so both are read as TEXT[].
    hit = f"{fn}(collect(uid, partition_by=team), collect(uid || '', partition_by=team))"
    resp = await pg.execute(SlayerQuery.model_validate({"population": "typed", "dimensions": [
        "typed.id", {"field": hit, "alias": "hit"}]}))
    assert {int(r["typed.id"]): r["hit"] for r in resp.data} == {
        1: True, 2: True, 3: True, 4: fn == "contains_all"}, resp.sql
