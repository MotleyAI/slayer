"""List columns as model metadata: the type, ingestion, eligibility, NULL elements, refusals, re-typing and rendering
(spec: models/list-columns)."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from typing import Any, Optional

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.models import Aggregation, Column, DatasourceConfig, SlayerModel
from slayer.engine.ingestion import (
    IntrospectedColumn, _inferred_elements, _introspected_column, _retyped_list, ingest_datasource,
    ingest_datasource_idempotent,
)
from slayer.engine.schema_scope import SchemaRef
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.collection_render import render_models_summary
from slayer.inspect.model_render import render_model_inspection
from slayer.search.render import render_column_text
from slayer.sql import engine_factory
from slayer.sql.dialects import ReflectedColumn, get_dialect
from slayer.sql.dialects.reflection import column_from_type_string
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2120_fixtures import contractors_engine, contractors_model, emit_contractors
from tests._dev2137_fixtures import list_engine, measure, num, query


def _array(element: str) -> dict[str, Any]:
    return {"type": "ARRAY", "element_type": element}


class TestTheType:
    def test_value_matches_sqlglot(self) -> None:
        assert DataType.ARRAY.value == exp.DataType.Type.ARRAY.value

    def test_missing_element_type_rejected(self) -> None:
        with pytest.raises(ValueError, match="(?s)skills.*element_type|element_type.*skills"):
            SlayerModel.model_validate({"name": "c", "sql_table": "c", "data_source": "test",
                                        "columns": [{"name": "skills", "type": "ARRAY"}]})

    def test_element_type_on_a_scalar_rejected(self) -> None:
        with pytest.raises(ValueError, match="team"):
            SlayerModel.model_validate({"name": "c", "sql_table": "c", "data_source": "test",
                                        "columns": [{"name": "team", "type": "TEXT", "element_type": "INT"}]})

    def test_nested_element_type_rejected(self) -> None:
        raw = {"name": "grid", **_array("ARRAY")}
        with pytest.raises(ValueError, match="grid"):
            Column.model_validate(raw)

    @pytest.mark.parametrize("element", ["TEXT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "UNKNOWN"])
    def test_scalar_and_opaque_elements_accepted(self, element: str) -> None:
        col = Column.model_validate({"name": "xs", **_array(element)})
        assert col.type is DataType.ARRAY
        assert col.element_type is DataType(element)

    def test_scalar_column_has_no_element_type(self) -> None:
        assert Column(name="team").element_type is None

    async def test_element_type_survives_storage(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name="test", type="duckdb", database=":memory:"))
        await storage.save_model(contractors_model())
        loaded = await storage.get_model("contractors", data_source="test")
        assert loaded is not None
        scores = loaded.get_column("scores")
        assert scores is not None
        assert (scores.type, scores.element_type) == (DataType.ARRAY, DataType.INT)


# --------------------------------------------------------------------------- #
# Ingestion
# --------------------------------------------------------------------------- #
def _mapped(sa_type: sa.types.TypeEngine) -> tuple[DataType, Optional[DataType], Optional[str]]:
    col = _introspected_column(ReflectedColumn(name="xs", type=sa_type), name="xs", primary_key=False)
    return col.type, col.element_type, col.db_type


class TestIngestionMapping:
    def test_postgres_integer_array(self) -> None:
        data_type, element, db_type = _mapped(postgresql.ARRAY(sa.Integer()))
        assert (data_type, element) == (DataType.ARRAY, DataType.INT)
        assert "INTEGER[]" in (db_type or "").upper()

    def test_postgres_text_array(self) -> None:
        assert _mapped(postgresql.ARRAY(sa.Text()))[:2] == (DataType.ARRAY, DataType.TEXT)

    def test_bigquery_repeated_string(self) -> None:
        bq = pytest.importorskip("sqlalchemy_bigquery")
        data_type, element, db_type = _mapped(sa.ARRAY(bq.STRING()))
        assert (data_type, element) == (DataType.ARRAY, DataType.TEXT)
        assert "STRING" in (db_type or "").upper(), db_type

    def test_bigquery_repeated_record_is_opaque(self) -> None:
        bq = pytest.importorskip("sqlalchemy_bigquery")
        assert _mapped(sa.ARRAY(bq.STRUCT(x=bq.INT64())))[:2] == (DataType.ARRAY, DataType.UNKNOWN)

    def test_clickhouse_wrapped_element(self) -> None:
        ch = pytest.importorskip("clickhouse_sqlalchemy.types")
        wrapped = ch.Array(ch.LowCardinality(ch.Nullable(ch.String)))
        assert _mapped(wrapped)[:2] == (DataType.ARRAY, DataType.TEXT)

    def test_clickhouse_nested_list_is_opaque(self) -> None:
        ch = pytest.importorskip("clickhouse_sqlalchemy.types")
        assert _mapped(ch.Array(ch.Array(ch.Int32)))[:2] == (DataType.ARRAY, DataType.UNKNOWN)

    def test_trino_array(self) -> None:
        assert _mapped(sa.ARRAY(sa.VARCHAR()))[:2] == (DataType.ARRAY, DataType.TEXT)

    def test_multidimensional_array_is_opaque(self) -> None:
        assert _mapped(postgresql.ARRAY(sa.Integer(), dimensions=2))[:2] == (DataType.ARRAY, DataType.UNKNOWN)

    def test_snowflake_structured_array(self) -> None:
        sf = pytest.importorskip("snowflake.sqlalchemy")
        assert _mapped(sf.ARRAY(value_type=sa.VARCHAR()))[:2] == (DataType.ARRAY, DataType.TEXT)

    def test_scalars_carry_no_element_type(self) -> None:
        assert _mapped(sa.Integer())[1] is None


DUCKDB_KINDS = {
    "tags": ("VARCHAR[]", DataType.TEXT), "scores": ("INTEGER[]", DataType.INT),
    "ratios": ("DOUBLE[]", DataType.DOUBLE), "flags": ("BOOLEAN[]", DataType.BOOLEAN),
    "days": ("DATE[]", DataType.DATE), "grid": ("INTEGER[][]", DataType.UNKNOWN),
    "recs": ("STRUCT(x INTEGER)[]", DataType.UNKNOWN),
}


@contextlib.contextmanager
def _duckdb_file(*statements: str):
    duckdb = pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "live.duckdb")
        with contextlib.closing(duckdb.connect(db_path)) as con:
            for stmt in statements:
                con.execute(stmt)
        ds = DatasourceConfig(name="test", type="duckdb", database=db_path)
        try:
            yield ds, tmp
        finally:
            engine_factory.invalidate_engine(ds)


class TestDuckDBIngestion:
    def test_lists_ingested_with_element_types(self) -> None:
        cols = ", ".join(f"{c} {t}" for c, (t, _) in DUCKDB_KINDS.items())
        with _duckdb_file(f"CREATE TABLE kinds (id INTEGER, {cols})") as (ds, _tmp):
            model = ingest_datasource(datasource=ds, include_tables=["kinds"])[0]
        for name, (db_type, element) in DUCKDB_KINDS.items():
            col = model.get_column(name)
            assert col is not None
            assert (col.type, col.element_type) == (DataType.ARRAY, element), name
            assert (col.db_type or "").upper() == db_type.upper(), name


# --------------------------------------------------------------------------- #
# Eligibility
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
async def engine():
    async with list_engine() as e:
        yield e


async def _refused(engine: SlayerQueryEngine, *, names: tuple[str, ...], **body: Any) -> str:
    q = query(**body)
    with pytest.raises(SlayerError) as info:
        await engine.execute(q)
    msg = str(info.value)
    for name in names:
        assert name in msg, msg
    return msg


def _names_the_list_type(msg: str) -> bool:
    return "list" in msg.lower() or "ARRAY" in msg


class TestEligibility:
    async def test_list_as_a_dimension(self, engine) -> None:
        await _refused(engine, names=("skills.value",), dimensions=["skills"])

    @pytest.mark.parametrize(("filter_", "remedy"), [
        ("skills = 'go'", "contains"), ("skills <> 'go'", "contains"), ("skills IN ('go', 'sql')", "contains_any"),
    ])
    async def test_comparing_a_list(self, engine, filter_: str, remedy: str) -> None:
        await _refused(engine, names=(remedy,), measures=[measure("count(*)", "n")], filters=[filter_])

    @pytest.mark.parametrize("filter_", [
        "skills < 'go'", "skills >= 'go'", "skills BETWEEN 'a' AND 'z'", "skills IS DISTINCT FROM required",
    ])
    async def test_other_comparisons_refused(self, engine, filter_: str) -> None:
        await _refused(engine, names=("skills",), measures=[measure("count(*)", "n")], filters=[filter_])

    async def test_distinct_count_of_a_list(self, engine) -> None:
        await _refused(engine, names=("count_distinct(skills.value)",),
                       measures=[measure("count_distinct(skills)", "n")])

    @pytest.mark.parametrize("agg", ["min", "max", "first", "last", "count_distinct_approx"])
    async def test_other_builtins_refused(self, engine, agg: str) -> None:
        await _refused(engine, names=("skills",), measures=[measure(f"{agg}(skills)", "n")])

    async def test_counting_lists(self, engine) -> None:
        resp = await engine.execute(query(measures=[measure("count(skills)", "n")],
                                          filters=["name IN ('Cid', 'Dee', 'Eve')"]))
        assert num(resp.data[0]["contractors.n"]) == 2

    async def test_counting_lists_colon_spelling(self, engine) -> None:
        resp = await engine.execute(query(measures=["skills:count"]))
        assert num(next(iter(resp.data[0].values()))) == 4

    async def test_custom_aggregation_over_a_list(self) -> None:
        model = contractors_model().model_copy(update={
            "aggregations": [Aggregation(name="my_agg", formula="COUNT({value})")]})
        async with contractors_engine(models=[model]) as e:
            msg = await _refused(e, names=(), measures=[measure("my_agg(skills)", "n")])
        assert _names_the_list_type(msg), msg

    async def test_expression_source_over_a_list(self, engine) -> None:
        msg = await _refused(engine, names=(), measures=[measure("max(coalesce(skills, required))", "n")])
        assert _names_the_list_type(msg), msg

    async def test_aggregating_a_collected_list(self, engine) -> None:
        msg = await _refused(engine, names=(), measures=[measure("min(collect(team))", "n")])
        assert _names_the_list_type(msg), msg

    def test_save_time_allowed_aggregations(self) -> None:
        raw = {"name": "c", "sql_table": "c", "data_source": "test", "columns": [
            {"name": "skills", "type": "ARRAY", "element_type": "TEXT", "allowed_aggregations": ["sum"]}]}
        with pytest.raises(ValueError, match="skills"):
            SlayerModel.model_validate(raw)

    def test_count_is_an_allowed_aggregation(self) -> None:
        SlayerModel(name="c", sql_table="c", data_source="test", columns=[
            Column(name="skills", type=DataType.ARRAY, element_type=DataType.TEXT, allowed_aggregations=["count"])])

    async def test_null_test(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["name"], filters=["skills IS NULL"]))
        assert {r["contractors.name"] for r in resp.data} == {"Dee"}


class TestNullElements:
    async def test_null_element_is_not_an_element(self, engine) -> None:
        bob = ["name = 'Bob'"]
        resp = await engine.execute(query(
            dimensions=["name", {"expression": "length(skills)", "name": "n"},
                        {"expression": "contains(skills, 'go')", "name": "has_go"}],
            filters=bob))
        row = resp.data[0]
        assert num(row["contractors.n"]) == 1
        assert row["contractors.has_go"] in (False, 0)
        elements = await engine.execute(query(dimensions=["skills.value"], measures=[measure("count(skills.*)", "n")],
                                              filters=bob))
        assert {r["contractors.skills.value"]: num(r["contractors.n"]) for r in elements.data} == {"sql": 1}


GRIDS = ("CREATE TABLE grids (id INTEGER, grid INTEGER[][])", "INSERT INTO grids VALUES (1, [[1, 2], [3]]), (2, NULL)")


def _grids_model() -> SlayerModel:
    return SlayerModel(name="grids", sql_table="grids", data_source="test", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="grid", type=DataType.ARRAY, element_type=DataType.UNKNOWN)])


class TestOpaqueElements:
    async def test_length_runs_and_element_access_refused(self) -> None:
        async with contractors_engine(models=[contractors_model(), _grids_model()], extra=GRIDS) as e:
            resp = await e.execute({"source_model": "grids", "dimensions": [
                "id", {"expression": "length(grid)", "name": "n"}]})
            assert {int(r["grids.id"]): r["grids.n"] for r in resp.data} == {1: 2, 2: None}
            with pytest.raises(SlayerError, match="UNKNOWN"):
                await e.execute({"source_model": "grids", "dimensions": ["grid.value"]})
            with pytest.raises(SlayerError, match="grid"):
                await e.execute({"source_model": "grids", "dimensions": ["id"],
                                 "filters": ["contains(grid, 1)"]})

    async def test_null_tests_run(self) -> None:
        async with contractors_engine(models=[contractors_model(), _grids_model()], extra=GRIDS) as e:
            resp = await e.execute({"source_model": "grids", "dimensions": ["id"], "filters": ["grid IS NULL"]})
            assert {int(r["grids.id"]) for r in resp.data} == {2}


# --------------------------------------------------------------------------- #
# Backends without native lists
# --------------------------------------------------------------------------- #
class TestNoNativeLists:
    async def test_list_function_on_sqlite(self) -> None:
        async with contractors_engine("sqlite") as e:
            q = query(measures=[measure("count(*)", "n")], filters=["contains(skills, 'go')"])
            with pytest.raises(SlayerError) as info:
                await e.execute(q)
        assert "contains" in str(info.value), str(info.value)
        assert "SQLite" in str(info.value), str(info.value)

    @pytest.mark.parametrize(("dialect", "database"), [("mysql", "MySQL"), ("tsql", "SQL Server"),
                                                       ("sqlite", "SQLite")])
    async def test_collect(self, dialect: str, database: str) -> None:
        q = query(dimensions=["city"], measures=[measure("collect(team)", "c")])
        with pytest.raises(SlayerError) as info:
            await emit_contractors(q, dialect=dialect)
        assert "collect" in str(info.value), str(info.value)
        assert database in str(info.value), str(info.value)

    @pytest.mark.parametrize(("dialect", "database"), [("mysql", "MySQL"), ("tsql", "SQL Server"),
                                                       ("sqlite", "SQLite")])
    async def test_element_access(self, dialect: str, database: str) -> None:
        q = query(measures=[measure("count(*)", "n")], filters=["skills.value = 'go'"])
        with pytest.raises(SlayerError) as info:
            await emit_contractors(q, dialect=dialect)
        assert database in str(info.value), str(info.value)

    @pytest.mark.parametrize(("dialect", "database"), [("mysql", "MySQL"), ("tsql", "SQL Server")])
    async def test_array_literal(self, dialect: str, database: str) -> None:
        q = query(measures=[measure("count(*)", "n")], filters=["contains_any(skills, ['go'])"])
        with pytest.raises(SlayerError) as info:
            await emit_contractors(q, dialect=dialect)
        assert database in str(info.value), str(info.value)


# --------------------------------------------------------------------------- #
# Re-ingest re-typing
# --------------------------------------------------------------------------- #
LIVE = ("CREATE TABLE contractors (id INTEGER, skills VARCHAR[], scores INTEGER[])",
        "INSERT INTO contractors VALUES (1, ['go'], [1])")


async def _reingest(ds: DatasourceConfig, tmp: str, stored: SlayerModel) -> SlayerModel:
    storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
    await storage.save_datasource(ds)
    await storage.save_model(stored)
    await ingest_datasource_idempotent(datasource=ds, storage=storage, include_tables=["contractors"])
    loaded = await storage.get_model("contractors", data_source=ds.name)
    assert loaded is not None
    return loaded


def _stored(*columns: Column) -> SlayerModel:
    return SlayerModel(name="contractors", sql_table="contractors", data_source="test",
                       columns=[Column(name="id", type=DataType.INT), *columns])


class TestReingest:
    async def test_retyping_keeps_metadata(self) -> None:
        stale = Column(name="skills", type=DataType.TEXT, description="What they can do", label="Skills",
                       meta={"source": "hr"})
        with _duckdb_file(*LIVE) as (ds, tmp):
            loaded = await _reingest(ds, tmp, _stored(stale))
        skills = loaded.get_column("skills")
        assert skills is not None
        assert (skills.type, skills.element_type) == (DataType.ARRAY, DataType.TEXT)
        assert (skills.description, skills.label, skills.meta) == ("What they can do", "Skills", {"source": "hr"})
        assert (skills.db_type or "").upper() == "VARCHAR[]"

    async def test_a_users_element_type_survives(self) -> None:
        mine = Column(name="scores", type=DataType.ARRAY, element_type=DataType.TEXT)
        with _duckdb_file(*LIVE) as (ds, tmp):
            loaded = await _reingest(ds, tmp, _stored(mine))
        scores = loaded.get_column("scores")
        assert scores is not None
        assert (scores.type, scores.element_type) == (DataType.ARRAY, DataType.TEXT)

    async def test_a_declared_unknown_element_type_survives(self) -> None:
        opaque = Column(name="scores", type=DataType.ARRAY, element_type=DataType.UNKNOWN)
        with _duckdb_file(*LIVE) as (ds, tmp):
            loaded = await _reingest(ds, tmp, _stored(opaque))
        scores = loaded.get_column("scores")
        assert scores is not None
        assert (scores.type, scores.element_type) == (DataType.ARRAY, DataType.UNKNOWN)

    @pytest.mark.parametrize(("live_db_type", "refilled"), [("ARRAY", DataType.INT), ("ARRAY(NUMBER(38,0))", None)])
    def test_only_an_untyped_snowflake_list_is_re_inferred(self, live_db_type: str, refilled: Any) -> None:
        stored = Column(name="scores", type=DataType.ARRAY, element_type=DataType.UNKNOWN)
        live = Column(name="scores", type=DataType.ARRAY, element_type=DataType.INT, db_type=live_db_type)
        merged = _retyped_list(persisted=stored, fresh=live)
        assert (merged.element_type if merged is not None else None) == refilled

    async def test_text_over_a_live_text_column_untouched(self) -> None:
        with _duckdb_file("CREATE TABLE contractors (id INTEGER, skills VARCHAR)") as (ds, tmp):
            loaded = await _reingest(ds, tmp, _stored(Column(name="skills", type=DataType.TEXT)))
        skills = loaded.get_column("skills")
        assert skills is not None
        assert (skills.type, skills.element_type) == (DataType.TEXT, None)


# --------------------------------------------------------------------------- #
# Rendering for agents
# --------------------------------------------------------------------------- #
def _find_column(node: Any, name: str) -> Optional[dict[str, Any]]:
    if isinstance(node, dict):
        if node.get("name") == name and "type" in node:
            return node
        children = node.values()
    elif isinstance(node, list):
        children = node
    else:
        return None
    for child in children:
        found = _find_column(child, name)
        if found is not None:
            return found
    return None


class TestRendering:
    async def test_inspect_model_markdown(self, engine) -> None:
        model = await engine.storage.get_model("contractors", data_source="test")
        out = await render_model_inspection(model=model, storage=engine.storage, engine=engine, format="markdown",
                                            sections=["columns"], compact=False)
        for needle in ("ARRAY<TEXT>", "go", "sql", "contains", "contains_all", "skills.value"):
            assert needle in out, (needle, out)

    async def test_inspect_model_json(self, engine) -> None:
        model = await engine.storage.get_model("contractors", data_source="test")
        out = await render_model_inspection(model=model, storage=engine.storage, engine=engine, format="json",
                                            sections=["columns"])
        skills = _find_column(json.loads(out), "skills")
        assert skills is not None
        assert (skills["type"], skills["element_type"]) == ("ARRAY", "TEXT")
        assert skills.get("sampled_values") == ["go", "sql"]

    def test_search_column_text(self) -> None:
        model = contractors_model()
        skills = model.get_column("skills")
        assert skills is not None
        text = render_column_text(model=model, column=skills)
        assert "ARRAY<TEXT>" in text, text
        assert "skills.value" in text, text
        assert "contains" in text, text

    def test_models_summary(self) -> None:
        models = [contractors_model()]
        markdown = render_models_summary(datasource_name="test", models=models, fmt="markdown", compact=False)
        assert "ARRAY<TEXT>" in markdown, markdown
        assert "ARRAY<INT>" in markdown, markdown
        skills = _find_column(json.loads(render_models_summary(datasource_name="test", models=models, fmt="json",
                                                               compact=False)), "skills")
        assert skills is not None
        assert skills["element_type"] == "TEXT"


class TestSnowflakeJson:
    async def test_json_value_reads_an_array_column(self) -> None:
        tags = Column(name="tags", type=DataType.ARRAY, element_type=DataType.TEXT, db_type="ARRAY")
        model = contractors_model().model_copy(update={"columns": [*contractors_model().columns, tags]})
        sql = await emit_contractors(query(dimensions=["id", {"expression": "json_value(tags, '$[0]')", "name": "t"}]),
                                     dialect="snowflake", models=[model])
        assert "TRY_PARSE_JSON" not in sql.upper(), sql


class TestCatalogFallbackLists:
    def test_a_bare_array_type_string_is_a_list_with_opaque_elements(self) -> None:
        col = column_from_type_string(name="tags", data_type="ARRAY")
        assert (col.type, col.element_type, col.db_type) == (DataType.ARRAY, DataType.UNKNOWN, "ARRAY")

    def test_a_snowflake_array_cursor_column_is_a_list(self) -> None:
        assert get_dialect("snowflake").map_cursor_type_code(10) == "list"

    def test_snowflake_element_inference_reads_the_table_in_its_own_database(self) -> None:
        statements: list[str] = []

        class _Rows:
            def fetchall(self) -> list[tuple[str]]:
                return [("INTEGER",)]

        class _Conn:
            def __enter__(self) -> "_Conn":
                return self

            def __exit__(self, *exc: object) -> None:
                return None

            def exec_driver_sql(self, sql: str) -> _Rows:
                statements.append(sql)
                return _Rows()

        class _Engine:
            dialect = type("D", (), {"name": "snowflake"})()

            def connect(self) -> _Conn:
                return _Conn()

        column = IntrospectedColumn(name="ints", type=DataType.ARRAY, element_type=DataType.UNKNOWN)
        typed = _inferred_elements(column, sa_engine=_Engine(), table_name="kinds",  # pyright: ignore[reportArgumentType]  # a minimal engine double
                                   ref=SchemaRef(catalog="OTHER_DB", name="RAW"))
        assert typed.element_type is DataType.INT
        assert "OTHER_DB.RAW.kinds" in statements[0], statements
