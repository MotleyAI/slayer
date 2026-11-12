"""SQLite keys and columns come from SQLite's own catalog: schema-scoped, no failing statement, today's results."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

import pytest
import sqlalchemy as sa

from slayer.core.enums import JoinCardinality
from slayer.core.models import SlayerModel
from slayer.database.client import ObjectNotFound, SlayerSQLClient
from slayer.live_schema import ingestion
from slayer.sql.dialects import SqliteDialect
from slayer.sql.dialects.reflection import (
    ReflectedColumn,
    TableKeys,
    TableLocation,
    column_from_sa_type,
    inspector_column_names,
    inspector_foreign_keys,
    inspector_unique_constraints,
    inspector_unique_indexes,
    without_primary_key,
)
from tests._sqlite_catalog_corpus import (
    ATTACHED,
    MAIN_TABLES,
    MISSING,
    VIEW,
    VIRTUAL,
    Corpus,
    build_corpus,
    corpus_client,
    corpus_locations,
    statement_errors,
)

# SQLAlchemy warns while skipping the expression index its Inspector cannot reflect.
pytestmark = pytest.mark.filterwarnings("ignore::sqlalchemy.exc.SAWarning")

_SCOPED = (TableLocation(name="comp_pk", schema_name=ATTACHED), TableLocation(name="tmp_keys", schema_name="temp"))


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Corpus:
    return build_corpus(tmp_path_factory.mktemp("sqlite_corpus"))


@pytest.fixture
async def reader(corpus: Corpus) -> AsyncGenerator[tuple[SlayerSQLClient, sa.Engine]]:
    async with corpus_client(corpus) as pair:
        yield pair


class _Shape:
    """Order-insensitive comparison form of a key read."""

    def __init__(self, keys: TableKeys) -> None:
        self.primary_key = list(keys.primary_key)
        self.foreign_keys = sorted(
            (tuple(fk.columns), fk.referred_table, tuple(fk.referred_columns)) for fk in keys.foreign_keys
        )
        self.unique_key_sets = sorted(tuple(s) for s in keys.unique_key_sets)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Shape) and vars(self) == vars(other)

    def __repr__(self) -> str:
        return repr(vars(self))


def _inspector_keys(insp: Any, name: str) -> TableKeys:
    """What the Inspector reads, unqualified (no failing statement), through SLayer's base key rules."""
    pk = inspector_column_names(insp.get_pk_constraint(name).get("constrained_columns")) or []
    uniques = [
        *inspector_unique_constraints(insp, table=name, token=None),
        *inspector_unique_indexes(insp, table=name, token=None),
    ]
    return TableKeys(
        primary_key=pk, foreign_keys=inspector_foreign_keys(insp, table=name, token=None),
        unique_key_sets=without_primary_key(pk, uniques),
    )


def _inspector_columns(insp: Any, name: str) -> list[ReflectedColumn]:
    dialect = SqliteDialect()
    return [
        column_from_sa_type(
            name=c["name"], sa_type=c["type"], comment=c.get("comment"), zoned_types=dialect.zoned_timestamp_db_types,
            json_types=dialect.json_db_types, sa_dialect=insp.dialect,
        )
        for c in insp.get_columns(name)
    ]


class TestCorpus:
    async def test_corpus_builds(self, reader: tuple[SlayerSQLClient, sa.Engine]) -> None:
        client, _ = reader
        main = await client.list_objects(None)
        assert set(MAIN_TABLES) <= set(main.tables)
        assert main.views == [VIEW]
        assert "comp_pk" in (await client.list_objects(ATTACHED)).tables


class TestNoFailingStatement:
    @pytest.mark.parametrize("schema", [None, "main"])
    async def test_key_and_column_reads(self, reader: tuple[SlayerSQLClient, sa.Engine], schema: str | None) -> None:
        client, engine = reader
        with statement_errors(engine) as failed:
            for location in (*corpus_locations(schema=schema), *_SCOPED):
                await client.reflect_table_keys(location)
                await client.reflect_columns(location)
        assert failed == []


class TestParityWithInspector:
    @pytest.mark.parametrize("schema", [None, "main"])
    @pytest.mark.parametrize("name", [*MAIN_TABLES, VIEW])
    async def test_keys(self, reader: tuple[SlayerSQLClient, sa.Engine], name: str, schema: str | None) -> None:
        client, engine = reader
        with engine.connect() as conn:
            expected = _inspector_keys(sa.inspect(conn), name)
        keys = await client.reflect_table_keys(TableLocation(name=name, schema_name=schema))
        assert _Shape(keys) == _Shape(expected)
        assert {fk.referred_schema for fk in keys.foreign_keys} <= {schema}

    @pytest.mark.parametrize("schema", [None, "main"])
    @pytest.mark.parametrize("name", [*MAIN_TABLES, VIEW])
    async def test_columns(self, reader: tuple[SlayerSQLClient, sa.Engine], name: str, schema: str | None) -> None:
        client, engine = reader
        with engine.connect() as conn:
            expected = _inspector_columns(sa.inspect(conn), name)
        assert await client.reflect_columns(TableLocation(name=name, schema_name=schema)) == expected

    @pytest.mark.parametrize("schema", [None, "main"])
    async def test_missing_table_is_not_found(
        self, reader: tuple[SlayerSQLClient, sa.Engine], schema: str | None,
    ) -> None:
        client, engine = reader
        location = TableLocation(name=MISSING, schema_name=schema)
        with statement_errors(engine) as failed:
            with pytest.raises(ObjectNotFound):
                await client.reflect_columns(location)
            with pytest.raises(ObjectNotFound):
                await client.reflect_table_keys(location)
        assert failed == []

    async def test_virtual_table_excludes_hidden_columns(self, reader: tuple[SlayerSQLClient, sa.Engine]) -> None:
        client, _ = reader
        columns = await client.reflect_columns(TableLocation(name=VIRTUAL, schema_name="main"))
        assert [c.name for c in columns] == ["title", "body"]

    async def test_generated_columns_are_kept(self, reader: tuple[SlayerSQLClient, sa.Engine]) -> None:
        client, _ = reader
        columns = await client.reflect_columns(TableLocation(name="gen", schema_name="main"))
        assert [c.name for c in columns] == ["id", "a", "twice", "shout"]


class TestSchemaScoping:
    async def test_attached_twin_has_its_own_keys(self, reader: tuple[SlayerSQLClient, sa.Engine]) -> None:
        client, _ = reader
        attached = await client.reflect_table_keys(TableLocation(name="comp_pk", schema_name=ATTACHED))
        main = await client.reflect_table_keys(TableLocation(name="comp_pk", schema_name="main"))
        assert (attached.primary_key, attached.unique_key_sets) == (["x"], [["b"]])
        assert (main.primary_key, main.unique_key_sets) == (["a", "b"], [])

    async def test_each_location_has_its_own_columns(self, reader: tuple[SlayerSQLClient, sa.Engine]) -> None:
        client, _ = reader

        async def names(location: TableLocation) -> list[str]:
            return [c.name for c in await client.reflect_columns(location)]

        assert await names(TableLocation(name="comp_pk", schema_name=ATTACHED)) == ["x", "a", "b"]
        assert await names(TableLocation(name="comp_pk", schema_name="main")) == ["a", "b", "v"]
        assert await names(TableLocation(name="tmp_keys", schema_name="temp")) == ["id", "code", "parent_id"]

    async def test_temp_table_has_its_keys(self, reader: tuple[SlayerSQLClient, sa.Engine]) -> None:
        client, _ = reader
        keys = await client.reflect_table_keys(TableLocation(name="tmp_keys", schema_name="temp"))
        assert (keys.primary_key, keys.unique_key_sets) == (["id"], [["code"]])


def _models(models: list[SlayerModel]) -> dict[str, SlayerModel]:
    return {m.name: m for m in models}


def _pk(model: SlayerModel) -> list[str]:
    return [c.name for c in model.columns if c.primary_key]


def _unique(model: SlayerModel) -> set[str]:
    return {c.name for c in model.columns if c.unique}


def _joins(model: SlayerModel) -> dict[str, tuple[list[list[str]], JoinCardinality | None]]:
    return {j.target_model: (j.join_pairs, j.cardinality) for j in model.joins}


class TestIngestion:
    async def test_every_declared_key_without_a_failing_statement(
        self, corpus: Corpus, reader: tuple[SlayerSQLClient, sa.Engine],
    ) -> None:
        client, engine = reader
        with statement_errors(engine) as failed:
            report = await ingestion.ingest_datasource_report_async(corpus.datasource, client=client)
        assert failed == []
        models = _models(report.models)
        assert _pk(models["comp_pk"]) == ["a", "b"]
        assert _pk(models["no_rowid"]) == ["k"]
        assert _joins(models["implicit_child"])["parents"] == ([["parent_id", "id"]], JoinCardinality.MANY_TO_ONE)
        assert _joins(models["comp_child"])["comp_pk"] == ([["ca", "a"], ["cb", "b"]], JoinCardinality.MANY_TO_ONE)
        assert _unique(models["uniq_c"]) == {"email"}
        assert _unique(models["uniq_i"]) == {"code"}
        for keyless in ("keyless", VIEW):
            assert (_pk(models[keyless]), _unique(models[keyless]), models[keyless].joins) == ([], set(), [])
