"""Every column a facade catalog advertises is selectable alone, with its advertised type
(spec: facade/projection; principle 17)."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation
from slayer.facade.catalog import build_catalog
from slayer.pg_facade import protocol as proto
from tests._dev2120_fixtures import contractors_model
from tests._dev2172_fixtures import (
    COLLECT_FALLBACK_XFAIL,
    DS,
    PARTS,
    THINGS,
    FacadeStore,
    Servers,
    build_store,
    flight_query,
    flight_table_schemas,
    pg_query,
    serve,
)

#: Advertised type name (pg ``information_schema`` / JDBC) → the Postgres OID a selection carries.
PG_OIDS: dict[str, int] = {
    "int8": proto.OID_INT8, "text": proto.OID_TEXT, "float8": proto.OID_FLOAT8, "bool": proto.OID_BOOL,
    "date": proto.OID_DATE, "timestamp": proto.OID_TIMESTAMP,
    "BIGINT": proto.OID_INT8, "VARCHAR": proto.OID_TEXT, "DOUBLE": proto.OID_FLOAT8, "BOOLEAN": proto.OID_BOOL,
    "DATE": proto.OID_DATE, "TIMESTAMP": proto.OID_TIMESTAMP,
}
#: Advertised JDBC type name → the Arrow type a Flight selection carries.
ARROW_TYPES: dict[str, pa.DataType] = {
    "BIGINT": pa.int64(), "VARCHAR": pa.utf8(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_(),
    "DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"),
}


@pytest.fixture(scope="module")
def store(tmp_path_factory: pytest.TempPathFactory) -> FacadeStore:
    return asyncio.run(build_store(tmp_path_factory.mktemp("dev2172-catalogs")))


@pytest.fixture(scope="module")
def servers(store: FacadeStore) -> Iterator[Servers]:
    with serve(store) as running:
        yield running


def _sql_column(name: str) -> str:
    """A plain column double-quoted; a dotted joined dimension as written in facade SQL."""
    return name if "." in name else f'"{name}"'


def _pg_information_schema(servers: Servers) -> list[tuple[str, str, str]]:
    rows = pg_query(servers.pg_port, "SELECT table_name, column_name, data_type FROM information_schema.columns "
                                     "WHERE table_schema = 'public'").rows
    return [(t, c, d) for t, c, d in rows]


def _pg_attribute(servers: Servers) -> list[tuple[str, str, str]]:
    oid_names = {proto.OID_INT8: "int8", proto.OID_TEXT: "text", proto.OID_FLOAT8: "float8", proto.OID_BOOL: "bool",
                 proto.OID_DATE: "date", proto.OID_TIMESTAMP: "timestamp"}
    rows = pg_query(servers.pg_port, "SELECT c.relname, a.attname, a.atttypid FROM pg_attribute a "
                                     "JOIN pg_class c ON a.attrelid = c.oid WHERE a.attnum > 0").rows
    tables = {t for t, _, _ in _pg_information_schema(servers)}
    return [(t, c, oid_names[oid]) for t, c, oid in rows if t in tables]


def _dimensions(servers: Servers, *, wire: str) -> list[tuple[str, str, str]]:
    sql = "SELECT table_name, dimension_name, data_type FROM INFORMATION_SCHEMA.DIMENSIONS"
    if wire == "pg":
        return [(t, c, d) for t, c, d in pg_query(servers.pg_port, sql).rows]
    return [(r["table_name"], r["dimension_name"], r["data_type"]) for r in flight_query(servers.flight_port, sql).to_pylist()]


def _flight_columns(servers: Servers) -> list[tuple[str, str, str]]:
    rows = flight_query(servers.flight_port, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS").to_pylist()
    return [(r["table_name"], r["column_name"], r["data_type"]) for r in rows if r["column_kind"] == "DIMENSION"]


def _select_pg(servers: Servers, table: str, column: str, type_name: str) -> str | None:
    try:
        result = pg_query(servers.pg_port, f"SELECT {_sql_column(column)} FROM {table}")
    except Exception as exc:  # noqa: BLE001 — every failure is reported together
        return f"{table}.{column}: {type(exc).__name__}: {exc}"
    if not result.rows:
        return f"{table}.{column}: no rows"
    if result.type_codes != [PG_OIDS[type_name]]:
        return f"{table}.{column}: OID {result.type_codes} != advertised {type_name}"
    return None


def _select_flight(servers: Servers, table: str, column: str, type_name: str) -> str | None:
    try:
        result = flight_query(servers.flight_port, f"SELECT {_sql_column(column)} FROM {table}")
    except Exception as exc:  # noqa: BLE001 — every failure is reported together
        return f"{table}.{column}: {type(exc).__name__}: {str(exc)[:300]}"
    if result.num_rows == 0:
        return f"{table}.{column}: no rows"
    if result.schema.field(0).type != ARROW_TYPES[type_name]:
        return f"{table}.{column}: Arrow {result.schema.field(0).type} != advertised {type_name}"
    return None


def _check_all(advertised: list[tuple[str, str, str]], select: Any, servers: Servers) -> None:
    assert advertised, "the catalog advertises nothing"
    assert {"tags", "payload", "uid", "price", "blob", "span"} <= {c for t, c, _ in advertised if t == "things"}
    failures = [f for t, c, d in advertised if (f := select(servers, t, c, d)) is not None]
    assert not failures, "\n".join(failures)


class TestEveryAdvertisedColumnIsSelectable:
    @COLLECT_FALLBACK_XFAIL
    def test_pg_information_schema_columns(self, servers: Servers) -> None:
        _check_all(_pg_information_schema(servers), _select_pg, servers)

    @COLLECT_FALLBACK_XFAIL
    def test_pg_attribute(self, servers: Servers) -> None:
        _check_all(_pg_attribute(servers), _select_pg, servers)

    def test_pg_information_schema_dimensions(self, servers: Servers) -> None:
        advertised = _dimensions(servers, wire="pg")
        assert any("." in c for _, c, _ in advertised), "joined dimensions are advertised"
        _check_all(advertised, _select_pg, servers)

    def test_flight_information_schema_dimensions(self, servers: Servers) -> None:
        _check_all(_dimensions(servers, wire="flight"), _select_flight, servers)

    def test_flight_information_schema_columns(self, servers: Servers) -> None:
        _check_all(_flight_columns(servers), _select_flight, servers)

    def test_flight_get_tables_schemas(self, servers: Servers) -> None:
        """Every field a ``GetTables(include_schema)`` table schema advertises is selectable with that type."""
        failures = []
        for table, schema in flight_table_schemas(servers.flight_port).items():
            for field in schema:
                try:
                    result = flight_query(servers.flight_port, f"SELECT {_sql_column(field.name)} FROM {table}")
                except Exception as exc:  # noqa: BLE001 — every failure is reported together
                    failures.append(f"{table}.{field.name}: {type(exc).__name__}: {str(exc)[:300]}")
                    continue
                if result.schema.field(0).type != field.type:
                    failures.append(f"{table}.{field.name}: {result.schema.field(0).type} != {field.type}")
        assert not failures, "\n".join(failures)


#: Per-record rows of ``SELECT <column> FROM things``: local and to-one columns once per thing, ``parts.*``
#: once per joined part (a thing without parts keeps one NULL row).
def _thing_rows(column: str) -> Counter[Any]:
    if column == "teams.label":
        return Counter({1: "red", 2: "blue"}[t["team_id"]] for t in THINGS)
    if column == "parts.thing_id":
        return Counter([thing for _, thing, _ in PARTS] + [None])
    if column == "parts.qty":
        return Counter([qty for _, _, qty in PARTS] + [None])
    raise KeyError(column)


class TestFanningAndToOneDimensions:
    @pytest.mark.parametrize("column", ["teams.label", "parts.thing_id", "parts.qty"])
    def test_pg_returns_one_row_per_joined_record(self, servers: Servers, column: str) -> None:
        rows = pg_query(servers.pg_port, f"SELECT {column} FROM things").rows
        assert Counter(r[0] for r in rows) == _thing_rows(column)

    @pytest.mark.parametrize("column", ["teams.label", "parts.thing_id", "parts.qty"])
    def test_flight_returns_one_row_per_joined_record(self, servers: Servers, column: str) -> None:
        table = flight_query(servers.flight_port, f"SELECT {column} FROM things")
        assert Counter(table.column(0).to_pylist()) == _thing_rows(column)

    def test_select_star_omits_fanning_dimensions(self, servers: Servers) -> None:
        result = pg_query(servers.pg_port, "SELECT * FROM things")
        assert not any(c.startswith("parts.") for c in result.columns), result.columns
        assert {"tags", "payload", "uid", "blob", "span"} <= set(result.columns)
        assert len(result.rows) == len(THINGS)

    def test_metabase_left_join_subquery(self, servers: Servers) -> None:
        sql = ('SELECT "Teams"."label" AS "Teams__label" FROM "public"."things" '
               'LEFT JOIN (SELECT * FROM "public"."teams") AS "Teams" '
               'ON "public"."things"."team_id" = "Teams"."id"')
        rows = pg_query(servers.pg_port, sql).rows
        assert Counter(r[0] for r in rows) == _thing_rows("teams.label")


class TestListColumnsInTheCatalog:
    def test_no_custom_aggregation_metric_over_a_list(self) -> None:
        model = contractors_model(data_source=DS).model_copy(
            update={"aggregations": [Aggregation(name="my_agg", formula="SUM({value})")]})
        catalog = build_catalog(models_by_datasource={DS: [model]})
        (table,) = [t for s in catalog.schemas for t in s.tables if t.name == "contractors"]
        names = {m.name for m in table.metrics}
        list_columns = {c.name for c in model.columns if c.type is DataType.ARRAY}
        assert not {f"{c}_my_agg" for c in list_columns} & names
        assert {f"{c.name}_my_agg" for c in model.columns if c.type is not DataType.ARRAY} <= names

    def test_list_column_is_advertised_as_text(self, servers: Servers) -> None:
        advertised = {(t, c): d for t, c, d in _pg_information_schema(servers)}
        assert advertised[("contractors", "skills")] == "text"
        assert advertised[("things", "tags")] == "text"
