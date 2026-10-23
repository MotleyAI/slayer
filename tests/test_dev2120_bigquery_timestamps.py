"""BigQuery: SLayer's TIMESTAMP is naive DATETIME; a zoned TIMESTAMP column compares through a cast of the other side."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine import ingestion
from slayer.engine.introspect_utils import _get_columns_fallback
from tests._dev2120_fixtures import emit_lib, lib_model, lib_query
from tests._engine_helpers import _engine_generate


def _zoned_lib() -> SlayerModel:
    base = lib_model()
    return base.model_copy(update={"columns": [
        c.model_copy(update={"db_type": "TIMESTAMP"}) if c.name == "ts" else c for c in base.columns]})


async def _sql(expr: str, *, dialect: str = "bigquery", filtered: bool = False, zoned: bool = False) -> str:
    query = lib_query(filters=[expr]) if filtered else lib_query(expr)
    sql = await emit_lib(query, dialect=dialect, models=[_zoned_lib()] if zoned else None)
    return " ".join(sql.upper().split())


class TestNaiveValues:
    async def test_now_is_current_datetime(self) -> None:
        sql = await _sql("now()")
        assert "CURRENT_DATETIME()" in sql, sql
        assert "CURRENT_TIMESTAMP" not in sql, sql

    @pytest.mark.parametrize("expr", ["TRY_CAST(code AS TIMESTAMP)", "json_value(payload, '$.t' RETURNING TIMESTAMP)"])
    async def test_portable_timestamp_casts_read_as_datetime(self, expr: str) -> None:
        sql = await _sql(expr)
        assert "AS DATETIME)" in sql, sql
        assert "AS TIMESTAMP)" not in sql, sql

    async def test_author_cast_is_the_databases_own(self) -> None:
        assert "CAST(LIB.CODE AS TIMESTAMP)" in await _sql("CAST(code AS TIMESTAMP)")

    @pytest.mark.parametrize("unit", ["hour", "day", "week", "month"])
    async def test_date_add_stays_datetime(self, unit: str) -> None:
        sql = await _sql(f"date_add(ts, 1, '{unit}')")
        assert "DATETIME_ADD(CAST(LIB.TS AS DATETIME)" in sql, sql
        assert "TIMESTAMP" not in sql, sql

    async def test_sub_day_date_diff_uses_datetime_functions(self) -> None:
        sql = await _sql("date_diff('hour', ts, now())")
        assert "DATETIME_DIFF(" in sql, sql
        assert "DATETIME_TRUNC(" in sql, sql
        assert "TIMESTAMP_" not in sql, sql

    async def test_naive_column_compares_with_now_directly(self) -> None:
        assert "WHERE LIB.TS < CURRENT_DATETIME()" in await _sql("ts < now()", filtered=True)


class TestZonedColumn:
    @pytest.mark.parametrize(("expr", "where"), [
        ("ts < now()", "LIB.TS < CAST(CURRENT_DATETIME() AS TIMESTAMP)"),
        ("now() > ts", "CAST(CURRENT_DATETIME() AS TIMESTAMP) > LIB.TS"),
        ("ts < date_add(now(), -1, 'day')", "LIB.TS < CAST(DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY) AS TIMESTAMP)"),
    ])
    async def test_other_side_converts_and_the_column_stays_bare(self, expr: str, where: str) -> None:
        assert f"WHERE {where}" in await _sql(expr, filtered=True, zoned=True)

    async def test_temporal_literal_keeps_its_iso_text(self) -> None:
        sql = await _sql("ts > TIMESTAMP '2024-01-01 10:00'", filtered=True, zoned=True)
        assert "WHERE LIB.TS > '2024-01-01 10:00:00'" in sql, sql

    async def test_column_against_itself_is_untouched(self) -> None:
        assert "WHERE LIB.TS = LIB.TS" in await _sql("ts = ts", filtered=True, zoned=True)

    @pytest.mark.parametrize(("zoned", "where"), [
        (True, "WHERE S1.TS < CAST(CURRENT_DATETIME() AS TIMESTAMP)"), (False, "WHERE S1.TS < CURRENT_DATETIME()"),
    ])
    async def test_zoned_type_survives_a_stage(self, zoned: bool, where: str) -> None:
        s1 = SlayerQuery.model_validate({"name": "s1", "source_model": "lib", "dimensions": ["id", "ts"]})
        root = SlayerQuery.model_validate({"source_model": "s1", "dimensions": ["id"], "filters": ["ts < now()"]})
        sql = await _engine_generate(query=[s1, root], model=_zoned_lib() if zoned else lib_model(), dialect="bigquery")
        assert where in " ".join(sql.upper().split()), sql

    async def test_other_dialects_ignore_the_db_type(self) -> None:
        zoned = await _sql("ts < now()", dialect="postgres", filtered=True, zoned=True)
        assert zoned == await _sql("ts < now()", dialect="postgres", filtered=True)


class TestIngestionRecordsZonedType:
    @pytest.mark.parametrize(("sa_type", "zoned", "db_type"), [
        (sa.TIMESTAMP(), frozenset({"TIMESTAMP"}), "TIMESTAMP"),
        (sa.DATETIME(), frozenset({"TIMESTAMP"}), None),
        (sa.TIMESTAMP(), frozenset(), None),
    ])
    def test_inspector_column(self, sa_type: sa.types.TypeEngine, zoned: frozenset[str], db_type: str | None) -> None:
        col = ingestion._introspected_column({"type": sa_type}, name="ts", primary_key=False, zoned_types=zoned)
        assert col.type is DataType.TIMESTAMP
        assert col.db_type == db_type

    @pytest.mark.parametrize(("dialect", "db_type"), [("bigquery", "TIMESTAMP"), ("postgresql", None)])
    def test_inspector_path_reads_the_engine_dialect(self, dialect: str, db_type: str | None) -> None:
        engine = MagicMock()
        engine.dialect.name = dialect
        with patch.object(ingestion, "_safe_get_pk_constraint", return_value={}), \
                patch.object(ingestion, "_safe_get_columns", return_value=[{"name": "ts", "type": sa.TIMESTAMP()}]):
            cols = ingestion._introspect_query_columns_via_inspector(engine, MagicMock(), "t", None)
        assert [c.db_type for c in cols] == [db_type]

    @pytest.mark.parametrize(("dialect", "expected"), [
        ("bigquery", {"ts": "TIMESTAMP", "dt": None}), ("postgresql", {"ts": None, "dt": None}),
    ])
    def test_information_schema_fallback(self, dialect: str, expected: dict[str, str | None]) -> None:
        engine = MagicMock()
        engine.dialect.name = dialect
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = [("ts", "TIMESTAMP"), ("dt", "DATETIME")]
        result = _get_columns_fallback(sa_engine=engine, table_name="t", ref=None)
        assert {c["name"]: c["db_type"] for c in result} == expected
