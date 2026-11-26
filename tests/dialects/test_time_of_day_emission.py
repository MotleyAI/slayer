"""A time of day on every dialect: the constructs emit on each Tier-1 database, MariaDB as MySQL, and Tier-2 keeps
its cast and refuses the rest (spec: queries/time-of-day › Every Tier-1 database holds a time of day)."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import CastTypeError, SlayerError, UnsupportedServerFunctionError
from slayer.sql.dialects import dialect_for_ds_type
from tests._dev2000_fixtures import PinnedClock, instant
from tests._dev2120_fixtures import patch_profile
from tests._ossie_gate import TIER1
from tests._time_of_day_fixtures import NY, d, emit, m, q

#: Ossie's five required time-of-day rows, the three parts, and the other uses of the type.
CONSTRUCTS: dict[str, str] = {
    "CURRENT_TIME": "CURRENT_TIME",
    "CURRENT_TIME()": "CURRENT_TIME()",
    "TIME literal": "TIME '10:30:00'",
    "CAST(text AS TIME)": "CAST('10:30:00' AS TIME)",
    "CAST AS TIME": "CAST(shifts.label AS TIME)",
    "hour": "date_part('hour', shifts.starts_at)",
    "minute": "EXTRACT(MINUTE FROM TIME '10:30:45')",
    "second": "SECOND(shifts.starts_at)",
    "::TIME": "shifts.created_at::TIME",
    "TRY_CAST": "TRY_CAST(shifts.label AS TIME)",
    "to text": "CAST(shifts.starts_at AS VARCHAR)",
    "field": "shifts.starts_at",
    "conditional": "coalesce(shifts.ends_at, TIME '23:59:59')",
}
#: Tier-2 databases that render a ``TIME`` cast today, and those that refuse it.
TIER2_CASTS = ("redshift", "oracle")
TIER2_NO_CAST = ("databricks", "spark")
TIER2 = (*TIER2_CASTS, *TIER2_NO_CAST)
NEW_YEAR = "2025-01-01 03:30:00"  # 22:30 the day before in New York


@pytest.fixture(autouse=True)
def _current_server(monkeypatch: pytest.MonkeyPatch) -> None:
    patch_profile(monkeypatch, version=(99, 0, 0))


def _time_casts(sql: str, *, ds_type: str) -> list[exp.Cast]:
    tree = sqlglot.parse_one(sql, dialect=dialect_for_ds_type(ds_type).sqlglot_name)
    return [c for c in tree.find_all(exp.Cast) if c.to.this is exp.DataType.Type.TIME]


@pytest.mark.parametrize("dialect", TIER1)
class TestTier1:
    @pytest.mark.parametrize("name", sorted(CONSTRUCTS))
    async def test_a_construct_emits(self, dialect: str, name: str) -> None:
        await emit(q(dimensions=["shifts.id", d(CONSTRUCTS[name])]), dialect=dialect)

    async def test_filter_aggregate_and_order_emit(self, dialect: str) -> None:
        await emit(q(
            dimensions=["shifts.starts_at"],
            metrics=[m("min(shifts.ends_at)", "lo"), m("max(shifts.ends_at)", "hi"),
                     m("count_distinct(shifts.ends_at)", "n"), m("first(shifts.ends_at, shifts.created_at)", "f")],
            filter=["shifts.starts_at >= '09:00'", "shifts.ends_at BETWEEN TIME '12:00' AND CURRENT_TIME",
                    "shifts.starts_at IN ('09:30', '17:00')", "shifts.ends_at IS DISTINCT FROM TIME '17:00'"],
            order=[{"field": "shifts.starts_at", "direction": "desc"}],
        ), dialect=dialect)

    async def test_the_literal_keeps_its_microseconds(self, dialect: str) -> None:
        sql = await emit(q(dimensions=[d("TIME '10:30:00.123456'")]), dialect=dialect)
        assert "10:30:00.123456" in sql, sql

    async def test_under_a_zone_the_clock_is_a_literal(self, dialect: str) -> None:
        sql = await emit(q(time_zone=NY, dimensions=[d("current_time()", "ct"), d("now()", "nw")]), dialect=dialect,
                         clock=PinnedClock(instant(NEW_YEAR)))
        assert "22:30:00" in sql, sql
        assert "CURRENT_TIME" not in sql.upper().replace("CURRENT_TIMESTAMP", ""), sql

    async def test_a_time_field_is_not_zone_converted(self, dialect: str) -> None:
        plain = await emit(q(dimensions=["shifts.starts_at"]), dialect=dialect, datasource_fields={"time_zone": "UTC"})
        zoned = await emit(q(dimensions=["shifts.starts_at"], time_zone=NY), dialect=dialect,
                           datasource_fields={"time_zone": "UTC"})
        assert zoned == plain


class TestSqliteText:
    @pytest.mark.parametrize(("text", "canonical"), [("TIME '10:30:00'", "10:30:00.000000"),
                                                     ("TIME '10:30'", "10:30:00.000000"),
                                                     ("TIME '10:30:00.5'", "10:30:00.500000")])
    async def test_a_literal_is_canonical_text(self, text: str, canonical: str) -> None:
        sql = await emit(q(dimensions=[d(text)]), dialect="sqlite")
        assert f"'{canonical}'" in sql, sql


class TestMariadb:
    @pytest.mark.parametrize("name", sorted(CONSTRUCTS))
    async def test_as_mysql(self, name: str) -> None:
        query = q(dimensions=["shifts.id", d(CONSTRUCTS[name])])
        assert await emit(query, dialect="mariadb") == await emit(query, dialect="mysql")


class TestTier2:
    @pytest.mark.parametrize("ds_type", TIER2_CASTS)
    @pytest.mark.parametrize("text", ["CAST('10:30:00' AS TIME)", "TIME '10:30:00'"])
    async def test_the_literal_and_the_cast_keep_the_cast(self, ds_type: str, text: str) -> None:
        sql = await emit(q(dimensions=[d(text)]), dialect=ds_type)
        casts = _time_casts(sql, ds_type=ds_type)
        assert casts, sql
        assert all(isinstance(c.this, exp.Literal) and c.this.name == "10:30:00" for c in casts), sql

    @pytest.mark.parametrize("ds_type", TIER2_NO_CAST)
    @pytest.mark.parametrize("text", ["CAST('10:30:00' AS TIME)", "TIME '10:30:00'"])
    async def test_a_database_without_the_type_still_refuses(self, ds_type: str, text: str) -> None:
        with pytest.raises(CastTypeError) as info:
            await emit(q(dimensions=[d(text)]), dialect=ds_type)
        assert ds_type in str(info.value).lower()

    @pytest.mark.parametrize("ds_type", TIER2)
    @pytest.mark.parametrize("text", ["current_time()", "CURRENT_TIME", "CURRENT_TIME()"])
    async def test_the_clock_is_refused(self, ds_type: str, text: str) -> None:
        with pytest.raises(UnsupportedServerFunctionError) as info:
            await emit(q(dimensions=[d(text)]), dialect=ds_type)
        msg = str(info.value)
        assert dialect_for_ds_type(ds_type).database_name in msg, msg
        assert "current_time" in msg.lower(), msg

    @pytest.mark.parametrize("ds_type", TIER2)
    @pytest.mark.parametrize("text", ["date_part('hour', shifts.starts_at)", "EXTRACT(MINUTE FROM shifts.starts_at)",
                                      "SECOND(shifts.starts_at)"])
    async def test_the_parts_of_a_time_of_day_are_refused(self, ds_type: str, text: str) -> None:
        with pytest.raises(SlayerError) as info:
            await emit(q(dimensions=[d(text)]), dialect=ds_type)
        assert dialect_for_ds_type(ds_type).database_name in str(info.value), str(info.value)

    @pytest.mark.parametrize("ds_type", TIER2)
    async def test_the_parts_of_a_timestamp_still_emit(self, ds_type: str) -> None:
        await emit(q(dimensions=[d("date_part('hour', shifts.created_at)")]), dialect=ds_type)
