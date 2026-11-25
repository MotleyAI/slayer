"""Zone conversion renders on every Tier-1 dialect, once per read, with literals in the converted type
(spec: queries/time-zones › Zone conversion renders on every Tier-1 dialect)."""

from __future__ import annotations

import re

import pytest
from sqlglot import exp

from slayer.core.errors import TimeZoneError
from slayer.sql.dialects import get_dialect

from tests._dev2000_emission_fixtures import (
    INSTANT_DEPTH,
    NAIVE_DEPTH,
    TIER1,
    TIER2,
    conversions_above,
    converted_sides,
    literal_contexts,
    parse,
    reads,
    render,
    zoned_query,
)
from tests._dev2000_fixtures import NY, TOKYO, d, m

FIELDS = {"ordered_at": NAIVE_DEPTH, "paid_at": INSTANT_DEPTH}
#: An IANA zone with no CLDR windowsZones mapping (confirm against the vendored table).
UNMAPPED = "Antarctica/Troll"
JAN_MAR = ["2025-01-01", "2025-03-31"]
_ZONED_CH = re.compile(r"America/New_York")


def _queries(name: str) -> dict[str, dict]:
    ref = f"orders.{name}"
    return {
        "bucket": {"time_dimensions": [{"field": ref, "granularity": "day"}], "metrics": [m("sum(orders.amount)", "s")]},
        "extract": {"dimensions": [d(f"hour({ref})", "h"), d(f"day_of_week({ref})", "dow")],
                    "metrics": [m("count(orders.*)", "n")]},
        "window": {"time_dimensions": [{"field": ref, "granularity": "month"}],
                   "metrics": [m("sum(orders.amount, window='7d')", "w")]},
        "projection": {"dimensions": ["orders.id", ref, d(f"date_add({ref}, 1, 'day')", "next_day")]},
        "aggregate": {"metrics": [m(f"min({ref})", "lo"), m(f"max({ref})", "hi")]},
    }


@pytest.mark.parametrize("shape", ["bucket", "extract", "window", "projection", "aggregate"])
@pytest.mark.parametrize("name", ["ordered_at", "paid_at"])
@pytest.mark.parametrize("dialect", TIER1)
async def test_every_read_converts_once(dialect: str, name: str, shape: str) -> None:
    sql = await render(dialect, zoned_query(**_queries(name)[shape]))
    tree = parse(sql, dialect)
    found = reads(tree, name)
    assert found, sql
    expected = FIELDS[name][dialect]
    depths = [conversions_above(c) for c in found]
    assert depths == [expected] * len(found), f"{depths} != {expected} per read\n{sql}"


def _flat(sql: str) -> str:
    """``sql`` lower-cased without parentheses, identifier quotes or repeated whitespace."""
    return " ".join(re.sub(r"[()\"`]", " ", sql).split()).lower()


@pytest.mark.parametrize("dialect", TIER1)
async def test_without_a_zone_nothing_converts(dialect: str) -> None:
    for name in FIELDS:
        sql = await render(dialect, zoned_query(zone=None, **_queries(name)["bucket"]))
        tree = parse(sql, dialect)
        assert all(conversions_above(c) == 0 for c in reads(tree, name)), sql


@pytest.mark.parametrize(("dialect", "fragment"), [
    ("postgres", "(orders.ordered_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'"),
    ("duckdb", "(orders.ordered_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'"),
    ("snowflake", "CONVERT_TIMEZONE('UTC', 'America/New_York', orders.ordered_at)"),
    ("bigquery", "DATETIME(TIMESTAMP(orders.ordered_at, 'UTC'), 'America/New_York')"),
    ("mysql", "CONVERT_TZ(orders.ordered_at, 'UTC', 'America/New_York')"),
    ("clickhouse", "toTimeZone("),
    ("sqlite", "slayer_convert_tz(orders.ordered_at, 'UTC', 'America/New_York')"),
    ("trino", "with_timezone(orders.ordered_at, 'UTC')"),
    ("tsql", "AT TIME ZONE 'Eastern Standard Time'"),
])
async def test_naive_primitive(dialect: str, fragment: str) -> None:
    sql = await render(dialect, zoned_query(**_queries("ordered_at")["bucket"]))
    assert _flat(fragment) in _flat(sql), sql


@pytest.mark.parametrize(("dialect", "fragment"), [
    ("postgres", "orders.paid_at AT TIME ZONE 'America/New_York'"),
    ("duckdb", "orders.paid_at AT TIME ZONE 'America/New_York'"),
    ("snowflake", "CONVERT_TIMEZONE('America/New_York', orders.paid_at)"),
    ("bigquery", "DATETIME(orders.paid_at, 'America/New_York')"),
    ("mysql", "UNIX_TIMESTAMP(orders.paid_at)"),
    ("clickhouse", "toTimeZone(orders.paid_at, 'America/New_York')"),
    ("trino", "orders.paid_at AT TIME ZONE 'America/New_York'"),
    ("tsql", "orders.paid_at AT TIME ZONE 'Eastern Standard Time'"),
])
async def test_instant_primitive(dialect: str, fragment: str) -> None:
    sql = await render(dialect, zoned_query(**_queries("paid_at")["bucket"]))
    assert _flat(fragment) in _flat(sql), sql


async def test_sql_server_uses_windows_zone_names_only() -> None:
    sql = await render("tsql", zoned_query(**_queries("ordered_at")["bucket"]))
    assert "Eastern Standard Time" in sql, sql
    assert "America/New_York" not in sql, sql
    assert "datetime2" in sql.lower(), sql


@pytest.mark.parametrize(("zone", "windows"), [
    ("Asia/Almaty", "West Asia Standard Time"),  # CLDR 46+: Kazakhstan on UTC+5 since 2024-03-01
    ("Asia/Bishkek", "Central Asia Standard Time"),
    ("America/Vancouver", "Pacific Standard Time"),  # not British Columbia Standard Time, a 2025 Windows zone
    ("America/Edmonton", "Mountain Standard Time"),
])
async def test_sql_server_windows_zone_ids(zone: str, windows: str) -> None:
    sql = await render("tsql", zoned_query(zone=zone, **_queries("ordered_at")["bucket"]))
    assert f"'{windows}'" in sql, sql


async def test_sql_server_unmapped_zone_is_a_typed_error() -> None:
    query = zoned_query(zone=UNMAPPED, **_queries("ordered_at")["bucket"])
    with pytest.raises(TimeZoneError, match=re.escape(UNMAPPED)):
        await render("tsql", query)


@pytest.mark.parametrize("dialect", TIER2)
async def test_tier2_fails_when_a_conversion_is_needed(dialect: str) -> None:
    query = zoned_query(zone="UTC", **_queries("ordered_at")["bucket"])
    with pytest.raises(TimeZoneError):
        await render(dialect, query)


@pytest.mark.parametrize("dialect", TIER2)
async def test_tier2_renders_without_a_conversion(dialect: str) -> None:
    sql = await render(dialect, zoned_query(
        zone="UTC", dimensions=["orders.local_slot"],
        time_dimensions=[{"field": "orders.order_day", "granularity": "month"}], metrics=[m("count(orders.*)", "n")],
    ))
    assert "local_slot" in sql, sql


@pytest.mark.parametrize("name", ["ordered_at", "paid_at"])
async def test_trino_conversions_keep_microseconds(name: str) -> None:
    sql = await render("trino", zoned_query(**_queries(name)["projection"]))
    casts = [c.to.sql("trino") for c in parse(sql, "trino").find_all(exp.Cast)
             if isinstance(c.this, exp.AtTimeZone)]
    assert casts, sql
    assert set(casts) == {"TIMESTAMP(6)"}, sql


async def test_mysql_probes_only_zones_the_statement_converts_to() -> None:
    mysql = get_dialect("mysql")
    converting = parse(await render("mysql", zoned_query(**_queries("ordered_at")["bucket"])), "mysql")
    assert mysql.converted_zones(converting, zones={NY, TOKYO, "UTC"}) == [NY, "UTC"]  # source zone UTC too
    civil = parse(await render("mysql", zoned_query(
        dimensions=["orders.local_slot"], time_dimensions=[{"field": "orders.order_day", "granularity": "month"}],
        metrics=[m("count(orders.*)", "n")],
    )), "mysql")
    assert mysql.converted_zones(civil, zones={NY}) == []


# ---------------------------------------------------------------------------
# Literals, origins, spine and window bounds in the converted type
# ---------------------------------------------------------------------------

def _literal_queries(name: str) -> dict[str, dict]:
    ref = f"orders.{name}"
    return {
        "filter": {"dimensions": ["orders.id"], "filter": [f"date_diff('day', {ref}, '2025-01-01 00:00:00') < 40",
                                                           f"{ref} <> '2025-01-01 00:00:00'"]},
        "date_range": {"time_dimensions": [{"field": ref, "granularity": "month", "date_range": JAN_MAR}],
                       "metrics": [m("sum(orders.amount)", "s")]},
        "origin": {"time_dimensions": [{"field": ref, "granularity": "fiscal_week"}],
                   "metrics": [m("sum(orders.amount)", "s")]},
        "window": {"time_dimensions": [{"field": ref, "granularity": "month", "date_range": JAN_MAR}],
                   "metrics": [m("sum(orders.amount, window='7d')", "w")]},
        "spine": {"time_dimensions": [{"field": "time_spine.timestamp", "granularity": "month", "date_range": JAN_MAR}],
                  "metrics": [m("sum(orders.amount)", "s")]},
    }


_LITERAL_PREFIXES = {"filter": ("2025-01-01",), "date_range": ("2025-01-01", "2025-04-01"),
                     "origin": ("2024-12-30",), "window": ("2025-01-01", "2025-04-01"),
                     "spine": ("2025-01-01", "2025-04-01")}


@pytest.mark.parametrize("shape", ["filter", "date_range", "origin", "window", "spine"])
@pytest.mark.parametrize("name", ["ordered_at", "paid_at"])
async def test_clickhouse_literals_carry_the_query_zone(name: str, shape: str) -> None:
    sql = await render("clickhouse", zoned_query(**_literal_queries(name)[shape]))
    tree = parse(sql, "clickhouse")
    contexts = [c for p in _LITERAL_PREFIXES[shape] for c in literal_contexts(tree, p, dialect="clickhouse")]
    assert contexts, sql
    assert all(_ZONED_CH.search(c) for c in contexts), "\n".join(contexts)
    assert "DateTime64(6, 'America/New_York')" in sql or "6, 'America/New_York')" in sql, sql


@pytest.mark.parametrize("shape", ["filter", "date_range", "origin", "window", "spine"])
@pytest.mark.parametrize("name", ["ordered_at", "paid_at"])
async def test_bigquery_converted_values_meet_datetime_literals(name: str, shape: str) -> None:
    sql = await render("bigquery", zoned_query(**_literal_queries(name)[shape]))
    tree = parse(sql, "bigquery")
    for prefix in _LITERAL_PREFIXES[shape]:
        for ctx in literal_contexts(tree, prefix, dialect="bigquery"):
            assert "AS TIMESTAMP)" not in ctx.upper(), ctx
            assert not ctx.upper().startswith("TIMESTAMP("), ctx
    for _side, other in converted_sides(tree, name):
        assert not other.find(exp.Timestamp), other.sql(dialect="bigquery")
        casts = [c for c in [other, *other.find_all(exp.Cast)] if isinstance(c, exp.Cast)]
        assert all(not c.to.is_type(exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ) for c in casts), sql


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("shape", ["filter", "date_range", "origin", "window", "spine"])
async def test_zoned_shapes_render_on_every_dialect(dialect: str, shape: str) -> None:
    for name in FIELDS:
        sql = await render(dialect, zoned_query(**_literal_queries(name)[shape]))
        tree = parse(sql, dialect)
        if shape != "spine":
            assert any(conversions_above(c) for c in reads(tree, name)), sql
