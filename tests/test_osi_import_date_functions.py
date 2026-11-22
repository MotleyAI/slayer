"""Ossie import: date truncation, date addition, extraction and casts in a metric operand are carried in the
measure formula (spec: importers/osi-import)."""

from pathlib import Path
from typing import Any

import pytest

from tests._osi_export_helpers import Env, convert_document, make_env
from tests._osi_helpers import by_name, ds, f, flat, hidden, m, scalar, unconverted


@pytest.fixture
async def lite(tmp_path: Path) -> Env:
    return await make_env(tmp_path, dialect="sqlite")


def _document(*metrics: dict[str, Any]) -> dict[str, Any]:
    fields = [f("id", "o_orderkey"), f("status"), f("ordered_on"), f("ordered_at")]
    return flat([ds("orders", fields=fields, primary_key=["id"], metrics=list(metrics))])


def _compact(text: str) -> str:
    return "".join(text.split()).lower()


async def _import(env: Env, *metrics: dict[str, Any]) -> Any:
    result = convert_document(env, _document(*metrics))
    for metric in metrics:
        assert not unconverted(result, metric["name"]), result.unconverted_metrics
    return result


def _formula(result: Any, name: str) -> str:
    measure = by_name(result)["orders"].get_metric(name)
    assert measure is not None, name
    return _compact(measure.text)


async def test_date_truncation_operand(lite: Env) -> None:
    result = await _import(lite, m("months", "COUNT(DISTINCT DATE_TRUNC('month', CAST(ordered_at AS TIMESTAMP)))"))
    assert _formula(result, "months") == _compact("count_distinct(date_trunc('month', CAST(ordered_at AS TIMESTAMP)))")
    assert hidden(by_name(result)["orders"]) == []
    value = await scalar(result=result, db=lite.db, dialect="sqlite", dataset="orders", expression="months")
    assert value == 3


@pytest.mark.parametrize("part", ["second", "minute", "hour", "day", "week", "month", "quarter", "year"])
async def test_every_truncation_part_is_carried(lite: Env, part: str) -> None:
    result = await _import(lite, m("t", f"MAX(DATE_TRUNC('{part}', CAST(ordered_at AS TIMESTAMP)))"))
    assert _compact(f"date_trunc('{part}', CAST(ordered_at AS TIMESTAMP))") in _formula(result, "t")
    assert hidden(by_name(result)["orders"]) == []


@pytest.mark.parametrize("part", ["second", "minute", "hour", "day", "week", "month", "quarter", "year"])
async def test_every_date_add_part_is_carried(lite: Env, part: str) -> None:
    result = await _import(lite, m("a", f"MAX(DATEADD({part}, 2, ordered_at))"))
    assert _compact(f"date_add(ordered_at, 2, '{part}')") in _formula(result, "a")
    assert hidden(by_name(result)["orders"]) == []


async def test_date_addition_operand(lite: Env) -> None:
    result = await _import(lite, m("next_day", "MAX(DATEADD(day, 1, ordered_at))"))
    assert _compact("date_add(ordered_at, 1, 'day')") in _formula(result, "next_day")
    assert hidden(by_name(result)["orders"]) == []
    value = await scalar(result=result, db=lite.db, dialect="sqlite", dataset="orders", expression="next_day")
    assert str(value)[:19].replace("T", " ") == "2026-03-03 08:00:00"


@pytest.mark.parametrize("sql", [
    pytest.param("COUNT(DISTINCT DATE_PART(month, ordered_at))", id="date-part"),
    pytest.param("COUNT(DISTINCT EXTRACT(MONTH FROM ordered_at))", id="extract"),
    pytest.param("COUNT(DISTINCT MONTH(ordered_at))", id="month-function"),
])
async def test_extraction_operand(lite: Env, sql: str) -> None:
    result = await _import(lite, m("month_numbers", sql))
    assert _compact("date_part('month', ordered_at)") in _formula(result, "month_numbers")
    assert hidden(by_name(result)["orders"]) == []
    value = await scalar(result=result, db=lite.db, dialect="sqlite", dataset="orders", expression="month_numbers")
    assert value == 3


_FIXED_PARTS = ["year", "quarter", "month", "day", "dayofyear", "hour", "minute", "second"]


@pytest.mark.parametrize("part", _FIXED_PARTS)
@pytest.mark.parametrize("spelling", ["DATE_PART({part}, ordered_at)", "EXTRACT({part} FROM ordered_at)",
                                      "{part}(ordered_at)"])
async def test_every_fixed_extraction_part_is_carried(lite: Env, part: str, spelling: str) -> None:
    result = await _import(lite, m("p", f"MAX({spelling.format(part=part.upper())})"))
    assert _compact(f"date_part('{part}', ordered_at)") in _formula(result, "p")
    assert hidden(by_name(result)["orders"]) == []


@pytest.mark.parametrize(("column", "cast_type"), [
    ("ordered_at", "DATE"), ("ordered_at", "TIME"), ("ordered_at", "TIMESTAMP"), ("id", "VARCHAR"),
    ("id", "INTEGER"), ("id", "BIGINT"), ("id", "DOUBLE"), ("id", "DECIMAL(18,2)"), ("id", "BOOLEAN"),
])
async def test_cast_operand_is_carried(lite: Env, column: str, cast_type: str) -> None:
    result = await _import(lite, m("c", f"COUNT(DISTINCT CAST({column} AS {cast_type}))"))
    assert _compact(f"CAST({column} AS {cast_type})") in _formula(result, "c")
    assert hidden(by_name(result)["orders"]) == []


async def test_extraction_part_without_a_fixed_numbering(tmp_path: Path) -> None:
    duck = await make_env(tmp_path)
    result = await _import(duck, m("weekdays", "COUNT(DISTINCT DATE_PART(dayofweek, ordered_at))"))
    [helper] = hidden(by_name(result)["orders"])
    assert _formula(result, "weekdays") == _compact(f"count_distinct({helper.name})")
    assert await scalar(result=result, db=duck.db, dataset="orders", expression="weekdays") == 3
