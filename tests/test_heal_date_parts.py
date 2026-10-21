"""Date-part aliases, ``extract`` and the ambiguous weekday names (spec: queries/date-functions)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

import pytest

from slayer.core.errors import GranularityDefinitionError, SlayerError
from slayer.core.models import Aggregation, DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.syntax import Arith, Cmp, Literal, Ref, parse_expr
from tests._dev1737_fixtures import PARTS
from tests._query_heal_fixtures import (
    BACKENDS,
    assert_equivalent,
    heal_engine,
    heal_models,
    orders_model,
    table,
)

ALIASES = {
    "isodow": "day_of_week", "day_of_week": "day_of_week",
    "doy": "day_of_year", "dayofyear": "day_of_year", "day_of_year": "day_of_year",
    "isoyear": "iso_year", "iso_year": "iso_year",
    "isoweek": "week", "iso_week": "week", "weekofyear": "week",
    "dayofmonth": "day", "day_of_month": "day",
}
AMBIGUOUS_WEEKDAYS = ("dow", "dayofweek", "weekday")
BOTH_CONVENTIONS = ("date_part('day_of_week', x)", "date_part('day_of_week', x) % 7")
SPELLINGS = {
    "function": "{alias}(created_at)",
    "part": "date_part('{alias}', created_at)",
    "extract": "extract({alias} from created_at)",
}


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with heal_engine(request.param) as eng:
        yield eng


def canonical(part: str, operand: str = "created_at") -> str:
    return f"date_part('{part}', {operand})"


def every_position(expr: str) -> dict[str, Any]:
    """``expr`` as a dimension, a filter, an aggregation input and inside an order key."""
    return {
        "source_model": "orders",
        "dimensions": [{"expression": expr, "name": "p"}],
        "measures": [{"formula": f"max({expr})", "name": "m"}, {"formula": "count(*)", "name": "n"}],
        "filters": [f"{expr} > 1"],
        "order": [{"column": f"min({expr})", "direction": "desc"}, {"column": "p"}],
    }


async def rejection(engine: SlayerQueryEngine, query: dict[str, Any]) -> str:
    with pytest.raises((ValueError, SlayerError)) as info:
        await engine.execute(query)
    return str(info.value)


class TestAliasParsesAsDatePart:
    @pytest.mark.parametrize("alias", sorted(ALIASES))
    def test_alias_function(self, alias: str) -> None:
        assert parse_expr(f"{alias}(created_at)") == parse_expr(canonical(ALIASES[alias]))

    @pytest.mark.parametrize("alias", sorted(ALIASES))
    def test_alias_part(self, alias: str) -> None:
        assert parse_expr(f"date_part('{alias}', created_at)") == parse_expr(canonical(ALIASES[alias]))

    @pytest.mark.parametrize("spelling", ["ISODOW(created_at)", "IsoDow(created_at)", "Day_Of_Week(created_at)",
                                          "date_part('ISODOW', created_at)", "EXTRACT(IsoDow FROM created_at)"])
    def test_case_folds(self, spelling: str) -> None:
        assert parse_expr(spelling) == parse_expr(canonical("day_of_week"))

    def test_isoweek_part_in_filter(self) -> None:
        assert parse_expr("date_part('ISOWEEK', created_at) = 1") == parse_expr(
            "date_part('week', created_at) = 1")

    @pytest.mark.parametrize(("expr", "count"), [("isodow(created_at, 1)", "2"), ("isodow()", "0")])
    def test_wrong_arity_rejected(self, expr: str, count: str) -> None:
        with pytest.raises(ValueError) as info:
            parse_expr(expr)
        assert "isodow" in str(info.value)
        assert count in str(info.value)

    def test_keywords_rejected(self) -> None:
        with pytest.raises(ValueError, match="isodow"):
            parse_expr("isodow(created_at, unit='x')")

    @pytest.mark.parametrize("bucket", ["week", "quarter", "month", "year", "day"])
    def test_granularity_call_is_not_an_alias(self, bucket: str) -> None:
        assert parse_expr(f"{bucket}(created_at)") != parse_expr(canonical(bucket))


class TestExtractParsesAsDatePart:
    @pytest.mark.parametrize("part", PARTS)
    def test_every_canonical_part(self, part: str) -> None:
        assert parse_expr(f"extract({part} from created_at)") == parse_expr(canonical(part))

    @pytest.mark.parametrize("alias", sorted(ALIASES))
    def test_every_alias_part(self, alias: str) -> None:
        assert parse_expr(f"extract({alias} from created_at)") == parse_expr(canonical(ALIASES[alias]))

    @pytest.mark.parametrize("spelling", [
        "extract('isodow' from created_at)", "EXTRACT(ISODOW FROM created_at)", "Extract(isodow From created_at)",
        "extract(\n  isodow\n  from\n  created_at\n)", "extract( isodow   from   created_at )",
    ])
    def test_spellings(self, spelling: str) -> None:
        assert parse_expr(spelling) == parse_expr(canonical("day_of_week"))

    @pytest.mark.parametrize(("healed", "canonical_text"), [
        ("max(EXTRACT(day FROM coalesce(shipped_at, created_at)))",
         "max(date_part('day', coalesce(shipped_at, created_at)))"),
        ("extract(day from created_at) + 1", "date_part('day', created_at) + 1"),
        ("2 * extract(month from date_add(created_at, 1, 'day'))", "2 * date_part('month', date_add(created_at, 1, 'day'))"),
        ("extract(day from created_at) - extract(day from shipped_at)",
         "date_part('day', created_at) - date_part('day', shipped_at)"),
        ("extract(year from extract_from)", "date_part('year', extract_from)"),
        ("extract(day from from_date)", "date_part('day', from_date)"),
        ("extract(day from created_at) == 'from x'", "date_part('day', created_at) == 'from x'"),
    ])
    def test_nested_and_composed(self, healed: str, canonical_text: str) -> None:
        assert parse_expr(healed) == parse_expr(canonical_text)

    def test_filter_spelling(self) -> None:
        assert parse_expr("EXTRACT(day FROM created_at) = 1") == parse_expr(
            "date_part('day', created_at) = 1")

    def test_keyword_text_in_string_literal_unchanged(self) -> None:
        assert parse_expr("status == 'extract(day from x)'") == Cmp(
            op="==", left=Ref(name="status"), right=Literal(value="extract(day from x)"))

    def test_identifier_named_extract_from_untouched(self) -> None:
        assert parse_expr("extract_from + 1") == Arith(
            op="+", left=Ref(name="extract_from"), right=Literal(value=Decimal(1)))

    @pytest.mark.parametrize("expr", ["myextract(day from created_at)", "éextract(day from created_at)"])
    def test_identifier_ending_in_extract_is_not_extract(self, expr: str) -> None:
        with pytest.raises(ValueError) as info:
            parse_expr(expr)
        assert "date_part" not in str(info.value)

    @pytest.mark.parametrize("expr", [
        "extract(day from created_at", "extract(day from (created_at)", "extract(day from)", "extract(from created_at)",
        "extract(day created_at)",
    ])
    def test_malformed_extract_rejected(self, expr: str) -> None:
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_unknown_part_lists_accepted_parts(self) -> None:
        with pytest.raises(ValueError) as info:
            parse_expr("extract(epoch from created_at) > 0")
        message = str(info.value)
        assert "epoch" in message
        for part in PARTS:
            assert part in message


class TestAmbiguousWeekdays:
    @pytest.mark.parametrize("name", AMBIGUOUS_WEEKDAYS)
    @pytest.mark.parametrize("template", ["date_part('{n}', created_at)", "extract({n} from created_at)"])
    def test_part_names_both_conventions(self, name: str, template: str) -> None:
        text = template.format(n=name) + " = 1"
        with pytest.raises(ValueError) as info:
            parse_expr(text)
        for remedy in BOTH_CONVENTIONS:
            assert remedy in str(info.value)

    @pytest.mark.parametrize("name", [*AMBIGUOUS_WEEKDAYS, "DOW", "DayOfWeek"])
    async def test_function_names_both_conventions(self, engine: SlayerQueryEngine, name: str) -> None:
        message = await rejection(engine, {
            "source_model": "orders",
            "dimensions": [{"expression": f"{name}(created_at)", "name": "d"}], "measures": ["count(*)"],
        })
        for remedy in BOTH_CONVENTIONS:
            assert remedy in message

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_custom_aggregation_named_weekday_still_works(self, backend: str) -> None:
        orders = orders_model(aggregations=[Aggregation(name="weekday", formula="MAX({value})")])
        async with heal_engine(backend, models=heal_models(orders=orders)) as eng:
            resp = await eng.execute({"source_model": "orders", "dimensions": ["region"], "measures": ["weekday(amount)"]})
        assert {r["orders.region"]: r[resp.columns[1]] for r in resp.data} == {"US": 50.0, "EU": 60.0, None: 70.0}


class TestReservedNames:
    @pytest.mark.parametrize("name", [*sorted(ALIASES), "extract", "ISODOW", "Extract"])
    def test_custom_aggregation_rejected(self, name: str) -> None:
        with pytest.raises(ValueError) as info:
            Aggregation(name=name, formula="SUM({value})")
        assert name in str(info.value)

    @pytest.mark.parametrize("name", [*sorted(ALIASES), "extract", "IsoDow"])
    def test_custom_granularity_rejected(self, name: str) -> None:
        ds = DatasourceConfig.model_validate({
            "name": "gds", "type": "sqlite", "database": ":memory:", "granularities": [{"name": name, "base": "day"}],
        })
        with pytest.raises(GranularityDefinitionError) as info:
            ds.check_granularities()
        assert name in str(info.value)


class TestExecutedEquivalence:
    async def test_alias_function_as_named_dimension(self, engine: SlayerQueryEngine) -> None:
        def query(expr: str) -> dict[str, Any]:
            return {"source_model": "orders", "dimensions": [{"expression": expr, "name": "dow"}], "measures": ["count(*)"]}

        resp = await assert_equivalent(engine, query("isodow(created_at)"), query(canonical("day_of_week")))
        assert {r["orders.dow"]: r["orders._count"] for r in resp.data} == {1: 2, 3: 1, 4: 1, 6: 1, 7: 2}

    async def test_extract_with_alias_part(self, engine: SlayerQueryEngine) -> None:
        def query(expr: str) -> dict[str, Any]:
            return {"source_model": "orders", "dimensions": [{"expression": expr, "name": "p"}], "measures": ["count(*)"]}

        await assert_equivalent(engine, query("extract(isodow from created_at)"), query(canonical("day_of_week")))

    async def test_extract_over_nested_operand(self, engine: SlayerQueryEngine) -> None:
        def query(formula: str) -> dict[str, Any]:
            return {"source_model": "orders", "dimensions": ["region"], "measures": [{"formula": formula, "name": "m"}]}

        await assert_equivalent(
            engine, query("max(EXTRACT(day FROM coalesce(shipped_at, created_at)))"),
            query("max(date_part('day', coalesce(shipped_at, created_at)))"),
        )

    async def test_wrong_alias_arity_in_filter(self, engine: SlayerQueryEngine) -> None:
        message = await rejection(engine, {
            "source_model": "orders", "measures": ["count(*)"], "filters": ["isodow(created_at, 1) = 1"],
        })
        assert "isodow" in message
        assert "2" in message

    async def test_unknown_extract_part_in_filter(self, engine: SlayerQueryEngine) -> None:
        message = await rejection(engine, {
            "source_model": "orders", "measures": ["count(*)"], "filters": ["extract(epoch from created_at) > 0"],
        })
        assert "day_of_week" in message

    async def test_keyword_text_in_filter_literal(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute({
            "source_model": "orders", "measures": ["count(*)"], "filters": ["status == 'extract(day from x)'"],
        })
        assert [r["orders._count"] for r in resp.data] in ([0], [])

    @pytest.mark.parametrize("bucket", ["week", "month"])
    async def test_granularity_dimension_keeps_bucket_meaning(self, engine: SlayerQueryEngine, bucket: str) -> None:
        await assert_equivalent(
            engine,
            {"source_model": "orders", "dimensions": [f"{bucket}(created_at)"], "measures": ["count(*)"]},
            {"source_model": "orders", "time_dimensions": [{"dimension": "created_at", "granularity": bucket}],
             "measures": ["count(*)"]},
        )

    @pytest.mark.parametrize("spelling", sorted(SPELLINGS))
    @pytest.mark.parametrize("alias", sorted(ALIASES))
    async def test_every_alias_in_every_position(self, engine: SlayerQueryEngine, alias: str, spelling: str) -> None:
        healed = SPELLINGS[spelling].format(alias=alias)
        resp = await assert_equivalent(
            engine, every_position(healed), every_position(canonical(ALIASES[alias])), ordered=True,
        )
        assert table(resp, ordered=True)
