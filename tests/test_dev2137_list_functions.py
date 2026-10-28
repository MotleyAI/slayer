"""The Mode-B list functions, the array literal and their typing (spec: queries/list-functions); the executed
per-function matrix lives in ``test_dev2120_library_matrix``."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.keys import VALUE_KEY_TYPES, InKey, LiteralKey, ScalarCallKey, _FrozenKey
from slayer.core.models import Column
from slayer.core.scope import ModelScope
from slayer.core.syntax import parse_expr
from slayer.engine.binding import bind_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev2120_fixtures import NATIVE_LIST_DIALECTS, contractors_model, emit_contractors
from tests._dev2137_fixtures import list_engine, measure, num, query


@pytest.fixture(scope="module")
async def engine():
    async with list_engine() as e:
        yield e


async def _values(engine, expr: str, **body: Any) -> dict[str, Any]:
    resp = await engine.execute(query(dimensions=["name", {"expression": expr, "name": "v"}], **body))
    return {r["contractors.name"]: r["contractors.v"] for r in resp.data}


async def _count(engine, filter_: str, **body: Any) -> float | None:
    resp = await engine.execute(query(measures=[measure("count(*)", "n")], filters=[filter_], **body))
    return num(resp.data[0]["contractors.n"])


async def _names(engine, filter_: str, **body: Any) -> set[str]:
    resp = await engine.execute(query(dimensions=["name"], filters=[filter_], **body))
    return {r["contractors.name"] for r in resp.data}


def _truth(value: Any) -> Any:
    return None if value is None else bool(value)


class TestContains:
    async def test_membership(self, engine) -> None:
        got = {k: _truth(v) for k, v in (await _values(engine, "contains(skills, 'go')")).items()}
        assert got == {"Ann": True, "Bob": False, "Cid": False, "Dee": None, "Eve": True}

    async def test_filtering_by_membership(self, engine) -> None:
        assert await _count(engine, "contains(skills, 'go')") == 2

    async def test_absence(self, engine) -> None:
        assert await _names(engine, "NOT contains(skills, 'go')") == {"Bob", "Cid"}

    async def test_the_string_meaning_is_unchanged(self, engine) -> None:
        assert await _names(engine, "contains(team, 'A')") == {"Ann", "Bob"}


class TestContainsAnyAll:
    async def test_all_of_several_values(self, engine) -> None:
        assert await _count(engine, "contains_all(skills, ['go', 'sql'])") == 1

    async def test_any_of_several_values(self, engine) -> None:
        assert await _count(engine, "contains_any(skills, ['go', 'sql'])") == 3

    async def test_another_list_column(self, engine) -> None:
        assert "Ann" in await _names(engine, "contains_any(skills, required)")

    async def test_an_empty_selection(self, engine) -> None:
        empty = {"variables": {"wanted": []}}
        assert await _names(engine, "contains_all(skills, {wanted})", **empty) == {"Ann", "Bob", "Cid", "Eve"}
        assert await _names(engine, "contains_any(skills, {wanted})", **empty) == set()

    async def test_null_list_is_null(self, engine) -> None:
        got = await _values(engine, "contains_any(skills, ['go'])")
        assert got["Dee"] is None


class TestLength:
    async def test_element_counts(self, engine) -> None:
        got = {k: num(v) for k, v in (await _values(engine, "length(skills)")).items()}
        assert got == {"Ann": 3, "Bob": 1, "Cid": 0, "Dee": None, "Eve": 1}

    async def test_character_count_unchanged(self, engine) -> None:
        got = {k: num(v) for k, v in (await _values(engine, "length(name)")).items()}
        assert got == {"Ann": 3, "Bob": 3, "Cid": 3, "Dee": 3, "Eve": 3}


class TestPositions:
    async def test_in_a_measure_formula(self, engine) -> None:
        resp = await engine.execute(query(measures=[measure("sum(length(skills))", "n")]))
        assert num(resp.data[0]["contractors.n"]) == 5

    async def test_in_a_measure_filter(self, engine) -> None:
        resp = await engine.execute(query(measures=[measure("count(*)", "n")], dimensions=["team"],
                                          filters=["contains(skills, 'go')"]))
        assert {r["contractors.team"]: num(r["contractors.n"]) for r in resp.data} == {"A": 1, "B": 1}

    async def test_in_a_case_expression(self, engine) -> None:
        got = await _values(engine, "CASE WHEN contains(skills, 'sql') THEN 'yes' ELSE 'no' END")
        assert got == {"Ann": "yes", "Bob": "yes", "Cid": "no", "Dee": "no", "Eve": "no"}


class TestArrayLiterals:
    async def test_null_element_in_a_literal(self, engine) -> None:
        with pytest.raises(SlayerError, match="NULL"):
            await _count(engine, "contains_any(skills, ['go', NULL])")

    async def test_mixed_literal_types(self, engine) -> None:
        with pytest.raises(SlayerError) as info:
            await _count(engine, "contains_any(skills, ['go', 1])")
        msg = str(info.value)
        assert "'go'" in msg, msg
        assert "1" in msg, msg

    async def test_a_literal_as_a_dimension(self, engine) -> None:
        with pytest.raises(SlayerError) as info:
            await _values(engine, "['go', 'sql']")
        assert "list" in str(info.value).lower() or "ARRAY" in str(info.value), str(info.value)

    async def test_a_literal_as_an_equality_operand(self, engine) -> None:
        with pytest.raises(SlayerError):
            await _count(engine, "name = ['Ann']")

    async def test_untyped_empty_literal(self, engine) -> None:
        with pytest.raises(SlayerError, match=r"\[\]"):
            await _values(engine, "length([])")

    async def test_list_variable_in_contains_any(self, engine) -> None:
        assert await _names(engine, "contains_any(skills, {wanted})", variables={"wanted": ["rust", "go"]}) == {
            "Ann", "Eve"}


class TestArgumentTypes:
    async def test_a_numeric_list_and_a_text_literal(self, engine) -> None:
        assert await _names(engine, "contains(scores, '5')") == {"Eve"}

    async def test_incompatible_types(self, engine) -> None:
        with pytest.raises(SlayerError) as info:
            await _count(engine, "contains(skills, DATE '2025-01-01')")
        msg = str(info.value)
        for needle in ("contains", "TEXT", "DATE"):
            assert needle in msg, msg

    async def test_text_argument_to_a_list_only_function(self, engine) -> None:
        with pytest.raises(SlayerError, match="contains_any"):
            await _count(engine, "contains_any(team, ['A'])")

    async def test_list_argument_to_a_text_only_function(self, engine) -> None:
        with pytest.raises(SlayerError, match="skills"):
            await _values(engine, "upper(skills)")


def _bind(text: str):
    model = contractors_model()
    bundle = ResolvedSourceBundle(dialect="duckdb", source_model=model, referenced_models=[])
    return bind_expr(parse_expr(text), scope=ModelScope(source_model=model), bundle=bundle,
                     allow_measures=True).value_key


class TestArrayLiteralKey:
    def _literal(self) -> _FrozenKey:
        key = _bind("contains_any(skills, ['go', 'sql'])")
        assert isinstance(key, ScalarCallKey), key
        literal = key.args[1]
        assert isinstance(literal, _FrozenKey), literal
        return literal

    def test_is_its_own_value_key_kind(self) -> None:
        literal = self._literal()
        assert type(literal) in VALUE_KEY_TYPES
        assert not isinstance(literal, (LiteralKey, InKey, ScalarCallKey))

    def test_overrides_the_traversal_protocol(self) -> None:
        kind = type(self._literal())
        assert kind.children is not _FrozenKey.children
        assert kind.map_children is not _FrozenKey.map_children

    def test_identity_map_returns_self(self) -> None:
        literal = self._literal()
        assert literal.map_children(lambda k: k) is literal

    def test_equal_literals_share_one_key(self) -> None:
        first, second = self._literal(), self._literal()
        assert first == second
        assert hash(first) == hash(second)


class TestEmission:
    @pytest.mark.parametrize("dialect", [d for d in NATIVE_LIST_DIALECTS if d != "postgres"])
    @pytest.mark.parametrize("expr", ["contains_any(skills, ['go', 'sql'])", "contains_all(skills, ['go', 'sql'])",
                                      "contains_any(skills, required)", "contains_all(skills, required)"])
    async def test_no_postgres_only_operators(self, dialect: str, expr: str) -> None:
        sql = await emit_contractors(query(dimensions=["id", {"expression": expr, "name": "v"}]), dialect=dialect)
        assert "&&" not in sql, sql
        assert "@>" not in sql, sql

    @pytest.mark.parametrize("expr", ["contains(skills, 'go')", "contains_any(skills, required)",
                                      "contains_all(skills, required)", "length(skills)"])
    async def test_no_lambda_arrow_on_duckdb(self, expr: str) -> None:
        sql = await emit_contractors(query(dimensions=["id", {"expression": expr, "name": "v"}]), dialect="duckdb")
        assert "->" not in sql, sql

    @pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
    async def test_date_lists_emit(self, dialect: str) -> None:
        days = Column(name="days", type=DataType.ARRAY, element_type=DataType.DATE)
        model = contractors_model().model_copy(update={"columns": [*contractors_model().columns, days]})
        await emit_contractors(query(dimensions=["id", {"expression": "contains(days, DATE '2025-01-01')",
                                                        "name": "v"}]), dialect=dialect, models=[model])
