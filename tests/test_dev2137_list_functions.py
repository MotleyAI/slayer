"""The Mode-B list functions, the array literal and their typing (spec: queries/list-functions); the executed
per-function matrix lives in ``test_dev2120_library_matrix``."""

from __future__ import annotations

import contextlib
from typing import Any

import duckdb
import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ListTypeError, SlayerError
from slayer.core.keys import VALUE_KEY_TYPES, ArrayLiteralKey, InKey, LiteralKey, ScalarCallKey, _FrozenKey
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.refs import key_display
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.engine.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev2120_fixtures import NATIVE_LIST_DIALECTS, contractors_model, emit, emit_contractors
from tests._dev2137_fixtures import list_engine, measure, num, query
from tests._engine_helpers import seeded_exec_engine


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


async def _plan(engine, filter_: str, **body: Any) -> None:
    await engine.execute(query(dimensions=["name"], filters=[filter_], **body), dry_run=True)


def _truth(value: Any) -> Any:
    return None if value is None else bool(value)


def _assert_names(exc: BaseException, *needles: str) -> None:
    msg = str(exc)
    for needle in needles:
        assert needle in msg, msg


class TestContains:
    async def test_membership(self, engine) -> None:
        got = {k: _truth(v) for k, v in (await _values(engine, "contains(skills, 'go')")).items()}
        assert got == {"Ann": True, "Bob": False, "Cid": False, "Dee": None, "Eve": True}

    async def test_filtering_by_membership(self, engine) -> None:
        assert await _count(engine, "contains(skills, 'go')") == 2

    async def test_absence(self, engine) -> None:
        assert await _names(engine, "NOT contains(skills, 'go')") == {"Bob", "Cid"}

    async def test_contains_on_text(self, engine) -> None:
        with pytest.raises(ListTypeError) as info:
            await _names(engine, "contains(team, 'A')")
        _assert_names(info.value, "LIKE")

    async def test_null_value_is_null(self, engine) -> None:
        got = await _values(engine, "contains(skills, pick)")
        assert got["Bob"] is None


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

    async def test_null_second_list_is_null(self, engine) -> None:
        got = await _values(engine, "contains_all(skills, required)")
        assert got["Eve"] is None

    async def test_a_two_list_function_given_a_non_list(self, engine) -> None:
        with pytest.raises(ListTypeError) as info:
            await _count(engine, "contains_any(team, ['A'])")
        _assert_names(info.value, "contains_any", "team", "LIKE")


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
        with pytest.raises(ListTypeError, match="NULL"):
            await _count(engine, "contains_any(skills, ['go', NULL])")

    async def test_mixed_literal_types(self, engine) -> None:
        with pytest.raises(ListTypeError) as info:
            await _count(engine, "contains_any(skills, ['go', 1])")
        _assert_names(info.value, "'go'", "1")

    async def test_a_literal_as_a_dimension(self, engine) -> None:
        with pytest.raises(ListTypeError) as info:
            await _values(engine, "['go', 'sql']")
        assert "list" in str(info.value).lower() or "ARRAY" in str(info.value), str(info.value)

    async def test_a_literal_as_an_equality_operand(self, engine) -> None:
        with pytest.raises(ListTypeError):
            await _count(engine, "name = ['Ann']")

    async def test_untyped_empty_literal(self, engine) -> None:
        with pytest.raises(ListTypeError, match=r"\[\]"):
            await _values(engine, "length([])")

    async def test_empty_literal_typed_by_the_other_operand(self, engine) -> None:
        assert await _names(engine, "contains_all(skills, [])") == {"Ann", "Bob", "Cid", "Eve"}


class TestListVariables:
    async def test_list_variable_in_contains_any(self, engine) -> None:
        assert await _names(engine, "contains_any(skills, {wanted})", variables={"wanted": ["rust", "go"]}) == {
            "Ann", "Eve"}

    async def test_nested_list_variable(self, engine) -> None:
        with pytest.raises(ValueError, match=r"Variable 'wanted' list element must be"):
            await _plan(engine, "contains_any(skills, {wanted})", variables={"wanted": [["go"]]})

    async def test_repeated_placeholder_in_two_list_slots(self, engine) -> None:
        got = await _names(engine, "contains_any(skills, {wanted}) OR contains_all(required, {wanted})",
                           variables={"wanted": ["rust", "go"]})
        assert got == {"Ann", "Eve"}

    async def test_repeated_placeholder_in_a_list_slot_and_an_in_list(self, engine) -> None:
        got = await _names(engine, "contains_any(skills, {wanted}) AND pick IN ({wanted})",
                           variables={"wanted": ["rust", "go"]})
        assert got == {"Eve"}

    @pytest.mark.parametrize(("filter_", "fn"), [
        ("length({wanted}) > 0", "length"),
        ("contains(skills, {wanted})", "contains"),
    ])
    async def test_list_placeholder_in_a_non_list_slot(self, engine, filter_: str, fn: str) -> None:
        """Outside a list slot the variable binds as an IN-list body, so the call's arity refuses it at plan time."""
        with pytest.raises((ValueError, SlayerError), match=rf"{fn}.*argument"):
            await _plan(engine, filter_, variables={"wanted": ["go", "sql"]})


class TestArgumentTypes:
    async def test_a_numeric_list_and_a_text_literal(self, engine) -> None:
        assert await _names(engine, "contains(scores, '5')") == {"Eve"}

    @pytest.mark.parametrize(("text", "kept"), [
        ("'9.0'", {"Ann"}),
        ("'2.5'", set()),
        ("'9e0'", {"Ann"}),
        ("'+9'", {"Ann"}),
        ("' 9 '", {"Ann"}),
        ("'.5e1'", {"Eve"}),
    ])
    async def test_numeric_text_spellings_convert(self, engine, text: str, kept: set[str]) -> None:
        assert await _names(engine, f"contains(scores, {text})") == kept

    async def test_numeric_text_elements_of_a_literal_convert(self, engine) -> None:
        assert await _names(engine, "contains_all(scores, [' 3 ', '9e0'])") == {"Ann"}

    @pytest.mark.parametrize("text", ["'five'", "'NaN'", "'inf'", "'-Infinity'", "''", "'1e'", "'0x9'",
                                      "'9 apples'"])
    async def test_a_numeric_list_and_a_non_numeric_text_literal(self, engine, text: str) -> None:
        with pytest.raises(ListTypeError) as info:
            await _names(engine, f"contains(scores, {text})")
        _assert_names(info.value, "contains", "INT", "TEXT")

    @pytest.mark.parametrize("value", ["team", "upper(team)"])
    async def test_a_numeric_list_and_a_text_column(self, engine, value: str) -> None:
        with pytest.raises(ListTypeError) as info:
            await _names(engine, f"contains(scores, {value})")
        _assert_names(info.value, "contains", "INT", "TEXT")

    @pytest.mark.parametrize("fn", ["contains_any", "contains_all"])
    async def test_a_numeric_list_and_a_text_list(self, engine, fn: str) -> None:
        with pytest.raises(ListTypeError) as info:
            await _names(engine, f"{fn}(scores, codes)")
        _assert_names(info.value, fn, "INT", "TEXT")

    async def test_a_mixed_numeric_text_literal_stays_mixed(self, engine) -> None:
        with pytest.raises(ListTypeError) as info:
            await _names(engine, "contains_any(scores, ['1', 2])")
        _assert_names(info.value, "'1'", "2")

    async def test_incompatible_types(self, engine) -> None:
        with pytest.raises(ListTypeError) as info:
            await _count(engine, "contains(skills, 5)")
        _assert_names(info.value, "contains", "TEXT", "INT")

    async def test_list_argument_to_a_text_only_function(self, engine) -> None:
        with pytest.raises(ListTypeError, match="skills"):
            await _values(engine, "upper(skills)")


# ``mixed``: 1 → scores [2, 3], prices [2.0]; 2 → scores [4], prices [2.5].
MIXED_DDL = ("CREATE TABLE mixed (id INTEGER, scores INTEGER[], prices DOUBLE[])",
             "INSERT INTO mixed VALUES (1, [2, 3], [2.0]), (2, [4], [2.5])")


def mixed_model() -> SlayerModel:
    return SlayerModel(name="mixed", sql_table="mixed", data_source="test", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="scores", type=DataType.ARRAY, element_type=DataType.INT),
        Column(name="prices", type=DataType.ARRAY, element_type=DataType.DOUBLE),
    ])


def _seed_mixed(db_path: str) -> None:
    with contextlib.closing(duckdb.connect(db_path)) as con:
        for stmt in MIXED_DDL:
            con.execute(stmt)


MIXED_FILTERS = ["contains(scores, 2.0)", "contains_any(scores, prices)", "contains_all(prices, [2])",
                 "contains(prices, 2)"]


class TestMixedNumerics:
    @pytest.mark.parametrize("filter_", MIXED_FILTERS)
    async def test_executes_on_duckdb(self, filter_: str) -> None:
        async with seeded_exec_engine(dialect="duckdb", seed=_seed_mixed, models=[mixed_model()]) as (eng, _db):
            resp = await eng.execute(SlayerQuery.model_validate(
                {"source_model": "mixed", "dimensions": ["id"], "filters": [filter_]}))
        assert {int(r["mixed.id"]) for r in resp.data} == {1}, resp.sql

    @pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
    @pytest.mark.parametrize("filter_", MIXED_FILTERS)
    async def test_emits(self, dialect: str, filter_: str) -> None:
        await emit(SlayerQuery.model_validate({"source_model": "mixed", "dimensions": ["id"], "filters": [filter_]}),
                   dialect=dialect, models=[mixed_model()])


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
        assert isinstance(literal, ArrayLiteralKey), literal
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

    def test_displays_as_a_list_inside_a_call(self) -> None:
        assert key_display(_bind("contains_any(skills, ['go', 'sql'])")) == "contains_any(skills, ['go', 'sql'])"

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
        start = Column(name="start", type=DataType.DATE)
        model = contractors_model().model_copy(update={"columns": [*contractors_model().columns, days, start]})
        await emit_contractors(query(dimensions=["id", {"expression": "contains(days, start)", "name": "v"}]),
                               dialect=dialect, models=[model])
