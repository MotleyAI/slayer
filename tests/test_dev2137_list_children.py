"""Element access: a list column is a keyless one-to-many child (spec: queries/list-children; semantics Axiom 3.2)."""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import SlayerError
from slayer.core.join_walker import list_child, with_list_children
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects.base import ServerProfile
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1836_fixtures import broadcast_warnings
from tests._dev1841_fixtures import associated_warnings
from tests._dev2120_fixtures import NATIVE_LIST_DIALECTS, contractors_model, emit_contractors
from tests._dev2137_fixtures import all_models, list_engine, measure, num, query, twins_model


@pytest.fixture(scope="module")
async def engine():
    async with list_engine() as e:
        yield e


@pytest.fixture(scope="module")
async def keyless():
    async with list_engine(keyed=False) as e:
        yield e


def _per(resp, key: str, value: str = "n", *, model: str = "contractors") -> dict[Any, Any]:
    return {r[f"{model}.{key}"]: num(r[f"{model}.{value}"]) for r in resp.data}


class TestTheChild:
    async def test_counting_element_occurrences(self, engine) -> None:
        resp = await engine.execute(query(measures=[measure("count(skills.*)", "n")]))
        assert num(resp.data[0]["contractors.n"]) == 5

    async def test_keyless_model(self, keyless) -> None:
        resp = await keyless.execute(query(dimensions=["name"], filters=["skills.value = 'go'"]))
        assert {r["contractors.name"] for r in resp.data} == {"Ann", "Eve"}

    async def test_parent_columns_attributable_from_the_child(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["skills.value", "name"],
                                          measures=[measure("count(skills.*)", "n")]))
        got = {(r["contractors.skills.value"], r["contractors.name"]): num(r["contractors.n"]) for r in resp.data}
        assert got == {("go", "Ann"): 2, ("sql", "Ann"): 1, ("sql", "Bob"): 1, ("go", "Eve"): 1}

    def test_double_underscore_names_keep_distinct_children(self) -> None:
        def listed(name: str, column: str) -> SlayerModel:
            return SlayerModel(name=name, sql_table=name, data_source="test", columns=[
                Column(name=column, type=DataType.ARRAY, element_type=DataType.TEXT)])

        models = with_list_children({"a__b": listed("a__b", "c"), "a": listed("a", "b__c")})
        assert {m.list_of for m in models.values() if m.list_of} == {("a__b", "c"), ("a", "b__c")}

    def test_a_child_is_built_once_per_parent_and_column(self) -> None:
        parent = contractors_model()
        skills = parent.get_column("skills")
        assert skills is not None
        assert list_child(parent=parent, column=skills) is list_child(parent=parent, column=skills)

    def test_a_changed_parent_gets_a_fresh_child(self) -> None:
        parent = contractors_model()
        skills = parent.get_column("skills")
        assert skills is not None
        first = list_child(parent=parent, column=skills)
        retyped = skills.model_copy(update={"element_type": DataType.INT})
        changed = parent.model_copy(update={
            "data_source": "other", "columns": [retyped if c is skills else c for c in parent.columns]})
        child = list_child(parent=changed, column=retyped)
        assert child is not first
        value = child.get_column("value")
        assert value is not None
        assert (child.data_source, value.type) == ("other", DataType.INT)

    def test_a_column_edited_in_place_gets_a_fresh_child(self) -> None:
        parent = contractors_model()
        skills = parent.get_column("skills")
        assert skills is not None
        first = list_child(parent=parent, column=skills)
        skills.element_type = DataType.INT
        child = list_child(parent=parent, column=skills)
        value = child.get_column("value")
        assert child is not first
        assert value is not None
        assert value.type is DataType.INT

    async def test_element_type_is_the_columns(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "contractors", "dimensions": ["scores.value"],
            "measures": [measure("count(scores.*)", "n")], "filters": ["scores.value > '2'"]}))
        assert _per(resp, "scores.value") == {3: 1, 9: 1, 5: 1}


class TestElementFilters:
    async def test_restricting_by_an_element(self, engine) -> None:
        resp = await engine.execute(query(measures=[measure("count(*)", "n"), measure("sum(rate)", "s")],
                                          filters=["skills.value = 'go'"]))
        assert (num(resp.data[0]["contractors.n"]), num(resp.data[0]["contractors.s"])) == (2, 60)

    async def test_any_of_several_elements(self, engine) -> None:
        resp = await engine.execute(query(measures=[measure("count(*)", "n")],
                                          filters=["skills.value IN ('go', 'sql')"]))
        assert num(resp.data[0]["contractors.n"]) == 3

    async def test_negated_element_filter(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["name"], filters=["NOT (skills.value = 'go')"]))
        assert {r["contractors.name"] for r in resp.data} == {"Ann", "Bob"}


class TestElementDimension:
    async def test_element_occurrences_per_skill(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["skills.value"], measures=[measure("count(skills.*)", "n")]))
        assert _per(resp, "skills.value") == {"go": 3, "sql": 2}

    async def test_default_broadcast_of_a_parent_measure(self, engine) -> None:
        q = query(dimensions=["skills.value"], measures=[measure("count(*)", "n")])
        with pytest.warns(UserWarning):
            resp = await engine.execute(q)
        assert _per(resp, "skills.value") == {"go": 5, "sql": 5}
        assert broadcast_warnings(resp)


class TestKeylessAssociation:
    async def test_contractors_per_skill(self, keyless) -> None:
        q = query(dimensions=["skills.value"], measures=[measure("count(*)", "n")], to_many_handling="associate")
        with pytest.warns(UserWarning):
            resp = await keyless.execute(q)
        assert _per(resp, "skills.value") == {"go": 2, "sql": 2}
        assert associated_warnings(resp)

    async def test_identical_parent_rows_stay_distinct(self, keyless) -> None:
        q = SlayerQuery.model_validate({
            "source_model": "twins", "dimensions": ["skills.value"], "measures": [measure("count(*)", "n")],
            "to_many_handling": "associate"})
        with pytest.warns(UserWarning):
            resp = await keyless.execute(q)
        assert _per(resp, "skills.value", model="twins") == {"go": 2}

    async def test_another_to_many_hop_keeps_the_key_requirement(self) -> None:
        contractors = contractors_model(keyed=False).model_copy(update={"joins": [ModelJoin(
            target_model="assignments", join_pairs=[["id", "contractor_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY)]})
        assignments = SlayerModel(name="assignments", sql_table="assignments", data_source="test", columns=[
            Column(name="id", type=DataType.INT, primary_key=True), Column(name="contractor_id", type=DataType.INT)])
        async with list_engine(models=[contractors, assignments, twins_model()]) as e:
            q = query(dimensions=["skills.value", "assignments.id"],
                      measures=[measure("count(*)", "n")], to_many_handling="associate")
            with pytest.raises(SlayerError, match="primary or unique key"):
                await e.execute(q)


class TestNoFanOut:
    async def test_parent_and_element_aggregates_together(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["team"], measures=[
            measure("count(*)", "n"), measure("sum(rate)", "s"), measure("count_distinct(skills.value)", "d")]))
        got = {r["contractors.team"]: (num(r["contractors.n"]), num(r["contractors.s"]), num(r["contractors.d"]))
               for r in resp.data}
        assert got == {"A": (2, 30, 2), "B": (3, 120, 1)}


class TestComposition:
    async def test_through_a_to_one_join(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "assignments", "dimensions": ["id"], "filters": ["contractors.skills.value = 'sql'"]}))
        assert {int(r["assignments.id"]) for r in resp.data} == {1, 2, 3}

    async def test_element_dimension_through_a_to_one_join(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "assignments", "dimensions": ["contractors.skills.value"],
            "measures": [measure("count(contractors.skills.*)", "n")]}))
        assert _per(resp, "contractors.skills.value", model="assignments") == {"go": 3, "sql": 2}

    async def test_derived_list_column(self) -> None:
        derived = Column(name="a_skills", sql="CASE WHEN team = 'A' THEN skills END", type=DataType.ARRAY,
                         element_type=DataType.TEXT)
        base = contractors_model()
        model = base.model_copy(update={"columns": [*base.columns, derived]})
        async with list_engine(models=[model, *all_models()[1:]]) as e:
            resp = await e.execute(query(dimensions=["a_skills.value"], measures=[measure("count(a_skills.*)", "n")]))
        assert _per(resp, "a_skills.value") == {"go": 2, "sql": 2}

    async def test_element_aggregates_filtered_across_a_declared_to_many(self) -> None:
        contractors = contractors_model().model_copy(update={"joins": [ModelJoin(
            target_model="assignments", join_pairs=[["id", "contractor_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY)]})
        assignments = SlayerModel(name="assignments", sql_table="assignments", data_source="test", columns=[
            Column(name="id", type=DataType.INT, primary_key=True), Column(name="contractor_id", type=DataType.INT)])
        async with list_engine(models=[contractors, assignments, twins_model()]) as e:
            resp = await e.execute(query(dimensions=["team"], filters=["assignments.id = 1"], measures=[
                measure("count(skills.*)", "n"), measure("count_distinct(skills.value)", "d"),
                measure("collect(skills.value)", "c")]))
        got = {r["contractors.team"]: (num(r["contractors.n"]), num(r["contractors.d"]), list(r["contractors.c"]))
               for r in resp.data}
        assert got == {"A": (3, 2, ["go", "sql"])}

    async def test_a_collected_list_in_a_later_stage(self, engine) -> None:
        stage = {"name": "s", "source_model": "contractors", "dimensions": ["team"],
                 "measures": [measure("collect(skills.value)", "team_skills")]}
        kept = await engine.execute([stage, {"source_model": "s", "dimensions": ["team"],
                                             "filters": ["contains(team_skills, 'go')"]}])
        assert {_cell(r, "team") for r in kept.data} == {"A", "B"}
        per_skill = await engine.execute([stage, {"source_model": "s", "dimensions": ["team_skills.value"],
                                                  "measures": [measure("count(team_skills.*)", "n")]}])
        assert {_cell(r, "team_skills.value"): num(_cell(r, "n")) for r in per_skill.data} == {"go": 2, "sql": 1}


def _cell(row: dict[str, Any], suffix: str) -> Any:
    (value,) = [v for k, v in row.items() if k.endswith(f".{suffix}")]
    return value


# --------------------------------------------------------------------------- #
# Save-time name collisions
# --------------------------------------------------------------------------- #
def _plain(name: str, *, joins: list[ModelJoin] | None = None) -> SlayerModel:
    return SlayerModel(name=name, sql_table=name, data_source="test", columns=[
        Column(name="id", type=DataType.INT, primary_key=True)], joins=joins or [])


@pytest.fixture
async def storage(tmp_path) -> YAMLStorage:
    s = YAMLStorage(base_dir=str(tmp_path))
    await s.save_datasource(DatasourceConfig(name="test", type="duckdb", database=":memory:"))
    return s


class TestNameCollisions:
    async def test_a_list_column_named_like_a_joined_model(self, storage) -> None:
        await storage.save_model(_plain("skills"))
        model = contractors_model().model_copy(update={"joins": [ModelJoin(target_model="skills",
                                                                           join_pairs=[["id", "id"]])]})
        with pytest.raises(ValueError, match="skills"):
            await storage.save_model(model)

    async def test_a_list_column_named_like_an_edge(self, storage) -> None:
        await storage.save_model(_plain("people"))
        model = contractors_model().model_copy(update={"joins": [ModelJoin(target_model="people",
                                                                           join_pairs=[["id", "id"]],
                                                                           name="skills")]})
        with pytest.raises(ValueError, match="skills"):
            await storage.save_model(model)

    async def test_the_collision_created_from_the_other_side(self, storage) -> None:
        await storage.save_model(contractors_model())
        skills = _plain("skills", joins=[ModelJoin(target_model="contractors", join_pairs=[["id", "id"]])])
        with pytest.raises(ValueError, match=r"contractors\.skills"):
            await storage.save_model(skills)

    async def test_a_named_edge_created_from_the_other_side(self, storage) -> None:
        await storage.save_model(contractors_model())
        people = _plain("people", joins=[ModelJoin(target_model="contractors", join_pairs=[["id", "id"]],
                                                    name="skills")])
        with pytest.raises(ValueError, match=r"contractors\.skills"):
            await storage.save_model(people)

    async def test_no_collision_saves(self, storage) -> None:
        await storage.save_model(_plain("people"))
        await storage.save_model(contractors_model().model_copy(update={"joins": [ModelJoin(
            target_model="people", join_pairs=[["id", "id"]])]}))


# --------------------------------------------------------------------------- #
# Emission inside the parent's own rows
# --------------------------------------------------------------------------- #
EMITTED = {
    "element-filter": query(measures=[measure("count(*)", "n")], filters=["skills.value = 'go'"]),
    "element-dimension": query(dimensions=["skills.value"], measures=[measure("count(skills.*)", "n")]),
}


def _contractors_scans(sql: str, *, dialect: str) -> int:
    """The most scans of ``contractors`` within any one SELECT (2+ is a self-join)."""
    tree = sqlglot.parse_one(sql, read=dialect)
    return max(sum(1 for t in tree.find_all(exp.Table) if t.name == "contractors" and t.find_ancestor(exp.Select) is s)
               for s in tree.find_all(exp.Select))


@pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
@pytest.mark.parametrize("name", sorted(EMITTED))
async def test_no_self_join(name: str, dialect: str) -> None:
    sql = await emit_contractors(EMITTED[name], dialect=dialect)
    assert _contractors_scans(sql, dialect=dialect) == 1, sql


async def _correlating(_self: Any) -> ServerProfile:
    return ServerProfile(version=(25, 8), readonly=0, correlated_subqueries=True)


@pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
async def test_element_aggregates_filtered_across_another_to_many_emit(
    dialect: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SlayerSQLClient, "server_profile", _correlating)
    q = query(dimensions=["team"], filters=["assignments.id = 1"],
              measures=[measure("count(skills.*)", "n"), measure("collect(skills.value)", "c")])
    await emit_contractors(q, dialect=dialect, models=all_models())


ELEMENT_FILTERS = ["skills.value = 'go'", "skills.value IN ('go', 'sql')", "NOT (skills.value = 'go')"]


@pytest.mark.parametrize(("dialect", "function"), [("trino", "ANY_MATCH"), ("clickhouse", "ARRAYEXISTS"),
                                                 ("snowflake", "FILTER(")])
@pytest.mark.parametrize("condition", ELEMENT_FILTERS)
async def test_element_filter_is_a_lambda_without_a_correlated_subquery(
    dialect: str, function: str, condition: str,
) -> None:
    sql = await emit_contractors(query(measures=[measure("count(*)", "n")], filters=[condition]), dialect=dialect)
    tree = sqlglot.parse_one(sql, read=dialect)
    assert not list(tree.find_all(exp.Exists)), sql
    assert function in sql.upper(), sql


@pytest.mark.parametrize(("dialect", "function"), [("trino", "ANY_MATCH"), ("clickhouse", "ARRAYEXISTS"),
                                                 ("snowflake", "FILTER(")])
async def test_element_filter_through_a_to_one_join_reads_the_parents_row(
    dialect: str, function: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SlayerSQLClient, "server_profile", _correlating)
    q = SlayerQuery.model_validate({"source_model": "assignments", "dimensions": ["id"],
                                    "filters": ["contractors.skills.value = 'sql'"]})
    sql = await emit_contractors(q, dialect=dialect, models=all_models())
    tree = sqlglot.parse_one(sql, read=dialect)
    assert not list(tree.find_all(exp.Unnest)), sql
    assert not [j for j in tree.find_all(exp.Join) if j.args.get("kind") == "ARRAY"], sql
    assert function in sql.upper(), sql


async def test_a_snowflake_list_child_flattens_the_typed_list_without_a_subquery() -> None:
    sql = await emit_contractors(query(dimensions=["skills.value"], measures=[measure("count(skills.*)", "n")]),
                                 dialect="snowflake")
    tree = sqlglot.parse_one(sql, read="snowflake")
    assert not [s for s in tree.find_all(exp.Subquery) if "FLATTEN" in s.sql("snowflake").upper()], sql
    assert "AS ARRAY(VARCHAR)" in sql.upper(), sql


@pytest.mark.parametrize("expr", ["contains(skills, 'go')", "contains_any(skills, ['go'])",
                                  "contains_all(skills, ['go'])", "length(skills)"])
async def test_snowflake_list_functions_read_a_structured_list_as_semi_structured(expr: str) -> None:
    sql = await emit_contractors(query(dimensions=["id", {"expression": expr, "name": "v"}]), dialect="snowflake")
    assert "CAST(CONTRACTORS.SKILLS AS ARRAY)" in sql.upper(), sql
