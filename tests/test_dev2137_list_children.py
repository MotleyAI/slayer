"""Element access: a list column is a keyless one-to-many child (spec: queries/list-children; semantics Axiom 3.2)."""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.relationship_walker import list_child, with_list_children
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.database.client import SlayerSQLClient
from slayer.sql.dialects.base import ServerProfile
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1836_fixtures import broadcast_warnings
from tests._dev1841_fixtures import associated_warnings
from tests._dev2120_fixtures import NATIVE_LIST_DIALECTS, contractors_dataset, emit_contractors
from tests._dev2137_fixtures import all_datasets, list_engine, metric, num, query, twins_dataset
from slayer.storage.base import StorageBackend


@pytest.fixture(scope="module")
async def engine():
    async with list_engine() as e:
        yield e


@pytest.fixture(scope="module")
async def keyless():
    async with list_engine(keyed=False) as e:
        yield e


def _per(resp, key: str, value: str = "n", *, dataset: str = "contractors") -> dict[Any, Any]:
    return {r[f"{dataset}.{key}"]: num(r[value]) for r in resp.data}


class TestTheChild:
    async def test_counting_element_occurrences(self, engine) -> None:
        resp = await engine.execute(query(metrics=[metric("count(contractors.skills.*)", "n")]))
        assert num(resp.data[0]["n"]) == 5

    async def test_keyless_model(self, keyless) -> None:
        resp = await keyless.execute(query(dimensions=["contractors.name"], filter=["contractors.skills.value = 'go'"]))
        assert {r["contractors.name"] for r in resp.data} == {"Ann", "Eve"}

    async def test_parent_columns_attributable_from_the_child(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["contractors.skills.value", "contractors.name"],
                                          metrics=[metric("count(contractors.skills.*)", "n")]))
        got = {(r["contractors.skills.value"], r["contractors.name"]): num(r["n"]) for r in resp.data}
        assert got == {("go", "Ann"): 2, ("sql", "Ann"): 1, ("sql", "Bob"): 1, ("go", "Eve"): 1}

    def test_double_underscore_names_keep_distinct_children(self) -> None:
        def listed(name: str, column: str) -> SlayerDataset:
            return SlayerDataset(name=name, sql_table=name, data_source="test", fields=[
                DatasetField(name=column, datatype=DataType.ARRAY, element_type=DataType.STRING)])

        models = with_list_children({"a__b": listed("a__b", "c"), "a": listed("a", "b__c")})
        assert {m.list_of for m in models.values() if m.list_of} == {("a__b", "c"), ("a", "b__c")}

    def test_a_child_is_built_once_per_parent_and_column(self) -> None:
        parent = contractors_dataset()
        skills = parent.get_field("skills")
        assert skills is not None
        assert list_child(parent=parent, field=skills) is list_child(parent=parent, field=skills)

    def test_a_changed_parent_gets_a_fresh_child(self) -> None:
        parent = contractors_dataset()
        skills = parent.get_field("skills")
        assert skills is not None
        first = list_child(parent=parent, field=skills)
        retyped = skills.model_copy(update={"element_type": DataType.INTEGER})
        changed = parent.model_copy(update={
            "data_source": "other", "fields": [retyped if c is skills else c for c in parent.fields]})
        child = list_child(parent=changed, field=retyped)
        assert child is not first
        value = child.get_field("value")
        assert value is not None
        assert (child.data_source, value.datatype) == ("other", DataType.INTEGER)

    def test_a_column_edited_in_place_gets_a_fresh_child(self) -> None:
        parent = contractors_dataset()
        skills = parent.get_field("skills")
        assert skills is not None
        first = list_child(parent=parent, field=skills)
        skills.element_type = DataType.INTEGER
        child = list_child(parent=parent, field=skills)
        value = child.get_field("value")
        assert child is not first
        assert value is not None
        assert value.datatype is DataType.INTEGER

    async def test_element_type_is_the_columns(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "contractors", "dimensions": ["contractors.scores.value"],
            "metrics": [metric("count(contractors.scores.*)", "n")], "filter": ["contractors.scores.value > '2'"]}))
        assert _per(resp, "scores.value") == {3: 1, 9: 1, 5: 1}


class TestElementFilters:
    async def test_restricting_by_an_element(self, engine) -> None:
        resp = await engine.execute(query(metrics=[metric("count(contractors.*)", "n"), metric("sum(contractors.rate)", "s")],
                                          filter=["contractors.skills.value = 'go'"]))
        assert (num(resp.data[0]["n"]), num(resp.data[0]["s"])) == (2, 60)

    async def test_any_of_several_elements(self, engine) -> None:
        resp = await engine.execute(query(metrics=[metric("count(contractors.*)", "n")],
                                          filter=["contractors.skills.value IN ('go', 'sql')"]))
        assert num(resp.data[0]["n"]) == 3

    async def test_negated_element_filter(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["contractors.name"], filter=["NOT (contractors.skills.value = 'go')"]))
        assert {r["contractors.name"] for r in resp.data} == {"Ann", "Bob"}


class TestElementDimension:
    async def test_element_occurrences_per_skill(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["contractors.skills.value"], metrics=[metric("count(contractors.skills.*)", "n")]))
        assert _per(resp, "skills.value") == {"go": 3, "sql": 2}

    async def test_default_association_of_a_parent_measure(self, engine) -> None:
        q = query(dimensions=["contractors.skills.value"], metrics=[metric("count(*)", "n")])
        with pytest.warns(UserWarning):
            resp = await engine.execute(q)
        assert _per(resp, "skills.value") == {"go": 2, "sql": 2}
        assert associated_warnings(resp)

    async def test_broadcast_of_a_parent_measure(self, engine) -> None:
        q = query(dimensions=["contractors.skills.value"], metrics=[metric("count(*)", "n")], to_many_handling="broadcast")
        with pytest.warns(UserWarning):
            resp = await engine.execute(q)
        assert _per(resp, "skills.value") == {"go": 5, "sql": 5}
        assert broadcast_warnings(resp)


class TestKeylessAssociation:
    async def test_contractors_per_skill(self, keyless) -> None:
        q = query(dimensions=["contractors.skills.value"], metrics=[metric("count(*)", "n")], to_many_handling="associate")
        with pytest.warns(UserWarning):
            resp = await keyless.execute(q)
        assert _per(resp, "skills.value") == {"go": 2, "sql": 2}
        assert associated_warnings(resp)

    async def test_identical_parent_rows_stay_distinct(self, keyless) -> None:
        q = SlayerQuery.model_validate({
            "population": "twins", "dimensions": ["twins.skills.value"], "metrics": [metric("count(*)", "n")],
            "to_many_handling": "associate"})
        with pytest.warns(UserWarning):
            resp = await keyless.execute(q)
        assert _per(resp, "skills.value", dataset="twins") == {"go": 2}

    async def test_another_to_many_hop_uses_the_row_identity(self) -> None:
        contractors = contractors_dataset(keyed=False).model_copy(update={"relationships": [DatasetRelationship(
            to="assignments", from_columns=["id"], to_columns=["contractor_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY)]})
        assignments = SlayerDataset(name="assignments", sql_table="assignments", data_source="test", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="contractor_id", datatype=DataType.INTEGER)])
        async with list_engine(datasets=[contractors, assignments, twins_dataset()]) as e:
            resp = await e.execute(query(dimensions=["contractors.skills.value", "assignments.id"],
                                         metrics=[metric("count(contractors.*)", "n")], to_many_handling="associate"))
        cells = {(r["contractors.skills.value"], r["assignments.id"]): r["n"] for r in resp.data}
        assert cells == {("go", 1): 1, ("go", 2): 1, ("go", 5): 1, ("sql", 1): 1, ("sql", 2): 1, ("sql", 3): 1}
        assert [w for w in resp.warnings or [] if w.kind == "associated"]


class TestNoFanOut:
    async def test_parent_and_element_aggregates_together(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["contractors.team"], metrics=[
            metric("count(contractors.*)", "n"), metric("sum(contractors.rate)", "s"), metric("count_distinct(contractors.skills.value)", "d")]))
        got = {r["contractors.team"]: (num(r["n"]), num(r["s"]), num(r["d"]))
               for r in resp.data}
        assert got == {"A": (2, 30, 2), "B": (3, 120, 1)}


class TestComposition:
    async def test_through_a_to_one_join(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "assignments", "dimensions": ["assignments.id"], "filter": ["contractors.skills.value = 'sql'"]}))
        assert {int(r["assignments.id"]) for r in resp.data} == {1, 2, 3}

    async def test_element_dimension_through_a_to_one_join(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "assignments", "dimensions": ["contractors.skills.value"],
            "metrics": [metric("count(contractors.skills.*)", "n")]}))
        assert _per(resp, "contractors.skills.value", dataset="assignments") == {"go": 3, "sql": 2}

    async def test_derived_list_column(self) -> None:
        derived = DatasetField(name="a_skills", sql="CASE WHEN team = 'A' THEN skills END", datatype=DataType.ARRAY,
                         element_type=DataType.STRING)
        base = contractors_dataset()
        model = base.model_copy(update={"fields": [*base.fields, derived]})
        async with list_engine(datasets=[model, *all_datasets()[1:]]) as e:
            resp = await e.execute(query(dimensions=["contractors.a_skills.value"], metrics=[metric("count(contractors.a_skills.*)", "n")]))
        assert _per(resp, "a_skills.value") == {"go": 2, "sql": 2}

    async def test_element_aggregates_filtered_across_a_declared_to_many(self) -> None:
        contractors = contractors_dataset().model_copy(update={"relationships": [DatasetRelationship(
            to="assignments", from_columns=["id"], to_columns=["contractor_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY)]})
        assignments = SlayerDataset(name="assignments", sql_table="assignments", data_source="test", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="contractor_id", datatype=DataType.INTEGER)])
        async with list_engine(datasets=[contractors, assignments, twins_dataset()]) as e:
            resp = await e.execute(query(dimensions=["contractors.team"], filter=["assignments.id = 1"], metrics=[
                metric("count(contractors.skills.*)", "n"), metric("count_distinct(contractors.skills.value)", "d"),
                metric("collect(contractors.skills.value)", "c")]))
        got = {r["contractors.team"]: (num(r["n"]), num(r["d"]), list(r["c"]))
               for r in resp.data}
        assert got == {"A": (3, 2, ["go", "sql"])}

    async def test_a_collected_list_in_a_later_stage(self, engine) -> None:
        stage = {"name": "s", "population": "contractors", "dimensions": ["contractors.team"],
                 "metrics": [metric("collect(contractors.skills.value)", "team_skills")]}
        kept = await engine.execute([stage, {"population": "s", "dimensions": ["s.team"],
                                             "filter": ["contains(s.team_skills, 'go')"]}])
        assert {_cell(r, "team") for r in kept.data} == {"A", "B"}
        per_skill = await engine.execute([stage, {"population": "s", "dimensions": ["s.team_skills.value"],
                                                  "metrics": [metric("count(s.team_skills.*)", "n")]}])
        assert {_cell(r, "team_skills.value"): num(_cell(r, "n")) for r in per_skill.data} == {"go": 2, "sql": 1}


def _cell(row: dict[str, Any], suffix: str) -> Any:
    (value,) = [v for k, v in row.items() if k == suffix or k.endswith(f".{suffix}")]
    return value


# --------------------------------------------------------------------------- #
# Save-time name collisions
# --------------------------------------------------------------------------- #
def _plain(name: str, *, joins: list[DatasetRelationship] | None = None) -> SlayerDataset:
    return SlayerDataset(name=name, sql_table=name, data_source="test", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)], relationships=joins or [])


@pytest.fixture
async def storage(tmp_path) -> YAMLStorage:
    s = YAMLStorage(base_dir=str(tmp_path))
    await s.save_datasource(DatasourceConfig(name="test", type="duckdb", database=":memory:"))
    return s


class TestNameCollisions:
    async def test_a_list_column_named_like_a_joined_model(self, storage: StorageBackend) -> None:
        await storage.save_dataset(_plain("skills"))
        model = contractors_dataset().model_copy(update={"relationships": [DatasetRelationship(to="skills",
                                                                           from_columns=["id"], to_columns=["id"])]})
        with pytest.raises(ValueError, match="skills"):
            await storage.save_dataset(model)

    async def test_a_list_column_named_like_an_edge(self, storage: StorageBackend) -> None:
        await storage.save_dataset(_plain("people"))
        model = contractors_dataset().model_copy(update={"relationships": [DatasetRelationship(to="people",
                                                                           from_columns=["id"], to_columns=["id"],
                                                                           name="skills")]})
        with pytest.raises(ValueError, match="skills"):
            await storage.save_dataset(model)

    async def test_the_collision_created_from_the_other_side(self, storage: StorageBackend) -> None:
        await storage.save_dataset(contractors_dataset())
        skills = _plain("skills", joins=[DatasetRelationship(to="contractors", from_columns=["id"], to_columns=["id"])])
        with pytest.raises(ValueError, match=r"contractors\.skills"):
            await storage.save_dataset(skills)

    async def test_a_named_edge_created_from_the_other_side(self, storage: StorageBackend) -> None:
        await storage.save_dataset(contractors_dataset())
        people = _plain("people", joins=[DatasetRelationship(to="contractors", from_columns=["id"], to_columns=["id"],
                                                    name="skills")])
        with pytest.raises(ValueError, match=r"contractors\.skills"):
            await storage.save_dataset(people)

    async def test_no_collision_saves(self, storage: StorageBackend) -> None:
        await storage.save_dataset(_plain("people"))
        await storage.save_dataset(contractors_dataset().model_copy(update={"relationships": [DatasetRelationship(
            to="people", from_columns=["id"], to_columns=["id"])]}))


# --------------------------------------------------------------------------- #
# Emission inside the parent's own rows
# --------------------------------------------------------------------------- #
EMITTED = {
    "element-filter": query(metrics=[metric("count(*)", "n")], filter=["contractors.skills.value = 'go'"]),
    "element-dimension": query(dimensions=["contractors.skills.value"], metrics=[metric("count(contractors.skills.*)", "n")]),
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
    q = query(dimensions=["contractors.team"], filter=["assignments.id = 1"],
              metrics=[metric("count(contractors.skills.*)", "n"), metric("collect(contractors.skills.value)", "c")])
    await emit_contractors(q, dialect=dialect, datasets=all_datasets())


ELEMENT_FILTERS = ["contractors.skills.value = 'go'", "contractors.skills.value IN ('go', 'sql')", "NOT (contractors.skills.value = 'go')"]


@pytest.mark.parametrize(("dialect", "function"), [("trino", "ANY_MATCH"), ("clickhouse", "ARRAYEXISTS"),
                                                 ("snowflake", "FILTER(")])
@pytest.mark.parametrize("condition", ELEMENT_FILTERS)
async def test_element_filter_is_a_lambda_without_a_correlated_subquery(
    dialect: str, function: str, condition: str,
) -> None:
    sql = await emit_contractors(query(metrics=[metric("count(contractors.*)", "n")], filter=[condition]), dialect=dialect)
    tree = sqlglot.parse_one(sql, read=dialect)
    assert not list(tree.find_all(exp.Exists)), sql
    assert function in sql.upper(), sql


@pytest.mark.parametrize(("dialect", "function"), [("trino", "ANY_MATCH"), ("clickhouse", "ARRAYEXISTS"),
                                                 ("snowflake", "FILTER(")])
async def test_element_filter_through_a_to_one_join_reads_the_parents_row(
    dialect: str, function: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SlayerSQLClient, "server_profile", _correlating)
    q = SlayerQuery.model_validate({"population": "assignments", "dimensions": ["assignments.id"],
                                    "filter": ["contractors.skills.value = 'sql'"]})
    sql = await emit_contractors(q, dialect=dialect, datasets=all_datasets())
    tree = sqlglot.parse_one(sql, read=dialect)
    assert not list(tree.find_all(exp.Unnest)), sql
    assert not [j for j in tree.find_all(exp.Join) if j.args.get("kind") == "ARRAY"], sql
    assert function in sql.upper(), sql


async def test_a_snowflake_list_child_flattens_the_typed_list_without_a_subquery() -> None:
    sql = await emit_contractors(query(dimensions=["contractors.skills.value"], metrics=[metric("count(contractors.skills.*)", "n")]),
                                 dialect="snowflake")
    tree = sqlglot.parse_one(sql, read="snowflake")
    assert not [s for s in tree.find_all(exp.Subquery) if "FLATTEN" in s.sql("snowflake").upper()], sql
    assert "AS ARRAY(VARCHAR)" in sql.upper(), sql


@pytest.mark.parametrize("expr", ["contains(contractors.skills, 'go')", "contains_any(contractors.skills, ['go'])",
                                  "contains_all(contractors.skills, ['go'])", "length(contractors.skills)"])
async def test_snowflake_list_functions_read_a_structured_list_as_semi_structured(expr: str) -> None:
    sql = await emit_contractors(query(dimensions=["contractors.id", {"field": expr, "alias": "v"}]), dialect="snowflake")
    assert "CAST(CONTRACTORS.SKILLS AS ARRAY)" in sql.upper(), sql
