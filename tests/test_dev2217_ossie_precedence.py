"""Ossie precedence in native resolution: a route Ossie determines uniquely, and an anchor Ossie qualifies uniquely,
win over every other candidate.

Spec: queries/references › The anchor is reached by its only to-one path; queries/population › Dimension-determined
default.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import UnresolvableDimensionRelationshipError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.population.inference import _ossie_anchor
from tests._dev2217_fixtures import NATIVE, STRICT, rows, run_as, sql_engine, strict_error
from tests._dev2243_fixtures import dataset, document_text, field, relationship

CONFORMANCES = (NATIVE, STRICT)

# a → b, c → a, c → b; c's primary key is a_id, so a reaches c to-one in reverse.
TRIANGLE_SQL = """
CREATE TABLE b (id VARCHAR PRIMARY KEY, name VARCHAR);
INSERT INTO b VALUES ('B1', 'x'), ('B2', 'y');
CREATE TABLE a (id VARCHAR PRIMARY KEY, b_id VARCHAR);
INSERT INTO a VALUES ('A1', 'B1'), ('A2', 'B2');
CREATE TABLE c (a_id VARCHAR PRIMARY KEY, b_id VARCHAR);
INSERT INTO c VALUES ('A1', 'B2'), ('A2', 'B1');
"""
TRIANGLE = document_text(name="triangle", datasets=[
    dataset("a", primary_key=["id"], fields=[field("id"), field("b_id")]),
    dataset("b", primary_key=["id"], fields=[field("id"), field("name")]),
    dataset("c", primary_key=["a_id"], fields=[field("a_id"), field("b_id")]),
], relationships=[
    relationship("a_to_b", src="a", to="b", from_columns=["b_id"], to_columns=["id"]),
    relationship("c_to_a", src="c", to="a", from_columns=["a_id"], to_columns=["id"]),
    relationship("c_to_b", src="c", to="b", from_columns=["b_id"], to_columns=["id"]),
])

# a → p → q → b, r → b, r → a; r's primary key is b_id, so b reaches a to-one in two hops through r.
DETOUR_SQL = """
CREATE TABLE a (id VARCHAR PRIMARY KEY, p_id VARCHAR);
INSERT INTO a VALUES ('A1', 'P1'), ('A2', 'P1');
CREATE TABLE p (id VARCHAR PRIMARY KEY, q_id VARCHAR);
INSERT INTO p VALUES ('P1', 'Q1');
CREATE TABLE q (id VARCHAR PRIMARY KEY, b_id VARCHAR);
INSERT INTO q VALUES ('Q1', 'B1');
CREATE TABLE b (id VARCHAR PRIMARY KEY, name VARCHAR);
INSERT INTO b VALUES ('B1', 'p'), ('B2', 'z');
CREATE TABLE r (b_id VARCHAR PRIMARY KEY, a_id VARCHAR);
INSERT INTO r VALUES ('B1', 'A1'), ('B2', 'A2');
"""
DETOUR = document_text(name="detour", datasets=[
    dataset("a", primary_key=["id"], fields=[field("id"), field("p_id")]),
    dataset("p", primary_key=["id"], fields=[field("id"), field("q_id")]),
    dataset("q", primary_key=["id"], fields=[field("id"), field("b_id")]),
    dataset("b", primary_key=["id"], fields=[field("id"), field("name")]),
    dataset("r", primary_key=["b_id"], fields=[field("b_id"), field("a_id")]),
], relationships=[
    relationship("a_to_p", src="a", to="p", from_columns=["p_id"], to_columns=["id"]),
    relationship("p_to_q", src="p", to="q", from_columns=["q_id"], to_columns=["id"]),
    relationship("q_to_b", src="q", to="b", from_columns=["b_id"], to_columns=["id"]),
    relationship("r_to_b", src="r", to="b", from_columns=["b_id"], to_columns=["id"]),
    relationship("r_to_a", src="r", to="a", from_columns=["a_id"], to_columns=["id"]),
])

# a → b, a → c, c → b: two paths of Ossie steps from a to b.
TWO_PATHS_SQL = """
CREATE TABLE b (id VARCHAR PRIMARY KEY, name VARCHAR);
INSERT INTO b VALUES ('B1', 'x');
CREATE TABLE c (id VARCHAR PRIMARY KEY, b_id VARCHAR);
INSERT INTO c VALUES ('C1', 'B1');
CREATE TABLE a (id VARCHAR PRIMARY KEY, b_id VARCHAR, c_id VARCHAR);
INSERT INTO a VALUES ('A1', 'B1', 'C1');
"""
TWO_PATHS = document_text(name="two_paths", datasets=[
    dataset("a", primary_key=["id"], fields=[field("id"), field("b_id"), field("c_id")]),
    dataset("b", primary_key=["id"], fields=[field("id"), field("name")]),
    dataset("c", primary_key=["id"], fields=[field("id"), field("b_id")]),
], relationships=[
    relationship("a_to_b", src="a", to="b", from_columns=["b_id"], to_columns=["id"]),
    relationship("a_to_c", src="a", to="c", from_columns=["c_id"], to_columns=["id"]),
    relationship("c_to_b", src="c", to="b", from_columns=["b_id"], to_columns=["id"]),
])

# x → y and y → z → x: x and y each reach the other along Ossie steps, x in fewer hops.
MUTUAL_SQL = """
CREATE TABLE x (id INTEGER PRIMARY KEY, y_id INTEGER, v VARCHAR);
INSERT INTO x VALUES (1, 1, 'x1'), (2, 1, 'x2');
CREATE TABLE y (id INTEGER PRIMARY KEY, z_id INTEGER, v VARCHAR);
INSERT INTO y VALUES (1, 1, 'y1'), (2, 1, 'y2');
CREATE TABLE z (id INTEGER PRIMARY KEY, x_id INTEGER);
INSERT INTO z VALUES (1, 1);
"""
MUTUAL = document_text(name="mutual", datasets=[
    dataset("x", primary_key=["id"], fields=[field("id", datatype="Integer"), field("y_id", datatype="Integer"),
                                             field("v")]),
    dataset("y", primary_key=["id"], fields=[field("id", datatype="Integer"), field("z_id", datatype="Integer"),
                                             field("v")]),
    dataset("z", primary_key=["id"], fields=[field("id", datatype="Integer"), field("x_id", datatype="Integer")]),
], relationships=[
    relationship("x_to_y", src="x", to="y", from_columns=["y_id"], to_columns=["id"]),
    relationship("y_to_z", src="y", to="z", from_columns=["z_id"], to_columns=["id"]),
    relationship("z_to_x", src="z", to="x", from_columns=["x_id"], to_columns=["id"]),
])


@pytest.mark.parametrize("conformance", CONFORMANCES)
async def test_a_reversed_to_one_hop_never_makes_ossies_path_ambiguous(tmp_path: Path, conformance: str) -> None:
    """A reversed to-one hop never makes Ossie's path ambiguous."""
    async with sql_engine(tmp_path, sql=TRIANGLE_SQL, documents=[TRIANGLE]) as engine:
        response = await run_as(engine, {"dimensions": ["b.name", "a.id"]}, conformance)
    assert rows(response) == [("x", "A1"), ("y", "A2")]


@pytest.mark.parametrize("conformance", CONFORMANCES)
async def test_ossie_settles_a_tie_between_candidates(tmp_path: Path, conformance: str) -> None:
    """Ossie settles a tie between candidates."""
    async with sql_engine(tmp_path, sql=TRIANGLE_SQL, documents=[TRIANGLE]) as engine:
        query = {"dimensions": ["a.b_id", "c.b_id"]} if conformance == NATIVE else {
            "dimensions": ["a.b_id", {"field": "c.b_id", "alias": "c_b_id"}]}
        response = await run_as(engine, query, conformance)
    assert rows(response) == [("B1", "B2"), ("B2", "B1")]
    if conformance == NATIVE:
        assert response.population == "c"


@pytest.mark.parametrize("conformance", CONFORMANCES)
async def test_ossies_anchor_takes_precedence_over_fewer_hops(tmp_path: Path, conformance: str) -> None:
    """Ossie's anchor takes precedence over fewer hops."""
    async with sql_engine(tmp_path, sql=DETOUR_SQL, documents=[DETOUR]) as engine:
        response = await run_as(engine, {"dimensions": ["a.id", "b.name"]}, conformance)
    assert rows(response) == [("A1", "p"), ("A2", "p")]
    if conformance == NATIVE:
        assert response.population == "a"


async def test_ossies_anchor_takes_precedence_for_quoted_names(tmp_path: Path) -> None:
    async with sql_engine(tmp_path, sql=DETOUR_SQL, documents=[DETOUR]) as engine:
        response = await engine.execute({"dimensions": ['"a".id', '"b".name']})
    assert rows(response) == [("A1", "p"), ("A2", "p")]
    assert response.population == "a"


async def test_two_paths_of_ossie_steps_stay_ambiguous(tmp_path: Path) -> None:
    """Two paths of Ossie steps stay ambiguous."""
    async with sql_engine(tmp_path, sql=TWO_PATHS_SQL, documents=[TWO_PATHS]) as engine:
        with pytest.raises(UnresolvableDimensionRelationshipError) as raised:
            await engine.execute({"population": "a", "dimensions": ["b.name"]})
    assert "'a.a_to_b.name'" in str(raised.value)
    assert "'a.a_to_c.c_to_b.name'" in str(raised.value)


async def test_two_paths_of_ossie_steps_under_strict(tmp_path: Path) -> None:
    async with sql_engine(tmp_path, sql=TWO_PATHS_SQL, documents=[TWO_PATHS]) as engine:
        await strict_error(engine, {"dimensions": ["b.name", "a.id"]}, "E_AMBIGUOUS_PATH")


MUTUAL_W_SQL = MUTUAL_SQL + """
CREATE TABLE w (id INTEGER PRIMARY KEY, x_id INTEGER, tag VARCHAR);
INSERT INTO w VALUES (1, 1, 'a');
"""
MUTUAL_W = document_text(name="mutual", datasets=[
    dataset("x", primary_key=["id"], fields=[field("id", datatype="Integer"), field("y_id", datatype="Integer"),
                                             field("v")]),
    dataset("y", primary_key=["id"], fields=[field("id", datatype="Integer"), field("z_id", datatype="Integer"),
                                             field("v")]),
    dataset("z", primary_key=["id"], fields=[field("id", datatype="Integer"), field("x_id", datatype="Integer")]),
    dataset("w", primary_key=["id"], fields=[field("id", datatype="Integer"), field("x_id", datatype="Integer"),
                                             field("tag")]),
], relationships=[
    relationship("x_to_y", src="x", to="y", from_columns=["y_id"], to_columns=["id"]),
    relationship("y_to_z", src="y", to="z", from_columns=["z_id"], to_columns=["id"]),
    relationship("z_to_x", src="z", to="x", from_columns=["x_id"], to_columns=["id"]),
    relationship("w_to_x", src="w", to="x", from_columns=["x_id"], to_columns=["id"]),
])


async def test_a_filter_no_anchor_reaches_precedes_anchor_ambiguity(tmp_path: Path) -> None:
    """Row 16 before row 17: neither qualifying anchor reaches the filter's dataset."""
    async with sql_engine(tmp_path, sql=MUTUAL_W_SQL, documents=[MUTUAL_W]) as engine:
        query = {"dimensions": ["x.v", {"field": "y.v", "alias": "y_v"}], "filter": "w.tag = 'a'"}
        await strict_error(engine, query, "E_UNSUPPORTED_FILTER_PATH")


async def test_several_anchors_ossie_qualifies_leave_native_rules_in_charge(tmp_path: Path) -> None:
    """Several anchors Ossie qualifies leave native rules in charge."""
    async with sql_engine(tmp_path, sql=MUTUAL_SQL, documents=[MUTUAL]) as engine:
        response = await engine.execute({"dimensions": ["x.v", "y.v"]})
        assert response.population == "x"
        assert rows(response) == [("x1", "y1"), ("x2", "y1")]
        await strict_error(engine, {"dimensions": ["x.v", {"field": "y.v", "alias": "y_v"}]}, "E_AMBIGUOUS_PATH")


def _diamond_chain(n: int) -> dict[str, SlayerDataset]:
    """``n0 → … → n{n}`` through many-to-one diamonds, and ``n{n}`` many-to-many to ``goal``: no Ossie route to it."""
    out: dict[str, SlayerDataset] = {}

    def add(name: str, keys: list[str], rels: list[DatasetRelationship]) -> None:
        out[name] = SlayerDataset(name=name, sql_table=name, data_source="ds", relationships=rels, fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            *(DatasetField(name=k, datatype=DataType.INTEGER) for k in keys)])

    def to(target: str, key: str, cardinality: RelationshipCardinality) -> DatasetRelationship:
        return DatasetRelationship(to=target, name=f"{key}_{target}", from_columns=[key], to_columns=["id"],
                                   cardinality=cardinality)

    m2o = RelationshipCardinality.MANY_TO_ONE
    for i in range(n):
        add(f"n{i}", ["l", "r"], [to(f"l{i}", "l", m2o), to(f"r{i}", "r", m2o)])
        for side in ("l", "r"):
            add(f"{side}{i}", ["n"], [to(f"n{i + 1}", "n", m2o)])
    add(f"n{n}", ["g"], [to("goal", "g", RelationshipCardinality.MANY_TO_MANY)])
    add("goal", [], [])
    return out


def test_ossies_anchor_is_settled_without_walking_every_path() -> None:
    datasets = _diamond_chain(20)
    start = time.monotonic()
    assert _ossie_anchor(["n0.id", "goal.id"], datasets_by_name=datasets) is None
    assert time.monotonic() - start < 2
