"""The distinct projection of an untrusted hop on a case-folding backend (spec: queries/semantics › Declared
cardinality never changes a value)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, LiteralString, cast

import psycopg
import pytest
from sqlglot import parse_one
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import ServerProfile
from tests._pg_helpers import pg_database, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)

# Cluster P1 appears twice: each account counts once.
DDL = [
    'CREATE TABLE clusters ("CLSTR_PIN" TEXT, score DOUBLE PRECISION)',
    "INSERT INTO clusters VALUES ('P1', 1.5), ('P1', 1.5), ('P2', 2.5)",
    "CREATE TABLE accounts (id INTEGER, clu_ref TEXT, amount DOUBLE PRECISION)",
    "INSERT INTO accounts VALUES (1, 'P1', 10), (2, 'P1', 20), (3, 'P2', 40)",
]


def datasets(ds: str) -> list[SlayerDataset]:
    clusters = SlayerDataset(name="clusters", sql_table="clusters", data_source=ds, fields=[
        DatasetField(name="CLSTR_PIN", datatype=DataType.STRING, primary_key=True),
        DatasetField(name="score", datatype=DataType.FLOAT)])
    accounts = SlayerDataset(
        name="accounts", sql_table="accounts", data_source=ds,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="clu_ref", datatype=DataType.STRING),
                DatasetField(name="amount", datatype=DataType.FLOAT)],
        relationships=[DatasetRelationship(to="clusters", from_columns=["clu_ref"], to_columns=["CLSTR_PIN"],
                                           cardinality=RelationshipCardinality.MANY_TO_ONE)],
        metrics=[DatasetMetric(name="revenue", expression="sum(amount)")])
    return [clusters, accounts]


def _rewritten(version: tuple[int, int, int] | None) -> str:
    dialect = get_dialect("postgres").model_copy(update={"server_profile": ServerProfile(version=version)})
    statement = parse_one("SELECT k, ANY_VALUE(b) AS b, ANY_VALUE(a) AS a FROM t GROUP BY k", read="postgres")
    assert isinstance(statement, Expression)
    return dialect.rewrite_statement(statement).sql(dialect="postgres")


@pytest.mark.parametrize(("version", "native"), [((15, 4, 0), False), (None, False), ((16, 0, 0), True)])
def test_any_value_is_native_only_from_postgres_16(version: tuple[int, int, int] | None, native: bool) -> None:
    assert ("ANY_VALUE" in _rewritten(version)) is native


def test_the_pre_16_any_value_reads_a_non_null_boolean_or_array(postgresql_proc: Any) -> None:
    ddl = ["CREATE TABLE t (k INTEGER, b BOOLEAN, a TEXT[])",
           "INSERT INTO t VALUES (1, NULL, NULL), (1, TRUE, ARRAY['x', 'y']), (2, NULL, NULL)"]
    with pg_database(postgresql_proc, statements=ddl) as ds, psycopg.connect(
            host=ds.host, port=ds.port, user=ds.username, dbname=ds.database) as conn:
        rows = conn.execute(cast(LiteralString, _rewritten((15, 4, 0)) + " ORDER BY k")).fetchall()
    assert rows == [(1, True, ["x", "y"]), (2, None, None)]


async def test_an_unorderable_input_reads_the_representative_row(postgresql_proc: Any, tmp_path: Path) -> None:
    # ``json`` has no ordering: the representative row must not order by it.
    ddl = [*DDL, "ALTER TABLE clusters ADD COLUMN meta JSON", "UPDATE clusters SET meta = '{\"a\": 1}'"]
    with pg_database(postgresql_proc, statements=ddl) as ds:
        found = datasets(ds.name)
        found[0].fields.append(DatasetField(name="meta", datatype=DataType.STRING))
        async with pg_query_engine(ds, base_dir=str(tmp_path), datasets=found) as eng:
            resp = await eng.execute(SlayerQuery.model_validate(
                {"population": "accounts", "metrics": [{"metric": "sum(accounts.amount * CASE WHEN clusters.meta IS NULL THEN 0 ELSE 1 END)", "alias": "n"}]}))
    assert [float(r["n"]) for r in resp.data] == [70.0]


async def test_a_mixed_case_join_key_reads_the_projection(postgresql_proc: Any, tmp_path: Path) -> None:
    with pg_database(postgresql_proc, statements=DDL) as ds:
        async with pg_query_engine(ds, base_dir=str(tmp_path), datasets=datasets(ds.name)) as eng:
            resp = await eng.execute(SlayerQuery.model_validate(
                {"population": "accounts", "dimensions": ["clusters.score"], "metrics": ["accounts.revenue"]}))
    assert {float(r["clusters.score"]): float(r["accounts.revenue"]) for r in resp.data} == {1.5: 30.0, 2.5: 40.0}
