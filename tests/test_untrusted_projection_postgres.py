"""The distinct projection of an untrusted hop on a case-folding backend (spec: queries/semantics › Declared
cardinality never changes a value)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
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


async def test_a_mixed_case_join_key_reads_the_projection(postgresql_proc: Any, tmp_path: Path) -> None:
    with pg_database(postgresql_proc, statements=DDL) as ds:
        async with pg_query_engine(ds, base_dir=str(tmp_path), datasets=datasets(ds.name)) as eng:
            resp = await eng.execute(SlayerQuery.model_validate(
                {"population": "accounts", "dimensions": ["clusters.score"], "metrics": ["accounts.revenue"]}))
    assert {float(r["clusters.score"]): float(r["accounts.revenue"]) for r in resp.data} == {1.5: 30.0, 2.5: 40.0}
