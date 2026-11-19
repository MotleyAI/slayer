"""Postgres ingestion assigns Ossie's finer datatypes (spec: compatibility/ossie-vocabulary › Ossie datatype values)."""

from pathlib import Path

import pytest

from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine
from tests._pg_helpers import pg_database

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


async def test_ingestion_assigns_the_finer_type(postgresql, tmp_path: Path) -> None:
    statements = ["CREATE TABLE t (id INTEGER PRIMARY KEY, price NUMERIC(10, 2), ratio DOUBLE PRECISION, "
                  "at TIMESTAMPTZ, ts TIMESTAMP, d DATE)"]
    with pg_database(postgresql.info, statements=statements) as ds:
        async with owned_query_engine(storage=YAMLStorage(base_dir=str(tmp_path / "store"))) as engine:
            [dataset] = await engine.ingest(ds)
    types = {f.name: f.datatype.value for f in dataset.fields}
    assert types == {"id": "Integer", "price": "Decimal", "ratio": "Float", "at": "DateTimeTz", "ts": "DateTime",
                     "d": "Date"}
