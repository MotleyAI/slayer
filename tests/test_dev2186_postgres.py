"""A FILTERed custom aggregation on a throwaway Postgres.

Spec: aggregations/functional-form › An aggregation accepts a FILTER clause.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2101_fixtures import check_filtered_custom_aggregations, dev2101_models, seed_server
from tests._pg_helpers import client_datasource, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg_engine(postgresql, tmp_path) -> AsyncIterator[SlayerQueryEngine]:
    seed_server(postgresql.cursor(), dialect="postgres")
    postgresql.commit()
    async with pg_query_engine(
        client_datasource(postgresql.info), base_dir=str(tmp_path), models=dev2101_models(data_source="pg"),
    ) as engine:
        yield engine


async def test_filtered_custom_aggregations(pg_engine: SlayerQueryEngine) -> None:
    await check_filtered_custom_aggregations(pg_engine)
