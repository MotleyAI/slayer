"""Boolean aggregation parameters and conditional branch types on a throwaway Postgres.

Spec: aggregations/boolean-inputs; queries/mixed-type-coercion.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2177_fixtures import (
    BOOLEAN_PARAMETER_CHECKS,
    check_concat_branches_rejected,
    check_custom_aggregation_receives_raw_boolean,
    dev2177_models,
)
from tests._mcp_idiom_fixtures import idiom_seed_statements

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg_engine(postgresql, tmp_path) -> SlayerQueryEngine:
    cur = postgresql.cursor()
    for stmt in idiom_seed_statements():
        cur.execute(stmt)
    postgresql.commit()
    storage = YAMLStorage(base_dir=str(tmp_path))
    info = postgresql.info
    await storage.save_datasource(DatasourceConfig(
        name="pg", type="postgres", host=info.host, port=info.port,
        database=info.dbname, username=info.user, password="",
    ))
    for model in dev2177_models(data_source="pg"):
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


@pytest.mark.parametrize("check", BOOLEAN_PARAMETER_CHECKS, ids=[c.__name__ for c in BOOLEAN_PARAMETER_CHECKS])
async def test_boolean_parameter(pg_engine: SlayerQueryEngine,
                                 check: Callable[[SlayerQueryEngine], Awaitable[None]]) -> None:
    await check(pg_engine)


async def test_custom_aggregation_receives_raw_boolean(pg_engine: SlayerQueryEngine) -> None:
    await check_custom_aggregation_receives_raw_boolean(pg_engine)


async def test_concatenation_branch_rejected_at_plan_time(pg_engine: SlayerQueryEngine) -> None:
    await check_concat_branches_rejected(pg_engine)
