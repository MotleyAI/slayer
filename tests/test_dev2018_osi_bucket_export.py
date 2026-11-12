"""Ossie export: a DATE bucket's exported expression evaluates to the bucket's logical type."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, LiteralString, cast

import psycopg
import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType

from tests._dev2018_fixtures import PG_SESSION_ZONE, physical_type_error
from tests._osi_export_fixtures import PG_DDL, col, customers, orders, orders_columns
from tests._osi_export_helpers import dataset, eval_field, export_models, expression_sql, field, make_env
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)

D = datetime
# ``ordered_on`` per ``o_orderkey``, from the export seed.
MONTHS = [date(2026, 1, 1), date(2026, 1, 1), date(2026, 2, 1), date(2026, 2, 1), date(2026, 3, 1)]
HOURS = [D(2026, 1, 5), D(2026, 1, 6), D(2026, 2, 1), D(2026, 2, 3), D(2026, 3, 2)]
BUCKETS = {"mo": (MONTHS, DataType.DATE), "ho": (HOURS, DataType.TIMESTAMP)}


async def _exported_orders(tmp_path: Path) -> tuple[Any, dict[str, Any]]:
    env = await make_env(tmp_path)
    columns = orders_columns() + [
        col("mo", DataType.DATE, sql="date_trunc('month', ordered_on)"),
        col("ho", DataType.TIMESTAMP, sql="date_trunc('hour', ordered_on)"),
    ]
    result = await export_models(env, customers(), orders(columns=columns))
    return env, dataset(result.document, "orders")


@pytest.mark.parametrize("name", list(BUCKETS))
async def test_portable_reader(tmp_path: Path, name: str) -> None:
    env, ds = await _exported_orders(tmp_path)
    expected, dt = BUCKETS[name]
    values = eval_field(env, ds, name, order_by="o_orderkey")
    assert values == expected
    assert [physical_type_error(v, dt, backend="duckdb") for v in values] == [None] * len(values)


@pytest.mark.parametrize("name", list(BUCKETS))
async def test_postgres_target_in_a_non_utc_session(tmp_path: Path, postgresql_proc: Any, name: str) -> None:
    _env, ds = await _exported_orders(tmp_path)
    entity = field(ds, name)
    assert entity is not None
    expected, dt = BUCKETS[name]
    sql = f"SELECT {expression_sql(entity, target='postgres')} FROM orders ORDER BY o_orderkey"
    with pg_database(postgresql_proc, statements=PG_DDL, name="osi_bucket_pg") as pg:
        with psycopg.connect(host=pg.host, port=pg.port, user=pg.username, dbname=pg.database) as conn:
            conn.execute(cast(LiteralString, f"SET TIME ZONE '{PG_SESSION_ZONE}'"))
            values = [r[0] for r in conn.execute(cast(LiteralString, sql)).fetchall()]
    assert values == expected
    assert [physical_type_error(v, dt, backend="postgres") for v in values] == [None] * len(values)
