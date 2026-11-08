"""A grouped query ordered by an off-grain row expression sorts each group by its MIN (asc) / MAX (desc)."""

import pytest

from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests.test_dev1733_order_only_transform_composite import _SEED, _save_models


@pytest.fixture
async def engine(tmp_path):
    db = tmp_path / "t.db"
    with transaction(str(db)) as conn:
        conn.executescript(_SEED)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=str(db)))
    await _save_models(storage)
    return SlayerQueryEngine(storage=storage)


async def _statuses(engine: SlayerQueryEngine, *, column: str, direction: str) -> list[str]:
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "orders", "dimensions": ["status"], "measures": ["sum(amount)"],
        "order": [{"column": column, "direction": direction}],
    }))
    return [r["orders.status"] for r in resp.data]


# amount - fee: paid 9, 38; open 16, -3.  suppliers.revenue - fee: paid 999, 998; open 1996, 1992.
@pytest.mark.parametrize(("column", "direction", "expected"), [
    ("amount - fee", "desc", ["paid", "open"]),
    ("amount - fee", "asc", ["open", "paid"]),
    ("fee - amount", "desc", ["open", "paid"]),
    ("suppliers.revenue - fee", "desc", ["open", "paid"]),
    ("lower(customers.region)", "asc", ["open", "paid"]),
    ("amount > 30", "desc", ["paid", "open"]),
])
async def test_off_grain_row_expression_sorts_by_group_extremum(
    engine: SlayerQueryEngine, column: str, direction: str, expected: list[str],
) -> None:
    assert await _statuses(engine, column=column, direction=direction) == expected


async def test_same_order_as_explicit_extremum(engine: SlayerQueryEngine) -> None:
    for direction, agg in (("asc", "min"), ("desc", "max")):
        assert await _statuses(engine, column="amount - fee", direction=direction) == await _statuses(
            engine, column=f"{agg}(amount - fee)", direction=direction,
        )


async def test_grouped_by_non_identifier_dimensions(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "orders", "dimensions": ["id", "amount"],
        "order": [{"column": "amount - fee", "direction": "desc"}],
    }))
    assert [r["orders.id"] for r in resp.data] == [2, 3, 1, 4]
