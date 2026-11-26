"""Spec: importers/osi-export › Window functions export as written.

Data: the ``orders`` and ``customers`` tables of ``tests/_dev2457_fixtures.py`` in DuckDB.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.osi.exporter import OsiExportResult
from tests._dev1737_fixtures import server_table_statements
from tests._dev2243_fixtures import semantic_model_module, upstream_validate
from tests._dev2457_fixtures import (
    CUSTOMERS, ORDERS, RN, RN_OF_ID, col, customers_dataset, ints, orders_dataset, raw,
)
from tests._osi_export_helpers import (
    Env, all_metrics, carries, dataset, export, export_datasets, export_text, field, make_env, mentions, metric,
    reimport, rows, same_sql,
)

#: Postgres DDL, which DuckDB reads as written.
STATEMENTS = [s for table in (ORDERS, CUSTOMERS) for s in server_table_statements(table, backend="postgres")]
REVENUE_RANK = "RANK() OVER (ORDER BY SUM(amount) DESC)"
#: Region revenue EU 60, US 150, AS 70.
RANK_OF_REGION = {"US": 1, "AS": 2, "EU": 3}
BY_REGION = {"population": "orders", "dimensions": ["orders.region"], "metrics": ["orders.revenue_rank"]}


def windowed_orders(*, fields: Sequence[DatasetField] = (),
                    metrics: Sequence[DatasetMetric] = ()) -> list[SlayerDataset]:
    return [customers_dataset(), orders_dataset(fields=list(fields), metrics=list(metrics))]


async def answers(env: Env) -> tuple[dict[Any, Any], dict[Any, Any]]:
    """``rn`` per order id, and ``revenue_rank`` per region."""
    rn = ints({int(r["id"]): r["v0"] for r in await rows(env, raw("orders", "orders.rn"))})
    rank = ints({r["orders.region"]: r["orders.revenue_rank"] for r in await rows(env, BY_REGION)})
    return rn, rank


async def validates_upstream(result: OsiExportResult, tmp: Path) -> None:
    assert semantic_model_module().ossie_validity_errors(result.document) == []
    path = tmp / "exported.osi.yaml"
    path.write_text(export_text(result), encoding="utf-8")
    done = upstream_validate([path], tmp=tmp)
    assert done.returncode == 0, done.stdout + done.stderr


async def test_a_windowed_measure_exports_as_a_metric(tmp_path: Path) -> None:
    """Spec scenario: A windowed measure exports as a metric."""
    env = await make_env(tmp_path / "src", statements=STATEMENTS)
    result = await export_datasets(env, *windowed_orders(
        fields=[col("rn", expression=RN)], metrics=[DatasetMetric(name="revenue_rank", expression=REVENUE_RANK)]))
    orders = dataset(result.document, "orders")
    revenue_rank, rn = metric(orders, "revenue_rank"), field(orders, "rn")
    assert revenue_rank is not None
    assert rn is not None
    assert same_sql(revenue_rank["expression"]["sql"], REVENUE_RANK)
    assert same_sql(rn["expression"]["sql"], RN)
    assert not mentions(result, "revenue_rank", category="extension_only")
    await validates_upstream(result, tmp_path)
    assert await answers(env) == (RN_OF_ID, RANK_OF_REGION)
    plain = await export(env, extensions=False)
    for name, document in (("reimported", result), ("plain", plain)):
        target, imported = await reimport(env, document, name=name)
        assert imported.refused == []
        assert await answers(target) == (RN_OF_ID, RANK_OF_REGION), name


async def test_a_slayer_only_operand_keeps_the_measure_in_the_extension(tmp_path: Path) -> None:
    """Spec scenario: A SLayer-only operand keeps the measure in the extension."""
    env = await make_env(tmp_path, statements=STATEMENTS)
    r30 = DatasetMetric(name="r30", expression="RANK() OVER (ORDER BY sum(amount, window='30d') DESC)")
    result = await export_datasets(env, *windowed_orders(metrics=[r30]))
    assert "r30" not in {m["name"] for m in all_metrics(result.document)}
    assert carries(dataset(result.document, "orders"), r30.text)
    assert mentions(result, "r30", severity="info", category="extension_only")


async def test_a_window_inside_a_window_stays_in_the_extension(tmp_path: Path) -> None:
    """Spec scenario: A window inside a window stays in the extension."""
    env = await make_env(tmp_path / "src", statements=STATEMENTS)
    rn_sql, top_sql = "ROW_NUMBER() OVER (ORDER BY id)", "MAX(orders.rn) OVER ()"
    result = await export_datasets(env, *windowed_orders(
        fields=[col("rn", expression=rn_sql), col("top", expression=top_sql)]))
    orders = dataset(result.document, "orders")
    rn = field(orders, "rn")
    assert rn is not None
    assert same_sql(rn["expression"]["sql"], rn_sql)
    assert field(orders, "top") is None
    assert carries(orders, top_sql)
    assert mentions(result, "top", severity="info", category="extension_only")
    await validates_upstream(result, tmp_path)
