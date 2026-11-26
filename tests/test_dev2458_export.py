"""Ossie export of the DISTINCT and percentile aggregates (spec: importers/osi-export › Metrics)."""

from pathlib import Path
from typing import Any

import pytest

from tests._osi_export_fixtures import customers, orders, slayer_metric
from tests._osi_export_helpers import dataset, export_datasets, make_env, metric, reimport, rows, same_sql

#: Export seed: quantity 1, 2, 3, 1, 4; amount 10, 5, 7, 4, 20.
PORTABLE = [("q", "sum_distinct(quantity)", "SUM(DISTINCT quantity)", 10),
            ("p25", "percentile_disc(amount, 0.25)", "PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY amount)", 5)]


def _metric(document: dict[str, Any], name: str) -> dict[str, Any]:
    found = metric(dataset(document, "orders"), name)
    assert found is not None, name
    return found


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
async def test_distinct_sum_and_discrete_percentile_are_portable(tmp_path: Path, dialect: str) -> None:
    """Spec: DISTINCT sum and discrete percentile are portable."""
    env = await make_env(tmp_path, dialect=dialect)
    model = orders(metrics=[slayer_metric(name, formula) for name, formula, _, _ in PORTABLE])
    result = await export_datasets(env, customers(), model)
    for name, _, sql, _ in PORTABLE:
        assert same_sql(_metric(result.document, name)["expression"]["sql"], sql)
    imported, _ = await reimport(env, result.document)
    data = await rows(imported, {"population": "orders", "metrics": [f"orders.{n}" for n, _, _, _ in PORTABLE]})
    assert [float(data[0][f"orders.{n}"]) for n, _, _, _ in PORTABLE] == [float(v) for _, _, _, v in PORTABLE]


async def test_distinct_average_and_approximate_percentile_are_dialect_entries(tmp_path: Path) -> None:
    """Spec: DISTINCT average and approximate percentile are dialect entries."""
    env = await make_env(tmp_path)
    model = orders(metrics=[slayer_metric("a", "avg_distinct(quantity)"),
                            slayer_metric("m", "percentile_approx(amount, 0.5)")])
    result = await export_datasets(env, customers(), model)
    for name in ("a", "m"):
        expression = _metric(result.document, name)["expression"]
        assert "sql" not in expression
        assert [d["dialect"] for d in expression["dialects"]] == ["DUCKDB"]
    imported, _ = await reimport(env, result.document)
    data = await rows(imported, {"population": "orders", "metrics": ["orders.a", "orders.m"]})
    assert float(data[0]["orders.a"]) == pytest.approx(2.5)
    assert float(data[0]["orders.m"]) == pytest.approx(7, abs=1)
